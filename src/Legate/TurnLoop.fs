// SPDX-License-Identifier: Apache-2.0
#nowarn "3261"

namespace Legate

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.AI

// Internal Akka-free ReAct loop core. Per iteration the loop drains pending
// Inject inbox entries at the iteration boundary (after the prior tool
// results are appended, before the next provider call), folds each
// UserMessagePayload into a ChatRole.User message in position order, and
// repeats until a response carries no function calls. Folding never spends
// the MaxIterations budget; the timeout still bounds the turn. Unknown tool
// names and tool exceptions map to Error: texts and continue; cancellation
// and lease loss propagate. Tool results over the configured char limit are
// truncated with a single marker. Per-turn budgets (MaxIterations,
// Timeout) are enforced here; metadata wrap belongs to a follow-up issue.
// The wall-clock budget is a hard deadline fired off the injected ILlmDelay
// seam (issue 35), never the real clock: under a virtual clock the deadline
// fires when the test advances past it, and a delay that is already complete
// settles the turn as Failed immediately. The provider path streams via LlmStreaming (one TextDelta per non-empty
// text chunk, one ReasoningDelta per non-empty reasoning chunk, full
// messages accumulated with raw blocks intact, single-delta fallback for
// non-streaming providers).
//
// Nullness warning 3261 is suppressed in this file: MEAI interop surfaces
// nulls (null responses, messages, contents, usage, result objects) that the
// F# nullable analysis cannot prove absent, and the loop treats every one as
// empty rather than failing.
module internal TurnLoop =

    /// Marker appended when a tool result exceeds the configured char limit.
    [<Literal>]
    let TruncationMarker = "[truncated]"

    /// Tool result text used when the model calls a tool name absent from the resolved map.
    [<Literal>]
    let UnknownToolMessage = "Error: Unknown tool"

    /// Default maximum tool-result chars before truncation.
    [<Literal>]
    let DefaultMaxToolResultChars = 4000

    /// Reason carried by <see cref="T:Legate.TurnFailed" /> when the turn
    /// spends its model-iteration budget. Never contains secrets or tool
    /// arguments.
    [<Literal>]
    let MaxIterationsExceededMessage = "The turn reached its maximum iterations."

    /// Reason carried by <see cref="T:Legate.TurnFailed" /> when the turn
    /// spends its wall-clock budget. Never contains secrets or tool
    /// arguments.
    [<Literal>]
    let TimeoutExceededMessage = "The turn exceeded its timeout."

    /// Internal loop tuning: the tool-result char limit plus the effective
    /// per-turn budget, with the optional last-moment claim fence. The
    /// budget fields always carry resolved values (see
    /// <c>resolveBudget</c>); inject and metadata belong to follow-up
    /// issues. VerifyClaim carries issue 33's per-tool fence
    /// (ClaimFence.checkBeforeCallAsync): each tool invocation verifies the
    /// claim at the last moment and loses the lease on a fenced-out claim.
    /// None means no fence; the session path resolves through
    /// <c>resolveBudget</c> instead, which leaves the fence unset.
    type TurnLoopOptions =
        {
            /// Maximum tool-result chars before truncation with <see cref="TruncationMarker" />.
            MaxToolResultChars: int
            /// Maximum model iterations the turn may spend. At least 1.
            MaxIterations: int
            /// Maximum wall-clock time the turn may spend. Positive.
            Timeout: TimeSpan
            /// The last-moment per-tool claim fence, or None for no fence.
            VerifyClaim: (unit -> Task<bool>) option
        }

        /// Default tuning: 4000 chars before truncation with the iteration
        /// and wall-clock budgets mirroring the <c>Turns</c> configuration
        /// defaults and no claim fence. The session path resolves through
        /// <c>resolveBudget</c> instead.
        static member Default =
            {
                MaxToolResultChars = DefaultMaxToolResultChars
                MaxIterations = TurnsOptions().DefaultMaxIterations
                Timeout = TurnsOptions().DefaultTimeout
                VerifyClaim = None
            }

    /// Resolves the effective per-turn budget: the session's explicit knobs
    /// win, unset knobs (0 iterations, empty timeout) fall back to the
    /// configured <c>Turns</c> defaults. Raises
    /// <see cref="T:System.ArgumentOutOfRangeException" /> on any explicit
    /// or resolved non-positive value.
    let resolveBudget (sessionOptions: SessionOptions) (turns: TurnsOptions) : TurnLoopOptions =
        ArgumentNullException.ThrowIfNull(sessionOptions)
        ArgumentNullException.ThrowIfNull(turns)

        let maxIterations =
            if sessionOptions.MaxIterations = 0 then
                turns.DefaultMaxIterations
            elif sessionOptions.MaxIterations < 0 then
                raise (
                    ArgumentOutOfRangeException(
                        nameof sessionOptions,
                        "SessionOptions.MaxIterations must be 0 (the configured default) or positive."
                    )
                )
            else
                sessionOptions.MaxIterations

        if maxIterations < 1 then
            raise (ArgumentOutOfRangeException(nameof turns, "The resolved MaxIterations must be at least 1."))

        let timeout =
            if not sessionOptions.Timeout.HasValue then
                turns.DefaultTimeout
            elif sessionOptions.Timeout.Value <= TimeSpan.Zero then
                raise (
                    ArgumentOutOfRangeException(
                        nameof sessionOptions,
                        "SessionOptions.Timeout must be empty (the configured default) or positive."
                    )
                )
            else
                sessionOptions.Timeout.Value

        if timeout <= TimeSpan.Zero then
            raise (ArgumentOutOfRangeException(nameof turns, "The resolved Timeout must be positive."))

        { TurnLoopOptions.Default with
            MaxIterations = maxIterations
            Timeout = timeout
        }

    /// Raised when the lease-check hook reports the turn lease is lost.
    /// Propagates instead of completing so the actor can fence the loser
    /// with zero further effects.
    type TurnLeaseLostException(message: string) =
        inherit Exception(message)

        /// Creates the exception with the default lease-lost message.
        new() = TurnLeaseLostException("The turn lease was lost.")

    /// Pull-model drain hook for pending Inject inbox entries: returns the
    /// session's currently pending entries in position order. The loop owns
    /// the fold plus the pending signal; the session actor (#34) owns store
    /// routing, journal writes, and the new-turn start and provides the
    /// store-backed implementation. Must be non-destructive: entries leave
    /// the pending set only through the consume hook. A null return is
    /// treated as empty.
    type DrainInjected = unit -> IReadOnlyList<InboxEntry>

    /// Journal hook for one folded Inject entry: records the entry's user
    /// message once under the running turn. Owned by the session actor
    /// (#34); the loop calls it exactly once per folded entry, after the
    /// message is appended to history and before the consume hook.
    type JournalInjected = InboxEntry -> unit

    /// Consume hook for one folded Inject entry: marks it consumed so a
    /// later drain never returns it again. Owned by the session actor
    /// (#34); the loop calls it exactly once per folded entry, after the
    /// journal hook.
    type ConsumeInjected = InboxEntry -> unit

    /// Completion of a loop run with the inject fold applied: the settled
    /// turn result plus the new-turn signal for the session actor (#34).
    /// HasPendingInjects is true only when a would-complete turn peeked
    /// pending Inject user messages, left them pending, and folded nothing
    /// on that path; the actor starts the new turn.
    type TurnLoopCompletion =
        {
            /// The settled turn result.
            Result: TurnResult
            /// True when Inject entries stayed pending past a would-complete
            /// turn and the actor must start a new turn to act on them.
            HasPendingInjects: bool
        }

    /// No-op drain: no pending Inject entries.
    let private noInjects () : IReadOnlyList<InboxEntry> =
        ResizeArray<InboxEntry>() :> IReadOnlyList<InboxEntry>

    /// No-op journal/consume hook: records and marks nothing.
    let private ignoreInject (_: InboxEntry) : unit = ()

    /// Selects the foldable entries from a drain result in position order:
    /// Delivery Inject carrying a UserMessagePayload. Queue, Interrupt, and
    /// Reply payloads are ignored and stay pending for the session actor.
    /// Null entries and a null drain result are treated as empty.
    let private selectInjects (entries: IReadOnlyList<InboxEntry>) : InboxEntry list =
        if isNull (box entries) then
            []
        else
            entries
            |> Seq.filter (fun entry ->
                not (isNull (box entry))
                && entry.Delivery = DeliveryMode.Inject
                && (entry.Payload :? UserMessagePayload))
            |> Seq.sortBy (fun entry -> entry.Position)
            |> List.ofSeq

    /// Converts one Inject user message to a ChatRole.User history message,
    /// appending the raw parts verbatim in order. Metadata is ignored here:
    /// the metadata wrap owns it. Null messages, parts, and part entries
    /// are treated as empty rather than failing.
    let private injectToMessage (entry: InboxEntry) : ChatMessage =
        let payload = entry.Payload :?> UserMessagePayload

        let parts =
            if
                isNull (box payload)
                || isNull (box payload.Message)
                || isNull (box payload.Message.Parts)
            then
                ResizeArray<AIContent>() :> IList<AIContent>
            else
                payload.Message.Parts
                |> Seq.filter (fun part -> not (isNull (box part)))
                |> List.ofSeq
                |> ResizeArray<AIContent>
                :> IList<AIContent>

        ChatMessage(ChatRole.User, parts)

    /// Truncates a tool result to the configured limit, appending
    /// TruncationMarker when cut. Exactly-at-limit passes through.
    let truncateToolResult (options: TurnLoopOptions) (text: string) : string =
        let value = if isNull text then "" else text

        if value.Length > options.MaxToolResultChars then
            value.Substring(0, options.MaxToolResultChars) + TruncationMarker
        else
            value

    /// Converts a tool return value to text. Null becomes empty, strings
    /// pass through, anything else uses ToString. Never includes arguments.
    let private toolValueToString (value: obj) : string =
        if isNull value then
            ""
        else
            match value with
            | :? string as text -> if isNull text then "" else text
            | other ->
                let text = other.ToString()
                if isNull text then "" else text

    /// Maps a tool exception to an Error: continuation. Message only, never
    /// secrets or arguments; an empty message falls back to the type name.
    let private toolExceptionToString (ex: Exception) : string =
        let message =
            if isNull ex || String.IsNullOrEmpty ex.Message then
                ex.GetType().Name
            else
                ex.Message

        "Error: " + message

    /// Collects function calls in order across every response message.
    let private collectCalls (messages: IList<ChatMessage>) : FunctionCallContent list =
        [
            for message in messages do
                if not (isNull message) && not (isNull message.Contents) then
                    for content in message.Contents do
                        if not (isNull content) then
                            match content with
                            | :? FunctionCallContent as call when not (isNull call) -> yield call
                            | _ -> ()
        ]

    /// Adds usage counts, treating missing usage and missing counters as zero.
    let private addUsage (inputTokens: int64 byref) (outputTokens: int64 byref) (usage: UsageDetails) =
        if not (isNull usage) then
            if usage.InputTokenCount.HasValue then
                inputTokens <- inputTokens + usage.InputTokenCount.Value

            if usage.OutputTokenCount.HasValue then
                outputTokens <- outputTokens + usage.OutputTokenCount.Value

    /// Invokes one resolved tool and returns its result text. Unknown names
    /// and non-invokable tools map to UnknownToolMessage; tool exceptions
    /// map to Error: texts. Cancellation propagates.
    let private invokeOneAsync
        (tools: IReadOnlyDictionary<string, AITool>)
        (call: FunctionCallContent)
        (cancellationToken: CancellationToken)
        : Task<string> =
        task {
            let mutable tool = Unchecked.defaultof<AITool>
            let found = tools.TryGetValue(call.Name, &tool)

            if not found || isNull tool then
                return UnknownToolMessage
            else
                match tool with
                | :? AIFunction as fn ->
                    try
                        let args =
                            if isNull call.Arguments then
                                AIFunctionArguments()
                            else
                                AIFunctionArguments(call.Arguments)

                        let! result = fn.InvokeAsync(args, cancellationToken)
                        return toolValueToString result
                    with
                    | :? OperationCanceledException as canceled -> return! Task.FromException<string>(canceled)
                    | ex -> return toolExceptionToString ex
                | _ -> return UnknownToolMessage
        }

    /// Builds the Failed TurnResult for an exhausted budget: the spent
    /// iteration count and accumulated usage with a TurnFailed outcome
    /// carrying the typed reason. AssistantText is empty: the turn produced
    /// no final text.
    let private failedResult
        (iterations: int)
        (inputTokens: int64)
        (outputTokens: int64)
        (reason: string)
        : TurnResult =
        {
            AssistantText = ""
            Status = TurnStatus.Failed
            Iterations = iterations
            Usage =
                {
                    InputTokens = inputTokens
                    OutputTokens = outputTokens
                }
            Outcome = TurnFailed(reason) :> TurnOutcome
        }

    /// Runs the ReAct loop to completion with delta callbacks and the
    /// Inject fold. Each provider call streams through LlmStreaming: one
    /// text delta per non-empty text chunk and one reasoning delta per
    /// non-empty reasoning chunk fan out to <c>onTextDelta</c> and
    /// <c>onReasoningDelta</c> as they arrive, the accumulated messages
    /// carry raw blocks verbatim, and non-streaming providers fall back to
    /// a single delta per kind. Reasoning never reaches AssistantText: it
    /// concatenates TextContent only. Appends response and tool-result
    /// messages to history in order and returns the final assistant text
    /// as a Completed TurnResult.
    /// Checks the lease hook before every provider call and every tool
    /// invocation; cancellation and lease loss propagate. When
    /// TurnLoopOptions carries the verifyClaim hook (issue 33's
    /// ClaimFence.checkBeforeCallAsync), each tool invocation additionally
    /// verifies the claim at the last moment and raises
    /// TurnLeaseLostException on a fenced-out claim, so the loser of a
    /// takeover never invokes the tool.
    /// At each iteration boundary, after the lease and budget checks and
    /// before the provider call, drains <c>drainInjected</c> and folds each
    /// Inject UserMessagePayload into a ChatRole.User message in position
    /// order: the message is appended to history, journaled once through
    /// <c>onInjectJournaled</c>, and marked consumed once through
    /// <c>onInjectConsumed</c>. Queue, Interrupt, and Reply entries are
    /// ignored and stay pending. Folding never spends the iteration
    /// budget. A would-complete turn peeks the drain instead of folding:
    /// pending Inject user messages stay pending and surface as
    /// HasPendingInjects for the session actor's new turn.
    /// The iteration budget is a pre-call check that settles the turn as
    /// Failed with a TurnFailed reason instead of calling the model again.
    /// The timeout is a seam-fired hard deadline covering provider and tool
    /// execution: the injected <c>ILlmDelay</c> wait for the Timeout budget
    /// cancels the deadline scope when it elapses (virtual time under a
    /// virtual clock), and the terminal-write boundary settles as Failed
    /// without further effects, while in-flight cancellation caused only by
    /// the deadline maps to the timeout reason and external cancellation
    /// still propagates. The
    /// lease hook stays first so a fenced loser still produces zero effects.
    let runAsyncWithDeltasAndInjects
        (client: IChatClient)
        (history: IList<ChatMessage>)
        (tools: IReadOnlyDictionary<string, AITool>)
        (options: TurnLoopOptions)
        (delay: ILlmDelay)
        (cancellationToken: CancellationToken)
        (isLeaseValid: unit -> bool)
        (onTextDelta: string -> unit)
        (onReasoningDelta: string -> unit)
        (drainInjected: DrainInjected)
        (onInjectJournaled: JournalInjected)
        (onInjectConsumed: ConsumeInjected)
        : Task<TurnLoopCompletion> =
        ArgumentNullException.ThrowIfNull(client)
        ArgumentNullException.ThrowIfNull(history)
        ArgumentNullException.ThrowIfNull(tools)
        ArgumentNullException.ThrowIfNull(delay)
        ArgumentNullException.ThrowIfNull(isLeaseValid)
        ArgumentNullException.ThrowIfNull(onTextDelta)
        ArgumentNullException.ThrowIfNull(onReasoningDelta)
        ArgumentNullException.ThrowIfNull(drainInjected)
        ArgumentNullException.ThrowIfNull(onInjectJournaled)
        ArgumentNullException.ThrowIfNull(onInjectConsumed)

        if options.MaxToolResultChars <= 0 then
            raise (ArgumentOutOfRangeException(nameof options, "MaxToolResultChars must be positive."))

        if options.MaxIterations < 1 then
            raise (ArgumentOutOfRangeException(nameof options, "MaxIterations must be at least 1."))

        if options.Timeout <= TimeSpan.Zero then
            raise (ArgumentOutOfRangeException(nameof options, "Timeout must be positive."))

        let timeoutCts = new CancellationTokenSource()

        // Seam-fired hard deadline: the budget elapses on the injected
        // delay seam, never on the real clock. Under a virtual clock the
        // wait completes when the test advances past the Timeout; under the
        // system clock it completes after the Timeout elapses for real. Only
        // a completed wait cancels the scope: external cancellation faults
        // the wait instead, so the isTimeout check below never conflates an
        // abort with a timeout. A faulted wait is out of contract and fires
        // nothing; the loop keeps its external-cancellation behaviour.
        delay
            .Delay(options.Timeout, cancellationToken)
            .ContinueWith(
                Action<Task>(fun elapsed ->
                    if elapsed.Status = TaskStatus.RanToCompletion then
                        try
                            timeoutCts.Cancel()
                        with :? ObjectDisposedException ->
                            ()),
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default
            )
        |> ignore

        let linkedCts =
            CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token)

        let linkedToken = linkedCts.Token

        // True only when the linked token died to our own deadline: the
        // caller's token is untouched, so this never conflates an
        // external abort with a timeout.
        let isTimeout () =
            timeoutCts.IsCancellationRequested
            && not cancellationToken.IsCancellationRequested

        let chatOptions = ChatOptions()
        chatOptions.Tools <- ResizeArray<AITool>(tools.Values) :> IList<AITool>

        // Folds the pending Inject user messages at the iteration
        // boundary in position order: append, journal once, consume once.
        let foldInjects () =
            let pending = selectInjects (drainInjected ())

            for entry in pending do
                history.Add(injectToMessage entry)
                onInjectJournaled entry
                onInjectConsumed entry

        // Peeks the drain for the would-complete path: true when Inject
        // user messages arrived during the final iteration and stay
        // pending for the session actor's new turn.
        let hasPendingInjects () =
            selectInjects (drainInjected ()) |> List.isEmpty |> not

        let completedCompletion iterations inputTokens outputTokens assistantText : TurnLoopCompletion =
            {
                Result =
                    {
                        AssistantText = assistantText
                        Status = TurnStatus.Completed
                        Iterations = iterations
                        Usage =
                            {
                                InputTokens = inputTokens
                                OutputTokens = outputTokens
                            }
                        Outcome = null
                    }
                HasPendingInjects = hasPendingInjects ()
            }

        let failedCompletion iterations inputTokens outputTokens reason : TurnLoopCompletion =
            {
                Result = failedResult iterations inputTokens outputTokens reason
                HasPendingInjects = false
            }

        // Runs one round of tool calls in order. Returns None when every
        // call ran, or the timeout settlement when the deadline fired
        // mid-round so no further tool runs.
        let rec runTools roundIterations roundInput roundOutput pending : Task<TurnLoopCompletion option> =
            task {
                match pending with
                | [] -> return None
                | call :: rest ->
                    cancellationToken.ThrowIfCancellationRequested()

                    if not (isLeaseValid ()) then
                        raise (TurnLeaseLostException())

                    if timeoutCts.IsCancellationRequested then
                        return Some(failedCompletion roundIterations roundInput roundOutput TimeoutExceededMessage)
                    else
                        // Last-moment fence (issue 33): the sync hook above
                        // is the heartbeat's cached view; the options hook
                        // verifies the claim token immediately before the
                        // tool runs, so a takeover between the check and
                        // the call still fences the loser out.
                        match options.VerifyClaim with
                        | Some verify ->
                            let! live = verify ()

                            if not live then
                                raise (TurnLeaseLostException())
                        | None -> ()

                        let! rawText = invokeOneAsync tools call linkedToken
                        let text = truncateToolResult options rawText
                        let resultContent = FunctionResultContent(call.CallId, text)

                        let toolMessage =
                            ChatMessage(
                                ChatRole.Tool,
                                ResizeArray<AIContent>([| resultContent :> AIContent |]) :> IList<AIContent>
                            )

                        history.Add(toolMessage)
                        return! runTools roundIterations roundInput roundOutput rest
            }

        let rec loop iterations inputTokens outputTokens : Task<TurnLoopCompletion> =
            task {
                cancellationToken.ThrowIfCancellationRequested()

                if not (isLeaseValid ()) then
                    return! Task.FromException<TurnLoopCompletion>(TurnLeaseLostException())
                elif iterations >= options.MaxIterations then
                    return failedCompletion iterations inputTokens outputTokens MaxIterationsExceededMessage
                elif timeoutCts.IsCancellationRequested then
                    // The deadline fired before the next provider call:
                    // external cancellation already propagated above, so
                    // this is ours. Settle without calling the model again.
                    return failedCompletion iterations inputTokens outputTokens TimeoutExceededMessage
                else
                    foldInjects ()

                    try
                        let! response =
                            LlmStreaming.streamResponseAsync
                                client
                                history
                                chatOptions
                                linkedToken
                                onTextDelta
                                onReasoningDelta

                        let nextIterations = iterations + 1
                        let mutable nextInput = inputTokens
                        let mutable nextOutput = outputTokens

                        if isNull response then
                            return completedCompletion nextIterations nextInput nextOutput ""
                        else
                            addUsage &nextInput &nextOutput response.Usage

                            if not (isNull response.Messages) then
                                for message in response.Messages do
                                    if not (isNull message) then
                                        history.Add(message)

                            let calls =
                                if isNull response.Messages then
                                    []
                                else
                                    collectCalls response.Messages

                            if calls.IsEmpty then
                                let assistantText = if isNull response.Text then "" else response.Text

                                return completedCompletion nextIterations nextInput nextOutput assistantText
                            else
                                let! toolOutcome = runTools nextIterations nextInput nextOutput calls

                                match toolOutcome with
                                | Some timedOut -> return timedOut
                                | None -> return! loop nextIterations nextInput nextOutput
                    with :? OperationCanceledException when isTimeout () ->
                        // In-flight provider or tool work died to the
                        // deadline alone: the hard-deadline stop cause.
                        return failedCompletion iterations inputTokens outputTokens TimeoutExceededMessage
            }

        task {
            try
                return! loop 0 0L 0L
            finally
                timeoutCts.Dispose()
                linkedCts.Dispose()
        }

    /// Runs the ReAct loop to completion with the Inject fold and delta
    /// callbacks dropped: provider calls still stream (or fall back for
    /// non-streaming providers) with no deltas emitted, and pending Inject
    /// entries still fold at each iteration boundary. Returns the settled
    /// result plus the new-turn pending signal.
    let runAsyncWithInjects
        (client: IChatClient)
        (history: IList<ChatMessage>)
        (tools: IReadOnlyDictionary<string, AITool>)
        (options: TurnLoopOptions)
        (delay: ILlmDelay)
        (cancellationToken: CancellationToken)
        (isLeaseValid: unit -> bool)
        (drainInjected: DrainInjected)
        (onInjectJournaled: JournalInjected)
        (onInjectConsumed: ConsumeInjected)
        : Task<TurnLoopCompletion> =
        runAsyncWithDeltasAndInjects
            client
            history
            tools
            options
            delay
            cancellationToken
            isLeaseValid
            ignore
            ignore
            drainInjected
            onInjectJournaled
            onInjectConsumed

    /// Runs the ReAct loop to completion with delta callbacks. Same as
    /// <c>runAsyncWithDeltasAndInjects</c> with a no-op Inject fold:
    /// the drain stays empty and the journal/consume hooks record nothing,
    /// so the pending signal is always false.
    let runAsyncWithDeltas
        (client: IChatClient)
        (history: IList<ChatMessage>)
        (tools: IReadOnlyDictionary<string, AITool>)
        (options: TurnLoopOptions)
        (delay: ILlmDelay)
        (cancellationToken: CancellationToken)
        (isLeaseValid: unit -> bool)
        (onTextDelta: string -> unit)
        (onReasoningDelta: string -> unit)
        : Task<TurnResult> =
        runAsyncWithDeltasAndInjects
            client
            history
            tools
            options
            delay
            cancellationToken
            isLeaseValid
            onTextDelta
            onReasoningDelta
            noInjects
            ignoreInject
            ignoreInject
        |> fun inner ->
            task {
                let! completion = inner
                return completion.Result
            }

    /// Runs the ReAct loop to completion. Same as
    /// <c>runAsyncWithDeltas</c> with the delta callbacks dropped: provider
    /// calls still stream (or fall back for non-streaming providers), but no
    /// deltas are emitted.
    let runAsync
        (client: IChatClient)
        (history: IList<ChatMessage>)
        (tools: IReadOnlyDictionary<string, AITool>)
        (options: TurnLoopOptions)
        (delay: ILlmDelay)
        (cancellationToken: CancellationToken)
        (isLeaseValid: unit -> bool)
        : Task<TurnResult> =
        runAsyncWithDeltas client history tools options delay cancellationToken isLeaseValid ignore ignore
