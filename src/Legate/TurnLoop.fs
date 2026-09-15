// SPDX-License-Identifier: Apache-2.0
#nowarn "3261"

namespace Legate

open System
open System.Collections.Generic
open System.Text.Json
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

    /// Per-iteration-boundary compaction hook (issue 45): receives the
    /// running history plus the turn's accumulated usage totals and the
    /// iteration's linked token, runs at most one compaction pass, and
    /// returns the totals with any summariser usage folded in. Built by
    /// Compaction.createHook; None on TurnLoopOptions disables compaction.
    type CompactionHook = IList<ChatMessage> -> int64 -> int64 -> CancellationToken -> Task<int64 * int64>

    /// What one settled tool invocation looked like: the name the model
    /// called it by, the call id the result answers, the appended result
    /// text, and the failure when the invocation raised instead of
    /// returning. Error is Some only when the invocation raised (an
    /// unknown or non-invokable tool counts as raised); denials carry
    /// their denial text with no error, and suspensions never observe:
    /// a suspended call has not settled. The nested task-tool runner
    /// journals these observations as the sub-agent execution markers the
    /// transcript read links back to the parent call.
    type ToolCallObservation =
        {
            /// The name the model called the tool by.
            ToolName: string
            /// The tool-call id the result answers.
            ToolCallId: string
            /// The appended result text, already bounded by truncation.
            Text: string
            /// Why the invocation raised, or None when it returned.
            Error: string option
        }

    /// Internal loop tuning: the tool-result char limit plus the effective
    /// per-turn budget, with the optional last-moment claim fence. The
    /// budget fields always carry resolved values (see
    /// <c>resolveBudget</c>); inject and metadata belong to follow-up
    /// issues. VerifyClaim carries issue 33's per-tool fence
    /// (ClaimFence.checkBeforeCallAsync): each tool invocation verifies the
    /// claim at the last moment and loses the lease on a fenced-out claim.
    /// None means no fence; the session path resolves through
    /// <c>resolveBudget</c> instead, which leaves the fence unset.
    /// Compaction carries issue 45's per-iteration-boundary hook: Some runs
    /// one compaction pass after the lease and budget checks and before the
    /// provider call, None compacts nothing.
    /// AskUser carries the ask_user headless policy (issue 64): None
    /// suspends for a host answer (interactive), Some Fail fails the turn
    /// fast, Some AnswerWith continues with the canned answer.
    /// OnToolCall carries the settled-invocation observer (issue 72): Some
    /// observes every settled tool invocation after its result appends
    /// (denials included, suspensions excluded), None observes nothing.
    /// The nested task-tool runner sets it to journal the sub-agent
    /// execution markers; parent turns leave it unset.
    /// TaskNested carries the task-tool nested runner (issue 72): Some
    /// runs the requested sub-agent through the nested loop, None reads a
    /// task call as an unknown tool.
    /// StructuredOutcome carries the structured-outcome mode (issue 82):
    /// true offers the finish/fail descriptors to the model and settles the
    /// turn through them, false runs the host tool map untouched with a null
    /// completion outcome.
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
            /// The per-iteration-boundary compaction hook, or None for no
            /// compaction.
            Compaction: CompactionHook option
            /// The ask_user headless policy, or None to suspend for a host
            /// answer.
            AskUser: AskUserOptions option
            /// The settled-invocation observer, or None to observe nothing.
            OnToolCall: (ToolCallObservation -> Task<unit>) option
            /// The task-tool nested runner, or None when the turn offers no
            /// task tool.
            TaskNested: TaskNestedRun option
            /// True when the turn runs with the structured outcome mode: the
            /// loop offers the finish/fail descriptors and populates
            /// TurnResult.Outcome. False runs the host tool map untouched.
            StructuredOutcome: bool
        }

        /// Default tuning: 4000 chars before truncation with the iteration
        /// and wall-clock budgets mirroring the <c>Turns</c> configuration
        /// defaults, no claim fence, and no compaction. The session path resolves through
        /// <c>resolveBudget</c> instead.
        static member Default =
            {
                MaxToolResultChars = DefaultMaxToolResultChars
                MaxIterations = TurnsOptions().DefaultMaxIterations
                Timeout = TurnsOptions().DefaultTimeout
                VerifyClaim = None
                Compaction = None
                AskUser = None
                OnToolCall = None
                TaskNested = None
                StructuredOutcome = false
            }

    /// One task-tool nested run: the parent call plus everything the
    /// nested loop reuses from the parent scope. The hook implementation
    /// (the task tool) filters the parent tool map into the nested pool,
    /// resolves the agent definition and model override, and runs the
    /// nested loop under the nested deadline with the parent claim fence
    /// still applied, so a takeover loser performs zero nested effects.
    and TaskNestedRequest =
        {
            /// The parent task call spawning the nested run.
            Call: FunctionCallContent
            /// The parent tool map the nested pool filters from.
            Tools: IReadOnlyDictionary<string, AITool>
            /// The parent tuning and budget: the nested run inherits the
            /// char limit, iteration budget, claim fence, compaction hook,
            /// headless policy, and observation hook, with only the
            /// timeout narrowed to the nested deadline.
            Options: TurnLoopOptions
            /// The chat client the parent turn runs against: the nested
            /// run reuses it unless the call carries a model override.
            Client: IChatClient
            /// The delay seam the nested deadline fires off.
            Delay: ILlmDelay
            /// The parent deadline scope: the nested run abandons when the
            /// parent budget or the nested deadline fires.
            CancellationToken: CancellationToken
            /// The lease hook the nested loop checks.
            IsLeaseValid: unit -> bool
            /// The permission policy the nested calls evaluate against, or
            /// null for no gate.
            Policy: IPermissionPolicy
            /// The session the nested run belongs to.
            SessionId: SessionId
            /// The parent turn spawning the nested run.
            ParentTurnId: TurnId
            /// Mints stable request ids, or None for GUIDs.
            NewRequestId: (unit -> string) option
            /// Tool names the host already allowed for the session, shared
            /// with the parent so AllowForSession memory stays session-wide.
            AllowedForSession: HashSet<string>
        }

    /// What one task-tool nested run settled with: the shaped tool result
    /// text with the nested totals the parent folds into its budget. A
    /// nested suspension never returns: the hook raises
    /// <see cref="T:Legate.TurnLoop.TaskNestedSuspended" /> carrying the
    /// nested cursor with the resume that continues it, and the parent
    /// task branch parks the parent turn on it.
    and TaskNestedResult =
        {
            /// The shaped result text: success wrapped for the model,
            /// failures as <c>Sub-agent failed: ...</c>.
            Text: string
            /// Model iterations the nested run spent.
            Iterations: int
            /// Input tokens the nested run spent.
            InputTokens: int64
            /// Output tokens the nested run spent.
            OutputTokens: int64
        }

    /// Continues a suspended nested run with the host's reply: resumes the
    /// nested loop to its next suspension or its settled result. A nested
    /// re-suspension raises
    /// <see cref="T:Legate.TurnLoop.TaskNestedSuspended" /> again, so the
    /// loop re-parks the parent carrying the fresh cursor; every reply
    /// re-enters the nested loop before the parent continues. Runners never
    /// call it directly: they invoke the suspension's ResumeAsync with the
    /// reply instead.
    and TaskNestedResume = Reply -> CancellationToken -> Task<TaskNestedResult>

    /// Runs one task-tool nested run over the parent scope. Returns the
    /// settled result; a nested suspension raises
    /// <see cref="T:Legate.TurnLoop.TaskNestedSuspended" /> instead of
    /// returning. Lease loss and cancellation propagate as their own
    /// exceptions instead of shaping, so the takeover loser never reports
    /// success.
    and TaskNestedRun = TaskNestedRequest -> Task<TaskNestedResult>

    /// Resolves the effective per-turn budget: the session's explicit knobs
    /// win, unset knobs (0 iterations, empty timeout) fall back to the
    /// configured <c>Turns</c> defaults. The structured-outcome flag follows
    /// the session's outcome mode, so whoever resolves the budget from the
    /// session options carries the finish/fail interception with it. Raises
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
            StructuredOutcome = (sessionOptions.Outcome = SessionOutcomeMode.Structured)
        }

    // ────────────────── Structured outcomes (issue 82) ──────────────────

    /// The structured-outcome tool name that ends the turn with success:
    /// reserved when TurnLoopOptions carries StructuredOutcome. A host tool
    /// with this name collides: the loop raises ArgumentException naming the
    /// reservation instead of shadowing either tool.
    [<Literal>]
    let FinishToolName = "finish"

    /// The structured-outcome tool name that ends the turn with failure:
    /// reserved when TurnLoopOptions carries StructuredOutcome, shadowing
    /// neither the host tool nor the settlement: a collision raises like
    /// FinishToolName.
    [<Literal>]
    let FailToolName = "fail"

    /// Tool result text appended when a finish call carries no usable
    /// summary: the model sees the error and continues without settling, so
    /// a malformed call never ends the turn.
    [<Literal>]
    let FinishMissingSummaryMessage =
        "Error: the finish tool needs a summary: pass what the turn accomplished in 'summary'."

    /// Tool result text appended when a fail call carries no usable error:
    /// the model sees the error and continues without settling, so a
    /// malformed call never ends the turn.
    [<Literal>]
    let FailMissingErrorMessage =
        "Error: the fail tool needs an error: pass why the turn failed in 'error'."

    /// The JSON schema served on the finish tool: summary is a required
    /// string, output_files an optional array of strings. Output files are
    /// accepted for forward compatibility with the completion sink and are
    /// not surfaced on the outcome.
    let private finishSchemaJson =
        """{"type":"object","description":"Arguments for the finish tool.","properties":{"summary":{"type":"string","description":"What the turn accomplished. Required."},"output_files":{"type":"array","items":{"type":"string"},"description":"Output files the turn produced. Optional; accepted for forward compatibility and not surfaced on the outcome."}},"required":["summary"],"additionalProperties":false}"""

    /// The JSON schema served on the fail tool: error is a required string.
    let private failSchemaJson =
        """{"type":"object","description":"Arguments for the fail tool.","properties":{"error":{"type":"string","description":"Why the turn failed. Required."}},"required":["error"],"additionalProperties":false}"""

    /// The parsed schema documents backing the structured-outcome tools:
    /// the JsonSchema elements borrow them, so they live as long as the
    /// process.
    module private StructuredSchemas =

        /// The parsed finish schema backing every finish tool instance.
        let finishDocument = JsonDocument.Parse finishSchemaJson

        /// The parsed fail schema backing every fail tool instance.
        let failDocument = JsonDocument.Parse failSchemaJson

    /// The finish function served to the model in structured turns.
    /// Internal: the loop intercepts finish calls into immediate settlement
    /// before any invocation, so a direct call is out of contract and raises.
    [<Sealed>]
    type internal FinishFunction() =
        inherit AIFunction()

        /// This tool's name for error text.
        override _.Name = FinishToolName

        /// This tool's description for the model.
        override _.Description =
            "Ends the turn with success (finish). Pass what the turn accomplished in 'summary' (required) and optional 'output_files'. The turn settles immediately with no further model calls."

        /// This tool's argument schema.
        override _.JsonSchema = StructuredSchemas.finishDocument.RootElement

        /// Raises: execution belongs to the turn loop's structured-outcome
        /// interception (settle on the call, never invoke), so a direct call
        /// is out of contract.
        /// Cancellation propagates as-is.
        override _.InvokeCoreAsync
            (args: AIFunctionArguments, cancellationToken: CancellationToken)
            : ValueTask<obj | null> =
            ValueTask<obj | null>(
                task {
                    cancellationToken.ThrowIfCancellationRequested()

                    if not (isNull (box args)) then
                        ()

                    return
                        raise (
                            InvalidOperationException(
                                "The finish tool runs through the turn loop's structured-outcome interception: invoke it through a turn, never directly."
                            )
                        )
                }
            )

    /// The fail function served to the model in structured turns. Internal:
    /// the loop intercepts fail calls into immediate settlement before any
    /// invocation, so a direct call is out of contract and raises, like the
    /// finish function.
    [<Sealed>]
    type internal FailFunction() =
        inherit AIFunction()

        /// This tool's name for error text.
        override _.Name = FailToolName

        /// This tool's description for the model.
        override _.Description =
            "Ends the turn with failure (fail). Pass why the turn failed in 'error' (required). The turn settles immediately with no further model calls."

        /// This tool's argument schema.
        override _.JsonSchema = StructuredSchemas.failDocument.RootElement

        /// Raises: execution belongs to the turn loop's structured-outcome
        /// interception (settle on the call, never invoke), so a direct call
        /// is out of contract.
        /// Cancellation propagates as-is.
        override _.InvokeCoreAsync
            (args: AIFunctionArguments, cancellationToken: CancellationToken)
            : ValueTask<obj | null> =
            ValueTask<obj | null>(
                task {
                    cancellationToken.ThrowIfCancellationRequested()

                    if not (isNull (box args)) then
                        ()

                    return
                        raise (
                            InvalidOperationException(
                                "The fail tool runs through the turn loop's structured-outcome interception: invoke it through a turn, never directly."
                            )
                        )
                }
            )

    /// Builds the tool map the loop offers when the turn runs structured:
    /// the host tools plus the finish/fail descriptors. Non-structured turns
    /// run the host map untouched, so finish/fail never reach the model.
    /// Raises ArgumentException when a host tool already claims a reserved
    /// name: the loop fails fast naming the reservation instead of shadowing
    /// either tool.
    /// <param name="options">The turn loop tuning carrying the structured flag.</param>
    /// <param name="tools">The host tool map. Must not be null.</param>
    /// <returns>The map the loop offers and invokes through.</returns>
    let internal effectiveTools
        (options: TurnLoopOptions)
        (tools: IReadOnlyDictionary<string, AITool>)
        : IReadOnlyDictionary<string, AITool> =
        ArgumentNullException.ThrowIfNull(tools)

        if not options.StructuredOutcome then
            tools
        else
            for reserved in [| FinishToolName; FailToolName |] do
                if tools.ContainsKey(reserved) then
                    raise (
                        ArgumentException(
                            sprintf
                                "The tool name '%s' is reserved for the structured-outcome tool: rename the host tool."
                                reserved,
                            nameof tools
                        )
                    )

            ToolNameRules.Validate(FinishToolName) |> ignore
            ToolNameRules.Validate(FailToolName) |> ignore

            let merged = Dictionary<string, AITool>(tools.Count + 2)

            for entry in tools do
                merged[entry.Key] <- entry.Value

            merged[FinishToolName] <- FinishFunction() :> AITool
            merged[FailToolName] <- FailFunction() :> AITool
            merged :> IReadOnlyDictionary<string, AITool>

    /// Reads one structured-outcome call argument as text: plain strings and
    /// JSON string elements; anything else is not text.
    /// <param name="args">The call's arguments.</param>
    /// <param name="name">The argument to read.</param>
    /// <returns>The text, or null when the argument is missing or not text.</returns>
    let private structuredArgText (args: IDictionary<string, obj>) (name: string) : string | null =
        if isNull (box args) || isNull (box name) then
            null
        else
            let mutable value: obj = null

            if args.TryGetValue(name, &value) && not (isNull (box value)) then
                match value with
                | :? string as text -> text
                | :? JsonElement as element when element.ValueKind = JsonValueKind.String -> element.GetString()
                | _ -> null
            else
                null

    /// Builds the Completed TurnResult for an explicit finish call: the
    /// spent iteration count and accumulated usage with a TurnFinished
    /// outcome carrying the model's summary as an explicit (non-implicit)
    /// outcome. AssistantText carries the summary too, so hosts that read
    /// only the text see what the turn accomplished.
    /// <param name="iterations">The model iterations the turn spent.</param>
    /// <param name="inputTokens">The input tokens the turn spent.</param>
    /// <param name="outputTokens">The output tokens the turn spent.</param>
    /// <param name="summary">The model's finish summary. Must not be null.</param>
    /// <returns>The settled explicit-finish result.</returns>
    let private finishedResult
        (iterations: int)
        (inputTokens: int64)
        (outputTokens: int64)
        (summary: string)
        : TurnResult =
        {
            AssistantText = summary
            Status = TurnStatus.Completed
            Iterations = iterations
            Usage =
                {
                    InputTokens = inputTokens
                    OutputTokens = outputTokens
                }
            Outcome = TurnFinished(summary) :> TurnOutcome
        }

    /// Builds the Completed TurnResult for a structured turn the model
    /// stopped without calling finish or fail: a TurnFinished outcome
    /// carrying the final assistant text with the implicit flag set.
    /// <param name="iterations">The model iterations the turn spent.</param>
    /// <param name="inputTokens">The input tokens the turn spent.</param>
    /// <param name="outputTokens">The output tokens the turn spent.</param>
    /// <param name="assistantText">The final assistant text.</param>
    /// <returns>The settled implicit-finish result.</returns>
    let private implicitFinishedResult
        (iterations: int)
        (inputTokens: int64)
        (outputTokens: int64)
        (assistantText: string)
        : TurnResult =
        let text = if isNull assistantText then "" else assistantText
        let outcome = TurnFinished(text)
        outcome.IsImplicit <- true

        {
            AssistantText = text
            Status = TurnStatus.Completed
            Iterations = iterations
            Usage =
                {
                    InputTokens = inputTokens
                    OutputTokens = outputTokens
                }
            Outcome = outcome :> TurnOutcome
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

    /// What a suspended turn waits on: a permission decision for one tool
    /// call, or an answer to one agent question. The session actor journals
    /// the matching PermissionRequestedEvent or QuestionAskedEvent and owns
    /// the store-first WaitingForInput transition; the loop only carries the
    /// cursor the resume continues from.
    type SuspensionKind =

        /// The turn waits on a PermissionDecision answering RequestId.
        | PermissionSuspension

        /// The turn waits on a QuestionAnswer answering RequestId.
        | QuestionSuspension

    /// The cursor a suspended turn resumes from: the pending request id plus
    /// the preserved loop position (history, usage, iterations, and the tool
    /// call that raised the request). HistorySnapshot is a copy taken at
    /// suspend time, so later mutation of the running history never moves
    /// the resume point. PendingCall carries the FunctionCallContent that
    /// raised the request, so the resume executes or skips that same call.
    /// Nested carries the sub-agent resume when a nested task-tool run
    /// suspended the parent: Some wraps the nested cursor with the resume
    /// that continues the nested loop before the parent continues, None
    /// resumes through the standard permission/question continuations.
    type TurnLoopSuspension =
        {
            /// The stable id the host answers: a PermissionDecision answers
            /// a permission suspension with it, a QuestionAnswer answers a
            /// question suspension with it as the question id.
            RequestId: string
            /// The tool whose call raised the request (ask_user for questions).
            ToolName: string
            /// The tool-call id that raised the request.
            ToolCallId: string
            /// Which reply resumes the turn.
            Kind: SuspensionKind
            /// The question the agent asked, or empty for permission suspensions.
            QuestionText: string
            /// The options hint the ask_user call carried, or empty for
            /// free-text questions and permission suspensions.
            QuestionOptions: string list
            /// The history up to the suspend point, copied at suspend time.
            HistorySnapshot: IList<ChatMessage>
            /// Input tokens spent up to the suspend point.
            InputTokens: int64
            /// Output tokens spent up to the suspend point.
            OutputTokens: int64
            /// Model iterations spent up to the suspend point.
            Iterations: int
            /// The tool call that raised the request.
            PendingCall: FunctionCallContent
            /// The sub-agent resume, or None for a directly suspended turn.
            Nested: NestedResume option
        }

    /// Resumes a suspended nested run with the host's reply and continues
    /// the parent turn: the nested cursor to resume plus the parent
    /// continuation that maps the nested outcome back onto the parent
    /// loop. Runners invoke ResumeAsync with the reply instead of the
    /// standard continuations when Nested is Some; a crash rebuild loses
    /// it and retries the parent turn from its inbox entry instead.
    and NestedResume =
        {
            /// The nested suspend cursor the resume continues from.
            Cursor: TurnLoopSuspension
            /// Continues the nested run with the reply, then continues the
            /// parent turn with the nested outcome applied.
            ResumeAsync: Reply -> CancellationToken -> Task<TurnLoopCompletion>
        }

    /// Completion of a loop run with the inject fold applied: the settled
    /// turn result plus the new-turn signal for the session actor (#34).
    /// HasPendingInjects is true only when a would-complete turn peeked
    /// pending Inject user messages, left them pending, and folded nothing
    /// on that path; the actor starts the new turn. Suspension carries the
    /// unified Suspended carrier (issue 36): Some when an Ask verdict or an
    /// ask_user question suspended the turn mid-run (Result carries
    /// TurnStatus.Suspended then), None when the turn settled normally.
    /// Reply-never-starts-a-turn: a suspension never starts work, it only
    /// parks the cursor the matching Reply resumes from.
    and TurnLoopCompletion =
        {
            /// The settled turn result.
            Result: TurnResult
            /// True when Inject entries stayed pending past a would-complete
            /// turn and the actor must start a new turn to act on them.
            HasPendingInjects: bool
            /// The suspend cursor, or None when the turn settled.
            Suspension: TurnLoopSuspension option
        }

    /// Raised by the task-tool nested runner when the nested loop suspends
    /// (a nested permission Ask or question): carries the nested cursor
    /// with the resume that continues it. The parent task branch catches it
    /// and parks the parent turn carrying the cursor for the matching
    /// reply, mirroring how TurnLeaseLostException carries control flow.
    /// Lease loss and cancellation propagate as their own exceptions
    /// instead, so a fenced-out loser never parks a turn it lost.
    exception TaskNestedSuspended of cursor: TurnLoopSuspension * resume: TaskNestedResume

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

    /// Invokes one resolved tool and returns its result text with the
    /// failure. Unknown names and non-invokable tools map to
    /// UnknownToolMessage with that message as the failure; tool
    /// exceptions map to Error: texts with the mapped text as the
    /// failure. Cancellation propagates.
    let private invokeOneWithErrorAsync
        (tools: IReadOnlyDictionary<string, AITool>)
        (call: FunctionCallContent)
        (cancellationToken: CancellationToken)
        : Task<string * string option> =
        task {
            let mutable tool = Unchecked.defaultof<AITool>
            let found = tools.TryGetValue(call.Name, &tool)

            if not found || isNull tool then
                return UnknownToolMessage, Some UnknownToolMessage
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
                        return toolValueToString result, None
                    with
                    | :? OperationCanceledException as canceled ->
                        return! Task.FromException<string * string option>(canceled)
                    | ex ->
                        let message = toolExceptionToString ex
                        return message, Some message
                | _ -> return UnknownToolMessage, Some UnknownToolMessage
        }

    /// Observes one settled tool invocation through the options hook: the
    /// text is the appended result text, the error marks a raised
    /// invocation. Suspensions never observe: a suspended call has not
    /// settled. An observing failure propagates, so a fenced-out loser
    /// never reports success past the fence.
    /// <param name="options">The turn loop tuning carrying the observer.</param>
    /// <param name="call">The settled call.</param>
    /// <param name="text">The appended result text.</param>
    /// <param name="error">Why the invocation raised, or None when it returned.</param>
    let private observeToolCallAsync
        (options: TurnLoopOptions)
        (call: FunctionCallContent)
        (text: string)
        (error: string option)
        : Task<unit> =
        match options.OnToolCall with
        | Some observe ->
            let name =
                if isNull (box call) || isNull call.Name then
                    ""
                else
                    call.Name

            let callId =
                if isNull (box call) || isNull call.CallId then
                    ""
                else
                    call.CallId

            observe
                {
                    ToolName = name
                    ToolCallId = callId
                    Text = if isNull text then "" else text
                    Error = error
                }
        | None -> Task.FromResult(())

    /// Invokes one resolved tool, observes the settlement, and returns its
    /// result text: the single path every tool round uses, so the nested
    /// task-tool observer sees every settled invocation in turn order.
    /// <param name="options">The turn loop tuning carrying the observer.</param>
    /// <param name="tools">The resolved tool map.</param>
    /// <param name="call">The call to invoke.</param>
    /// <param name="cancellationToken">Abandons the invocation.</param>
    let private invokeAndObserveAsync
        (options: TurnLoopOptions)
        (tools: IReadOnlyDictionary<string, AITool>)
        (call: FunctionCallContent)
        (cancellationToken: CancellationToken)
        : Task<string> =
        task {
            let! text, error = invokeOneWithErrorAsync tools call cancellationToken
            do! observeToolCallAsync options call text error
            return text
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
    /// budget. When <c>options</c> carries a Compaction hook (issue 45), the
    /// boundary then runs one compaction pass and continues with its
    /// updated usage totals. A would-complete turn peeks the drain instead of folding:
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
    /// When TurnLoopOptions carries StructuredOutcome (issue 82), the loop
    /// offers the finish/fail descriptors and a call to either settles the
    /// turn immediately under the same fence; a structured turn the model
    /// stops without calling either settles Completed with an implicit
    /// TurnFinished outcome.
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

        // Structured outcomes (issue 82): the offered map gains the
        // finish/fail descriptors, or the host map passes through untouched.
        // A host collision raises here, before any provider call.
        let tools = effectiveTools options tools

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
                    if options.StructuredOutcome then
                        // No finish call ended this turn (a finish call
                        // settles immediately), so the model stopped without
                        // calling either: synthesize the implicit Finished.
                        implicitFinishedResult iterations inputTokens outputTokens assistantText
                    else
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
                Suspension = None
            }

        let failedCompletion iterations inputTokens outputTokens reason : TurnLoopCompletion =
            {
                Result = failedResult iterations inputTokens outputTokens reason
                HasPendingInjects = false
                Suspension = None
            }

        // Runs one round of tool calls in order. Returns None when every
        // call ran, or the timeout settlement when the deadline fired
        // mid-round so no further tool runs.
        let rec runTools
            roundIterations
            roundInput
            roundOutput
            (pending: FunctionCallContent list)
            : Task<TurnLoopCompletion option> =
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

                        // Structured outcomes (issue 82): finish/fail calls
                        // settle the turn immediately under the fence above,
                        // so the takeover loser raises TurnLeaseLostException
                        // there instead of settling. Malformed calls append
                        // an Error: continuation and run on, so a bad call
                        // never ends the turn. Calls after a settling call in
                        // the same round never run.
                        if
                            options.StructuredOutcome
                            && String.Equals(call.Name, FinishToolName, StringComparison.Ordinal)
                        then
                            match Option.ofObj (structuredArgText call.Arguments "summary") with
                            | Some summary when not (String.IsNullOrWhiteSpace summary) ->
                                let resultContent = FunctionResultContent(call.CallId, summary)

                                let toolMessage =
                                    ChatMessage(
                                        ChatRole.Tool,
                                        ResizeArray<AIContent>([| resultContent :> AIContent |]) :> IList<AIContent>
                                    )

                                history.Add(toolMessage)
                                do! observeToolCallAsync options call summary None

                                return
                                    Some(
                                        {
                                            Result = finishedResult roundIterations roundInput roundOutput summary
                                            HasPendingInjects = hasPendingInjects ()
                                            Suspension = None
                                        }
                                    )
                            | _ ->
                                let text = FinishMissingSummaryMessage
                                let resultContent = FunctionResultContent(call.CallId, text)

                                let toolMessage =
                                    ChatMessage(
                                        ChatRole.Tool,
                                        ResizeArray<AIContent>([| resultContent :> AIContent |]) :> IList<AIContent>
                                    )

                                history.Add(toolMessage)
                                do! observeToolCallAsync options call text (Some text)
                                return! runTools roundIterations roundInput roundOutput rest
                        elif
                            options.StructuredOutcome
                            && String.Equals(call.Name, FailToolName, StringComparison.Ordinal)
                        then
                            match Option.ofObj (structuredArgText call.Arguments "error") with
                            | Some error when not (String.IsNullOrWhiteSpace error) ->
                                let resultContent = FunctionResultContent(call.CallId, error)

                                let toolMessage =
                                    ChatMessage(
                                        ChatRole.Tool,
                                        ResizeArray<AIContent>([| resultContent :> AIContent |]) :> IList<AIContent>
                                    )

                                history.Add(toolMessage)
                                do! observeToolCallAsync options call error None

                                return
                                    Some(
                                        {
                                            Result = failedResult roundIterations roundInput roundOutput error
                                            HasPendingInjects = false
                                            Suspension = None
                                        }
                                    )
                            | _ ->
                                let text = FailMissingErrorMessage
                                let resultContent = FunctionResultContent(call.CallId, text)

                                let toolMessage =
                                    ChatMessage(
                                        ChatRole.Tool,
                                        ResizeArray<AIContent>([| resultContent :> AIContent |]) :> IList<AIContent>
                                    )

                                history.Add(toolMessage)
                                do! observeToolCallAsync options call text (Some text)
                                return! runTools roundIterations roundInput roundOutput rest
                        else
                            let! rawText = invokeAndObserveAsync options tools call linkedToken
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

                    // Compaction boundary (issue 45): one pass per
                    // iteration, after the lease and budget checks and
                    // before the provider call. The hook folds any
                    // summariser usage into the totals the iteration
                    // continues with; without a hook the totals pass
                    // through untouched.
                    let! compactedInput, compactedOutput =
                        match options.Compaction with
                        | Some compact -> compact history inputTokens outputTokens linkedToken
                        | None -> Task.FromResult((inputTokens, outputTokens))

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
                        let mutable nextInput = compactedInput
                        let mutable nextOutput = compactedOutput

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

    // ────────────────── Suspend and resume (issue 36) ──────────────────

    /// The built-in tool name that asks the host a question instead of
    /// executing: a call to this tool suspends with a QuestionSuspension
    /// carrying the question text, never invoking a tool.
    [<Literal>]
    let AskUserToolName = "ask_user"

    /// The built-in tool name that loads a packaged skill's content: a call
    /// to this tool reads the skill's SKILL.md from the active package
    /// version and journals a SkillLoadedEvent. The loader bypasses the
    /// permission gate like ask_user: package metadata is already authorized
    /// by the session bind, so there is nothing for a policy to decide;
    /// companion files the skill names are read afterward through the normal
    /// file tools with the policy applied.
    [<Literal>]
    let SkillToolName = "skill"

    /// The built-in tool name that runs a sub-agent as a nested turn loop:
    /// a call to this tool runs the requested agent definition through
    /// runSuspendableAsync sharing the session workspace and journal, with
    /// its own filtered tool pool, model override, and depth limit. The
    /// loop intercepts the call into the TurnLoopOptions.TaskNested runner
    /// before any invocation, exactly like ask_user and skill; the
    /// AIFunction itself only carries the schema and never executes.
    [<Literal>]
    let TaskToolName = "task"

    /// Reason carried by <see cref="T:Legate.TurnFailed" /> when the turn
    /// calls ask_user under a Fail headless policy: the question has no
    /// host to answer it. Never contains the question or the options hint.
    [<Literal>]
    let AskUserHeadlessFailMessage =
        "The turn called ask_user, but this session cannot answer questions."

    /// Reason carried by <see cref="T:Legate.TurnFailed" /> when the turn
    /// calls ask_user under an AnswerWith headless policy whose canned
    /// answer is missing: configuration promised an answer it did not
    /// carry. Never contains the question or the options hint.
    [<Literal>]
    let AskUserCannedMissingMessage =
        "The turn called ask_user, but the session's canned answer is missing."

    /// Prefix for the tool result appended when a permission policy denies
    /// a call: the model sees the denial and continues without the effect.
    [<Literal>]
    let DenyResultPrefix = "Error: denied: "

    /// Copies the running history at the suspend point so later mutation
    /// never moves the resume point.
    /// <param name="history">The running history.</param>
    /// <returns>A copy of the history.</returns>
    let private snapshotHistory (history: IList<ChatMessage>) : IList<ChatMessage> =
        let copy = ResizeArray<ChatMessage>(history.Count)

        for message in history do
            copy.Add(message)

        copy :> IList<ChatMessage>

    /// Builds the permission request for one tool call. The argument preview
    /// is the empty string: bounded and redacted by construction (never
    /// secrets), until the redaction epic supplies the truncated preview.
    /// <param name="sessionId">The session the call belongs to.</param>
    /// <param name="turnId">The turn the call belongs to.</param>
    /// <param name="toolName">The tool the model called.</param>
    /// <param name="requestId">The stable request id the host answers with.</param>
    /// <returns>The request the policy evaluates.</returns>
    let private buildPermissionRequest
        (sessionId: SessionId)
        (turnId: TurnId)
        (toolName: string)
        (requestId: string)
        : PermissionRequest =
        {
            SessionId = sessionId
            TurnId = turnId
            ToolName = toolName
            ToolSourceId = null
            ArgumentPreview = ""
            RequestId = requestId
        }

    /// Extracts the question text from an ask_user call: the "question"
    /// string argument when present, otherwise the empty string. Never
    /// includes tool arguments beyond the question itself.
    /// <param name="call">The ask_user call.</param>
    /// <returns>The question text.</returns>
    let private extractQuestion (call: FunctionCallContent) : string =
        if isNull (box call) || isNull (box call.Arguments) then
            ""
        else
            let mutable value: obj = null

            if call.Arguments.TryGetValue("question", &value) && not (isNull value) then
                match value with
                | :? string as text when not (isNull text) -> text
                | other ->
                    let text = other.ToString()
                    if isNull text then "" else text
            else
                ""

    /// Extracts the options hint from an ask_user call: the "options"
    /// string-array argument when present, otherwise empty (free text).
    /// JSON string arrays and string enumerables become the hint; a lone
    /// string violates the array schema and degrades to free text, and
    /// non-string elements are skipped. Never includes tool arguments
    /// beyond the hint itself.
    /// <param name="call">The ask_user call.</param>
    /// <returns>The hint, or empty when the call carries no usable hint.</returns>
    let private extractOptions (call: FunctionCallContent) : string list =
        if isNull (box call) || isNull (box call.Arguments) then
            []
        else
            let mutable value: obj = null

            if call.Arguments.TryGetValue("options", &value) && not (isNull (box value)) then
                match value with
                | :? JsonElement as element when element.ValueKind = JsonValueKind.Array ->
                    [
                        for item in element.EnumerateArray() do
                            if item.ValueKind = JsonValueKind.String then
                                match item.GetString() with
                                | null -> ()
                                | text -> yield text
                    ]
                | :? string -> []
                | :? System.Collections.IEnumerable as items ->
                    [
                        for item in items do
                            match item with
                            | :? string as text when not (isNull text) -> yield text
                            | _ -> ()
                    ]
                | _ -> []
            else
                []

    /// Builds the Suspended completion for one pending request: Result
    /// carries TurnStatus.Suspended with empty text, HasPendingInjects is
    /// false (a suspended turn never starts the new-turn signal), and
    /// Suspension carries the cursor the matching Reply resumes from.
    /// <param name="suspension">The suspend cursor.</param>
    /// <returns>The Suspended completion.</returns>
    let private suspendedCompletion (suspension: TurnLoopSuspension) : TurnLoopCompletion =
        {
            Result =
                {
                    AssistantText = ""
                    Status = TurnStatus.Suspended
                    Iterations = suspension.Iterations
                    Usage =
                        {
                            InputTokens = suspension.InputTokens
                            OutputTokens = suspension.OutputTokens
                        }
                    Outcome = null
                }
            HasPendingInjects = false
            Suspension = Some suspension
        }

    /// Appends one tool result message to the running history.
    /// <param name="history">The running history.</param>
    /// <param name="callId">The tool-call id the result answers.</param>
    /// <param name="text">The result text.</param>
    let private appendToolResult (history: IList<ChatMessage>) (callId: string) (text: string) : unit =
        let resultContent = FunctionResultContent(callId, text)

        let toolMessage =
            ChatMessage(ChatRole.Tool, ResizeArray<AIContent>([| resultContent :> AIContent |]) :> IList<AIContent>)

        history.Add(toolMessage)

    /// Runs the ReAct loop with the permission gate (issue 36): Allow
    /// executes, Deny appends a denied result and continues without the
    /// effect, Ask suspends with the unified Suspended carrier. The
    /// AllowForSession memory is consulted before Evaluate: a tool name the
    /// host already allowed for the session executes without calling the
    /// policy again, so a policy stays free to keep returning Ask. The
    /// ask_user tool bypasses the policy and suspends as a question with
    /// the same carrier, unless TurnLoopOptions carries the headless
    /// policy: Fail fails the turn fast and AnswerWith continues with the
    /// canned answer, neither suspending. The skill tool bypasses the policy
    /// too and executes directly: it reads package metadata only, already
    /// authorized by the session bind. The caller (SessionActor) journals the matching
    /// PermissionRequestedEvent or QuestionAskedEvent and owns the
    /// store-first WaitingForInput transition; the loop only parks the
    /// cursor. Reply-never-starts-a-turn: a suspension never starts work.
    /// When TurnLoopOptions carries StructuredOutcome (issue 82), the loop
    /// offers the finish/fail descriptors and a call to either settles the
    /// turn immediately under the same fence; a structured turn the model
    /// stops without calling either settles Completed with an implicit
    /// TurnFinished outcome.
    /// <param name="client">The chat client the turn runs against.</param>
    /// <param name="history">The running history, mutated in place.</param>
    /// <param name="tools">The tools the turn may call.</param>
    /// <param name="options">The turn loop tuning and per-turn budget.</param>
    /// <param name="delay">The delay seam the hard deadline fires off.</param>
    /// <param name="cancellationToken">Abandons the turn.</param>
    /// <param name="isLeaseValid">The lease hook the loop checks.</param>
    /// <param name="drainInjected">The Inject drain hook.</param>
    /// <param name="onInjectJournaled">The Inject journal hook.</param>
    /// <param name="onInjectConsumed">The Inject consume hook.</param>
    /// <param name="policy">The permission policy, or null for no gate (every call executes).</param>
    /// <param name="sessionId">The session the turn runs in.</param>
    /// <param name="turnId">The turn the calls belong to.</param>
    /// <param name="newRequestId">Mints stable request ids, or None for GUIDs.</param>
    /// <param name="allowedForSession">Tool names the host already allowed for the session, or null for none.</param>
    /// <returns>The settled result or the Suspended carrier.</returns>
    let rec runSuspendableAsync
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
        (policy: IPermissionPolicy)
        (sessionId: SessionId)
        (turnId: TurnId)
        (newRequestId: (unit -> string) option)
        (allowedForSession: HashSet<string>)
        : Task<TurnLoopCompletion> =
        ArgumentNullException.ThrowIfNull(client)
        ArgumentNullException.ThrowIfNull(history)
        ArgumentNullException.ThrowIfNull(tools)
        ArgumentNullException.ThrowIfNull(delay)
        ArgumentNullException.ThrowIfNull(isLeaseValid)
        ArgumentNullException.ThrowIfNull(drainInjected)
        ArgumentNullException.ThrowIfNull(onInjectJournaled)
        ArgumentNullException.ThrowIfNull(onInjectConsumed)

        if options.MaxToolResultChars <= 0 then
            raise (ArgumentOutOfRangeException(nameof options, "MaxToolResultChars must be positive."))

        if options.MaxIterations < 1 then
            raise (ArgumentOutOfRangeException(nameof options, "MaxIterations must be at least 1."))

        if options.Timeout <= TimeSpan.Zero then
            raise (ArgumentOutOfRangeException(nameof options, "Timeout must be positive."))

        // Structured outcomes (issue 82): the offered map gains the
        // finish/fail descriptors, or the host map passes through untouched.
        // A host collision raises here, before any provider call.
        let tools = effectiveTools options tools

        let mintId =
            match newRequestId with
            | Some mint -> mint
            | None -> fun () -> Guid.NewGuid().ToString("N")

        let allowed =
            if isNull (box allowedForSession) then
                HashSet<string>()
            else
                allowedForSession

        let timeoutCts = new CancellationTokenSource()

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

        let isTimeout () =
            timeoutCts.IsCancellationRequested
            && not cancellationToken.IsCancellationRequested

        let chatOptions = ChatOptions()
        chatOptions.Tools <- ResizeArray<AITool>(tools.Values) :> IList<AITool>

        let foldInjects () =
            let pending = selectInjects (drainInjected ())

            for entry in pending do
                history.Add(injectToMessage entry)
                onInjectJournaled entry
                onInjectConsumed entry

        let hasPendingInjects () =
            selectInjects (drainInjected ()) |> List.isEmpty |> not

        let completedCompletion iterations inputTokens outputTokens assistantText : TurnLoopCompletion =
            {
                Result =
                    if options.StructuredOutcome then
                        // No finish call ended this turn (a finish call
                        // settles immediately), so the model stopped without
                        // calling either: synthesize the implicit Finished.
                        implicitFinishedResult iterations inputTokens outputTokens assistantText
                    else
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
                Suspension = None
            }

        let failedCompletion iterations inputTokens outputTokens reason : TurnLoopCompletion =
            {
                Result = failedResult iterations inputTokens outputTokens reason
                HasPendingInjects = false
                Suspension = None
            }

        let suspendFor
            (call: FunctionCallContent)
            (toolName: string)
            (kind: SuspensionKind)
            (questionText: string)
            (questionOptions: string list)
            (iterations: int)
            (inputTokens: int64)
            (outputTokens: int64)
            : TurnLoopCompletion =
            let requestId = mintId ()

            let suspension =
                {
                    RequestId = requestId
                    ToolName = toolName
                    ToolCallId = call.CallId
                    Kind = kind
                    QuestionText = questionText
                    QuestionOptions = questionOptions
                    HistorySnapshot = snapshotHistory history
                    InputTokens = inputTokens
                    OutputTokens = outputTokens
                    Iterations = iterations
                    PendingCall = call
                    Nested = None
                }

            suspendedCompletion suspension

        /// Wraps one nested resume into the parent continuation: runs the
        /// nested resume with the reply, then maps the nested outcome back
        /// onto the parent turn. A settled nested run appends its shaped
        /// text as the task call's tool result and continues the parent
        /// loop from a fresh budget scope with the spent totals folded
        /// back, mirroring the standard resume continuations. A
        /// re-suspended nested run parks the parent again carrying the
        /// fresh cursor with this same wrapper, so every reply re-enters
        /// the nested loop before the parent continues.
        let rec wrapNestedResume
            (taskCall: FunctionCallContent)
            (prefixIterations: int)
            (prefixInput: int64)
            (prefixOutput: int64)
            (resumeNested: TaskNestedResume)
            : Reply -> CancellationToken -> Task<TurnLoopCompletion> =
            fun reply resumeToken ->
                task {
                    try
                        let! nested = resumeNested reply resumeToken

                        let shaped = truncateToolResult options nested.Text
                        appendToolResult history taskCall.CallId shaped
                        do! observeToolCallAsync options taskCall shaped None

                        let! continued =
                            runSuspendableAsync
                                client
                                history
                                tools
                                options
                                delay
                                resumeToken
                                isLeaseValid
                                drainInjected
                                onInjectJournaled
                                onInjectConsumed
                                policy
                                sessionId
                                turnId
                                newRequestId
                                allowed

                        let totalIterations =
                            prefixIterations + nested.Iterations + continued.Result.Iterations

                        let totalInput =
                            prefixInput + nested.InputTokens + continued.Result.Usage.InputTokens

                        let totalOutput =
                            prefixOutput + nested.OutputTokens + continued.Result.Usage.OutputTokens

                        let totalResult =
                            { continued.Result with
                                Iterations = totalIterations
                                Usage =
                                    {
                                        InputTokens = totalInput
                                        OutputTokens = totalOutput
                                    }
                            }

                        return { continued with Result = totalResult }
                    with TaskNestedSuspended(cursor, resume) ->
                        return
                            suspendedCompletion (
                                parentSuspensionOf taskCall prefixIterations prefixInput prefixOutput cursor resume
                            )
                }

        /// Parks the parent turn on one nested suspension: a fresh
        /// parent-level request id the host answers, the nested tool name
        /// and question for display, the parent task call parked, and the
        /// nested cursor with the wrapped resume for the matching reply.
        /// Totals fold the nested spend into the parent prefix, like the
        /// standard suspend points.
        and parentSuspensionOf
            (taskCall: FunctionCallContent)
            (prefixIterations: int)
            (prefixInput: int64)
            (prefixOutput: int64)
            (cursor: TurnLoopSuspension)
            (resumeNested: TaskNestedResume)
            : TurnLoopSuspension =
            {
                RequestId = mintId ()
                ToolName = cursor.ToolName
                ToolCallId = taskCall.CallId
                Kind = cursor.Kind
                QuestionText = cursor.QuestionText
                QuestionOptions = cursor.QuestionOptions
                HistorySnapshot = snapshotHistory history
                InputTokens = prefixInput + cursor.InputTokens
                OutputTokens = prefixOutput + cursor.OutputTokens
                Iterations = prefixIterations + cursor.Iterations
                PendingCall = taskCall
                Nested =
                    Some
                        {
                            Cursor = cursor
                            ResumeAsync =
                                wrapNestedResume taskCall prefixIterations prefixInput prefixOutput resumeNested
                        }
            }

        /// Runs one task-tool call through the options hook and maps the
        /// nested outcome onto the parent round: a settled nested run
        /// appends its shaped text as the tool result with the nested
        /// totals folded into the parent budget; a suspended nested run
        /// parks the parent carrying the nested cursor. A missing hook
        /// reads the call as an unknown tool. Lease loss and cancellation
        /// propagate instead of shaping, so a fenced-out loser never
        /// reports success past the fence.
        /// <param name="call">The parent task call.</param>
        /// <param name="roundIterations">The parent iterations spent this round.</param>
        /// <param name="roundInput">The parent input tokens spent this round.</param>
        /// <param name="roundOutput">The parent output tokens spent this round.</param>
        /// <returns>The next parent totals with the suspension, or the totals with no suspension.</returns>
        let runTaskCallAsync
            (call: FunctionCallContent)
            (roundIterations: int)
            (roundInput: int64)
            (roundOutput: int64)
            : Task<int * int64 * int64 * TurnLoopCompletion option> =
            task {
                match options.TaskNested with
                | None ->
                    appendToolResult history call.CallId UnknownToolMessage
                    do! observeToolCallAsync options call UnknownToolMessage (Some UnknownToolMessage)
                    return roundIterations, roundInput, roundOutput, None
                | Some runNested ->
                    let request: TaskNestedRequest =
                        {
                            Call = call
                            Tools = tools
                            Options = options
                            Client = client
                            Delay = delay
                            CancellationToken = linkedToken
                            IsLeaseValid = isLeaseValid
                            Policy = policy
                            SessionId = sessionId
                            ParentTurnId = turnId
                            NewRequestId = newRequestId
                            AllowedForSession = allowed
                        }

                    try
                        let! nested = runNested request
                        let shaped = truncateToolResult options nested.Text
                        appendToolResult history call.CallId shaped
                        do! observeToolCallAsync options call shaped None

                        return
                            roundIterations + nested.Iterations,
                            roundInput + nested.InputTokens,
                            roundOutput + nested.OutputTokens,
                            None
                    with TaskNestedSuspended(cursor, resume) ->
                        let suspension =
                            parentSuspensionOf call roundIterations roundInput roundOutput cursor resume

                        return roundIterations, roundInput, roundOutput, Some(suspendedCompletion suspension)
            }

        let rec runTools
            (roundIterations: int)
            (roundInput: int64)
            (roundOutput: int64)
            (pending: FunctionCallContent list)
            : Task<TurnLoopCompletion option> =
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
                        match options.VerifyClaim with
                        | Some verify ->
                            let! live = verify ()

                            if not live then
                                raise (TurnLeaseLostException())
                        | None -> ()

                        let toolName = if isNull call.Name then "" else call.Name

                        // Structured outcomes (issue 82): finish/fail settle
                        // the turn immediately under the fence above, so the
                        // takeover loser raises TurnLeaseLostException there
                        // instead of settling. The permission gate never sees
                        // them: they are runtime control calls like ask_user,
                        // not host tools. Malformed calls append an Error:
                        // continuation and run on; calls after a settling
                        // call in the same round never run.
                        if
                            options.StructuredOutcome
                            && String.Equals(toolName, FinishToolName, StringComparison.Ordinal)
                        then
                            match Option.ofObj (structuredArgText call.Arguments "summary") with
                            | Some summary when not (String.IsNullOrWhiteSpace summary) ->
                                appendToolResult history call.CallId summary
                                do! observeToolCallAsync options call summary None

                                return
                                    Some(
                                        {
                                            Result = finishedResult roundIterations roundInput roundOutput summary
                                            HasPendingInjects = hasPendingInjects ()
                                            Suspension = None
                                        }
                                    )
                            | _ ->
                                appendToolResult history call.CallId FinishMissingSummaryMessage

                                do!
                                    observeToolCallAsync
                                        options
                                        call
                                        FinishMissingSummaryMessage
                                        (Some FinishMissingSummaryMessage)

                                return! runTools roundIterations roundInput roundOutput rest
                        elif
                            options.StructuredOutcome
                            && String.Equals(toolName, FailToolName, StringComparison.Ordinal)
                        then
                            match Option.ofObj (structuredArgText call.Arguments "error") with
                            | Some error when not (String.IsNullOrWhiteSpace error) ->
                                appendToolResult history call.CallId error
                                do! observeToolCallAsync options call error None

                                return
                                    Some(
                                        {
                                            Result = failedResult roundIterations roundInput roundOutput error
                                            HasPendingInjects = false
                                            Suspension = None
                                        }
                                    )
                            | _ ->
                                appendToolResult history call.CallId FailMissingErrorMessage

                                do!
                                    observeToolCallAsync
                                        options
                                        call
                                        FailMissingErrorMessage
                                        (Some FailMissingErrorMessage)

                                return! runTools roundIterations roundInput roundOutput rest
                        elif String.Equals(toolName, AskUserToolName, StringComparison.Ordinal) then
                            let question = extractQuestion call
                            let hint = extractOptions call

                            // Headless policy (issue 64): None suspends for
                            // a host answer; AnswerWith continues with the
                            // canned answer as the tool result; anything
                            // else (Fail, or an unknown mode) fails closed
                            // fast instead of hallucinating user input. The
                            // failure reasons never carry the question.
                            match options.AskUser with
                            | None ->
                                return
                                    Some(
                                        suspendFor
                                            call
                                            toolName
                                            QuestionSuspension
                                            question
                                            hint
                                            roundIterations
                                            roundInput
                                            roundOutput
                                    )
                            | Some ask when ask.Mode = AskUserMode.AnswerWith ->
                                match Option.ofObj ask.CannedAnswer with
                                | Some canned when not (String.IsNullOrWhiteSpace canned) ->
                                    appendToolResult history call.CallId canned
                                    do! observeToolCallAsync options call canned None
                                    return! runTools roundIterations roundInput roundOutput rest
                                | _ ->
                                    return
                                        Some(
                                            failedCompletion
                                                roundIterations
                                                roundInput
                                                roundOutput
                                                AskUserCannedMissingMessage
                                        )
                            | Some _ ->
                                return
                                    Some(
                                        failedCompletion
                                            roundIterations
                                            roundInput
                                            roundOutput
                                            AskUserHeadlessFailMessage
                                    )
                        elif String.Equals(toolName, SkillToolName, StringComparison.Ordinal) then
                            // Skill loader bypass (issue 68): the loader reads
                            // package metadata only, already authorized by the
                            // session bind, so it never consults the policy.
                            // The last-moment VerifyClaim fence above still
                            // applies, so a takeover loser never reaches the
                            // invocation.
                            let! rawText = invokeAndObserveAsync options tools call linkedToken
                            let text = truncateToolResult options rawText
                            appendToolResult history call.CallId text
                            return! runTools roundIterations roundInput roundOutput rest
                        elif String.Equals(toolName, TaskToolName, StringComparison.Ordinal) then
                            // Task tool (issue 72): the call runs the
                            // requested sub-agent through the nested loop
                            // under the options hook instead of invoking.
                            // The fence above still applies, so a takeover
                            // loser never starts the nested run; the policy
                            // is not consulted for the task call itself
                            // (like the skill bypass), the nested loop gates
                            // every nested call instead. A nested suspension
                            // parks the parent carrying the nested cursor
                            // for the matching reply.
                            let! nextIterations, nextInput, nextOutput, suspended =
                                runTaskCallAsync call roundIterations roundInput roundOutput

                            match suspended with
                            | Some completion -> return Some completion
                            | None -> return! runTools nextIterations nextInput nextOutput rest
                        else
                            let remembered = allowed.Contains(toolName)

                            if remembered || isNull (box policy) then
                                let! rawText = invokeAndObserveAsync options tools call linkedToken
                                let text = truncateToolResult options rawText
                                appendToolResult history call.CallId text
                                return! runTools roundIterations roundInput roundOutput rest
                            else
                                let requestId = mintId ()
                                let request = buildPermissionRequest sessionId turnId toolName requestId
                                let verdict = policy.Evaluate(request)

                                if isNull (box verdict) then
                                    return!
                                        Task.FromException<TurnLoopCompletion option>(
                                            InvalidOperationException(
                                                "The permission policy returned null instead of a verdict."
                                            )
                                        )
                                elif verdict :? AllowVerdict then
                                    let! rawText = invokeAndObserveAsync options tools call linkedToken
                                    let text = truncateToolResult options rawText
                                    appendToolResult history call.CallId text
                                    return! runTools roundIterations roundInput roundOutput rest
                                elif verdict :? DenyVerdict then
                                    let deny = verdict :?> DenyVerdict
                                    let reason = if isNull deny.Reason then "" else deny.Reason
                                    appendToolResult history call.CallId (DenyResultPrefix + reason)
                                    do! observeToolCallAsync options call (DenyResultPrefix + reason) None
                                    return! runTools roundIterations roundInput roundOutput rest
                                elif verdict :? AskVerdict then
                                    let suspension =
                                        {
                                            RequestId = requestId
                                            ToolName = toolName
                                            ToolCallId = call.CallId
                                            Kind = PermissionSuspension
                                            QuestionText = ""
                                            QuestionOptions = []
                                            HistorySnapshot = snapshotHistory history
                                            InputTokens = roundInput
                                            OutputTokens = roundOutput
                                            Iterations = roundIterations
                                            PendingCall = call
                                            Nested = None
                                        }

                                    return Some(suspendedCompletion suspension)
                                else
                                    return!
                                        Task.FromException<TurnLoopCompletion option>(
                                            InvalidOperationException(
                                                sprintf
                                                    "The permission policy returned an unknown verdict: %s."
                                                    (verdict.GetType().FullName)
                                            )
                                        )
            }

        let rec loop iterations inputTokens outputTokens : Task<TurnLoopCompletion> =
            task {
                cancellationToken.ThrowIfCancellationRequested()

                if not (isLeaseValid ()) then
                    return! Task.FromException<TurnLoopCompletion>(TurnLeaseLostException())
                elif iterations >= options.MaxIterations then
                    return failedCompletion iterations inputTokens outputTokens MaxIterationsExceededMessage
                elif timeoutCts.IsCancellationRequested then
                    return failedCompletion iterations inputTokens outputTokens TimeoutExceededMessage
                else
                    foldInjects ()

                    // Compaction boundary (issue 45): one pass per
                    // iteration, after the lease and budget checks and
                    // before the provider call, like the Inject fold above.
                    let! compactedInput, compactedOutput =
                        match options.Compaction with
                        | Some compact -> compact history inputTokens outputTokens linkedToken
                        | None -> Task.FromResult((inputTokens, outputTokens))

                    try
                        let! response =
                            LlmStreaming.streamResponseAsync client history chatOptions linkedToken ignore ignore

                        let nextIterations = iterations + 1
                        let mutable nextInput = compactedInput
                        let mutable nextOutput = compactedOutput

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
                                | Some suspended -> return suspended
                                | None -> return! loop nextIterations nextInput nextOutput
                    with :? OperationCanceledException when isTimeout () ->
                        return failedCompletion iterations inputTokens outputTokens TimeoutExceededMessage
            }

        task {
            try
                return! loop 0 0L 0L
            finally
                timeoutCts.Dispose()
                linkedCts.Dispose()
        }

    /// Resumes a permission suspension from its cursor: AllowOnce executes
    /// the parked call, AllowForSession executes it and remembers the tool
    /// for the session, Deny skips it with a denied result. The parked call
    /// is the same FunctionCallContent the suspend carried, so the resume
    /// continues from the same tool call. The loop then continues with the
    /// gate still applied to later calls.
    /// <param name="suspension">The suspend cursor. Must be a permission suspension.</param>
    /// <param name="decision">What the host decided.</param>
    /// <param name="client">The chat client the continued turn runs against.</param>
    /// <param name="history">The running history (already holds the suspend point).</param>
    /// <param name="tools">The tools the continued turn may call.</param>
    /// <param name="options">The turn loop tuning and per-turn budget.</param>
    /// <param name="delay">The delay seam the continued deadline fires off.</param>
    /// <param name="cancellationToken">Abandons the continued turn.</param>
    /// <param name="isLeaseValid">The lease hook the continued loop checks.</param>
    /// <param name="policy">The permission policy for later calls, or null for no gate.</param>
    /// <param name="allowedForSession">The session memory, mutated on AllowForSession.</param>
    /// <returns>The settled result or the next Suspended carrier.</returns>
    let resumePermissionAsync
        (suspension: TurnLoopSuspension)
        (decision: PermissionDecisionKind)
        (client: IChatClient)
        (history: IList<ChatMessage>)
        (tools: IReadOnlyDictionary<string, AITool>)
        (options: TurnLoopOptions)
        (delay: ILlmDelay)
        (cancellationToken: CancellationToken)
        (isLeaseValid: unit -> bool)
        (policy: IPermissionPolicy)
        (allowedForSession: HashSet<string>)
        : Task<TurnLoopCompletion> =
        if isNull (box suspension) then
            raise (ArgumentNullException(nameof suspension))

        ArgumentNullException.ThrowIfNull(client)
        ArgumentNullException.ThrowIfNull(history)
        ArgumentNullException.ThrowIfNull(tools)
        ArgumentNullException.ThrowIfNull(delay)
        ArgumentNullException.ThrowIfNull(isLeaseValid)

        if suspension.Kind <> PermissionSuspension then
            raise (ArgumentException("The suspension is not a permission suspension.", nameof suspension))

        if not (Enum.IsDefined(typeof<PermissionDecisionKind>, decision)) then
            raise (ArgumentOutOfRangeException(nameof decision, "Unknown permission decision."))

        let allowed =
            if isNull (box allowedForSession) then
                HashSet<string>()
            else
                allowedForSession

        task {
            match decision with
            | PermissionDecisionKind.AllowOnce -> ()
            | PermissionDecisionKind.AllowForSession -> allowed.Add(suspension.ToolName) |> ignore
            | PermissionDecisionKind.Deny ->
                appendToolResult history suspension.ToolCallId (DenyResultPrefix + "the host denied the call")

                do!
                    observeToolCallAsync
                        options
                        suspension.PendingCall
                        (DenyResultPrefix + "the host denied the call")
                        None
            | _ -> raise (ArgumentOutOfRangeException(nameof decision, "Unknown permission decision."))

            match decision with
            | PermissionDecisionKind.Deny -> ()
            | _ ->
                if not (isLeaseValid ()) then
                    raise (TurnLeaseLostException())

                match options.VerifyClaim with
                | Some verify ->
                    let! live = verify ()

                    if not live then
                        raise (TurnLeaseLostException())
                | None -> ()

                let! rawText = invokeAndObserveAsync options tools suspension.PendingCall cancellationToken
                let text = truncateToolResult options rawText
                appendToolResult history suspension.ToolCallId text

            let sessionId = Unchecked.defaultof<SessionId>
            let turnId = Unchecked.defaultof<TurnId>

            let! continued =
                runSuspendableAsync
                    client
                    history
                    tools
                    options
                    delay
                    cancellationToken
                    isLeaseValid
                    noInjects
                    ignoreInject
                    ignoreInject
                    policy
                    sessionId
                    turnId
                    None
                    allowed

            // The continued run restarts its own iteration and usage
            // counters from zero; fold the spent prefix back so the caller
            // observes the turn-total, not the post-resume tail.
            let totalIterations = suspension.Iterations + continued.Result.Iterations

            let totalInput = suspension.InputTokens + continued.Result.Usage.InputTokens

            let totalOutput = suspension.OutputTokens + continued.Result.Usage.OutputTokens

            let totalResult =
                { continued.Result with
                    Iterations = totalIterations
                    Usage =
                        {
                            InputTokens = totalInput
                            OutputTokens = totalOutput
                        }
                }

            return { continued with Result = totalResult }
        }

    /// Resumes a question suspension from its cursor: appends the host's
    /// answer as the ask_user tool result, then continues the loop with the
    /// gate still applied to later calls.
    /// <param name="suspension">The suspend cursor. Must be a question suspension.</param>
    /// <param name="answer">The host's answer, verbatim.</param>
    /// <param name="client">The chat client the continued turn runs against.</param>
    /// <param name="history">The running history (already holds the suspend point).</param>
    /// <param name="tools">The tools the continued turn may call.</param>
    /// <param name="options">The turn loop tuning and per-turn budget.</param>
    /// <param name="delay">The delay seam the continued deadline fires off.</param>
    /// <param name="cancellationToken">Abandons the continued turn.</param>
    /// <param name="isLeaseValid">The lease hook the continued loop checks.</param>
    /// <param name="policy">The permission policy for later calls, or null for no gate.</param>
    /// <param name="allowedForSession">The session memory.</param>
    /// <returns>The settled result or the next Suspended carrier.</returns>
    let resumeQuestionAsync
        (suspension: TurnLoopSuspension)
        (answer: string)
        (client: IChatClient)
        (history: IList<ChatMessage>)
        (tools: IReadOnlyDictionary<string, AITool>)
        (options: TurnLoopOptions)
        (delay: ILlmDelay)
        (cancellationToken: CancellationToken)
        (isLeaseValid: unit -> bool)
        (policy: IPermissionPolicy)
        (allowedForSession: HashSet<string>)
        : Task<TurnLoopCompletion> =
        if isNull (box suspension) then
            raise (ArgumentNullException(nameof suspension))

        ArgumentNullException.ThrowIfNull(client)
        ArgumentNullException.ThrowIfNull(history)
        ArgumentNullException.ThrowIfNull(tools)
        ArgumentNullException.ThrowIfNull(delay)
        ArgumentNullException.ThrowIfNull(isLeaseValid)

        if suspension.Kind <> QuestionSuspension then
            raise (ArgumentException("The suspension is not a question suspension.", nameof suspension))

        let text = if isNull answer then "" else answer

        let allowed =
            if isNull (box allowedForSession) then
                HashSet<string>()
            else
                allowedForSession

        task {
            appendToolResult history suspension.ToolCallId text

            let sessionId = Unchecked.defaultof<SessionId>
            let turnId = Unchecked.defaultof<TurnId>

            let! continued =
                runSuspendableAsync
                    client
                    history
                    tools
                    options
                    delay
                    cancellationToken
                    isLeaseValid
                    noInjects
                    ignoreInject
                    ignoreInject
                    policy
                    sessionId
                    turnId
                    None
                    allowed

            let totalIterations = suspension.Iterations + continued.Result.Iterations

            let totalInput = suspension.InputTokens + continued.Result.Usage.InputTokens

            let totalOutput = suspension.OutputTokens + continued.Result.Usage.OutputTokens

            let totalResult =
                { continued.Result with
                    Iterations = totalIterations
                    Usage =
                        {
                            InputTokens = totalInput
                            OutputTokens = totalOutput
                        }
                }

            return { continued with Result = totalResult }
        }
