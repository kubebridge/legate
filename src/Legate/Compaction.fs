// SPDX-License-Identifier: Apache-2.0
#nowarn "3261"

namespace Legate

open System
open System.Collections.Generic
open System.Text
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.AI

// Nullness warning 3261 is suppressed in this file: MEAI interop surfaces
// nulls (null messages, contents, usage, result objects) that the F#
// nullable analysis cannot prove absent, and compaction treats every one as
// empty rather than failing.

// Internal compaction (issue 45): summarise the conversation when its
// estimate crosses the compaction threshold, using the session's model or
// the configured compaction model override.
//
// Estimates and thresholds reuse issue 44 (ContextPruning.Estimate over
// transient message-mapped cells, ContextPruning.PruneThreshold with
// ReservedBufferTokens), never re-derived: the estimator stays a documented
// heuristic bound, not an exact tokenizer count. The rewrite keeps the
// leading system message plus a summary message plus the last K messages
// (K is LlmOptions.CompactionKeepMessages), replacing everything between
// the system message and the tail.
//
// The TurnLoop calls one hook per iteration boundary, before the provider
// call: a single summarise-and-rewrite pass per boundary, never a
// while-over-threshold loop, so the summariser call cannot recursively
// re-trip the check. Usage from the summariser call reports to
// IUsageObserver like any call (one checkpoint with the turn-cumulative
// totals); a denied model, a provider error, or an empty summary journals
// a CompactionFailedEvent and continues the turn uncompacted, never a
// TurnFailedEvent.
//
// Fencing: journal writes check the claim token at the last moment through
// the injected lease hook; a fenced-out write raises
// TurnLoop.TurnLeaseLostException like any other loser, so the takeover
// loser journals nothing.
//
// Suspend/resume (issue 36): compaction rewrites only the running history
// in place at the iteration boundary, which is the single mutation point.
// A suspension parked after compaction snapshots the compacted history in
// HistorySnapshot; crash rebuild replays the journal (CompactedEvent and
// CompactionFailedEvent included) for pending-request recovery. No separate
// snapshot handling.
//
// The runner stays internal-callable so issue 46 (ILegateClient.Compact)
// reuses it: CompactionRequest carries everything per attempt, createHook
// adapts it to the TurnLoop boundary shape.
module internal Compaction =

    /// Instruction appended as the final user message of the summariser
    /// request: the summary preserves facts, decisions, and open tasks and
    /// carries text only.
    [<Literal>]
    let SummarizeInstruction =
        "Summarize the conversation so far concisely, preserving key facts, decisions, and open tasks. Reply with the summary text only."

    /// Marker prefixing the summary message written into the rewritten
    /// history, mirroring ContextPruningOptions.DefaultPrunedMarker so hosts
    /// can detect compacted context.
    [<Literal>]
    let SummaryMarker = "[legate-compacted-summary]"

    /// What one compaction attempt needs: the turn's client, history, and
    /// usage totals plus the resolved settings, identities, and the fenced
    /// journal sink. History is rewritten in place only after the
    /// CompactedEvent journals.
    type CompactionRequest =
        {
            /// The chat client the turn runs against; the summariser call
            /// goes through it with the session or override model.
            Client: IChatClient
            /// The running history, rewritten in place on success.
            History: IList<ChatMessage>
            /// The session's model: the threshold catalog lookup and the
            /// default summariser model.
            SessionModel: ModelReference
            /// The configured override in provider/model form, or null for
            /// the session's model.
            CompactionModel: string | null
            /// How many of the most recent history messages the rewrite
            /// keeps after the summary.
            KeepMessages: int
            /// The session model's catalog entry, or null when the model is
            /// unknown (the threshold falls back to the catalog defaults).
            CatalogEntry: ModelCatalogEntry | null
            /// The tokens held back beyond the reserved output when deriving
            /// the threshold.
            ReservedBufferTokens: int
            /// Receives the summariser usage checkpoint, or null for no
            /// observation.
            Observer: IUsageObserver | null
            /// Authorises the summariser model call, or null for no gate.
            ModelPolicy: IModelPolicy | null
            /// The tenant the turn belongs to.
            Tenant: TenantId
            /// The session the turn runs in.
            SessionId: SessionId
            /// The turn attempting the compaction.
            TurnId: TurnId
            /// The 1-based attempt the turn runs under.
            Attempt: int
            /// Input tokens the turn spent before this boundary.
            InputTokens: int64
            /// Output tokens the turn spent before this boundary.
            OutputTokens: int64
            /// Journals one event under the claim fence.
            JournalAsync: SessionEvent -> Task<JournalWriter.JournalWriteResult>
            /// The last-moment claim fence the journal write checks.
            IsLeaseValid: unit -> bool
            /// Abandons the summariser call.
            CancellationToken: CancellationToken
        }

    /// What one compaction attempt decided.
    type CompactionOutcome =
        /// Under threshold (or nothing to replace): no provider call, no
        /// journal write, history untouched.
        | NotNeeded
        /// Summarised and rewritten: carries the before/after estimates plus
        /// the turn totals with the summariser usage folded in.
        | Compacted of beforeEstimate: int64 * afterEstimate: int64 * inputTokens: int64 * outputTokens: int64
        /// A denied model, provider error, or empty summary: carries the
        /// client-safe reason plus the unchanged turn totals. The failure
        /// event is journaled; the turn continues uncompacted.
        | FailedContinue of reason: string * inputTokens: int64 * outputTokens: int64

    /// Everything createHook fixes per turn; the boundary supplies the
    /// history, usage totals, and cancellation token per iteration.
    type CompactionHookDeps =
        {
            /// The chat client the turn runs against.
            Client: IChatClient
            /// The session's model: the threshold lookup and default
            /// summariser model.
            SessionModel: ModelReference
            /// The configured override in provider/model form, or null for
            /// the session's model.
            CompactionModel: string | null
            /// How many of the most recent history messages the rewrite
            /// keeps after the summary.
            KeepMessages: int
            /// The session model's catalog entry, or null when unknown.
            CatalogEntry: ModelCatalogEntry | null
            /// The tokens held back beyond the reserved output.
            ReservedBufferTokens: int
            /// Receives the summariser usage checkpoint, or null.
            Observer: IUsageObserver | null
            /// Authorises the summariser model call, or null.
            ModelPolicy: IModelPolicy | null
            /// The tenant the turn belongs to.
            Tenant: TenantId
            /// The session the turn runs in.
            SessionId: SessionId
            /// The turn compacting.
            TurnId: TurnId
            /// The 1-based attempt the turn runs under.
            Attempt: int
            /// Journals one event under the claim fence.
            JournalAsync: SessionEvent -> Task<JournalWriter.JournalWriteResult>
            /// The last-moment claim fence the journal write checks.
            IsLeaseValid: unit -> bool
        }

    /// Maps one history message to its estimate kind: user, assistant, and
    /// system roles map to their cell kinds, tool messages to ToolResult,
    /// anything else to System. Null messages map to System.
    let private kindOf (message: ChatMessage) : SessionCellKind =
        if isNull (box message) then
            SessionCellKind.System
        elif message.Role = ChatRole.User then
            SessionCellKind.User
        elif message.Role = ChatRole.Assistant then
            SessionCellKind.Assistant
        elif message.Role = ChatRole.Tool then
            SessionCellKind.ToolResult
        elif message.Role = ChatRole.System then
            SessionCellKind.System
        else
            SessionCellKind.System

    /// Converts a tool return value to text. Null becomes empty, strings
    /// pass through, anything else uses ToString. Mirrors
    /// TurnLoop's private mapping so estimates stay symmetric.
    let private toolValueToString (value: obj) : string =
        if isNull value then
            ""
        else
            match value with
            | :? string as text -> if isNull text then "" else text
            | other ->
                let text = other.ToString()
                if isNull text then "" else text

    /// Extracts one history message's text for estimation: TextContent
    /// parts joined with newlines plus FunctionResultContent payloads in
    /// order. Tool names estimate through the ToolName mapping, not here.
    let private textOf (message: ChatMessage) : string =
        if isNull (box message) || isNull (box message.Contents) then
            ""
        else
            let builder = StringBuilder()
            let mutable parts = 0

            let append (text: string) =
                if not (isNull text) then
                    if parts > 0 then
                        builder.Append '\n' |> ignore

                    builder.Append(text) |> ignore
                    parts <- parts + 1

            for content in message.Contents do
                if not (isNull (box content)) then
                    match content with
                    | :? TextContent as text when not (isNull (box text)) -> append text.Text
                    | :? FunctionResultContent as result when not (isNull (box result)) ->
                        append (toolValueToString result.Result)
                    | _ -> ()

            builder.ToString()

    /// Extracts the first function-call name in a history message for the
    /// ToolName estimate leg, or null when the message calls nothing.
    let private callNameOf (message: ChatMessage) : string | null =
        if isNull (box message) || isNull (box message.Contents) then
            Unchecked.defaultof<string>
        else
            let mutable name: string | null = Unchecked.defaultof<string>

            for content in message.Contents do
                if isNull (box name) && not (isNull (box content)) then
                    match content with
                    | :? FunctionCallContent as call when not (isNull (box call)) -> name <- call.Name
                    | _ -> ()

            name

    /// Maps a history to transient estimate cells, one per message in
    /// order. Pure: reads the history, returns fresh cells, performs no
    /// I/O. The cells exist only for ContextPruning.Estimate; the estimate
    /// is a heuristic bound, not an exact tokenizer count.
    /// <param name="history">The running history. Must not be null.</param>
    /// <param name="sessionId">The session the turn belongs to; every cell carries it.</param>
    /// <param name="turnId">The turn compacting; every cell carries it.</param>
    /// <returns>The transient cells, in history order.</returns>
    let toEstimateCells
        (history: IList<ChatMessage>)
        (sessionId: SessionId)
        (turnId: TurnId)
        : IReadOnlyList<SessionCell> =
        if isNull (box history) then
            raise (ArgumentNullException(nameof history))

        let cells = ResizeArray<SessionCell>(history.Count)

        for message in history do
            cells.Add
                {
                    Id = Unchecked.defaultof<CellId>
                    SessionId = sessionId
                    TurnId = turnId
                    Kind = kindOf message
                    Content = textOf message
                    ToolName = callNameOf message
                    ToolCallId = Unchecked.defaultof<string>
                    IsError = false
                    Iteration = 0
                    Metadata = Unchecked.defaultof<IReadOnlyDictionary<string, string>>
                    Artifacts = Unchecked.defaultof<IReadOnlyList<string>>
                    Timestamp = DateTimeOffset.UtcNow
                }

        cells :> IReadOnlyList<SessionCell>

    /// Estimates a history's token size through ContextPruning.Estimate over
    /// the transient cells. Pure.
    /// <param name="history">The running history. Must not be null.</param>
    /// <param name="sessionId">The session the turn belongs to.</param>
    /// <param name="turnId">The turn compacting.</param>
    /// <returns>The history's estimated token count.</returns>
    let estimateHistory (history: IList<ChatMessage>) (sessionId: SessionId) (turnId: TurnId) : int64 =
        ContextPruning.Estimate(toEstimateCells history sessionId turnId)

    /// Reports whether a history crosses the compaction threshold: its
    /// estimate strictly exceeds ContextPruning.PruneThreshold for the
    /// session model's catalog entry. Pure; the threshold is never
    /// re-derived here.
    /// <param name="history">The running history. Must not be null.</param>
    /// <param name="sessionId">The session the turn belongs to.</param>
    /// <param name="turnId">The turn compacting.</param>
    /// <param name="entry">The session model's catalog entry, or null when the model is unknown.</param>
    /// <param name="reservedBufferTokens">The extra buffer held back beyond the reserved output.</param>
    /// <returns>Whether the history crosses the threshold, with the estimate and the threshold.</returns>
    let shouldCompact
        (history: IList<ChatMessage>)
        (sessionId: SessionId)
        (turnId: TurnId)
        (entry: ModelCatalogEntry | null)
        (reservedBufferTokens: int)
        : bool * int64 * int =
        let estimate = estimateHistory history sessionId turnId
        let threshold = ContextPruning.PruneThreshold(entry, reservedBufferTokens)
        (estimate > int64 threshold, estimate, threshold)

    /// Whether the leading history message is a system message: the one
    /// message the rewrite always keeps.
    let private hasSystemPrefix (history: IList<ChatMessage>) : bool =
        history.Count > 0
        && not (isNull (box history[0]))
        && history[0].Role = ChatRole.System

    /// Whether rewriting would replace anything: at least one message sits
    /// outside the kept system prefix and the kept tail. Pure.
    /// <param name="history">The running history. Must not be null.</param>
    /// <param name="keepMessages">How many of the most recent messages the rewrite keeps. Must be at least 0.</param>
    /// <returns>True when the rewrite would replace at least one message.</returns>
    let wouldReplace (history: IList<ChatMessage>) (keepMessages: int) : bool =
        if isNull (box history) then
            raise (ArgumentNullException(nameof history))

        if keepMessages < 0 then
            raise (ArgumentOutOfRangeException(nameof keepMessages, "CompactionKeepMessages must be at least 0."))

        let startIndex = if hasSystemPrefix history then 1 else 0
        let replaceable = history.Count - startIndex
        replaceable > 0 && keepMessages < replaceable

    /// Plans the rewritten history: the leading system message when
    /// present, one user message carrying the marked summary, then the last
    /// keepMessages messages. Returns None when nothing would be replaced,
    /// so the caller skips the summariser call. Pure.
    /// <param name="history">The running history. Must not be null.</param>
    /// <param name="summary">The summary text. Must not be null.</param>
    /// <param name="keepMessages">How many of the most recent messages to keep. Must be at least 0.</param>
    /// <returns>The planned history, or None when the rewrite would replace nothing.</returns>
    let planRewrite (history: IList<ChatMessage>) (summary: string) (keepMessages: int) : ChatMessage list option =
        if isNull (box history) then
            raise (ArgumentNullException(nameof history))

        if isNull (box summary) then
            raise (ArgumentNullException(nameof summary))

        if keepMessages < 0 then
            raise (ArgumentOutOfRangeException(nameof keepMessages, "CompactionKeepMessages must be at least 0."))

        if not (wouldReplace history keepMessages) then
            None
        else
            let prefix = if hasSystemPrefix history then [ history[0] ] else []
            let startIndex = prefix.Length
            let replaceable = history.Count - startIndex
            let tailCount = min keepMessages replaceable

            let tail =
                [
                    for index in history.Count - tailCount .. history.Count - 1 -> history[index]
                ]

            let summaryMessage = ChatMessage(ChatRole.User, SummaryMarker + "\n" + summary)

            Some(prefix @ [ summaryMessage ] @ tail)

    /// Applies a planned rewrite to the running history in place.
    /// <param name="history">The running history. Must not be null.</param>
    /// <param name="planned">The planned history. Must not be null.</param>
    let applyRewrite (history: IList<ChatMessage>) (planned: ChatMessage list) : unit =
        if isNull (box history) then
            raise (ArgumentNullException(nameof history))

        if isNull (box planned) then
            raise (ArgumentNullException(nameof planned))

        history.Clear()

        for message in planned do
            history.Add(message)

    /// Resolves the summariser model: the configured override when set,
    /// otherwise the session's model. Raises ArgumentException on an
    /// invalid override; the caller maps it to a failure-continue.
    let private resolveCompactionModel
        (sessionModel: ModelReference)
        (compactionModel: string | null)
        : ModelReference =
        match Option.ofObj compactionModel with
        | Some raw when not (String.IsNullOrWhiteSpace raw) ->
            let mutable parsed = Unchecked.defaultof<ModelReference>

            if ModelReference.TryParse(raw, &parsed) then
                parsed
            else
                raise (
                    ArgumentException(
                        $"The compaction model '%s{raw}' is not a valid model reference in provider/model form.",
                        nameof compactionModel
                    )
                )
        | _ -> sessionModel

    /// Maps a summariser exception to its client-safe failure reason:
    /// message only, never secrets or arguments; an empty message falls
    /// back to the type name. Mirrors the TurnLoop tool-error mapping.
    let private summarizeErrorToString (ex: Exception) : string =
        let message =
            if isNull ex || String.IsNullOrEmpty ex.Message then
                ex.GetType().Name
            else
                ex.Message

        "Compaction failed: " + message

    /// Runs the summariser call: the history plus the summarise instruction
    /// through the turn's client in one non-streaming call. Cancellation
    /// propagates; any other failure raises InvalidOperationException with
    /// the client-safe reason.
    let private summarizeAsync
        (client: IChatClient)
        (history: IList<ChatMessage>)
        (cancellationToken: CancellationToken)
        : Task<string * int64 * int64> =
        ArgumentNullException.ThrowIfNull(client)
        ArgumentNullException.ThrowIfNull(history)

        task {
            let request = ResizeArray<ChatMessage>(history)
            request.Add(ChatMessage(ChatRole.User, SummarizeInstruction))

            let! response = client.GetResponseAsync(request :> IList<ChatMessage>, ChatOptions(), cancellationToken)

            if isNull (box response) then
                return raise (InvalidOperationException("The compaction summarizer returned no response."))
            else
                let summary = if isNull response.Text then "" else response.Text

                if String.IsNullOrWhiteSpace summary then
                    return raise (InvalidOperationException("The compaction summarizer returned an empty summary."))
                else
                    let mutable inputTokens = 0L
                    let mutable outputTokens = 0L

                    if not (isNull response.Usage) then
                        if response.Usage.InputTokenCount.HasValue then
                            inputTokens <- inputTokens + response.Usage.InputTokenCount.Value

                        if response.Usage.OutputTokenCount.HasValue then
                            outputTokens <- outputTokens + response.Usage.OutputTokenCount.Value

                    return (summary, inputTokens, outputTokens)
        }

    /// Journals one compaction event under the last-moment claim fence: a
    /// fenced-out claim raises TurnLeaseLostException before the store is
    /// touched, and a store-side stale-token rejection raises the same, so
    /// the takeover loser journals nothing. A failed write is best-effort:
    /// compaction is auxiliary, so the turn continues either way.
    let private journalOrFence
        (journalAsync: SessionEvent -> Task<JournalWriter.JournalWriteResult>)
        (isLeaseValid: unit -> bool)
        (event: SessionEvent)
        : Task<unit> =
        if isNull (box journalAsync) then
            raise (ArgumentNullException(nameof journalAsync))

        if isNull (box isLeaseValid) then
            raise (ArgumentNullException(nameof isLeaseValid))

        if isNull (box event) then
            raise (ArgumentNullException(nameof event))

        task {
            if not (isLeaseValid ()) then
                raise (TurnLoop.TurnLeaseLostException())

            let! write = journalAsync event

            match write with
            | JournalWriter.JournalAppended _ -> ()
            | JournalWriter.JournalFailed _ -> ()
            | JournalWriter.JournalRejected _ -> raise (TurnLoop.TurnLeaseLostException())
        }

    /// Reports one summariser usage checkpoint with the turn-cumulative
    /// totals. Guarded: a throwing observer never kills the turn.
    let private reportCheckpoint
        (observer: IUsageObserver | null)
        (tenant: TenantId)
        (sessionId: SessionId)
        (turnId: TurnId)
        (attempt: int)
        (reference: ModelReference)
        (inputTokens: int64)
        (outputTokens: int64)
        : unit =
        if not (isNull (box observer)) then
            match box observer with
            | :? IUsageObserver as live ->
                try
                    live.OnCheckpoint
                        {
                            Tenant = tenant
                            SessionId = sessionId
                            TurnId = turnId
                            Attempt = attempt
                            Provider = reference.Provider
                            Model = reference.Model
                            InputTokens = inputTokens
                            OutputTokens = outputTokens
                            IdempotencyKey = Guid.NewGuid().ToString("N")
                        }
                with _ ->
                    ()
            | _ -> ()

    /// Journals the failure event and continues uncompacted with the
    /// unchanged totals.
    let private journalFailureAsync (request: CompactionRequest) (reason: string) : Task<CompactionOutcome> =
        task {
            let safe =
                if String.IsNullOrWhiteSpace reason then
                    "Compaction failed without a reason."
                else
                    reason

            let failure =
                CompactionFailedEvent(request.SessionId, request.TurnId, Nullable<int64>(), DateTimeOffset.UtcNow, safe)
                :> SessionEvent

            do! journalOrFence request.JournalAsync request.IsLeaseValid failure

            return FailedContinue(safe, request.InputTokens, request.OutputTokens)
        }

    /// Attempts one compaction pass: at most one summariser call and one
    /// rewrite per call (the single-pass-per-boundary guard). Cancellation
    /// and lease loss propagate; every other failure journals a
    /// CompactionFailedEvent and continues uncompacted.
    /// <param name="request">What the attempt needs. Its client, history, journal sink, and lease hook must not be null.</param>
    /// <returns>What the attempt decided.</returns>
    let tryCompactAsync (request: CompactionRequest) : Task<CompactionOutcome> =
        if isNull (box request.Client) then
            raise (ArgumentNullException(nameof request))

        if isNull (box request.History) then
            raise (ArgumentNullException(nameof request))

        if isNull (box request.JournalAsync) then
            raise (ArgumentNullException(nameof request))

        if isNull (box request.IsLeaseValid) then
            raise (ArgumentNullException(nameof request))

        task {
            let beforeEstimate =
                estimateHistory request.History request.SessionId request.TurnId

            let threshold =
                ContextPruning.PruneThreshold(request.CatalogEntry, request.ReservedBufferTokens)

            if
                beforeEstimate <= int64 threshold
                || not (wouldReplace request.History request.KeepMessages)
            then
                return NotNeeded
            else
                // Resolve the model and the policy gate synchronously: a
                // bad override or a deny is a failure-continue, never a
                // turn fault.
                let mutable compactionRef = request.SessionModel
                let mutable failure: string | null = null

                try
                    compactionRef <- resolveCompactionModel request.SessionModel request.CompactionModel

                    match box request.ModelPolicy with
                    | null -> ()
                    | :? IModelPolicy as policy ->
                        let decision =
                            policy.Authorize(request.Tenant, compactionRef.Provider, compactionRef.Model)

                        if isNull (box decision) then
                            failure <- "The model policy returned null instead of a decision."
                        else
                            match decision with
                            | :? ModelAllowed -> ()
                            | :? ModelDenied as denied ->
                                failure <-
                                    if isNull denied.Message then
                                        "The compaction model was denied by the model policy."
                                    else
                                        denied.Message
                            | _ ->
                                failure <-
                                    $"The model policy returned an unknown decision: %s{decision.GetType().FullName}."
                    | _ -> failure <- "The model policy has an unknown shape."
                with :? ArgumentException as invalid ->
                    failure <- invalid.Message

                if not (isNull (box failure)) then
                    match failure with
                    | null -> return FailedContinue("Compaction failed without a reason.", 0L, 0L)
                    | reason -> return! journalFailureAsync request reason
                else
                    let! summaryResult =
                        task {
                            try
                                let! summary = summarizeAsync request.Client request.History request.CancellationToken
                                return Ok summary
                            with
                            | :? OperationCanceledException as canceled ->
                                return! Task.FromException<Result<string * int64 * int64, string>>(canceled)
                            | ex -> return Error(summarizeErrorToString ex)
                        }

                    match summaryResult with
                    | Error reason -> return! journalFailureAsync request reason
                    | Ok(summary, summaryInput, summaryOutput) ->
                        match planRewrite request.History summary request.KeepMessages with
                        | None -> return NotNeeded
                        | Some planned ->
                            let afterEstimate =
                                estimateHistory
                                    (ResizeArray<ChatMessage>(planned) :> IList<ChatMessage>)
                                    request.SessionId
                                    request.TurnId

                            let compacted =
                                CompactedEvent(
                                    request.SessionId,
                                    request.TurnId,
                                    Nullable<int64>(),
                                    DateTimeOffset.UtcNow,
                                    beforeEstimate,
                                    afterEstimate
                                )
                                :> SessionEvent

                            do! journalOrFence request.JournalAsync request.IsLeaseValid compacted

                            applyRewrite request.History planned

                            let totalInput = request.InputTokens + summaryInput
                            let totalOutput = request.OutputTokens + summaryOutput

                            reportCheckpoint
                                request.Observer
                                request.Tenant
                                request.SessionId
                                request.TurnId
                                request.Attempt
                                compactionRef
                                totalInput
                                totalOutput

                            return Compacted(beforeEstimate, afterEstimate, totalInput, totalOutput)
        }

    /// Adapts one attempt's fixed dependencies to the TurnLoop boundary
    /// shape: the boundary supplies the history, usage totals, and
    /// cancellation token per iteration and continues with the updated
    /// totals. Internal-callable so issue 46 reuses it for
    /// ILegateClient.Compact.
    /// <param name="deps">What the turn's attempts need. Its client, journal sink, and lease hook must not be null.</param>
    /// <returns>The boundary hook.</returns>
    let createHook (deps: CompactionHookDeps) : TurnLoop.CompactionHook =
        if isNull (box deps.Client) then
            raise (ArgumentNullException(nameof deps))

        if isNull (box deps.JournalAsync) then
            raise (ArgumentNullException(nameof deps))

        if isNull (box deps.IsLeaseValid) then
            raise (ArgumentNullException(nameof deps))

        fun history inputTokens outputTokens cancellationToken ->
            task {
                let request: CompactionRequest =
                    {
                        Client = deps.Client
                        History = history
                        SessionModel = deps.SessionModel
                        CompactionModel = deps.CompactionModel
                        KeepMessages = deps.KeepMessages
                        CatalogEntry = deps.CatalogEntry
                        ReservedBufferTokens = deps.ReservedBufferTokens
                        Observer = deps.Observer
                        ModelPolicy = deps.ModelPolicy
                        Tenant = deps.Tenant
                        SessionId = deps.SessionId
                        TurnId = deps.TurnId
                        Attempt = deps.Attempt
                        InputTokens = inputTokens
                        OutputTokens = outputTokens
                        JournalAsync = deps.JournalAsync
                        IsLeaseValid = deps.IsLeaseValid
                        CancellationToken = cancellationToken
                    }

                let! outcome = tryCompactAsync request

                match outcome with
                | NotNeeded -> return (inputTokens, outputTokens)
                | Compacted(_, _, totalInput, totalOutput) -> return (totalInput, totalOutput)
                | FailedContinue(_, totalInput, totalOutput) -> return (totalInput, totalOutput)
            }
