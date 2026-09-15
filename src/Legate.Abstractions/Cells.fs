// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Generic
open System.Text.Json
open System.Text.Json.Serialization
open Microsoft.Extensions.AI

// Session cell contracts. SessionCell is the coarse transcript record hosts
// and stores list and export: cells derive from the fine-grained session
// event stream rather than being journaled themselves. The derivation is
// one shared pure fold (SessionCellDeriver.Fold), following the BlobKeys
// precedent: one shared helper so the journal writer, store, and export
// cannot drift, and conformance tests share one implementation. Cells
// serialise with System.Text.Json exactly like the rest of the contract
// surface (PascalCase properties, numeric enums), and SessionCellJson is
// the single JSONL helper for line-oriented export.

/// The coarse kind of one transcript cell of a session turn.
type SessionCellKind =

    /// The user's prompt that started the turn, one cell per message.
    | User = 0

    /// Accumulated assistant text from one contiguous delta run.
    | Assistant = 1

    /// A tool call began; its arguments are journaled, never streamed, so
    /// content is empty until the runtime enriches the cell.
    | ToolCall = 2

    /// A tool call settled: accumulated output fragments, or the error
    /// reason when the call failed with no output.
    | ToolResult = 3

    /// An out-of-band system remark surfaced from an event: a permission
    /// request or decision, a question or answer, or a turn failure.
    | System = 4

/// One coarse transcript record of a session: what hosts list and what
/// stores persist and export. Cells are derived from the turn's journaled
/// user message and ordered events by <see cref="T:Legate.SessionCellDeriver" />,
/// which documents and pins the rules this section states:
/// <list type="table">
/// <item><term>User</term><description>one cell per journaled user message: the turn's initial message, plus one per matching-turn UserMessageEvent (a folded Inject message); content joins its TextContent parts with newlines, metadata carries the message's host metadata, iteration 0, timestamp the message's journaled timestamp.</description></item>
/// <item><term>Assistant</term><description>one cell per contiguous run of TextDeltaEvents; content concatenates the deltas; timestamp is the first delta's. Reasoning deltas are transient and produce no cell.</description></item>
/// <item><term>ToolCall</term><description>one cell per ToolCallStartedEvent, emitted immediately so hosts render the call while it runs; toolName and toolCallId set; content is empty because arguments are journaled, never streamed.</description></item>
/// <item><term>ToolResult</term><description>one cell per ToolCallCompletedEvent: content is the call's accumulated ToolCallOutputEvent fragments, or the error reason when the call failed with no output; isError mirrors the completion event's error. A call started but never completed yields no result cell (abort case).</description></item>
/// <item><term>System</term><description>one cell per PermissionRequestedEvent, PermissionResolvedEvent, QuestionAskedEvent, or QuestionAnsweredEvent, one per TurnFailedEvent, one per ContextPrunedEvent, and one per CompactionFailedEvent; content is the tool name, decision, question, answer, failure reason, or prune summary; metadata carries the request or question id (or the pruned count plus the before/after estimates) plus the event discriminator.</description></item>
/// </list>
/// Progress markers (turnStarted, usage, compacted, turnCompleted,
/// turnAborted, sessionClosed) produce no cells; hosts read them from the
/// event stream. Non-tool kinds carry null ToolName and ToolCallId; the
/// fold never sets Artifacts.
/// <param name="id">The cell's id, stamped by the store on persist; the default id while unstamped.</param>
/// <param name="sessionId">The session the cell belongs to.</param>
/// <param name="turnId">The turn the cell was derived in.</param>
/// <param name="kind">The cell's kind.</param>
/// <param name="content">The cell's text content; the empty string when the kind carries none (tool arguments are journaled, never streamed, so a ToolCall cell's content is empty).</param>
/// <param name="toolName">The name of the tool called, or null for non-tool kinds.</param>
/// <param name="toolCallId">The id of the tool call, or null for non-tool kinds.</param>
/// <param name="isError">Whether the cell reports a failure: a failed tool call or a failed turn.</param>
/// <param name="iteration">The ReAct iteration the cell belongs to; 0 for the user message, 1 or higher for everything the model or tools produced.</param>
/// <param name="metadata">Host or derivation metadata, or null when there is none.</param>
/// <param name="artifacts">Artifact blob references produced by the cell, or null when there are none; the fold never sets them, the runtime may enrich at journaling time.</param>
/// <param name="timestamp">When the cell's content was produced.</param>
[<CLIMutable>]
type SessionCell =
    {
        /// The cell's id, stamped by the session store on persist. The
        /// default id means the cell is unstamped.
        Id: CellId

        /// The session the cell belongs to.
        SessionId: SessionId

        /// The turn the cell was derived in.
        TurnId: TurnId

        /// The cell's kind.
        Kind: SessionCellKind

        /// The cell's text content; the empty string when the kind
        /// carries none. Tool arguments are journaled, never streamed,
        /// so a ToolCall cell's content stays empty.
        Content: string | null

        /// The name of the tool called, or null for non-tool kinds.
        ToolName: string | null

        /// The id of the tool call, or null for non-tool kinds.
        ToolCallId: string | null

        /// Whether the cell reports a failure: a failed tool call
        /// (ToolResult) or a failed turn (System).
        IsError: bool

        /// The ReAct iteration the cell belongs to: 0 for the user
        /// message, then one plus the number of iteration boundaries
        /// before it, where a boundary is a completed tool call followed
        /// by new model activity (a text or reasoning delta or a new
        /// tool-call start).
        Iteration: int

        /// Host metadata carried with a user message or derivation
        /// metadata attached to a system cell, or null when there is
        /// none.
        Metadata: IReadOnlyDictionary<string, string> | null

        /// Artifact blob references produced by the cell, or null when
        /// there are none. The fold never sets them; no event kind
        /// carries artifact references, so the field exists for the
        /// runtime to enrich cells at journaling time.
        Artifacts: IReadOnlyList<string> | null

        /// When the cell's content was produced: the message's journaled
        /// timestamp for the user cell, the first delta's timestamp for
        /// an assistant cell, the starting event's timestamp for a tool
        /// or system cell.
        Timestamp: DateTimeOffset
    }

/// The single JSONL helper for transcript export: one line per cell, the
/// line being exactly the cell's System.Text.Json JSON object with default
/// options (PascalCase properties, numeric enum, invariant culture). STJ
/// escapes newlines inside string values, so a cell never spans lines and
/// export stays trivially line-parseable. Shared so the journal writer,
/// store, and export cannot drift.
type SessionCellJson() =

    /// The default serialisation options cells round-trip under: exactly
    /// the options the contract tests use, with the identifier converters
    /// carried on the types and no naming policy.
    static member val Options: JsonSerializerOptions = JsonSerializerOptions() with get, set

    /// Serialises one cell to its single-line JSON representation.
    /// <param name="cell">The cell to serialise. Must not be null.</param>
    /// <returns>The one-line JSON object for the cell.</returns>
    /// <exception cref="T:System.ArgumentNullException">The cell is null.</exception>
    static member Line(cell: SessionCell) : string =
        if isNull (box cell) then
            raise (ArgumentNullException(nameof cell))

        JsonSerializer.Serialize(cell, SessionCellJson.Options)

    /// Serialises cells to a JSONL document: one line per cell, in
    /// transcript order.
    /// <param name="cells">The cells, in transcript order. Must not be null.</param>
    /// <returns>The JSONL document; the empty string for an empty transcript.</returns>
    /// <exception cref="T:System.ArgumentNullException">The cell list is null.</exception>
    static member Document(cells: IReadOnlyList<SessionCell>) : string =
        if isNull (box cells) then
            raise (ArgumentNullException(nameof cells))

        if cells.Count = 0 then
            ""
        else
            let builder = Text.StringBuilder()

            for index in 0 .. cells.Count - 1 do
                if index > 0 then
                    builder.Append '\n' |> ignore

                builder.Append(SessionCellJson.Line cells[index]) |> ignore

            builder.ToString()

    /// Parses one JSONL line back into a cell.
    /// <param name="line">The single-line JSON object for one cell. Must not be null.</param>
    /// <returns>The parsed cell.</returns>
    /// <exception cref="T:System.ArgumentNullException">The line is null.</exception>
    /// <exception cref="T:System.Text.Json.JsonException">The line is not a valid cell.</exception>
    static member Parse(line: string) : SessionCell =
        if isNull (box line) then
            raise (ArgumentNullException(nameof line))

        match JsonSerializer.Deserialize(line, SessionCellJson.Options) with
        | null -> raise (JsonException("A JSONL line must be a JSON object, not null."))
        | cell -> cell

    /// Parses a JSONL document back into its cells, in line order.
    /// <param name="document">The JSONL document. Must not be null.</param>
    /// <returns>The parsed cells, in line order.</returns>
    /// <exception cref="T:System.ArgumentNullException">The document is null.</exception>
    /// <exception cref="T:System.Text.Json.JsonException">A line is not a valid cell.</exception>
    static member ParseDocument(document: string) : IReadOnlyList<SessionCell> =
        if isNull (box document) then
            raise (ArgumentNullException(nameof document))

        let cells = ResizeArray<SessionCell>()

        // The empty document round-trips to no cells: Line/Document emit
        // no lines for an empty transcript, so ParseDocument accepts it
        // back as an empty transcript.
        let mutable start = 0

        while start < document.Length do
            let stop = document.IndexOf('\n', start)

            let lineEnd = if stop = -1 then document.Length else stop

            if lineEnd > start then
                cells.Add(SessionCellJson.Parse(document.Substring(start, lineEnd - start)))

            start <- lineEnd + 1

        cells :> IReadOnlyList<SessionCell>

// ───────────────────────────────────────────────────────────────────────────
// Derivation
//
// One pure whole-turn fold over the ordered journal (one user message plus
// that turn's events), the single normative home for the rules documented
// on SessionCell. The journal writer, store, and export all fold through
// this helper, so implementations cannot drift.

/// The pure derivation rules that fold one turn's journaled user message
/// and ordered events into transcript cells, documented in full on
/// <see cref="T:Legate.SessionCell" />. Every consumer (journal writer,
/// store rehydration, export, conformance tests) folds through
/// <see cref="M:Legate.SessionCellDeriver.Fold" />, so the rules are
/// implemented once and cannot drift. The fold is scoped per turn: events
/// whose turn id differ are ignored, so sub-agent activity derives in its
/// own turn's fold and tool-call ids keep the parent linkage.
type SessionCellDeriver() =

    /// Folds one turn's user message and ordered events into transcript
    /// cells in transcript order. Pure: reads the inputs, returns fresh
    /// cells, no I/O.
    /// <param name="sessionId">The session the turn belongs to; every cell carries it.</param>
    /// <param name="turnId">The turn being folded; events from other turns are ignored.</param>
    /// <param name="userMessage">The turn's journaled user message, or null when the turn had none (a resumed or headless turn, for instance).</param>
    /// <param name="userTimestamp">The journaled timestamp of the user message, used verbatim as the user cell's timestamp.</param>
    /// <param name="events">The turn's events in journaled order. Must not be null.</param>
    /// <returns>The derived cells, in transcript order.</returns>
    /// <exception cref="T:System.ArgumentNullException">The event list is null.</exception>
    static member Fold
        (
            sessionId: SessionId,
            turnId: TurnId,
            userMessage: UserMessage | null,
            userTimestamp: DateTimeOffset,
            events: IReadOnlyList<SessionEvent>
        ) : IReadOnlyList<SessionCell> =
        if isNull (box events) then
            raise (ArgumentNullException(nameof events))

        let cells = ResizeArray<SessionCell>()

        // The metadata and string parameters are nullable exactly where
        // the record fields are; None reads as the null field value so
        // the nullness rule never sees a raw null literal.
        let addCell
            (kind: SessionCellKind)
            (content: string | null)
            (toolName: string | null)
            (toolCallId: string | null)
            (isError: bool)
            (iteration: int)
            (metadata: IReadOnlyDictionary<string, string> option)
            (timestamp: DateTimeOffset)
            : unit =
            let metadataOrNull =
                match metadata with
                | Some value -> value

                // Null only when metadata is None: the nullable record
                // field carries it, and Unchecked.defaultof is the
                // null literal the nullness rule accepts.
                | None -> Unchecked.defaultof<IReadOnlyDictionary<string, string>>

            cells.Add
                {
                    Id = Unchecked.defaultof<CellId>
                    SessionId = sessionId
                    TurnId = turnId
                    Kind = kind
                    Content = content
                    ToolName = toolName
                    ToolCallId = toolCallId
                    IsError = isError
                    Iteration = iteration

                    // Null only when metadata is None: the match above
                    // proves it to the nullness checker.
                    Metadata = metadataOrNull

                    Artifacts = null
                    Timestamp = timestamp
                }

        // Rule 1: the user message is the first cell, iteration 0, stamped
        // with the journaled timestamp and host metadata. Text parts join
        // with newlines; a message with no text parts derives empty
        // content (the non-text parts stay visible in the journal).
        match userMessage with
        | null -> ()
        | message ->
            let builder = Text.StringBuilder()
            let mutable parts = 0

            for part in message.Parts do
                match part with
                | :? TextContent as text ->
                    if parts > 0 then
                        builder.Append '\n' |> ignore

                    builder.Append(text.Text) |> ignore
                    parts <- parts + 1
                | _ -> ()

            let userMetadata =
                match message.Metadata with
                | null -> None
                | value -> Some value

            addCell SessionCellKind.User (builder.ToString()) null null false 0 userMetadata userTimestamp

        // The assistant text run being accumulated: flushed into one
        // Assistant cell when any non-delta event arrives or the events
        // end, so a run is contiguous TextDeltaEvents only.
        let mutable pendingRun: (Text.StringBuilder * DateTimeOffset) option = None

        // The pending tool call is tracked per open call id, so
        // interleaved calls (parallel tool use) each keep their own name;
        // the most recently started call id is remembered for the
        // no-output fallback case.
        let openCalls = Dictionary<string, string>()

        // Output fragments accumulate per open call id, so interleaved
        // calls each keep their own result text.
        let openOutputs = Dictionary<string, Text.StringBuilder>()

        // Advances the iteration at an iteration boundary: a completed
        // tool call followed by new model activity.
        let mutable iteration = 1
        let mutable completedCalls = 0

        let advanceIteration () =
            if completedCalls > 0 then
                iteration <- iteration + 1
                completedCalls <- 0

        // Emits the pending assistant run, if any, before the next cell.
        let flushRun () =
            match pendingRun with
            | Some(builder, firstDelta) ->
                addCell SessionCellKind.Assistant (builder.ToString()) null null false iteration None firstDelta

                pendingRun <- None
            | None -> ()

        for event in events do
            if event.TurnId = turnId then
                match event with
                | :? UserMessageEvent as injected when not (isNull (box injected)) ->
                    // Rule 1b: each injected user message folded into this
                    // turn derives one User cell, iteration 0 with the
                    // message's host metadata and the event's timestamp,
                    // exactly like the initial user cell. Text parts join
                    // with newlines; a message with no text parts derives
                    // empty content. Flushes any open assistant run first so
                    // the cell lands in journaled order.
                    flushRun ()

                    let builder = Text.StringBuilder()
                    let mutable parts = 0

                    let injectedMessage = injected.Message

                    if not (isNull (box injectedMessage)) && not (isNull (box injectedMessage.Parts)) then
                        for part in injectedMessage.Parts do
                            match part with
                            | :? TextContent as text when not (isNull (box text)) ->
                                if parts > 0 then
                                    builder.Append '\n' |> ignore

                                builder.Append(text.Text) |> ignore
                                parts <- parts + 1
                            | _ -> ()

                    let injectedMetadata =
                        if isNull (box injectedMessage) then
                            None
                        else
                            match injectedMessage.Metadata with
                            | null -> None
                            | value -> Some value

                    addCell
                        SessionCellKind.User
                        (builder.ToString())
                        null
                        null
                        false
                        0
                        injectedMetadata
                        injected.Timestamp

                | :? TextDeltaEvent as delta ->
                    // Rule 2: a contiguous text-delta run folds into one
                    // assistant cell, stamped with the first delta's
                    // timestamp. Progress markers between deltas do not
                    // break the run; any other event kind does.
                    match pendingRun with
                    | Some(builder, _) -> builder.Append(delta.Text) |> ignore
                    | None ->
                        advanceIteration ()
                        pendingRun <- Some(Text.StringBuilder(delta.Text), delta.Timestamp)

                | :? ReasoningDeltaEvent ->
                    // Transient, not transcript: reasoning produces no
                    // cell, but it is model activity after a completed
                    // call, so it advances the iteration and ends any
                    // open text run.
                    flushRun ()
                    advanceIteration ()

                | :? ToolCallStartedEvent as started ->
                    // Rule 3: one ToolCall cell, emitted immediately so
                    // hosts render the call while it runs. Content stays
                    // empty: arguments are journaled, never streamed.
                    flushRun ()
                    advanceIteration ()

                    addCell
                        SessionCellKind.ToolCall
                        ""
                        started.ToolName
                        started.ToolCallId
                        false
                        iteration
                        None
                        started.Timestamp

                    openCalls[started.ToolCallId] <- started.ToolName
                    openOutputs[started.ToolCallId] <- Text.StringBuilder()

                | :? ToolCallOutputEvent as output ->
                    // Rule 4a: output fragments accumulate per call id
                    // (concatenated like text deltas), even for calls
                    // whose start was not observed in this fold window
                    // (resumed journals).
                    if openOutputs.ContainsKey(output.ToolCallId) then
                        let builder = openOutputs[output.ToolCallId]
                        builder.Append(output.Output) |> ignore

                | :? ToolCallCompletedEvent as completed ->
                    // Rule 4b: the completion emits the ToolResult cell:
                    // accumulated output, or the error reason when the
                    // call failed with no output. The result's timestamp
                    // is the completion event's.
                    flushRun ()

                    if openOutputs.ContainsKey(completed.ToolCallId) then
                        let builder = openOutputs[completed.ToolCallId]
                        openOutputs.Remove(completed.ToolCallId) |> ignore

                        let isError = not (isNull (box completed.Error))

                        let content =
                            if isError && builder.Length = 0 then
                                // Error is non-null exactly when the call
                                // failed; the match proves it to the
                                // nullness checker.
                                match completed.Error with
                                | null -> builder.ToString()
                                | reason -> reason
                            else
                                builder.ToString()

                        let toolName =
                            // Null only when the completed call's start was
                            // not observed in this fold window (resumed
                            // journals): the ToolResult keeps its call id
                            // and derives its name from the pending call.
                            match openCalls.TryGetValue(completed.ToolCallId) with
                            | true, name -> name
                            | false, _ -> Unchecked.defaultof<string>

                        addCell
                            SessionCellKind.ToolResult
                            content
                            toolName
                            completed.ToolCallId
                            isError
                            iteration
                            None
                            completed.Timestamp

                        openCalls.Remove(completed.ToolCallId) |> ignore
                        completedCalls <- completedCalls + 1

                | :? PermissionRequestedEvent as requested ->
                    // Rule 5: one system cell per permission or question
                    // event; metadata carries the request or question id
                    // plus the discriminator, so hosts can correlate.
                    flushRun ()

                    let metadata = Dictionary<string, string>()
                    metadata["requestId"] <- requested.RequestId
                    metadata["event"] <- "permissionRequested"

                    addCell
                        SessionCellKind.System
                        requested.ToolName
                        null
                        null
                        false
                        iteration
                        (Some(metadata :> IReadOnlyDictionary<string, string>))
                        requested.Timestamp

                | :? PermissionResolvedEvent as resolved ->
                    flushRun ()

                    let metadata = Dictionary<string, string>()
                    metadata["requestId"] <- resolved.RequestId
                    metadata["event"] <- "permissionResolved"
                    metadata["decision"] <- resolved.Decision.ToString()

                    addCell
                        SessionCellKind.System
                        (resolved.Decision.ToString())
                        null
                        null
                        false
                        iteration
                        (Some(metadata :> IReadOnlyDictionary<string, string>))
                        resolved.Timestamp

                | :? QuestionAskedEvent as asked ->
                    flushRun ()

                    let metadata = Dictionary<string, string>()
                    metadata["questionId"] <- asked.QuestionId
                    metadata["event"] <- "questionAsked"

                    addCell
                        SessionCellKind.System
                        asked.Question
                        null
                        null
                        false
                        iteration
                        (Some(metadata :> IReadOnlyDictionary<string, string>))
                        asked.Timestamp

                | :? QuestionAnsweredEvent as answered ->
                    flushRun ()

                    let metadata = Dictionary<string, string>()
                    metadata["questionId"] <- answered.QuestionId
                    metadata["event"] <- "questionAnswered"

                    addCell
                        SessionCellKind.System
                        answered.Answer
                        null
                        null
                        false
                        iteration
                        (Some(metadata :> IReadOnlyDictionary<string, string>))
                        answered.Timestamp

                | :? TurnFailedEvent as failed ->
                    // Rule 6 (extension beyond the issue's three example
                    // mappings, flagged in the ledger): one system cell
                    // carrying the failure reason, isError true.
                    flushRun ()

                    let metadata = Dictionary<string, string>()
                    metadata["event"] <- "turnFailed"

                    addCell
                        SessionCellKind.System
                        failed.Reason
                        null
                        null
                        true
                        iteration
                        (Some(metadata :> IReadOnlyDictionary<string, string>))
                        failed.Timestamp

                | :? ContextPrunedEvent as pruned ->
                    // Rule 7: one system cell carrying the prune audit: a
                    // summary of the replaced count plus the before/after
                    // estimates. Metadata carries the discriminator plus
                    // the counts, so hosts can filter on either, exactly
                    // like the permission and question remarks above.
                    flushRun ()

                    let metadata = Dictionary<string, string>()
                    metadata["event"] <- "contextPruned"
                    metadata["prunedCount"] <- string pruned.PrunedCount
                    metadata["beforeEstimate"] <- string pruned.BeforeEstimate
                    metadata["afterEstimate"] <- string pruned.AfterEstimate

                    addCell
                        SessionCellKind.System
                        ($"pruned {pruned.PrunedCount} tool result(s): estimated tokens {pruned.BeforeEstimate} -> {pruned.AfterEstimate}")
                        null
                        null
                        false
                        iteration
                        (Some(metadata :> IReadOnlyDictionary<string, string>))
                        pruned.Timestamp

                | :? CompactionFailedEvent as failed ->
                    // Rule 8: one system cell carrying the compaction failure
                    // reason, isError true like the turn-failure remark: the
                    // turn continues uncompacted, but hosts surface the miss.
                    flushRun ()

                    let metadata = Dictionary<string, string>()
                    metadata["event"] <- "compactionFailed"

                    addCell
                        SessionCellKind.System
                        failed.Reason
                        null
                        null
                        true
                        iteration
                        (Some(metadata :> IReadOnlyDictionary<string, string>))
                        failed.Timestamp

                | _ ->
                    // Progress markers (turnStarted, usage, compacted,
                    // turnCompleted, turnAborted, sessionClosed) produce
                    // no cells; hosts read them from the event stream.
                    // They do not break an open text run: a usage
                    // checkpoint mid-run is not model output, so the run
                    // continues after it. The event-kind pin test maps
                    // every registered discriminator to its documented
                    // rule, so a future event kind fails the suite here
                    // rather than being silently classified.
                    ()

        flushRun ()

        cells :> IReadOnlyList<SessionCell>
