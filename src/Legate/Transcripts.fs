// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.AI

// Transcript replay (issue 50, incremental derivation in issue 388): the
// bounded paging loop over ISessionEventStore.Replay feeding the incremental
// derivation below. Appends stay on JournalWriter as the sole write path;
// this module only reads. Outcome handling follows the store contract:
// unknown session, expired journal, and end of stream are settled tails, so
// the read returns the cells derived from the events it saw (none on a
// fresh unknown or expired journal) instead of throwing. Callers needing a
// control-plane precondition (an unknown-session failure) enforce it
// before calling. Any other outcome (a null or unrecognized shape) fails
// explicitly: a partial journal is never advertised as complete.

/// Pages one session's event journal into transcript cells.
module internal Transcripts =

    // ────────────────── Incremental derivation ──────────────────
    //
    // Issue 388: the bounded incremental derivation state the paging loop
    // feeds. TurnFeed mirrors SessionCellDeriver.Fold event by event (the
    // pending text run, the open call names and outputs, the iteration
    // counters, the start/completion dedupe): Fold stays the single
    // normative rule set in Cells.fs and is untouched here, and parity
    // tests prove the feeder equivalent across chunkings. IncrementalReader
    // adds the session-level linkage Fold does not own: the first-start-wins
    // tool-call starter map and the first-seen turn order, mirroring
    // TranscriptReader's three passes. Raw events are discarded after
    // feeding: retained state is the derived output cells (inherent
    // output-proportional cost) plus small per-turn pending buffers, the
    // starter map, the turn order, and the per-turn observed call ids used
    // once at the end for sub-agent gating. Sub-agent gating and artifact
    // enrichment apply once at the end over derived cells only, preserving
    // the current equivalence. Open text runs, uncompleted tool outputs,
    // and interleaved turns keep correlation state proportional to that
    // open content: no constant total memory or constant-time full read is
    // promised (see the cost contract on TranscriptReader).

    /// One turn's incremental fold state, mirroring
    /// <see cref="T:Legate.SessionCellDeriver" />. Feed carries one
    /// journaled event in journal order; FinishTurn flushes the open text
    /// run and returns the turn's cells. The null-message transcript path
    /// is the only path: the initial user message is not journaled, so no
    /// user cell derives here beyond folded UserMessageEvents.
    type TurnFeed(sessionId: SessionId, turnId: TurnId) =

        let cells = ResizeArray<SessionCell>()

        // The assistant text run being accumulated: flushed into one
        // Assistant cell when any non-delta event arrives or the turn
        // finishes, so a run is contiguous TextDeltaEvents only.
        let mutable pendingRun: (Text.StringBuilder * DateTimeOffset) option = None

        // The pending tool call is tracked per open call id, so
        // interleaved calls (parallel tool use) each keep their own name.
        let openCalls = Dictionary<string, string>()

        // Output fragments accumulate per open call id, so interleaved
        // calls each keep their own result text.
        let openOutputs = Dictionary<string, Text.StringBuilder>()

        // Dedupe: repeated continuations or reports must not create distinct
        // duplicate logical entries, so a repeated start or completion for
        // one call id derives once (the first wins).
        let seenStarts = HashSet<string>(StringComparer.Ordinal)
        let seenCompletions = HashSet<string>(StringComparer.Ordinal)

        // Advances the iteration at an iteration boundary: a completed
        // tool call followed by new model activity.
        let mutable iteration = 1
        let mutable completedCalls = 0

        let addCell
            (kind: SessionCellKind)
            (content: string | null)
            (toolName: string | null)
            (toolCallId: string | null)
            (isError: bool)
            (cellIteration: int)
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
                    Iteration = cellIteration

                    // Null only when metadata is None: the match above
                    // proves it to the nullness checker.
                    Metadata = metadataOrNull

                    Artifacts = null
                    Timestamp = timestamp
                }

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

        /// Feeds one journaled event for this turn, in journal order.
        /// Only this turn's events derive here: events from other turns
        /// are ignored, exactly like the fold.
        /// <param name="ev">The event to feed. Must not be null.</param>
        /// <exception cref="T:System.ArgumentNullException">The event is null.</exception>
        member _.Feed(ev: SessionEvent) : unit =
            if isNull (box ev) then
                raise (ArgumentNullException(nameof ev))

            if ev.TurnId = turnId then
                match ev with
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
                    // empty: arguments are journaled, never streamed. A
                    // repeated start for one call id derives nothing more:
                    // the first logical entry wins.
                    flushRun ()
                    advanceIteration ()

                    if not (isNull (box started.ToolCallId)) && seenStarts.Contains(started.ToolCallId) then
                        ()
                    else
                        if not (isNull (box started.ToolCallId)) then
                            seenStarts.Add(started.ToolCallId) |> ignore

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
                    // is the completion event's. A repeated completion for
                    // one call id derives nothing more.
                    flushRun ()

                    if
                        not (isNull (box completed.ToolCallId))
                        && seenCompletions.Contains(completed.ToolCallId)
                    then
                        ()
                    elif openOutputs.ContainsKey(completed.ToolCallId) then
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

                        if not (isNull (box completed.ToolCallId)) then
                            seenCompletions.Add(completed.ToolCallId) |> ignore

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
                    // Rule 6: one system cell carrying the failure reason,
                    // isError true.
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
                    // estimates.
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
                    // reason, isError true like the turn-failure remark.
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

                | :? SkillInvalidEvent as invalid ->
                    // Rule 9: one system cell carrying the discovery skip,
                    // isError true. Metadata carries the discriminator plus
                    // the skill name, so hosts can filter on either.
                    flushRun ()

                    let metadata = Dictionary<string, string>()
                    metadata["event"] <- "skillInvalid"
                    metadata["skillName"] <- invalid.SkillName

                    addCell
                        SessionCellKind.System
                        invalid.Reason
                        null
                        null
                        true
                        iteration
                        (Some(metadata :> IReadOnlyDictionary<string, string>))
                        invalid.Timestamp

                | :? AgentInvalidEvent as invalid ->
                    // Rule 10: one system cell carrying the kept agent's
                    // flag, mirroring the skill-skip rule.
                    flushRun ()

                    let metadata = Dictionary<string, string>()
                    metadata["event"] <- "agentInvalid"
                    metadata["agentName"] <- invalid.AgentName

                    addCell
                        SessionCellKind.System
                        invalid.Reason
                        null
                        null
                        true
                        iteration
                        (Some(metadata :> IReadOnlyDictionary<string, string>))
                        invalid.Timestamp

                | :? AgentSwitchedEvent as switched ->
                    // Rule 11: one system cell carrying the agent switch
                    // audit: the previous and new agent.
                    flushRun ()

                    let metadata = Dictionary<string, string>()
                    metadata["event"] <- "agentSwitched"
                    metadata["previousAgentId"] <- switched.PreviousAgentId.ToString()
                    metadata["newAgentId"] <- switched.NewAgentId.ToString()

                    addCell
                        SessionCellKind.System
                        ($"switched agent {switched.PreviousAgentId} -> {switched.NewAgentId}")
                        null
                        null
                        false
                        iteration
                        (Some(metadata :> IReadOnlyDictionary<string, string>))
                        switched.Timestamp

                | _ ->
                    // Progress markers (turnStarted, usage, compacted,
                    // turnCompleted, turnAborted, sessionClosed,
                    // skillLoaded) produce no cells; hosts read them from
                    // the event stream. They do not break an open text
                    // run: a usage checkpoint mid-run is not model output,
                    // so the run continues after it.
                    ()

        /// Finishes the turn: flushes the open text run, if any, and
        /// returns the turn's derived cells, in transcript order.
        /// Uncompleted tool calls keep no result cell (the abort case):
        /// their pending buffers are dropped here.
        member _.FinishTurn() : IReadOnlyList<SessionCell> =
            flushRun ()
            cells :> IReadOnlyList<SessionCell>

    /// The bounded incremental derivation over one session's journal:
    /// feed every replayed event in sequence order, then finish once for
    /// the gated transcript. Pure except for the mutable derivation
    /// state it holds; performs no I/O. Mirrors
    /// <see cref="T:Legate.TranscriptReader" />: the first-start-wins
    /// tool-call starter map, the first-seen turn order, and the end
    /// sub-agent gate are the same three passes, evaluated incrementally
    /// per event and once at the end.
    type IncrementalReader(sessionId: SessionId) =

        // Pass 1 state: every tool-call id attributed to the turn of its
        // first ToolCallStarted, in feed order. First start wins: the turn
        // that opened the call owns it, and any later turn carrying the
        // id links back to that parent. Ids with no observed start stay
        // unattributed and are never gated.
        let starters = Dictionary<string, TurnId>(StringComparer.Ordinal)

        // Pass 2 state: per-turn feeds in first-seen order, so multi-turn
        // transcripts concatenate in journal order.
        let order = ResizeArray<TurnId>()
        let feeds = Dictionary<TurnId, TurnFeed>()

        // Pass 3 state: the tool-call ids each turn carried, evaluated
        // once at the end against the final starter map.
        let observed = Dictionary<TurnId, HashSet<string>>()

        let mutable eventsFed = 0

        // The ToolCallId one tool event carries, or null for event kinds
        // without one. Mirrors the reader's guarded downcasts: only the
        // three tool-call event kinds carry the linkage.
        let toolCallIdOf (ev: SessionEvent) : string | null =
            match ev with
            | :? ToolCallStartedEvent as started when not (isNull (box started)) -> started.ToolCallId
            | :? ToolCallOutputEvent as output when not (isNull (box output)) -> output.ToolCallId
            | :? ToolCallCompletedEvent as completed when not (isNull (box completed)) -> completed.ToolCallId
            | _ -> null

        /// How many events were fed: exactly the replayed event count on
        /// a complete read. Each event is processed once no matter how
        /// the journal pages: there is no per-page prefix refolding.
        member _.EventsFed = eventsFed

        /// How many distinct turns were observed, in first-seen order.
        member _.TurnCount = order.Count

        /// Feeds one replayed event into the derivation, in sequence
        /// order. The event is discarded after feeding: only derived
        /// cells and the small linkage buffers are retained.
        /// <param name="ev">The event to feed. Must not be null.</param>
        /// <exception cref="T:System.ArgumentNullException">The event is null.</exception>
        member _.Feed(ev: SessionEvent) : unit =
            if isNull (box ev) then
                raise (ArgumentNullException(nameof ev))

            eventsFed <- eventsFed + 1

            match ev with
            | :? ToolCallStartedEvent as started when not (isNull (box started)) ->
                if
                    not (isNull (box started.ToolCallId))
                    && not (starters.ContainsKey(started.ToolCallId))
                then
                    starters[started.ToolCallId] <- started.TurnId
            | _ -> ()

            let feed =
                match feeds.TryGetValue(ev.TurnId) with
                | true, existing -> existing
                | false, _ ->
                    order.Add(ev.TurnId)

                    let created = TurnFeed(sessionId, ev.TurnId)
                    feeds[ev.TurnId] <- created
                    observed[ev.TurnId] <- HashSet<string>(StringComparer.Ordinal)
                    created

            // Null only on event kinds without a ToolCallId: only
            // non-null ids participate in the linkage.
            match toolCallIdOf ev with
            | null -> ()
            | id -> observed[ev.TurnId].Add(id) |> ignore

            feed.Feed(ev)

        /// Finishes the derivation: flushes every turn and concatenates
        /// the ungated turns' cells in first-seen order. A turn is gated
        /// exactly like the reader: it carries a tool-call id first
        /// started in another turn, and the options exclude sub-agent
        /// cells. The parent's own call cells stay: the id's first start
        /// is the parent turn itself, so the parent never links to
        /// itself.
        /// <param name="options">The read knobs. Must not be null.</param>
        /// <returns>The derived cells, in transcript order.</returns>
        /// <exception cref="T:System.ArgumentNullException">The options are null.</exception>
        member _.Finish(options: ReadTranscriptOptions) : IReadOnlyList<SessionCell> =
            if isNull (box options) then
                raise (ArgumentNullException(nameof options))

            let cells = ResizeArray<SessionCell>()

            for turnId in order do
                let turnCells = feeds[turnId].FinishTurn()

                let mutable linked = false

                for id in observed[turnId] do
                    if not linked then
                        match starters.TryGetValue(id) with
                        | true, starter when starter <> turnId -> linked <- true
                        | _ -> ()

                if not (linked && not options.IncludeSubAgentCells) then
                    cells.AddRange(turnCells)

            cells :> IReadOnlyList<SessionCell>

    /// Reads one session's transcript: streams the journal through
    /// bounded pages into the incremental derivation, then enriches
    /// ToolResult cells with their artifact references, so the result is
    /// identical for every page size (chunking invariance is pinned by
    /// test, not by code: pages only transport, the derivation folds the
    /// sequence). Raw events are discarded after feeding: no second
    /// complete raw journal is retained beside the output.
    ///
    /// Cursor hardening: a nonempty page whose continuation cursor does
    /// not advance (repeated, non-advancing, or regressing) fails
    /// explicitly instead of replaying indefinitely or duplicating
    /// output; an empty page carrying a continuation settles the read
    /// without looping. Cancellation is observed between the bounded
    /// fetch and derivation units, including after the last fetch, and a
    /// cancelled read raises instead of advertising complete output.
    /// <param name="eventStore">The journal to replay.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session whose transcript to read.</param>
    /// <param name="options">The read knobs.</param>
    /// <param name="pageSize">The replay page size. Must be positive.</param>
    /// <param name="cancellationToken">Token that abandons the replay.</param>
    /// <returns>The derived cells, in transcript order.</returns>
    let readTranscript
        (eventStore: ISessionEventStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (options: ReadTranscriptOptions)
        (pageSize: int)
        (cancellationToken: CancellationToken)
        : Task<IReadOnlyList<SessionCell>> =
        if isNull (box eventStore) then
            raise (ArgumentNullException(nameof eventStore))

        if isNull (box options) then
            raise (ArgumentNullException(nameof options))

        if pageSize <= 0 then
            raise (ArgumentOutOfRangeException(nameof pageSize, "PageSize must be positive."))

        task {
            let derivation = IncrementalReader(sessionId)
            let mutable cursor = 0L
            let mutable paging = true

            while paging do
                cancellationToken.ThrowIfCancellationRequested()

                let! outcome = eventStore.Replay(tenant, sessionId, cursor, pageSize, cancellationToken)

                if isNull (box outcome) then
                    raise (InvalidOperationException("The event store returned a null replay outcome."))
                else
                    match outcome with
                    | :? EventReplayPage as page when not (isNull (box page)) ->
                        cancellationToken.ThrowIfCancellationRequested()

                        let mutable eventCount = 0

                        if not (isNull (box page.Events)) then
                            for ev in page.Events do
                                if not (isNull (box ev)) then
                                    eventCount <- eventCount + 1

                        if page.NextCursor.HasValue then
                            if eventCount = 0 then
                                // An empty page resolves to the asked-for
                                // cursor, so paging on would never advance:
                                // settle the read without looping.
                                paging <- false
                            elif page.NextCursor.Value <= cursor then
                                // A nonempty page must advance the cursor:
                                // repeating or regressing it would replay
                                // indefinitely or duplicate output, so fail
                                // explicitly instead.
                                raise (
                                    InvalidOperationException(
                                        $"The event store returned a non-advancing replay cursor for session %O{sessionId}: cursor %d{cursor} did not advance to %d{page.NextCursor.Value}."
                                    )
                                )
                            else
                                cursor <- page.NextCursor.Value

                                for ev in page.Events do
                                    if not (isNull (box ev)) then
                                        derivation.Feed(ev)

                                cancellationToken.ThrowIfCancellationRequested()
                        else
                            if not (isNull (box page.Events)) then
                                for ev in page.Events do
                                    if not (isNull (box ev)) then
                                        derivation.Feed(ev)

                            paging <- false
                            cancellationToken.ThrowIfCancellationRequested()
                    | :? EventReplayEndOfStream -> paging <- false
                    | :? EventReplayUnknownSession -> paging <- false
                    | :? EventReplayJournalExpired -> paging <- false
                    | _ -> raise (InvalidOperationException("The event store returned an unknown replay outcome."))

            // Observe cancellation after the last fetch too: a cancel
            // landing on the terminal page must not read back as a
            // complete transcript.
            cancellationToken.ThrowIfCancellationRequested()

            return derivation.Finish(options) |> SessionArtifactEnrichment.enrichCells
        }
