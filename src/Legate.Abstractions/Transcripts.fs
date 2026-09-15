// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Generic

// Transcript-read contracts (issue 50). SessionCell is the coarse
// transcript hosts list; SessionCellDeriver.Fold is the single normative
// pure fold deriving one turn's cells from its journaled user message and
// ordered events. This file adds the session-level read over that fold:
// ReadTranscriptOptions carries the sub-agent include/exclude knob, and
// TranscriptReader is the pure replayed-events-to-cells read the runtime's
// paging loop feeds. The fold itself is untouched: derivation stays in
// Cells.fs, stores keep events only, and no event field is added. The
// sub-agent linkage reused here is the documented event-layer one: every
// event raised inside a sub-agent turn carries the parent call's id on its
// ToolCallId (Events.fs), so the reader attributes each tool-call id to
// the turn of its first ToolCallStarted and treats any other turn
// carrying that id as the sub-agent turn it spawned.

/// Options for the <c>ReadTranscript</c> reader: whether the transcript
/// keeps the cells derived from sub-agent turns. A plain class with
/// mutable properties and defaults, mirroring
/// <see cref="T:Legate.ContextPruningOptions" />. A single boolean has
/// no invalid state, so there is no <c>Validate</c> method.
[<Sealed>]
type ReadTranscriptOptions() =

    /// Whether cells derived from sub-agent turns stay in the transcript.
    /// A sub-agent turn is a turn carrying a tool-call id first started in
    /// another turn (the documented parent linkage). Default true: the
    /// transcript keeps full fidelity, and the sub-agent tool cells carry
    /// the parent call's id. Hosts building a flat UI set this to false
    /// to drop every cell folded from sub-agent turns while keeping the
    /// parent turn's own tool call and result cells.
    member val IncludeSubAgentCells: bool = true with get, set

/// The pure session-transcript read behind <c>ReadTranscript</c>
/// (ARCHITECTURE.md, Reading): derives one session's transcript cells
/// from its replayed journal events by folding each turn through
/// <see cref="T:Legate.SessionCellDeriver" /> and gating sub-agent turns
/// behind <see cref="T:Legate.ReadTranscriptOptions" />. Every method is
/// pure: it reads the inputs, returns fresh cells, and performs no I/O.
/// <see cref="T:Legate.SessionCellDeriver" /> remains the only transcript
/// fold; this reader groups and gates, never re-derives.
type TranscriptReader() =

    /// Derives the transcript cells from one session's replayed journal
    /// events, in transcript order: turns concatenate in first-seen order
    /// and each turn's cells follow
    /// <see cref="M:Legate.SessionCellDeriver.Fold" /> order. The initial
    /// user message of a turn is not journaled (only folded Inject
    /// messages arrive as <see cref="T:Legate.UserMessageEvent" />), so
    /// every turn folds with a null user message: User cells derive from
    /// UserMessageEvents alone.
    /// <param name="events">One session's journaled events in sequence order. Must not be null and must not contain null.</param>
    /// <param name="options">The read knobs. Must not be null.</param>
    /// <returns>The derived cells, in transcript order; empty when the journal is empty.</returns>
    /// <exception cref="T:System.ArgumentNullException">The event list or the options are null, or an event is null.</exception>
    static member Read
        (events: IReadOnlyList<SessionEvent>, options: ReadTranscriptOptions)
        : IReadOnlyList<SessionCell> =
        if isNull (box events) then
            raise (ArgumentNullException(nameof events))

        if isNull (box options) then
            raise (ArgumentNullException(nameof options))

        for event in events do
            if isNull (box event) then
                raise (ArgumentNullException(nameof events))

        // The ToolCallId one tool event carries, or null for event kinds
        // without one. Mirrors the fold's guarded downcasts: only the
        // three tool-call event kinds carry the linkage.
        let toolCallIdOf (event: SessionEvent) : string | null =
            match event with
            | :? ToolCallStartedEvent as started when not (isNull (box started)) -> started.ToolCallId
            | :? ToolCallOutputEvent as output when not (isNull (box output)) -> output.ToolCallId
            | :? ToolCallCompletedEvent as completed when not (isNull (box completed)) -> completed.ToolCallId
            | _ -> null

        // Pass 1: attribute every tool-call id to the turn of its first
        // ToolCallStarted, in journal order. First start wins: the turn
        // that opened the call owns it, and any later turn carrying the
        // id (the sub-agent execution markers the event docs describe, or
        // a resumed journal reusing the id) links back to that parent.
        // Ids with no observed start (a resumed or retained journal that
        // lost the start) stay unattributed and are never gated: the read
        // keeps cells without positive linkage evidence.
        let starters = Dictionary<string, TurnId>(StringComparer.Ordinal)

        for event in events do
            match event with
            | :? ToolCallStartedEvent as started when not (isNull (box started)) ->
                if
                    not (isNull (box started.ToolCallId))
                    && not (starters.ContainsKey(started.ToolCallId))
                then
                    starters[started.ToolCallId] <- started.TurnId
            | _ -> ()

        // Pass 2: group events by turn in first-seen order, so multi-turn
        // transcripts concatenate in journal order.
        let order = ResizeArray<TurnId>()
        let turns = Dictionary<TurnId, ResizeArray<SessionEvent>>()

        for event in events do
            match turns.TryGetValue(event.TurnId) with
            | true, group -> group.Add(event)
            | false, _ ->
                order.Add(event.TurnId)

                let group = ResizeArray<SessionEvent>()
                group.Add(event)
                turns[event.TurnId] <- group

        // Pass 3: a turn is a sub-agent turn when one of its events
        // carries a tool-call id first started in another turn. The
        // parent's own call cells stay: the id's first start is the
        // parent turn itself, so the parent never links to itself.
        let isSubAgentTurn (turnId: TurnId) (group: ResizeArray<SessionEvent>) =
            let mutable linked = false

            for event in group do
                if not linked then
                    // Null only on event kinds without a ToolCallId: the
                    // match proves the non-null key to the checker.
                    match toolCallIdOf event with
                    | null -> ()
                    | id ->
                        match starters.TryGetValue(id) with
                        | true, starter when starter <> turnId -> linked <- true
                        | _ -> ()

            linked

        let cells = ResizeArray<SessionCell>()

        for turnId in order do
            let group = turns[turnId]

            let gated = isSubAgentTurn turnId group && not options.IncludeSubAgentCells

            if not gated then
                // The initial user message is not journaled, so every
                // turn folds with a null message: the timestamp argument
                // is unused on that path and carries the turn's first
                // event timestamp.
                let first = group[0]

                let folded =
                    SessionCellDeriver.Fold(
                        first.SessionId,
                        turnId,
                        null,
                        first.Timestamp,
                        group :> IReadOnlyList<SessionEvent>
                    )

                cells.AddRange(folded)

        cells :> IReadOnlyList<SessionCell>
