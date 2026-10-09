// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks

// Checkpoint-resumed bounded journal consumption (issue 389): the shared
// hardened consume-pages helper plus the compacted-base checkpoint the
// context/recovery consumers share. Every consumer replays only the
// post-checkpoint incremental suffix (the last observed CompactedEvent
// sequence, else the last consumed sequence), validated for continuity,
// and feeds the resulting base-plus-suffix shape to the existing #387
// tryRecoverCompacted function and the #388 hardened read pattern. The one
// full read after (re)start with an unknown checkpoint is the documented
// initial reconstruction; a stale checkpoint (a gap between the cached
// cursor and the first suffix sequence) falls back to explicit
// reconstruction, never silent truncation. No ISessionEventStore surface
// change, no migration, no new budget default. Cancellation is observed
// between bounded units; non-advancing cursors fail explicitly.
//
// Retention: the suffix read retains only the last observed CompactedEvent
// plus the post-base suffix events (at most suffix плюс one), never a
// second complete raw journal beside the output. The full scan in the
// initial-reconstruction path still pages the whole journal once, but it
// retains the same bounded shape while scanning.

/// Checkpoint-resumed bounded journal consumption shared by the working
/// context and recovery readers. Internal so no cache or stats type ever
/// crosses the public API.
module internal BoundedReplay =

    /// How one hardened consume finished.
    type ConsumeStats =
        {
            /// The cursor the consume started from (the checkpoint, or 0L).
            StartCursor: int64
            /// How many Replay calls the consume issued.
            PagesFetched: int
            /// How many non-null events the consume fed to the folder.
            EventsSeen: int
            /// The highest cursor observed (the checkpoint to resume from).
            LastCursor: int64
            /// Why the consume settled: endOfStream, unknownSession,
            /// journalExpired, or settledEmptyContinuation.
            EndReason: string
        }

    /// What one suffix read resolved: the cached or newly observed
    /// compacted base plus the post-base suffix, with the consume stats
    /// and whether this read was the documented initial reconstruction or
    /// a stale-checkpoint fallback.
    type SuffixRead =
        {
            /// The compacted base event the suffix builds on, when any.
            BaseEvent: CompactedEvent option
            /// The post-base suffix events in sequence order (bounded).
            Suffix: IReadOnlyList<SessionEvent>
            /// How the suffix pages were consumed.
            Stats: ConsumeStats
            /// True when no checkpoint existed and the read scanned from
            /// cursor 0 to establish one (the documented initial
            /// reconstruction).
            IsInitialReconstruction: bool
            /// True when a stale checkpoint fell back to explicit
            /// reconstruction from cursor 0.
            HadFallback: bool
        }

    let private consumed = ConcurrentDictionary<string, int64>()
    let private bases = ConcurrentDictionary<string, CompactedEvent>()

    /// Builds the per-session checkpoint key. Tenant and session scope the
    /// checkpoint so cached context never crosses isolation boundaries.
    let private checkpointKey (tenant: TenantId) (sessionId: SessionId) : string = sprintf "%O|%O" tenant sessionId

    /// Clears all checkpoints and cached bases. Tests only: production
    /// code never clears, so checkpoints survive across turns in process.
    let clear () : unit =
        consumed.Clear()
        bases.Clear()

    /// Reads the cached consumed cursor for one session, when any.
    let tryGetConsumed (tenant: TenantId) (sessionId: SessionId) : int64 option =
        match consumed.TryGetValue(checkpointKey tenant sessionId) with
        | true, cursor -> Some cursor
        | false, _ -> None

    /// Reads the cached compacted base for one session, when any.
    let tryGetBase (tenant: TenantId) (sessionId: SessionId) : CompactedEvent option =
        match bases.TryGetValue(checkpointKey tenant sessionId) with
        | true, baseEvent when not (isNull (box baseEvent)) -> Some baseEvent
        | _ -> None

    /// Records the consumed cursor for one session.
    let private recordConsumed (tenant: TenantId) (sessionId: SessionId) (cursor: int64) : unit =
        consumed[checkpointKey tenant sessionId] <- cursor

    /// Records the compacted base for one session.
    let private recordBase (tenant: TenantId) (sessionId: SessionId) (baseEvent: CompactedEvent) : unit =
        if not (isNull (box baseEvent)) then
            bases[checkpointKey tenant sessionId] <- baseEvent

    /// Consumes one session's journal pages through the hardened #388
    /// pattern, folding every non-null event in sequence order. Pages only
    /// transport: every page split folds identically. Unknown session,
    /// expired journal, and end of stream settle with what was seen; an
    /// empty page carrying a continuation settles without looping; a
    /// nonempty page whose continuation does not advance fails explicitly;
    /// a null or unrecognized outcome fails explicitly. Cancellation is
    /// observed before the first fetch, between the bounded fetch and fold
    /// units, and after the last fetch, so a cancelled consume raises
    /// instead of advertising complete output.
    /// <param name="eventStore">The journal to replay. Must not be null.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to consume.</param>
    /// <param name="startCursor">The exclusive cursor to start from.</param>
    /// <param name="pageSize">The replay page size. Must be positive.</param>
    /// <param name="cancellationToken">Token that abandons the consume.</param>
    /// <param name="onEvent">Folds one non-null event in sequence order. Must not be null.</param>
    /// <returns>How the consume finished.</returns>
    let consumePagesAsync
        (eventStore: ISessionEventStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (startCursor: int64)
        (pageSize: int)
        (cancellationToken: CancellationToken)
        (onEvent: SessionEvent -> unit)
        : Task<ConsumeStats> =
        ArgumentNullException.ThrowIfNull(eventStore)

        if isNull (box onEvent) then
            raise (ArgumentNullException(nameof onEvent))

        if pageSize <= 0 then
            raise (ArgumentOutOfRangeException(nameof pageSize, "PageSize must be positive."))

        task {
            cancellationToken.ThrowIfCancellationRequested()

            let mutable cursor = startCursor
            let mutable paging = true
            let mutable pages = 0
            let mutable seen = 0
            let mutable endReason = "endOfStream"

            while paging do
                cancellationToken.ThrowIfCancellationRequested()

                let! outcome = eventStore.Replay(tenant, sessionId, cursor, pageSize, cancellationToken)

                if isNull (box outcome) then
                    raise (InvalidOperationException("The event store returned a null replay outcome."))
                else
                    match outcome with
                    | :? EventReplayPage as page when not (isNull (box page)) ->
                        cancellationToken.ThrowIfCancellationRequested()
                        pages <- pages + 1

                        let mutable eventCount = 0

                        if not (isNull (box page.Events)) then
                            for ev in page.Events do
                                if not (isNull (box ev)) then
                                    eventCount <- eventCount + 1

                        if page.NextCursor.HasValue then
                            if eventCount = 0 then
                                endReason <- "settledEmptyContinuation"
                                paging <- false
                            elif page.NextCursor.Value <= cursor then
                                raise (
                                    InvalidOperationException(
                                        sprintf
                                            "The event store returned a non-advancing replay cursor for session %O: cursor %d did not advance to %d."
                                            sessionId
                                            cursor
                                            page.NextCursor.Value
                                    )
                                )
                            else
                                cursor <- page.NextCursor.Value

                                if not (isNull (box page.Events)) then
                                    for ev in page.Events do
                                        if not (isNull (box ev)) then
                                            onEvent ev
                                            seen <- seen + 1

                                cancellationToken.ThrowIfCancellationRequested()
                        else
                            if not (isNull (box page.Events)) then
                                for ev in page.Events do
                                    if not (isNull (box ev)) then
                                        onEvent ev
                                        seen <- seen + 1

                            paging <- false
                            cancellationToken.ThrowIfCancellationRequested()
                    | :? EventReplayEndOfStream ->
                        endReason <- "endOfStream"
                        paging <- false
                    | :? EventReplayUnknownSession ->
                        endReason <- "unknownSession"
                        paging <- false
                    | :? EventReplayJournalExpired ->
                        endReason <- "journalExpired"
                        paging <- false
                    | _ -> raise (InvalidOperationException("The event store returned an unknown replay outcome."))

            cancellationToken.ThrowIfCancellationRequested()

            return
                {
                    StartCursor = startCursor
                    PagesFetched = pages
                    EventsSeen = seen
                    LastCursor = cursor
                    EndReason = endReason
                }
        }

    /// Reads one session's post-checkpoint suffix with its compacted base
    /// (issue 389): with a cached compacted base the read resumes from the
    /// cached consumed cursor and replays only the incremental suffix,
    /// validated for continuity; the base plus the suffix feeds
    /// tryRecoverCompacted as [base] + suffix, which equals the
    /// full-journal resolution because superseded pre-compaction context
    /// drops. With no cached base the whole prefix is still applicable
    /// context (nothing is superseded), so the read scans from cursor 0
    /// while retaining only the base plus the post-base suffix. The one
    /// full read after (re)start with an unknown checkpoint is the
    /// documented initial reconstruction. A stale checkpoint (the first
    /// suffix sequence skips past the cached cursor plus one) falls back
    /// to explicit reconstruction from cursor 0, never silent truncation.
    /// Unstamped sequences (no Sequence value) skip gap validation:
    /// continuity is proven by the store cursors alone.
    /// <param name="eventStore">The journal to replay. Must not be null.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to read.</param>
    /// <param name="pageSize">The replay page size. Must be positive.</param>
    /// <param name="cancellationToken">Token that abandons the read.</param>
    /// <returns>The base plus the bounded suffix with its stats.</returns>
    let readSuffixWithBaseAsync
        (eventStore: ISessionEventStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (pageSize: int)
        (cancellationToken: CancellationToken)
        : Task<SuffixRead> =
        ArgumentNullException.ThrowIfNull(eventStore)

        if pageSize <= 0 then
            raise (ArgumentOutOfRangeException(nameof pageSize, "PageSize must be positive."))

        task {
            let key = checkpointKey tenant sessionId

            let cachedCursor =
                match consumed.TryGetValue(key) with
                | true, cursor -> Some cursor
                | false, _ -> None

            let cachedBase =
                match bases.TryGetValue(key) with
                | true, baseEvent when not (isNull (box baseEvent)) -> Some baseEvent
                | _ -> None

            match cachedCursor, cachedBase with
            | Some resumeFrom, Some _ ->
                let suffix = ResizeArray<SessionEvent>()
                let mutable lastCompacted = cachedBase
                let mutable sawNewCompacted = false

                let! stats =
                    consumePagesAsync eventStore tenant sessionId resumeFrom pageSize cancellationToken (fun ev ->
                        suffix.Add(ev)

                        match ev with
                        | :? CompactedEvent as compacted when not (isNull (box compacted)) ->
                            lastCompacted <- Some compacted
                            sawNewCompacted <- true
                        | _ -> ())

                let gap =
                    if suffix.Count = 0 then
                        false
                    else
                        let first = suffix[0]

                        if isNull (box first) then
                            false
                        elif first.Sequence.HasValue then
                            first.Sequence.Value <> resumeFrom + 1L
                        else
                            false

                if gap then
                    let fullSuffix = ResizeArray<SessionEvent>()
                    let mutable fullBase: CompactedEvent option = None
                    let mutable tail = ResizeArray<SessionEvent>()

                    let! fullStats =
                        consumePagesAsync eventStore tenant sessionId 0L pageSize cancellationToken (fun ev ->
                            match ev with
                            | :? CompactedEvent as compacted when not (isNull (box compacted)) ->
                                fullBase <- Some compacted
                                tail <- ResizeArray<SessionEvent>()
                            | _ -> tail.Add(ev)

                            fullSuffix.Add(ev))

                    // Retention stays bounded: keep only the base plus the
                    // post-base tail, never the full prefix copy.
                    let boundedTail = tail :> IReadOnlyList<SessionEvent>

                    recordConsumed tenant sessionId fullStats.LastCursor

                    match fullBase with
                    | Some baseEvent -> recordBase tenant sessionId baseEvent
                    | None -> ()

                    // Drop the full-prefix copy; only the tail survives.
                    fullSuffix.Clear()

                    return
                        {
                            BaseEvent = fullBase
                            Suffix = boundedTail
                            Stats = fullStats
                            IsInitialReconstruction = false
                            HadFallback = true
                        }
                else
                    recordConsumed tenant sessionId stats.LastCursor

                    if sawNewCompacted then
                        match lastCompacted with
                        | Some baseEvent -> recordBase tenant sessionId baseEvent
                        | None -> ()

                    // When the suffix carries a newer compacted base, only
                    // the post-base tail feeds recovery: superseded prefix
                    // inside the suffix drops the same way the full-journal
                    // shape drops it.
                    let effectiveSuffix, effectiveBase =
                        match lastCompacted with
                        | None -> (suffix :> IReadOnlyList<SessionEvent>), None
                        | Some baseEvent ->
                            let mutable baseIndex = -1

                            for index in 0 .. suffix.Count - 1 do
                                if Object.ReferenceEquals(suffix[index], (baseEvent :> SessionEvent)) then
                                    baseIndex <- index

                            if baseIndex < 0 then
                                (suffix :> IReadOnlyList<SessionEvent>), lastCompacted
                            else
                                let tail = ResizeArray<SessionEvent>()

                                for index in baseIndex + 1 .. suffix.Count - 1 do
                                    tail.Add(suffix[index])

                                (tail :> IReadOnlyList<SessionEvent>), lastCompacted

                    return
                        {
                            BaseEvent = effectiveBase
                            Suffix = effectiveSuffix
                            Stats = stats
                            IsInitialReconstruction = false
                            HadFallback = false
                        }
            | _ ->
                // No cached base: the whole prefix is still applicable
                // context, so scan from cursor 0 while retaining only the
                // base plus the post-base suffix. Only the very first such
                // read counts as the documented initial reconstruction;
                // later uncompacted scans are the inherent full-prefix cost
                // of applicable (non-superseded) history, disclosed here
                // rather than concealed behind a suffix-only shortcut.
                let isFirst = cachedCursor.IsNone
                let mutable foundBase: CompactedEvent option = None
                let mutable tail = ResizeArray<SessionEvent>()

                let! stats =
                    consumePagesAsync eventStore tenant sessionId 0L pageSize cancellationToken (fun ev ->
                        match ev with
                        | :? CompactedEvent as compacted when not (isNull (box compacted)) ->
                            foundBase <- Some compacted
                            tail <- ResizeArray<SessionEvent>()
                        | _ -> tail.Add(ev))

                recordConsumed tenant sessionId stats.LastCursor

                match foundBase with
                | Some baseEvent -> recordBase tenant sessionId baseEvent
                | None -> ()

                return
                    {
                        BaseEvent = foundBase
                        Suffix = tail :> IReadOnlyList<SessionEvent>
                        Stats = stats
                        IsInitialReconstruction = isFirst
                        HadFallback = false
                    }
        }

    /// Builds the #387 recovery input from one suffix read: the base event
    /// (when any) followed by the suffix, in sequence order. Feeding this
    /// small list to tryRecoverCompacted equals the full-journal resolution
    /// because superseded pre-compaction context drops either way.
    /// <param name="read">The suffix read. Must not be null.</param>
    /// <returns>The base-plus-suffix events for tryRecoverCompacted.</returns>
    let recoveryInputOf (read: SuffixRead) : IReadOnlyList<SessionEvent> =
        if isNull (box read) then
            raise (ArgumentNullException(nameof read))

        let combined = ResizeArray<SessionEvent>()

        match read.BaseEvent with
        | Some baseEvent -> combined.Add(baseEvent :> SessionEvent)
        | None -> ()

        if not (isNull (box read.Suffix)) then
            for ev in read.Suffix do
                if not (isNull (box ev)) then
                    // Skip the base event itself when the suffix still
                    // carries it by reference: the base leads exactly once.
                    match read.BaseEvent with
                    | Some baseEvent when Object.ReferenceEquals(ev, (baseEvent :> SessionEvent)) -> ()
                    | _ -> combined.Add(ev)

        combined :> IReadOnlyList<SessionEvent>
