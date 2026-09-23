// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Generic
open System.Text.Json
open System.Threading
open System.Threading.Tasks

// Cross-node subscription routing (issue 133): the owning-entity
// subscriber registry plus the bounded replay cache the shard region
// serves remote subscribers from. Local mode keeps the process-local
// SessionEventBus replay-then-live path unchanged; cluster modes route
// Subscribe through the session entity, which streams journaled events
// back to the subscribing node and falls back to ISessionEventStore.Replay
// on rebind. Delivery is at-least-once with gap detection and redelivery:
// sequence numbers let the consumer detect gaps and resume from a cursor,
// duplicates are acceptable, gaps are not. Tenant isolation is enforced by
// keying every operation on the tenant; unknown-session and expired-journal
// surface as their typed outcomes so the caller maps them to throws.
module internal CrossNodeSubscriptions =

    /// How many events one entity-to-subscriber batch carries at most.
    /// Bounded batches keep a large backlog from growing one Ask reply
    /// without bound; the consumer follows NextCursor while EndOfStream
    /// is false.
    [<Literal>]
    let MaxBatchEvents = 100

    /// A cross-node subscribe request routed to the owning entity through
    /// the session shard region. The entity answers with event batches
    /// from the cursor; the consumer resumes from its last sequence on
    /// entity move or node restart. The subscriber token makes re-polls
    /// idempotent: the same token re-attaches without consuming another
    /// subscriber slot, so every poll is a resume and duplicates stay
    /// allowed while gaps are not.
    type CrossNodeSubscribeRequest =
        {
            /// The tenant the session belongs to.
            Tenant: TenantId
            /// The session to subscribe to.
            SessionId: SessionId
            /// The exclusive cursor: events strictly greater than it stream
            /// back; 0 replays from the journal's first event.
            FromSequence: int64
            /// The subscriber token identifying this stream across
            /// re-polls and rebinds. Must be a non-empty string.
            SubscriberToken: string
        }

    /// A cross-node unsubscribe routed to the owning entity when the
    /// remote consumer disposes its stream. Best-effort: a lost
    /// unsubscribe only holds one subscriber slot until the entity
    /// restarts; the cap stays bounded either way.
    type CrossNodeUnsubscribe =
        {
            /// The tenant the session belongs to.
            Tenant: TenantId
            /// The session to detach from.
            SessionId: SessionId
            /// The subscriber token to detach.
            SubscriberToken: string
        }

    /// One entity-to-subscriber batch: the events in sequence order, the
    /// cursor to pass into the next request, and whether the journal holds
    /// nothing more past the cursor right now.
    type CrossNodeEventBatch =
        {
            /// The session the events belong to.
            SessionId: SessionId
            /// The events in sequence order; empty at end of stream.
            Events: IReadOnlyList<SessionEvent>
            /// The cursor to resume from: the greatest streamed sequence,
            /// or the request cursor when the batch holds no events.
            NextCursor: int64
            /// True when the journal holds nothing past NextCursor right
            /// now; false when more pages may follow.
            EndOfStream: bool
        }

    /// What one entity-side subscription batch resolves to: a page to
    /// stream, or a control-plane branch the caller maps to a throw.
    /// Mirrors EventReplayOutcome so error propagation stays uniform.
    type CrossNodeBatchOutcome =
        /// A page of events with its resume cursor.
        | BatchPage of batch: CrossNodeEventBatch
        /// The session id names no session in this tenant.
        | BatchUnknownSession of sessionId: SessionId
        /// The journal for the session is gone.
        | BatchJournalExpired of sessionId: SessionId
        /// The session holds the subscriber cap already.
        | BatchSubscriberCapped of sessionId: SessionId * limit: int
        /// One event breaches the per-event payload bound.
        | BatchEventOversized of sessionId: SessionId * sequence: int64 * limit: int * observed: int

    /// One per-session entity subscription hub: the live subscriber tokens
    /// plus the bounded replay cache of recent journaled events. The gate
    /// serialises attach, append, and detach; the cache evicts oldest
    /// first past the bound so a slow consumer redelivers from the store
    /// with duplicates allowed and no gaps. Tokens make re-polls
    /// idempotent: re-attaching a known token never consumes another slot.
    type SubscriptionHub(options: SessionSubscriptionOptions) =
        do ArgumentNullException.ThrowIfNull(options)

        let gate = obj ()
        let subscribers = HashSet<string>(StringComparer.Ordinal)
        let cache = Queue<SessionEvent>()
        let maxSubscribers = max 1 options.MaxSubscribersPerSession
        let cacheBound = max 1 options.ReplayCacheSize

        /// Serialises attach, append, and detach for the session.
        member _.Gate = gate

        /// How many remote subscribers are attached right now.
        member _.SubscriberCount = lock gate (fun () -> subscribers.Count)

        /// How many events the replay cache holds right now.
        member _.CacheCount = lock gate (fun () -> cache.Count)

        /// Attaches one remote subscriber token or reports the cap.
        /// Re-attaching a known token succeeds without consuming another
        /// slot, so re-polls and rebinds never leak the cap. Returns true
        /// when attached, false when the session already holds the
        /// subscriber cap for a new token.
        /// <param name="token">The subscriber token. Must be non-empty.</param>
        /// <returns>True when attached; false at the cap.</returns>
        member _.TryAttach(token: string) : bool =
            if String.IsNullOrWhiteSpace token then
                raise (ArgumentException("The subscriber token must be a non-empty string.", nameof token))

            lock gate (fun () ->
                if subscribers.Contains(token) then
                    true
                elif subscribers.Count >= maxSubscribers then
                    false
                else
                    subscribers.Add(token) |> ignore
                    true)

        /// Detaches one remote subscriber token. Unknown tokens no-op.
        /// <param name="token">The subscriber token to detach.</param>
        member _.Detach(token: string) : unit =
            if not (String.IsNullOrWhiteSpace token) then
                lock gate (fun () -> subscribers.Remove(token) |> ignore)

        /// Appends stamped events to the bounded replay cache, evicting
        /// oldest first past the bound. Null events never land.
        /// <param name="stamped">The stamped events in sequence order.</param>
        member _.AppendToCache(stamped: IReadOnlyList<SessionEvent>) : unit =
            if not (isNull (box stamped)) then
                lock gate (fun () ->
                    for evt in stamped do
                        if not (isNull (box evt)) then
                            cache.Enqueue(evt)

                            while cache.Count > cacheBound do
                                cache.Dequeue() |> ignore)

        /// Reads the cached events strictly greater than the cursor, in
        /// sequence order, at most maxBatch. Cache-only: older cursors
        /// miss and fall back to the store replay.
        /// <param name="fromSequence">The exclusive cursor.</param>
        /// <param name="maxBatch">The maximum events to return.</param>
        /// <returns>The cached events past the cursor.</returns>
        member _.ReadCached(fromSequence: int64, maxBatch: int) : IReadOnlyList<SessionEvent> =
            lock gate (fun () ->
                let bound = max 1 maxBatch
                let collected = ResizeArray<SessionEvent>()

                for evt in cache do
                    if collected.Count < bound then
                        if
                            not (isNull (box evt))
                            && evt.Sequence.HasValue
                            && evt.Sequence.Value > fromSequence
                        then
                            collected.Add(evt)

                collected :> IReadOnlyList<SessionEvent>)

        /// The smallest cached sequence, or None when the cache is empty.
        /// <returns>The cache floor, or None when empty.</returns>
        member _.CacheFloor() : int64 option =
            lock gate (fun () ->
                let mutable floor: int64 option = None

                for evt in cache do
                    if not (isNull (box evt)) && evt.Sequence.HasValue then
                        match floor with
                        | None -> floor <- Some evt.Sequence.Value
                        | Some current when evt.Sequence.Value < current -> floor <- Some evt.Sequence.Value
                        | _ -> ()

                floor)

        /// The greatest cached sequence, or None when the cache is empty.
        /// <returns>The cache ceiling, or None when empty.</returns>
        member _.CacheCeiling() : int64 option =
            lock gate (fun () ->
                let mutable ceiling: int64 option = None

                for evt in cache do
                    if not (isNull (box evt)) && evt.Sequence.HasValue then
                        match ceiling with
                        | None -> ceiling <- Some evt.Sequence.Value
                        | Some current when evt.Sequence.Value > current -> ceiling <- Some evt.Sequence.Value
                        | _ -> ()

                ceiling)

    /// Estimates one event's wire bytes through the durable JSON shape so
    /// the entity refuses oversized events before crossing. Uses the plain
    /// STJ defaults the durable layer speaks (the same $type
    /// discriminators the wire envelope carries).
    /// <param name="evt">The event to measure. Must not be null.</param>
    /// <returns>The estimated payload bytes.</returns>
    let estimateEventBytes (evt: SessionEvent) : int =
        ArgumentNullException.ThrowIfNull(evt)
        JsonSerializer.SerializeToUtf8Bytes(evt, evt.GetType(), JsonSerializerOptions()).Length

    /// Serves one cross-node batch from the cache with a store fallback:
    /// cached events past the cursor stream first; when the cache misses
    /// (empty cache or a cursor at or behind the cache floor) the store
    /// replay fills the page. Unknown session and expired journal resolve
    /// to their branches; oversized events refuse before crossing.
    /// At-least-once throughout: evicted cache entries redeliver from the
    /// store with duplicates allowed and no gaps.
    /// <param name="eventStore">The journal to fall back to. Must not be null.</param>
    /// <param name="hub">The entity hub. Must not be null.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to serve.</param>
    /// <param name="fromSequence">The exclusive cursor.</param>
    /// <param name="maxBatch">The maximum events on the page.</param>
    /// <param name="maxEventBytes">The per-event payload bound.</param>
    /// <param name="cancellationToken">Abandons the store fallback.</param>
    let serveBatchAsync
        (
            eventStore: ISessionEventStore,
            hub: SubscriptionHub,
            tenant: TenantId,
            sessionId: SessionId,
            fromSequence: int64,
            maxBatch: int,
            maxEventBytes: int,
            cancellationToken: CancellationToken
        ) : Task<CrossNodeBatchOutcome> =
        ArgumentNullException.ThrowIfNull(eventStore)
        ArgumentNullException.ThrowIfNull(hub)

        task {
            if fromSequence < 0L then
                raise (ArgumentOutOfRangeException(nameof fromSequence, "The cursor must not be negative."))

            let bound = max 1 maxBatch
            let cached = hub.ReadCached(fromSequence, bound)

            if cached.Count > 0 then
                let mutable oversized: (int64 * int) option = None

                for evt in cached do
                    match oversized with
                    | Some _ -> ()
                    | None ->
                        let observed = estimateEventBytes evt

                        let sequence =
                            if evt.Sequence.HasValue then
                                evt.Sequence.Value
                            else
                                fromSequence

                        if observed > maxEventBytes then
                            oversized <- Some(sequence, observed)

                match oversized with
                | Some(sequence, observed) -> return BatchEventOversized(sessionId, sequence, maxEventBytes, observed)
                | None ->
                    let last = cached[cached.Count - 1]

                    let next =
                        if last.Sequence.HasValue then
                            last.Sequence.Value
                        else
                            fromSequence

                    let batch =
                        {
                            SessionId = sessionId
                            Events = cached
                            NextCursor = next
                            EndOfStream = cached.Count < bound
                        }

                    return BatchPage batch
            else
                // Cache miss: fall back to the store replay. The live tail
                // also lands here as an empty page, so appends that reached
                // the shared store from another node always surface on the
                // next poll; the cache serves repeated-cursor and rebind
                // replays without a store round-trip.
                let! outcome = eventStore.Replay(tenant, sessionId, fromSequence, bound, cancellationToken)

                if isNull (box outcome) then
                    return raise (InvalidOperationException("The event store returned null."))
                else
                    match outcome with
                    | :? EventReplayPage as page when not (isNull (box page)) ->
                        let events =
                            if isNull (box page.Events) then
                                Array.Empty<SessionEvent>() :> IReadOnlyList<SessionEvent>
                            else
                                page.Events
                                |> Seq.filter (fun evt -> not (isNull (box evt)))
                                |> ResizeArray<SessionEvent>
                                :> IReadOnlyList<SessionEvent>

                        let mutable oversized: (int64 * int) option = None

                        for evt in events do
                            match oversized with
                            | Some _ -> ()
                            | None ->
                                let observed = estimateEventBytes evt

                                let sequence =
                                    if evt.Sequence.HasValue then
                                        evt.Sequence.Value
                                    else
                                        fromSequence

                                if observed > maxEventBytes then
                                    oversized <- Some(sequence, observed)

                        match oversized with
                        | Some(sequence, observed) ->
                            return BatchEventOversized(sessionId, sequence, maxEventBytes, observed)
                        | None ->
                            let next =
                                if page.NextCursor.HasValue then
                                    page.NextCursor.Value
                                elif events.Count > 0 then
                                    let last = events[events.Count - 1]

                                    if last.Sequence.HasValue then
                                        last.Sequence.Value
                                    else
                                        fromSequence
                                else
                                    fromSequence

                            let batch =
                                {
                                    SessionId = sessionId
                                    Events = events
                                    NextCursor = next
                                    EndOfStream = events.Count < bound
                                }

                            hub.AppendToCache(events)
                            return BatchPage batch
                    | :? EventReplayEndOfStream ->
                        let batch =
                            {
                                SessionId = sessionId
                                Events = Array.Empty<SessionEvent>() :> IReadOnlyList<SessionEvent>
                                NextCursor = fromSequence
                                EndOfStream = true
                            }

                        return BatchPage batch
                    | :? EventReplayUnknownSession as unknown when not (isNull (box unknown)) ->
                        return BatchUnknownSession unknown.SessionId
                    | :? EventReplayJournalExpired as expired when not (isNull (box expired)) ->
                        return BatchJournalExpired expired.SessionId
                    | _ ->
                        return raise (InvalidOperationException("The event store returned an unknown replay outcome."))
        }

    /// Maps a batch outcome onto the consumer throw: unknown session
    /// throws SessionNotFoundException, an expired journal throws
    /// SessionJournalExpiredException, a capped session throws the typed
    /// limit error, and an oversized event throws the typed event-limit
    /// error. Pages return None (no throw).
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="outcome">The batch outcome. Must not be null.</param>
    /// <returns>None for pages; otherwise raises.</returns>
    let raiseForOutcome (tenant: TenantId) (outcome: CrossNodeBatchOutcome) : unit =
        match outcome with
        | BatchPage _ -> ()
        | BatchUnknownSession sessionId ->
            raise (SessionNotFoundException(sessionId, sprintf "No session %O exists in tenant %O." sessionId tenant))
        | BatchJournalExpired sessionId ->
            raise (SessionJournalExpiredException(sessionId, sprintf "The journal for session %O is gone." sessionId))
        | BatchSubscriberCapped(sessionId, limit) ->
            raise (
                SessionSubscriptionLimitExceededException(
                    sessionId,
                    limit,
                    sprintf "The session holds %d live subscribers." limit
                )
            )
        | BatchEventOversized(sessionId, sequence, limit, observed) ->
            raise (
                EventLimitExceededException(
                    "perEventBytes",
                    int64 limit,
                    int64 observed,
                    sprintf
                        "The event at sequence %d in session %O is %d bytes, above the cross-node bound."
                        sequence
                        sessionId
                        observed
                )
            )
