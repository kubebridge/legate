// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Threading
open System.Threading.Channels
open System.Threading.Tasks

// Session event bus (issue 49): the replay-then-live hub the merged
// JournalWriter (#48) publishes its stamped events to. The writer stays the
// sole append path and publishes through its internal notification; this hub
// never sanitises or persists (the writer owns sanitize/bound, the store
// owns persist-what-given). Subscribe replays via ISessionEventStore.Replay
// then attaches live gap-free, and ReadEvents is a thin Replay wrapper.
// Rejected: the Akka event stream as the bus (actor-thread blocking plus
// cluster semantics overkill for a process-local bus); cell derivation and
// ReadTranscript here (owned by #50).

/// How a live event subscription buffers: the per-session subscriber cap
/// and the per-subscriber channel bound. Bound from configuration; mutable
/// so hosts can set properties before registering. Defaults hold 512 live
/// subscribers per session with 128 buffered events each.
type SessionSubscriptionOptions() =

    /// The live subscribers one session holds. A new subscription past the
    /// cap is rejected with
    /// <see cref="T:Legate.SessionSubscriptionLimitExceededException" />
    /// instead of evicting an existing one. Default 512.
    member val MaxSubscribersPerSession: int = 512 with get, set

    /// The events one subscriber buffers before it is considered slow. A
    /// subscriber past the bound is disconnected with
    /// <see cref="T:Legate.SessionSubscriptionLaggedException" /> instead of
    /// stalling the publishing turn. Default 128.
    member val PerSubscriberBufferSize: int = 128 with get, set

    /// Returns null when every knob is in range, otherwise a message for the
    /// first violation.
    /// <returns>The first violation's message, or null when the settings are valid.</returns>
    member this.Validate() : string | null =
        if this.MaxSubscribersPerSession < 1 then
            "MaxSubscribersPerSession must be at least 1."
        elif this.PerSubscriberBufferSize < 1 then
            "PerSubscriberBufferSize must be at least 1."
        else
            null

/// One live subscriber: its bounded channel. Publishers never block on it:
/// a full channel disconnects the subscriber with the lagged error instead.
type private Subscriber(channel: Channel<SessionEvent>) =

    /// The bounded channel the subscriber drains.
    member _.Channel = channel

/// One per-session hub: the live subscribers for a single tenant session.
/// The gate serialises attach, publish, and detach; channels carry the
/// events so publishers never block.
type private Hub() =

    /// Serialises attach, publish, and detach for the session.
    member val Gate = obj () with get

    /// The live subscribers, in attach order.
    member val Subscribers = ResizeArray<Subscriber>() with get

/// Enumerates one subscription replay-then-live: bounded replay pages from
/// the store up to the live position, then the attached channel with
/// sequence deduplication across the handoff. Unknown session and expired
/// journal surface as their typed exceptions on the first move; a slow
/// subscriber surfaces the lagged exception instead of stalling; the
/// session-closed event is terminal.
type private SubscribeEnumerator
    (
        eventStore: ISessionEventStore,
        hubs: ConcurrentDictionary<TenantId * SessionId, Hub>,
        hub: Hub,
        subscriber: Subscriber,
        tenant: TenantId,
        sessionId: SessionId,
        fromSequence: int64,
        subscribeToken: CancellationToken,
        enumeratorToken: CancellationToken
    ) =

    let mutable replayCursor = fromSequence
    let replayQueue = Queue<SessionEvent>()
    let mutable replayDone = false
    let mutable current: SessionEvent = Unchecked.defaultof<SessionEvent>
    let mutable finished = false
    let mutable unsubscribed = false

    let unsubscribe () =
        if not unsubscribed then
            unsubscribed <- true

            lock hub.Gate (fun () ->
                hub.Subscribers.Remove(subscriber) |> ignore

                try
                    subscriber.Channel.Writer.TryComplete() |> ignore
                with _ ->
                    ()

                if hub.Subscribers.Count = 0 then
                    let mutable removed = Unchecked.defaultof<Hub>
                    hubs.TryRemove((tenant, sessionId), &removed) |> ignore)

    member _.Current = current

    member _.MoveNextAsync() : ValueTask<bool> =
        ValueTask<bool>(
            task {
                use linkedCts =
                    CancellationTokenSource.CreateLinkedTokenSource(subscribeToken, enumeratorToken)

                if finished then
                    return false
                else
                    let mutable step: bool option = None

                    while step.IsNone do
                        if replayQueue.Count > 0 then
                            let next = replayQueue.Dequeue()
                            current <- next

                            if next :? SessionClosedEvent then
                                finished <- true
                                unsubscribe ()

                            step <- Some true
                        elif not replayDone then
                            let! outcome = eventStore.Replay(tenant, sessionId, replayCursor, 100, linkedCts.Token)

                            if isNull (box outcome) then
                                unsubscribe ()
                                raise (InvalidOperationException("The event store returned null."))
                            else
                                match outcome with
                                | :? EventReplayPage as page when not (isNull (box page)) ->
                                    if isNull (box page.Events) || page.Events.Count = 0 then
                                        if page.NextCursor.HasValue then
                                            replayCursor <- page.NextCursor.Value
                                        else
                                            replayDone <- true
                                    else
                                        for evt in page.Events do
                                            if not (isNull (box evt)) then
                                                replayQueue.Enqueue(evt)

                                        if page.NextCursor.HasValue then
                                            replayCursor <- page.NextCursor.Value
                                        else
                                            let last = page.Events[page.Events.Count - 1]

                                            if not (isNull (box last)) && last.Sequence.HasValue then
                                                replayCursor <- last.Sequence.Value

                                            replayDone <- true
                                | :? EventReplayEndOfStream -> replayDone <- true
                                | :? EventReplayUnknownSession as unknown when not (isNull (box unknown)) ->
                                    unsubscribe ()

                                    raise (
                                        SessionNotFoundException(
                                            unknown.SessionId,
                                            sprintf "No session %O exists in tenant %O." unknown.SessionId tenant
                                        )
                                    )
                                | :? EventReplayJournalExpired as expired when not (isNull (box expired)) ->
                                    unsubscribe ()

                                    raise (
                                        SessionJournalExpiredException(
                                            expired.SessionId,
                                            sprintf "The journal for session %O is gone." expired.SessionId
                                        )
                                    )
                                | _ ->
                                    unsubscribe ()

                                    raise (
                                        InvalidOperationException("The event store returned an unknown replay outcome.")
                                    )
                        else
                            try
                                let! live = subscriber.Channel.Reader.ReadAsync(linkedCts.Token).AsTask()

                                if isNull (box live) then
                                    ()
                                elif live.Sequence.HasValue && live.Sequence.Value <= replayCursor then
                                    ()
                                else
                                    if live.Sequence.HasValue then
                                        replayCursor <- live.Sequence.Value

                                    current <- live

                                    if live :? SessionClosedEvent then
                                        finished <- true
                                        unsubscribe ()

                                    step <- Some true
                            with
                            | :? OperationCanceledException as canceled ->
                                unsubscribe ()
                                raise canceled
                            | :? SessionSubscriptionLaggedException as lagged ->
                                unsubscribe ()
                                raise lagged
                            | :? ChannelClosedException as closed ->
                                match closed.InnerException with
                                | :? SessionSubscriptionLaggedException as lagged ->
                                    unsubscribe ()
                                    raise lagged
                                | _ ->
                                    unsubscribe ()
                                    step <- Some false

                    match step with
                    | Some value -> return value
                    | None -> return false
            }
        )

    member _.DisposeAsync() : ValueTask =
        unsubscribe ()
        ValueTask.CompletedTask

    interface IAsyncEnumerator<SessionEvent> with
        member this.Current = this.Current
        member this.MoveNextAsync() = this.MoveNextAsync()
        member this.DisposeAsync() = this.DisposeAsync()

/// One replay-then-live enumeration over a single attached subscriber.
type private SubscribeEnumerable
    (
        eventStore: ISessionEventStore,
        hubs: ConcurrentDictionary<TenantId * SessionId, Hub>,
        hub: Hub,
        subscriber: Subscriber,
        tenant: TenantId,
        sessionId: SessionId,
        fromSequence: int64,
        subscribeToken: CancellationToken
    ) =

    interface IAsyncEnumerable<SessionEvent> with
        member _.GetAsyncEnumerator(cancellationToken: CancellationToken) : IAsyncEnumerator<SessionEvent> =
            upcast
                SubscribeEnumerator(
                    eventStore,
                    hubs,
                    hub,
                    subscriber,
                    tenant,
                    sessionId,
                    fromSequence,
                    subscribeToken,
                    cancellationToken
                )

/// The replay-then-live event hub: the JournalWriter publishes its stamped
/// batches here, and hosts read them back through ReadEvents and Subscribe.
/// Per (tenant, session) hubs with bounded per-subscriber channels (slow
/// subscribers disconnect with the typed lagged error, never stalling the
/// turn) and a subscriber cap (new subscriptions past the cap reject with
/// the typed limit error). Tenant isolation is enforced by the hub key and
/// by the store calls carrying the tenant; fenced-out writes publish
/// nothing because the writer only notifies on landed batches.
[<Sealed>]
type SessionEventBus(eventStore: ISessionEventStore, options: SessionSubscriptionOptions) =
    do ArgumentNullException.ThrowIfNull(eventStore)
    do ArgumentNullException.ThrowIfNull(options)

    do
        let violation = options.Validate()

        if not (isNull (box violation)) then
            raise (ArgumentException(violation, nameof options))

    let hubs = ConcurrentDictionary<TenantId * SessionId, Hub>()
    let maxSubscribers = options.MaxSubscribersPerSession
    let bufferSize = options.PerSubscriberBufferSize
    let mutable disposed = 0

    let isDisposed () = Volatile.Read(&disposed) = 1

    let publishToHub (tenant: TenantId) (sessionId: SessionId) (stamped: IReadOnlyList<SessionEvent>) =
        match hubs.TryGetValue((tenant, sessionId)) with
        | false, _ -> ()
        | true, hub ->
            lock hub.Gate (fun () ->
                let lagged = ResizeArray<Subscriber>()

                for subscriber in hub.Subscribers do
                    let mutable overflowed = false

                    for evt in stamped do
                        if not overflowed && not (isNull (box evt)) then
                            if not (subscriber.Channel.Writer.TryWrite(evt)) then
                                overflowed <- true

                    if overflowed then
                        lagged.Add(subscriber)

                for subscriber in lagged do
                    hub.Subscribers.Remove(subscriber) |> ignore

                    let error =
                        SessionSubscriptionLaggedException(
                            sessionId,
                            "slowSubscriber",
                            "The event subscriber fell behind its bounded channel and was disconnected."
                        )

                    try
                        subscriber.Channel.Writer.Complete(error) |> ignore
                    with _ ->
                        ()

                if hub.Subscribers.Count = 0 then
                    let mutable removed = Unchecked.defaultof<Hub>
                    hubs.TryRemove((tenant, sessionId), &removed) |> ignore)

    do JournalWriter.Published.Add(fun (tenant, sessionId, stamped) -> publishToHub tenant sessionId stamped)

    /// Constructs the hub over the given journal with default subscription
    /// options (512 subscribers per session, 128 buffered events each).
    /// <param name="eventStore">The journal Subscribe replays from. Must not be null.</param>
    new(eventStore: ISessionEventStore) = new SessionEventBus(eventStore, SessionSubscriptionOptions())

    /// The journal Subscribe replays from.
    /// <returns>The event store.</returns>
    member _.EventStore: ISessionEventStore = eventStore

    /// The subscription bounds the hub enforces, snapshotted at
    /// construction.
    /// <returns>The maximum subscribers per session and the per-subscriber buffer size.</returns>
    member _.Options: SessionSubscriptionOptions = options

    /// Publishes stamped events to the session's live subscribers.
    /// Internal: hosts never publish; the journal writer notifies landed
    /// batches here, and fenced-out writes never arrive. Never blocks: a
    /// full subscriber disconnects with the lagged error instead.
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session whose subscribers receive the events.</param>
    /// <param name="stamped">The stamped events, in sequence order. Must not be null.</param>
    member internal _.Publish(tenant: TenantId, sessionId: SessionId, stamped: IReadOnlyList<SessionEvent>) : unit =
        ArgumentNullException.ThrowIfNull(stamped)

        if isDisposed () then
            raise (ObjectDisposedException(nameof SessionEventBus))

        publishToHub tenant sessionId stamped

    /// Reads one bounded page of the session's journal: the events with a
    /// sequence strictly greater than the cursor, in sequence order, at most
    /// the limit. A thin wrapper over
    /// <see cref="M:Legate.ISessionEventStore.Replay*" /> mapping control
    /// plane branches to throws: unknown session throws
    /// <see cref="T:Legate.SessionNotFoundException" />, an expired journal
    /// throws <see cref="T:Legate.SessionJournalExpiredException" />, and
    /// end of stream returns an empty list.
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session whose journal to read.</param>
    /// <param name="fromSequence">The exclusive cursor: read events with a sequence strictly greater than it; 0 reads from the journal's first event.</param>
    /// <param name="limit">The maximum number of events to return; must be positive.</param>
    /// <param name="cancellationToken">Token that abandons the read.</param>
    /// <returns>The journaled events, in sequence order; empty at end of stream.</returns>
    member _.ReadEventsAsync
        (tenant: TenantId, sessionId: SessionId, fromSequence: int64, limit: int, cancellationToken: CancellationToken)
        : Task<IReadOnlyList<SessionEvent>> =
        if limit <= 0 then
            raise (ArgumentOutOfRangeException(nameof limit, "The limit must be positive."))

        if fromSequence < 0L then
            raise (ArgumentOutOfRangeException(nameof fromSequence, "The cursor must not be negative."))

        if isDisposed () then
            raise (ObjectDisposedException(nameof SessionEventBus))

        task {
            let! outcome = eventStore.Replay(tenant, sessionId, fromSequence, limit, cancellationToken)

            if isNull (box outcome) then
                return raise (InvalidOperationException("The event store returned null."))
            else
                match outcome with
                | :? EventReplayPage as page when not (isNull (box page)) ->
                    if isNull (box page.Events) then
                        return Array.Empty<SessionEvent>() :> IReadOnlyList<SessionEvent>
                    else
                        return page.Events
                | :? EventReplayEndOfStream -> return Array.Empty<SessionEvent>() :> IReadOnlyList<SessionEvent>
                | :? EventReplayUnknownSession as unknown when not (isNull (box unknown)) ->
                    return
                        raise (
                            SessionNotFoundException(
                                unknown.SessionId,
                                sprintf "No session %O exists in tenant %O." unknown.SessionId tenant
                            )
                        )
                | :? EventReplayJournalExpired as expired when not (isNull (box expired)) ->
                    return
                        raise (
                            SessionJournalExpiredException(
                                expired.SessionId,
                                sprintf "The journal for session %O is gone." expired.SessionId
                            )
                        )
                | _ -> return raise (InvalidOperationException("The event store returned an unknown replay outcome."))
        }

    /// Subscribes to the session's events from the cursor: replays the
    /// journal via <see cref="M:Legate.ISessionEventStore.Replay*" /> up to
    /// the live position, then yields live publishes gap-free with no
    /// duplicates across the handoff. Events published during the replay
    /// land in both paths and deduplicate by sequence. Unknown session
    /// throws <see cref="T:Legate.SessionNotFoundException" /> and an
    /// expired journal throws
    /// <see cref="T:Legate.SessionJournalExpiredException" /> on the first
    /// move; a slow subscriber throws
    /// <see cref="T:Legate.SessionSubscriptionLaggedException" /> instead of
    /// stalling; the session-closed event is terminal.
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to subscribe to.</param>
    /// <param name="fromSequence">The exclusive cursor: replay events with a sequence strictly greater than it; 0 replays from the journal's first event.</param>
    /// <param name="cancellationToken">Token that abandons the replay and the live wait.</param>
    /// <returns>The replay-then-live event stream.</returns>
    member _.Subscribe
        (tenant: TenantId, sessionId: SessionId, fromSequence: int64, cancellationToken: CancellationToken)
        : IAsyncEnumerable<SessionEvent> =
        if fromSequence < 0L then
            raise (ArgumentOutOfRangeException(nameof fromSequence, "The cursor must not be negative."))

        if isDisposed () then
            raise (ObjectDisposedException(nameof SessionEventBus))

        let hub = hubs.GetOrAdd((tenant, sessionId), fun _ -> Hub())

        try
            let subscriber =
                lock hub.Gate (fun () ->
                    if hub.Subscribers.Count >= maxSubscribers then
                        raise (
                            SessionSubscriptionLimitExceededException(
                                sessionId,
                                maxSubscribers,
                                sprintf "The session holds %d live subscribers." maxSubscribers
                            )
                        )

                    let channelOptions = BoundedChannelOptions(bufferSize)
                    channelOptions.FullMode <- BoundedChannelFullMode.Wait
                    channelOptions.SingleReader <- true
                    channelOptions.SingleWriter <- false

                    let channel = Channel.CreateBounded<SessionEvent>(channelOptions)
                    let attached = Subscriber(channel)
                    hub.Subscribers.Add(attached)
                    attached)

            upcast
                SubscribeEnumerable(
                    eventStore,
                    hubs,
                    hub,
                    subscriber,
                    tenant,
                    sessionId,
                    fromSequence,
                    cancellationToken
                )
        with ex ->
            lock hub.Gate (fun () ->
                if hub.Subscribers.Count = 0 then
                    let mutable removed = Unchecked.defaultof<Hub>
                    hubs.TryRemove((tenant, sessionId), &removed) |> ignore)

            raise ex

    /// Releases the hub: further publishes and subscriptions throw
    /// ObjectDisposedException, and live readers observe completion.
    member _.Dispose() : unit =
        if Interlocked.Exchange(&disposed, 1) = 0 then
            for entry in hubs do
                lock entry.Value.Gate (fun () ->
                    for subscriber in entry.Value.Subscribers do
                        try
                            subscriber.Channel.Writer.TryComplete() |> ignore
                        with _ ->
                            ())

            hubs.Clear()

    interface IDisposable with
        member this.Dispose() = this.Dispose()
