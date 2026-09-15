// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.SessionEventBusTests

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Legate.Storage.InMemory
open Xunit

let stamp = DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero)
let noSequence = Unchecked.defaultof<Nullable<int64>>

let tenantOf (name: string) = TenantId.Create name

let makeStores () =
    let database = InMemoryDatabase()
    let sessions = InMemorySessionStore(database) :> ISessionStore
    let events = InMemorySessionEventStore(database) :> ISessionEventStore
    (database, sessions, events)

let makeSession (sessions: ISessionStore) (tenant: TenantId) (sessionId: SessionId) =
    task {
        let session =
            {
                Id = sessionId
                Tenant = tenant
                AgentId = AgentId.New()
                Title = "bus"
                State = SessionState.Idle
                CurrentTurnId = Nullable()
                CreatedAt = DateTimeOffset.MinValue
                UpdatedAt = DateTimeOffset.MinValue
                ClosedAt = Nullable()
                WorkspaceBinding = null
                Options = SessionOptions()
            }

        let! created = sessions.CreateSession(tenant, session, CancellationToken.None)

        let message = UserMessagePayload(UserMessage.Text("bus")) :> InboxPayload

        let! _ = sessions.AppendInboxMessage(tenant, created.Id, message, DeliveryMode.Queue, CancellationToken.None)

        let! claimed =
            sessions.ClaimNextTurn(tenant, created.Id, "bus-owner", TimeSpan.FromMinutes 5., CancellationToken.None)

        let claim = (claimed :?> TurnLeaseRenewed).Claim
        return (created.Id, claim)
    }

let delta (sessionId: SessionId) (turnId: TurnId) (text: string) =
    TextDeltaEvent(sessionId, turnId, noSequence, stamp, text) :> SessionEvent

let closed (sessionId: SessionId) (turnId: TurnId) =
    SessionClosedEvent(sessionId, turnId, noSequence, stamp) :> SessionEvent

let private appendViaWriter
    (events: ISessionEventStore)
    (tenant: TenantId)
    (sessionId: SessionId)
    (token: string)
    (batch: IReadOnlyList<SessionEvent>)
    =
    JournalWriter.appendWithTokenAsync events tenant sessionId token batch CancellationToken.None

let collectAll (enumerable: IAsyncEnumerable<SessionEvent>) =
    task {
        let results = ResizeArray<SessionEvent>()
        let enumerator = enumerable.GetAsyncEnumerator(CancellationToken.None)
        let mutable more = true

        while more do
            let! has = enumerator.MoveNextAsync().AsTask()
            more <- has

            if has then
                results.Add(enumerator.Current)

        do! enumerator.DisposeAsync().AsTask()

        return results :> IReadOnlyList<SessionEvent>
    }

let collectTake (enumerable: IAsyncEnumerable<SessionEvent>) (take: int) =
    task {
        let results = ResizeArray<SessionEvent>()
        let enumerator = enumerable.GetAsyncEnumerator(CancellationToken.None)
        let mutable more = true
        let mutable count = 0

        while more && count < take do
            let! has = enumerator.MoveNextAsync().AsTask()
            more <- has

            if has then
                results.Add(enumerator.Current)
                count <- count + 1

        do! enumerator.DisposeAsync().AsTask()

        return results :> IReadOnlyList<SessionEvent>
    }

[<Fact>]
let ``Subscription options default to 512 subscribers and validate`` () =
    let options = SessionSubscriptionOptions()
    options.MaxSubscribersPerSession |> should equal 512
    options.PerSubscriberBufferSize |> should equal 128
    options.Validate() |> should equal null

    let capped = SessionSubscriptionOptions()
    capped.MaxSubscribersPerSession <- 0
    capped.Validate() |> should equal "MaxSubscribersPerSession must be at least 1."

    let unbuffered = SessionSubscriptionOptions()
    unbuffered.PerSubscriberBufferSize <- 0

    unbuffered.Validate()
    |> should equal "PerSubscriberBufferSize must be at least 1."

[<Fact>]
let ``New subscription exceptions derive from LegateException`` () =
    let session = SessionId.New()

    typeof<SessionJournalExpiredException>.IsSubclassOf(typeof<LegateException>)
    |> should equal true

    typeof<SessionSubscriptionLaggedException>.IsSubclassOf(typeof<LegateException>)
    |> should equal true

    typeof<SessionSubscriptionLimitExceededException>.IsSubclassOf(typeof<LegateException>)
    |> should equal true

    let expired = SessionJournalExpiredException(session, "gone")
    expired.SessionId |> should equal session

    let lagged = SessionSubscriptionLaggedException(session, "slowSubscriber", "slow")
    lagged.Reason |> should equal "slowSubscriber"

    let capped = SessionSubscriptionLimitExceededException(session, 2, "full")
    capped.Limit |> should equal 2

[<Fact>]
let ``ReadEvents pages by cursor and returns empty at end of stream`` () =
    task {
        let _, sessions, events = makeStores ()
        let tenant = tenantOf "acme"
        use bus = new SessionEventBus(events)
        let! sessionId, claim = makeSession sessions tenant (SessionId.New())

        let batch =
            ([
                delta sessionId claim.TurnId "a"
                delta sessionId claim.TurnId "b"
            ]
            :> IReadOnlyList<_>)

        let! written = appendViaWriter events tenant sessionId claim.Token batch

        match written with
        | JournalWriter.JournalAppended _ -> ()
        | _ -> failwith "expected the append to land"

        let! first = bus.ReadEventsAsync(tenant, sessionId, 0L, 1, CancellationToken.None)
        first.Count |> should equal 1
        first[0].Sequence.Value |> should equal 1L

        let! second = bus.ReadEventsAsync(tenant, sessionId, 1L, 10, CancellationToken.None)
        second.Count |> should equal 1
        second[0].Sequence.Value |> should equal 2L

        let! tail = bus.ReadEventsAsync(tenant, sessionId, 2L, 10, CancellationToken.None)
        tail.Count |> should equal 0
    }

[<Fact>]
let ``ReadEvents maps unknown session and expired journal to typed throws`` () =
    task {
        let _, sessions, events = makeStores ()
        let tenant = tenantOf "acme"
        use bus = new SessionEventBus(events)
        let! sessionId, claim = makeSession sessions tenant (SessionId.New())

        let! _ = appendViaWriter events tenant sessionId claim.Token [ delta sessionId claim.TurnId "a" ]

        let unknown = SessionId.New()

        try
            let! _ = bus.ReadEventsAsync(tenant, unknown, 0L, 10, CancellationToken.None)
            failwith "expected SessionNotFoundException"
        with :? SessionNotFoundException as ex ->
            ex.SessionId |> should equal unknown

        let! granted =
            events.TryClaimCleanup(tenant, sessionId, "bus-worker", TimeSpan.FromMinutes 5., CancellationToken.None)

        let lease = (granted :?> EventCleanupClaimed).Claim
        let! _ = events.CompleteCleanup(tenant, sessionId, lease.Token, CancellationToken.None)

        try
            let! _ = bus.ReadEventsAsync(tenant, sessionId, 0L, 10, CancellationToken.None)
            failwith "expected SessionJournalExpiredException"
        with :? SessionJournalExpiredException as ex ->
            ex.SessionId |> should equal sessionId
    }

[<Fact>]
let ``Subscribe replays then goes live gap-free with no duplicates`` () =
    task {
        let _, sessions, events = makeStores ()
        let tenant = tenantOf "acme"
        use bus = new SessionEventBus(events)
        let! sessionId, claim = makeSession sessions tenant (SessionId.New())

        let! _ =
            appendViaWriter
                events
                tenant
                sessionId
                claim.Token
                ([
                    delta sessionId claim.TurnId "one"
                    delta sessionId claim.TurnId "two"
                ]
                :> IReadOnlyList<_>)

        let stream = bus.Subscribe(tenant, sessionId, 0L, CancellationToken.None)

        let! _ =
            appendViaWriter
                events
                tenant
                sessionId
                claim.Token
                ([
                    delta sessionId claim.TurnId "three"
                    delta sessionId claim.TurnId "four"
                    closed sessionId claim.TurnId
                ]
                :> IReadOnlyList<_>)

        let! received = collectAll stream
        let sequences = received |> Seq.map (fun evt -> evt.Sequence.Value) |> Seq.toList
        sequences |> should equal [ 1L; 2L; 3L; 4L; 5L ]
    }

[<Fact>]
let ``Subscribe handoff under concurrent append loses and duplicates nothing`` () =
    task {
        let _, sessions, events = makeStores ()
        let tenant = tenantOf "acme"
        use bus = new SessionEventBus(events)
        let! sessionId, claim = makeSession sessions tenant (SessionId.New())

        let! _ = appendViaWriter events tenant sessionId claim.Token [ delta sessionId claim.TurnId "seed" ]

        let stream = bus.Subscribe(tenant, sessionId, 0L, CancellationToken.None)
        let collect = collectTake stream 6

        let appendMore =
            appendViaWriter
                events
                tenant
                sessionId
                claim.Token
                ([
                    delta sessionId claim.TurnId "a"
                    delta sessionId claim.TurnId "b"
                    delta sessionId claim.TurnId "c"
                    delta sessionId claim.TurnId "d"
                    closed sessionId claim.TurnId
                ]
                :> IReadOnlyList<_>)

        let! _ = Task.WhenAll(collect :> Task, appendMore :> Task)

        let! received = collect
        let sequences = received |> Seq.map (fun evt -> evt.Sequence.Value) |> Seq.toList
        sequences |> should equal [ 1L; 2L; 3L; 4L; 5L; 6L ]
    }

[<Fact>]
let ``Slow subscriber disconnects with the typed lagged error instead of stalling`` () =
    task {
        let _, sessions, events = makeStores ()
        let tenant = tenantOf "acme"

        let options = SessionSubscriptionOptions()
        options.PerSubscriberBufferSize <- 2

        use bus = new SessionEventBus(events, options)
        let! sessionId, _ = makeSession sessions tenant (SessionId.New())

        let stream = bus.Subscribe(tenant, sessionId, 0L, CancellationToken.None)

        let turnId = TurnId.New()

        let publishTen () =
            for index in 1..10 do
                let stamped =
                    TextDeltaEvent(sessionId, turnId, Nullable(int64 index), stamp, $"e{index}") :> SessionEvent

                bus.Publish(tenant, sessionId, [ stamped ] :> IReadOnlyList<_>)

        publishTen ()

        try
            let! _ = collectAll stream
            failwith "expected SessionSubscriptionLaggedException"
        with :? SessionSubscriptionLaggedException as ex ->
            ex.SessionId |> should equal sessionId
            ex.Reason |> should equal "slowSubscriber"
    }

[<Fact>]
let ``Subscriber cap rejects past the limit`` () =
    task {
        let _, sessions, events = makeStores ()
        let tenant = tenantOf "acme"

        let options = SessionSubscriptionOptions()
        options.MaxSubscribersPerSession <- 2

        use bus = new SessionEventBus(events, options)
        let! sessionId, _ = makeSession sessions tenant (SessionId.New())

        let first = bus.Subscribe(tenant, sessionId, 0L, CancellationToken.None)
        let firstEnumerator = first.GetAsyncEnumerator(CancellationToken.None)
        let second = bus.Subscribe(tenant, sessionId, 0L, CancellationToken.None)
        let secondEnumerator = second.GetAsyncEnumerator(CancellationToken.None)

        try
            bus.Subscribe(tenant, sessionId, 0L, CancellationToken.None) |> ignore
            failwith "expected SessionSubscriptionLimitExceededException"
        with :? SessionSubscriptionLimitExceededException as ex ->
            ex.SessionId |> should equal sessionId
            ex.Limit |> should equal 2

        do! secondEnumerator.DisposeAsync().AsTask()
        do! firstEnumerator.DisposeAsync().AsTask()
    }

[<Fact>]
let ``Subscribe maps unknown session and expired journal distinctly`` () =
    task {
        let _, sessions, events = makeStores ()
        let tenant = tenantOf "acme"
        use bus = new SessionEventBus(events)
        let! sessionId, claim = makeSession sessions tenant (SessionId.New())

        let! _ = appendViaWriter events tenant sessionId claim.Token [ delta sessionId claim.TurnId "a" ]

        let unknown = SessionId.New()

        try
            let! _ = collectAll (bus.Subscribe(tenant, unknown, 0L, CancellationToken.None))
            failwith "expected SessionNotFoundException"
        with :? SessionNotFoundException as ex ->
            ex.SessionId |> should equal unknown

        let! granted =
            events.TryClaimCleanup(tenant, sessionId, "bus-worker", TimeSpan.FromMinutes 5., CancellationToken.None)

        let lease = (granted :?> EventCleanupClaimed).Claim
        let! _ = events.CompleteCleanup(tenant, sessionId, lease.Token, CancellationToken.None)

        try
            let! _ = collectAll (bus.Subscribe(tenant, sessionId, 0L, CancellationToken.None))
            failwith "expected SessionJournalExpiredException"
        with :? SessionJournalExpiredException as ex ->
            ex.SessionId |> should equal sessionId
    }

[<Fact>]
let ``Takeover loser appends nothing and publishes nothing`` () =
    task {
        let _, sessions, events = makeStores ()
        let tenant = tenantOf "acme"
        use bus = new SessionEventBus(events)
        let! sessionId, claim = makeSession sessions tenant (SessionId.New())

        let stream = bus.Subscribe(tenant, sessionId, 0L, CancellationToken.None)

        let! rejected = appendViaWriter events tenant sessionId "stale-token" [ delta sessionId claim.TurnId "loser" ]

        match rejected with
        | JournalWriter.JournalRejected _ -> ()
        | _ -> failwith "expected the stale write to reject"

        let! _ =
            appendViaWriter
                events
                tenant
                sessionId
                claim.Token
                ([
                    delta sessionId claim.TurnId "winner"
                    closed sessionId claim.TurnId
                ]
                :> IReadOnlyList<_>)

        let! received = collectAll stream
        let sequences = received |> Seq.map (fun evt -> evt.Sequence.Value) |> Seq.toList
        sequences |> should equal [ 1L; 2L ]
    }

[<Fact>]
let ``Tenants stay isolated on the bus and the store`` () =
    task {
        let _, sessions, events = makeStores ()
        let tenantA = tenantOf "alpha"
        let tenantB = tenantOf "beta"
        let sharedId = SessionId.New()
        use bus = new SessionEventBus(events)

        let! _, claimA = makeSession sessions tenantA sharedId
        let! _, claimB = makeSession sessions tenantB sharedId

        let! _ =
            appendViaWriter
                events
                tenantA
                sharedId
                claimA.Token
                [
                    delta sharedId claimA.TurnId "alpha-one"
                ]

        let! fromA = bus.ReadEventsAsync(tenantA, sharedId, 0L, 10, CancellationToken.None)
        fromA.Count |> should equal 1

        let! fromB = bus.ReadEventsAsync(tenantB, sharedId, 0L, 10, CancellationToken.None)
        fromB.Count |> should equal 0

        let! _ =
            appendViaWriter events tenantB sharedId claimB.Token ([ closed sharedId claimB.TurnId ] :> IReadOnlyList<_>)

        let! receivedB = collectAll (bus.Subscribe(tenantB, sharedId, 0L, CancellationToken.None))
        receivedB.Count |> should equal 1
        (receivedB[0] :? SessionClosedEvent) |> should equal true
    }
