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
                PermissionGrants = ResizeArray<string>() :> IReadOnlyList<string>
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
        let! _ = events.CompleteCleanup(tenant, sessionId, lease.Token, null, CancellationToken.None)

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

/// Runs one bus call and captures any exception instead of raising, so
/// the test's resumable body stays a straight-line await plus a return.
/// The call is deferred so synchronous throws are captured too.
let private captureCall (call: unit -> Task) : Task<exn option> =
    task {
        try
            do! call ()
            return None
        with ex ->
            return Some ex
    }

/// Asserts the captured outcome is the typed slow-subscriber lag naming
/// the session. Pure so the resumable test stays a straight-line await
/// plus a return.
let private checkLagged (sessionId: SessionId) (captured: exn option) =
    match captured with
    | Some(:? SessionSubscriptionLaggedException as ex) ->
        ex.SessionId |> should equal sessionId
        ex.Reason |> should equal "slowSubscriber"
    | Some unexpected -> failwith $"expected SessionSubscriptionLaggedException but got {unexpected.GetType().Name}"
    | None -> failwith "expected SessionSubscriptionLaggedException"

/// Attempts a third subscription synchronously and captures any refusal
/// instead of raising. Synchronous, so the resumable test keeps only
/// straight-line awaits around it.
let private tryThirdSubscribe (bus: SessionEventBus) (tenant: TenantId) (sessionId: SessionId) : exn option =
    try
        bus.Subscribe(tenant, sessionId, 0L, CancellationToken.None) |> ignore
        None
    with ex ->
        Some ex

/// Asserts the captured outcome is the typed subscriber-cap refusal.
/// Pure so the resumable test stays a straight-line await plus a return.
let private checkCapRefusal (sessionId: SessionId) (captured: exn option) =
    match captured with
    | Some(:? SessionSubscriptionLimitExceededException as ex) ->
        ex.SessionId |> should equal sessionId
        ex.Limit |> should equal 2
    | Some unexpected ->
        failwith $"expected SessionSubscriptionLimitExceededException but got {unexpected.GetType().Name}"
    | None -> failwith "expected SessionSubscriptionLimitExceededException"

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

        let! captured = captureCall (fun () -> collectAll stream :> Task)

        checkLagged sessionId captured
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

        let captured = tryThirdSubscribe bus tenant sessionId
        checkCapRefusal sessionId captured

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
        let! _ = events.CompleteCleanup(tenant, sessionId, lease.Token, null, CancellationToken.None)

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

// ──────────────────────────────────────────────────────────────────────────
// Logging scopes (issue 93)

/// One captured log line with the scopes active when it logged.
type private LoggedLine =
    {
        Level: string
        Text: string
        Scopes: (string * obj) list
    }

/// An ILogger capturing every entry with the scopes active at log time.
type private ScopeCapturingLogger() =
    let gate = obj ()
    let entries = ResizeArray<LoggedLine>()
    let stack = ResizeArray<(string * obj) list>()

    let toPairs (state: obj | null) : (string * obj) list =
        if isNull (box state) then
            []
        else
            match state with
            | :? IReadOnlyList<KeyValuePair<string, obj>> as kvs ->
                kvs |> Seq.map (fun kv -> kv.Key, kv.Value) |> List.ofSeq
            | :? IEnumerable<KeyValuePair<string, obj>> as kvs ->
                kvs |> Seq.map (fun kv -> kv.Key, kv.Value) |> List.ofSeq
            | _ -> []

    interface Microsoft.Extensions.Logging.ILogger with
        member _.BeginScope<'TState when 'TState: not null>(state: 'TState) : IDisposable =
            let pairs = toPairs (box state)
            lock gate (fun () -> stack.Add(pairs))

            { new IDisposable with
                member _.Dispose() =
                    lock gate (fun () ->
                        if stack.Count > 0 then
                            stack.RemoveAt(stack.Count - 1))
            }

        member _.IsEnabled(_) = true

        member _.Log<'TState>
            (
                logLevel: Microsoft.Extensions.Logging.LogLevel,
                _eventId: Microsoft.Extensions.Logging.EventId,
                state: 'TState,
                ex: exn,
                formatter: Func<'TState, exn, string>
            ) : unit =
            let text = formatter.Invoke(state, ex)
            let scopes = lock gate (fun () -> stack |> Seq.concat |> List.ofSeq)

            lock gate (fun () ->
                entries.Add(
                    {
                        Level = logLevel.ToString()
                        Text = text
                        Scopes = scopes
                    }
                ))

    /// Every captured line, oldest first.
    member _.Entries: LoggedLine list = lock gate (fun () -> entries |> List.ofSeq)

[<Fact>]
let ``Bus subscribe and publish carry all six scopes`` () =
    task {
        let _, sessions, events = makeStores ()
        let tenant = tenantOf "acme"
        let sessionId = SessionId.New()
        let! _, claim = makeSession sessions tenant sessionId
        let logger = ScopeCapturingLogger()

        use bus =
            new SessionEventBus(events, SessionSubscriptionOptions(), logger :> Microsoft.Extensions.Logging.ILogger)

        let! received =
            task {
                let! enumerator =
                    task {
                        let enumerable = bus.Subscribe(tenant, sessionId, 0L, CancellationToken.None)
                        return enumerable.GetAsyncEnumerator(CancellationToken.None)
                    }

                let! _ =
                    appendViaWriter
                        events
                        tenant
                        sessionId
                        claim.Token
                        ([ delta sessionId claim.TurnId "hello" ] :> IReadOnlyList<_>)

                let! has = enumerator.MoveNextAsync().AsTask()
                has |> should equal true
                let first = enumerator.Current
                do! enumerator.DisposeAsync().AsTask()
                return first
            }

        received |> should not' (equal null)

        let entries = logger.Entries
        entries |> should not' (equal [])

        for entry in entries do
            for key in
                [
                    LoggingScopes.SessionIdKey
                    LoggingScopes.TurnIdKey
                    LoggingScopes.AgentIdKey
                    LoggingScopes.TenantIdKey
                    LoggingScopes.AttemptKey
                    LoggingScopes.ClaimOwnerKey
                ] do
                entry.Scopes |> List.exists (fun (name, _) -> name = key) |> should equal true
    }
