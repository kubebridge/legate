// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.CrossNodeSubscriptionTests

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Net
open System.Net.Sockets
open System.Threading
open System.Threading.Tasks
open Akka.Actor
open Akka.Cluster
open Akka.Configuration
open Akka.FSharp
open FsUnit.Xunit
open Legate
open Legate.Storage.InMemory
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Options
open Xunit

// Cross-node subscriptions (issue 133): the owning-entity subscriber
// registry with its bounded replay cache, the host-options bounds, and
// the cluster-mode Subscribe router streaming through the shard region
// with store-replay resume. Local mode keeps the process-local bus path
// unchanged (SessionEventBusTests covers it); these tests cover the
// entity side and the router, including rebind with no gaps.

let stamp = DateTimeOffset(2024, 6, 7, 8, 9, 10, TimeSpan.Zero)
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
                Title = "cross-node"
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
        let message = UserMessagePayload(UserMessage.Text("cross-node")) :> InboxPayload

        let! _ = sessions.AppendInboxMessage(tenant, created.Id, message, DeliveryMode.Queue, CancellationToken.None)

        let! claimed =
            sessions.ClaimNextTurn(
                tenant,
                created.Id,
                "cross-node-owner",
                TimeSpan.FromMinutes 5.,
                CancellationToken.None
            )

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

let collectTakeBounded (enumerable: IAsyncEnumerable<SessionEvent>) (take: int) (bound: TimeSpan) =
    task {
        let results = ResizeArray<SessionEvent>()
        use cts = new CancellationTokenSource(bound)
        let enumerator = enumerable.GetAsyncEnumerator(cts.Token)
        let mutable more = true
        let mutable count = 0

        try
            while more && count < take do
                let! has = enumerator.MoveNextAsync().AsTask()
                more <- has

                if has then
                    results.Add(enumerator.Current)
                    count <- count + 1
        finally
            try
                enumerator.DisposeAsync().AsTask() |> ignore
            with _ ->
                ()

        return results :> IReadOnlyList<SessionEvent>
    }

let collectBounded (enumerable: IAsyncEnumerable<SessionEvent>) (bound: TimeSpan) =
    task {
        let results = ResizeArray<SessionEvent>()
        use cts = new CancellationTokenSource(bound)
        let enumerator = enumerable.GetAsyncEnumerator(cts.Token)
        let mutable more = true

        try
            while more do
                let! has = enumerator.MoveNextAsync().AsTask()
                more <- has

                if has then
                    results.Add(enumerator.Current)
        finally
            try
                enumerator.DisposeAsync().AsTask() |> ignore
            with _ ->
                ()

        return results :> IReadOnlyList<SessionEvent>
    }

let private defaultOptions () = SessionSubscriptionOptions()

// ────────────────── Host-options bounds ──────────────────

[<Fact>]
let ``Subscription options default to 512 subscribers, 128 buffer, 256 cache, 1MiB payload`` () =
    let options = SessionSubscriptionOptions()
    options.MaxSubscribersPerSession |> should equal 512
    options.PerSubscriberBufferSize |> should equal 128
    options.ReplayCacheSize |> should equal 256
    options.MaxEventPayloadBytes |> should equal 1048576
    options.Validate() |> should equal null

[<Fact>]
let ``Subscription options validate the cache and payload bounds`` () =
    let cacheless = SessionSubscriptionOptions()
    cacheless.ReplayCacheSize <- 0
    cacheless.Validate() |> should equal "ReplayCacheSize must be at least 1."

    let payloadless = SessionSubscriptionOptions()
    payloadless.MaxEventPayloadBytes <- 0

    payloadless.Validate()
    |> should equal "MaxEventPayloadBytes must be at least 1."

[<Fact>]
let ``Sessions options carry the subscription bounds with matching defaults`` () =
    let sessions = SessionsOptions()
    sessions.MaxSubscribersPerSession |> should equal 512
    sessions.PerSubscriberBufferSize |> should equal 128
    sessions.SubscriptionReplayCacheSize |> should equal 256
    sessions.SubscriptionMaxEventPayloadBytes |> should equal 1048576
    sessions.Validate() |> should equal null

    SessionsOptions(MaxSubscribersPerSession = 0).Validate()
    |> should equal "MaxSubscribersPerSession must be at least 1."

    SessionsOptions(PerSubscriberBufferSize = 0).Validate()
    |> should equal "PerSubscriberBufferSize must be at least 1."

    SessionsOptions(SubscriptionReplayCacheSize = 0).Validate()
    |> should equal "SubscriptionReplayCacheSize must be at least 1."

    SessionsOptions(SubscriptionMaxEventPayloadBytes = 0).Validate()
    |> should equal "SubscriptionMaxEventPayloadBytes must be at least 1."

[<Fact>]
let ``Sessions options map onto the runtime subscription options`` () =
    let sessions = SessionsOptions()
    sessions.MaxSubscribersPerSession <- 4
    sessions.PerSubscriberBufferSize <- 8
    sessions.SubscriptionReplayCacheSize <- 16
    sessions.SubscriptionMaxEventPayloadBytes <- 32768

    let mapped = SessionClientWiring.subscriptionOptionsOf sessions
    mapped.MaxSubscribersPerSession |> should equal 4
    mapped.PerSubscriberBufferSize |> should equal 8
    mapped.ReplayCacheSize |> should equal 16
    mapped.MaxEventPayloadBytes |> should equal 32768
    mapped.Validate() |> should equal null

[<Fact>]
let ``Subscription bounds bind from the Legate configuration section`` () =
    let pairs =
        [
            "Legate:Sessions:MaxSubscribersPerSession", "4"
            "Legate:Sessions:PerSubscriberBufferSize", "8"
            "Legate:Sessions:SubscriptionReplayCacheSize", "16"
            "Legate:Sessions:SubscriptionMaxEventPayloadBytes", "32768"
        ]
        |> Seq.map (fun (key, value) -> KeyValuePair<string, string>(key, value))
        |> Seq.toArray

    let section =
        ConfigurationBuilder().AddInMemoryCollection(pairs).Build().GetSection("Legate")

    let options = LegateOptionsBinding.bind section
    options.Sessions.MaxSubscribersPerSession |> should equal 4
    options.Sessions.PerSubscriberBufferSize |> should equal 8
    options.Sessions.SubscriptionReplayCacheSize |> should equal 16
    options.Sessions.SubscriptionMaxEventPayloadBytes |> should equal 32768
    options.Validate() |> should equal null

    let mapped = SessionClientWiring.subscriptionOptionsOf options.Sessions
    mapped.MaxSubscribersPerSession |> should equal 4
    mapped.ReplayCacheSize |> should equal 16
    mapped.Validate() |> should equal null

[<Fact>]
let ``Subscription bounds reject invalid configuration values`` () =
    let pairs =
        [
            "Legate:Sessions:SubscriptionReplayCacheSize", "0"
        ]
        |> Seq.map (fun (key, value) -> KeyValuePair<string, string>(key, value))
        |> Seq.toArray

    let section =
        ConfigurationBuilder().AddInMemoryCollection(pairs).Build().GetSection("Legate")

    let ex =
        Assert.Throws<InvalidOperationException>(fun () -> LegateOptionsBinding.bind section |> ignore)

    ex.Message.Contains("SubscriptionReplayCacheSize") |> should equal true

// ────────────────── Entity registry ──────────────────

[<Fact>]
let ``Hub attach is idempotent per token and rejects past the cap`` () =
    let options = defaultOptions ()
    options.MaxSubscribersPerSession <- 2
    let hub = CrossNodeSubscriptions.SubscriptionHub(options)

    hub.TryAttach("alpha") |> should equal true
    hub.TryAttach("alpha") |> should equal true
    hub.SubscriberCount |> should equal 1

    hub.TryAttach("beta") |> should equal true
    hub.TryAttach("gamma") |> should equal false
    hub.SubscriberCount |> should equal 2

    hub.Detach("alpha")
    hub.SubscriberCount |> should equal 1
    hub.TryAttach("gamma") |> should equal true

    hub.Detach("missing") |> ignore
    hub.SubscriberCount |> should equal 2

[<Fact>]
let ``Hub replay cache evicts oldest first past the bound`` () =
    let options = defaultOptions ()
    options.ReplayCacheSize <- 3
    let hub = CrossNodeSubscriptions.SubscriptionHub(options)
    let sessionId = SessionId.New()
    let turnId = TurnId.New()

    let stamped i =
        TextDeltaEvent(sessionId, turnId, Nullable<int64>(i), stamp, $"e{i}") :> SessionEvent

    hub.AppendToCache([ stamped 1L; stamped 2L ] :> IReadOnlyList<_>)
    hub.AppendToCache([ stamped 3L; stamped 4L; stamped 5L ] :> IReadOnlyList<_>)
    hub.CacheCount |> should equal 3

    let cached = hub.ReadCached(0L, 10)

    cached
    |> Seq.map (fun evt -> evt.Sequence.Value)
    |> Seq.toList
    |> should equal [ 3L; 4L; 5L ]

    hub.CacheFloor() |> should equal (Some 3L)
    hub.CacheCeiling() |> should equal (Some 5L)

    let tail = hub.ReadCached(4L, 10)

    tail
    |> Seq.map (fun evt -> evt.Sequence.Value)
    |> Seq.toList
    |> should equal [ 5L ]

/// Asserts the first serve streamed both events from the store and
/// warmed the hub cache. Pure so the resumable test stays a
/// straight-line await plus a return.
let private checkFirstBatch
    (hub: CrossNodeSubscriptions.SubscriptionHub)
    (outcome: CrossNodeSubscriptions.CrossNodeBatchOutcome)
    =
    match outcome with
    | CrossNodeSubscriptions.BatchPage batch ->
        batch.Events.Count |> should equal 2
        batch.NextCursor |> should equal 2L
        batch.EndOfStream |> should equal true
        hub.CacheCount |> should equal 2
    | CrossNodeSubscriptions.BatchUnknownSession _ -> failwith "expected a page, not unknown session"
    | CrossNodeSubscriptions.BatchJournalExpired _ -> failwith "expected a page, not an expired journal"
    | CrossNodeSubscriptions.BatchSubscriberCapped _ -> failwith "expected a page, not a cap"
    | CrossNodeSubscriptions.BatchEventOversized _ -> failwith "expected a page, not an oversized event"

/// Asserts the second serve replayed both events from the hub cache.
/// Pure so the resumable test stays a straight-line await plus a return.
let private checkCachedBatch (cached: CrossNodeSubscriptions.CrossNodeBatchOutcome) =
    match cached with
    | CrossNodeSubscriptions.BatchPage batch ->
        batch.Events
        |> Seq.map (fun evt -> evt.Sequence.Value)
        |> Seq.toList
        |> should equal [ 1L; 2L ]
    | CrossNodeSubscriptions.BatchUnknownSession _ -> failwith "expected a cached page"
    | CrossNodeSubscriptions.BatchJournalExpired _ -> failwith "expected a cached page"
    | CrossNodeSubscriptions.BatchSubscriberCapped _ -> failwith "expected a cached page"
    | CrossNodeSubscriptions.BatchEventOversized _ -> failwith "expected a cached page"

/// Asserts the serve refused the oversized event before crossing. Pure
/// so the resumable test stays a straight-line await plus a return.
let private checkOversizedRefusal (sessionId: SessionId) (outcome: CrossNodeSubscriptions.CrossNodeBatchOutcome) =
    match outcome with
    | CrossNodeSubscriptions.BatchEventOversized(oversizedId, _, limit, observed) ->
        oversizedId |> should equal sessionId
        limit |> should equal 1
        (observed > 1) |> should equal true
    | CrossNodeSubscriptions.BatchPage _ -> failwith "expected an oversized refusal"
    | CrossNodeSubscriptions.BatchUnknownSession _ -> failwith "expected an oversized refusal"
    | CrossNodeSubscriptions.BatchJournalExpired _ -> failwith "expected an oversized refusal"
    | CrossNodeSubscriptions.BatchSubscriberCapped _ -> failwith "expected an oversized refusal"

/// Asserts the entity answered the subscribe with the two-event batch.
/// Pure so the resumable test stays a straight-line await plus a return.
let private checkEntityBatchReply (reply: obj) =
    match reply with
    | :? CrossNodeSubscriptions.CrossNodeEventBatch as batch ->
        batch.Events
        |> Seq.map (fun evt -> evt.Sequence.Value)
        |> Seq.toList
        |> should equal [ 1L; 2L ]

        batch.NextCursor |> should equal 2L
    | :? Exception as error -> failwith $"expected a batch but the entity answered {error.GetType().Name}"
    | _ -> failwith "expected a batch reply"

/// Polls the hub until the unsubscribe detaches or the bound lapses.
/// A straight-line loop task so the entity test keeps only awaits and
/// calls in its own resumable body.
let private waitForDetachAsync
    (hubs: ConcurrentDictionary<TenantId * string, CrossNodeSubscriptions.SubscriptionHub>)
    (tenant: TenantId)
    (sessionId: SessionId)
    : Task<bool> =
    task {
        let mutable detached = false

        for _ in 1..50 do
            if hubs[(tenant, sessionId.ToString())].SubscriberCount = 0 then
                detached <- true

            if not detached then
                do! Task.Delay(20)

        return detached
    }

[<Fact>]
let ``Serve batch prefers the cache and falls back to the store`` () =
    task {
        let _, sessions, events = makeStores ()
        let tenant = tenantOf "registry"
        let! sessionId, claim = makeSession sessions tenant (SessionId.New())
        let options = defaultOptions ()
        let hub = CrossNodeSubscriptions.SubscriptionHub(options)

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

        let! outcome =
            CrossNodeSubscriptions.serveBatchAsync (
                events,
                hub,
                tenant,
                sessionId,
                0L,
                100,
                1048576,
                CancellationToken.None
            )

        checkFirstBatch hub outcome

        let! cached =
            CrossNodeSubscriptions.serveBatchAsync (
                events,
                hub,
                tenant,
                sessionId,
                0L,
                100,
                1048576,
                CancellationToken.None
            )

        checkCachedBatch cached
    }

[<Fact>]
let ``Serve batch maps unknown session and expired journal distinctly`` () =
    task {
        let _, sessions, events = makeStores ()
        let tenant = tenantOf "registry-errors"
        let! sessionId, claim = makeSession sessions tenant (SessionId.New())
        let options = defaultOptions ()
        let hub = CrossNodeSubscriptions.SubscriptionHub(options)

        let! _ = appendViaWriter events tenant sessionId claim.Token [ delta sessionId claim.TurnId "a" ]

        let! unknown =
            CrossNodeSubscriptions.serveBatchAsync (
                events,
                hub,
                tenant,
                SessionId.New(),
                0L,
                100,
                1048576,
                CancellationToken.None
            )

        match unknown with
        | CrossNodeSubscriptions.BatchUnknownSession _ -> ()
        | CrossNodeSubscriptions.BatchPage _ -> failwith "expected unknown session"
        | CrossNodeSubscriptions.BatchJournalExpired _ -> failwith "expected unknown session"
        | CrossNodeSubscriptions.BatchSubscriberCapped _ -> failwith "expected unknown session"
        | CrossNodeSubscriptions.BatchEventOversized _ -> failwith "expected unknown session"

        let! granted =
            events.TryClaimCleanup(
                tenant,
                sessionId,
                "registry-worker",
                TimeSpan.FromMinutes 5.,
                CancellationToken.None
            )

        let lease = (granted :?> EventCleanupClaimed).Claim
        let! _ = events.CompleteCleanup(tenant, sessionId, lease.Token, null, CancellationToken.None)

        let! expired =
            CrossNodeSubscriptions.serveBatchAsync (
                events,
                hub,
                tenant,
                sessionId,
                0L,
                100,
                1048576,
                CancellationToken.None
            )

        match expired with
        | CrossNodeSubscriptions.BatchJournalExpired expiredId -> expiredId |> should equal sessionId
        | CrossNodeSubscriptions.BatchPage _ -> failwith "expected an expired journal"
        | CrossNodeSubscriptions.BatchUnknownSession _ -> failwith "expected an expired journal"
        | CrossNodeSubscriptions.BatchSubscriberCapped _ -> failwith "expected an expired journal"
        | CrossNodeSubscriptions.BatchEventOversized _ -> failwith "expected an expired journal"
    }

[<Fact>]
let ``Serve batch refuses oversized events before crossing`` () =
    task {
        let _, sessions, events = makeStores ()
        let tenant = tenantOf "registry-bounds"
        let! sessionId, claim = makeSession sessions tenant (SessionId.New())
        let options = defaultOptions ()
        let hub = CrossNodeSubscriptions.SubscriptionHub(options)

        let! _ = appendViaWriter events tenant sessionId claim.Token [ delta sessionId claim.TurnId "big" ]

        let! outcome =
            CrossNodeSubscriptions.serveBatchAsync (events, hub, tenant, sessionId, 0L, 100, 1, CancellationToken.None)

        checkOversizedRefusal sessionId outcome
    }

[<Fact>]
let ``Batch outcomes map onto the subscriber throws`` () =
    let tenant = tenantOf "registry-throws"
    let sessionId = SessionId.New()

    CrossNodeSubscriptions.raiseForOutcome
        tenant
        (CrossNodeSubscriptions.BatchPage(
            {
                SessionId = sessionId
                Events = Array.Empty<SessionEvent>() :> IReadOnlyList<SessionEvent>
                NextCursor = 0L
                EndOfStream = true
            }
        ))

    try
        CrossNodeSubscriptions.raiseForOutcome tenant (CrossNodeSubscriptions.BatchUnknownSession sessionId)
        failwith "expected SessionNotFoundException"
    with :? SessionNotFoundException as ex ->
        ex.SessionId |> should equal sessionId

    try
        CrossNodeSubscriptions.raiseForOutcome tenant (CrossNodeSubscriptions.BatchJournalExpired sessionId)
        failwith "expected SessionJournalExpiredException"
    with :? SessionJournalExpiredException as ex ->
        ex.SessionId |> should equal sessionId

    try
        CrossNodeSubscriptions.raiseForOutcome tenant (CrossNodeSubscriptions.BatchSubscriberCapped(sessionId, 2))
        failwith "expected SessionSubscriptionLimitExceededException"
    with :? SessionSubscriptionLimitExceededException as ex ->
        ex.Limit |> should equal 2

    try
        CrossNodeSubscriptions.raiseForOutcome tenant (CrossNodeSubscriptions.BatchEventOversized(sessionId, 9L, 8, 16))
        failwith "expected EventLimitExceededException"
    with :? EventLimitExceededException as ex ->
        ex.LimitKind |> should equal "perEventBytes"

[<Fact>]
let ``Tenants stay isolated through the entity serve path`` () =
    task {
        let _, sessions, events = makeStores ()
        let tenantA = tenantOf "serve-alpha"
        let tenantB = tenantOf "serve-beta"
        let sharedId = SessionId.New()
        let options = defaultOptions ()
        let hubA = CrossNodeSubscriptions.SubscriptionHub(options)
        let hubB = CrossNodeSubscriptions.SubscriptionHub(options)

        let! _, claimA = makeSession sessions tenantA sharedId
        let! _, _claimB = makeSession sessions tenantB sharedId

        let! _ =
            appendViaWriter
                events
                tenantA
                sharedId
                claimA.Token
                ([
                    delta sharedId claimA.TurnId "alpha-one"
                ]
                :> IReadOnlyList<_>)

        let! pageA =
            CrossNodeSubscriptions.serveBatchAsync (
                events,
                hubA,
                tenantA,
                sharedId,
                0L,
                100,
                1048576,
                CancellationToken.None
            )

        match pageA with
        | CrossNodeSubscriptions.BatchPage batch -> batch.Events.Count |> should equal 1
        | _ -> failwith "expected tenant A's page"

        let! pageB =
            CrossNodeSubscriptions.serveBatchAsync (
                events,
                hubB,
                tenantB,
                sharedId,
                0L,
                100,
                1048576,
                CancellationToken.None
            )

        match pageB with
        | CrossNodeSubscriptions.BatchPage batch -> batch.Events.Count |> should equal 0
        | _ -> failwith "expected tenant B's empty page"
    }

// ────────────────── Entity behavior ──────────────────

let private localSystem () : ActorSystem =
    let hocon =
        """
        akka {
          actor {
            provider = "local"
          }
          loglevel = "WARNING"
          stdout-loglevel = "WARNING"
        }
        """

    ActorSystem.Create("legate-cross-node-test", ConfigurationFactory.ParseString(hocon))

let private entityFor
    (system: ActorSystem)
    (sessionId: SessionId)
    (events: ISessionEventStore)
    (options: SessionSubscriptionOptions)
    (hubs: ConcurrentDictionary<TenantId * string, CrossNodeSubscriptions.SubscriptionHub>)
    : IActorRef =
    let deps: ClusterActorSystem.SubscriptionDeps =
        {
            EventStore = events
            Options = options
            Hubs = hubs
        }

    let props =
        ClusterActorSystem.entityPropsWithSubscriptions
            (sessionId.ToString())
            (fun _ context name -> spawn context name (actorOf (fun (_: obj) -> ())))
            deps

    system.ActorOf(props, $"cross-node-entity-{Guid.NewGuid():N}")

let private askEntity (entity: IActorRef) (message: obj) =
    task {
        use cts = new CancellationTokenSource(TimeSpan.FromSeconds 10.0)
        return! entity.Ask<obj>(message, cts.Token)
    }

[<Fact>]
let ``Entity serves subscribe batches and detaches on unsubscribe`` () =
    task {
        let system = localSystem ()

        try
            let _, sessions, events = makeStores ()
            let tenant = tenantOf "entity"
            let! sessionId, claim = makeSession sessions tenant (SessionId.New())
            let options = defaultOptions ()

            let hubs =
                ConcurrentDictionary<TenantId * string, CrossNodeSubscriptions.SubscriptionHub>()

            let entity = entityFor system sessionId events options hubs

            let! _ =
                appendViaWriter
                    events
                    tenant
                    sessionId
                    claim.Token
                    ([
                        delta sessionId claim.TurnId "one"
                        closed sessionId claim.TurnId
                    ]
                    :> IReadOnlyList<_>)

            let request: CrossNodeSubscriptions.CrossNodeSubscribeRequest =
                {
                    Tenant = tenant
                    SessionId = sessionId
                    FromSequence = 0L
                    SubscriberToken = "entity-sub"
                }

            let! reply = askEntity entity (request :> obj)

            checkEntityBatchReply reply

            hubs[(tenant, sessionId.ToString())].SubscriberCount |> should equal 1

            // Re-polling with the same token never consumes another slot.
            let! _ = askEntity entity (request :> obj)
            hubs[(tenant, sessionId.ToString())].SubscriberCount |> should equal 1

            let unsubscribe: CrossNodeSubscriptions.CrossNodeUnsubscribe =
                {
                    Tenant = tenant
                    SessionId = sessionId
                    SubscriberToken = "entity-sub"
                }

            entity.Tell(unsubscribe :> obj)

            let! detached = waitForDetachAsync hubs tenant sessionId
            detached |> should equal true
        finally
            system.Terminate().GetAwaiter().GetResult() |> ignore
    }

[<Fact>]
let ``Entity rejects past the subscriber cap with the typed limit error`` () =
    task {
        let system = localSystem ()

        try
            let _, sessions, events = makeStores ()
            let tenant = tenantOf "entity-cap"
            let! sessionId, claim = makeSession sessions tenant (SessionId.New())
            let options = defaultOptions ()
            options.MaxSubscribersPerSession <- 1

            let hubs =
                ConcurrentDictionary<TenantId * string, CrossNodeSubscriptions.SubscriptionHub>()

            let entity = entityFor system sessionId events options hubs

            let! _ = appendViaWriter events tenant sessionId claim.Token [ delta sessionId claim.TurnId "a" ]

            let first: CrossNodeSubscriptions.CrossNodeSubscribeRequest =
                {
                    Tenant = tenant
                    SessionId = sessionId
                    FromSequence = 0L
                    SubscriberToken = "first"
                }

            let! _ = askEntity entity (first :> obj)

            let second: CrossNodeSubscriptions.CrossNodeSubscribeRequest =
                {
                    Tenant = tenant
                    SessionId = sessionId
                    FromSequence = 0L
                    SubscriberToken = "second"
                }

            let! reply = askEntity entity (second :> obj)

            match reply with
            | :? SessionSubscriptionLimitExceededException as capped -> capped.Limit |> should equal 1
            | :? CrossNodeSubscriptions.CrossNodeEventBatch -> failwith "expected the cap, not a batch"
            | :? Exception as error -> failwith $"expected the cap but got {error.GetType().Name}"
            | _ -> failwith "expected the cap reply"
        finally
            system.Terminate().GetAwaiter().GetResult() |> ignore
    }

[<Fact>]
let ``Entity surfaces unknown sessions across the node boundary`` () =
    task {
        let system = localSystem ()

        try
            let _, _, events = makeStores ()
            let tenant = tenantOf "entity-unknown"
            let options = defaultOptions ()

            let hubs =
                ConcurrentDictionary<TenantId * string, CrossNodeSubscriptions.SubscriptionHub>()

            let entity = entityFor system (SessionId.New()) events options hubs

            let request: CrossNodeSubscriptions.CrossNodeSubscribeRequest =
                {
                    Tenant = tenant
                    SessionId = SessionId.New()
                    FromSequence = 0L
                    SubscriberToken = "unknown-sub"
                }

            let! reply = askEntity entity (request :> obj)

            match reply with
            | :? SessionNotFoundException as missing -> missing.SessionId |> should equal request.SessionId
            | :? CrossNodeSubscriptions.CrossNodeEventBatch -> failwith "expected unknown session, not a batch"
            | :? Exception as error -> failwith $"expected unknown session but got {error.GetType().Name}"
            | _ -> failwith "expected the unknown-session reply"
        finally
            system.Terminate().GetAwaiter().GetResult() |> ignore
    }

// ────────────────── Cluster router ──────────────────

let private stubResolver (entity: IActorRef) =
    { new ISessionResolver with
        member _.ResolveSessionAsync(sessionId: string, cancellationToken: CancellationToken) =
            if String.IsNullOrWhiteSpace sessionId then
                raise (ArgumentException("Session id must be a non-empty string.", nameof sessionId))

            cancellationToken.ThrowIfCancellationRequested()
            Task.FromResult entity
    }

[<Fact>]
let ``Router streams entity batches replay-then-live with no gaps`` () =
    task {
        let system = localSystem ()

        try
            let _, sessions, events = makeStores ()
            let tenant = tenantOf "router"
            let! sessionId, claim = makeSession sessions tenant (SessionId.New())
            let options = defaultOptions ()

            let hubs =
                ConcurrentDictionary<TenantId * string, CrossNodeSubscriptions.SubscriptionHub>()

            let entity = entityFor system sessionId events options hubs

            let router =
                ClusterSubscriptions.ClusterSubscribeRouter(stubResolver entity, events, options)

            let stream =
                (router :> ISubscribeRouter).Subscribe(tenant, sessionId, 0L, CancellationToken.None)

            let collect = collectBounded stream (TimeSpan.FromSeconds 15.0)

            let! _ =
                appendViaWriter
                    events
                    tenant
                    sessionId
                    claim.Token
                    ([
                        delta sessionId claim.TurnId "one"
                        delta sessionId claim.TurnId "two"
                        closed sessionId claim.TurnId
                    ]
                    :> IReadOnlyList<_>)

            let! received = collect

            received
            |> Seq.map (fun evt -> evt.Sequence.Value)
            |> Seq.toList
            |> should equal [ 1L; 2L; 3L ]
        finally
            system.Terminate().GetAwaiter().GetResult() |> ignore
    }

[<Fact>]
let ``Router resumes from its cursor with redelivery covering the gap`` () =
    task {
        let system = localSystem ()

        try
            let _, sessions, events = makeStores ()
            let tenant = tenantOf "router-resume"
            let! sessionId, claim = makeSession sessions tenant (SessionId.New())
            let options = defaultOptions ()

            let hubs =
                ConcurrentDictionary<TenantId * string, CrossNodeSubscriptions.SubscriptionHub>()

            let entity = entityFor system sessionId events options hubs

            let router =
                ClusterSubscriptions.ClusterSubscribeRouter(stubResolver entity, events, options)

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

            // Take the two journaled events, then dispose: the resume from
            // the last cursor must redeliver everything journaled since
            // with no gaps.
            let! first =
                collectTakeBounded
                    ((router :> ISubscribeRouter).Subscribe(tenant, sessionId, 0L, CancellationToken.None))
                    2
                    (TimeSpan.FromSeconds 15.0)

            first
            |> Seq.map (fun evt -> evt.Sequence.Value)
            |> Seq.toList
            |> should equal [ 1L; 2L ]

            let! _ =
                appendViaWriter
                    events
                    tenant
                    sessionId
                    claim.Token
                    ([
                        delta sessionId claim.TurnId "three"
                        closed sessionId claim.TurnId
                    ]
                    :> IReadOnlyList<_>)

            let! resumed =
                collectBounded
                    ((router :> ISubscribeRouter).Subscribe(tenant, sessionId, 2L, CancellationToken.None))
                    (TimeSpan.FromSeconds 15.0)

            resumed
            |> Seq.map (fun evt -> evt.Sequence.Value)
            |> Seq.toList
            |> should equal [ 3L; 4L ]
        finally
            system.Terminate().GetAwaiter().GetResult() |> ignore
    }

/// A resolver failing its first resolves: the router must fall back to
/// the store replay and rebind, observing no gaps.
type private FlakyResolver(inner: ISessionResolver, failures: int) =
    let mutable remaining = failures

    interface ISessionResolver with
        member _.ResolveSessionAsync(sessionId: string, cancellationToken: CancellationToken) =
            if remaining > 0 then
                remaining <- remaining - 1
                Task.FromException<IActorRef>(TimeoutException("The owner is unreachable."))
            else
                inner.ResolveSessionAsync(sessionId, cancellationToken)

[<Fact>]
let ``Router falls back to store replay on rebind with no gaps`` () =
    task {
        let system = localSystem ()

        try
            let _, sessions, events = makeStores ()
            let tenant = tenantOf "router-rebind"
            let! sessionId, claim = makeSession sessions tenant (SessionId.New())
            let options = defaultOptions ()

            let hubs =
                ConcurrentDictionary<TenantId * string, CrossNodeSubscriptions.SubscriptionHub>()

            let entity = entityFor system sessionId events options hubs

            let flaky = FlakyResolver(stubResolver entity, 3) :> ISessionResolver
            let router = ClusterSubscriptions.ClusterSubscribeRouter(flaky, events, options)

            let! _ =
                appendViaWriter
                    events
                    tenant
                    sessionId
                    claim.Token
                    ([
                        delta sessionId claim.TurnId "one"
                        delta sessionId claim.TurnId "two"
                        closed sessionId claim.TurnId
                    ]
                    :> IReadOnlyList<_>)

            let! received =
                collectBounded
                    ((router :> ISubscribeRouter).Subscribe(tenant, sessionId, 0L, CancellationToken.None))
                    (TimeSpan.FromSeconds 15.0)

            received
            |> Seq.map (fun evt -> evt.Sequence.Value)
            |> Seq.toList
            |> should equal [ 1L; 2L; 3L ]
        finally
            system.Terminate().GetAwaiter().GetResult() |> ignore
    }

[<Fact>]
let ``Router surfaces unknown sessions and expired journals`` () =
    task {
        let system = localSystem ()

        try
            let _, sessions, events = makeStores ()
            let tenant = tenantOf "router-errors"
            let! sessionId, claim = makeSession sessions tenant (SessionId.New())
            let options = defaultOptions ()

            let hubs =
                ConcurrentDictionary<TenantId * string, CrossNodeSubscriptions.SubscriptionHub>()

            let entity = entityFor system sessionId events options hubs

            let router =
                ClusterSubscriptions.ClusterSubscribeRouter(stubResolver entity, events, options)

            let unknown = SessionId.New()

            try
                let! _ =
                    collectBounded
                        ((router :> ISubscribeRouter).Subscribe(tenant, unknown, 0L, CancellationToken.None))
                        (TimeSpan.FromSeconds 15.0)

                failwith "expected SessionNotFoundException"
            with :? SessionNotFoundException as ex ->
                ex.SessionId |> should equal unknown

            let! _ = appendViaWriter events tenant sessionId claim.Token [ delta sessionId claim.TurnId "a" ]

            let! granted =
                events.TryClaimCleanup(
                    tenant,
                    sessionId,
                    "router-worker",
                    TimeSpan.FromMinutes 5.,
                    CancellationToken.None
                )

            let lease = (granted :?> EventCleanupClaimed).Claim
            let! _ = events.CompleteCleanup(tenant, sessionId, lease.Token, null, CancellationToken.None)

            try
                let! _ =
                    collectBounded
                        ((router :> ISubscribeRouter).Subscribe(tenant, sessionId, 0L, CancellationToken.None))
                        (TimeSpan.FromSeconds 15.0)

                failwith "expected SessionJournalExpiredException"
            with :? SessionJournalExpiredException as ex ->
                ex.SessionId |> should equal sessionId
        finally
            system.Terminate().GetAwaiter().GetResult() |> ignore
    }

[<Fact>]
let ``Subscribe without a router keeps the local bus path`` () =
    task {
        let _, sessions, events = makeStores ()
        let tenant = tenantOf "local-passthrough"
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
                    closed sessionId claim.TurnId
                ]
                :> IReadOnlyList<_>)

        let resolve _ _ =
            Task.FromException<IActorRef>(InvalidOperationException("The bus path never resolves an actor."))

        let client =
            new SessionClient(
                sessions,
                tenant,
                resolve,
                bus,
                TimeSpan.FromMinutes 5.0,
                SystemLlmDelay(TimeProvider.System),
                None
            )

        client.SubscribeRouter |> should equal None

        let! received =
            collectBounded
                (SessionClientOperations.Subscribe(client, sessionId, 0L, CancellationToken.None))
                (TimeSpan.FromSeconds 15.0)

        received
        |> Seq.map (fun evt -> evt.Sequence.Value)
        |> Seq.toList
        |> should equal [ 1L; 2L ]
    }

// ────────────────── Two-node loopback ──────────────────

/// Allocates a free loopback port through the OS.
let private freePort () : int =
    use listener = new TcpListener(IPAddress.Loopback, 0)
    listener.Start()
    (listener.LocalEndpoint :?> IPEndPoint).Port

/// Builds cluster options over the given mode configuration.
let private clusterOptions (configure: LegateOptions -> unit) : IOptions<LegateOptions> =
    let options = LegateOptions()
    configure options
    OptionsWrapper<LegateOptions>(options) :> IOptions<LegateOptions>

/// Awaits this node's own MemberUp with a named bound.
let private awaitUp (system: ActorSystem | null) (bound: TimeSpan) (what: string) =
    task {
        match box system with
        | null -> failwith $"The test cluster service has no running system before {what}."
        | :? ActorSystem as running ->
            let reached = TaskCompletionSource<Address>()

            Cluster
                .Get(running)
                .RegisterOnMemberUp(Action(fun () -> reached.TrySetResult(Cluster.Get(running).SelfAddress) |> ignore))

            try
                let! _ = reached.Task.WaitAsync(bound, CancellationToken.None)
                ()
            with :? TimeoutException ->
                failwith $"The test timed out waiting for {what}."
        | _ -> failwith $"The test cluster service has no running system before {what}."
    }

/// Starts one sharded node on the port with the shared journal and the
/// subscription wiring a hosted session entity needs. Roles decide
/// placement: session-role nodes host entities, role-less nodes hold
/// proxies.
let private startNode
    (port: int)
    (seeds: string list)
    (roles: string list)
    (events: ISessionEventStore)
    (options: SessionSubscriptionOptions)
    : ClusterActorSystemService =
    let service =
        ClusterActorSystemService(
            clusterOptions (fun root ->
                root.Cluster.Mode <- ClusterMode.StaticSeeds

                for role in roles do
                    root.Cluster.Roles.Add(role) |> ignore

                for seed in seeds do
                    root.Cluster.SeedNodes.Add(seed) |> ignore),
            TimeProvider.System
        )

    service.RemotingPort <- port
    service.SubscriptionEventStore <- Some events
    service.SubscriptionOptions <- Some options
    (service :> IHostedService).StartAsync(CancellationToken.None).GetAwaiter().GetResult()
    service

let private stopQuietly (service: ClusterActorSystemService) : unit =
    try
        (service :> IHostedService).StopAsync(CancellationToken.None).GetAwaiter().GetResult()
    with _ ->
        ()

/// A subscriber on node A observes the events of a session owned by node
/// B: the subscription routes through the session shard region, the wire
/// envelope carries the batches, and an unknown session surfaces as its
/// typed exception across the same path.
[<Fact>]
let ``Subscriber on node A observes a session owned by node B`` () =
    task {
        let _, sessions, events = makeStores ()
        let tenant = tenantOf "two-node"
        let options = defaultOptions ()
        let portA = freePort ()

        // Node A carries the api role (proxy only) and node B the session
        // role (entity owner): every session lands on B, so every
        // subscription from A crosses the node boundary. The seed node
        // names itself: an empty seed list never joins.
        let serviceA = startNode portA [ $"127.0.0.1:{portA}" ] [ "api" ] events options
        do! awaitUp serviceA.System (TimeSpan.FromSeconds 30.0) "node A to come Up"

        let portB = freePort ()
        let serviceB = startNode portB [ $"127.0.0.1:{portA}" ] [ "session" ] events options

        try
            do! awaitUp serviceB.System (TimeSpan.FromSeconds 30.0) "node B to join"

            let resolverA = serviceA :> ISessionResolver
            let! sessionId, claim = makeSession sessions tenant (SessionId.New())

            // The resolver fronts the entity with a node-local proxy, so
            // the owner check asks the proxy with the wire-safe string
            // marker: the entity answers its child, whose address proves
            // the session lives on node B.
            use warmCts = new CancellationTokenSource(TimeSpan.FromSeconds 60.0)
            let! proxy = resolverA.ResolveSessionAsync(sessionId.ToString(), warmCts.Token)
            let! child = proxy.Ask<IActorRef>(sessionId.ToString(), warmCts.Token)

            if child.Path.Address.Port.GetValueOrDefault(0) <> portB then
                failwith $"Expected the session child on port {portB} but resolved {child.Path} (node A port {portA})."

            let router = ClusterSubscriptions.ClusterSubscribeRouter(resolverA, events, options)

            let stream =
                (router :> ISubscribeRouter).Subscribe(tenant, sessionId, 0L, CancellationToken.None)

            let collect = collectBounded stream (TimeSpan.FromSeconds 30.0)

            let! _ =
                appendViaWriter
                    events
                    tenant
                    sessionId
                    claim.Token
                    ([
                        delta sessionId claim.TurnId "remote-one"
                        delta sessionId claim.TurnId "remote-two"
                        closed sessionId claim.TurnId
                    ]
                    :> IReadOnlyList<_>)

            let! received = collect

            received
            |> Seq.map (fun evt -> evt.Sequence.Value)
            |> Seq.toList
            |> should equal [ 1L; 2L; 3L ]

            let unknown = SessionId.New()

            try
                let! _ =
                    collectBounded
                        ((router :> ISubscribeRouter).Subscribe(tenant, unknown, 0L, CancellationToken.None))
                        (TimeSpan.FromSeconds 30.0)

                failwith "expected SessionNotFoundException across nodes"
            with :? SessionNotFoundException as ex ->
                ex.SessionId |> should equal unknown
        finally
            stopQuietly serviceB
            stopQuietly serviceA
    }
