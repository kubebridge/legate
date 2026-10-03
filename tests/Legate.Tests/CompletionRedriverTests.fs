// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.CompletionRedriverTests

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Legate.Storage.InMemory
open Legate.Testing
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Logging
open Microsoft.Extensions.Logging.Abstractions
open Xunit

// Completion re-drive service (issue 378): the sole external completion
// delivery path. Rows carry an immutable tenant destination snapshot;
// the redriver resolves the row's destination from host registration,
// awaits receiver acknowledgement, then marks delivered under the same
// fenced owner. Unsupported, unknown, or unavailable routes stay pending
// with safe diagnostics, never rerouted or marked.

let tenant = TenantId.Create "acme"

/// Recording sink that keeps every acknowledged payload: the test asserts
/// at-least-once delivery (every row delivered) and dedupe (one unique
/// key) separately.
type DedupingSink() =
    let received = ResizeArray<SessionCompletion>()
    let gate = obj ()

    interface ISessionCompletionSink with
        member _.NotifyAsync(completion: SessionCompletion, _cancellationToken: CancellationToken) =
            lock gate (fun () -> received.Add(completion))
            Task.CompletedTask

    /// Every acknowledged payload, in call order.
    member _.Received: IReadOnlyList<SessionCompletion> =
        received :> IReadOnlyList<SessionCompletion>

    /// How many distinct idempotency keys were delivered.
    member _.UniqueKeys: int =
        lock gate (fun () ->
            received
            |> Seq.map (fun completion -> completion.IdempotencyKey)
            |> Seq.distinct
            |> Seq.length)

/// A session row carrying a data-only destination id, the snapshot the
/// settlement persists. No live sink travels in options.
let sampleSessionWithDestination () =
    let options = SessionOptions()
    options.CompletionDestinationId <- "test-receiver"

    {
        Id = SessionId.New()
        Tenant = tenant
        AgentId = AgentId.New()
        Title = "headless"
        State = SessionState.Idle
        CurrentTurnId = Unchecked.defaultof<Nullable<TurnId>>
        CreatedAt = DateTimeOffset.MinValue
        UpdatedAt = DateTimeOffset.MinValue
        ClosedAt = Unchecked.defaultof<Nullable<DateTimeOffset>>
        WorkspaceBinding = null
        Options = options
        PermissionGrants = ResizeArray<string>() :> IReadOnlyList<string>
    }

/// A session row with no destination: settlement enqueues nothing.
let sampleSessionWithoutSink () =
    {
        Id = SessionId.New()
        Tenant = tenant
        AgentId = AgentId.New()
        Title = "interactive"
        State = SessionState.Idle
        CurrentTurnId = Unchecked.defaultof<Nullable<TurnId>>
        CreatedAt = DateTimeOffset.MinValue
        UpdatedAt = DateTimeOffset.MinValue
        ClosedAt = Unchecked.defaultof<Nullable<DateTimeOffset>>
        WorkspaceBinding = null
        Options = SessionOptions()
        PermissionGrants = ResizeArray<string>() :> IReadOnlyList<string>
    }

/// A completion carrying the key, the shape settlement enqueues.
let sampleCompletion (sessionId: SessionId) (idempotencyKey: string) : SessionCompletion =
    {
        SessionId = sessionId
        TurnResult =
            {
                AssistantText = "done"
                Status = TurnStatus.Completed
                Iterations = 1
                Usage = { InputTokens = 1L; OutputTokens = 2L }
                Outcome = null
            }
        Metadata = null
        IdempotencyKey = idempotencyKey
    }

/// A store over a fresh database on the clock, plus the clock.
let createStore () =
    let clock = TestClock()
    let store = InMemoryStoreFactory.sessionStore (InMemoryDatabase(clock))
    store, clock

/// A destination resolver over exactly the supplied tenant registrations.
let private routesFor (registrations: (TenantId * string * ISessionCompletionSink) list) =
    let services = ServiceCollection()

    for tenantId, destinationId, sink in registrations do
        services.AddKeyedSingleton<ISessionCompletionSink>(box (tenantId, destinationId), sink)
        |> ignore

    CompletionDestinations(services.BuildServiceProvider())

let private drive (store: ISessionStore) (routes: CompletionDestinations) (owner: string) (clock: TestClock) =
    CompletionRedriver.passOnceAsync
        store
        routes
        owner
        (CompletionOptions())
        clock
        (TurnLoopTests.NeverDelay() :> ILlmDelay)
        (NullLogger.Instance :> ILogger)
        CancellationToken.None

[<Fact>]
let ``Redrive delivers a pending row at-least-once under the shared key`` () =
    task {
        let store, clock = createStore ()
        let sink = DedupingSink()

        let routes =
            routesFor
                [
                    tenant, "test-receiver", (sink :> ISessionCompletionSink)
                ]

        let! created = store.CreateSession(tenant, sampleSessionWithDestination (), CancellationToken.None)

        // The settlement enqueued the immutable route snapshot; the sink
        // observed nothing until the redriver delivers.
        let! _ =
            store.EnqueueCompletionOutbox(
                tenant,
                "test-receiver",
                sampleCompletion created.Id "key-1",
                CancellationToken.None
            )

        Assert.Equal(0, sink.Received.Count)

        do! drive store routes "redriver-a" clock

        Assert.Equal(1, sink.Received.Count)
        Assert.Equal("key-1", sink.Received[0].IdempotencyKey)
        Assert.Equal(created.Id, sink.Received[0].SessionId)

        // A second pass redelivers nothing: the row is marked delivered.
        do! drive store routes "redriver-a" clock

        Assert.Equal(1, sink.Received.Count)
        Assert.Equal(1, sink.UniqueKeys)
    }

[<Fact>]
let ``Redelivery survives a restart: a new owner claims the expired lease`` () =
    task {
        let clock = TestClock()
        let database = InMemoryDatabase(clock)
        let first = InMemoryStoreFactory.sessionStore database
        let sink = DedupingSink()

        let routes =
            routesFor
                [
                    tenant, "test-receiver", (sink :> ISessionCompletionSink)
                ]

        let! created = first.CreateSession(tenant, sampleSessionWithDestination (), CancellationToken.None)

        let! _ =
            first.EnqueueCompletionOutbox(
                tenant,
                "test-receiver",
                sampleCompletion created.Id "key-1",
                CancellationToken.None
            )

        // The first owner claims the row, then its process dies before the
        // acknowledgement lands.
        let! claimed = first.ClaimCompletionOutbox("crashed-owner", 10, TimeSpan.FromMinutes 5., CancellationToken.None)

        Assert.Single(claimed) |> ignore

        // A restarted service over the same store (same database) takes
        // over once the lease lapses.
        clock.Advance(TimeSpan.FromMinutes 6.)

        let restarted = InMemoryStoreFactory.sessionStore database

        do! drive restarted routes "restarted-owner" clock

        Assert.Equal(1, sink.Received.Count)
        Assert.Equal("key-1", sink.Received[0].IdempotencyKey)
    }

[<Fact>]
let ``Delivered rows purge after the retention window while pending rows survive`` () =
    task {
        let store, clock = createStore ()
        let sink = DedupingSink()

        let routes =
            routesFor
                [
                    tenant, "test-receiver", (sink :> ISessionCompletionSink)
                ]

        let! created = store.CreateSession(tenant, sampleSessionWithDestination (), CancellationToken.None)

        let! _ =
            store.EnqueueCompletionOutbox(
                tenant,
                "test-receiver",
                sampleCompletion created.Id "old",
                CancellationToken.None
            )

        do! drive store routes "redriver-a" clock

        Assert.Equal(1, sink.Received.Count)

        // A newer row stays pending while the delivered one ages out.
        let! _ =
            store.EnqueueCompletionOutbox(
                tenant,
                "test-receiver",
                sampleCompletion created.Id "pending",
                CancellationToken.None
            )

        clock.Advance(TimeSpan.FromDays 8.)

        let cutoff = clock.GetUtcNow() - TimeSpan.FromDays 7.
        let! purged = store.PurgeDeliveredCompletions(cutoff, CancellationToken.None)

        purged |> should equal 1

        // The pending row redrives after the purge: retention never removes
        // pending rows, however old the delivered ones beside them are.
        do! drive store routes "redriver-a" clock

        Assert.Equal(2, sink.Received.Count)
        Assert.Equal("pending", sink.Received[1].IdempotencyKey)
    }

[<Fact>]
let ``Takeover loser redrives nothing: zero effects`` () =
    task {
        let store, clock = createStore ()
        let sink = DedupingSink()

        let routes =
            routesFor
                [
                    tenant, "test-receiver", (sink :> ISessionCompletionSink)
                ]

        let! created = store.CreateSession(tenant, sampleSessionWithDestination (), CancellationToken.None)

        let! _ =
            store.EnqueueCompletionOutbox(
                tenant,
                "test-receiver",
                sampleCompletion created.Id "key-1",
                CancellationToken.None
            )

        // The loser claims the row, then stalls past its lease.
        let! _ = store.ClaimCompletionOutbox("loser", 10, TimeSpan.FromMinutes 5., CancellationToken.None)

        clock.Advance(TimeSpan.FromMinutes 6.)

        // The loser verifies false on its lapsed lease: fenced out before
        // any acknowledgement, with zero effects (the row stays pending).
        let! loserLive = store.VerifyCompletionClaim(tenant, "key-1", "loser", CancellationToken.None)
        Assert.False(loserLive)

        let! loserMarked = store.MarkCompletionDelivered(tenant, "key-1", "loser", CancellationToken.None)
        Assert.False(loserMarked)

        let! untouched = store.ClaimCompletionOutbox("probe", 10, TimeSpan.FromMinutes 5., CancellationToken.None)

        Assert.Single(untouched) |> ignore

        // The winner's pass delivers and marks under its own live lease.
        clock.Advance(TimeSpan.FromMinutes 6.)

        do! drive store routes "winner" clock

        Assert.Equal(1, sink.Received.Count)

        // The loser's own pass claims nothing delivered, so zero sink
        // effects too.
        do! drive store routes "loser" clock

        Assert.Equal(1, sink.Received.Count)
        Assert.Equal(1, sink.UniqueKeys)
    }

[<Fact>]
let ``Failed acknowledgement keeps the row pending for durable redelivery`` () =
    task {
        let store, clock = createStore ()
        let sink = DedupingSink()

        let routes =
            routesFor
                [
                    tenant, "test-receiver", (sink :> ISessionCompletionSink)
                ]

        let! created = store.CreateSession(tenant, sampleSessionWithDestination (), CancellationToken.None)

        let completion = sampleCompletion created.Id "key-1"

        let! _ = store.EnqueueCompletionOutbox(tenant, "test-receiver", completion, CancellationToken.None)

        // A direct acknowledgement outside the redriver carries the stored
        // key; the redriver still delivers and marks exactly once.
        do! (sink :> ISessionCompletionSink).NotifyAsync(completion, CancellationToken.None)

        do! drive store routes "redriver-a" clock

        Assert.Equal(2, sink.Received.Count)
        Assert.Equal(1, sink.UniqueKeys)
        Assert.Equal("key-1", sink.Received[0].IdempotencyKey)
        Assert.Equal("key-1", sink.Received[1].IdempotencyKey)
    }

[<Fact>]
let ``An unknown destination keeps its row pending without delivery`` () =
    task {
        let store, clock = createStore ()
        // No host registered test-receiver on this redriver.
        let routes = routesFor []

        let! created = store.CreateSession(tenant, sampleSessionWithDestination (), CancellationToken.None)

        let! _ =
            store.EnqueueCompletionOutbox(
                tenant,
                "test-receiver",
                sampleCompletion created.Id "key-1",
                CancellationToken.None
            )

        // Unknown routing refuses the row: nothing delivered, nothing
        // marked, so the row stays pending for a correctly configured host.
        do! drive store routes "redriver-a" clock

        let! stillLeased = store.VerifyCompletionClaim(tenant, "key-1", "redriver-a", CancellationToken.None)
        Assert.True(stillLeased)

        // Once that lease lapses the row is claimable again: pending rows
        // are never dropped, however many refused passes run.
        clock.Advance(TimeSpan.FromMinutes 2.)

        let! stillPending = store.ClaimCompletionOutbox("probe", 10, TimeSpan.FromMinutes 5., CancellationToken.None)

        Assert.Single(stillPending) |> ignore
        Assert.Equal("key-1", (Seq.head stillPending).IdempotencyKey)
        Assert.Equal("test-receiver", (Seq.head stillPending).DestinationId)

        // Once the destination registers, the unchanged row redelivers.
        let sink = DedupingSink()

        let restored =
            routesFor
                [
                    tenant, "test-receiver", (sink :> ISessionCompletionSink)
                ]

        clock.Advance(TimeSpan.FromMinutes 6.)
        do! drive store restored "redriver-b" clock

        Assert.Equal(1, sink.Received.Count)
        Assert.Equal("key-1", sink.Received[0].IdempotencyKey)
    }
