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
open Xunit

// Completion re-drive service (issue 84): leased at-least-once delivery
// that survives restarts via the store, retention purge of delivered rows,
// takeover loser-zero-effects, and inline/re-drive dedupe on the shared
// idempotency key. All time advances through the TestClock the
// in-memory database reads: never sleeps.

let tenant = TenantId.Create "acme"

/// Recording sink that keeps every Notify payload: the test asserts
/// at-least-once delivery (every row delivered) and dedupe (one unique
/// key) separately.
type DedupingSink() =
    let received = ResizeArray<SessionCompletion>()
    let gate = obj ()

    interface ISessionCompletionSink with
        member _.Notify(completion: SessionCompletion) =
            lock gate (fun () -> received.Add(completion))

    /// Every Notify payload, in call order.
    member _.Received: IReadOnlyList<SessionCompletion> =
        received :> IReadOnlyList<SessionCompletion>

    /// How many distinct idempotency keys were delivered.
    member _.UniqueKeys: int =
        lock gate (fun () ->
            received
            |> Seq.map (fun completion -> completion.IdempotencyKey)
            |> Seq.distinct
            |> Seq.length)

/// A session row carrying the sink, the snapshot the re-driver resolves.
let sampleSessionWithSink (sink: ISessionCompletionSink) =
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
        Options = SessionOptions(CompletionSink = sink)
        PermissionGrants = ResizeArray<string>() :> IReadOnlyList<string>
    }

/// A session row with no sink: the re-driver resolves nothing.
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

[<Fact>]
let ``Redrive delivers a pending row at-least-once under the shared key`` () =
    task {
        let store, clock = createStore ()
        let sink = DedupingSink()

        let! created =
            store.CreateSession(tenant, sampleSessionWithSink (sink :> ISessionCompletionSink), CancellationToken.None)

        // The settlement crashed before its inline Notify: the row is
        // pending and the sink observed nothing.
        let! _ = store.EnqueueCompletionOutbox(tenant, sampleCompletion created.Id "key-1", CancellationToken.None)

        Assert.Equal(0, sink.Received.Count)

        do! CompletionRedriver.passOnceAsync store "redriver-a" (CompletionOptions()) clock CancellationToken.None

        Assert.Equal(1, sink.Received.Count)
        Assert.Equal("key-1", sink.Received[0].IdempotencyKey)
        Assert.Equal(created.Id, sink.Received[0].SessionId)

        // A second pass redelivers nothing: the row is marked delivered.
        do! CompletionRedriver.passOnceAsync store "redriver-a" (CompletionOptions()) clock CancellationToken.None

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

        let! created =
            first.CreateSession(tenant, sampleSessionWithSink (sink :> ISessionCompletionSink), CancellationToken.None)

        let! _ = first.EnqueueCompletionOutbox(tenant, sampleCompletion created.Id "key-1", CancellationToken.None)

        // The first owner claims the row, then its process dies before the
        // Notify lands.
        let! claimed = first.ClaimCompletionOutbox("crashed-owner", 10, TimeSpan.FromMinutes 5., CancellationToken.None)

        Assert.Single(claimed) |> ignore

        // A restarted service over the same store (same database) takes
        // over once the lease lapses.
        clock.Advance(TimeSpan.FromMinutes 6.)

        let restarted = InMemoryStoreFactory.sessionStore database

        do!
            CompletionRedriver.passOnceAsync
                restarted
                "restarted-owner"
                (CompletionOptions())
                clock
                CancellationToken.None

        Assert.Equal(1, sink.Received.Count)
        Assert.Equal("key-1", sink.Received[0].IdempotencyKey)
    }

[<Fact>]
let ``Delivered rows purge after the retention window while pending rows survive`` () =
    task {
        let store, clock = createStore ()
        let sink = DedupingSink()

        let! created =
            store.CreateSession(tenant, sampleSessionWithSink (sink :> ISessionCompletionSink), CancellationToken.None)

        let! _ = store.EnqueueCompletionOutbox(tenant, sampleCompletion created.Id "old", CancellationToken.None)

        do! CompletionRedriver.passOnceAsync store "redriver-a" (CompletionOptions()) clock CancellationToken.None

        Assert.Equal(1, sink.Received.Count)

        // A newer row stays pending while the delivered one ages out.
        let! _ = store.EnqueueCompletionOutbox(tenant, sampleCompletion created.Id "pending", CancellationToken.None)

        clock.Advance(TimeSpan.FromDays 8.)

        let cutoff = clock.GetUtcNow() - TimeSpan.FromDays 7.
        let! purged = store.PurgeDeliveredCompletions(cutoff, CancellationToken.None)

        purged |> should equal 1

        // The pending row redrives after the purge: retention never removes
        // pending rows, however old the delivered ones beside them are.
        do! CompletionRedriver.passOnceAsync store "redriver-a" (CompletionOptions()) clock CancellationToken.None

        Assert.Equal(2, sink.Received.Count)
        Assert.Equal("pending", sink.Received[1].IdempotencyKey)
    }

[<Fact>]
let ``Takeover loser redrives nothing: zero effects`` () =
    task {
        let store, clock = createStore ()
        let sink = DedupingSink()

        let! created =
            store.CreateSession(tenant, sampleSessionWithSink (sink :> ISessionCompletionSink), CancellationToken.None)

        let! _ = store.EnqueueCompletionOutbox(tenant, sampleCompletion created.Id "key-1", CancellationToken.None)

        // The loser claims the row, then stalls past its lease.
        let! _ = store.ClaimCompletionOutbox("loser", 10, TimeSpan.FromMinutes 5., CancellationToken.None)

        clock.Advance(TimeSpan.FromMinutes 6.)

        // The loser verifies false on its lapsed lease: fenced out before
        // any Notify, with zero effects (the row stays pending).
        let! loserLive = store.VerifyCompletionClaim(tenant, "key-1", "loser", CancellationToken.None)
        Assert.False(loserLive)

        let! loserMarked = store.MarkCompletionDelivered(tenant, "key-1", "loser", CancellationToken.None)
        Assert.False(loserMarked)

        let! untouched = store.ClaimCompletionOutbox("probe", 10, TimeSpan.FromMinutes 5., CancellationToken.None)

        Assert.Single(untouched) |> ignore

        // The winner's pass delivers and marks under its own live lease.
        clock.Advance(TimeSpan.FromMinutes 6.)

        do! CompletionRedriver.passOnceAsync store "winner" (CompletionOptions()) clock CancellationToken.None

        Assert.Equal(1, sink.Received.Count)

        // The loser's own pass claims nothing delivered, so zero sink
        // effects too.
        do! CompletionRedriver.passOnceAsync store "loser" (CompletionOptions()) clock CancellationToken.None

        Assert.Equal(1, sink.Received.Count)
        Assert.Equal(1, sink.UniqueKeys)
    }

[<Fact>]
let ``Inline and redrive share one key: the sink dedupes`` () =
    task {
        let store, clock = createStore ()
        let sink = DedupingSink()

        let! created =
            store.CreateSession(tenant, sampleSessionWithSink (sink :> ISessionCompletionSink), CancellationToken.None)

        let completion = sampleCompletion created.Id "key-1"

        let! _ = store.EnqueueCompletionOutbox(tenant, completion, CancellationToken.None)

        // The settlement's inline Notify lands first with the stored key.
        (sink :> ISessionCompletionSink).Notify(completion)

        // The re-drive overlaps with the same key: at-least-once delivery
        // the receiver deduplicates.
        do! CompletionRedriver.passOnceAsync store "redriver-a" (CompletionOptions()) clock CancellationToken.None

        Assert.Equal(2, sink.Received.Count)
        Assert.Equal(1, sink.UniqueKeys)
        Assert.Equal("key-1", sink.Received[0].IdempotencyKey)
        Assert.Equal("key-1", sink.Received[1].IdempotencyKey)
    }

[<Fact>]
let ``A sinkless session keeps its row pending for a later sink`` () =
    task {
        let store, clock = createStore ()

        let! created = store.CreateSession(tenant, sampleSessionWithoutSink (), CancellationToken.None)

        let! _ = store.EnqueueCompletionOutbox(tenant, sampleCompletion created.Id "key-1", CancellationToken.None)

        // No sink resolves: the pass delivers nothing and marks nothing,
        // so the row stays pending under the pass lease.
        do! CompletionRedriver.passOnceAsync store "redriver-a" (CompletionOptions()) clock CancellationToken.None

        let! stillLeased = store.VerifyCompletionClaim(tenant, "key-1", "redriver-a", CancellationToken.None)
        Assert.True(stillLeased)

        // Once that lease lapses the row is claimable again: pending rows
        // are never dropped, however many sinkless passes run.
        clock.Advance(TimeSpan.FromMinutes 2.)

        let! stillPending = store.ClaimCompletionOutbox("probe", 10, TimeSpan.FromMinutes 5., CancellationToken.None)

        Assert.Single(stillPending) |> ignore
        Assert.Equal("key-1", (Seq.head stillPending).IdempotencyKey)
    }
