// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.PromptWaitHubTests

open System
open System.Threading.Tasks
open Legate
open Xunit

let private resultOf (text: string) : TurnResult =
    {
        AssistantText = text
        Status = TurnStatus.Completed
        Iterations = 1
        Usage = { InputTokens = 0L; OutputTokens = 0L }
        Outcome = null
    }

[<Fact>]
let ``An enqueued waiter receives the next settle`` () : Task =
    task {
        let hub = PromptWaitHub()
        let waiter = hub.EnqueueSettle()

        hub.ObserveSettled(resultOf "done")

        let! got = waiter.Task
        Assert.Equal("done", got.AssistantText)
        Assert.Equal(0, hub.PendingWaiterCount)
        Assert.Equal(1, hub.Settled.Count)
    }

[<Fact>]
let ``Waiters resolve in FIFO settle order`` () : Task =
    task {
        let hub = PromptWaitHub()
        let first = hub.EnqueueSettle()
        let second = hub.EnqueueSettle()

        hub.ObserveSettled(resultOf "one")
        hub.ObserveSettled(resultOf "two")

        let! gotFirst = first.Task
        let! gotSecond = second.Task
        Assert.Equal("one", gotFirst.AssistantText)
        Assert.Equal("two", gotSecond.AssistantText)
        Assert.Equal(0, hub.PendingWaiterCount)
        Assert.Equal(2, hub.Settled.Count)
    }

[<Fact>]
let ``A settle with no waiter still records`` () =
    let hub = PromptWaitHub()

    hub.ObserveSettled(resultOf "late")

    Assert.Equal(0, hub.PendingWaiterCount)
    Assert.Equal(1, hub.Settled.Count)
    Assert.Equal("late", hub.Settled[0].AssistantText)

[<Fact>]
let ``Cancelling a waiter never steals a later settle`` () : Task =
    task {
        let hub = PromptWaitHub()
        let abandoned = hub.EnqueueSettle()
        let live = hub.EnqueueSettle()

        hub.Cancel(abandoned)

        Assert.True(abandoned.Task.IsCanceled)
        Assert.Equal(1, hub.PendingWaiterCount)

        hub.ObserveSettled(resultOf "next")

        let! got = live.Task
        Assert.Equal("next", got.AssistantText)
        Assert.Equal(0, hub.PendingWaiterCount)
    }

[<Fact>]
let ``Settling skips waiters the race already abandoned`` () : Task =
    task {
        let hub = PromptWaitHub()
        let abandoned = hub.EnqueueSettle()
        let live = hub.EnqueueSettle()

        hub.Cancel(abandoned)
        hub.ObserveSettled(resultOf "next")

        Assert.True(abandoned.Task.IsCanceled)

        let! got = live.Task
        Assert.Equal("next", got.AssistantText)
    }

[<Fact>]
let ``The registry keys hubs by session id`` () =
    let first = SessionId.New()
    let second = SessionId.New()

    Assert.True(Object.ReferenceEquals(PromptWaitHubs.GetOrAdd first, PromptWaitHubs.GetOrAdd first))
    Assert.False(Object.ReferenceEquals(PromptWaitHubs.GetOrAdd first, PromptWaitHubs.GetOrAdd second))

[<Fact>]
let ``Cancelling a null waiter rejects`` () =
    Assert.Throws<ArgumentNullException>(fun () ->
        PromptWaitHub().Cancel(Unchecked.defaultof<TaskCompletionSource<TurnResult>>)
        |> ignore)
    |> ignore

// ──────────────────────────────────────────────────────────────
// Bounded transient state (issue 384): isolated tables, never the
// process singletons, so these facts stay parallel-safe.
// ──────────────────────────────────────────────────────────────

let private testTenant = TenantId.Create "transient-bounds"

let private smallLimits () : TransientWaitLimits =
    {
        MaxHubs = 1
        MaxSettledPerHub = 2
        MaxWaitersPerHub = 1
        MaxHintKeys = 1
        MaxObserversPerKey = 2
    }

[<Fact>]
let ``The settled ring retains the bound newest results`` () =
    let hub = PromptWaitHub(2, 64)

    hub.ObserveSettled(resultOf "one")
    hub.ObserveSettled(resultOf "two")
    hub.ObserveSettled(resultOf "three")

    Assert.Equal(2, hub.Settled.Count)
    Assert.Equal("two", hub.Settled[0].AssistantText)
    Assert.Equal("three", hub.Settled[1].AssistantText)

[<Fact>]
let ``A waiter queued past the cap rejects without parking`` () =
    let hub = PromptWaitHub(8, 1)
    let live = hub.EnqueueSettle()

    let rejected =
        Assert.Throws<AdmissionRejectedException>(fun () -> hub.EnqueueSettle() |> ignore)

    Assert.Equal("settleWaitersAtCapacity", rejected.Reason)
    Assert.Equal(1, hub.PendingWaiterCount)

    hub.Cancel(live)
    let retried = hub.EnqueueSettle()
    Assert.Equal(1, hub.PendingWaiterCount)
    hub.Cancel(retried)

[<Fact>]
let ``The hub table rejects past its cap and admits after release`` () =
    let table = HubTable(smallLimits ())
    let first = SessionId.New()
    let second = SessionId.New()

    let hub = table.GetOrAddScoped(testTenant, first)
    Assert.Equal(1, table.HubCount)

    let rejected =
        Assert.Throws<AdmissionRejectedException>(fun () -> table.GetOrAddScoped(testTenant, second) |> ignore)

    Assert.Equal("transientWaitHubsAtCapacity", rejected.Reason)

    Assert.True(table.ReleaseSession(testTenant, first))
    Assert.False(table.ReleaseSession(testTenant, first))
    Assert.Equal(0, table.HubCount)

    let fresh = table.GetOrAddScoped(testTenant, second)
    Assert.False(Object.ReferenceEquals(hub, fresh))

[<Fact>]
let ``The observe path never throws past the hub cap`` () =
    let table = HubTable(smallLimits ())
    table.GetOrAddScoped(testTenant, SessionId.New()) |> ignore

    // The actor settle path drops the transient record instead of failing
    // settlement: the durable row stays the truth.
    table.ObserveSettledScoped(testTenant, SessionId.New(), resultOf "dropped")
    Assert.Equal(1, table.HubCount)

[<Fact>]
let ``The hint table rejects past its key cap and admits after unsubscribe`` () =
    let table = HintTable(smallLimits ())
    let first = SessionId.New()
    let second = SessionId.New()

    let hint, unsubscribe = table.SubscribePosition(testTenant, first, 1L)
    Assert.Equal(1, table.KeyCount)
    Assert.Equal(1, table.ObserverCount)

    let rejected =
        Assert.Throws<AdmissionRejectedException>(fun () -> table.SubscribePosition(testTenant, second, 2L) |> ignore)

    Assert.Equal("positionHintsAtCapacity", rejected.Reason)
    Assert.False(hint.IsCompleted)

    unsubscribe ()
    Assert.Equal(0, table.KeyCount)

    let retried, unsub2 = table.SubscribePosition(testTenant, second, 2L)
    Assert.Equal(1, table.KeyCount)
    unsub2 ()
    Assert.False(retried.IsCompleted)

[<Fact>]
let ``The hint table rejects past its per-hint observer cap without evicting live observers`` () =
    let table = HintTable(smallLimits ())
    let session = SessionId.New()

    let first, unsub1 = table.SubscribePosition(testTenant, session, 1L)
    let second, unsub2 = table.SubscribePosition(testTenant, session, 1L)

    let rejected =
        Assert.Throws<AdmissionRejectedException>(fun () -> table.SubscribePosition(testTenant, session, 1L) |> ignore)

    Assert.Equal("positionHintObserversAtCapacity", rejected.Reason)
    Assert.False(first.IsCompleted)
    Assert.False(second.IsCompleted)
    Assert.Equal(2, table.ObserverCount)

    unsub1 ()
    unsub2 ()
    Assert.Equal(0, table.KeyCount)

[<Fact>]
let ``Notify wakes every live observer and duplicate notifies stay idempotent`` () =
    let table = HintTable(TransientWaitLimits.defaults ())
    let session = SessionId.New()

    let first, unsub1 = table.SubscribePosition(testTenant, session, 7L)
    let second, unsub2 = table.SubscribePosition(testTenant, session, 7L)

    table.NotifyPositionScoped(testTenant, session, 7L)

    Assert.True(first.IsCompleted)
    Assert.True(second.IsCompleted)
    Assert.Equal(0, table.KeyCount)

    // A duplicate notify with no subscribers is a no-op.
    table.NotifyPositionScoped(testTenant, session, 7L)

    unsub1 ()
    unsub2 ()

[<Fact>]
let ``A missed hint stays pending until a later notify wakes it`` () =
    let table = HintTable(TransientWaitLimits.defaults ())
    let session = SessionId.New()

    // The commit raced the subscribe and no notify fires: the observer
    // misses the hint and converges by re-reading the durable row, never
    // by manufacturing a verdict from the hint.
    table.NotifyPositionScoped(testTenant, session, 3L)

    let missed, unsubscribe = table.SubscribePosition(testTenant, session, 3L)
    Assert.False(missed.IsCompleted)

    table.NotifyPositionScoped(testTenant, session, 3L)
    Assert.True(missed.IsCompleted)
    unsubscribe ()

[<Fact>]
let ``Release wakes pending observers and drops the session keys`` () =
    let table = HintTable(TransientWaitLimits.defaults ())
    let session = SessionId.New()
    let other = SessionId.New()

    let first, unsub1 = table.SubscribePosition(testTenant, session, 1L)
    let second, unsub2 = table.SubscribePosition(testTenant, session, 2L)
    let foreign, unsub3 = table.SubscribePosition(testTenant, other, 1L)
    Assert.Equal(3, table.KeyCount)

    Assert.Equal(2, table.ReleaseSession(testTenant, session))
    Assert.True(first.IsCompleted)
    Assert.True(second.IsCompleted)
    Assert.False(foreign.IsCompleted)
    Assert.Equal(1, table.KeyCount)

    Assert.Equal(0, table.ReleaseSession(testTenant, session))

    unsub1 ()
    unsub2 ()
    unsub3 ()

[<Fact>]
let ``Unsubscribe removes only its own observer`` () =
    let table = HintTable(TransientWaitLimits.defaults ())
    let session = SessionId.New()

    let first, unsub1 = table.SubscribePosition(testTenant, session, 9L)
    let second, _unsub2 = table.SubscribePosition(testTenant, session, 9L)

    unsub1 ()
    Assert.Equal(1, table.ObserverCount)
    Assert.False(first.IsCompleted)
    Assert.False(second.IsCompleted)

    table.NotifyPositionScoped(testTenant, session, 9L)
    Assert.True(second.IsCompleted)

[<Fact>]
let ``Transient limits mirror the sessions options defaults`` () =
    let sessions = SessionsOptions()
    let limits = TransientWaitLimits.fromSessions sessions

    Assert.Equal(sessions.MaxWaitHubs, limits.MaxHubs)
    Assert.Equal(sessions.MaxSettledResultsPerHub, limits.MaxSettledPerHub)
    Assert.Equal(sessions.MaxSettleWaitersPerHub, limits.MaxWaitersPerHub)
    Assert.Equal(sessions.MaxPositionHints, limits.MaxHintKeys)
    Assert.Equal(sessions.MaxPositionHintObservers, limits.MaxObserversPerKey)

    let defaults = TransientWaitLimits.defaults ()
    Assert.Equal(defaults, limits)
