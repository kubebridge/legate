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
