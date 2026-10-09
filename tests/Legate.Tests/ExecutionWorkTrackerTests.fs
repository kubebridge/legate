// SPDX-License-Identifier: Apache-2.0
namespace Legate.Tests

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open Legate
open Microsoft.Extensions.Time.Testing
open Xunit

module ExecutionWorkTrackerTests =

    [<Fact>]
    let ``closed tracker rejects work without invoking the operation`` () =
        let tracker = new ExecutionWorkTracker()
        let mutable invoked = false

        tracker.CloseAdmission()

        Assert.Throws<SessionScopeRejectedException>(fun () ->
            tracker.Track(fun () ->
                invoked <- true
                Task.FromResult(()))
            |> ignore)
        |> ignore

        Assert.False(invoked)
        Assert.Equal(0, tracker.RunningCount)

    [<Fact>]
    let ``tracker drain retains a blocked runner until it completes`` () =
        let tracker = new ExecutionWorkTracker()

        let blocker =
            TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

        tracker.Track(fun () -> blocker.Task) |> ignore
        tracker.CloseAdmission()

        let drain =
            tracker.DrainAsync "ExecutionWork" (TimeSpan.FromSeconds 1.0) TimeProvider.System CancellationToken.None

        Assert.False(drain.IsCompleted)
        Assert.Equal(1, tracker.RunningCount)

        blocker.SetResult(())
        drain.GetAwaiter().GetResult()

        Assert.Equal(0, tracker.RunningCount)

    [<Fact>]
    let ``tracker timeout is surfaced and a later retry can drain`` () =
        let clock = FakeTimeProvider()
        let tracker = new ExecutionWorkTracker()

        let blocker =
            TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

        tracker.Track(fun () -> blocker.Task) |> ignore
        tracker.CloseAdmission()

        let drain =
            tracker.DrainAsync "ExecutionWork" (TimeSpan.FromSeconds 5.0) clock CancellationToken.None

        Assert.False(drain.IsCompleted)
        clock.Advance(TimeSpan.FromSeconds 5.0)

        Assert.Throws<AggregateException>(fun () -> drain.Wait(TimeSpan.FromSeconds 5.0) |> ignore)
        |> ignore

        Assert.Throws<DeadlineExceededException>(fun () -> drain.GetAwaiter().GetResult())
        |> ignore

        Assert.Equal(1, tracker.RunningCount)
        blocker.SetResult(())

        let retry =
            tracker.DrainAsync "ExecutionWork" (TimeSpan.FromSeconds 1.0) clock CancellationToken.None

        retry.GetAwaiter().GetResult()

        Assert.Equal(0, tracker.RunningCount)

    [<Fact>]
    let ``subscription lifetime links tokens disposes once and rejects after close`` () =
        let lifetime = new SessionSubscriptionLifetime()
        use subscribeCancellation = new CancellationTokenSource()
        let mutable received = CancellationToken.None
        let mutable disposed = 0

        let stream =
            lifetime.Wrap(
                subscribeCancellation.Token,
                fun token ->
                    received <- token

                    { new IAsyncEnumerable<SessionEvent> with
                        member _.GetAsyncEnumerator(_token) =
                            { new IAsyncEnumerator<SessionEvent> with
                                member _.Current = Unchecked.defaultof<SessionEvent>
                                member _.MoveNextAsync() = ValueTask<bool>(Task.FromResult false)

                                member _.DisposeAsync() =
                                    Interlocked.Increment(&disposed) |> ignore
                                    ValueTask.CompletedTask
                            }
                    }
            )

        let enumerator = stream.GetAsyncEnumerator(CancellationToken.None)
        Assert.False(received.IsCancellationRequested)

        subscribeCancellation.Cancel()
        Assert.True(received.IsCancellationRequested)

        enumerator.DisposeAsync().AsTask().GetAwaiter().GetResult()
        enumerator.DisposeAsync().AsTask().GetAwaiter().GetResult()
        Assert.Equal(1, disposed)

        lifetime.BeginClose()

        let close =
            lifetime.CloseAsync (TimeSpan.FromSeconds 1.0) TimeProvider.System CancellationToken.None

        close.GetAwaiter().GetResult()

        Assert.Throws<SessionScopeRejectedException>(fun () ->
            stream.GetAsyncEnumerator(CancellationToken.None) |> ignore)
        |> ignore
