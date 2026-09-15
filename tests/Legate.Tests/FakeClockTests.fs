// SPDX-License-Identifier: Apache-2.0
namespace Legate.Tests

open System
open System.Threading
open Legate
open Legate.Testing
open Xunit

module FakeClockTests =

    [<Fact>]
    let ``Advance fires due timers in due-time order`` () =
        let clock = FakeClock(DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero))
        let fired = ResizeArray<string>()

        let provider = clock :> TimeProvider

        // Schedule three timers out of due order: the earlier ones fire
        // first on advance.
        provider.CreateTimer(TimerCallback(fun _ -> fired.Add "late"), null, TimeSpan.FromSeconds 30., TimeSpan.Zero)
        |> ignore

        provider.CreateTimer(TimerCallback(fun _ -> fired.Add "early"), null, TimeSpan.FromSeconds 10., TimeSpan.Zero)
        |> ignore

        provider.CreateTimer(TimerCallback(fun _ -> fired.Add "middle"), null, TimeSpan.FromSeconds 20., TimeSpan.Zero)
        |> ignore

        clock.Advance(TimeSpan.FromSeconds 30.)

        Assert.Equal<string list>([ "early"; "middle"; "late" ], (fired :> seq<string>) |> Seq.toList)

    [<Fact>]
    let ``Advancing in steps fires timers exactly when due`` () =
        let clock = FakeClock()
        let provider = clock :> TimeProvider
        let fired = ResizeArray<DateTimeOffset>()

        provider.CreateTimer(
            TimerCallback(fun _ -> fired.Add clock.Instant),
            null,
            TimeSpan.FromMinutes 5.,
            TimeSpan.Zero
        )
        |> ignore

        clock.Advance(TimeSpan.FromMinutes 4.)
        Assert.Equal(0, fired.Count)

        clock.Advance(TimeSpan.FromMinutes 1.)
        Assert.Equal(1, fired.Count)
        Assert.Equal(DateTimeOffset(2026, 1, 1, 0, 5, 0, TimeSpan.Zero), fired[0])

    [<Fact>]
    let ``Repeating timers fire once per period within an advance`` () =
        let clock = FakeClock()
        let provider = clock :> TimeProvider
        let fired = ResizeArray<int>()

        provider.CreateTimer(
            TimerCallback(fun _ -> fired.Add fired.Count),
            null,
            TimeSpan.FromMinutes 10.,
            TimeSpan.FromMinutes 5.
        )
        |> ignore

        clock.Advance(TimeSpan.FromMinutes 15.)

        // Due at 10, re-armed to 15 by the period: two fires; the timer
        // re-arms for 20 and stays queued.
        Assert.Equal(2, fired.Count)
        Assert.Equal(1, clock.PendingTimerCount)

    [<Fact>]
    let ``JumpTo moves the clock forward and fires due timers`` () =
        let clock = FakeClock()
        let provider = clock :> TimeProvider
        let fired = ResizeArray<DateTimeOffset>()

        provider.CreateTimer(
            TimerCallback(fun _ -> fired.Add clock.Instant),
            null,
            TimeSpan.FromHours 2.,
            TimeSpan.Zero
        )
        |> ignore

        clock.JumpTo(DateTimeOffset(2026, 1, 1, 2, 0, 0, TimeSpan.Zero))

        Assert.Equal(1, fired.Count)
        Assert.Equal(DateTimeOffset(2026, 1, 1, 2, 0, 0, TimeSpan.Zero), clock.Instant)

    [<Fact>]
    let ``A disposed timer never fires`` () =
        let clock = FakeClock()
        let provider = clock :> TimeProvider
        let fired = ResizeArray<int>()

        let timer =
            provider.CreateTimer(TimerCallback(fun _ -> fired.Add 1), null, TimeSpan.FromSeconds 5., TimeSpan.Zero)

        timer.Dispose()

        clock.Advance(TimeSpan.FromHours 1.)

        Assert.Equal(0, fired.Count)
        Assert.Equal(0, clock.PendingTimerCount)

    [<Fact>]
    let ``An infinite period is a one-shot per the TimeProvider contract`` () =
        let clock = FakeClock()
        let provider = clock :> TimeProvider
        let fired = ResizeArray<int>()

        // Task.Delay over a custom clock passes InfiniteTimeSpan as the
        // period: CreateTimer must accept it as a one-shot, firing once
        // and leaving nothing queued.
        use timer =
            provider.CreateTimer(
                TimerCallback(fun _ -> fired.Add 1),
                null,
                TimeSpan.FromSeconds 5.,
                Timeout.InfiniteTimeSpan
            )

        clock.Advance(TimeSpan.FromSeconds 5.)
        Assert.Equal(1, fired.Count)

        clock.Advance(TimeSpan.FromHours 1.)
        Assert.Equal(1, fired.Count)
        Assert.Equal(0, clock.PendingTimerCount)

    [<Fact>]
    let ``The clock rejects moving backwards`` () =
        let clock = FakeClock(DateTimeOffset(2026, 1, 1, 12, 0, 0, TimeSpan.Zero))

        Assert.Throws<ArgumentOutOfRangeException>(fun () -> clock.Advance(TimeSpan.FromMinutes -1.) |> ignore)
        |> ignore

        Assert.Throws<ArgumentOutOfRangeException>(fun () ->
            clock.JumpTo(DateTimeOffset(2026, 1, 1, 11, 0, 0, TimeSpan.Zero)) |> ignore)
        |> ignore

module SeededRandomTests =

    [<Fact>]
    let ``The same seed produces the same sequence`` () =
        let first = SeededRandom(42) :> ILlmRandom
        let second = SeededRandom(42) :> ILlmRandom

        let firstSequence = [ for _ in 1..8 -> first.NextDouble() ]
        let secondSequence = [ for _ in 1..8 -> second.NextDouble() ]

        Assert.Equal<double list>(firstSequence, secondSequence)

    [<Fact>]
    let ``Different seeds diverge`` () =
        let first = SeededRandom(1) :> ILlmRandom
        let second = SeededRandom(2) :> ILlmRandom

        Assert.NotEqual<double>(first.NextDouble(), second.NextDouble())

    [<Fact>]
    let ``Next honours the exclusive upper bound`` () =
        let random = SeededRandom(7) :> ILlmRandom

        for _ in 1..64 do
            let value = random.Next 5
            Assert.True(value >= 0 && value < 5)

    [<Fact>]
    let ``Next rejects a negative bound`` () =
        let random = SeededRandom(7) :> ILlmRandom

        Assert.Throws<ArgumentOutOfRangeException>(fun () -> random.Next(-1) |> ignore)
        |> ignore

module RecordingDelayTests =

    [<Fact>]
    let ``Clear resets the recording`` () =
        let recording = RecordingDelay()
        let delay = recording :> ILlmDelay

        delay.Delay(TimeSpan.FromSeconds 3., CancellationToken.None) |> ignore

        Assert.Equal(1, Seq.length recording.Recorded)

        recording.Clear()

        Assert.Equal(0, Seq.length recording.Recorded)
