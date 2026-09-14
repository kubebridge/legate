// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.SeamsTests

open System
open System.Diagnostics
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Legate.Testing
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Time.Testing
open Xunit

// ───────────────────────────────────────────────────────────────────────────
// SystemLlmDelay

[<Fact>]
let ``SystemLlmDelay waits at least the requested real duration`` () =
    let delay = SystemLlmDelay() :> ILlmDelay
    let requested = TimeSpan.FromMilliseconds 150.

    // CI runners can schedule the stopwatch start and the timer callback
    // with sub-millisecond skew in either direction, so Task.Delay can
    // observe its deadline just before the stopwatch's: a bare
    // `>= requested` flaked in CI with a 152 ms measurement against a
    // 150 ms request. Assert the at-least semantics with a small
    // scheduling grace instead.
    let schedulingGrace = TimeSpan.FromMilliseconds 10.

    let stopwatch = Stopwatch.StartNew()

    delay.Delay(requested, CancellationToken.None).GetAwaiter().GetResult()
    stopwatch.Stop()

    stopwatch.Elapsed >= requested - schedulingGrace |> should equal true

[<Fact>]
let ``SystemLlmDelay over a fake clock completes with zero real wall time`` () =
    let fake = FakeTimeProvider()
    let delay = SystemLlmDelay(fake) :> ILlmDelay

    let task = delay.Delay(TimeSpan.FromSeconds 30., CancellationToken.None)

    task.IsCompleted |> should equal false
    fake.Advance(TimeSpan.FromSeconds 30.)

    let sw = Stopwatch.StartNew()
    task.Wait(TimeSpan.FromSeconds 1.) |> should equal true
    sw.Stop()
    task.IsCompleted |> should equal true
    (sw.Elapsed < TimeSpan.FromSeconds 1.) |> should equal true

[<Fact>]
let ``FakeTimeProvider timer fires for lease-heartbeat semantics`` () =
    let fake = FakeTimeProvider()
    let mutable fired = 0

    use timer =
        fake.CreateTimer(
            (fun _ -> Interlocked.Increment(&fired) |> ignore),
            null,
            TimeSpan.FromMilliseconds 100.,
            TimeSpan.FromMilliseconds 100.
        )

    fired |> should equal 0
    fake.Advance(TimeSpan.FromMilliseconds 100.)
    fired |> should equal 1
    fake.Advance(TimeSpan.FromMilliseconds 300.)
    fired |> should equal 4

[<Fact>]
let ``SystemLlmDelay honours cancellation`` () =
    let delay = SystemLlmDelay() :> ILlmDelay
    use source = new CancellationTokenSource(TimeSpan.FromMilliseconds 50.)

    let aggregate () =
        try
            delay.Delay(TimeSpan.FromSeconds 30., source.Token).GetAwaiter().GetResult()
            "completed"
        with :? TaskCanceledException ->
            "cancelled"

    aggregate () |> should equal "cancelled"

// ───────────────────────────────────────────────────────────────────────────
// SystemLlmRandom

[<Fact>]
let ``SystemLlmRandom returns in-range values`` () =
    let random = SystemLlmRandom(1234) :> ILlmRandom

    for _ in 1..200 do
        let d = random.NextDouble()
        d >= 0.0 |> should equal true
        d < 1.0 |> should equal true

    for maxValue in [ 1; 2; 10; 100 ] do
        for _ in 1..50 do
            let n = random.Next(maxValue)
            n >= 0 |> should equal true
            n < maxValue |> should equal true

[<Fact>]
let ``SystemLlmRandom identical seeds produce identical sequences`` () =
    let randomA = SystemLlmRandom(99) :> ILlmRandom
    let randomB = SystemLlmRandom(99) :> ILlmRandom

    let sequenceA =
        [
            for _ in 1..20 -> randomA.NextDouble()
        ]

    let sequenceB =
        [
            for _ in 1..20 -> randomB.NextDouble()
        ]

    sequenceA |> should equal sequenceB

[<Fact>]
let ``SystemLlmRandom different seeds produce different sequences`` () =
    let randomA = SystemLlmRandom(1) :> ILlmRandom
    let randomB = SystemLlmRandom(2) :> ILlmRandom

    let sequenceA =
        [
            for _ in 1..20 -> randomA.NextDouble()
        ]

    let sequenceB =
        [
            for _ in 1..20 -> randomB.NextDouble()
        ]

    sequenceA |> should not' (equal sequenceB)

// ───────────────────────────────────────────────────────────────────────────
// AddLegate registration

[<Fact>]
let ``AddLegate resolves the seams as singletons`` () =
    let services = ServiceCollection()
    let provider = AddLegate(services).BuildServiceProvider()

    let clock = provider.GetService<TimeProvider>()
    let delay = provider.GetService<ILlmDelay>()
    let random = provider.GetService<ILlmRandom>()

    clock |> should not' (be null)
    delay |> should not' (be null)
    random |> should not' (be null)

    provider.GetService<TimeProvider>() |> should equal clock
    provider.GetService<ILlmDelay>() |> should equal delay
    provider.GetService<ILlmRandom>() |> should equal random

[<Fact>]
let ``AddLegate does not override host-registered seams`` () =
    let fake = FakeTimeProvider()

    let services = ServiceCollection()
    services.AddSingleton<TimeProvider>(fake) |> ignore
    services.AddSingleton<ILlmDelay>(RecordingDelay() :> ILlmDelay) |> ignore

    let provider = AddLegate(services).BuildServiceProvider()

    provider.GetService<TimeProvider>() |> should equal fake
    let delay = provider.GetService<ILlmDelay>()
    (delay :? RecordingDelay) |> should equal true

[<Fact>]
let ``AddLegate delay composes with the registered clock`` () =
    let fake = FakeTimeProvider()
    let services = ServiceCollection()
    services.AddSingleton<TimeProvider>(fake) |> ignore
    let provider = AddLegate(services).BuildServiceProvider()

    let delay = provider.GetService<ILlmDelay>() |> nonNull
    let task = delay.Delay(TimeSpan.FromMinutes 1., CancellationToken.None)

    task.IsCompleted |> should equal false
    fake.Advance(TimeSpan.FromMinutes 1.)
    task.IsCompleted |> should equal true

[<Fact>]
let ``RecordingDelay records every requested delay`` () =
    let recording = RecordingDelay()
    let delay = recording :> ILlmDelay

    delay.Delay(TimeSpan.FromSeconds 1., CancellationToken.None).GetAwaiter().GetResult()
    delay.Delay(TimeSpan.FromSeconds 2., CancellationToken.None).GetAwaiter().GetResult()

    recording.Recorded
    |> Seq.toList
    |> should
        equal
        [
            TimeSpan.FromSeconds 1.
            TimeSpan.FromSeconds 2.
        ]
