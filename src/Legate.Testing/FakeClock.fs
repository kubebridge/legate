// SPDX-License-Identifier: Apache-2.0
namespace Legate.Testing

open System
open System.Collections.Generic
open System.Threading

/// One queued timer: its due instant, its repeat period (zero for a
/// one-shot), its callback, and its cancellation flag.
type internal FakeClockTimer =
    {
        mutable Due: DateTimeOffset
        mutable Period: TimeSpan
        Callback: TimerCallback
        mutable Cancelled: bool
    }

/// A manually advanced <see cref="T:System.TimeProvider" /> whose timers
/// fire when the test advances the clock: <see cref="M:System.TimeProvider.CreateTimer*" />
/// queues the callback and <see cref="M:Legate.Testing.FakeClock.Advance*" />
/// fires every timer whose due time has passed, in due-time order (ties in
/// scheduling order). Tests assert exact retry timings without sleeping.
///
/// <para>The clock coexists with <see cref="T:Legate.Testing.TestClock" />:
/// the store conformance suites stay pinned to TestClock's minimal
/// scheduling contract, while FakeClock is the callback-capable clock the
/// runtime fakes need. Callbacks fire outside the clock's internal lock,
/// so a callback may advance the clock or queue further timers.</para>
type FakeClock(start: DateTimeOffset) =
    inherit TimeProvider()

    let gate = obj ()

    let mutable instant = start

    let timers = List<FakeClockTimer>()

    /// Constructs the clock at the given instant.
    new() = FakeClock(DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero))

    /// The clock's current instant.
    member _.Instant = instant

    /// Moves the clock forward by the given span, firing every timer whose
    /// due time has passed, in due-time order (ties in scheduling order).
    /// Repeating timers re-arm at due + period and may fire again within
    /// the same advance when the span covers their next due time.
    /// <param name="span">How far to advance; must be non-negative.</param>
    member _.Advance(span: TimeSpan) =
        if span < TimeSpan.Zero then
            raise (ArgumentOutOfRangeException(nameof span, "The clock only moves forward."))

        lock gate (fun () -> instant <- instant + span)

        // A due timer fires once per Advance pass it becomes due in;
        // callbacks may queue or cancel timers, so each pass re-reads the
        // live queue and callbacks run outside the gate.
        let mutable progressed = true

        while progressed do
            progressed <- false

            let dueTimers =
                lock gate (fun () ->
                    timers
                    |> Seq.filter (fun timer -> not timer.Cancelled && timer.Due <= instant)
                    |> Seq.sortBy (fun timer -> timer.Due)
                    |> Seq.toList)

            for timer in dueTimers do
                // Re-arm a repeating timer before its callback runs, so a
                // callback that advances the clock sees consistent state;
                // a one-shot is removed from the queue.
                let repeat =
                    lock gate (fun () ->
                        if timer.Cancelled || timer.Due > instant then
                            false
                        elif timer.Period > TimeSpan.Zero then
                            timer.Due <- timer.Due + timer.Period
                            progressed <- true
                            true
                        else
                            timer.Cancelled <- true
                            timers.Remove timer |> ignore
                            true)

                if repeat then
                    timer.Callback.Invoke(null)

    /// Moves the clock to the given instant; must not move backwards.
    /// <param name="target">The instant to jump to.</param>
    member clock.JumpTo(target: DateTimeOffset) =
        if target < instant then
            raise (ArgumentOutOfRangeException(nameof target, "The clock only moves forward."))

        clock.Advance(target - instant)

    /// Reports how many timers are currently queued and not cancelled.
    member _.PendingTimerCount =
        lock gate (fun () -> timers |> Seq.filter (fun timer -> not timer.Cancelled) |> Seq.length)

    override _.GetUtcNow() = instant

    override _.CreateTimer(callback: TimerCallback, _state: obj, dueTime: TimeSpan, period: TimeSpan) : ITimer =
        if isNull (box callback) then
            raise (ArgumentNullException(nameof callback))

        if period < TimeSpan.Zero then
            raise (ArgumentOutOfRangeException(nameof period, "The timer period must not be negative."))

        let entry: FakeClockTimer =
            lock gate (fun () ->
                let created =
                    {
                        Due = instant + (if dueTime < TimeSpan.Zero then TimeSpan.Zero else dueTime)
                        Period = (if period < TimeSpan.Zero then TimeSpan.Zero else period)
                        Callback = callback
                        Cancelled = false
                    }

                timers.Add created
                created)

        { new ITimer with
            member _.Change(dueTime: TimeSpan, period: TimeSpan) =
                lock gate (fun () ->
                    entry.Due <- instant + (if dueTime < TimeSpan.Zero then TimeSpan.Zero else dueTime)

                    entry.Period <- (if period < TimeSpan.Zero then TimeSpan.Zero else period)
                    entry.Cancelled <- false)

                true

            member _.Dispose() =
                lock gate (fun () ->
                    entry.Cancelled <- true
                    timers.Remove entry |> ignore)

            member timer.DisposeAsync() : System.Threading.Tasks.ValueTask =
                timer.Dispose()

                System.Threading.Tasks.ValueTask()
        }
