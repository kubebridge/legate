// SPDX-License-Identifier: Apache-2.0
namespace Legate.Testing

open System

/// A manually advanced <see cref="T:System.TimeProvider" /> for
/// deterministic tests: the test sets the instant and advances it, and
/// the stores under test read it for lease expiry and timestamps.
/// Scheduling is deliberately minimal: tests advance the clock themselves
/// and assert the states they observe at each instant, rather than
/// relying on timer callbacks.
type TestClock(start: DateTimeOffset) =
    inherit TimeProvider()

    let mutable instant = start

    /// Constructs the clock at the given instant.
    new() = TestClock(DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero))

    /// Moves the clock forward by the given span.
    /// <param name="span">How far to advance; must be non-negative.</param>
    member _.Advance(span: TimeSpan) =
        if span < TimeSpan.Zero then
            raise (ArgumentOutOfRangeException(nameof span, "The clock only moves forward."))

        instant <- instant + span

    /// Moves the clock to the given instant; must not move backwards.
    /// <param name="target">The instant to jump to.</param>
    member _.JumpTo(target: DateTimeOffset) =
        if target < instant then
            raise (ArgumentOutOfRangeException(nameof target, "The clock only moves forward."))

        instant <- target

    /// The clock's current instant.
    member _.Instant = instant

    override _.GetUtcNow() = instant
