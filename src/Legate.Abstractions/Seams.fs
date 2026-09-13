// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Threading
open System.Threading.Tasks

/// Waits on behalf of the runtime (retry backoff, cooldowns, suspension
/// pauses) so leases and retries are testable without sleeping. Runtime code
/// injects this instead of calling <c>Task.Delay</c> directly; the
/// system-backed default waits on the injected
/// <see cref="T:System.TimeProvider" />.
type ILlmDelay =
    /// Waits for <paramref name="delay" /> before completing the task, using
    /// the seam's clock, and honours <paramref name="cancellationToken" />.
    /// <param name="delay">How long to wait before the task completes.</param>
    /// <param name="cancellationToken">Token that abandons the wait.</param>
    /// <returns>A task that completes once the requested delay has elapsed.</returns>
    abstract Delay: delay: TimeSpan * cancellationToken: CancellationToken -> Task

/// Supplies random values for jitter and backoff so repeated runs are
/// reproducible under test. Runtime code injects this instead of touching
/// <see cref="T:System.Random" /> or <see cref="P:System.Random.Shared" />
/// directly.
type ILlmRandom =
    /// Returns a random floating-point value in the range [0.0, 1.0).
    /// <returns>A double greater than or equal to 0.0 and less than 1.0.</returns>
    abstract NextDouble: unit -> float

    /// Returns a random integer in the range [0, maxValue).
    /// <param name="maxValue">Exclusive upper bound; must be non-negative.</param>
    /// <returns>An integer greater than or equal to 0 and less than maxValue.</returns>
    abstract Next: maxValue: int -> int
