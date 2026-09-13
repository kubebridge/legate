// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Threading
open System.Threading.Tasks

// System-backed implementations of the Abstractions seams. These are the only
// runtime types allowed to touch the underlying clock and random sources;
// everything else goes through TimeProvider, ILlmDelay, and ILlmRandom.

/// System-backed delay seam that waits on the injected
/// <see cref="T:System.TimeProvider" />, so hosts running under a virtual
/// clock (for example a fake time provider) get virtual-time waits.
/// <param name="timeProvider">The clock the delay waits on.</param>
type SystemLlmDelay(timeProvider: TimeProvider) =

    /// Initialises the delay seam over the machine's real clock.
    new() = SystemLlmDelay(TimeProvider.System)

    interface ILlmDelay with
        member _.Delay(delay, cancellationToken) =
            Task.Delay(delay, timeProvider, cancellationToken)

/// System-backed random seam over <see cref="T:System.Random" />, thread-safe
/// for singleton registration and seedable so repeated runs produce the same
/// sequence. <param name="random">The random source to wrap.</param>
type SystemLlmRandom(random: Random) =

    let gate = obj ()

    /// Initialises the seam with an explicit seed for reproducible sequences.
    /// <param name="seed">The seed that determines the value sequence.</param>
    new(seed: int) = SystemLlmRandom(Random(seed))

    /// Initialises the seam with a time-derived seed.
    new() = SystemLlmRandom(Random.Shared.Next())

    interface ILlmRandom with
        member _.NextDouble() =
            lock gate (fun () -> random.NextDouble())

        member _.Next(maxValue) =
            lock gate (fun () -> random.Next(maxValue))
