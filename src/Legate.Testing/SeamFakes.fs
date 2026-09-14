// SPDX-License-Identifier: Apache-2.0
namespace Legate.Testing

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open Legate

/// An <see cref="T:Legate.ILlmDelay" /> that records every requested delay
/// and completes immediately: tests assert exact retry and backoff
/// timings against <see cref="P:Legate.Testing.RecordingDelay.Recorded" />
/// without sleeping. Shared fake for the runtime seams; the test-local
/// copies it replaces are gone.
type RecordingDelay() =

    let gate = obj ()
    let recorded = List<TimeSpan>()

    interface ILlmDelay with

        member _.Delay(delay, _cancellationToken) =
            lock gate (fun () -> recorded.Add delay)

            Task.CompletedTask

    /// The delays requested from the seam, in request order.
    member _.Recorded: IReadOnlyList<TimeSpan> =
        lock gate (fun () -> recorded :> IReadOnlyList<TimeSpan>)

    /// Clears the recording so a test segment starts from a known state.
    member _.Clear() = lock gate (fun () -> recorded.Clear())

/// An <see cref="T:Legate.ILlmRandom" /> driven by a seeded
/// <see cref="T:System.Random" />, so repeated runs are reproducible: the
/// same seed produces the same sequence of jitter and backoff values.
type SeededRandom(seed: int) =

    let gate = obj ()
    let random = Random seed

    /// Constructs the fake from the seed the test pins.
    new() = SeededRandom(0)

    interface ILlmRandom with

        member _.NextDouble() =
            lock gate (fun () -> random.NextDouble())

        member _.Next(maxValue: int) =
            if maxValue < 0 then
                raise (ArgumentOutOfRangeException(nameof maxValue, "The upper bound must be non-negative."))

            lock gate (fun () -> random.Next maxValue)
