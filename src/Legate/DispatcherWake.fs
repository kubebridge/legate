// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks

// Coalesced in-process wake sink (issue 120). Hosts tell the dispatcher a
// session has pending work without touching the poll: RequestWake records
// the session and pulses the service loop, which drains the pending set at
// the start of its next cycle and runs its authoritative sweep. Coalesced:
// at most one wake per session per cycle, however many requests arrive
// between drains. Latency only: a wake never starts work itself, so it can
// neither duplicate nor start out of turn; the poll pass and the actor's
// Idle re-check stay the single authority. No external services: one lock,
// one set, one task source.

// ──────────────────────────────────────────────────────────────────────────
// Wake sink

/// The coalesced in-process wake sink the dispatcher service drains. A
/// singleton in the container; hosts and the prompt path share it.
type internal DispatcherWakeSink() =

    let gate = obj ()
    let mutable pending = HashSet<SessionId>()

    let mutable signal =
        new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously)

    /// Records a wake hint for the session and pulses the service loop.
    /// Coalesces: repeated requests for one session between drains keep a
    /// single entry.
    /// <param name="sessionId">The session with pending work.</param>
    member _.RequestWake(sessionId: SessionId) : unit =
        lock gate (fun () ->
            pending.Add(sessionId) |> ignore
            signal.TrySetResult(true) |> ignore)

    /// Drains the pending wakes and resets the pulse, starting the next
    /// cycle's coalescing window. The drained ids ride along for
    /// observability only: the sweep that follows stays authoritative over
    /// every pending session, woken or not.
    /// <returns>The sessions woken since the last drain, at most once each.</returns>
    member _.TakePending() : IReadOnlyList<SessionId> =
        lock gate (fun () ->
            let drained = ResizeArray<SessionId>(pending) :> IReadOnlyList<SessionId>
            pending.Clear()

            signal <- new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously)

            drained)

    /// Waits for the next wake pulse: completes when a RequestWake lands
    /// after the last TakePending, or when the token cancels. The service
    /// loop races this against its poll-interval delay, so a wake only
    /// shortens the wait for the next sweep.
    /// <param name="cancellationToken">Abandons the wait.</param>
    /// <returns>A task that completes on the next wake.</returns>
    member _.WaitAsync(cancellationToken: CancellationToken) : Task =
        let pulse = lock gate (fun () -> signal.Task)
        pulse.WaitAsync(cancellationToken) :> Task
