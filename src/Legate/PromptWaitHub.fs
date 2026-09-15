// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Threading.Tasks

// Internal per-session waiter hub for PromptAndWait (issue 85): a FIFO
// settle queue fed by OnTurnSettled wired at actor spawn (both props
// sites), mirroring the HarnessSignals precedent in SessionHarness.
// Waiters queue per PromptAndWait call before the Queue prompt, so
// sequential turns never steal each other's signal and concurrent waits
// resolve in Queue settle order; a settle with no waiter still records.
type internal PromptWaitHub() =

    let gate = obj ()
    let settled = ResizeArray<TurnResult>()
    let waiters = Queue<TaskCompletionSource<TurnResult>>()

    /// Records a settled result and wakes its waiter when one waits.
    member _.ObserveSettled(result: TurnResult) =
        if not (isNull (box result)) then
            lock gate (fun () ->
                settled.Add(result)

                while waiters.Count > 0 && waiters.Peek().Task.IsCanceled do
                    waiters.Dequeue() |> ignore

                if waiters.Count > 0 then
                    waiters.Dequeue().TrySetResult(result) |> ignore)

    /// Queues a waiter for the next settle.
    member _.EnqueueSettle() : TaskCompletionSource<TurnResult> =
        lock gate (fun () ->
            let waiter = TaskCompletionSource<TurnResult>()
            waiters.Enqueue(waiter)
            waiter)

    /// Cancels a waiter the race abandoned (prompt failure, suspension,
    /// cancellation, or wait-bound lapse) and removes it from the queue so
    /// it never steals a later settle.
    member _.Cancel(waiter: TaskCompletionSource<TurnResult>) : unit =
        if isNull (box waiter) then
            raise (ArgumentNullException(nameof waiter))

        lock gate (fun () ->
            if waiters.Count > 0 then
                let remaining = ResizeArray<TaskCompletionSource<TurnResult>>()

                while waiters.Count > 0 do
                    let candidate = waiters.Dequeue()

                    if not (Object.ReferenceEquals(candidate, waiter)) then
                        remaining.Add(candidate)

                for candidate in remaining do
                    waiters.Enqueue(candidate)

            waiter.TrySetCanceled() |> ignore)

    /// Every settled result, in settle order.
    member _.Settled: IReadOnlyList<TurnResult> =
        lock gate (fun () -> ResizeArray<TurnResult>(settled) :> IReadOnlyList<TurnResult>)

    /// How many waiters currently queue for the next settle.
    member _.PendingWaiterCount: int = lock gate (fun () -> waiters.Count)

/// The process-wide per-session hubs the actor settle hooks fan out to.
// SessionActor's spawn sites (both props constructions) observe through
// ObserveSettled; SessionClient enqueues and cancels through GetOrAdd.
// SessionId is globally unique, so it keys the hub without the tenant.
module internal PromptWaitHubs =

    let private hubs = ConcurrentDictionary<SessionId, PromptWaitHub>()

    /// Returns the hub for a session, creating it when missing.
    let GetOrAdd (sessionId: SessionId) : PromptWaitHub =
        hubs.GetOrAdd(sessionId, fun _ -> PromptWaitHub())

    /// Records a settled result on the session's hub, creating the hub
    /// when no waiter ever queued (a settle with no waiter still records).
    let ObserveSettled (sessionId: SessionId) (result: TurnResult) : unit =
        (GetOrAdd sessionId).ObserveSettled(result)

    /// Drops every hub. Tests only: isolates static settle state between
    /// facts sharing the process.
    let Clear () : unit = hubs.Clear()
