// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Threading.Tasks

// Internal per-session waiter hub for the legacy WaitForSettle path plus
// the issue 383 per-operation live hints: a FIFO settle queue fed by
// OnTurnSettled wired at actor spawn (both props sites), mirroring the
// HarnessSignals precedent in SessionHarness. The FIFO queue is the verdict
// source only for the legacy WaitForSettleAsync companion; receipt-bound
// waits (PromptAndWaitAsync, WaitForOperationAsync) resolve from the
// durable execution_settlements row and use the position-keyed hint
// registry below as a fast-path wake only. A settle with no waiter still
// records.
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

    /// Queues a waiter for the next settle. Waiters run their
    /// continuations asynchronously (the DispatcherWake precedent): the
    /// settle hook fires on the session actor thread, so an inline
    /// continuation would run host code on that thread and stall the
    /// settle it waits for.
    member _.EnqueueSettle() : TaskCompletionSource<TurnResult> =
        lock gate (fun () ->
            let waiter =
                TaskCompletionSource<TurnResult>(TaskCreationOptions.RunContinuationsAsynchronously)

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

    let private hubs = ConcurrentDictionary<TenantId * SessionId, PromptWaitHub>()

    let GetOrAddScoped tenant sessionId =
        hubs.GetOrAdd((tenant, sessionId), fun _ -> PromptWaitHub())

    let ObserveSettledScoped tenant sessionId result =
        (GetOrAddScoped tenant sessionId).ObserveSettled(result)

    /// Returns the hub for a session, creating it when missing.
    let GetOrAdd (sessionId: SessionId) : PromptWaitHub =
        GetOrAddScoped TenantId.Default sessionId

    /// Records a settled result on the session's hub, creating the hub
    /// when no waiter ever queued (a settle with no waiter still records).
    let ObserveSettled (sessionId: SessionId) (result: TurnResult) : unit =
        (GetOrAdd sessionId).ObserveSettled(result)

    /// Drops every hub. Tests only: isolates static settle state between
    /// facts sharing the process.
    let Clear () : unit = hubs.Clear()

    // ────────────────── Per-operation live hints (issue 383) ──────────────────

    // Position-keyed observers fed by committed settlement. A hint only
    // wakes the observer so it re-reads the durable row; it never carries a
    // verdict, so late, reconnected, restarted, and remote observers that
    // miss it still converge by polling the same store truth. Subscribe
    // always precedes the durable re-read, and the actor always commits
    // before notifying, so every outcome is either read directly or wakes
    // the hint: nothing settles silently.
    let private positionHints =
        ConcurrentDictionary<TenantId * SessionId * int64, ResizeArray<TaskCompletionSource<unit>>>()

    /// Subscribes one observer to the live hint for an accepted operation.
    /// Returns the hint task plus an unsubscribe removing only this
    /// observer: cancelling abandons only this observation. The hint task
    /// completes when the owning actor settles that position, or never when
    /// the hint is missed (a remote or restarted observer): the caller must
    /// re-read the durable row after subscribing and poll while unsettled.
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session the operation was accepted into.</param>
    /// <param name="position">The immutable per-session inbox position.</param>
    let SubscribePosition (tenant: TenantId) (sessionId: SessionId) (position: int64) : Task * (unit -> unit) =
        let key = (tenant, sessionId, position)

        let waiter =
            TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

        let live =
            positionHints.GetOrAdd(key, fun _ -> ResizeArray<TaskCompletionSource<unit>>())

        lock live (fun () -> live.Add(waiter))

        let unsubscribe () =
            lock live (fun () ->
                let mutable found = -1

                for index = 0 to live.Count - 1 do
                    if found < 0 && Object.ReferenceEquals(live[index], waiter) then
                        found <- index

                if found >= 0 then
                    live.RemoveAt(found)

                if live.Count = 0 then
                    positionHints.TryRemove(key) |> ignore)

        waiter.Task :> Task, unsubscribe

    /// Feeds the live hint for one settled position: wakes every subscribed
    /// observer so each re-reads the durable row. Best-effort and
    /// idempotent: observers that miss it converge by polling, and duplicate
    /// notifies only trigger another re-read of the same committed winner.
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session the operation was accepted into.</param>
    /// <param name="position">The immutable per-session inbox position.</param>
    let NotifyPositionScoped (tenant: TenantId) (sessionId: SessionId) (position: int64) : unit =
        let key = (tenant, sessionId, position)

        match positionHints.TryRemove(key) with
        | true, live when not (isNull (box live)) ->
            lock live (fun () ->
                for waiter in live do
                    if not (isNull (box waiter)) then
                        waiter.TrySetResult(()) |> ignore)
        | _ -> ()
