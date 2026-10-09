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
// registry below as a fast-path wake only.
//
// Issue 384 bounds: the settled history is a bounded ring (newest wins,
// oldest evicted past the bound), the waiter queue rejects past its cap
// with the typed admission error, and the process-wide hub and hint tables
// reject new keys past their caps the same way, never evicting a live
// waiter or observer. Closed and expired sessions release their hub and
// hint keys; the release wakes pending hint observers to re-read the
// durable row, while hub waiters keep their own bound (a release never
// cancels a live wait, so a released hub's waiters lapse visibly through
// their bound instead of losing observation silently). The durable row
// stays the convergence path throughout: transient removal never deletes
// durable outcomes and waiterless settles retain at most the ring.

/// The finite bounds one transient wait table enforces. Internal so hosts
/// configure through SessionsOptions; the tables take these directly so
/// bound tests construct isolated tables without touching process state.
type internal TransientWaitLimits =
    {
        /// The process-wide hubs keyed by tenant and session.
        MaxHubs: int
        /// The settled results one hub retains, newest wins.
        MaxSettledPerHub: int
        /// The live waiters one hub queues for the next settle.
        MaxWaitersPerHub: int
        /// The live position-hint keys awaiting settlement.
        MaxHintKeys: int
        /// The live observers one position hint wakes.
        MaxObserversPerKey: int
    }

/// Constructors for <see cref="T:Legate.TransientWaitLimits" />.
module internal TransientWaitLimits =

    /// The process-wide defaults, mirroring the SessionsOptions knobs.
    let defaults () : TransientWaitLimits =
        let sessions = SessionsOptions()

        {
            MaxHubs = sessions.MaxWaitHubs
            MaxSettledPerHub = sessions.MaxSettledResultsPerHub
            MaxWaitersPerHub = sessions.MaxSettleWaitersPerHub
            MaxHintKeys = sessions.MaxPositionHints
            MaxObserversPerKey = sessions.MaxPositionHintObservers
        }

    /// The bounds the configured host Sessions options carry.
    /// <param name="sessions">The configured sessions options. Must not be null.</param>
    let fromSessions (sessions: SessionsOptions) : TransientWaitLimits =
        ArgumentNullException.ThrowIfNull(sessions)

        {
            MaxHubs = sessions.MaxWaitHubs
            MaxSettledPerHub = sessions.MaxSettledResultsPerHub
            MaxWaitersPerHub = sessions.MaxSettleWaitersPerHub
            MaxHintKeys = sessions.MaxPositionHints
            MaxObserversPerKey = sessions.MaxPositionHintObservers
        }

type internal PromptWaitHub(?maxSettled: int, ?maxWaiters: int) =

    let gate = obj ()
    let settled = ResizeArray<TurnResult>()
    let waiters = Queue<TaskCompletionSource<TurnResult>>()

    let keepSettled =
        defaultArg maxSettled (TransientWaitLimits.defaults ()).MaxSettledPerHub
        |> max 1

    let keepWaiters =
        defaultArg maxWaiters (TransientWaitLimits.defaults ()).MaxWaitersPerHub
        |> max 1

    /// Records a settled result and wakes its waiter when one waits. The
    /// history retains at most the bound newest results: a settle past it
    /// evicts the oldest retained result, so waiterless settles never grow
    /// an archive.
    member _.ObserveSettled(result: TurnResult) =
        if not (isNull (box result)) then
            lock gate (fun () ->
                settled.Add(result)

                while settled.Count > keepSettled do
                    settled.RemoveAt(0)

                while waiters.Count > 0 && waiters.Peek().Task.IsCanceled do
                    waiters.Dequeue() |> ignore

                if waiters.Count > 0 then
                    waiters.Dequeue().TrySetResult(result) |> ignore)

    /// Queues a waiter for the next settle. Waiters run their
    /// continuations asynchronously (the DispatcherWake precedent): the
    /// settle hook fires on the session actor thread, so an inline
    /// continuation would run host code on that thread and stall the
    /// settle it waits for. Past the waiter cap the queue rejects with the
    /// typed admission error instead of parking an unbounded queue.
    member _.EnqueueSettle() : TaskCompletionSource<TurnResult> =
        lock gate (fun () ->
            while waiters.Count > 0 && waiters.Peek().Task.IsCanceled do
                waiters.Dequeue() |> ignore

            if waiters.Count >= keepWaiters then
                raise (
                    AdmissionRejectedException(
                        "settleWaitersAtCapacity",
                        sprintf
                            "The session holds %d live settle waiters; the wait was rejected without observing the turn."
                            keepWaiters
                    )
                )

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

    /// Every retained settled result, in settle order: at most the bound
    /// newest.
    member _.Settled: IReadOnlyList<TurnResult> =
        lock gate (fun () -> ResizeArray<TurnResult>(settled) :> IReadOnlyList<TurnResult>)

    /// How many waiters currently queue for the next settle.
    member _.PendingWaiterCount: int = lock gate (fun () -> waiters.Count)

/// One bounded process-wide hub table: the per-session hubs the actor
/// settle hooks fan out to. Internal so bound tests construct isolated
/// tables; the module below holds the process singleton.
type internal HubTable(limits: TransientWaitLimits) =

    let gate = obj ()
    let hubs = ConcurrentDictionary<TenantId * SessionId, PromptWaitHub>()
    let mutable bounds = limits

    /// The bounds this table enforces.
    member _.Limits
        with get () = lock gate (fun () -> bounds)
        and set (value: TransientWaitLimits) = lock gate (fun () -> bounds <- value)

    /// How many session hubs the table currently holds.
    member _.HubCount: int = hubs.Count

    /// Returns the hub for a session, creating it when missing. Past the
    /// hub cap a missing session rejects with the typed admission error
    /// instead of evicting a live session's hub.
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session whose hub to return.</param>
    member _.GetOrAddScoped(tenant: TenantId, sessionId: SessionId) : PromptWaitHub =
        match hubs.TryGetValue((tenant, sessionId)) with
        | true, hub when not (isNull (box hub)) -> hub
        | _ ->
            lock gate (fun () ->
                match hubs.TryGetValue((tenant, sessionId)) with
                | true, hub when not (isNull (box hub)) -> hub
                | _ ->
                    if hubs.Count >= bounds.MaxHubs then
                        raise (
                            AdmissionRejectedException(
                                "transientWaitHubsAtCapacity",
                                sprintf
                                    "The process holds %d transient wait hubs; the session was rejected without observing the turn."
                                    bounds.MaxHubs
                            )
                        )

                    hubs.GetOrAdd(
                        (tenant, sessionId),
                        fun _ -> PromptWaitHub(bounds.MaxSettledPerHub, bounds.MaxWaitersPerHub)
                    ))

    /// Records a settled result on the session's hub without ever throwing
    /// for the hub cap: the actor settle path drops the transient record
    /// when the table is full while the durable row stays the truth, so
    /// settlement never fails for a transient bound.
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session that settled.</param>
    /// <param name="result">The settled result.</param>
    member _.ObserveSettledScoped(tenant: TenantId, sessionId: SessionId, result: TurnResult) : unit =
        match hubs.TryGetValue((tenant, sessionId)) with
        | true, hub when not (isNull (box hub)) -> hub.ObserveSettled(result)
        | _ ->
            let mutable admitted = false

            lock gate (fun () ->
                match hubs.TryGetValue((tenant, sessionId)) with
                | true, hub when not (isNull (box hub)) ->
                    hub.ObserveSettled(result)
                    admitted <- true
                | _ ->
                    if hubs.Count < bounds.MaxHubs then
                        (hubs.GetOrAdd(
                            (tenant, sessionId),
                            fun _ -> PromptWaitHub(bounds.MaxSettledPerHub, bounds.MaxWaitersPerHub)
                        ))
                            .ObserveSettled(result)

                        admitted <- true
                    else
                        admitted <- false)

            ()

    /// Releases the session's hub: closed and expired sessions drop their
    /// retained history and waiter queue entry. Live waiters are never
    /// cancelled by the release; they lapse through their own bound, and a
    /// later wait resolves through a fresh hub plus the durable row.
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to release.</param>
    /// <returns>True when a hub was held and removed.</returns>
    member _.ReleaseSession(tenant: TenantId, sessionId: SessionId) : bool =
        hubs.TryRemove((tenant, sessionId)) |> fst

    /// Drops every hub. Tests only: isolates static settle state between
    /// facts sharing the process.
    member _.Clear() : unit = hubs.Clear()

/// One bounded position-hint table: the per-operation live observers fed by
/// committed settlement. Internal so bound tests construct isolated
/// tables; the module below holds the process singleton.
type internal HintTable(limits: TransientWaitLimits) =

    let gate = obj ()

    let hints =
        ConcurrentDictionary<TenantId * SessionId * int64, ResizeArray<TaskCompletionSource<unit>>>()

    let mutable bounds = limits

    /// The bounds this table enforces.
    member _.Limits
        with get () = lock gate (fun () -> bounds)
        and set (value: TransientWaitLimits) = lock gate (fun () -> bounds <- value)

    /// How many live hint keys the table currently holds.
    member _.KeyCount: int = hints.Count

    /// How many live hint observers the table currently holds across keys.
    member _.ObserverCount: int =
        let mutable total = 0

        for live in hints.Values do
            if not (isNull (box live)) then
                lock live (fun () -> total <- total + live.Count)

        total

    /// Subscribes one observer to the live hint for an accepted operation.
    /// Returns the hint task plus an unsubscribe removing only this
    /// observer: cancelling abandons only this observation. The hint task
    /// completes when the owning actor settles that position, or never when
    /// the hint is missed (a remote or restarted observer): the caller must
    /// re-read the durable row after subscribing and poll while unsettled.
    /// Past the key or per-hint observer cap the subscribe rejects with the
    /// typed admission error instead of evicting a live observer.
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session the operation was accepted into.</param>
    /// <param name="position">The immutable per-session inbox position.</param>
    member _.SubscribePosition(tenant: TenantId, sessionId: SessionId, position: int64) : Task * (unit -> unit) =
        let key = (tenant, sessionId, position)

        let waiter =
            TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

        let live =
            lock gate (fun () ->
                match hints.TryGetValue(key) with
                | true, existing when not (isNull (box existing)) -> existing
                | _ ->
                    if hints.Count >= bounds.MaxHintKeys then
                        raise (
                            AdmissionRejectedException(
                                "positionHintsAtCapacity",
                                sprintf
                                    "The process holds %d live position hints; the observation was rejected without losing a live observer."
                                    bounds.MaxHintKeys
                            )
                        )

                    hints.GetOrAdd(key, fun _ -> ResizeArray<TaskCompletionSource<unit>>()))

        lock live (fun () ->
            if live.Count >= bounds.MaxObserversPerKey then
                raise (
                    AdmissionRejectedException(
                        "positionHintObserversAtCapacity",
                        sprintf
                            "The hint holds %d live observers; the observation was rejected without losing a live observer."
                            bounds.MaxObserversPerKey
                    )
                )

            live.Add(waiter))

        let unsubscribe () =
            lock live (fun () ->
                let mutable found = -1

                for index = 0 to live.Count - 1 do
                    if found < 0 && Object.ReferenceEquals(live[index], waiter) then
                        found <- index

                if found >= 0 then
                    live.RemoveAt(found)

                if live.Count = 0 then
                    hints.TryRemove(key) |> ignore)

        waiter.Task :> Task, unsubscribe

    /// Feeds the live hint for one settled position: wakes every subscribed
    /// observer so each re-reads the durable row. Best-effort and
    /// idempotent: observers that miss it converge by polling, and duplicate
    /// notifies only trigger another re-read of the same committed winner.
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session the operation was accepted into.</param>
    /// <param name="position">The immutable per-session inbox position.</param>
    member _.NotifyPositionScoped(tenant: TenantId, sessionId: SessionId, position: int64) : unit =
        let key = (tenant, sessionId, position)

        match hints.TryRemove(key) with
        | true, live when not (isNull (box live)) ->
            lock live (fun () ->
                for waiter in live do
                    if not (isNull (box waiter)) then
                        waiter.TrySetResult(()) |> ignore)
        | _ -> ()

    /// Releases the session's hint keys, waking every pending observer so
    /// each re-reads the durable row at once instead of waiting out the
    /// poll: eviction wakes visibly, never drops silently.
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to release.</param>
    /// <returns>How many hint keys were held and removed.</returns>
    member _.ReleaseSession(tenant: TenantId, sessionId: SessionId) : int =
        let mutable removed = 0

        for entry in hints.ToArray() do
            let (keyTenant, keySession, _) = entry.Key

            if keyTenant = tenant && keySession = sessionId then
                match hints.TryRemove(entry.Key) with
                | true, live when not (isNull (box live)) ->
                    removed <- removed + 1

                    lock live (fun () ->
                        for waiter in live do
                            if not (isNull (box waiter)) then
                                waiter.TrySetResult(()) |> ignore)
                | _ -> ()

        removed

    /// Drops every hint. Tests only.
    member _.Clear() : unit = hints.Clear()

/// The process-wide per-session hubs the actor settle hooks fan out to.
// SessionActor's spawn sites (both props constructions) observe through
// ObserveSettled; SessionClient enqueues and cancels through GetOrAdd.
// SessionId is globally unique, so it keys the hub without the tenant.
module internal PromptWaitHubs =

    let private hubTable = HubTable(TransientWaitLimits.defaults ())
    let private hintTable = HintTable(TransientWaitLimits.defaults ())

    /// Replaces the bounds both process tables enforce. Host wiring calls
    /// this once from the configured Sessions options; the bounds are
    /// process-wide and last-write-wins across tenants.
    /// <param name="limits">The bounds to enforce.</param>
    let Configure (limits: TransientWaitLimits) : unit =
        hubTable.Limits <- limits
        hintTable.Limits <- limits

    /// Replaces the bounds from the configured host Sessions options.
    /// <param name="sessions">The configured sessions options. Must not be null.</param>
    let ConfigureFromSessions (sessions: SessionsOptions) : unit =
        Configure(TransientWaitLimits.fromSessions sessions)

    /// How many session hubs the process currently holds.
    let HubCount () : int = hubTable.HubCount

    /// How many live position-hint keys the process currently holds.
    let HintKeyCount () : int = hintTable.KeyCount

    /// How many live position-hint observers the process currently holds.
    let HintObserverCount () : int = hintTable.ObserverCount

    let GetOrAddScoped tenant sessionId =
        hubTable.GetOrAddScoped(tenant, sessionId)

    let ObserveSettledScoped tenant sessionId result =
        hubTable.ObserveSettledScoped(tenant, sessionId, result)

    /// Returns the hub for a session, creating it when missing.
    let GetOrAdd (sessionId: SessionId) : PromptWaitHub =
        GetOrAddScoped TenantId.Default sessionId

    /// Records a settled result on the session's hub, creating the hub
    /// when no waiter ever queued (a settle with no waiter still records,
    /// bounded by the settled ring).
    let ObserveSettled (sessionId: SessionId) (result: TurnResult) : unit =
        (GetOrAdd sessionId).ObserveSettled(result)

    /// Releases the session's transient wait state: its hub plus its live
    /// hint keys (pending hint observers wake to re-read the durable row).
    /// Closed and expired sessions release here; the durable rows stay the
    /// convergence path.
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to release.</param>
    /// <returns>True when a hub was held and removed.</returns>
    let ReleaseSession (tenant: TenantId) (sessionId: SessionId) : bool =
        hintTable.ReleaseSession(tenant, sessionId) |> ignore
        hubTable.ReleaseSession(tenant, sessionId)

    /// Drops every hub and hint. Tests only: isolates static settle state between
    /// facts sharing the process.
    let Clear () : unit =
        hubTable.Clear()
        hintTable.Clear()

    // ────────────────── Per-operation live hints (issue 383) ──────────────────

    // Position-keyed observers fed by committed settlement. A hint only
    // wakes the observer so it re-reads the durable row; it never carries a
    // verdict, so late, reconnected, restarted, and remote observers that
    // miss it still converge by polling the same store truth. Subscribe
    // always precedes the durable re-read, and the actor always commits
    // before notifying, so every outcome is either read directly or wakes
    // the hint: nothing settles silently.
    let SubscribePosition (tenant: TenantId) (sessionId: SessionId) (position: int64) : Task * (unit -> unit) =
        hintTable.SubscribePosition(tenant, sessionId, position)

    /// Feeds the live hint for one settled position: wakes every subscribed
    /// observer so each re-reads the durable row. Best-effort and
    /// idempotent: observers that miss it converge by polling, and duplicate
    /// notifies only trigger another re-read of the same committed winner.
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session the operation was accepted into.</param>
    /// <param name="position">The immutable per-session inbox position.</param>
    let NotifyPositionScoped (tenant: TenantId) (sessionId: SessionId) (position: int64) : unit =
        hintTable.NotifyPositionScoped(tenant, sessionId, position)
