// SPDX-License-Identifier: Apache-2.0
module internal Legate.Storage.Packages.PackageLeaseLoop

open System
open System.Threading
open System.Threading.Tasks
open Legate

// One renewing-lease step over an injected renew delegate, mirroring
// ClaimHeartbeat.stepAsync: the attempt is pure over the delegate so
// virtual-time tests cover every outcome without a store. Internal: hosts
// program against IAgentPackageLeaseService, never this loop.

/// What one renewal attempt decided: renewed carries the fresh lease the
/// loop continues under; lost carries the renewal's reason; timed out
/// means the attempt exceeded the renewal budget on the loop's clock.
type StepOutcome =
    | Renewed of lease: AgentPackageLease
    | Lost of reason: string
    | TimedOut

/// What the renew loop finished with: the work's result beside the latest
/// lease (so the caller releases the current token), or the latest lease
/// beside the failure reason (so the caller still releases best-effort).
type LoopResult<'T> =
    | Completed of result: 'T * lease: AgentPackageLease
    | Failed of lease: AgentPackageLease * reason: string

/// Runs one renewal attempt and branches every outcome: a lost renewal
/// returns the store's reason, and an attempt whose clock-measured span
/// exceeds the renewal timeout counts as failed even when the renewal
/// itself landed, so scripted slow renewals cancel deterministically
/// under a manual clock.
let stepAsync
    (renew: AgentPackageLease -> TimeSpan -> CancellationToken -> Task<PackageLeaseRenewal>)
    (current: AgentPackageLease)
    (leaseDuration: TimeSpan)
    (renewalTimeout: TimeSpan)
    (clock: TimeProvider)
    (cancellationToken: CancellationToken)
    : Task<StepOutcome> =
    task {
        let started = clock.GetUtcNow()
        let! renewal = renew current leaseDuration cancellationToken
        let elapsed = clock.GetUtcNow() - started

        if elapsed > renewalTimeout then
            return TimedOut
        else if isNull (box renewal) then
            return
                raise (
                    InvalidOperationException("The package lease renewal returned null instead of a renewal outcome.")
                )
        else
            match renewal with
            | :? PackageLeaseRenewed as renewed -> return Renewed renewed.Lease
            | :? PackageLeaseLost as lost -> return Lost lost.Reason
            | unknown ->
                return
                    raise (
                        InvalidOperationException(
                            $"The package lease renewal returned an unknown outcome: %s{unknown.GetType().FullName}."
                        )
                    )
    }

/// Runs the work while renewing the lease every interval on the delay
/// seam: the work and the renew wait race, a lost or over-timeout renewal
/// cancels the work token and returns the failure beside the latest lease,
/// and a finished work returns its result beside the latest lease. The
/// caller owns release in both cases.
let runAsync
    (renew: AgentPackageLease -> TimeSpan -> CancellationToken -> Task<PackageLeaseRenewal>)
    (initial: AgentPackageLease)
    (leaseDuration: TimeSpan)
    (renewInterval: TimeSpan)
    (renewalTimeout: TimeSpan)
    (clock: TimeProvider)
    (delay: ILlmDelay)
    (work: CancellationToken -> Task<'T>)
    (cancellationToken: CancellationToken)
    : Task<LoopResult<'T>> =
    task {
        ArgumentNullException.ThrowIfNull(clock)
        ArgumentNullException.ThrowIfNull(delay)

        if isNull (box initial) then
            raise (ArgumentNullException(nameof initial))

        if isNull (box work) then
            raise (ArgumentNullException(nameof work))

        use workCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken)
        let workTask = work workCts.Token

        if isNull (box workTask) then
            return raise (InvalidOperationException("The leased work returned null instead of a task."))
        else
            let mutable current = initial
            let mutable failure: string option = None

            while failure.IsNone && not workTask.IsCompleted do
                let delayTask = delay.Delay(renewInterval, workCts.Token)
                let! first = Task.WhenAny([| workTask :> Task; delayTask |])

                if Object.ReferenceEquals(first, (workTask :> Task)) then
                    // The work finished while the renew wait was pending:
                    // leave the wait behind and fall out of the loop.
                    ()
                else
                    // Surface a cancelled wait without throwing past the loop.
                    let mutable abandoned = false

                    try
                        do! delayTask
                    with :? OperationCanceledException ->
                        abandoned <- true

                    if not abandoned && not workTask.IsCompleted then
                        let! outcome = stepAsync renew current leaseDuration renewalTimeout clock workCts.Token

                        match outcome with
                        | Renewed next -> current <- next
                        | Lost reason -> failure <- Some $"lost:{reason}"
                        | TimedOut -> failure <- Some "timeout"

            match failure with
            | None ->
                let! result = workTask
                return Completed(result, current)
            | Some reason ->
                workCts.Cancel()

                try
                    let! _ = workTask
                    ()
                with _ ->
                    ()

                return Failed(current, reason)
    }
