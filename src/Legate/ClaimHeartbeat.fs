// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Threading
open System.Threading.Tasks

// Turn-claim lease heartbeat (issue 33): renews a held TurnClaim on the
// injected TimeProvider/ILlmDelay seams and branches every renewal outcome,
// so the session actor path (SessionActor.createTurnRunner) can back
// TurnLoop's isLeaseValid hook with a live lease instead of the always-live
// stub. The heartbeat consumes ISessionStore.ObserveAndRenewClaim (the
// atomic renew-plus-observe entry; until issue 35 grows the
// cancellation-request carrier it behaves as RenewClaim, so cancellation is
// observed through the injected isCancellationRequested delegate the abort
// epic will back); it never touches inbox or lifecycle state, which stay
// with issue 32. The per-step decision (stepAsync) is pure over a scripted
// renew delegate so virtual-time tests cover every lease outcome without a
// store; the loop (runAsync) drives the real store.
//
// Reconciliation note (stale-checkout correction): the plan assumed no
// session actor or turn loop existed and proposed standalone scaffolding.
// Both exist on origin/main (SessionActor.fs with an always-live lease hook
// in createTurnRunner, TurnLoop.fs with the isLeaseValid delegate checked
// before every provider call and tool invocation), so this module plugs
// into those seams: ClaimLeaseView.IsValid is the isLeaseValid delegate,
// and ClaimFence.checkBeforeCallAsync is TurnLoop's per-tool verify hook.
// Lease numbers likewise derive from the merged SessionsOptions (60 s
// lease, 15 s renewal with the under-half bound enforced) rather than the
// BridgeMCP-origin 120 s / 30 s literals quoted in the issue context; the
// bound is the invariant and fromSessions enforces it for any configured
// values.
module internal ClaimHeartbeat =

    /// How a claimed turn renews its lease: the lease duration each renewal
    /// grants and how often the heartbeat renews. Derived from
    /// SessionsOptions so the configured bound (renewal under half the
    /// lease, enforced by SessionsOptions.Validate) always holds here too.
    type ClaimHeartbeatOptions =
        {
            /// How long each renewal extends the lease. Positive.
            LeaseDuration: TimeSpan
            /// How often the heartbeat renews. Positive and under half the
            /// lease, so one missed heartbeat never loses the lease.
            RenewalInterval: TimeSpan
        }

    /// Builds heartbeat options from the session admission and lease
    /// settings, re-checking the interval bound at use (SessionsOptions is
    /// mutable, so a post-validate edit must not slip through).
    /// <param name="sessions">The session admission and lease settings. Must not be null.</param>
    /// <returns>The heartbeat tuning.</returns>
    let fromSessions (sessions: SessionsOptions) : ClaimHeartbeatOptions =
        ArgumentNullException.ThrowIfNull(sessions)

        if sessions.LeaseDuration <= TimeSpan.Zero then
            raise (
                ArgumentOutOfRangeException(
                    nameof sessions,
                    "SessionsOptions.LeaseDuration must be positive for the claim heartbeat."
                )
            )

        if sessions.LeaseRenewalInterval <= TimeSpan.Zero then
            raise (
                ArgumentOutOfRangeException(
                    nameof sessions,
                    "SessionsOptions.LeaseRenewalInterval must be positive for the claim heartbeat."
                )
            )

        if sessions.LeaseRenewalInterval >= sessions.LeaseDuration.Divide 2.0 then
            raise (
                ArgumentOutOfRangeException(
                    nameof sessions,
                    "SessionsOptions.LeaseRenewalInterval must be less than half LeaseDuration for the claim heartbeat."
                )
            )

        {
            LeaseDuration = sessions.LeaseDuration
            RenewalInterval = sessions.LeaseRenewalInterval
        }

    /// What one heartbeat renewal decided: the caller branches on it.
    /// Continue and RenewNow carry the current claim (renewed or still
    /// held); the stops carry no claim because the turn must release
    /// everything it held.
    type ClaimHeartbeatDecision =
        /// The lease is live: keep running under the carried claim.
        | Continue of claim: TurnClaim
        /// The lease still holds but is close to expiry: renew again
        /// immediately instead of waiting out the interval.
        | RenewNow of claim: TurnClaim
        /// The lease is gone (expired, taken over, or missing): stop acting
        /// on the turn with the lease-loss cause.
        | StopLeaseLost
        /// The host asked to cancel the turn: stop acting on it with the
        /// cancellation cause (issue 35 owns the carrier; here the
        /// injected observer).
        | StopCancelled

    /// Renews once and branches every renewal outcome: renewed and held
    /// continue, expiring renews now, lost and missing stop with lease
    /// loss, and an observed cancellation stops with cancellation even
    /// when the lease is live. The renew delegate is
    /// ISessionStore.ObserveAndRenewClaim partially applied; injecting it
    /// keeps the step testable without a store.
    /// <param name="renew">Renews the carried claim. Must not be null.</param>
    /// <param name="claim">The claim to renew.</param>
    /// <param name="isCancellationRequested">Observes a host cancel (issue 35 backs this). Must not be null.</param>
    /// <param name="cancellationToken">Abandons the renewal.</param>
    /// <returns>What the renewal decided.</returns>
    let stepAsync
        (renew: TurnClaim -> CancellationToken -> Task<TurnLeaseState>)
        (claim: TurnClaim)
        (isCancellationRequested: unit -> bool)
        (cancellationToken: CancellationToken)
        : Task<ClaimHeartbeatDecision> =
        if isNull (box renew) then
            raise (ArgumentNullException(nameof renew))

        if isNull (box claim) then
            raise (ArgumentNullException(nameof claim))

        if isNull (box isCancellationRequested) then
            raise (ArgumentNullException(nameof isCancellationRequested))

        task {
            let! state = renew claim cancellationToken

            // Cancellation wins over a live lease: a cancel observed
            // mid-renewal stops the turn even when the renew landed.
            let cancelled = isCancellationRequested ()

            if isNull (box state) then
                return raise (InvalidOperationException("The claim renewal returned null instead of a lease state."))
            else
                match state with
                | :? TurnLeaseRenewed as renewed ->
                    if cancelled then
                        return StopCancelled
                    else
                        return Continue renewed.Claim
                | :? TurnLeaseHeld as held ->
                    if cancelled then
                        return StopCancelled
                    else
                        return Continue held.Claim
                | :? TurnLeaseExpiring as expiring ->
                    if cancelled then
                        return StopCancelled
                    else
                        return RenewNow expiring.Claim
                | :? TurnLeaseLost -> return StopLeaseLost
                | :? TurnLeaseMissing -> return StopLeaseLost
                | unknown ->
                    return
                        raise (
                            InvalidOperationException(
                                $"The claim renewal returned an unknown lease state: %s{unknown.GetType().FullName}."
                            )
                        )
        }

    /// The heartbeat's live view of one claim: the loop observes every step
    /// decision into it, and TurnLoop's synchronous isLeaseValid hook reads
    /// IsValid. Validity is the last decision plus the clock: a live lease
    /// whose expiry the clock has passed reads invalid, so a fenced loser
    /// stops even before its next renewal lands.
    type ClaimLeaseView(clock: TimeProvider, initialClaim: TurnClaim) =

        do
            ArgumentNullException.ThrowIfNull(clock)

            if isNull (box initialClaim) then
                raise (ArgumentNullException(nameof initialClaim))

        let mutable current = initialClaim
        let mutable live = true

        /// The claim the view currently holds (the last renewed one).
        member _.Current = current

        /// Observes one step decision: live decisions refresh the carried
        /// claim, stops mark the lease dead.
        /// <param name="decision">The step decision to observe.</param>
        member _.Observe(decision: ClaimHeartbeatDecision) =
            match decision with
            | Continue claim
            | RenewNow claim -> current <- claim
            | StopLeaseLost
            | StopCancelled -> live <- false

        /// Whether the turn may still act: the lease has not stopped and
        /// the clock has not passed its expiry.
        /// <returns>True while the lease is live.</returns>
        member _.IsValid() : bool =
            live && clock.GetUtcNow() < current.ExpiresAt

    /// Runs the renewal loop until it stops: waits the renewal interval on
    /// the injected delay seam, renews through the injected renew delegate,
    /// and branches per stepAsync. Continue waits out the next interval;
    /// RenewNow renews again immediately; the stops return. Every step
    /// decision is observed into the view when one is given, so the
    /// isLeaseValid hook backed by the view flips the moment the lease is
    /// lost. Caller cancellation aborts the wait and propagates.
    /// <param name="renew">Renews the carried claim. Must not be null.</param>
    /// <param name="claim">The claim the loop renews.</param>
    /// <param name="options">The lease duration and renewal interval.</param>
    /// <param name="clock">The clock the view reads. Must not be null.</param>
    /// <param name="delay">The wait seam between renewals. Must not be null.</param>
    /// <param name="isCancellationRequested">Observes a host cancel (issue 35 backs this). Must not be null.</param>
    /// <param name="view">The live view observing each step, or None for no view.</param>
    /// <param name="cancellationToken">Aborts the loop.</param>
    /// <returns>The stopping decision.</returns>
    let runAsync
        (renew: TurnClaim -> CancellationToken -> Task<TurnLeaseState>)
        (claim: TurnClaim)
        (options: ClaimHeartbeatOptions)
        (clock: TimeProvider)
        (delay: ILlmDelay)
        (isCancellationRequested: unit -> bool)
        (view: ClaimLeaseView option)
        (cancellationToken: CancellationToken)
        : Task<ClaimHeartbeatDecision> =
        if isNull (box renew) then
            raise (ArgumentNullException(nameof renew))

        ArgumentNullException.ThrowIfNull(clock)
        ArgumentNullException.ThrowIfNull(delay)

        if isNull (box claim) then
            raise (ArgumentNullException(nameof claim))

        if isNull (box isCancellationRequested) then
            raise (ArgumentNullException(nameof isCancellationRequested))

        if options.LeaseDuration <= TimeSpan.Zero then
            raise (ArgumentOutOfRangeException(nameof options, "ClaimHeartbeatOptions.LeaseDuration must be positive."))

        if options.RenewalInterval <= TimeSpan.Zero then
            raise (
                ArgumentOutOfRangeException(nameof options, "ClaimHeartbeatOptions.RenewalInterval must be positive.")
            )

        if options.RenewalInterval >= options.LeaseDuration.Divide 2.0 then
            raise (
                ArgumentOutOfRangeException(
                    nameof options,
                    "ClaimHeartbeatOptions.RenewalInterval must be less than half LeaseDuration."
                )
            )

        let rec loop current waitFirst : Task<ClaimHeartbeatDecision> =
            task {
                if waitFirst then
                    do! delay.Delay(options.RenewalInterval, cancellationToken)

                let! decision = stepAsync renew current isCancellationRequested cancellationToken

                match view with
                | Some live -> live.Observe(decision)
                | None -> ()

                match decision with
                | Continue next -> return! loop next true
                | RenewNow next -> return! loop next false
                | StopLeaseLost -> return StopLeaseLost
                | StopCancelled -> return StopCancelled
            }

        loop claim true

    /// Runs the renewal loop against the durable store: ObserveAndRenewClaim
    /// with the configured lease duration is the renew delegate (the atomic
    /// renew-plus-observe entry; until issue 35 grows the
    /// cancellation-request carrier it behaves as RenewClaim, so the
    /// injected observer carries cancellation here).
    /// <param name="store">The durable store the lease renews through. Must not be null.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="claim">The claim the loop renews.</param>
    /// <param name="options">The lease duration and renewal interval.</param>
    /// <param name="clock">The clock the view reads. Must not be null.</param>
    /// <param name="delay">The wait seam between renewals. Must not be null.</param>
    /// <param name="isCancellationRequested">Observes a host cancel (issue 35 backs this). Must not be null.</param>
    /// <param name="view">The live view observing each step, or None for no view.</param>
    /// <param name="cancellationToken">Aborts the loop.</param>
    /// <returns>The stopping decision.</returns>
    let runWithStoreAsync
        (store: ISessionStore)
        (tenant: TenantId)
        (claim: TurnClaim)
        (options: ClaimHeartbeatOptions)
        (clock: TimeProvider)
        (delay: ILlmDelay)
        (isCancellationRequested: unit -> bool)
        (view: ClaimLeaseView option)
        (cancellationToken: CancellationToken)
        : Task<ClaimHeartbeatDecision> =
        ArgumentNullException.ThrowIfNull(store)

        let renew current token =
            store.ObserveAndRenewClaim(tenant, current, options.LeaseDuration, token)

        runAsync renew claim options clock delay isCancellationRequested view cancellationToken
