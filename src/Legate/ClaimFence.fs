// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks

// Last-moment claim fence (issue 33): verifies the TurnClaim token
// immediately before each post-claim side effect and skips the effect when
// the claim is lost or missing, so the loser of a takeover produces zero
// effects. Applies to usage checkpoints, journal appends, the tool-call
// boundary (via checkBeforeCallAsync, wired into TurnLoop's per-tool
// verify hook), completion-sink notification, and settlement.Checkpoint,
// settle, and journal appends are also fenced store-side (the stores
// return lost/rejected outcomes instead of acting); the fence adds the
// skip-without-calling layer, and for tool calls and sink notification,
// which no store fences, it is the fence. Never touches inbox or lifecycle
// state (issue 32) or cancellation plumbing (issue 35).
module internal ClaimFence =

    /// Whether a lease state proves the claim still holds the turn: held,
    /// renewed, and expiring are live; lost and missing are fenced out.
    /// Fail-closed: a null or unknown state reads as not live, so a fence
    /// that cannot prove liveness denies the effect.
    /// <param name="state">The lease state to classify. Null reads as fenced out.</param>
    /// <returns>True while the claim holds the turn.</returns>
    let isLiveState (state: TurnLeaseState) : bool =
        if isNull (box state) then
            false
        else
            match state with
            | :? TurnLeaseHeld -> true
            | :? TurnLeaseRenewed -> true
            | :? TurnLeaseExpiring -> true
            | :? TurnLeaseLost -> false
            | :? TurnLeaseMissing -> false
            | _ -> false

    /// Verifies the claim without changing anything: ISessionStore
    /// VerifyClaim at the last moment before an effect.
    /// <param name="store">The durable store. Must not be null.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="claim">The claim to verify. Must not be null.</param>
    /// <param name="cancellationToken">Abandons the verification.</param>
    /// <returns>The lease state the claim currently holds.</returns>
    let verifyAsync
        (store: ISessionStore)
        (tenant: TenantId)
        (claim: TurnClaim)
        (cancellationToken: CancellationToken)
        : Task<TurnLeaseState> =
        ArgumentNullException.ThrowIfNull(store)

        if isNull (box claim) then
            raise (ArgumentNullException(nameof claim))

        store.VerifyClaim(tenant, claim, cancellationToken)

    /// Runs the effect only while the claim is live: verifies at the last
    /// moment, runs the effect on a live lease, and skips it (returning
    /// None, with zero effects) on a lost or missing claim.
    /// <param name="store">The durable store. Must not be null.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="claim">The claim fencing the effect. Must not be null.</param>
    /// <param name="effect">The side effect to run while live. Must not be null.</param>
    /// <param name="cancellationToken">Abandons the verification or the effect.</param>
    /// <returns>The effect's result, or None when the claim was fenced out.</returns>
    let gateAsync<'T>
        (store: ISessionStore)
        (tenant: TenantId)
        (claim: TurnClaim)
        (effect: unit -> Task<'T>)
        (cancellationToken: CancellationToken)
        : Task<'T option> =
        if isNull (box effect) then
            raise (ArgumentNullException(nameof effect))

        task {
            let! state = verifyAsync store tenant claim cancellationToken

            if isLiveState state then
                let! result = effect ()
                return Some result
            else
                return None
        }

    /// The tool-call boundary check: verifies the claim at the last moment
    /// before a tool invocation. TurnLoop calls this through its per-tool
    /// verify hook; a false result raises TurnLeaseLostException there, so
    /// the fenced loser never invokes the tool.
    /// <param name="store">The durable store. Must not be null.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="claim">The claim fencing the call. Must not be null.</param>
    /// <param name="cancellationToken">Abandons the verification.</param>
    /// <returns>True while the claim holds the turn.</returns>
    let checkBeforeCallAsync
        (store: ISessionStore)
        (tenant: TenantId)
        (claim: TurnClaim)
        (cancellationToken: CancellationToken)
        : Task<bool> =
        task {
            let! state = verifyAsync store tenant claim cancellationToken
            return isLiveState state
        }

    /// Checkpoints usage only while the claim is live.
    /// <param name="store">The durable store. Must not be null.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="claim">The claim fencing the checkpoint. Must not be null.</param>
    /// <param name="usage">The usage to checkpoint.</param>
    /// <param name="cancellationToken">Abandons the checkpoint.</param>
    /// <returns>True when the checkpoint landed, false when the claim was fenced out.</returns>
    let checkpointUsageAsync
        (store: ISessionStore)
        (tenant: TenantId)
        (claim: TurnClaim)
        (usage: UsageSummary)
        (cancellationToken: CancellationToken)
        : Task<bool> =
        task {
            let! landed =
                gateAsync
                    store
                    tenant
                    claim
                    (fun () -> store.CheckpointUsage(tenant, claim, usage, cancellationToken))
                    cancellationToken

            match landed with
            | Some state -> return isLiveState state
            | None -> return false
        }

    /// Appends journal events only while the claim is live, under the
    /// claim's token. A fenced-out append is skipped (None) with zero
    /// writes; a live append returns the store's outcome (applied or
    /// rejected, which the caller branches on).
    /// <param name="sessionStore">The session store verifying the claim. Must not be null.</param>
    /// <param name="eventStore">The journal the events append to. Must not be null.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session whose journal appends.</param>
    /// <param name="claim">The claim fencing the append. Must not be null.</param>
    /// <param name="events">The events to append, in order. Must not be null.</param>
    /// <param name="cancellationToken">Abandons the append.</param>
    /// <returns>The store's append outcome, or None when the claim was fenced out.</returns>
    let appendEventsAsync
        (sessionStore: ISessionStore)
        (eventStore: ISessionEventStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (claim: TurnClaim)
        (events: IReadOnlyList<SessionEvent>)
        (cancellationToken: CancellationToken)
        : Task<EventAppendOutcome option> =
        ArgumentNullException.ThrowIfNull(eventStore)

        if isNull (box events) then
            raise (ArgumentNullException(nameof events))

        gateAsync
            sessionStore
            tenant
            claim
            (fun () -> eventStore.Append(tenant, sessionId, claim.Token, events, cancellationToken))
            cancellationToken

    /// Settles the turn only while the claim is live. A fenced-out settle
    /// is skipped (None) with zero effects; a live settle returns the
    /// store's settlement (settled, already settled, or rejected), which
    /// the caller branches on.
    /// <param name="store">The durable store. Must not be null.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="claim">The claim fencing the settlement. Must not be null.</param>
    /// <param name="status">The terminal status to apply.</param>
    /// <param name="outcome">The structured outcome, or null when the turn carries none.</param>
    /// <param name="cancellationToken">Abandons the settlement.</param>
    /// <returns>The store's settlement, or None when the claim was fenced out.</returns>
    let settleTurnAsync
        (store: ISessionStore)
        (tenant: TenantId)
        (claim: TurnClaim)
        (status: TurnStatus)
        (outcome: TurnOutcome | null)
        (cancellationToken: CancellationToken)
        : Task<TurnSettlement option> =
        gateAsync
            store
            tenant
            claim
            (fun () -> store.SettleTurn(tenant, claim, status, outcome, cancellationToken))
            cancellationToken

    /// Notifies the completion sink only while the claim is live:
    /// verify-then-call with skip on loss. Notify is synchronous void and
    /// cannot be store-fenced, so this check is the fence; duplicates rely
    /// on the completion idempotency key, and a null sink notifies nothing.
    /// <param name="store">The durable store. Must not be null.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="claim">The claim fencing the notification. Must not be null.</param>
    /// <param name="sink">The sink to notify, or null when the host observes completion another way.</param>
    /// <param name="completion">The completion to deliver. Must not be null.</param>
    /// <param name="cancellationToken">Abandons the verification.</param>
    /// <returns>True when the sink was notified, false when fenced out or sinkless.</returns>
    let notifyIfLiveAsync
        (store: ISessionStore)
        (tenant: TenantId)
        (claim: TurnClaim)
        (sink: ISessionCompletionSink)
        (completion: SessionCompletion)
        (cancellationToken: CancellationToken)
        : Task<bool> =
        if isNull (box completion) then
            raise (ArgumentNullException(nameof completion))

        task {
            if isNull (box sink) then
                return false
            else
                let! state = verifyAsync store tenant claim cancellationToken

                if isLiveState state then
                    sink.Notify(completion)
                    return true
                else
                    return false
        }

    /// Stamps the attempt a turn runs under from its claim: attempt numbers
    /// come from TurnClaim.Attempt (the store increments on resume
    /// re-claim) and surface on Turn.Attempt.
    /// <param name="claim">The claim the turn runs under. Must not be null.</param>
    /// <param name="turn">The turn to stamp. Must not be null.</param>
    /// <returns>The turn carrying the claim's attempt.</returns>
    let stampAttempt (claim: TurnClaim) (turn: Turn) : Turn =
        if isNull (box claim) then
            raise (ArgumentNullException(nameof claim))

        if isNull (box turn) then
            raise (ArgumentNullException(nameof turn))

        { turn with Attempt = claim.Attempt }
