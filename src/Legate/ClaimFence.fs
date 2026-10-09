// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks

// Last-moment claim fence (issue 33, extended by issue 377): verifies the
// TurnClaim token immediately before each post-claim side effect and skips
// the effect when the claim is lost or missing, so the loser of a takeover
// produces zero effects. Applies to usage checkpoints, journal appends,
// the tool-call boundary (via checkBeforeCallAsync, wired into TurnLoop's
// per-tool verify hook), completion-sink notification, settlement, and the
// fenced nonterminal writes (inbox consumption, execution lifecycle, and
// persistent grants). Checkpoint, settle, journal appends, and the
// nonterminal writes are also fenced store-side (the stores return
// lost/rejected outcomes instead of acting); the fence adds the
// skip-without-calling layer, and for tool calls and sink notification,
// which no store fences, it is the fence. Never touches cancellation
// plumbing (issue 35); host and idle inbox/lifecycle paths stay unfenced
// by design (issue 373).
/// Execution-context-scoped fenced-write claim (issue 377): the captured
/// turn claim the running attempt consumes Inject entries under. The
/// actor enters it before invoking the runner (alongside ControlAdmission
/// and LeaseAdmission); the Inject fold drain reads it to consume
/// atomically under the same claim, so a takeover between a preliminary
/// verification and the consume rejects with zero effects. AsyncLocal, so
/// nested runner continuations inherit it.
module internal FencedClaimScope =
    let private current = AsyncLocal<TurnClaim option>()

    /// Reads the running attempt's fenced-write claim, or None outside a
    /// fenced turn.
    /// <returns>The claim, or None when no fenced turn is running.</returns>
    let currentClaim () : TurnClaim option = current.Value

    /// Enters the scope for one running attempt.
    /// <param name="claim">The captured claim, or None for unclaimed shells.</param>
    /// <returns>The scope to dispose when the attempt reports.</returns>
    let enter (claim: TurnClaim option) =
        let previous = current.Value
        current.Value <- claim

        { new IDisposable with
            member _.Dispose() = current.Value <- previous
        }

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

    /// Consumes inbox entries atomically under the claim: the token plus
    /// the current turn and attempt are checked in the same store
    /// statement or transaction as the write, so a takeover between a
    /// preliminary verification and the write rejects with zero effects.
    /// Never verify-then-write around the unfenced consume: that leaves
    /// the takeover window this fence closes.
    /// <param name="store">The durable store. Must not be null.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="claim">The claim fencing the consumption. Must not be null.</param>
    /// <param name="sessionId">The session whose entries to consume.</param>
    /// <param name="positions">The positions to consume. Must not be null.</param>
    /// <param name="cancellationToken">Abandons the consume.</param>
    /// <returns>True when the consume landed, false when the claim was fenced out.</returns>
    let consumeInboxAsync
        (store: ISessionStore)
        (tenant: TenantId)
        (claim: TurnClaim)
        (sessionId: SessionId)
        (positions: IReadOnlyList<int64>)
        (cancellationToken: CancellationToken)
        : Task<bool> =
        ArgumentNullException.ThrowIfNull(store)

        if isNull (box claim) then
            raise (ArgumentNullException(nameof claim))

        task {
            let! state = store.ConsumeInboxUnderClaim(tenant, claim, sessionId, positions, cancellationToken)
            return isLiveState state
        }

    /// Updates the execution-owned lifecycle state atomically under the
    /// claim: the token plus the current turn and attempt are checked in
    /// the same store statement or transaction as the write. Running and
    /// WaitingForInput only; host and idle paths keep the unfenced update.
    /// <param name="store">The durable store. Must not be null.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="claim">The claim fencing the update. Must not be null.</param>
    /// <param name="sessionId">The session to update.</param>
    /// <param name="state">The new execution-owned lifecycle state.</param>
    /// <param name="cancellationToken">Abandons the update.</param>
    /// <returns>True when the update landed, false when the claim was fenced out.</returns>
    let updateSessionStateAsync
        (store: ISessionStore)
        (tenant: TenantId)
        (claim: TurnClaim)
        (sessionId: SessionId)
        (state: SessionState)
        (cancellationToken: CancellationToken)
        : Task<bool> =
        ArgumentNullException.ThrowIfNull(store)

        if isNull (box claim) then
            raise (ArgumentNullException(nameof claim))

        task {
            let! lease = store.UpdateSessionStateUnderClaim(tenant, claim, sessionId, state, cancellationToken)
            return isLiveState lease
        }

    /// Records a persistent permission grant atomically under the claim:
    /// the token plus the current turn and attempt are checked in the same
    /// store statement or transaction as the write. Idempotent under the
    /// same live claim; a stale claim grants nothing.
    /// <param name="store">The durable store. Must not be null.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="claim">The claim fencing the grant. Must not be null.</param>
    /// <param name="sessionId">The session to grant the tool for.</param>
    /// <param name="toolName">The tool name the host allowed for the session. Must be a non-empty string.</param>
    /// <param name="cancellationToken">Abandons the grant.</param>
    /// <returns>True when the grant landed, false when the claim was fenced out.</returns>
    let grantSessionToolAsync
        (store: ISessionStore)
        (tenant: TenantId)
        (claim: TurnClaim)
        (sessionId: SessionId)
        (toolName: string)
        (cancellationToken: CancellationToken)
        : Task<bool> =
        ArgumentNullException.ThrowIfNull(store)

        if isNull (box claim) then
            raise (ArgumentNullException(nameof claim))

        task {
            let! lease = store.GrantSessionToolUnderClaim(tenant, claim, sessionId, toolName, cancellationToken)
            return isLiveState lease
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
                    do! sink.NotifyAsync(completion, cancellationToken)
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
