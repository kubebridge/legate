// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Generic
open System.Text.Json.Serialization
open System.Threading
open System.Threading.Tasks

// Session store contracts. ISessionStore is the durable store contract the
// Postgres, SQLite, and in-memory implementations implement: session CRUD,
// the session inbox in front of the turn queue, turn claims under a lease,
// dispatch candidates, and the capacity count queries the dispatcher (issue
// 13) enforces per-agent, per-tenant, and per-process limits with. The claim
// token and its owner live here, on the claim type, and never on Turn or the
// client surface: the boundary issue 7 drew. Lease states and settlement
// outcomes are result objects (the host must branch on them), control-plane
// preconditions throw the Exceptions.fs family. Every method takes a
// TenantId; the isolation is enforced in the stores, not only in the host.
// All types serialise with System.Text.Json and are constructible from C#
// through property setters.

// ───────────────────────────────────────────────────────────────────────────
// Claims and leases

/// A claim over one turn, held by one owner, fenced by an opaque token.
/// The runtime performs every side effect on behalf of the turn under this
/// claim: checkpoint, settle, and abort verify the token at the last moment,
/// so a stale owner cannot act after another took over. The token is opaque:
/// stores mint it, callers carry it through verbatim, and nothing may parse
/// or infer a lease state from it. Attempt mirrors
/// <see cref="T:Legate.Turn" />.Attempt (1-based, incremented on resume), so
/// a resumed run continues the same turn id under a new claim. Serialises
/// with System.Text.Json.
/// <param name="turnId">The turn claimed.</param>
/// <param name="token">The opaque claim token fencing the turn's side effects. Must not be null.</param>
/// <param name="owner">The claim owner (a session actor or node identity, for instance). Must not be null.</param>
/// <param name="expiresAt">When the lease expires unless renewed.</param>
/// <param name="attempt">The 1-based attempt the claim is for.</param>
[<CLIMutable; NoComparison>]
type TurnClaim =
    {
        /// The turn claimed.
        TurnId: TurnId
        /// The opaque claim token. Stores mint it; callers carry it verbatim
        /// into every fenced call (checkpoint, settle, abort, renew);
        /// nobody parses it.
        Token: string
        /// The claim owner identity: what a renew or fenced call must match.
        Owner: string
        /// When the lease expires unless renewed.
        ExpiresAt: DateTimeOffset
        /// The 1-based attempt the claim is for; incremented on resume.
        Attempt: int
    }

/// What the store decided when it last processed a claim on a turn: the
/// lease state the caller must branch on. A result object, never an
/// exception: lease states are expected branches of the distributed
/// lifecycle, not failures. Serialises polymorphically: every concrete
/// lease carries a stable <c>$type</c> discriminator on the wire, mirroring
/// <see cref="T:Legate.Reply" /> and <see cref="T:Legate.TurnOutcome" />.
/// The state set mirrors BridgeMCP's renewed / cancellation-observed /
/// lease-expired / ownership-lost / missing outcomes at turn granularity.
[<AbstractClass>]
[<JsonPolymorphic(TypeDiscriminatorPropertyName = "$type")>]
[<JsonDerivedType(typeof<TurnLeaseHeld>, "leaseHeld")>]
[<JsonDerivedType(typeof<TurnLeaseRenewed>, "leaseRenewed")>]
[<JsonDerivedType(typeof<TurnLeaseLost>, "leaseLost")>]
[<JsonDerivedType(typeof<TurnLeaseExpiring>, "leaseExpiring")>]
[<JsonDerivedType(typeof<TurnLeaseMissing>, "leaseMissing")>]
type TurnLeaseState() = class end

/// The claim is still live: the token matched, the lease had not expired,
/// and no takeover has happened. No store mutation occurred (VerifyClaim and
/// RenewClaim both return this when nothing needed to change).
/// <param name="claim">The claim as the store last knows it.</param>
and [<Sealed>] TurnLeaseHeld(claim: TurnClaim) =
    inherit TurnLeaseState()

    /// The claim as the store last knows it.
    member _.Claim = claim

/// The lease was renewed: the token matched and the expiry moved to the new
/// instant. Returned by RenewClaim (and the renew leg of
/// ObserveAndRenewClaim) when the lease stays live.
/// <param name="claim">The renewed claim, carrying the new expiry.</param>
and [<Sealed>] TurnLeaseRenewed(claim: TurnClaim) =
    inherit TurnLeaseState()

    /// The renewed claim, carrying the new expiry.
    member _.Claim = claim

/// The claim no longer holds the turn: the lease expired and another owner
/// claimed it, or a takeover moved it. The fenced effect must not run; the
/// caller releases everything it had claimed for and stops acting on the
/// turn.
/// <param name="turnId">The turn the claim lost.</param>
/// <param name="reason">Why the lease was lost: "expired" or "takenOver".</param>
and [<Sealed>] TurnLeaseLost(turnId: TurnId, reason: string) =
    inherit TurnLeaseState()

    /// The turn the claim lost.
    member _.TurnId = turnId

    /// Why the lease was lost: "expired" when the lease lapsed and the turn
    /// was reclaimed or is claimable again, "takenOver" when another owner
    /// holds the claim now. Never contains secrets or tool arguments.
    member _.Reason = reason

/// The lease still holds but is close to expiry: renew now or finish the
/// effect before the lease lapses. The returned claim carries the current,
/// not-yet-renewed expiry, so the caller can decide whether it can finish.
/// <param name="claim">The claim with the current, near-expiry expiry.</param>
and [<Sealed>] TurnLeaseExpiring(claim: TurnClaim) =
    inherit TurnLeaseState()

    /// The claim with the current, not-yet-renewed expiry.
    member _.Claim = claim

/// Which turn a store could not resolve a claim against, returned inside
/// the missing lease outcome. A reference type so the missing state can be
/// distinguished from "not looked up"; TurnId is always the asked-for turn.
/// <param name="turnId">The turn that does not exist or carries no claim.</param>
and [<Sealed>] TurnLeaseMissing(turnId: TurnId) =
    inherit TurnLeaseState()

    /// The turn that does not exist, is already settled, or carries no
    /// claim at all.
    member _.TurnId = turnId

// ───────────────────────────────────────────────────────────────────────────
// Settlement outcomes

/// What the store decided when a settle call landed: the caller must branch
/// on the outcome. A result object, never an exception: applying an outcome
/// twice (an at-least-once journal writer retrying, for instance) is an
/// expected branch, not a failure. Serialises polymorphically: every
/// concrete outcome carries a stable <c>$type</c> discriminator on the
/// wire, mirroring <see cref="T:Legate.Reply" /> and
/// <see cref="T:Legate.TurnOutcome" />.
[<AbstractClass>]
[<JsonPolymorphic(TypeDiscriminatorPropertyName = "$type")>]
[<JsonDerivedType(typeof<TurnSettled>, "turnSettled")>]
[<JsonDerivedType(typeof<TurnAlreadySettled>, "turnAlreadySettled")>]
[<JsonDerivedType(typeof<TurnSettleRejected>, "turnSettleRejected")>]
type TurnSettlement() = class end

/// The settlement was applied: the turn is now terminal.
/// <param name="turnId">The turn that settled.</param>
/// <param name="status">The terminal status the turn now carries.</param>
and [<Sealed>] TurnSettled(turnId: TurnId, status: TurnStatus) =
    inherit TurnSettlement()

    /// The turn that settled.
    member _.TurnId = turnId

    /// The terminal status the turn now carries: Completed, Aborted, or
    /// Failed.
    member _.Status = status

/// The turn had already been settled by this same outcome (an at-least-once
/// writer retrying an idempotent settle): nothing changed, the caller treats
/// the settlement as applied.
/// <param name="turnId">The turn that was already settled.</param>
and [<Sealed>] TurnAlreadySettled(turnId: TurnId) =
    inherit TurnSettlement()

    /// The turn that was already settled.
    member _.TurnId = turnId

/// The settle call was rejected because it arrived on a stale claim: the
/// lease the caller holds does not own the turn anymore. Nothing changed;
/// the caller must not treat the turn as settled.
/// <param name="turnId">The turn whose settlement was rejected.</param>
/// <param name="reason">Why the settlement was rejected: "staleClaim" or "alreadySettledByOther". Never contains secrets or tool arguments.</param>
and [<Sealed>] TurnSettleRejected(turnId: TurnId, reason: string) =
    inherit TurnSettlement()

    /// The turn whose settlement was rejected.
    member _.TurnId = turnId

    /// Why the settlement was rejected: "staleClaim" when the claim does
    /// not own the turn, "alreadySettledByOther" when a different outcome
    /// was already applied. Never contains secrets or tool arguments.
    member _.Reason = reason

// ───────────────────────────────────────────────────────────────────────────
// Inbox envelope

/// What an inbox entry carries: a <see cref="T:Legate.UserMessage" /> that
/// may start a turn, or a <see cref="T:Legate.Reply" /> that resumes a
/// suspended one. Serialises polymorphically: every concrete payload
/// carries a stable <c>$type</c> discriminator on the wire, mirroring
/// <see cref="T:Legate.Reply" /> and <see cref="T:Legate.TurnOutcome" />.
[<AbstractClass>]
[<JsonPolymorphic(TypeDiscriminatorPropertyName = "$type")>]
[<JsonDerivedType(typeof<UserMessagePayload>, "userMessage")>]
[<JsonDerivedType(typeof<ReplyPayload>, "reply")>]
type InboxPayload() = class end

/// The inbox entry carries a user message that may start a turn.
/// <param name="message">The user message. Must not be null.</param>
and [<Sealed>] UserMessagePayload(message: UserMessage) =
    inherit InboxPayload()

    do
        if box message |> isNull then
            raise (ArgumentNullException(nameof message))

    /// The user message.
    member _.Message = message

/// The inbox entry carries a reply that resumes a suspended turn.
/// <param name="reply">The reply. Must not be null.</param>
and [<Sealed>] ReplyPayload(reply: Reply) =
    inherit InboxPayload()

    do
        if box reply |> isNull then
            raise (ArgumentNullException(nameof reply))

    /// The reply.
    member _.Reply = reply

/// One pending entry of a session's inbox: the envelope the store persists
/// and the runtime consumes. The payload serialises polymorphically under
/// the stable <c>$type</c> discriminators
/// <see cref="T:Legate.UserMessagePayload" /> and
/// <see cref="T:Legate.ReplyPayload" /> declare; the envelope's own fields
/// (delivery mode, consumed flag, position) serialise flat. Position is
/// the store-assigned per-session ordering key: entries consume in
/// position order, and a store assigns the next position on append, so
/// readers never observe two entries with the same position in one
/// session. Consumed entries stay readable until the implementation's
/// retention policy removes them; ReadPendingInbox never returns one.
/// Constructible from C# through property setters and serialises with
/// System.Text.Json.
[<CLIMutable; NoComparison>]
type InboxEntry =
    {
        /// The session the entry was appended to.
        SessionId: SessionId
        /// The store-assigned per-session ordering position; entries
        /// consume in position order.
        Position: int64
        /// What the entry carries: a user message or a reply, wrapped in
        /// the payload hierarchy.
        Payload: InboxPayload
        /// How the message was delivered: Queue, Inject, or Interrupt.
        Delivery: DeliveryMode
        /// Whether the runtime has consumed the entry already. A consumed
        /// entry is never returned by ReadPendingInbox again.
        Consumed: bool
        /// When the entry was appended.
        AppendedAt: DateTimeOffset
    }

/// One bounded page of the session list: what
/// <see cref="M:Legate.ISessionStore.ListSessions*" /> returns. Continuation
/// is null on the last page; callers pass it verbatim into the next call.
/// Bounded: Items holds at most the asked-for page size. Constructible from
/// C# through property setters and serialises with System.Text.Json.
[<CLIMutable; NoComparison>]
type SessionPage =
    {
        /// The sessions on this page, at most the asked-for page size, in
        /// the store's stable ordering.
        Items: IReadOnlyList<Session>
        /// The continuation token for the next page, or null when the list
        /// is exhausted. Callers pass it verbatim into the next call.
        Continuation: string | null
    }

/// The input of
/// <see cref="M:Legate.ISessionStore.GetDispatchCandidates*" />: which
/// sessions have pending work, bounded to a batch the dispatcher may act on
/// at once. Bounded: Sessions holds at most the asked-for batch size.
/// Constructible from C# through property setters and serialises with
/// System.Text.Json.
[<CLIMutable; NoComparison>]
type DispatchBatch =
    {
        /// The sessions with pending inbox entries, at most the asked-for
        /// batch size, in the store's stable ordering.
        Sessions: IReadOnlyList<SessionId>
        /// Whether more pending sessions exist beyond this batch, so the
        /// caller knows to ask again.
        HasMore: bool
    }

// ───────────────────────────────────────────────────────────────────────────
// The store contract

/// The durable store contract for sessions: session CRUD and list paging,
/// the session inbox in front of the turn queue, turn claims under a
/// lease, dispatch candidates, and the capacity count queries the
/// dispatcher enforces per-agent, per-tenant, and per-process limits with.
/// Every method takes the tenant the data belongs to and must not see or
/// touch another tenant's rows (isolation is enforced here, not only in
/// the host).
///
/// <para>Control-plane precondition failures throw
/// <see cref="T:Legate.LegateException" /> subtypes: an unknown session id
/// throws <see cref="T:Legate.SessionNotFoundException" /> and a session in
/// a disallowed state throws
/// <see cref="T:Legate.InvalidSessionStateException" />. Lease states and
/// settlement outcomes are result objects the caller branches on, never
/// exceptions: expired, lost, and stale are expected branches of the
/// distributed lifecycle.</para>
///
/// <para>Atomicity rules implementations must honour:</para>
/// <list type="bullet">
/// <item><description><b>AppendInboxMessage</b> is atomic: the entry and
/// its assigned position become visible together or not at all.</description></item>
/// <item><description><b>ClaimNextTurn</b> is atomic: exactly one caller
/// wins a turn; a loser observes a pending-less session or a claimed
/// turn, never a double claim.</description></item>
/// <item><description><b>RenewClaim</b> (with and without cancellation
/// observation) and <b>CheckpointUsage</b> are atomic: the new expiry and
/// the usage snapshot each land in one transaction.</description></item>
/// <item><description><b>SettleTurn</b> is atomic: the outcome and the
/// turn's terminal status land together or not at all.</description></item>
/// <item><description><b>UpdateSessionState</b> is atomic: the state
/// change and its timestamp land together or not at all.</description></item>
/// </list>
///
/// <para>Fencing rules implementations and callers must honour: every side
/// effect performed on behalf of a turn (checkpoint, settle, abort, and
/// any tool call or journal write the runtime makes after claiming) must
/// verify the claim token at the last moment, immediately before the
/// effect. A correlation id is evidence, not authority; only
/// <see cref="T:Legate.TurnClaim" />.Token is. A stale token must never
/// produce an effect: renew, checkpoint, settle, and abort return the
/// lost/rejected outcomes instead of acting.</para>
type ISessionStore =

    // ── Sessions ──

    /// Creates a session row and returns the stored shape, timestamps
    /// stamped by the store. Idempotent by identity: creating the same
    /// session id twice is a host bug and rejects.
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="session">The session to create. Must not be null.</param>
    /// <param name="cancellationToken">Token that abandons the create.</param>
    /// <returns>The stored session.</returns>
    /// <exception cref="T:System.ArgumentNullException">The session is null.</exception>
    /// <exception cref="T:Legate.InvalidSessionStateException">A session with the same id already exists in this tenant.</exception>
    abstract CreateSession: tenant: TenantId * session: Session * cancellationToken: CancellationToken -> Task<Session>

    /// Reads one session. Returns null when the id does not exist in the
    /// tenant; the client-facing methods translate that into
    /// SessionNotFoundException.
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to read.</param>
    /// <param name="cancellationToken">Token that abandons the read.</param>
    /// <returns>The session, or null when absent.</returns>
    abstract GetSession:
        tenant: TenantId * sessionId: SessionId * cancellationToken: CancellationToken -> Task<Session | null>

    /// Lists sessions of one tenant, newest first by
    /// <see cref="T:Legate.Session" />.UpdatedAt, optionally filtered by
    /// state, bounded to one page with a continuation token.
    /// <param name="tenant">The tenant whose sessions to list.</param>
    /// <param name="state">The state to filter by, or null for every state.</param>
    /// <param name="pageSize">The maximum number of sessions on the page; must be positive.</param>
    /// <param name="continuation">The continuation token from the previous page, or null for the first page.</param>
    /// <param name="cancellationToken">Token that abandons the list.</param>
    /// <returns>One bounded page of sessions.</returns>
    /// <exception cref="T:System.ArgumentOutOfRangeException">The page size is not positive.</exception>
    abstract ListSessions:
        tenant: TenantId *
        state: Nullable<SessionState> *
        pageSize: int *
        continuation: string | null *
        cancellationToken: CancellationToken ->
            Task<SessionPage>

    /// Updates a session's lifecycle state and stamps
    /// <see cref="T:Legate.Session" />.UpdatedAt. Atomic: the state change
    /// and its timestamp land together. Closed is terminal: a session may
    /// not leave it.
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to update.</param>
    /// <param name="state">The new lifecycle state.</param>
    /// <param name="cancellationToken">Token that abandons the update.</param>
    /// <returns>The stored session after the update.</returns>
    /// <exception cref="T:Legate.SessionNotFoundException">The session id does not exist in this tenant.</exception>
    /// <exception cref="T:Legate.InvalidSessionStateException">The session is Closed and the call tries to move it out.</exception>
    abstract UpdateSessionState:
        tenant: TenantId * sessionId: SessionId * state: SessionState * cancellationToken: CancellationToken ->
            Task<Session>

    /// Closes a session: state becomes Closed and
    /// <see cref="T:Legate.Session" />.ClosedAt is stamped. Idempotent:
    /// closing an already-closed session is a no-op returning the stored
    /// shape.
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to close.</param>
    /// <param name="cancellationToken">Token that abandons the close.</param>
    /// <returns>The stored session after the close.</returns>
    /// <exception cref="T:Legate.SessionNotFoundException">The session id does not exist in this tenant.</exception>
    abstract CloseSession:
        tenant: TenantId * sessionId: SessionId * cancellationToken: CancellationToken -> Task<Session>

    /// Rebinds the agent a session converses with. Only while no turn is
    /// running: the state rules are
    /// <see cref="P:Legate.Session.CurrentTurnId" />-based.
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to rebind.</param>
    /// <param name="agentId">The agent the session converses with from now on.</param>
    /// <param name="cancellationToken">Token that abandons the rebind.</param>
    /// <returns>The stored session after the rebind.</returns>
    /// <exception cref="T:Legate.SessionNotFoundException">The session id does not exist in this tenant.</exception>
    /// <exception cref="T:Legate.InvalidSessionStateException">A turn is running or suspended in the session.</exception>
    abstract SetSessionAgent:
        tenant: TenantId * sessionId: SessionId * agentId: AgentId * cancellationToken: CancellationToken ->
            Task<Session>

    /// Renames a session's title.
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to rename.</param>
    /// <param name="title">The new title; never null (the empty string is the host clearing it).</param>
    /// <param name="cancellationToken">Token that abandons the rename.</param>
    /// <returns>The stored session after the rename.</returns>
    /// <exception cref="T:Legate.SessionNotFoundException">The session id does not exist in this tenant.</exception>
    abstract SetSessionTitle:
        tenant: TenantId * sessionId: SessionId * title: string * cancellationToken: CancellationToken -> Task<Session>

    // ── Inbox ──

    /// Appends one message to the session's inbox with its delivery mode.
    /// Atomic: the entry and its assigned position become visible
    /// together.
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to append to.</param>
    /// <param name="payload">What the entry carries: a user message or a reply. Must not be null.</param>
    /// <param name="delivery">How the message was delivered.</param>
    /// <param name="cancellationToken">Token that abandons the append.</param>
    /// <returns>The stored entry, position and timestamp stamped by the store.</returns>
    /// <exception cref="T:System.ArgumentNullException">The payload is null.</exception>
    /// <exception cref="T:Legate.SessionNotFoundException">The session id does not exist in this tenant.</exception>
    abstract AppendInboxMessage:
        tenant: TenantId *
        sessionId: SessionId *
        payload: InboxPayload *
        delivery: DeliveryMode *
        cancellationToken: CancellationToken ->
            Task<InboxEntry>

    /// Reads the session's pending (not yet consumed) inbox entries in
    /// position order. Never returns consumed entries.
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session whose inbox to read.</param>
    /// <param name="cancellationToken">Token that abandons the read.</param>
    /// <returns>The pending entries in position order; empty when the inbox has none.</returns>
    /// <exception cref="T:Legate.SessionNotFoundException">The session id does not exist in this tenant.</exception>
    abstract ReadPendingInbox:
        tenant: TenantId * sessionId: SessionId * cancellationToken: CancellationToken ->
            Task<IReadOnlyList<InboxEntry>>

    /// Marks inbox entries consumed by position, so
    /// <see cref="M:Legate.ISessionStore.ReadPendingInbox*" /> stops
    /// returning them. Consuming a position twice is a no-op.
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session whose entries to consume.</param>
    /// <param name="positions">The positions to consume. Must not be null.</param>
    /// <param name="cancellationToken">Token that abandons the consume.</param>
    /// <returns>How many entries flipped from pending to consumed.</returns>
    /// <exception cref="T:System.ArgumentNullException">The position list is null.</exception>
    abstract MarkInboxConsumed:
        tenant: TenantId * sessionId: SessionId * positions: IReadOnlyList<int64> * cancellationToken: CancellationToken ->
            Task<int>

    // ── Claims and leases ──

    /// Claims the session's next pending turn under a lease. Atomic:
    /// exactly one caller wins a turn. Returns the missing lease state
    /// when the session has no claimable turn, so the caller branches on
    /// the result rather than catching an exception.
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session whose next turn to claim.</param>
    /// <param name="owner">The claim owner identity. Must not be null.</param>
    /// <param name="leaseDuration">How long the lease lasts before it expires.</param>
    /// <param name="cancellationToken">Token that abandons the claim.</param>
    /// <returns>The claim, or the missing lease state when the session has no claimable turn.</returns>
    /// <exception cref="T:System.ArgumentNullException">The owner is null.</exception>
    /// <exception cref="T:Legate.SessionNotFoundException">The session id does not exist in this tenant.</exception>
    abstract ClaimNextTurn:
        tenant: TenantId *
        sessionId: SessionId *
        owner: string *
        leaseDuration: TimeSpan *
        cancellationToken: CancellationToken ->
            Task<TurnLeaseState>

    /// Renews the lease a claim holds, observing whether the host asked to
    /// cancel the turn while renewing. The result carries the new lease
    /// state (renewed, expiring, lost, missing) and, when observed, the
    /// cancellation request; the caller branches on it.
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="claim">The claim to renew. Must not be null.</param>
    /// <param name="leaseDuration">How long the renewed lease lasts.</param>
    /// <param name="cancellationToken">Token that abandons the renewal.</param>
    /// <returns>The new lease state after the renewal attempt.</returns>
    /// <exception cref="T:System.ArgumentNullException">The claim is null.</exception>
    abstract RenewClaim:
        tenant: TenantId * claim: TurnClaim * leaseDuration: TimeSpan * cancellationToken: CancellationToken ->
            Task<TurnLeaseState>

    /// Renew the lease and report whether the host asked to cancel the
    /// turn while renewing: the renewal and the cancellation observation
    /// land in one atomic step, so a renewing owner cannot miss a cancel
    /// that arrived mid-renewal.
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="claim">The claim to renew. Must not be null.</param>
    /// <param name="leaseDuration">How long the renewed lease lasts.</param>
    /// <param name="cancellationToken">Token that abandons the renewal.</param>
    /// <returns>The new lease state; a cancellation request, when observed, travels on the returned state so the caller can abort promptly.</returns>
    /// <exception cref="T:System.ArgumentNullException">The claim is null.</exception>
    abstract ObserveAndRenewClaim:
        tenant: TenantId * claim: TurnClaim * leaseDuration: TimeSpan * cancellationToken: CancellationToken ->
            Task<TurnLeaseState>

    /// Verifies a claim without changing anything: the caller fences an
    /// imminent side effect by checking the claim token at the last
    /// moment. Returns the current lease state, never mutating the lease.
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="claim">The claim to verify. Must not be null.</param>
    /// <param name="cancellationToken">Token that abandons the verification.</param>
    /// <returns>The lease state the claim currently holds.</returns>
    /// <exception cref="T:System.ArgumentNullException">The claim is null.</exception>
    abstract VerifyClaim:
        tenant: TenantId * claim: TurnClaim * cancellationToken: CancellationToken -> Task<TurnLeaseState>

    /// Checkpoints usage for the turn under the claim. Fenced: the token
    /// is verified at the last moment and a stale claim produces the lost
    /// lease state instead of a write. Atomic: the usage snapshot lands in
    /// one transaction.
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="claim">The claim fencing the checkpoint. Must not be null.</param>
    /// <param name="usage">The usage to checkpoint.</param>
    /// <param name="cancellationToken">Token that abandons the checkpoint.</param>
    /// <returns>The lease state after the fenced checkpoint: held when the checkpoint landed, lost or missing otherwise.</returns>
    /// <exception cref="T:System.ArgumentNullException">The claim is null.</exception>
    abstract CheckpointUsage:
        tenant: TenantId * claim: TurnClaim * usage: UsageSummary * cancellationToken: CancellationToken ->
            Task<TurnLeaseState>

    /// Settles the turn with an idempotent outcome: the first settle wins,
    /// a retry of the same outcome observes the already-settled state, and
    /// a different outcome on a settled turn is rejected. Fenced: the
    /// token is verified at the last moment. Atomic: the outcome and the
    /// terminal status land together.
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="claim">The claim fencing the settlement. Must not be null.</param>
    /// <param name="status">The terminal status to apply: Completed, Aborted, or Failed.</param>
    /// <param name="outcome">The structured outcome, or null when the turn carries none.</param>
    /// <param name="cancellationToken">Token that abandons the settlement.</param>
    /// <returns>The settlement result: settled, already settled, or rejected.</returns>
    /// <exception cref="T:System.ArgumentNullException">The claim is null.</exception>
    /// <exception cref="T:Legate.InvalidSessionStateException">The status is not a terminal status.</exception>
    abstract SettleTurn:
        tenant: TenantId *
        claim: TurnClaim *
        status: TurnStatus *
        outcome: TurnOutcome | null *
        cancellationToken: CancellationToken ->
            Task<TurnSettlement>

    /// Aborts the turn under the claim: the turn's terminal status becomes
    /// Aborted and the lease is released. Fenced: the token is verified at
    /// the last moment and a stale claim produces the lost lease state.
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="claim">The claim fencing the abort. Must not be null.</param>
    /// <param name="cancellationToken">Token that abandons the abort.</param>
    /// <returns>The lease state after the fenced abort: held when the abort landed, lost or missing otherwise.</returns>
    /// <exception cref="T:System.ArgumentNullException">The claim is null.</exception>
    abstract AbortTurn:
        tenant: TenantId * claim: TurnClaim * cancellationToken: CancellationToken -> Task<TurnLeaseState>

    // ── Dispatch and capacity ──

    /// Lists sessions of the tenant with pending inbox entries, bounded to
    /// a batch. The dispatcher polls this to wake sessions with work.
    /// <param name="tenant">The tenant whose pending sessions to list.</param>
    /// <param name="maxBatch">The maximum number of sessions in the batch; must be positive.</param>
    /// <param name="cancellationToken">Token that abandons the query.</param>
    /// <returns>The bounded batch, with HasMore telling the caller to poll again.</returns>
    /// <exception cref="T:System.ArgumentOutOfRangeException">The batch size is not positive.</exception>
    abstract GetDispatchCandidates:
        tenant: TenantId * maxBatch: int * cancellationToken: CancellationToken -> Task<DispatchBatch>

    /// Counts the tenant's sessions assigned to one agent, the per-agent
    /// capacity input. Atomic snapshot per call.
    /// <param name="tenant">The tenant whose sessions to count.</param>
    /// <param name="agentId">The agent whose sessions to count.</param>
    /// <param name="cancellationToken">Token that abandons the count.</param>
    /// <returns>The session count for the agent in the tenant.</returns>
    abstract CountSessionsByAgent:
        tenant: TenantId * agentId: AgentId * cancellationToken: CancellationToken -> Task<int>

    /// Counts the tenant's sessions, the per-tenant capacity input. Atomic
    /// snapshot per call.
    /// <param name="tenant">The tenant whose sessions to count.</param>
    /// <param name="cancellationToken">Token that abandons the count.</param>
    /// <returns>The session count for the tenant.</returns>
    abstract CountSessionsByTenant: tenant: TenantId * cancellationToken: CancellationToken -> Task<int>

    /// Counts the sessions with a turn in flight across every tenant the
    /// process serves, the per-process capacity input. Atomic snapshot per
    /// call.
    /// <param name="cancellationToken">Token that abandons the count.</param>
    /// <returns>The session count with a turn in flight.</returns>
    abstract CountRunningSessions: cancellationToken: CancellationToken -> Task<int>
