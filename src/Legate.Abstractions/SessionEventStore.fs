// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Generic
open System.Text.Json.Serialization
open System.Threading
open System.Threading.Tasks

// Session event store contracts. ISessionEventStore is the durable store
// contract for the ordered per-session event journal: it appends the
// SessionEvent stream under a claim fence, replays it by sequence cursor
// for Subscribe resumption, and leases journal cleanup to a background
// worker. It is deliberately separate from ISessionStore (session CRUD,
// inbox, turn claims): the journal is the system-facing audit trail and the
// source of Subscribe, while ISessionStore is the session lifecycle and
// turn queue. Sequence numbers are per-session, monotonic, gap-free, and
// assigned by the store on append: a SessionEvent arrives with an empty
// Sequence and the store stamps the next per-session value, so replay
// hydrates the same number through the constructor. The journal writer is
// the only appender: sanitisation and redaction are runtime concerns
// applied before the append, and the store persists what it is given,
// validating only bounds (the limits are host-configured runtime options;
// this contract fixes only how a breach is reported). Every method takes
// the tenant the data belongs to and must not see or touch another
// tenant's rows (isolation is enforced in the stores, not only in the
// host). Outcome families serialise polymorphically with System.Text.Json
// like the TurnLeaseState and TurnSettlement precedents.

// ───────────────────────────────────────────────────────────────────────────
// Append outcome

/// What the store decided when an event-journal append landed: the caller
/// (the journal writer) must branch on the outcome. A result object, never
/// an exception: a stale claim after a takeover is an expected branch of
/// the distributed lifecycle, not a failure. The events are stamped with
/// per-session monotonic sequences by the store on success and rejected
/// writes leave the journal untouched, so the caller can retry the batch
/// after recovering the claim without duplicating anything. Serialises
/// polymorphically: every concrete outcome carries a stable <c>$type</c>
/// discriminator on the wire, mirroring <see cref="T:Legate.TurnSettlement" />
/// and <see cref="T:Legate.TurnLeaseState" />.
[<AbstractClass>]
[<JsonPolymorphic(TypeDiscriminatorPropertyName = "$type")>]
[<JsonDerivedType(typeof<EventAppended>, "eventAppended")>]
[<JsonDerivedType(typeof<EventAppendRejected>, "eventAppendRejected")>]
type EventAppendOutcome() = class end

/// The append was applied: every event in the batch is journaled, in order,
/// with its per-session monotonic <see cref="T:Legate.SessionEvent" />.Sequence
/// stamped. The returned events are the stored shape: the caller should use
/// them (or the highest stamped sequence) for further appends and
/// subscribers.
/// <param name="events">The stamped events, in append order. The store created new instances with the sequence set; the input events are not mutated.</param>
and [<Sealed>] EventAppended(events: IReadOnlyList<SessionEvent>) =
    inherit EventAppendOutcome()

    /// The stamped events, in append order, each carrying its assigned
    /// per-session sequence number.
    member _.Events = events

/// The append was rejected because the claim token no longer holds the
/// turn: another owner took over (or the lease lapsed) between the claim
/// and the write. Nothing changed: the journal has no partial batch, and a
/// retry with the recovered token will land cleanly. The reason is a
/// stable string, never parsed from messages.
/// <param name="sessionId">The session whose append was rejected.</param>
/// <param name="reason">Why the append was rejected. Never contains secrets or tool arguments.</param>
and [<Sealed>] EventAppendRejected(sessionId: SessionId, reason: string) =
    inherit EventAppendOutcome()

    /// The session whose append was rejected.
    member _.SessionId = sessionId

    /// Why the append was rejected: "staleClaim" when the claim token does
    /// not own the turn's journal writes anymore. Never contains secrets
    /// or tool arguments.
    member _.Reason = reason

// ───────────────────────────────────────────────────────────────────────────
// Replay outcome

/// What the store returned for one bounded replay page of a session's
/// event journal: the caller (the runtime's Subscribe and resume paths)
/// must branch on the outcome. A result object, never an exception:
/// unknown session, expired journal, and end of stream are expected
/// branches, not failures. Serialises polymorphically: every concrete
/// outcome carries a stable <c>$type</c> discriminator on the wire,
/// mirroring <see cref="T:Legate.TurnSettlement" /> and
/// <see cref="T:Legate.TurnLeaseState" />. There is no malformed-cursor
/// branch: the cursor is an int64 sequence number, so it cannot be
/// malformed, and an out-of-range cursor resolves to end of stream.
[<AbstractClass>]
[<JsonPolymorphic(TypeDiscriminatorPropertyName = "$type")>]
[<JsonDerivedType(typeof<EventReplayPage>, "eventReplayPage")>]
[<JsonDerivedType(typeof<EventReplayEndOfStream>, "eventReplayEndOfStream")>]
[<JsonDerivedType(typeof<EventReplayUnknownSession>, "eventReplayUnknownSession")>]
[<JsonDerivedType(typeof<EventReplayJournalExpired>, "eventReplayJournalExpired")>]
type EventReplayOutcome() = class end

/// One bounded page of the journal: the events with a sequence strictly
/// greater than the asked-for cursor, in sequence order, at most the
/// asked-for limit, plus the cursor to pass verbatim into the next replay
/// call. NextCursor is empty (HasValue is false) when the page is the
/// last (callers replaying further will hit end of stream); a page with
/// events always carries a NextCursor of the highest stamped sequence in
/// the page.
/// <param name="sessionId">The session the page was read from.</param>
/// <param name="events">The journaled events, in sequence order, at most the asked-for limit. Never null.</param>
/// <param name="nextCursor">The cursor for the next replay call, or empty when the journal is exhausted after this page.</param>
and [<Sealed>] EventReplayPage(sessionId: SessionId, events: IReadOnlyList<SessionEvent>, nextCursor: Nullable<int64>) =
    inherit EventReplayOutcome()

    /// The session the page was read from.
    member _.SessionId = sessionId

    /// The journaled events, in sequence order, at most the asked-for
    /// limit. Never null.
    member _.Events = events

    /// The cursor to pass verbatim into the next replay call, or empty
    /// when the journal is exhausted after this page. It is the highest
    /// stamped sequence in the page, or the asked-for cursor when the
    /// page carries no events.
    member _.NextCursor = nextCursor

/// The replay read past the journal's last event: the cursor is valid, the
/// session exists, and no journaled event has a sequence beyond it. The
/// caller treats this as a settled tail: a later append will make replay
/// return events again.
/// <param name="sessionId">The session that was replayed.</param>
and [<Sealed>] EventReplayEndOfStream(sessionId: SessionId) =
    inherit EventReplayOutcome()

    /// The session that was replayed.
    member _.SessionId = sessionId

/// No session with that id exists in the tenant (or the session id is
/// unknown to the store). Distinct from journal expiry: a live session
/// with an empty journal replays end of stream, an unknown session does
/// not resolve at all.
/// <param name="sessionId">The session id that does not exist in the tenant.</param>
and [<Sealed>] EventReplayUnknownSession(sessionId: SessionId) =
    inherit EventReplayOutcome()

    /// The session id that does not exist in the tenant.
    member _.SessionId = sessionId

/// The session exists but its journal no longer does: it was archived or
/// deleted by cleanup (or retention) before this replay. The caller must
/// not treat this as end of stream: there are events it will never see.
/// <param name="sessionId">The session whose journal is gone.</param>
and [<Sealed>] EventReplayJournalExpired(sessionId: SessionId) =
    inherit EventReplayOutcome()

    /// The session whose journal is gone.
    member _.SessionId = sessionId

// ───────────────────────────────────────────────────────────────────────────
// Cleanup lease

/// One held lease over a session's journal cleanup, minted by
/// <see cref="M:Legate.ISessionEventStore.TryClaimCleanup*" />: a leased
/// background worker may archive and delete the journal under this claim.
/// The token is opaque exactly like <see cref="T:Legate.TurnClaim" />.Token:
/// the store mints it, the worker carries it verbatim into
/// CompleteCleanup and DeferCleanup, and nothing may parse or infer state
/// from it. Deliberately not <see cref="T:Legate.TurnClaim" />: a cleanup
/// claim is not a turn claim (no turn id, no attempt), and reusing the
/// turn-shaped record here would invite misuse. Constructible from C#
/// through property setters and serialises with System.Text.Json.
/// <param name="sessionId">The session whose journal cleanup is leased.</param>
/// <param name="token">The opaque claim token fencing the cleanup effects. Must not be null.</param>
/// <param name="owner">The lease owner identity: what a complete or defer call must match.</param>
/// <param name="expiresAt">When the lease expires unless the worker finishes or defers first.</param>
[<CLIMutable; NoComparison>]
type EventCleanupClaim =
    {
        /// The session whose journal cleanup is leased.
        SessionId: SessionId
        /// The opaque claim token. Stores mint it; the worker carries it
        /// verbatim into CompleteCleanup and DeferCleanup; nobody parses
        /// it. Must not be null.
        Token: string
        /// The claim owner identity.
        Owner: string
        /// When the lease expires unless the worker finishes or defers
        /// first.
        ExpiresAt: DateTimeOffset
    }

/// What the store decided when a cleanup-lease call landed: the background
/// worker must branch on the state. A result object, never an exception:
/// an already-claimed lease is an expected branch (another worker won the
/// race), not a failure. Serialises polymorphically: every concrete state
/// carries a stable <c>$type</c> discriminator on the wire, mirroring
/// <see cref="T:Legate.TurnLeaseState" />.
[<AbstractClass>]
[<JsonPolymorphic(TypeDiscriminatorPropertyName = "$type")>]
[<JsonDerivedType(typeof<EventCleanupClaimed>, "eventCleanupClaimed")>]
[<JsonDerivedType(typeof<EventCleanupNotClaimable>, "eventCleanupNotClaimable")>]
type EventCleanupState() = class end

/// The cleanup lease was granted: the caller may archive and delete the
/// journal under the claim's token before the claim's expiry, then settle
/// the lease through CompleteCleanup or DeferCleanup.
/// <param name="claim">The granted cleanup claim.</param>
and [<Sealed>] EventCleanupClaimed(claim: EventCleanupClaim) =
    inherit EventCleanupState()

    /// The granted cleanup claim, carrying the token the worker must
    /// present on complete and defer.
    member _.Claim = claim

/// The cleanup lease was not granted: the journal is already leased to
/// another owner whose lease has not expired, the journal is already
/// cleaned up, or the session does not exist in the tenant. Nothing
/// changed; the caller retries later or moves on.
/// <param name="sessionId">The session whose cleanup was requested.</param>
/// <param name="reason">Why the lease was not granted. Never contains secrets or tool arguments.</param>
and [<Sealed>] EventCleanupNotClaimable(sessionId: SessionId, reason: string) =
    inherit EventCleanupState()

    /// The session whose cleanup was requested.
    member _.SessionId = sessionId

    /// Why the lease was not granted: "leaseHeld" while another owner's
    /// lease has not expired, "journalGone" when there is no journal left
    /// to clean up, or "unknownSession" when the session id does not
    /// resolve in the tenant. Never contains secrets or tool arguments.
    member _.Reason = reason

// ───────────────────────────────────────────────────────────────────────────
// Cleanup settlement

/// What the store decided when a cleanup-leas settlement call landed: the
/// worker must branch on the outcome. A result object, never an exception:
/// a settlement arriving on a stale token is an expected branch of the
/// distributed lifecycle (the lease expired and another worker re-claimed),
/// not a failure. Serialises polymorphically: every concrete outcome
/// carries a stable <c>$type</c> discriminator on the wire, mirroring
/// <see cref="T:Legate.TurnSettlement" />.
[<AbstractClass>]
[<JsonPolymorphic(TypeDiscriminatorPropertyName = "$type")>]
[<JsonDerivedType(typeof<EventCleanupApplied>, "eventCleanupApplied")>]
[<JsonDerivedType(typeof<EventCleanupRejected>, "eventCleanupRejected")>]
type EventCleanupSettlement() = class end

/// The settlement was applied: CompleteCleanup marked the journal archived
/// (and deleted the live rows) or DeferCleanup released the lease back to
/// claimable, so the journal stays replayable.
/// <param name="sessionId">The session whose cleanup settled.</param>
/// <param name="completed">Whether the journal was actually cleaned up (a deferred settlement applies the release with completed false).</param>
and [<Sealed>] EventCleanupApplied(sessionId: SessionId, completed: bool) =
    inherit EventCleanupSettlement()

    /// The session whose cleanup settled.
    member _.SessionId = sessionId

    /// Whether the journal was cleaned up: true when the settlement is
    /// CompleteCleanup's applied outcome, false when it is DeferCleanup's
    /// applied release.
    member _.Completed = completed

/// The settlement was rejected because the claim token no longer holds the
/// cleanup lease: it expired and the journal re-opened to other claimants,
/// or another worker already settled it. Nothing changed beyond what the
/// winning worker did; the caller must not archive or delete anything.
/// <param name="sessionId">The session whose settlement was rejected.</param>
/// <param name="reason">Why the settlement was rejected. Never contains secrets or tool arguments.</param>
and [<Sealed>] EventCleanupRejected(sessionId: SessionId, reason: string) =
    inherit EventCleanupSettlement()

    /// The session whose settlement was rejected.
    member _.SessionId = sessionId

    /// Why the settlement was rejected: "staleClaim" when the token does
    /// not own the lease anymore, or "leaseExpired" when the lease lapsed
    /// before the settlement landed. Never contains secrets or tool
    /// arguments.
    member _.Reason = reason

// ───────────────────────────────────────────────────────────────────────────
// The store contract

/// The durable store contract for the ordered per-session event journal:
/// append events under a claim fence, replay from a sequence cursor with
/// bounded pages, and lease journal cleanup to a background worker. The
/// journal is the system-facing audit trail and the source of the runtime's
/// Subscribe; implementations back it with the Postgres, SQLite, and
/// in-memory stores the same way <see cref="T:Legate.ISessionStore" /> is.
/// Every method takes the tenant the data belongs to and must not see or
/// touch another tenant's rows (isolation is enforced here, not only in
/// the host).
///
/// <para>Limit reporting: the limit values (per-event bytes, per-session
/// event count, per-session bytes, append batch size) are host-configured
/// runtime options; they do not appear in this contract. An implementation
/// reports a breach by throwing
/// <see cref="T:Legate.EventLimitExceededException" /> with its structured
/// <see cref="P:Legate.EventLimitExceededException.LimitKind" />,
/// <see cref="P:Legate.EventLimitExceededException.Limit" />, and
/// <see cref="P:Legate.EventLimitExceededException.Observed" /> properties
/// populated, before any part of the batch lands: a limit breach never
/// leaves a partial write. Sanitisation and redaction are runtime concerns
/// applied before the append; the store persists what it is given and
/// validates only bounds.</para>
///
/// <para>Control-plane precondition failures throw
/// <see cref="T:Legate.LegateException" /> subtypes: an unknown session id
/// on <see cref="M:Legate.ISessionEventStore.Append*" /> throws
/// <see cref="T:Legate.SessionNotFoundException" /> (the same precondition
/// UpdateSessionState enforces: the appender must already hold a turn
/// claim over an existing session). Replay and cleanup resolve unknown
/// sessions as outcome objects the caller branches on, because those
/// callers race the journal's lifecycle and cannot treat the race as an
/// exceptional precondition failure.</para>
type ISessionEventStore =

    /// Appends a batch of events to the session's journal under the turn's
    /// claim token. The store assigns per-session monotonic, gap-free
    /// sequence numbers (1-based) to the events, stamps them onto the
    /// returned copies, and verifies the token at the last moment: a stale
    /// token produces <see cref="T:Legate.EventAppendRejected" /> and zero
    /// writes, so the loser of a takeover has no effects. Atomic: the whole
    /// batch lands or nothing does, so replay never observes a partial
    /// batch. A limit breach (per-event bytes, per-session count, or
    /// per-session bytes) throws
    /// <see cref="T:Legate.EventLimitExceededException" /> before anything
    /// is written.
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to append to.</param>
    /// <param name="claimToken">The opaque turn claim token fencing the journal write. Must not be null.</param>
    /// <param name="events">The events to append, in order, each with an empty (in-flight) Sequence. Must not be null or empty; at most the host's batch-size limit.</param>
    /// <param name="cancellationToken">Token that abandons the append.</param>
    /// <returns>The outcome: the stamped events, or the rejection with its reason.</returns>
    /// <exception cref="T:System.ArgumentNullException">The event list is null or one of its events is null.</exception>
    /// <exception cref="T:Legate.SessionNotFoundException">The session id does not exist in this tenant.</exception>
    /// <exception cref="T:Legate.EventLimitExceededException">An event, the batch, or the session's journal would breach a configured limit; nothing lands.</exception>
    abstract Append:
        tenant: TenantId *
        sessionId: SessionId *
        claimToken: string *
        events: IReadOnlyList<SessionEvent> *
        cancellationToken: CancellationToken ->
            Task<EventAppendOutcome>

    /// Replays one bounded page of the session's journal: the events with a
    /// sequence strictly greater than <paramref name="fromSequence" /> (0
    /// replays the whole journal), in sequence order, at most
    /// <paramref name="limit" /> events. The outcome carries the cursor to
    /// pass verbatim into the next call, or a distinct branch for unknown
    /// session, expired journal, and end of stream, so the resume and
    /// Subscribe paths never guess. There is no malformed-cursor branch: the
    /// cursor is an int64, and an out-of-range cursor resolves to end of
    /// stream.
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session whose journal to replay.</param>
    /// <param name="fromSequence">The exclusive cursor: replay events with a sequence strictly greater than it; 0 replays from the journal's first event.</param>
    /// <param name="limit">The maximum number of events on the page; must be positive.</param>
    /// <param name="cancellationToken">Token that abandons the replay.</param>
    /// <returns>The page outcome: a bounded page with the next cursor, end of stream, unknown session, or an expired journal.</returns>
    /// <exception cref="T:System.ArgumentOutOfRangeException">The limit is not positive or the cursor is negative.</exception>
    abstract Replay:
        tenant: TenantId *
        sessionId: SessionId *
        fromSequence: int64 *
        limit: int *
        cancellationToken: CancellationToken ->
            Task<EventReplayOutcome>

    /// Tries to grant the caller an exclusive lease over the session's
    /// journal cleanup, so one background worker at a time archives and
    /// deletes it. Refuses a second claimant while a live lease is held and
    /// resolves an unknown session as the not-claimable state (the worker
    /// polls and cannot treat the race as an exceptional precondition
    /// failure). Atomic: exactly one caller wins while a lease is open.
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session whose journal cleanup to lease.</param>
    /// <param name="owner">The lease owner identity. Must not be null.</param>
    /// <param name="leaseDuration">How long the lease lasts before it expires and the journal re-opens to other claimants.</param>
    /// <param name="cancellationToken">Token that abandons the claim.</param>
    /// <returns>The granted claim, or the not-claimable state with its reason.</returns>
    /// <exception cref="T:System.ArgumentNullException">The owner is null.</exception>
    /// <exception cref="T:System.ArgumentOutOfRangeException">The lease duration is not positive.</exception>
    abstract TryClaimCleanup:
        tenant: TenantId *
        sessionId: SessionId *
        owner: string *
        leaseDuration: TimeSpan *
        cancellationToken: CancellationToken ->
            Task<EventCleanupState>

    /// Completes the cleanup lease: the worker archived and deleted the
    /// journal. Fenced: the token is verified at the last moment and a
    /// stale token produces the rejected settlement instead of a write.
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session whose journal was cleaned up.</param>
    /// <param name="claimToken">The opaque token the granted claim carries. Must not be null.</param>
    /// <param name="cancellationToken">Token that abandons the settlement.</param>
    /// <returns>The settlement: applied (the journal is gone) or rejected with its reason.</returns>
    /// <exception cref="T:System.ArgumentNullException">The token is null.</exception>
    abstract CompleteCleanup:
        tenant: TenantId * sessionId: SessionId * claimToken: string * cancellationToken: CancellationToken ->
            Task<EventCleanupSettlement>

    /// Defers the cleanup lease: the worker could not finish (the archive
    /// upload failed, for instance) and releases the journal back to
    /// claimable without deleting anything. Fenced: the token is verified
    /// at the last moment and a stale token produces the rejected
    /// settlement instead of a write.
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session whose journal stays in place.</param>
    /// <param name="claimToken">The opaque token the granted claim carries. Must not be null.</param>
    /// <param name="cancellationToken">Token that abandons the settlement.</param>
    /// <returns>The settlement: applied (the lease is released, the journal intact) or rejected with its reason.</returns>
    /// <exception cref="T:System.ArgumentNullException">The token is null.</exception>
    abstract DeferCleanup:
        tenant: TenantId * sessionId: SessionId * claimToken: string * cancellationToken: CancellationToken ->
            Task<EventCleanupSettlement>
