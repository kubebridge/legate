// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks

/// A terminal settlement request for one accepted inbox entry, fenced by its captured prime claim.
/// The store validates captured claim authority and commits terminal consumption, lifecycle and
/// prime disposition, completion deduplication and outbox, and settlement bookkeeping under the
/// same takeover-serializing transaction or critical section. Observers and delivery follow a
/// committed winning outcome, never a preliminary liveness check.
[<Sealed>]
type SessionSettlementRequest
    (
        sessionId: SessionId,
        position: int64,
        claim: TurnClaim,
        executionId: Nullable<TurnId>,
        result: TurnResult,
        completionKey: string,
        terminalEvent: SessionEvent | null
    ) =

    /// The session containing the accepted entry.
    member _.SessionId = sessionId

    /// The immutable inbox entry position, not the shared prime turn id.
    member _.Position = position

    /// The authority captured when execution was admitted.
    member _.Claim = claim

    /// The attributable execution identity, absent for setup failure.
    member _.ExecutionId = executionId

    /// The selected verdict with actual cumulative usage evidence.
    member _.Result = result

    /// The stable logical completion key, preserved on retry.
    member _.CompletionKey = completionKey

    /// The sanitized, bounded optional terminal event. Never establishes authority.
    member _.TerminalEvent: SessionEvent | null = terminalEvent

/// The result of an atomic terminal settlement.
type SessionSettlementStatus =

    /// This request committed the winning settlement.
    | Applied = 0

    /// An identical request already committed. Do not notify FIFO observers again.
    | AlreadyApplied = 1

    /// Authority, attribution, or verdict conflicts with persisted state. No writes occurred.
    | Rejected = 2

/// The committed disposition of one inbox settlement.
[<Sealed>]
type SessionSettlementOutcome
    (
        status: SessionSettlementStatus,
        state: SessionState,
        result: TurnResult | null,
        completion: SessionCompletion | null,
        following: InboxEntry | null,
        events: IReadOnlyList<SessionEvent>,
        journalReason: string | null
    ) =

    /// Whether the request committed, retried identically, or rejected.
    member _.Status = status

    /// The authoritative lifecycle disposition, never an actor-computed inbox snapshot.
    member _.State = state

    /// The winning result, or null on rejection.
    member _.Result: TurnResult | null = result

    /// The committed outbox payload, or null for sinkless sessions or rejection.
    member _.Completion: SessionCompletion | null = completion

    /// The selected runnable entry, or null at quiescence. Execution still requires admission.
    member _.Following: InboxEntry | null = following

    /// Only events actually appended in the winning transaction. Publish after commit.
    member _.Events = events

    /// The safe optional-journal failure category, or null when no append failed.
    member _.JournalReason: string | null = journalReason

/// Required atomic settlement capability, additive to ISessionStore.
/// Implementations serialize settlement with ClaimNextTurn and takeover, host lifecycle writes,
/// and inbox append using database-level locking or conditional operations, not only a
/// process-local provider lock in Postgres. Verify-then-write is not conforming.
/// Unsupported compositions fail before the runtime accepts input.
type ISessionSettlementStore =

    /// Validates that the configured journal shares this capability's atomic boundary.
    /// A custom provider explicitly implements equivalent composition semantics.
    /// <param name="eventStore">The journal the terminal event appends to.</param>
    /// <returns>True when the journal composes atomically with this settlement capability.</returns>
    abstract SupportsSettlementJournal: eventStore: ISessionEventStore -> bool

    /// Admits an entry under the current captured claim without adopting another owner's pending
    /// execution. Returns false without writes for stale or closed state.
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session containing the entry.</param>
    /// <param name="position">The immutable inbox entry position.</param>
    /// <param name="claim">The captured claim authority. Must not be null.</param>
    /// <param name="cancellationToken">Token that abandons the admission.</param>
    /// <returns>True when execution is admitted under the captured claim.</returns>
    abstract AdmitExecution:
        tenant: TenantId *
        sessionId: SessionId *
        position: int64 *
        claim: TurnClaim *
        cancellationToken: CancellationToken ->
            Task<bool>

    /// Commits one terminal settlement under captured authority. Identical retry returns the
    /// persisted result and key; conflicting retry rejects without writes. Arbitrary external
    /// sinks are never called inside the transaction.
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="request">The terminal settlement request. Must not be null.</param>
    /// <param name="cancellationToken">Token that abandons the settlement.</param>
    /// <returns>The committed disposition of the settlement.</returns>
    abstract SettleExecution:
        tenant: TenantId * request: SessionSettlementRequest * cancellationToken: CancellationToken ->
            Task<SessionSettlementOutcome>

    /// Reads the committed winning settlement for one inbox position, or
    /// null when no terminal settlement has committed yet. Serves only the
    /// single authoritative <c>execution_settlements</c> row for
    /// (tenant, session, position): stale or losing completion reports never
    /// replace it, and an unsettled position reads as null, never a
    /// fabricated result. The returned correlation is evidence, never claim
    /// authority: callers must not treat the receipt or this read as
    /// permission to act for the turn. Tenant-scoped: rows of another tenant
    /// are invisible. Custom providers without this capability fail fast
    /// before the runtime accepts work that requires durable observation,
    /// with no process-local or unfenced fallback.
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session containing the entry.</param>
    /// <param name="position">The immutable inbox entry position.</param>
    /// <param name="cancellationToken">Token that abandons the read.</param>
    /// <returns>The winning settlement outcome, or null when unsettled.</returns>
    abstract TryReadCommitted:
        tenant: TenantId * sessionId: SessionId * position: int64 * cancellationToken: CancellationToken ->
            Task<SessionSettlementOutcome | null>

    /// Reads one inbox entry by position including consumed rows, or null
    /// when no entry carries that position in this tenant and session.
    /// Consumed entries stay readable until the retention policy removes
    /// them; pending entries read the same way. Tenant-scoped: rows of
    /// another tenant are invisible. Correlation is evidence, never claim
    /// authority.
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session containing the entry.</param>
    /// <param name="position">The immutable inbox entry position.</param>
    /// <param name="cancellationToken">Token that abandons the read.</param>
    /// <returns>The entry, or null when absent.</returns>
    abstract TryReadEntry:
        tenant: TenantId * sessionId: SessionId * position: int64 * cancellationToken: CancellationToken ->
            Task<InboxEntry | null>
