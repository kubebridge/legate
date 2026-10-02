// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Threading
open System.Threading.Tasks

/// Disposition of a durable targeted host stop request. Acceptance is not settlement.
type HostAbortOutcome =
    /// The immutable request committed.
    | Accepted = 0
    /// The original request had already committed.
    | AlreadyAccepted = 1
    /// No real entry is executing.
    | NoCurrentTurn = 2
    /// The supplied target is not current and has no terminal evidence.
    | TargetChanged = 3
    /// A control verdict was committed before this request.
    | AlreadyTerminal = 4

/// State of a real entry's control attribution, independent of its prime lease.
type ControlTargetState =
    /// Execution may be admitted while there is no accepted stop.
    | Active = 0
    /// A control verdict exists; settlement/retirement is still pending.
    | TerminalPendingRetirement = 1
    /// The original entry was consumed and its binding retired.
    | Retired = 2

/// A persisted real-entry target. It carries no execution authority.
[<CLIMutable; NoComparison>]
type AbortTarget =
    {
        /// Explicit supported control format version. Missing serialized versions are invalid.
        FormatVersion: int
        /// Session containing the original entry.
        SessionId: SessionId
        /// Stable real-entry identity, distinct from the journal prime.
        TurnId: TurnId
        /// Original inbox position, retained after consumption.
        InboxPosition: int64
        /// Current control state; not the session lifecycle.
        State: ControlTargetState
        /// Accepted durable stop, or null when none exists.
        Stop: HostAbortReceipt | null
    }

/// Durable host intent, not a terminal result or promise of completion publication.
and [<CLIMutable; NoComparison>] HostAbortReceipt =
    {
        /// Explicit supported control format version.
        FormatVersion: int
        /// Exact session scope.
        SessionId: SessionId
        /// Exact original target; never retargeted on retry.
        TurnId: TurnId
        /// Request disposition.
        Outcome: HostAbortOutcome
        /// Original accepted timestamp, absent on nonacceptance.
        AcceptedAt: Nullable<DateTimeOffset>
        /// First accepted abort-family cause, absent on nonacceptance.
        Cause: Nullable<StopCause>
        /// First safe bounded reason, null on nonacceptance.
        Reason: string | null
        /// Known committed control status only when terminal evidence exists.
        TerminalStatus: Nullable<TurnStatus>
    }

/// Outcomes of a claim-fenced control operation. Read retries do not grant authority.
type ControlOperationOutcome =
    /// This operation committed under current authority.
    | Applied = 0
    /// The exact decision already committed; continuation needs separate current authority.
    | AlreadyDecided = 1
    /// The exact retirement already committed; no new drain is authorized.
    | AlreadyRetired = 2
    /// The original entry is not provably consumed.
    | NotReady = 3
    /// The captured prime no longer grants execution authority.
    | LostAuthority = 4
    /// The exact target/position association does not match.
    | TargetChanged = 5
    /// An idempotency identifier was reused with different content.
    | Conflict = 6
    /// A different decision already won for this target.
    | DecisionConflict = 7
    /// Durable stop or pending terminal decision forbids execution.
    | Stopped = 8

/// Immutable per-target control verdict. It is not a publication or cleanup receipt.
[<CLIMutable; NoComparison>]
type ControlTargetDecision =
    {
        /// Explicit supported control format version.
        FormatVersion: int
        /// Exact session scope.
        SessionId: SessionId
        /// Real-entry identity.
        TurnId: TurnId
        /// Original entry association.
        InboxPosition: int64
        /// Stable report idempotency key, never authority.
        DecisionId: string
        /// Original proposed terminal status.
        ProposedStatus: TurnStatus
        /// Original proposed stop cause, if applicable.
        ProposedCause: Nullable<StopCause>
        /// Original proposed safe bounded reason, if applicable.
        ProposedReason: string | null
        /// Canonical terminal status selected against durable host intent.
        Status: TurnStatus
        /// Canonical stop cause for Aborted, if applicable.
        Cause: Nullable<StopCause>
        /// Canonical safe stop reason, if applicable.
        Reason: string | null
        /// Store-clock decision timestamp.
        DecidedAt: DateTimeOffset
        /// Whether the exact binding was retired after proven consumption.
        Retired: bool
    }

/// Result of a fenced control operation, carrying canonical evidence where known.
[<CLIMutable; NoComparison>]
type ControlOperationResult =
    {
        /// Operation disposition.
        Outcome: ControlOperationOutcome
        /// Current binding, or null when absent.
        Target: AbortTarget | null
        /// Canonical control verdict, or null when none exists.
        Decision: ControlTargetDecision | null
    }

/// Required capability on the configured session store. Every operation is tenant-scoped
/// and serialized with prime claims, current binding and lifecycle competitors.
/// Host acceptance never activates actors, acquires claims, consumes entries or settles work.
/// Cancellation after commit can leave acceptance uncertain: retry the same target.
type ISessionAbortControlStore =
    /// Reads current attribution without loading session options or activating execution.
    abstract ReadAbortTarget:
        tenant: TenantId * sessionId: SessionId * cancellationToken: CancellationToken -> Task<AbortTarget | null>

    /// Atomically compares and persists an immutable ExplicitAbort/HostShutdown request.
    /// Exact accepted receipts precede lifecycle/target comparison on retry. Reasons are
    /// bounded to 512 non-control characters and must contain no credentials or payloads.
    abstract RequestHostAbort:
        tenant: TenantId *
        sessionId: SessionId *
        expectedTurnId: TurnId *
        cause: StopCause *
        reason: string *
        cancellationToken: CancellationToken ->
            Task<HostAbortReceipt>

    /// Installs a real-entry target under the actual captured prime, never replacing an unresolved binding.
    abstract BindControlTarget:
        tenant: TenantId *
        sessionId: SessionId *
        targetTurnId: TurnId *
        inboxPosition: int64 *
        capturedPrimeClaim: TurnClaim *
        cancellationToken: CancellationToken ->
            Task<ControlOperationResult>

    /// Checks actual prime authority, exact association and durable stop before execution admission.
    abstract CheckControlTarget:
        tenant: TenantId *
        sessionId: SessionId *
        targetTurnId: TurnId *
        inboxPosition: int64 *
        capturedPrimeClaim: TurnClaim *
        cancellationToken: CancellationToken ->
            Task<ControlOperationResult>

    /// Commits a small canonical control verdict, retaining the prime and pending binding.
    /// An earlier accepted host stop selects Aborted; an earlier verdict prevents acceptance.
    /// Same id/content retries return original evidence, not execution authority.
    abstract TryDecideControlTarget:
        tenant: TenantId *
        sessionId: SessionId *
        targetTurnId: TurnId *
        inboxPosition: int64 *
        capturedPrimeClaim: TurnClaim *
        decisionId: string *
        proposedTerminalStatus: TurnStatus *
        proposedCause: Nullable<StopCause> *
        proposedReason: string | null *
        cancellationToken: CancellationToken ->
            Task<ControlOperationResult>

    /// Retires only the exact pending binding under genuine authority after provable entry consumption.
    /// It never consumes an entry or releases a prime. Duplicate retirement authorizes no drain.
    abstract RetireControlTarget:
        tenant: TenantId *
        sessionId: SessionId *
        targetTurnId: TurnId *
        inboxPosition: int64 *
        capturedPrimeClaim: TurnClaim *
        decisionId: string *
        cancellationToken: CancellationToken ->
            Task<ControlOperationResult>
