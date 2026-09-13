// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Generic
open System.Text.Json.Serialization
open System.Threading
open System.Threading.Tasks

// Host hooks: the three contracts hosts implement to observe and bound the
// runtime from outside. An ISessionCompletionSink receives a headless
// session's structured completion; an IAgentAuditSink is notified when a
// host-managed agent changes; an IArtifactQuota bounds artifact storage with
// reserve/commit/release accounting. The completion and quota payloads are
// plain records that serialise with System.Text.Json, and the quota outcome
// is a typed result hierarchy with stable $type discriminators like
// TurnOutcome and AgentUpdateOutcome: a quota breach is an expected branch,
// never an exception. Public signatures stay BCL-only (no option, list, or
// DU).

/// The structured completion of a headless session, delivered to
/// <see cref="M:Legate.ISessionCompletionSink.Notify*" /> when the session
/// completes. Carries the session's id, the settling turn's
/// <see cref="T:Legate.TurnResult" />, the host metadata the session was
/// opened with, and the idempotency key deduplication relies on. Usage never
/// carries cost; secrets never travel in metadata. Serialises with
/// System.Text.Json.
[<CLIMutable; NoComparison>]
type SessionCompletion =
    {
        /// The session that completed.
        SessionId: SessionId
        /// The result of the turn that settled the session: the final
        /// assistant text, status, iterations, usage, and, when the session
        /// ran with the structured outcome mode, the
        /// <see cref="T:Legate.TurnOutcome" />.
        TurnResult: TurnResult
        /// The host metadata carried with the session (for example a
        /// correlation id), or null when the session had none. Never logged
        /// by the runtime.
        Metadata: IReadOnlyDictionary<string, string> | null
        /// The stable key the runtime assigns this delivery; sinks
        /// deduplicate on it because delivery is at-least-once.
        IdempotencyKey: string
    }

/// How a host receives a headless session's structured completion: the
/// runtime calls Notify once per delivery attempt when the session
/// completes. The contract pins three rules:
/// <list type="bullet">
/// <item><description><b>Synchronous and non-blocking.</b> Notify returns
/// unit and is called inline when the session completes, like
/// <see cref="T:Legate.IUsageObserver" />: sinks hand the payload to their
/// own queue or sink and return immediately, never blocking or
/// waiting.</description></item>
/// <item><description><b>At-least-once delivery.</b> A completion may be
/// delivered more than once, for example after a crash and resume before the
/// delivery was durably recorded: sinks deduplicate on the payload's
/// <see cref="P:Legate.SessionCompletion.IdempotencyKey" />.</description></item>
/// <item><description><b>No cross-session ordering.</b> Deliveries from
/// different sessions carry no ordering guarantee.</description></item>
/// </list>
/// There is no default sink: when a session's
/// <see cref="P:Legate.SessionOptions.CompletionSink" /> is null, the
/// runtime notifies nothing. The signed-webhook implementation belongs to
/// the headless-sessions epic.
type ISessionCompletionSink =

    /// Receives one completion delivery for the session.
    /// <param name="completion">The session's structured completion, carrying the idempotency key sinks deduplicate on.</param>
    abstract Notify: completion: SessionCompletion -> unit

/// How hosts observe agent changes for audit: an optional asynchronous hook
/// invoked by hosts that manage agents through Legate's
/// <see cref="T:Legate.IAgentStore" /> wrapper. The host wrapper owns
/// invocation: the runtime never calls this hook, so there is no default and
/// no registration seam; a host that audits agent create/update writes calls
/// it after its wrapper lands the change. The
/// <see cref="T:Legate.Agent" /> record travels directly, like
/// <see cref="P:Legate.AgentUpdated.Agent" />.
type IAgentAuditSink =

    /// Notifies the sink that an agent changed.
    /// <param name="agent">The agent after the change, as stored.</param>
    /// <param name="cancellationToken">Token that abandons the audit.</param>
    /// <returns>A task that completes once the audit hook has handled the change.</returns>
    abstract OnAgentChanged: agent: Agent * cancellationToken: CancellationToken -> Task

/// What the runtime asks an <see cref="T:Legate.IArtifactQuota" /> to
/// reserve: which tenant and session want to store artifacts and how many
/// bytes the reservation covers. Serialises with System.Text.Json;
/// identifiers serialise as plain strings.
[<CLIMutable; NoComparison>]
type ArtifactQuotaRequest =
    {
        /// The tenant the artifacts belong to.
        Tenant: TenantId
        /// The session the artifacts belong to.
        SessionId: SessionId
        /// The number of bytes the caller wants to reserve.
        RequestedBytes: int64
    }

/// What an artifact quota decided about one reservation request. Serialises
/// polymorphically: every concrete decision carries a stable <c>$type</c>
/// discriminator on the wire, mirroring
/// <see cref="T:Legate.AgentUpdateOutcome" /> and
/// <see cref="T:Legate.SessionAdmissionDecision" />. A quota breach is an
/// expected branch, never an exception. The static conveniences
/// <see cref="M:Legate.ArtifactQuotaDecision.Grant(System.String)" /> and
/// <see cref="P:Legate.ArtifactQuotaDecision.Exhausted" /> build decisions
/// without naming the sealed subtypes.
[<AbstractClass>]
[<JsonPolymorphic(TypeDiscriminatorPropertyName = "$type")>]
[<JsonDerivedType(typeof<QuotaGranted>, "quotaGranted")>]
[<JsonDerivedType(typeof<QuotaExhausted>, "quotaExhausted")>]
type ArtifactQuotaDecision() =

    /// The decision that grants the reservation.
    /// <param name="reservationId">The opaque reservation id the caller presents verbatim to <see cref="M:Legate.IArtifactQuota.Commit*" /> and <see cref="M:Legate.IArtifactQuota.Release*" />.</param>
    /// <returns>A fresh <see cref="T:Legate.QuotaGranted" /> carrying the reservation id.</returns>
    static member Grant(reservationId: string) : ArtifactQuotaDecision =
        QuotaGranted(reservationId) :> ArtifactQuotaDecision

    /// The decision that denies the reservation because the quota is
    /// exhausted.
    /// <param name="requestedBytes">The number of bytes the caller asked to reserve.</param>
    /// <param name="allowedBytes">The number of bytes the quota could still grant at decision time.</param>
    /// <returns>A fresh <see cref="T:Legate.QuotaExhausted" /> carrying the sizes.</returns>
    static member Exhausted(requestedBytes: int64, allowedBytes: int64) : ArtifactQuotaDecision =
        QuotaExhausted(requestedBytes, allowedBytes) :> ArtifactQuotaDecision

/// The decision that granted the reservation: the caller may store up to
/// <see cref="T:Legate.QuotaGranted" />'s reserved bytes once it has
/// committed.
/// <param name="reservationId">The opaque reservation id the caller presents verbatim to commit and release.</param>
and [<Sealed>] QuotaGranted(reservationId: string) =
    inherit ArtifactQuotaDecision()

    /// The opaque reservation id the caller presents verbatim to
    /// <see cref="M:Legate.IArtifactQuota.Commit*" /> and
    /// <see cref="M:Legate.IArtifactQuota.Release*" />: a correlation id as
    /// evidence, never authority.
    member _.ReservationId = reservationId

/// The decision that denied the reservation: the quota had fewer bytes free
/// than requested. Nothing was reserved. The sizes travel back so the caller
/// can report how much it could have stored.
/// <param name="requestedBytes">The number of bytes the caller asked to reserve.</param>
/// <param name="allowedBytes">The number of bytes the quota could still grant at decision time.</param>
and [<Sealed>] QuotaExhausted(requestedBytes: int64, allowedBytes: int64) =
    inherit ArtifactQuotaDecision()

    /// The number of bytes the caller asked to reserve.
    member _.RequestedBytes = requestedBytes

    /// The number of bytes the quota could still grant at decision time.
    member _.AllowedBytes = allowedBytes

/// How the runtime bounds artifact storage for a session: hosts implement
/// the reserve/commit/release accounting and the runtime branches on the
/// decision. The contract pins the lifecycle:
/// <list type="bullet">
/// <item><description><b>Reserve</b> asks whether the bytes may be stored
/// and, when granted, holds them against the quota under the returned
/// reservation id.</description></item>
/// <item><description><b>Commit</b> turns a granted reservation into
/// accounted usage after the artifacts are durably stored; the reserved
/// bytes stay counted.</description></item>
/// <item><description><b>Release</b> gives a granted reservation's bytes
/// back without counting them, for artifacts that were not stored; commit
/// and release are both idempotent on an already-settled reservation
/// id.</description></item>
/// </list>
/// A denial is the expected branch <see cref="T:Legate.QuotaExhausted" />,
/// never an exception. The runtime default when a host registers none is
/// <see cref="T:Legate.UnlimitedArtifactQuota" />.
type IArtifactQuota =

    /// Asks the quota to reserve bytes for one session's artifacts.
    /// <param name="request">Which tenant and session ask, and how many bytes to reserve.</param>
    /// <param name="cancellationToken">Token that abandons the reservation.</param>
    /// <returns>The grant with its reservation id, or the exhausted outcome with the sizes.</returns>
    abstract Reserve:
        request: ArtifactQuotaRequest * cancellationToken: CancellationToken -> Task<ArtifactQuotaDecision>

    /// Counts a granted reservation's bytes as used, after the artifacts
    /// were stored. Idempotent on an already-committed reservation id.
    /// <param name="reservationId">The opaque reservation id a granted <see cref="T:Legate.QuotaGranted" /> returned.</param>
    /// <param name="cancellationToken">Token that abandons the commit.</param>
    /// <returns>A task that completes once the reservation is counted.</returns>
    abstract Commit: reservationId: string * cancellationToken: CancellationToken -> Task

    /// Gives a granted reservation's bytes back without counting them, for
    /// artifacts that were not stored. Idempotent on an already-released
    /// reservation id.
    /// <param name="reservationId">The opaque reservation id a granted <see cref="T:Legate.QuotaGranted" /> returned.</param>
    /// <param name="cancellationToken">Token that abandons the release.</param>
    /// <returns>A task that completes once the reservation is given back.</returns>
    abstract Release: reservationId: string * cancellationToken: CancellationToken -> Task

/// The artifact-quota default when a host registers no
/// <see cref="T:Legate.IArtifactQuota" />: every reservation is granted and
/// commit and release are no-ops. Sealed so the default cannot drift;
/// register a real quota to enforce limits.
[<Sealed>]
type UnlimitedArtifactQuota() =

    /// Grants every reservation.
    interface IArtifactQuota with
        member _.Reserve(_request: ArtifactQuotaRequest, _cancellationToken: CancellationToken) =
            Task.FromResult(ArtifactQuotaDecision.Grant "unlimited")

        member _.Commit(_reservationId: string, _cancellationToken: CancellationToken) = Task.CompletedTask

        member _.Release(_reservationId: string, _cancellationToken: CancellationToken) = Task.CompletedTask
