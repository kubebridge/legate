// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Threading
open System.Threading.Tasks

// Distributed LLM admission seam. The options contract and the admission
// interface live here (matching the ISessionStore/IToolSource precedent)
// so the Legate runtime admits through the seam without referencing the
// Redis package: Legate.Coordination.Redis implements this interface on
// StackExchange.Redis. Settings bind from the
// Legate:Llm:DistributedCoordination configuration section. Raw API keys
// never enter identity keys, logs, or diagnostics.

// ────────────────── Options ──────────────────────────────────────────

/// <summary>
/// How distributed LLM admission coordinates: local-only or through one
/// shared Redis instance. Bound from configuration; unknown values fail
/// binding.
/// </summary>
type DistributedCoordinationMode =

    /// <summary>
    /// No distributed coordination: the local coordinator admits in
    /// process. The default.
    /// </summary>
    | Disabled = 0

    /// <summary>
    /// Admit through one shared Redis instance: processes sharing the
    /// instance respect one combined concurrency limit.
    /// </summary>
    | Redis = 1

/// <summary>
/// Where distributed LLM admission lives and how it fails: the mode, the
/// Redis connection, the key prefix isolating one deployment's keys, and
/// the startup and failure switches. Bound from the
/// <c>Legate:Llm:DistributedCoordination</c> configuration section.
/// </summary>
type DistributedCoordinationOptions() =

    /// <summary>
    /// The configuration section the client binds from:
    /// <c>Legate:Llm:DistributedCoordination</c>.
    /// </summary>
    static member ConfigurationSectionPath = "Legate:Llm:DistributedCoordination"

    /// <summary>
    /// How admission coordinates. Defaults to
    /// <see cref="F:Legate.DistributedCoordinationMode.Disabled" />:
    /// a single node coordinates locally.
    /// </summary>
    member val Mode: DistributedCoordinationMode = DistributedCoordinationMode.Disabled with get, set

    /// <summary>
    /// The Redis connection string, for example
    /// <c>127.0.0.1:6379</c>. Required when
    /// <see cref="P:Legate.DistributedCoordinationOptions.Mode" />
    /// is Redis; ignored otherwise. Never logged.
    /// </summary>
    member val ConnectionString: string = "" with get, set

    /// <summary>
    /// The key prefix isolating one deployment's admission keys, for
    /// example <c>legate:llm</c>. Every script builds its keys as
    /// <c>{prefix}:v1:...</c> so a rolling upgrade never mixes script
    /// generations under one prefix. Must be non-empty.
    /// </summary>
    member val KeyPrefix: string = "legate:llm" with get, set

    /// <summary>
    /// Whether startup must prove Redis before reporting ready. Defaults
    /// to true: the startup canary gates readiness. Kept here so the
    /// canary and the client read one set of options.
    /// </summary>
    member val StartupRequired: bool = true with get, set

    /// <summary>
    /// Whether admission fails closed when Redis is unavailable. Defaults
    /// to true: new work is rejected. When false the coordinator admits
    /// locally under bounded emergency limits. Kept here so the fail
    /// policy and the client read one set of options.
    /// </summary>
    member val FailClosed: bool = true with get, set

    /// <summary>
    /// Checks the options: the mode must be defined, the prefix
    /// non-empty without whitespace or wildcards, and the connection
    /// string non-empty when the mode is Redis.
    /// </summary>
    /// <returns>Null when the options are valid; otherwise the reason they are not.</returns>
    member this.Validate() : string | null =
        if
            this.Mode <> DistributedCoordinationMode.Disabled
            && this.Mode <> DistributedCoordinationMode.Redis
        then
            "DistributedCoordinationOptions.Mode must be Disabled or Redis."
        elif String.IsNullOrWhiteSpace this.KeyPrefix then
            "DistributedCoordinationOptions.KeyPrefix must be a non-empty key prefix."
        elif
            this.KeyPrefix.Contains(" ")
            || this.KeyPrefix.Contains("*")
            || this.KeyPrefix.Contains(":v1:")
        then
            "DistributedCoordinationOptions.KeyPrefix must not contain spaces, wildcards, or the :v1: generation tag."
        elif isNull (box this.ConnectionString) then
            "DistributedCoordinationOptions.ConnectionString must not be null: use the empty string when disabled."
        elif
            this.Mode = DistributedCoordinationMode.Redis
            && String.IsNullOrWhiteSpace this.ConnectionString
        then
            "DistributedCoordinationOptions.ConnectionString must be non-empty when Mode is Redis."
        else
            null

// ────────────────── Admission seam ───────────────────────────────────

// Public contract for distributed LLM admission. The outcome is a sealed
// class with an enum kind (no DU on the boundary), and every client method
// returns Task or Task of bool. Owner ids fence every mutation: a lease
// held by one owner cannot be renewed, released, or completed by another.

/// <summary>
/// What one distributed admission attempt decided.
/// </summary>
type DistributedAdmissionDecision =

    /// <summary>
    /// The caller holds a lease: it may run.
    /// </summary>
    | Acquired = 0

    /// <summary>
    /// The caller waits in FIFO order: it holds no lease yet.
    /// </summary>
    | Queued = 1

    /// <summary>
    /// The identity cools down after an HTTP 429: no caller may run.
    /// </summary>
    | CooldownActive = 2

/// <summary>
/// The outcome of one distributed admission attempt: the decision, the
/// FIFO queue position (zero unless queued), and the lease expiry.
/// </summary>
[<Sealed>]
type DistributedAdmissionOutcome private (kind: DistributedAdmissionDecision, queuePosition: int) =

    /// <summary>
    /// What the attempt decided.
    /// </summary>
    member _.Kind: DistributedAdmissionDecision = kind

    /// <summary>
    /// The zero-based FIFO position while
    /// <see cref="P:Legate.DistributedAdmissionOutcome.Kind" />
    /// is Queued; otherwise zero.
    /// </summary>
    member _.QueuePosition: int = queuePosition

    /// <summary>
    /// Creates an acquired outcome.
    /// </summary>
    /// <returns>An acquired outcome.</returns>
    static member Acquired() : DistributedAdmissionOutcome =
        DistributedAdmissionOutcome(DistributedAdmissionDecision.Acquired, 0)

    /// <summary>
    /// Creates a queued outcome at the given FIFO position.
    /// </summary>
    /// <param name="queuePosition">The zero-based FIFO position. Must not be negative.</param>
    /// <returns>A queued outcome.</returns>
    static member Queued(queuePosition: int) : DistributedAdmissionOutcome =
        ArgumentOutOfRangeException.ThrowIfNegative(queuePosition)
        DistributedAdmissionOutcome(DistributedAdmissionDecision.Queued, queuePosition)

    /// <summary>
    /// Creates a cooldown outcome.
    /// </summary>
    /// <returns>A cooldown outcome.</returns>
    static member CooldownActive() : DistributedAdmissionOutcome =
        DistributedAdmissionOutcome(DistributedAdmissionDecision.CooldownActive, 0)

/// <summary>
/// Distributed LLM admission over one shared Redis instance: atomic Lua
/// scripts admit, renew, release, complete, and cool down per coordination
/// identity, so processes sharing the instance respect one combined
/// concurrency limit with FIFO waiters, waiter TTL, and owner fencing.
/// </summary>
type IDistributedLlmAdmission =

    /// <summary>
    /// Admits one owner under the identity's concurrency limit or queues
    /// it in FIFO order. Re-admits a current holder without queueing and
    /// refuses while the identity cools down.
    /// </summary>
    /// <param name="identity">The coordination identity, for example the provider and scope. Must be non-empty.</param>
    /// <param name="ownerId">The owner holding the lease on success. Must be non-empty.</param>
    /// <param name="maxConcurrency">The combined concurrency limit. Must be at least 1.</param>
    /// <param name="leaseTtl">How long the lease lives without renewal. Must be positive.</param>
    /// <param name="waiterTtl">How long a queued waiter blocks the queue. Must be positive.</param>
    /// <param name="cancellationToken">Token that abandons the attempt.</param>
    /// <returns>The admission outcome.</returns>
    abstract AcquireAsync:
        identity: string *
        ownerId: string *
        maxConcurrency: int *
        leaseTtl: TimeSpan *
        waiterTtl: TimeSpan *
        cancellationToken: CancellationToken ->
            Task<DistributedAdmissionOutcome>

    /// <summary>
    /// Renews one owner's lease. Only the holding owner renews: a foreign
    /// owner reports false.
    /// </summary>
    /// <param name="identity">The coordination identity. Must be non-empty.</param>
    /// <param name="ownerId">The owner holding the lease. Must be non-empty.</param>
    /// <param name="leaseTtl">The lease extension. Must be positive.</param>
    /// <param name="cancellationToken">Token that abandons the renewal.</param>
    /// <returns>True when renewed; false when fenced or missing.</returns>
    abstract RenewAsync:
        identity: string * ownerId: string * leaseTtl: TimeSpan * cancellationToken: CancellationToken -> Task<bool>

    /// <summary>
    /// Releases one owner's lease and dequeues its waiter entry. Only the
    /// holding owner releases its lease: a foreign owner reports false
    /// unless it holds its own queue entry.
    /// </summary>
    /// <param name="identity">The coordination identity. Must be non-empty.</param>
    /// <param name="ownerId">The owner releasing. Must be non-empty.</param>
    /// <param name="cancellationToken">Token that abandons the release.</param>
    /// <returns>True when a lease or queue entry was removed; otherwise false.</returns>
    abstract ReleaseAsync: identity: string * ownerId: string * cancellationToken: CancellationToken -> Task<bool>

    /// <summary>
    /// Completes one owner's lease: same fencing as release, kept separate
    /// so usage settlement can ride along later.
    /// </summary>
    /// <param name="identity">The coordination identity. Must be non-empty.</param>
    /// <param name="ownerId">The owner completing. Must be non-empty.</param>
    /// <param name="cancellationToken">Token that abandons the completion.</param>
    /// <returns>True when a lease or queue entry was removed; otherwise false.</returns>
    abstract CompleteAsync: identity: string * ownerId: string * cancellationToken: CancellationToken -> Task<bool>

    /// <summary>
    /// Starts a cooldown refusing new work for the identity until the TTL
    /// elapses.
    /// </summary>
    /// <param name="identity">The coordination identity. Must be non-empty.</param>
    /// <param name="cooldown">How long new work is refused. Must be positive.</param>
    /// <param name="cancellationToken">Token that abandons the cooldown.</param>
    /// <returns>A task completing when the cooldown is set.</returns>
    abstract StartCooldownAsync: identity: string * cooldown: TimeSpan * cancellationToken: CancellationToken -> Task
