// SPDX-License-Identifier: Apache-2.0
namespace Legate.Coordination.Redis

open System
open System.Threading
open System.Threading.Tasks

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
    /// <see cref="P:Legate.Coordination.Redis.DistributedAdmissionOutcome.Kind" />
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
