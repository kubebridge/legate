// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.Packages

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open Legate

// The shared renewing agent-package lease service (issue 107): an
// in-process token table on the TimeProvider clock, with WithLease running
// a renew loop on the ILlmDelay seam mirroring ClaimHeartbeat.runAsync.
// File-system and S3 hosts compose this beside their package stores; the
// stores stay lease-agnostic and this service stays storage-agnostic.

/// The system-backed delay seam this package falls back to when the
/// container holds no <see cref="T:Legate.ILlmDelay" />: waits on the
/// service's clock, so virtual-clock hosts that register their clock but
/// no delay seam still get virtual-time waits. Internal: hosts program
/// against <see cref="T:Legate.IAgentPackageLeaseService" />.
type internal PackageLeaseSystemDelay(clock: TimeProvider) =

    do ArgumentNullException.ThrowIfNull(clock)

    interface ILlmDelay with
        member _.Delay(delay, cancellationToken) =
            Task.Delay(delay, clock, cancellationToken)

/// The single shared <see cref="T:Legate.IAgentPackageLeaseService" />
/// reference implementation: an in-process token table on the injected
/// <see cref="T:System.TimeProvider" /> clock. Acquire grants when no live
/// lease covers the agent's package; renew mints a fresh token and expiry
/// only for the current token (the last-moment fence: a stale token renews
/// nothing and changes nothing); verify reports the current token while it
/// has not expired; release frees only the current token and is otherwise
/// a no-op. <see cref="M:Legate.IAgentPackageLeaseService.WithLease*" />
/// acquires first, renews every
/// <see cref="P:Legate.PackageLeaseOptions.RenewInterval" /> while the work
/// runs, cancels the work token when a renewal is lost or exceeds
/// <see cref="P:Legate.PackageLeaseOptions.RenewalTimeout" />, and releases
/// best-effort in a finally block, also on failure paths.
[<Sealed>]
type AgentPackageLeaseService(clock: TimeProvider, delay: ILlmDelay) =

    do
        ArgumentNullException.ThrowIfNull(clock)
        ArgumentNullException.ThrowIfNull(delay)

    let gate = obj ()

    let held = Dictionary<string * string, string * DateTimeOffset * string>()

    let key (tenant: TenantId) (agentId: AgentId) = (tenant.Value, agentId.Value)

    let checkDuration (name: string) (leaseDuration: TimeSpan) =
        if leaseDuration <= TimeSpan.Zero then
            raise (ArgumentOutOfRangeException(name, "The lease duration must be positive."))

    /// Initialises the service over the machine clock with system waits.
    new() = AgentPackageLeaseService(TimeProvider.System, PackageLeaseSystemDelay(TimeProvider.System))

    /// Runs work under an acquired lease with the pinned WithLease
    /// semantics both overloads share. Private: the public surface is the
    /// <see cref="T:Legate.IAgentPackageLeaseService" /> contract.
    member private this.WithLeaseCore<'T>
        (
            tenant: TenantId,
            agentId: AgentId,
            owner: string,
            leaseDuration: TimeSpan,
            options: PackageLeaseOptions,
            startWork: CancellationToken -> Task<'T>,
            cancellationToken: CancellationToken
        ) : Task<'T> =
        if isNull (box owner) then
            raise (ArgumentNullException(nameof owner))

        if isNull (box startWork) then
            raise (ArgumentNullException(nameof startWork))

        checkDuration (nameof leaseDuration) leaseDuration

        let resolved =
            if isNull (box options) then
                PackageLeaseOptions()
            else
                options

        if resolved.RenewInterval <= TimeSpan.Zero then
            raise (ArgumentException("PackageLeaseOptions.RenewInterval must be positive.", nameof options))

        if resolved.RenewalTimeout <= TimeSpan.Zero then
            raise (ArgumentException("PackageLeaseOptions.RenewalTimeout must be positive.", nameof options))

        task {
            let! first =
                (this :> IAgentPackageLeaseService).Acquire(tenant, agentId, owner, leaseDuration, cancellationToken)

            match first with
            | :? PackageLeaseAcquired as acquired ->
                let mutable current = acquired.Lease

                try
                    let! outcome =
                        PackageLeaseLoop.runAsync
                            (fun lease duration token ->
                                (this :> IAgentPackageLeaseService).Renew(lease, duration, token))
                            acquired.Lease
                            leaseDuration
                            resolved.RenewInterval
                            resolved.RenewalTimeout
                            clock
                            delay
                            startWork
                            cancellationToken

                    match outcome with
                    | PackageLeaseLoop.Completed(result, final) ->
                        current <- final
                        return result
                    | PackageLeaseLoop.Failed(final, reason) ->
                        current <- final

                        return
                            raise (
                                PackageLeaseException(
                                    agentId,
                                    owner,
                                    "renew",
                                    $"The package lease renewal {reason}: the leased work was cancelled and nothing it attempted afterwards took effect."
                                )
                            )
                finally
                    try
                        let! _ = (this :> IAgentPackageLeaseService).Release(current, CancellationToken.None)
                        ()
                    with _ ->
                        ()
            | _ ->
                return
                    raise (
                        PackageLeaseException(
                            agentId,
                            owner,
                            "acquire",
                            "The package lease could not be acquired: another writer holds the agent's package lease."
                        )
                    )
        }

    interface IAgentPackageLeaseService with

        /// Acquires a lease over the agent's package, granting when no
        /// live lease covers it and reporting the held branch otherwise.
        /// <param name="tenant">The tenant whose package to lease.</param>
        /// <param name="agentId">The agent whose package to lease.</param>
        /// <param name="owner">The caller's owner identity. Must not be null.</param>
        /// <param name="leaseDuration">How long the lease lasts without renewal. Must be positive.</param>
        /// <param name="cancellationToken">Token that abandons the acquire.</param>
        /// <returns>The granted lease, or the unavailable outcome with the reason.</returns>
        member _.Acquire(tenant, agentId, owner, leaseDuration, _) =
            if isNull (box owner) then
                raise (ArgumentNullException(nameof owner))

            checkDuration (nameof leaseDuration) leaseDuration

            let now = clock.GetUtcNow()

            lock gate (fun () ->
                match held.TryGetValue(key tenant agentId) with
                | true, (_, expiresAt, _) when expiresAt > now ->
                    PackageLeaseUnavailable(tenant, agentId, "leaseHeld") :> PackageLeaseState
                | _ ->
                    let token = Guid.NewGuid().ToString("N")
                    let expiresAt = now.Add leaseDuration
                    held[key tenant agentId] <- (token, expiresAt, owner)

                    PackageLeaseAcquired(AgentPackageLease(tenant, agentId, owner, token, expiresAt))
                    :> PackageLeaseState)
            |> Task.FromResult

        /// Renews a held lease, fenced on the opaque token at the last
        /// moment: only the current token renews, minting a fresh token
        /// and expiry; every other token reports the lost branch and
        /// changes nothing.
        /// <param name="lease">The lease to renew.</param>
        /// <param name="leaseDuration">The renewed duration from the renewal instant. Must be positive.</param>
        /// <param name="cancellationToken">Token that abandons the renewal.</param>
        /// <returns>The renewed lease with a fresh token and expiry, or the lost outcome with the reason.</returns>
        member _.Renew(lease, leaseDuration, _) =
            if isNull (box lease) then
                raise (ArgumentNullException(nameof lease))

            checkDuration (nameof leaseDuration) leaseDuration

            let now = clock.GetUtcNow()

            lock gate (fun () ->
                match held.TryGetValue(key lease.Tenant lease.AgentId) with
                | true, (token, expiresAt, owner) when String.Equals(token, lease.Token, StringComparison.Ordinal) ->
                    if expiresAt <= now then
                        held.Remove(key lease.Tenant lease.AgentId) |> ignore
                        PackageLeaseLost("leaseExpired") :> PackageLeaseRenewal
                    else
                        let fresh = Guid.NewGuid().ToString("N")
                        let renewedAt = now.Add leaseDuration
                        held[key lease.Tenant lease.AgentId] <- (fresh, renewedAt, owner)

                        PackageLeaseRenewed(
                            AgentPackageLease(lease.Tenant, lease.AgentId, lease.Owner, fresh, renewedAt)
                        )
                        :> PackageLeaseRenewal
                | _ -> PackageLeaseLost("staleToken") :> PackageLeaseRenewal)
            |> Task.FromResult

        /// Reports whether the lease is still held by its token and has
        /// not expired on the service's clock.
        /// <param name="lease">The lease to verify.</param>
        /// <param name="cancellationToken">Token that abandons the check.</param>
        /// <returns>true when the token still holds the lease; otherwise false.</returns>
        member _.Verify(lease, _) =
            if isNull (box lease) then
                raise (ArgumentNullException(nameof lease))

            let now = clock.GetUtcNow()

            lock gate (fun () ->
                match held.TryGetValue(key lease.Tenant lease.AgentId) with
                | true, (token, expiresAt, _) ->
                    String.Equals(token, lease.Token, StringComparison.Ordinal) && expiresAt > now
                | _ -> false)
            |> Task.FromResult

        /// Releases the lease when the token still holds it; a foreign or
        /// expired token is an expected no-op that changes nothing.
        /// <param name="lease">The lease to release.</param>
        /// <param name="cancellationToken">Token that abandons the release.</param>
        /// <returns>true when this call released the lease; false when it was already released or expired.</returns>
        member _.Release(lease, _) =
            if isNull (box lease) then
                raise (ArgumentNullException(nameof lease))

            lock gate (fun () ->
                match held.TryGetValue(key lease.Tenant lease.AgentId) with
                | true, (token, _, _) when String.Equals(token, lease.Token, StringComparison.Ordinal) ->
                    held.Remove(key lease.Tenant lease.AgentId) |> ignore
                    true
                | _ -> false)
            |> Task.FromResult

        /// Runs work while holding the lease with the pinned semantics:
        /// acquire first, renew on the interval, cancel on lost or
        /// over-timeout renewal, release best-effort in a finally block.
        /// <param name="tenant">The tenant whose package to lease.</param>
        /// <param name="agentId">The agent whose package to lease.</param>
        /// <param name="owner">The caller's owner identity. Must not be null.</param>
        /// <param name="leaseDuration">How long the lease lasts without renewal. Must be positive.</param>
        /// <param name="options">The renewal tuning, or the defaults when null.</param>
        /// <param name="work">The work to run while the lease is held. Must not be null.</param>
        /// <param name="cancellationToken">Token that abandons the lease lifecycle.</param>
        /// <returns>The work's result.</returns>
        member this.WithLease
            (
                tenant: TenantId,
                agentId: AgentId,
                owner: string,
                leaseDuration: TimeSpan,
                options: PackageLeaseOptions,
                work: Func<CancellationToken, Task<'T>>,
                cancellationToken: CancellationToken
            ) : Task<'T> =
            if isNull (box work) then
                raise (ArgumentNullException(nameof work))

            this.WithLeaseCore(
                tenant,
                agentId,
                owner,
                leaseDuration,
                options,
                (fun token -> work.Invoke(token)),
                cancellationToken
            )

        /// The non-generic form of WithLease for work that returns no value.
        /// <param name="tenant">The tenant whose package to lease.</param>
        /// <param name="agentId">The agent whose package to lease.</param>
        /// <param name="owner">The caller's owner identity. Must not be null.</param>
        /// <param name="leaseDuration">How long the lease lasts without renewal. Must be positive.</param>
        /// <param name="options">The renewal tuning, or the defaults when null.</param>
        /// <param name="work">The work to run while the lease is held. Must not be null.</param>
        /// <param name="cancellationToken">Token that abandons the lease lifecycle.</param>
        /// <returns>A task completing when the work and release both finish.</returns>
        member this.WithLease
            (
                tenant: TenantId,
                agentId: AgentId,
                owner: string,
                leaseDuration: TimeSpan,
                options: PackageLeaseOptions,
                work: Func<CancellationToken, Task>,
                cancellationToken: CancellationToken
            ) : Task =
            if isNull (box work) then
                raise (ArgumentNullException(nameof work))

            task {
                do!
                    this.WithLeaseCore(
                        tenant,
                        agentId,
                        owner,
                        leaseDuration,
                        options,
                        (fun token ->
                            task {
                                do! work.Invoke(token)
                                return ()
                            }),
                        cancellationToken
                    )
            }
            :> Task
