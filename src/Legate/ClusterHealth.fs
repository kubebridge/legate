// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Threading
open System.Threading.Tasks
open Akka.Cluster
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.DependencyInjection.Extensions
open Microsoft.Extensions.Diagnostics.HealthChecks
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Options

// Readiness for the clustered runtime. The legate-cluster health check
// reports whether this node may serve traffic: it fails the moment
// BeginDrain runs (before any wait starts), stays healthy in Local mode
// (no cluster runs there), and otherwise reads the live cluster view
// (SelfMember plus CurrentClusterState) for self Up, the session role on
// self, a reachable majority, and zero unreachable members. The pure
// snapshot evaluator keeps the predicate unit-testable without a running
// actor system.

// ──────────────────────────────────────────────────────────────────────────
// Predicate

/// The readiness predicate over one cluster snapshot. Internal so no Akka
/// type ever crosses the public API.
module internal ClusterHealth =

    /// The health-check name hosts probe.
    let checkName = "legate-cluster"

    /// Evaluates one readiness snapshot: draining fails first, then self
    /// Up, then the session role on self, then zero unreachable members,
    /// then a strict reachable majority of the known members.
    /// <param name="draining">Whether BeginDrain has run.</param>
    /// <param name="selfUp">Whether this member's status is Up.</param>
    /// <param name="hasSessionRole">Whether this member carries the session role.</param>
    /// <param name="reachable">The reachable member count.</param>
    /// <param name="total">The known member count.</param>
    /// <param name="unreachable">The unreachable member count.</param>
    /// <returns>The readiness result.</returns>
    let evaluateSnapshot
        (draining: bool)
        (selfUp: bool)
        (hasSessionRole: bool)
        (reachable: int)
        (total: int)
        (unreachable: int)
        : HealthCheckResult =
        if draining then
            HealthCheckResult.Unhealthy(
                "The cluster node is draining: BeginDrain failed readiness before the running-turn wait."
            )
        elif not selfUp then
            HealthCheckResult.Unhealthy("This cluster member is not Up.")
        elif not hasSessionRole then
            HealthCheckResult.Unhealthy("This cluster member does not carry the session role.")
        elif unreachable > 0 then
            HealthCheckResult.Unhealthy($"The cluster reports %d{unreachable} unreachable members.")
        elif reachable * 2 <= total then
            HealthCheckResult.Unhealthy($"The reachable set (%d{reachable} of %d{total}) lost majority.")
        else
            HealthCheckResult.Healthy(
                "The cluster member is Up with the session role, a reachable majority, and no unreachable members."
            )

// ──────────────────────────────────────────────────────────────────────────
// Check

/// The legate-cluster readiness check. Local mode is always ready; the
/// cluster modes read the live ReadView through the hosted service.
type internal ClusterHealthCheck(options: IOptions<LegateOptions>, provider: IServiceProvider) =

    do ArgumentNullException.ThrowIfNull(options)
    do ArgumentNullException.ThrowIfNull(provider)

    interface IHealthCheck with
        member _.CheckHealthAsync(context: HealthCheckContext, _cancellationToken: CancellationToken) =
            task {
                ArgumentNullException.ThrowIfNull(context)
                let clusterOptions = options.Value.Cluster

                match clusterOptions.Mode with
                | ClusterMode.Local -> return HealthCheckResult.Healthy("Local mode runs no cluster: always ready.")
                | ClusterMode.StaticSeeds
                | ClusterMode.Kubernetes as mode ->
                    let service =
                        provider.GetServices<IHostedService>()
                        |> Seq.tryPick (fun service ->
                            match service with
                            | :? ClusterActorSystemService as typed -> Some typed
                            | _ -> None)

                    match service with
                    | None ->
                        return
                            HealthCheckResult.Unhealthy(
                                "The cluster actor system is not registered: AddLegate registers it."
                            )
                    | Some clustered ->
                        match box clustered.System with
                        | null ->
                            return
                                HealthCheckResult.Unhealthy(
                                    $"The cluster actor system is not running in %O{mode} mode."
                                )
                        | :? Akka.Actor.ActorSystem as system ->
                            let cluster: Akka.Cluster.Cluster = Cluster.Get(system)
                            let self: Akka.Cluster.Member = cluster.SelfMember
                            let state: Akka.Cluster.ClusterEvent.CurrentClusterState = cluster.State

                            if isNull (box self) then
                                return HealthCheckResult.Unhealthy("The cluster reports no self member.")
                            elif isNull (box state) then
                                return HealthCheckResult.Unhealthy("The cluster reports no current state.")
                            else
                                let selfUp = self.Status = MemberStatus.Up
                                let hasRole = self.Roles.Contains(clusterOptions.SessionRole)
                                let total = state.Members.Count
                                let unreachable = state.Unreachable.Count
                                let reachable = total - unreachable

                                return
                                    ClusterHealth.evaluateSnapshot
                                        clustered.IsDraining
                                        selfUp
                                        hasRole
                                        reachable
                                        total
                                        unreachable
                        | _ -> return HealthCheckResult.Unhealthy("The cluster actor system is not running.")
                | _ ->
                    return
                        HealthCheckResult.Unhealthy(
                            $"Unknown Legate cluster mode '%O{clusterOptions.Mode}'. Expected one of: Local, StaticSeeds, Kubernetes."
                        )
            }

// ──────────────────────────────────────────────────────────────────────────
// Registration

/// Marker keeping the check registration idempotent across repeated
/// AddLegate calls.
type internal ClusterHealthMarker() = class end

/// Registers the legate-cluster readiness check.
module internal ClusterHealthRegistration =

    /// Registers ClusterHealthCheck under the legate-cluster name, but only
    /// when the host already uses health checks (a HealthCheckService
    /// registration is present), so hosts without health checks gain no new
    /// service. TryAdd semantics: the first registration wins, so repeated
    /// AddLegate calls never duplicate the check and a host check
    /// registered under its own name is untouched.
    /// <param name="services">The container to add the check to.</param>
    let register (services: IServiceCollection) : unit =
        ArgumentNullException.ThrowIfNull(services)

        let hasHealthChecks =
            services
            |> Seq.exists (fun descriptor -> descriptor.ServiceType = typeof<HealthCheckService>)

        if hasHealthChecks then
            let alreadyRegistered =
                services
                |> Seq.exists (fun descriptor -> descriptor.ServiceType = typeof<ClusterHealthMarker>)

            if not alreadyRegistered then
                services.TryAddSingleton<ClusterHealthMarker>() |> ignore

                services.AddHealthChecks().AddCheck<ClusterHealthCheck>(ClusterHealth.checkName)
                |> ignore
