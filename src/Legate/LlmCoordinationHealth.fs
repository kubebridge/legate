// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.DependencyInjection.Extensions
open Microsoft.Extensions.Diagnostics.HealthChecks
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Options

// Readiness for distributed LLM admission. The startup canary proves the
// admission scripts execute (a seam-level acquire plus release on a
// reserved identity, not a socket ping) when distributed coordination is
// required, and the legate-llm-coordination health check reports the stored
// canary outcome. Anything else (local coordination, disabled mode, or a
// startup that does not require the canary) is always ready. The canary
// outcome is set once at startup and never reset; a restart re-runs it.
// Per-call seam failures never latch the check: they stay observable
// through AdmissionRejectedException and the fail-open metric.

// ──────────────────────────────────────────────────────────────────────────
// Probe shape

/// The readiness constants for the coordination canary and check. Internal:
/// hosts probe the check by name.
module internal LlmCoordinationHealth =

    /// The health-check name hosts probe.
    let checkName = "legate-llm-coordination"

    /// The reserved canary identity. It contains no slash, so it can never
    /// equal a providerId/scope host key.
    [<Literal>]
    let CanaryIdentity = "__legate_canary__"

    /// The canary lease TTL: a fixed probe constant, not admission.
    let canaryLeaseTtl = TimeSpan.FromSeconds 10.0

    /// The canary waiter TTL: a fixed probe constant, not admission.
    let canaryWaiterTtl = TimeSpan.FromSeconds 10.0

    /// The canary concurrency limit: a fixed probe constant.
    [<Literal>]
    let CanaryMaxConcurrency = 1

// ──────────────────────────────────────────────────────────────────────────
// Canary

/// The startup canary proving the distributed admission scripts execute.
/// Runs once at host start; StartAsync never throws: every failure records
/// an Unhealthy outcome instead.
type internal LlmCoordinationCanaryService(legateOptions: IOptions<LegateOptions>, provider: IServiceProvider) =

    do ArgumentNullException.ThrowIfNull(legateOptions)
    do ArgumentNullException.ThrowIfNull(provider)

    let mutable outcome: HealthCheckResult option = None

    /// The stored canary outcome, or None while the canary has not run yet.
    member _.Outcome: HealthCheckResult option = outcome

    /// Reads the coordination mode: Disabled when the options were never
    /// registered (the host never called UseRedisCoordination).
    member private _.DistOptions() : DistributedCoordinationOptions =
        match provider.GetService<IOptions<DistributedCoordinationOptions>>() with
        | null -> DistributedCoordinationOptions()
        | options -> options.Value

    interface IHostedService with
        member this.StartAsync(cancellationToken: CancellationToken) =
            task {
                try
                    let llm = legateOptions.Value.Llm
                    let distributed = not (isNull (box llm)) && llm.DistributedCoordination
                    let distOptions = this.DistOptions()

                    if
                        not distributed
                        || distOptions.Mode <> DistributedCoordinationMode.Redis
                        || not distOptions.StartupRequired
                    then
                        outcome <-
                            Some(
                                HealthCheckResult.Healthy(
                                    "Distributed coordination is not required: the local coordinator admits in process."
                                )
                            )
                    else
                        match provider.GetService<IDistributedLlmAdmission>() with
                        | null ->
                            outcome <-
                                Some(
                                    HealthCheckResult.Unhealthy(
                                        "Distributed coordination requires Redis, but no IDistributedLlmAdmission client is registered: UseRedisCoordination registers it."
                                    )
                                )
                        | seam ->
                            try
                                let owner = "startup-canary-" + Guid.NewGuid().ToString("N")

                                // Any decision proves the scripts execute:
                                // even Queued or CooldownActive answers came
                                // from Lua, tolerating a stale lease from a
                                // concurrently starting process.
                                let! _ =
                                    seam.AcquireAsync(
                                        LlmCoordinationHealth.CanaryIdentity,
                                        owner,
                                        LlmCoordinationHealth.CanaryMaxConcurrency,
                                        LlmCoordinationHealth.canaryLeaseTtl,
                                        LlmCoordinationHealth.canaryWaiterTtl,
                                        cancellationToken
                                    )

                                try
                                    let! _ =
                                        seam.ReleaseAsync(
                                            LlmCoordinationHealth.CanaryIdentity,
                                            owner,
                                            CancellationToken.None
                                        )

                                    ()
                                with _ ->
                                    ()

                                outcome <-
                                    Some(
                                        HealthCheckResult.Healthy(
                                            "The distributed admission canary acquired and released its lease."
                                        )
                                    )
                            with ex ->
                                outcome <-
                                    Some(
                                        HealthCheckResult.Unhealthy(
                                            sprintf
                                                "The distributed admission canary failed with %s: the Legate:Llm:DistributedCoordination section names the endpoint."
                                                (ex.GetType().Name)
                                        )
                                    )
                with _ ->
                    outcome <-
                        Some(
                            HealthCheckResult.Unhealthy(
                                "The distributed admission canary failed before reaching the seam."
                            )
                        )

                return ()
            }

        member _.StopAsync(_cancellationToken: CancellationToken) = Task.CompletedTask

// ──────────────────────────────────────────────────────────────────────────
// Check

/// The legate-llm-coordination readiness check. Healthy when distributed
/// coordination is off, the mode is Disabled, or startup does not require
/// the canary; otherwise the stored canary outcome (a canary that has not
/// run yet reads Unhealthy).
type internal LlmCoordinationHealthCheck(legateOptions: IOptions<LegateOptions>, provider: IServiceProvider) =

    do ArgumentNullException.ThrowIfNull(legateOptions)
    do ArgumentNullException.ThrowIfNull(provider)

    interface IHealthCheck with
        member _.CheckHealthAsync(context: HealthCheckContext, _cancellationToken: CancellationToken) =
            task {
                ArgumentNullException.ThrowIfNull(context)
                let llm = legateOptions.Value.Llm
                let distributed = not (isNull (box llm)) && llm.DistributedCoordination

                if not distributed then
                    return
                        HealthCheckResult.Healthy(
                            "Distributed coordination is off: the local coordinator admits in process."
                        )
                else
                    match provider.GetService<IOptions<DistributedCoordinationOptions>>() with
                    | null ->
                        return
                            HealthCheckResult.Unhealthy(
                                "Distributed coordination is on, but DistributedCoordinationOptions is not registered: UseRedisCoordination registers it."
                            )
                    | options when options.Value.Mode <> DistributedCoordinationMode.Redis ->
                        return
                            HealthCheckResult.Healthy(
                                "Distributed coordination mode is Disabled: the local coordinator admits in process."
                            )
                    | options when not options.Value.StartupRequired ->
                        return HealthCheckResult.Healthy("The coordination canary is not required at startup.")
                    | _ ->
                        let service =
                            provider.GetServices<IHostedService>()
                            |> Seq.tryPick (fun service ->
                                match service with
                                | :? LlmCoordinationCanaryService as typed -> Some typed
                                | _ -> None)

                        match service with
                        | None ->
                            return
                                HealthCheckResult.Unhealthy(
                                    "The coordination canary is not registered: AddLegate registers it."
                                )
                        | Some canary ->
                            match canary.Outcome with
                            | None -> return HealthCheckResult.Unhealthy("The coordination canary has not run yet.")
                            | Some result -> return result
            }

// ──────────────────────────────────────────────────────────────────────────
// Registration

/// Marker keeping the check registration idempotent across repeated
/// AddLegate calls.
type internal LlmCoordinationHealthMarker() = class end

/// Registers the coordination canary hosted service and the
/// legate-llm-coordination readiness check.
module internal LlmCoordinationHealthRegistration =

    /// Registers the canary hosted service always (it short-circuits to
    /// Healthy when distributed coordination is not required) and the
    /// check only when the host already uses health checks (a
    /// HealthCheckService registration is present), so hosts without
    /// health checks gain no new check. TryAdd semantics: repeated
    /// AddLegate calls never duplicate either registration.
    /// <param name="services">The container to add the check to.</param>
    let register (services: IServiceCollection) : unit =
        ArgumentNullException.ThrowIfNull(services)

        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, LlmCoordinationCanaryService>())
        |> ignore

        let hasHealthChecks =
            services
            |> Seq.exists (fun descriptor -> descriptor.ServiceType = typeof<HealthCheckService>)

        if hasHealthChecks then
            let alreadyRegistered =
                services
                |> Seq.exists (fun descriptor -> descriptor.ServiceType = typeof<LlmCoordinationHealthMarker>)

            if not alreadyRegistered then
                services.TryAddSingleton<LlmCoordinationHealthMarker>() |> ignore

                services.AddHealthChecks().AddCheck<LlmCoordinationHealthCheck>(LlmCoordinationHealth.checkName)
                |> ignore
