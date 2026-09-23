// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.LlmCoordinationHealthTests

open System
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Diagnostics.HealthChecks
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Options
open Xunit

// Startup canary and readiness (issue 136): the seam-level canary gates
// the legate-llm-coordination check when distributed coordination is
// required; anything else stays Healthy. All seam doubles are local fakes:
// no Redis here (Redis coverage stays in the coordination test suite).

/// One scripted canary acquire: an outcome proving the scripts execute, or
/// a failure proving Redis is down.
type private CanaryAcquire =
    | CanaryOutcome of DistributedAdmissionOutcome
    | CanaryThrow of exn

/// A scripted IDistributedLlmAdmission for the canary: dequeues scripted
/// acquires and records every acquire and release.
type private ScriptedCanaryAdmission(script: ResizeArray<CanaryAcquire>) =
    let gate = obj ()
    let acquireCalls = ResizeArray<string * string * int * TimeSpan * TimeSpan>()
    let releases = ResizeArray<string * string>()

    /// The acquire calls seen, oldest first.
    member _.AcquireCalls = lock gate (fun () -> acquireCalls |> List.ofSeq)

    /// The releases seen, oldest first.
    member _.Releases = lock gate (fun () -> releases |> List.ofSeq)

    interface IDistributedLlmAdmission with
        member _.AcquireAsync(identity, ownerId, maxConcurrency, leaseTtl, waiterTtl, _) =
            lock gate (fun () -> acquireCalls.Add((identity, ownerId, maxConcurrency, leaseTtl, waiterTtl)))

            let step =
                lock gate (fun () ->
                    if script.Count = 0 then
                        failwith "The canary script ran out of scripted acquires."

                    let head = script[0]
                    script.RemoveAt(0)
                    head)

            match step with
            | CanaryOutcome outcome -> Task.FromResult(outcome)
            | CanaryThrow failure -> Task.FromException<DistributedAdmissionOutcome>(failure)

        member _.RenewAsync(_, _, _, _) = Task.FromResult(false)

        member _.ReleaseAsync(identity, ownerId, _) =
            lock gate (fun () -> releases.Add((identity, ownerId)))
            Task.FromResult(true)

        member _.CompleteAsync(_, _, _) = Task.FromResult(false)

        member _.StartCooldownAsync(_, _, _) = Task.CompletedTask

/// An admission whose release always throws: the canary's best-effort
/// release must still pass.
type private ThrowingReleaseAdmission() =
    interface IDistributedLlmAdmission with
        member _.AcquireAsync(_, _, _, _, _, _) =
            Task.FromResult(DistributedAdmissionOutcome.Acquired())

        member _.RenewAsync(_, _, _, _) = Task.FromResult(false)

        member _.ReleaseAsync(_, _, _) =
            Task.FromException<bool>(InvalidOperationException("release down"))

        member _.CompleteAsync(_, _, _) = Task.FromResult(false)
        member _.StartCooldownAsync(_, _, _) = Task.CompletedTask

/// Legate options with the distributed flag set as given.
let private legateOptions (distributed: bool) : LegateOptions =
    let options = LegateOptions()
    options.Llm.DistributedCoordination <- distributed
    options

/// Coordination options with the mode, startup, and fail switches set.
let private coordinationOptions
    (mode: DistributedCoordinationMode)
    (startupRequired: bool)
    (failClosed: bool)
    : DistributedCoordinationOptions =
    let dist = DistributedCoordinationOptions()
    dist.Mode <- mode
    dist.ConnectionString <- "127.0.0.1:6379"
    dist.StartupRequired <- startupRequired
    dist.FailClosed <- failClosed
    dist

/// Builds a provider resolving the coordination options, the admission
/// client, and the canary hosted service exactly when each is supplied.
let private buildProvider
    (dist: DistributedCoordinationOptions | null)
    (admission: IDistributedLlmAdmission | null)
    (canary: LlmCoordinationCanaryService | null)
    : ServiceProvider =
    let services = ServiceCollection() :> IServiceCollection

    match box dist with
    | null -> ()
    | :? DistributedCoordinationOptions as live ->
        services.AddSingleton<IOptions<DistributedCoordinationOptions>>(
            OptionsWrapper<DistributedCoordinationOptions>(live) :> IOptions<DistributedCoordinationOptions>
        )
        |> ignore
    | _ -> ()

    match box admission with
    | null -> ()
    | :? IDistributedLlmAdmission as live -> services.AddSingleton<IDistributedLlmAdmission>(live) |> ignore
    | _ -> ()

    match box canary with
    | null -> ()
    | :? LlmCoordinationCanaryService as live -> services.AddSingleton<IHostedService>(live :> IHostedService) |> ignore
    | _ -> ()

    services.BuildServiceProvider()

/// Builds the canary over the options and the provider.
let private makeCanary (options: LegateOptions) (provider: IServiceProvider) : LlmCoordinationCanaryService =
    LlmCoordinationCanaryService(OptionsWrapper<LegateOptions>(options) :> IOptions<LegateOptions>, provider)

/// Runs the check over the options and the provider.
let private checkHealth (options: LegateOptions) (provider: IServiceProvider) : Task<HealthCheckResult> =
    let check =
        LlmCoordinationHealthCheck(OptionsWrapper<LegateOptions>(options) :> IOptions<LegateOptions>, provider)
        :> IHealthCheck

    check.CheckHealthAsync(HealthCheckContext(), CancellationToken.None)

/// Whether the result description names the fragment.
let private describes (fragment: string) (result: HealthCheckResult) : bool =
    match box result.Description with
    | null -> false
    | :? string as description -> description.Contains(fragment)
    | _ -> false

[<Fact>]
let ``Canary passes on Acquired with the reserved probe identity`` () : Task =
    task {
        let options = legateOptions true
        let dist = coordinationOptions DistributedCoordinationMode.Redis true true

        let admission =
            ScriptedCanaryAdmission(
                ResizeArray(
                    [
                        CanaryOutcome(DistributedAdmissionOutcome.Acquired())
                    ]
                )
            )

        let unstarted = buildProvider dist (admission :> IDistributedLlmAdmission) null
        let canary = makeCanary options unstarted
        use provider = buildProvider dist (admission :> IDistributedLlmAdmission) canary

        do! (canary :> IHostedService).StartAsync(CancellationToken.None)

        match canary.Outcome with
        | None -> failwith "expected the canary to record an outcome"
        | Some outcome -> outcome.Status |> should equal HealthStatus.Healthy

        let calls = admission.AcquireCalls
        calls.Length |> should equal 1
        let identity, owner, cap, leaseTtl, waiterTtl = calls[0]
        identity |> should equal LlmCoordinationHealth.CanaryIdentity
        identity.Contains("/") |> should equal false
        owner.StartsWith("startup-canary-") |> should equal true
        cap |> should equal 1
        leaseTtl |> should equal (TimeSpan.FromSeconds 10.0)
        waiterTtl |> should equal (TimeSpan.FromSeconds 10.0)
        admission.Releases |> should equal [ (identity, owner) ]

        let! result = checkHealth options provider
        result.Status |> should equal HealthStatus.Healthy
    }

[<Fact>]
let ``Canary passes on a Queued outcome`` () : Task =
    task {
        let options = legateOptions true
        let dist = coordinationOptions DistributedCoordinationMode.Redis true true

        let admission =
            ScriptedCanaryAdmission(
                ResizeArray(
                    [
                        CanaryOutcome(DistributedAdmissionOutcome.Queued(2))
                    ]
                )
            )

        use provider = buildProvider dist (admission :> IDistributedLlmAdmission) null
        let canary = makeCanary options provider

        do! (canary :> IHostedService).StartAsync(CancellationToken.None)

        // Any decision proves the scripts execute: even a queued answer
        // came from Lua.
        match canary.Outcome with
        | None -> failwith "expected the canary to record an outcome"
        | Some outcome -> outcome.Status |> should equal HealthStatus.Healthy
    }

[<Fact>]
let ``Canary failure records Unhealthy and the check reflects it`` () : Task =
    task {
        let options = legateOptions true
        let dist = coordinationOptions DistributedCoordinationMode.Redis true true

        let admission =
            ScriptedCanaryAdmission(
                ResizeArray(
                    [
                        CanaryThrow(InvalidOperationException("redis down"))
                    ]
                )
            )

        // The canary hosted service stands in for the DI registration so
        // the check resolves it like ClusterHealthCheck resolves the
        // cluster service.
        let unstarted = buildProvider dist (admission :> IDistributedLlmAdmission) null
        let canary = makeCanary options unstarted
        use provider = buildProvider dist (admission :> IDistributedLlmAdmission) canary

        // StartAsync never throws: the failure records instead.
        do! (canary :> IHostedService).StartAsync(CancellationToken.None)

        match canary.Outcome with
        | None -> failwith "expected the canary to record an outcome"
        | Some outcome -> outcome.Status |> should equal HealthStatus.Unhealthy

        let! result = checkHealth options provider
        result.Status |> should equal HealthStatus.Unhealthy
    }

[<Fact>]
let ``Canary release failure still passes`` () : Task =
    task {
        let options = legateOptions true
        let dist = coordinationOptions DistributedCoordinationMode.Redis true true

        use provider =
            buildProvider dist (ThrowingReleaseAdmission() :> IDistributedLlmAdmission) null

        let canary = makeCanary options provider

        do! (canary :> IHostedService).StartAsync(CancellationToken.None)

        match canary.Outcome with
        | None -> failwith "expected the canary to record an outcome"
        | Some outcome -> outcome.Status |> should equal HealthStatus.Healthy
    }

[<Fact>]
let ``Disabled mode stays Healthy without a seam`` () : Task =
    task {
        let options = legateOptions true
        let dist = coordinationOptions DistributedCoordinationMode.Disabled true true
        use provider = buildProvider dist null null
        let canary = makeCanary options provider

        do! (canary :> IHostedService).StartAsync(CancellationToken.None)

        match canary.Outcome with
        | None -> failwith "expected the canary to record an outcome"
        | Some outcome -> outcome.Status |> should equal HealthStatus.Healthy

        let! result = checkHealth options provider
        result.Status |> should equal HealthStatus.Healthy
    }

[<Fact>]
let ``Flag off stays Healthy without touching the seam`` () : Task =
    task {
        let options = legateOptions false
        let dist = coordinationOptions DistributedCoordinationMode.Redis true true

        let admission =
            ScriptedCanaryAdmission(
                ResizeArray(
                    [
                        CanaryThrow(InvalidOperationException("redis down"))
                    ]
                )
            )

        use provider = buildProvider dist (admission :> IDistributedLlmAdmission) null
        let canary = makeCanary options provider

        do! (canary :> IHostedService).StartAsync(CancellationToken.None)

        match canary.Outcome with
        | None -> failwith "expected the canary to record an outcome"
        | Some outcome -> outcome.Status |> should equal HealthStatus.Healthy

        admission.AcquireCalls |> should be Empty

        let! result = checkHealth options provider
        result.Status |> should equal HealthStatus.Healthy
    }

[<Fact>]
let ``StartupRequired false stays Healthy without touching the seam`` () : Task =
    task {
        let options = legateOptions true
        let dist = coordinationOptions DistributedCoordinationMode.Redis false true

        let admission =
            ScriptedCanaryAdmission(
                ResizeArray(
                    [
                        CanaryThrow(InvalidOperationException("redis down"))
                    ]
                )
            )

        use provider = buildProvider dist (admission :> IDistributedLlmAdmission) null
        let canary = makeCanary options provider

        do! (canary :> IHostedService).StartAsync(CancellationToken.None)

        match canary.Outcome with
        | None -> failwith "expected the canary to record an outcome"
        | Some outcome -> outcome.Status |> should equal HealthStatus.Healthy

        admission.AcquireCalls |> should be Empty

        let! result = checkHealth options provider
        result.Status |> should equal HealthStatus.Healthy
    }

[<Fact>]
let ``Missing client records failure naming the client`` () : Task =
    task {
        let options = legateOptions true
        let dist = coordinationOptions DistributedCoordinationMode.Redis true true
        use provider = buildProvider dist null null
        let canary = makeCanary options provider

        do! (canary :> IHostedService).StartAsync(CancellationToken.None)

        match canary.Outcome with
        | None -> failwith "expected the canary to record an outcome"
        | Some outcome ->
            outcome.Status |> should equal HealthStatus.Unhealthy
            outcome |> describes "IDistributedLlmAdmission" |> should equal true

        let! result = checkHealth options provider
        result.Status |> should equal HealthStatus.Unhealthy
    }

[<Fact>]
let ``Pending canary reads Unhealthy when required`` () : Task =
    task {
        let options = legateOptions true
        let dist = coordinationOptions DistributedCoordinationMode.Redis true true

        let admission =
            ScriptedCanaryAdmission(
                ResizeArray(
                    [
                        CanaryOutcome(DistributedAdmissionOutcome.Acquired())
                    ]
                )
            )

        // Registered but never started: StartAsync has not run.
        let unstarted = buildProvider dist (admission :> IDistributedLlmAdmission) null
        let canary = makeCanary options unstarted
        use provider = buildProvider dist (admission :> IDistributedLlmAdmission) canary

        canary.Outcome |> should equal None

        let! result = checkHealth options provider
        result.Status |> should equal HealthStatus.Unhealthy
        result |> describes "not run yet" |> should equal true
    }

[<Fact>]
let ``Missing canary service reads Unhealthy naming AddLegate`` () : Task =
    task {
        let options = legateOptions true
        let dist = coordinationOptions DistributedCoordinationMode.Redis true true
        use provider = buildProvider dist null null

        let! result = checkHealth options provider
        result.Status |> should equal HealthStatus.Unhealthy
        result |> describes "AddLegate" |> should equal true
    }

[<Fact>]
let ``Registration adds the canary always and the check once with health checks`` () =
    // Without health checks: the canary registers, no check does. The
    // options registration mirrors AddLegate, which always runs first.
    let bare = ServiceCollection() :> IServiceCollection

    bare.AddSingleton<IOptions<LegateOptions>>(
        OptionsWrapper<LegateOptions>(LegateOptions()) :> IOptions<LegateOptions>
    )
    |> ignore

    LlmCoordinationHealthRegistration.register bare

    use bareProvider = bare.BuildServiceProvider()

    bareProvider.GetServices<IHostedService>()
    |> Seq.exists (fun service -> service :? LlmCoordinationCanaryService)
    |> should equal true

    bareProvider.GetServices<IHealthCheck>() |> List.ofSeq |> should be Empty

    // With health checks: the check registers exactly once.
    let services = ServiceCollection() :> IServiceCollection

    services.AddSingleton<IOptions<LegateOptions>>(
        OptionsWrapper<LegateOptions>(LegateOptions()) :> IOptions<LegateOptions>
    )
    |> ignore

    services.AddHealthChecks() |> ignore
    LlmCoordinationHealthRegistration.register services
    LlmCoordinationHealthRegistration.register services

    use provider = services.BuildServiceProvider()
    let configured = provider.GetRequiredService<IOptions<HealthCheckServiceOptions>>()

    let registrations =
        configured.Value.Registrations
        |> Seq.filter (fun registration -> registration.Name = LlmCoordinationHealth.checkName)
        |> List.ofSeq

    registrations.Length |> should equal 1
