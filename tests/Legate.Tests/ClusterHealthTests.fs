// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.ClusterHealthTests

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

// Readiness (issue 127): the legate-cluster predicate over snapshots plus
// the registration that rides along only when the host uses health checks.

// ──────────────────────────────────────────────────────────────────────────
// Predicate

/// Whether the result description names the fragment. Descriptions are
/// nullable, so a missing description reads as no match.
let private describes (fragment: string) (result: HealthCheckResult) : bool =
    match box result.Description with
    | null -> false
    | :? string as description -> description.Contains(fragment)
    | _ -> false

[<Fact>]
let ``Draining is Unhealthy before any other signal`` () =
    let result = ClusterHealth.evaluateSnapshot true true true 3 3 0
    result.Status |> should equal HealthStatus.Unhealthy
    result |> describes "draining" |> should equal true

[<Fact>]
let ``Self not Up is Unhealthy`` () =
    let result = ClusterHealth.evaluateSnapshot false false true 1 1 0
    result.Status |> should equal HealthStatus.Unhealthy
    result |> describes "not Up" |> should equal true

[<Fact>]
let ``Missing session role is Unhealthy`` () =
    let result = ClusterHealth.evaluateSnapshot false true false 1 1 0
    result.Status |> should equal HealthStatus.Unhealthy
    result |> describes "session role" |> should equal true

[<Fact>]
let ``Unreachable members are Unhealthy`` () =
    let result = ClusterHealth.evaluateSnapshot false true true 1 2 1
    result.Status |> should equal HealthStatus.Unhealthy
    result |> describes "unreachable" |> should equal true

[<Fact>]
let ``Losing majority is Unhealthy`` () =
    // Unreachable is zero here so the majority rule is the only tripwire:
    // one of three reachable loses the strict majority.
    let result = ClusterHealth.evaluateSnapshot false true true 1 3 0
    result.Status |> should equal HealthStatus.Unhealthy
    result |> describes "majority" |> should equal true

[<Fact>]
let ``Single node Up with its role is Healthy`` () =
    let result = ClusterHealth.evaluateSnapshot false true true 1 1 0
    result.Status |> should equal HealthStatus.Healthy

[<Fact>]
let ``Two of three reachable keeps majority Healthy`` () =
    // Two reachable of three total holds the strict majority with no
    // unreachable members.
    let result = ClusterHealth.evaluateSnapshot false true true 2 3 0
    result.Status |> should equal HealthStatus.Healthy

// ──────────────────────────────────────────────────────────────────────────
// Check in Local mode

[<Fact>]
let ``Local mode is Healthy without a cluster system`` () : Task =
    task {
        use provider = (ServiceCollection() :> IServiceCollection).BuildServiceProvider()

        let check =
            ClusterHealthCheck(OptionsWrapper<LegateOptions>(LegateOptions()) :> IOptions<LegateOptions>, provider)
            :> IHealthCheck

        let! result = check.CheckHealthAsync(HealthCheckContext(), CancellationToken.None)
        result.Status |> should equal HealthStatus.Healthy
    }

// ──────────────────────────────────────────────────────────────────────────
// Registration

[<Fact>]
let ``Registration skips hosts without health checks`` () =
    let services = ServiceCollection() :> IServiceCollection
    ClusterHealthRegistration.register services

    use provider = services.BuildServiceProvider()

    let checks = provider.GetServices<IHealthCheck>() |> List.ofSeq

    checks |> should be Empty

[<Fact>]
let ``Registration adds legate-cluster once when the host has health checks`` () =
    let services = ServiceCollection() :> IServiceCollection
    services.AddHealthChecks() |> ignore
    ClusterHealthRegistration.register services
    // A repeated AddLegate must not duplicate the check.
    ClusterHealthRegistration.register services

    use provider = services.BuildServiceProvider()
    let configured = provider.GetRequiredService<IOptions<HealthCheckServiceOptions>>()

    let registrations =
        configured.Value.Registrations
        |> Seq.filter (fun registration -> registration.Name = ClusterHealth.checkName)
        |> List.ofSeq

    registrations.Length |> should equal 1
