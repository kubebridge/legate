// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.LegateOptionsTests

open System
open System.Collections.Generic
open FsUnit.Xunit
open Legate
open Xunit

/// Smuggles a null past F# nullness checking for defensive-validation tests:
///
/// hosts in C# can always hand a null back, so the options Validate methods
/// guard against null even though F# callers cannot produce one directly.
let private nullRef<'T> : 'T = Unchecked.defaultof<'T>

// ──────────────────────────────────────────────────────────────────────────
// Defaults

[<Fact>]
let ``Defaults validate to null and describe a single node`` () =
    let options = LegateOptions()
    options.Validate() |> should equal null

    options.Sessions.Capacity |> should equal 4
    options.Sessions.LeaseDuration |> should equal (TimeSpan.FromSeconds 60.0)

    options.Sessions.LeaseRenewalInterval
    |> should equal (TimeSpan.FromSeconds 15.0)

    options.Turns.DefaultMaxIterations |> should equal 50
    options.Turns.DefaultTimeout |> should equal (TimeSpan.FromMinutes 30.0)
    options.Turns.DefaultDelivery |> should equal DeliveryMode.Queue
    options.Turns.CrashResume |> should equal TurnCrashResume.Fail
    options.Llm.DistributedCoordination |> should equal false
    options.Cluster.Mode |> should equal ClusterMode.Local
    options.Workspace.Mode |> should equal WorkspaceMode.Process
    options.Completion.MaxDeliveryAttempts |> should equal 3

    options.Sessions.Validate() |> should equal null
    options.Turns.Validate() |> should equal null
    options.Permissions.Validate() |> should equal null
    options.Llm.Validate() |> should equal null
    options.Workspace.Validate() |> should equal null
    options.Completion.Validate() |> should equal null
    options.Cluster.Validate() |> should equal null

[<Fact>]
let ``Options are mutable`` () =
    let options = LegateOptions()
    options.Sessions.Capacity <- 8
    options.Sessions.Capacity |> should equal 8
    options.Turns.DefaultMaxIterations <- 10
    options.Turns.DefaultMaxIterations |> should equal 10

// ──────────────────────────────────────────────────────────────────────────
// Sessions

[<Fact>]
let ``Sessions Validate flags capacity and lease bounds`` () =
    SessionsOptions(Capacity = 0).Validate()
    |> should equal "Capacity must be at least 1."

    SessionsOptions(MaxSessionsPerTenant = 0).Validate()
    |> should equal "MaxSessionsPerTenant must be at least 1."

    SessionsOptions(LeaseDuration = TimeSpan.Zero).Validate()
    |> should equal "LeaseDuration must be positive."

    SessionsOptions(LeaseRenewalInterval = TimeSpan.Zero).Validate()
    |> should equal "LeaseRenewalInterval must be positive."

[<Fact>]
let ``Sessions Validate requires renewal below half the lease`` () =
    // Renewal at exactly half the lease is rejected: a single missed
    // heartbeat would already lose the lease.
    let atHalf =
        SessionsOptions(LeaseDuration = TimeSpan.FromSeconds 60.0, LeaseRenewalInterval = TimeSpan.FromSeconds 30.0)

    atHalf.Validate()
    |> should equal "LeaseRenewalInterval must be less than half LeaseDuration."

    let aboveHalf =
        SessionsOptions(LeaseDuration = TimeSpan.FromSeconds 60.0, LeaseRenewalInterval = TimeSpan.FromSeconds 45.0)

    aboveHalf.Validate()
    |> should equal "LeaseRenewalInterval must be less than half LeaseDuration."

    // The default 15 s renewal against a 60 s lease passes.
    SessionsOptions().Validate() |> should equal null

// ──────────────────────────────────────────────────────────────────────────
// Turns

[<Fact>]
let ``Turns Validate flags budgets`` () =
    TurnsOptions(DefaultMaxIterations = 0).Validate()
    |> should equal "DefaultMaxIterations must be at least 1."

    TurnsOptions(DefaultTimeout = TimeSpan.Zero).Validate()
    |> should equal "DefaultTimeout must be positive."

[<Fact>]
let ``Turns Validate flags unknown delivery mode and crash resume`` () =
    TurnsOptions(DefaultDelivery = enum<DeliveryMode> 99).Validate()
    |> should equal "DefaultDelivery has an unknown delivery mode."

    TurnsOptions(CrashResume = enum<TurnCrashResume> 99).Validate()
    |> should equal "CrashResume has an unknown crash resume."

// ──────────────────────────────────────────────────────────────────────────
// Permissions

[<Fact>]
let ``Permissions defaults deny and validate`` () =
    let options = PermissionsOptions()
    options.DefaultDecision |> should equal PermissionDecisionKind.Deny
    options.Validate() |> should equal null

[<Fact>]
let ``Permissions Validate flags unknown decision and ask timeout`` () =
    PermissionsOptions(DefaultDecision = enum<PermissionDecisionKind> 99).Validate()
    |> should equal "DefaultDecision has an unknown permission decision."

    PermissionsOptions(AskTimeout = TimeSpan.Zero).Validate()
    |> should equal "AskTimeout must be positive."

[<Fact>]
let ``Permissions Validate flags bad rules with indices`` () =
    let options = PermissionsOptions()
    // A default rule carries a null pattern, which must fail validation.
    options.Rules.Add(PermissionRuleOptions())

    options.Validate()
    |> should equal "Rules[0]: ToolPattern must be a non-empty string."

    let unknownDecision = PermissionsOptions()
    unknownDecision.Rules.Add(PermissionRuleOptions(ToolPattern = "exec", Decision = enum<PermissionDecisionKind> 99))

    unknownDecision.Validate()
    |> should equal "Rules[0]: Decision has an unknown permission decision."

    let nullRule = PermissionsOptions()
    nullRule.Rules.Add(nullRef<PermissionRuleOptions>)
    nullRule.Validate() |> should equal "Rules[0]: rule must not be null."

// ──────────────────────────────────────────────────────────────────────────
// Llm

[<Fact>]
let ``Llm defaults coordinate locally with no providers`` () =
    let options = LlmOptions()
    options.DefaultModel |> should equal null
    options.DistributedCoordination |> should equal false
    options.Providers.Count |> should equal 0
    options.Validate() |> should equal null

[<Fact>]
let ``Llm Validate flags bad default model and coordination`` () =
    LlmOptions(DefaultModel = "not a reference").Validate()
    |> should equal "DefaultModel must be a valid model reference in provider/model form."

    let badCoordination = LlmOptions()
    badCoordination.Coordination.MaxConcurrentRequests <- 0

    badCoordination.Validate()
    |> should equal "MaxConcurrentRequests must be at least 1."

[<Fact>]
let ``Llm Validate flags bad provider map`` () =
    let nullProviders = LlmOptions()
    nullProviders.Providers <- nullRef<Dictionary<string, LlmProviderOptions>>
    nullProviders.Validate() |> should equal "Providers must not be null."

    let emptyKey = LlmOptions()
    emptyKey.Providers.Add("", LlmProviderOptions()) |> ignore

    emptyKey.Validate()
    |> should equal "Providers must be keyed by a non-empty provider id."

    let nullValue = LlmOptions()
    nullValue.Providers.Add("anthropic", nullRef<LlmProviderOptions>) |> ignore

    nullValue.Validate()
    |> should equal "Providers['anthropic']: settings must not be null."

    let valid = LlmOptions(DefaultModel = "anthropic/claude-sonnet")
    // A default provider carries a null API key, which is valid: hosts may
    // supply keys through IApiKeyProvider instead.
    valid.Providers.Add("anthropic", LlmProviderOptions()) |> ignore
    valid.Validate() |> should equal null

// ──────────────────────────────────────────────────────────────────────────
// Workspace

[<Fact>]
let ``Workspace defaults run scratch process workspaces`` () =
    let options = WorkspaceSectionOptions()
    options.Mode |> should equal WorkspaceMode.Process
    options.RootPath |> should equal null
    options.Validate() |> should equal null

[<Fact>]
let ``Workspace Validate flags unknown mode and bad paths`` () =
    WorkspaceSectionOptions(Mode = enum<WorkspaceMode> 99).Validate()
    |> should equal "Mode has an unknown workspace mode."

    WorkspaceSectionOptions(RootPath = "").Validate()
    |> should equal "RootPath must be a non-empty path when set."

    WorkspaceSectionOptions(IdleTeardownAfter = TimeSpan.Zero).Validate()
    |> should equal "IdleTeardownAfter must be positive."

// ──────────────────────────────────────────────────────────────────────────
// Completion

[<Fact>]
let ``Completion Validate flags attempts and negative delay`` () =
    CompletionOptions(MaxDeliveryAttempts = 0).Validate()
    |> should equal "MaxDeliveryAttempts must be at least 1."

    CompletionOptions(RetryDelay = TimeSpan.FromSeconds(-1.0)).Validate()
    |> should equal "RetryDelay must not be negative."

    CompletionOptions().Validate() |> should equal null

// ──────────────────────────────────────────────────────────────────────────
// Cluster

[<Fact>]
let ``Cluster defaults run local with no seeds`` () =
    let options = ClusterOptions()
    options.Mode |> should equal ClusterMode.Local
    options.SeedNodes.Count |> should equal 0
    options.ShutdownGraceSeconds |> should equal (TimeSpan.FromSeconds 30.0)
    options.Validate() |> should equal null

[<Fact>]
let ``Cluster Validate flags unknown mode and bad seeds`` () =
    ClusterOptions(Mode = enum<ClusterMode> 99).Validate()
    |> should equal "Mode has an unknown cluster mode."

    ClusterOptions(ShutdownGraceSeconds = TimeSpan.Zero).Validate()
    |> should equal "ShutdownGraceSeconds must be positive."

    ClusterOptions(ShutdownGraceSeconds = TimeSpan.FromSeconds(-1.0)).Validate()
    |> should equal "ShutdownGraceSeconds must be positive."

    let nullSeeds = ClusterOptions()
    nullSeeds.SeedNodes <- nullRef<List<string>>
    nullSeeds.Validate() |> should equal "SeedNodes must not be null."

    let blankSeed = ClusterOptions()
    blankSeed.SeedNodes.Add("  ") |> ignore

    blankSeed.Validate()
    |> should equal "SeedNodes[0] must be a non-empty host:port value."

    let seeded = ClusterOptions(Mode = ClusterMode.Clustered)
    seeded.SeedNodes.Add("127.0.0.1:5115") |> ignore
    seeded.Validate() |> should equal null

// ──────────────────────────────────────────────────────────────────────────
// Root composition

[<Fact>]
let ``Root Validate prefixes section violations`` () =
    let options = LegateOptions()
    options.Sessions.Capacity <- 0
    options.Validate() |> should equal "Sessions: Capacity must be at least 1."

    let turns = LegateOptions()
    turns.Turns.DefaultDelivery <- enum<DeliveryMode> 99

    turns.Validate()
    |> should equal "Turns: DefaultDelivery has an unknown delivery mode."

[<Fact>]
let ``Root Validate flags null sections`` () =
    let options = LegateOptions()
    options.Sessions <- nullRef<SessionsOptions>
    options.Validate() |> should equal "Sessions must not be null."

    let turns = LegateOptions()
    turns.Cluster <- nullRef<ClusterOptions>
    turns.Validate() |> should equal "Cluster must not be null."
