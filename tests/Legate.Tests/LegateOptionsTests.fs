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

    options.Sessions.SubAgents.MaxDepth |> should equal 1
    options.Sessions.SubAgents.Timeout |> should equal (TimeSpan.FromMinutes 10.0)

    options.Turns.DefaultMaxIterations |> should equal 50
    options.Turns.DefaultTimeout |> should equal (TimeSpan.FromMinutes 30.0)
    options.Turns.DefaultDelivery |> should equal DeliveryMode.Queue
    options.Turns.CrashResume |> should equal TurnCrashResume.Fail
    options.Llm.DistributedCoordination |> should equal false
    options.Cluster.Mode |> should equal ClusterMode.Local
    options.Workspace.Mode |> should equal WorkspaceMode.Process
    options.Completion.MaxDeliveryAttempts |> should equal 3
    options.Completion.RetryDelay |> should equal (TimeSpan.FromSeconds 30.0)
    options.Completion.DeliveredRetention |> should equal (TimeSpan.FromDays 7.0)
    options.Completion.RedriveInterval |> should equal (TimeSpan.FromSeconds 30.0)

    options.Completion.ClaimLeaseDuration
    |> should equal (TimeSpan.FromSeconds 60.0)

    options.Pruning.ReservedBufferTokens |> should equal 10_000
    options.Pruning.KeepLastAssistantTurns |> should equal 2

    options.Pruning.PrunedMarker
    |> should equal ContextPruningOptions.DefaultPrunedMarker

    options.AskUser.Mode |> should equal AskUserMode.Fail
    options.AskUser.CannedAnswer |> should equal null

    options.Dispatcher.PollInterval |> should equal (TimeSpan.FromSeconds 5.0)
    options.Dispatcher.MaxBatchSize |> should equal 50
    options.Dispatcher.MaxSessionsPerAgent |> should equal 4

    options.Sessions.Validate() |> should equal null
    options.Sessions.SubAgents.Validate() |> should equal null
    options.Turns.Validate() |> should equal null
    options.Permissions.Validate() |> should equal null
    options.Dispatcher.Validate() |> should equal null
    options.Llm.Validate() |> should equal null
    options.Workspace.Validate() |> should equal null
    options.Completion.Validate() |> should equal null
    options.Cluster.Validate() |> should equal null
    options.Pruning.Validate() |> should equal null
    options.AskUser.Validate() |> should equal null

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

[<Fact>]
let ``Sessions Validate defaults auto-title off without a model`` () =
    let options = SessionsOptions()
    options.AutoTitle |> should equal false
    options.AutoTitleModel |> should equal null
    options.Validate() |> should equal null

[<Fact>]
let ``Sessions Validate flags an invalid auto-title model`` () =
    SessionsOptions(AutoTitleModel = "bogus").Validate()
    |> should equal "AutoTitleModel must be a valid model reference in provider/model form."

    SessionsOptions(AutoTitle = true, AutoTitleModel = "anthropic/claude-sonnet").Validate()
    |> should equal null

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
    options.Compaction |> should equal null
    options.CompactionKeepMessages |> should equal 10
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

[<Fact>]
let ``Llm Validate flags a bad compaction model and keep count`` () =
    LlmOptions(Compaction = "not a reference").Validate()
    |> should equal "Compaction must be a valid model reference in provider/model form."

    LlmOptions(CompactionKeepMessages = -1).Validate()
    |> should equal "CompactionKeepMessages must be at least 0."

    // Blank overrides read as unset, like DefaultModel.
    LlmOptions(Compaction = "  ").Validate() |> should equal null

    let custom =
        LlmOptions(Compaction = "openai/gpt-4o-mini", CompactionKeepMessages = 0)

    custom.Validate() |> should equal null

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

    CompletionOptions(DeliveredRetention = TimeSpan.Zero).Validate()
    |> should equal "DeliveredRetention must be positive."

    CompletionOptions(RedriveInterval = TimeSpan.Zero).Validate()
    |> should equal "RedriveInterval must be positive."

    CompletionOptions(ClaimLeaseDuration = TimeSpan.Zero).Validate()
    |> should equal "ClaimLeaseDuration must be positive."

    CompletionOptions().Validate() |> should equal null

// ──────────────────────────────────────────────────────────────────────────
// Cluster

[<Fact>]
let ``Cluster defaults run local with no seeds`` () =
    let options = ClusterOptions()
    options.Mode |> should equal ClusterMode.Local
    options.SeedNodes.Count |> should equal 0
    options.Roles.Count |> should equal 0
    options.SessionRole |> should equal "session"
    options.ShardCount |> should equal 128
    options.ShardHashVersion |> should equal 1
    options.ShutdownGraceSeconds |> should equal (TimeSpan.FromSeconds 30.0)
    options.MaxWirePayloadBytes |> should equal 1048576
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

    let seeded = ClusterOptions(Mode = ClusterMode.StaticSeeds)
    seeded.SeedNodes.Add("127.0.0.1:5115") |> ignore
    seeded.Validate() |> should equal null

[<Fact>]
let ``Cluster Validate requires seeds in StaticSeeds but not in Kubernetes`` () =
    let missing = ClusterOptions(Mode = ClusterMode.StaticSeeds)

    missing.Validate()
    |> should equal "SeedNodes must not be empty in StaticSeeds mode."

    let kubernetes = ClusterOptions(Mode = ClusterMode.Kubernetes)
    kubernetes.Validate() |> should equal null

    let kubernetesSeeded = ClusterOptions(Mode = ClusterMode.Kubernetes)
    kubernetesSeeded.SeedNodes.Add("127.0.0.1:5115") |> ignore
    kubernetesSeeded.Validate() |> should equal null

[<Fact>]
let ``Cluster defaults bind ephemeral remoting on loopback`` () =
    let options = ClusterOptions()
    options.RemotingPort |> should equal 0
    options.RemotingHostname |> should equal "127.0.0.1"
    options.Validate() |> should equal null

[<Fact>]
let ``Cluster Validate flags bad remoting port and hostname`` () =
    ClusterOptions(RemotingPort = -1).Validate()
    |> should equal "RemotingPort must be between 0 and 65535."

    ClusterOptions(RemotingPort = 65536).Validate()
    |> should equal "RemotingPort must be between 0 and 65535."

    ClusterOptions(RemotingHostname = "  ").Validate()
    |> should equal "RemotingHostname must be a non-empty string."

    let nullHost = ClusterOptions()
    nullHost.RemotingHostname <- nullRef<string>

    nullHost.Validate()
    |> should equal "RemotingHostname must be a non-empty string."

    let compose = ClusterOptions(RemotingPort = 4053, RemotingHostname = "0.0.0.0")
    compose.Validate() |> should equal null

[<Fact>]
let ``Root Validate prefixes remoting violations with the cluster section`` () =
    let port = LegateOptions()
    port.Cluster.RemotingPort <- 70000

    port.Validate()
    |> should equal "Cluster: RemotingPort must be between 0 and 65535."

    let host = LegateOptions()
    host.Cluster.RemotingHostname <- "  "

    host.Validate()
    |> should equal "Cluster: RemotingHostname must be a non-empty string."

[<Fact>]
let ``Cluster Validate flags bad shard knobs roles and session role`` () =
    ClusterOptions(ShardCount = 0).Validate()
    |> should equal "ShardCount must be at least 1."

    ClusterOptions(ShardHashVersion = 0).Validate()
    |> should equal "ShardHashVersion must be at least 1."

    ClusterOptions(MaxWirePayloadBytes = 0).Validate()
    |> should equal "MaxWirePayloadBytes must be at least 1."

    ClusterOptions(MaxWirePayloadBytes = -8).Validate()
    |> should equal "MaxWirePayloadBytes must be at least 1."

    ClusterOptions(SessionRole = "  ").Validate()
    |> should equal "SessionRole must be a non-empty string."

    let nullRoles = ClusterOptions()
    nullRoles.Roles <- nullRef<List<string>>
    nullRoles.Validate() |> should equal "Roles must not be null."

    let blankRole = ClusterOptions()
    blankRole.Roles.Add("session") |> ignore
    blankRole.Roles.Add("  ") |> ignore

    blankRole.Validate() |> should equal "Roles[1] must be a non-empty string."

    let roleful = ClusterOptions(Mode = ClusterMode.StaticSeeds)
    roleful.SeedNodes.Add("127.0.0.1:5115") |> ignore
    roleful.Roles.Add("session") |> ignore
    roleful.SessionRole <- "session"
    roleful.ShardCount <- 32
    roleful.ShardHashVersion <- 2
    roleful.Validate() |> should equal null

[<Fact>]
let ``Cluster SBR defaults keep majority timings with a Legate-level exit cap`` () =
    let options = ClusterOptions()
    options.StableAfter |> should equal (TimeSpan.FromSeconds 20.0)
    options.DownRemovalMargin |> should equal TimeSpan.Zero
    options.DownAllWhenUnstable.HasValue |> should equal false
    options.JoinTimeout |> should equal (TimeSpan.FromSeconds 5.0)
    options.HostExitDeadline |> should equal (TimeSpan.FromSeconds 60.0)
    options.Validate() |> should equal null

[<Fact>]
let ``Cluster Validate flags bad SBR timings`` () =
    ClusterOptions(StableAfter = TimeSpan.Zero).Validate()
    |> should equal "StableAfter must be positive."

    ClusterOptions(DownRemovalMargin = TimeSpan.FromSeconds(-1.0)).Validate()
    |> should equal "DownRemovalMargin must not be negative."

    let negativeDownAll = ClusterOptions()
    negativeDownAll.DownAllWhenUnstable <- Nullable(TimeSpan.FromSeconds(-1.0))

    negativeDownAll.Validate()
    |> should equal "DownAllWhenUnstable must not be negative when set."

    ClusterOptions(JoinTimeout = TimeSpan.Zero).Validate()
    |> should equal "JoinTimeout must be positive."

    ClusterOptions(HostExitDeadline = TimeSpan.Zero).Validate()
    |> should equal "HostExitDeadline must be positive."

[<Fact>]
let ``Cluster Validate accepts off and duration SBR conventions`` () =
    let removalOff = ClusterOptions(DownRemovalMargin = TimeSpan.Zero)
    removalOff.Validate() |> should equal null

    let removalDuration = ClusterOptions(DownRemovalMargin = TimeSpan.FromSeconds 10.0)

    removalDuration.Validate() |> should equal null

    let downAllOff = ClusterOptions()
    downAllOff.DownAllWhenUnstable <- Nullable TimeSpan.Zero
    downAllOff.Validate() |> should equal null

    let downAllDuration = ClusterOptions()
    downAllDuration.DownAllWhenUnstable <- Nullable(TimeSpan.FromSeconds 5.0)
    downAllDuration.Validate() |> should equal null

[<Fact>]
let ``Root Validate prefixes SBR violations with the cluster section`` () =
    let options = LegateOptions()
    options.Cluster.StableAfter <- TimeSpan.Zero
    options.Validate() |> should equal "Cluster: StableAfter must be positive."

    let deadline = LegateOptions()
    deadline.Cluster.HostExitDeadline <- TimeSpan.Zero

    deadline.Validate()
    |> should equal "Cluster: HostExitDeadline must be positive."

[<Fact>]
let ``Cluster MinimumMembers defaults to the singleton quorum`` () =
    let options = ClusterOptions()
    options.MinimumMembers |> should equal 1
    options.Validate() |> should equal null

[<Fact>]
let ``Cluster Validate flags too few minimum members`` () =
    ClusterOptions(MinimumMembers = 0).Validate()
    |> should equal "MinimumMembers must be at least 1."

    ClusterOptions(MinimumMembers = -1).Validate()
    |> should equal "MinimumMembers must be at least 1."

[<Fact>]
let ``Root Validate prefixes MinimumMembers violations with the cluster section`` () =
    let options = LegateOptions()
    options.Cluster.MinimumMembers <- 0
    options.Validate() |> should equal "Cluster: MinimumMembers must be at least 1."

// ──────────────────────────────────────────────────────────────────────────
// Pruning

[<Fact>]
let ``Pruning defaults reserve buffer and protect two turns`` () =
    let options = ContextPruningOptions()
    options.ReservedBufferTokens |> should equal 10_000
    options.KeepLastAssistantTurns |> should equal 2
    options.PrunedMarker |> should equal ContextPruningOptions.DefaultPrunedMarker
    options.Validate() |> should equal null

[<Fact>]
let ``Pruning Validate flags negative knobs and empty marker`` () =
    ContextPruningOptions(ReservedBufferTokens = -1).Validate()
    |> should equal "ReservedBufferTokens must be at least 0."

    ContextPruningOptions(KeepLastAssistantTurns = -1).Validate()
    |> should equal "KeepLastAssistantTurns must be at least 0."

    ContextPruningOptions(PrunedMarker = "").Validate()
    |> should equal "PrunedMarker must be a non-empty string."

    let nullMarker = ContextPruningOptions()
    nullMarker.PrunedMarker <- nullRef<string>
    nullMarker.Validate() |> should equal "PrunedMarker must be a non-empty string."

    ContextPruningOptions().Validate() |> should equal null

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

    let pruning = LegateOptions()
    pruning.Pruning <- nullRef<ContextPruningOptions>
    pruning.Validate() |> should equal "Pruning must not be null."

[<Fact>]
let ``Root Validate prefixes pruning violations`` () =
    let options = LegateOptions()
    options.Pruning.ReservedBufferTokens <- -1

    options.Validate()
    |> should equal "Pruning: ReservedBufferTokens must be at least 0."

// ──────────────────────────────────────────────────────────────────────────
// AskUser

[<Fact>]
let ``AskUser defaults fail the turn and validate`` () =
    let options = AskUserOptions()
    options.Mode |> should equal AskUserMode.Fail
    options.CannedAnswer |> should equal null
    options.Validate() |> should equal null

[<Fact>]
let ``AskUser Validate flags unknown mode and missing canned answer`` () =
    AskUserOptions(Mode = enum<AskUserMode> 99).Validate()
    |> should equal "Mode has an unknown ask-user mode."

    AskUserOptions(Mode = AskUserMode.AnswerWith).Validate()
    |> should equal "CannedAnswer must be a non-empty string when Mode is AnswerWith."

    let blank = AskUserOptions(Mode = AskUserMode.AnswerWith, CannedAnswer = "  ")

    blank.Validate()
    |> should equal "CannedAnswer must be a non-empty string when Mode is AnswerWith."

    AskUserOptions(Mode = AskUserMode.AnswerWith, CannedAnswer = "canned").Validate()
    |> should equal null

[<Fact>]
let ``Root Validate prefixes ask-user violations`` () =
    let options = LegateOptions()
    options.AskUser.Mode <- enum<AskUserMode> 99

    options.Validate() |> should equal "AskUser: Mode has an unknown ask-user mode."

    let nullSection = LegateOptions()
    nullSection.AskUser <- nullRef<AskUserOptions>
    nullSection.Validate() |> should equal "AskUser must not be null."

// ──────────────────────────────────────────────────────────────────────────
// SubAgents

[<Fact>]
let ``SubAgents Validate flags depth and timeout bounds`` () =
    SubAgentsOptions(MaxDepth = 0).Validate()
    |> should equal "MaxDepth must be at least 1."

    SubAgentsOptions(Timeout = TimeSpan.Zero).Validate()
    |> should equal "Timeout must be positive."

    SubAgentsOptions(MaxDepth = 2, Timeout = TimeSpan.FromMinutes 5.0).Validate()
    |> should equal null

[<Fact>]
let ``Sessions Validate flags null sub-agents`` () =
    let options = SessionsOptions()
    options.SubAgents <- nullRef<SubAgentsOptions>
    options.Validate() |> should equal "SubAgents must not be null."

[<Fact>]
let ``Sessions Validate prefixes sub-agent violations`` () =
    let options = SessionsOptions()
    options.SubAgents.MaxDepth <- 0
    options.Validate() |> should equal "SubAgents: MaxDepth must be at least 1."

    let timeout = SessionsOptions()
    timeout.SubAgents.Timeout <- TimeSpan.Zero
    timeout.Validate() |> should equal "SubAgents: Timeout must be positive."

[<Fact>]
let ``Root Validate prefixes sub-agent violations`` () =
    let options = LegateOptions()
    options.Sessions.SubAgents.Timeout <- TimeSpan.Zero

    options.Validate()
    |> should equal "Sessions: SubAgents: Timeout must be positive."

// ──────────────────────────────────────────────────────────────────────────
// Dispatcher

[<Fact>]
let ``Dispatcher Validate flags interval and batch bounds`` () =
    DispatcherOptions(PollInterval = TimeSpan.Zero).Validate()
    |> should equal "PollInterval must be positive."

    DispatcherOptions(MaxBatchSize = 0).Validate()
    |> should equal "MaxBatchSize must be at least 1."

    DispatcherOptions(MaxSessionsPerAgent = 0).Validate()
    |> should equal "MaxSessionsPerAgent must be at least 1."

    DispatcherOptions(PollInterval = TimeSpan.FromSeconds 5.0, MaxBatchSize = 50, MaxSessionsPerAgent = 4).Validate()
    |> should equal null

[<Fact>]
let ``Root Validate prefixes dispatcher violations`` () =
    let options = LegateOptions()
    options.Dispatcher.PollInterval <- TimeSpan.Zero

    options.Validate() |> should equal "Dispatcher: PollInterval must be positive."

    let nullSection = LegateOptions()
    nullSection.Dispatcher <- nullRef<DispatcherOptions>
    nullSection.Validate() |> should equal "Dispatcher must not be null."
