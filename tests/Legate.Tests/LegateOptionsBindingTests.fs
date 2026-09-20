// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.LegateOptionsBindingTests

open System
open System.Collections.Generic
open FsUnit.Xunit
open Legate
open Microsoft.Extensions.Configuration
open Xunit

// The env-var test mutates process environment and xunit runs collections
// in parallel, so it owns a dedicated non-parallel collection.
[<CollectionDefinition("LegateOptionsEnv", DisableParallelization = true)>]
type LegateOptionsEnvCollection() = class end

/// Builds the Legate section from in-memory pairs keyed with : separators,
/// the platform provider's normalized form (env-var __ mapping is the
/// provider's job and is covered by the dedicated env test below).
let private buildSection (pairs: (string * string) seq) : IConfigurationSection =
    let keyValues =
        pairs
        |> Seq.map (fun (key, value) -> KeyValuePair<string, string>(key, value))
        |> Seq.toArray

    ConfigurationBuilder().AddInMemoryCollection(keyValues).Build().GetSection("Legate")

[<Fact>]
let ``Binds nested sections rules list and providers dictionary`` () =
    let section =
        buildSection
            [
                "Legate:Sessions:Capacity", "8"
                "Legate:Turns:DefaultMaxIterations", "10"
                "Legate:Turns:DefaultDelivery", "Inject"
                "Legate:Permissions:DefaultDecision", "AllowOnce"
                "Legate:Permissions:Rules:0:ToolPattern", "exec"
                "Legate:Permissions:Rules:0:Decision", "Ask"
                "Legate:Llm:DefaultModel", "anthropic/claude-sonnet"
                "Legate:Llm:Providers:anthropic:ApiKey", "test-key"
                "Legate:Cluster:Mode", "StaticSeeds"
                "Legate:Cluster:SeedNodes:0", "127.0.0.1:5115"
                "Legate:Workspace:Mode", "HostDirectory"
                "Legate:Workspace:RootPath", "/tmp/work"
                "Legate:Completion:MaxDeliveryAttempts", "5"
            ]

    // Ask is a PermissionVerdict name, not a PermissionDecisionKind: the
    // enum pass must reject it precisely rather than bind a zero.
    let ex =
        Assert.Throws<InvalidOperationException>(fun () -> LegateOptionsBinding.bind section |> ignore)

    ex.Message.Contains("Permissions:Rules:0:Decision") |> should equal true
    ex.Message.Contains("unknown permission decision") |> should equal true

[<Fact>]
let ``Binds a full valid section with camelCase keys`` () =
    let section =
        buildSection
            [
                "Legate:sessions:capacity", "8"
                "Legate:turns:defaultMaxIterations", "10"
                "Legate:turns:defaultDelivery", "inject"
                "Legate:permissions:defaultDecision", "allowOnce"
                "Legate:permissions:rules:0:toolPattern", "exec"
                "Legate:permissions:rules:0:decision", "deny"
                "Legate:llm:defaultModel", "anthropic/claude-sonnet"
                "Legate:llm:providers:anthropic:apiKey", "test-key"
                "Legate:llm:distributedCoordination", "true"
                "Legate:cluster:mode", "staticSeeds"
                "Legate:cluster:seedNodes:0", "127.0.0.1:5115"
                "Legate:workspace:mode", "hostDirectory"
                "Legate:workspace:rootPath", "/tmp/work"
                "Legate:completion:maxDeliveryAttempts", "5"
            ]

    let options = LegateOptionsBinding.bind section

    options.Sessions.Capacity |> should equal 8
    options.Turns.DefaultMaxIterations |> should equal 10
    options.Turns.DefaultDelivery |> should equal DeliveryMode.Inject

    options.Permissions.DefaultDecision
    |> should equal PermissionDecisionKind.AllowOnce

    options.Permissions.Rules.Count |> should equal 1
    options.Permissions.Rules[0].ToolPattern |> should equal "exec"

    options.Permissions.Rules[0].Decision
    |> should equal PermissionDecisionKind.Deny

    options.Llm.DefaultModel |> should equal "anthropic/claude-sonnet"
    options.Llm.Providers["anthropic"].ApiKey |> should equal "test-key"
    options.Llm.DistributedCoordination |> should equal true
    options.Cluster.Mode |> should equal ClusterMode.StaticSeeds
    options.Cluster.SeedNodes[0] |> should equal "127.0.0.1:5115"
    options.Workspace.Mode |> should equal WorkspaceMode.HostDirectory
    options.Workspace.RootPath |> should equal "/tmp/work"
    options.Completion.MaxDeliveryAttempts |> should equal 5
    options.Validate() |> should equal null

[<Fact>]
let ``Binds duration forms to TimeSpans`` () =
    let section =
        buildSection
            [
                "Legate:Sessions:LeaseDuration", "30s"
                "Legate:Sessions:LeaseRenewalInterval", "500ms"
                "Legate:Sessions:SubAgents:Timeout", "5m"
                "Legate:Turns:DefaultTimeout", "1h"
                "Legate:Permissions:AskTimeout", "15m"
                "Legate:Completion:RetryDelay", "30d"
                "Legate:Workspace:IdleTeardownAfter", "00:00:30"
                "Legate:Dispatcher:PollInterval", "5s"
            ]

    let options = LegateOptionsBinding.bind section

    options.Sessions.LeaseDuration |> should equal (TimeSpan.FromSeconds 30.0)

    options.Sessions.SubAgents.Timeout |> should equal (TimeSpan.FromMinutes 5.0)

    options.Sessions.LeaseRenewalInterval
    |> should equal (TimeSpan.FromMilliseconds 500.0)

    options.Turns.DefaultTimeout |> should equal (TimeSpan.FromHours 1.0)
    options.Permissions.AskTimeout |> should equal (TimeSpan.FromMinutes 15.0)
    options.Completion.RetryDelay |> should equal (TimeSpan.FromDays 30.0)
    options.Workspace.IdleTeardownAfter |> should equal (TimeSpan.FromSeconds 30.0)
    options.Dispatcher.PollInterval |> should equal (TimeSpan.FromSeconds 5.0)
    options.Validate() |> should equal null

[<Fact>]
let ``Non-positive dispatcher poll interval fails validation with the section path`` () =
    let section =
        buildSection
            [
                "Legate:Dispatcher:PollInterval", "0s"
            ]

    let ex =
        Assert.Throws<InvalidOperationException>(fun () -> LegateOptionsBinding.bind section |> ignore)

    ex.Message.Contains("Dispatcher: PollInterval must be positive.")
    |> should equal true

[<Fact>]
let ``Binds the compaction model override and keep count`` () =
    let section =
        buildSection
            [
                "Legate:Llm:Compaction", "openai/gpt-4o-mini"
                "Legate:Llm:CompactionKeepMessages", "5"
            ]

    let options = LegateOptionsBinding.bind section

    options.Llm.Compaction |> should equal "openai/gpt-4o-mini"
    options.Llm.CompactionKeepMessages |> should equal 5
    options.Validate() |> should equal null

[<Fact>]
let ``Unset compaction knobs keep their defaults`` () =
    let options = LegateOptionsBinding.bind (buildSection [])

    options.Llm.Compaction |> should equal null
    options.Llm.CompactionKeepMessages |> should equal 10
    options.Validate() |> should equal null

[<Fact>]
let ``Invalid compaction knobs fail validation with the section path`` () =
    let badModel =
        buildSection
            [
                "Legate:Llm:Compaction", "not a reference"
            ]

    let modelEx =
        Assert.Throws<InvalidOperationException>(fun () -> LegateOptionsBinding.bind badModel |> ignore)

    modelEx.Message.Contains("Llm") |> should equal true

    modelEx.Message.Contains("Compaction must be a valid model reference")
    |> should equal true

    let badKeep =
        buildSection
            [
                "Legate:Llm:CompactionKeepMessages", "-1"
            ]

    let keepEx =
        Assert.Throws<InvalidOperationException>(fun () -> LegateOptionsBinding.bind badKeep |> ignore)

    keepEx.Message.Contains("Llm") |> should equal true

    keepEx.Message.Contains("CompactionKeepMessages must be at least 0.")
    |> should equal true

[<Fact>]
let ``Binds the cluster shutdown grace duration`` () =
    let section =
        buildSection
            [
                "Legate:Cluster:ShutdownGraceSeconds", "45s"
            ]

    let options = LegateOptionsBinding.bind section

    options.Cluster.ShutdownGraceSeconds |> should equal (TimeSpan.FromSeconds 45.0)

    options.Validate() |> should equal null

[<Fact>]
let ``Non-positive shutdown grace fails validation with the section path`` () =
    let section =
        buildSection
            [
                "Legate:Cluster:ShutdownGraceSeconds", "0s"
            ]

    let ex =
        Assert.Throws<InvalidOperationException>(fun () -> LegateOptionsBinding.bind section |> ignore)

    ex.Message.Contains("Cluster") |> should equal true

    ex.Message.Contains("ShutdownGraceSeconds must be positive.")
    |> should equal true

[<Fact>]
let ``Binds the cluster wire maximum`` () =
    let section =
        buildSection
            [
                "Legate:Cluster:MaxWirePayloadBytes", "262144"
            ]

    let options = LegateOptionsBinding.bind section

    options.Cluster.MaxWirePayloadBytes |> should equal 262144

    options.Validate() |> should equal null

[<Fact>]
let ``Non-positive wire maximum fails validation with the section path`` () =
    let section =
        buildSection
            [
                "Legate:Cluster:MaxWirePayloadBytes", "0"
            ]

    let ex =
        Assert.Throws<InvalidOperationException>(fun () -> LegateOptionsBinding.bind section |> ignore)

    ex.Message.Contains("Cluster") |> should equal true

    ex.Message.Contains("MaxWirePayloadBytes must be at least 1.")
    |> should equal true

[<Fact>]
let ``Invalid duration fails with the section path`` () =
    let section =
        buildSection
            [
                "Legate:Sessions:LeaseDuration", "not-a-duration"
            ]

    let ex =
        Assert.Throws<InvalidOperationException>(fun () -> LegateOptionsBinding.bind section |> ignore)

    ex.Message.Contains("Legate:Sessions:LeaseDuration") |> should equal true
    ex.Message.Contains("unknown duration") |> should equal true

[<Fact>]
let ``Binds the sub-agent depth and timeout`` () =
    let section =
        buildSection
            [
                "Legate:Sessions:SubAgents:MaxDepth", "2"
                "Legate:Sessions:SubAgents:Timeout", "5m"
            ]

    let options = LegateOptionsBinding.bind section
    options.Sessions.SubAgents.MaxDepth |> should equal 2
    options.Sessions.SubAgents.Timeout |> should equal (TimeSpan.FromMinutes 5.0)

[<Fact>]
let ``Invalid sub-agent timeout fails with the section path`` () =
    let section =
        buildSection
            [
                "Legate:Sessions:SubAgents:Timeout", "not-a-duration"
            ]

    let ex =
        Assert.Throws<InvalidOperationException>(fun () -> LegateOptionsBinding.bind section |> ignore)

    ex.Message.Contains("Legate:Sessions:SubAgents:Timeout") |> should equal true
    ex.Message.Contains("unknown duration") |> should equal true

[<Fact>]
let ``Unknown delivery mode fails with the section path`` () =
    let section =
        buildSection
            [
                "Legate:Turns:DefaultDelivery", "Bogus"
            ]

    let ex =
        Assert.Throws<InvalidOperationException>(fun () -> LegateOptionsBinding.bind section |> ignore)

    ex.Message.Contains("Legate:Turns:DefaultDelivery") |> should equal true
    ex.Message.Contains("unknown delivery mode") |> should equal true

[<Fact>]
let ``Unknown cluster mode fails with the section path`` () =
    let section = buildSection [ "Legate:Cluster:Mode", "Galaxy" ]

    let ex =
        Assert.Throws<InvalidOperationException>(fun () -> LegateOptionsBinding.bind section |> ignore)

    ex.Message.Contains("Legate:Cluster:Mode") |> should equal true
    ex.Message.Contains("unknown cluster mode") |> should equal true
    ex.Message.Contains("Local") |> should equal true
    ex.Message.Contains("StaticSeeds") |> should equal true
    ex.Message.Contains("Kubernetes") |> should equal true

[<Fact>]
let ``Binds SBR durations including the Legate-level exit deadline`` () =
    let section =
        buildSection
            [
                "Legate:Cluster:StableAfter", "30s"
                "Legate:Cluster:DownRemovalMargin", "10s"
                "Legate:Cluster:DownAllWhenUnstable", "5s"
                "Legate:Cluster:JoinTimeout", "7s"
                "Legate:Cluster:HostExitDeadline", "90s"
            ]

    let options = LegateOptionsBinding.bind section

    options.Cluster.StableAfter |> should equal (TimeSpan.FromSeconds 30.0)

    options.Cluster.DownRemovalMargin |> should equal (TimeSpan.FromSeconds 10.0)

    options.Cluster.DownAllWhenUnstable.HasValue |> should equal true

    options.Cluster.DownAllWhenUnstable.Value
    |> should equal (TimeSpan.FromSeconds 5.0)

    options.Cluster.JoinTimeout |> should equal (TimeSpan.FromSeconds 7.0)

    options.Cluster.HostExitDeadline |> should equal (TimeSpan.FromSeconds 90.0)

    options.Validate() |> should equal null

[<Fact>]
let ``Unset SBR knobs keep on and off conventions`` () =
    let options = LegateOptionsBinding.bind (buildSection [])

    options.Cluster.StableAfter |> should equal (TimeSpan.FromSeconds 20.0)
    options.Cluster.DownRemovalMargin |> should equal TimeSpan.Zero
    options.Cluster.DownAllWhenUnstable.HasValue |> should equal false
    options.Cluster.JoinTimeout |> should equal (TimeSpan.FromSeconds 5.0)
    options.Cluster.HostExitDeadline |> should equal (TimeSpan.FromSeconds 60.0)
    options.Validate() |> should equal null

[<Fact>]
let ``Zero SBR durations bind the off convention`` () =
    let section =
        buildSection
            [
                "Legate:Cluster:DownRemovalMargin", "0s"
                "Legate:Cluster:DownAllWhenUnstable", "0s"
            ]

    let options = LegateOptionsBinding.bind section

    options.Cluster.DownRemovalMargin |> should equal TimeSpan.Zero
    options.Cluster.DownAllWhenUnstable.HasValue |> should equal true
    options.Cluster.DownAllWhenUnstable.Value |> should equal TimeSpan.Zero
    options.Validate() |> should equal null

[<Fact>]
let ``Non-positive SBR timings fail validation with the section path`` () =
    let section = buildSection [ "Legate:Cluster:StableAfter", "0s" ]

    let ex =
        Assert.Throws<InvalidOperationException>(fun () -> LegateOptionsBinding.bind section |> ignore)

    ex.Message.Contains("Cluster") |> should equal true
    ex.Message.Contains("StableAfter must be positive.") |> should equal true

    let deadlineSection =
        buildSection
            [
                "Legate:Cluster:HostExitDeadline", "0s"
            ]

    let deadlineEx =
        Assert.Throws<InvalidOperationException>(fun () -> LegateOptionsBinding.bind deadlineSection |> ignore)

    deadlineEx.Message.Contains("Cluster") |> should equal true

    deadlineEx.Message.Contains("HostExitDeadline must be positive.")
    |> should equal true

[<Fact>]
let ``Binds the cluster section with modes roles and shard knobs`` () =
    let staticSeeds =
        buildSection
            [
                "Legate:Cluster:Mode", "StaticSeeds"
                "Legate:Cluster:SeedNodes:0", "127.0.0.1:5115"
                "Legate:Cluster:Roles:0", "session"
                "Legate:Cluster:Roles:1", "api"
                "Legate:Cluster:SessionRole", "session"
                "Legate:Cluster:ShardCount", "32"
                "Legate:Cluster:ShardHashVersion", "2"
            ]

    let seeded = LegateOptionsBinding.bind staticSeeds
    seeded.Cluster.Mode |> should equal ClusterMode.StaticSeeds
    seeded.Cluster.SeedNodes[0] |> should equal "127.0.0.1:5115"
    seeded.Cluster.Roles |> List.ofSeq |> should equal [ "session"; "api" ]
    seeded.Cluster.SessionRole |> should equal "session"
    seeded.Cluster.ShardCount |> should equal 32
    seeded.Cluster.ShardHashVersion |> should equal 2
    seeded.Validate() |> should equal null

    let kubernetes =
        buildSection
            [
                "Legate:Cluster:Mode", "Kubernetes"
                "Legate:Cluster:Roles:0", "session"
            ]

    let clustered = LegateOptionsBinding.bind kubernetes
    clustered.Cluster.Mode |> should equal ClusterMode.Kubernetes
    clustered.Cluster.SeedNodes.Count |> should equal 0
    clustered.Cluster.Roles[0] |> should equal "session"
    clustered.Validate() |> should equal null

[<Fact>]
let ``StaticSeeds without seed nodes fails validation`` () =
    let section = buildSection [ "Legate:Cluster:Mode", "StaticSeeds" ]

    let ex =
        Assert.Throws<InvalidOperationException>(fun () -> LegateOptionsBinding.bind section |> ignore)

    ex.Message.Contains("SeedNodes must not be empty in StaticSeeds mode.")
    |> should equal true

[<Fact>]
let ``Unknown permission decision fails with the section path`` () =
    let section =
        buildSection
            [
                "Legate:Permissions:DefaultDecision", "Maybe"
            ]

    let ex =
        Assert.Throws<InvalidOperationException>(fun () -> LegateOptionsBinding.bind section |> ignore)

    ex.Message.Contains("Legate:Permissions:DefaultDecision") |> should equal true
    ex.Message.Contains("unknown permission decision") |> should equal true

[<Fact>]
let ``Unknown ask-user mode fails with the section path`` () =
    let section = buildSection [ "Legate:AskUser:Mode", "Sometimes" ]

    let ex =
        Assert.Throws<InvalidOperationException>(fun () -> LegateOptionsBinding.bind section |> ignore)

    ex.Message.Contains("Legate:AskUser:Mode") |> should equal true
    ex.Message.Contains("unknown ask-user mode") |> should equal true

[<Fact>]
let ``Binds the ask-user section with the canned answer`` () =
    let section =
        buildSection
            [
                "Legate:AskUser:Mode", "AnswerWith"
                "Legate:AskUser:CannedAnswer", "canned-42"
            ]

    let options = LegateOptionsBinding.bind section

    options.AskUser.Mode |> should equal AskUserMode.AnswerWith
    options.AskUser.CannedAnswer |> should equal "canned-42"
    options.Validate() |> should equal null

[<Fact>]
let ``AnswerWith without a canned answer fails validation`` () =
    let section = buildSection [ "Legate:AskUser:Mode", "AnswerWith" ]

    let ex =
        Assert.Throws<InvalidOperationException>(fun () -> LegateOptionsBinding.bind section |> ignore)

    ex.Message.Contains("AskUser") |> should equal true

    ex.Message.Contains("CannedAnswer must be a non-empty string when Mode is AnswerWith.")
    |> should equal true

[<Fact>]
let ``Binding then validating composes across nested sections`` () =
    // A bound capacity of zero passes binding but fails validation, and
    // the throw carries the nested section path.
    let section = buildSection [ "Legate:Sessions:Capacity", "0" ]

    let ex =
        Assert.Throws<InvalidOperationException>(fun () -> LegateOptionsBinding.bind section |> ignore)

    ex.Message.Contains("Legate") |> should equal true
    ex.Message.Contains("Sessions") |> should equal true
    ex.Message.Contains("Capacity must be at least 1.") |> should equal true

[<Fact>]
let ``Empty section binds to valid single-node defaults`` () =
    let section = buildSection []
    let options = LegateOptionsBinding.bind section
    options.Validate() |> should equal null
    options.Cluster.Mode |> should equal ClusterMode.Local
    options.Turns.DefaultDelivery |> should equal DeliveryMode.Queue
    options.Llm.DistributedCoordination |> should equal false

[<Fact>]
let ``Binds the pruning section with scalar knobs`` () =
    let section =
        buildSection
            [
                "Legate:Pruning:ReservedBufferTokens", "5000"
                "Legate:Pruning:KeepLastAssistantTurns", "1"
                "Legate:Pruning:PrunedMarker", "[pruned]"
            ]

    let options = LegateOptionsBinding.bind section

    options.Pruning.ReservedBufferTokens |> should equal 5000
    options.Pruning.KeepLastAssistantTurns |> should equal 1
    options.Pruning.PrunedMarker |> should equal "[pruned]"
    options.Validate() |> should equal null

[<Fact>]
let ``Binding then validating rejects a negative pruning buffer`` () =
    let section =
        buildSection
            [
                "Legate:Pruning:ReservedBufferTokens", "-1"
            ]

    let ex =
        Assert.Throws<InvalidOperationException>(fun () -> LegateOptionsBinding.bind section |> ignore)

    ex.Message.Contains("Pruning") |> should equal true

    ex.Message.Contains("ReservedBufferTokens must be at least 0.")
    |> should equal true

// ──────────────────────────────────────────────────────────────────────────
// Env vars (dedicated collection: mutates process environment)

[<Collection("LegateOptionsEnv")>]
type EnvBindingTests() =

    [<Fact>]
    member _.``Binds Legate__Sessions__Capacity from the environment``() =
        let key = "Legate__Sessions__Capacity"
        let prior = Environment.GetEnvironmentVariable(key)

        try
            Environment.SetEnvironmentVariable(key, "8")

            let section =
                ConfigurationBuilder().AddEnvironmentVariables().Build().GetSection("Legate")

            let options = LegateOptionsBinding.bind section
            options.Sessions.Capacity |> should equal 8
        finally
            Environment.SetEnvironmentVariable(key, prior)
