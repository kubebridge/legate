// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.BuilderTests

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Legate.Cluster
open Legate.Storage.InMemory
open Microsoft.Extensions.AI
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Options
open Xunit

// ──────────────────────────────────────────────────────────────────────────
// Stubs

/// An LLM provider with no clients: CreateChatClient is never exercised by
/// the builder tests, which only assert registration and startup checks.
type StubLlmProvider() =

    interface ILlmProvider with
        member _.Id = "stub"
        member _.DefaultModel = "stub-model"

        member _.Capabilities =
            {
                Streaming = false
                Reasoning = false
                ToolCalling = true
            }

        member _.CreateChatClient(_model: ModelReference, _options: LlmProviderOptions | null) =
            raise (NotImplementedException("The stub provider creates no clients."))

/// A workspace runtime that reports ready but binds nothing: Bind is never
/// exercised by the builder tests.
type StubWorkspaceRuntime() =

    interface IWorkspaceRuntime with
        member _.Bind(_session: Session, _options: WorkspaceOptions | null, _cancellationToken: CancellationToken) =
            raise (WorkspaceException("stub", "The stub runtime binds no workspaces."))

        member _.CheckReadiness(_cancellationToken: CancellationToken) =
            Task.FromResult(WorkspaceReadiness(true, null))

/// An admission policy that admits everything and counts nothing.
type StubAdmissionPolicy() =

    interface ISessionAdmissionPolicy with
        member _.Authorize(_context: SessionAdmissionContext) = SessionAdmissionDecision.Allow

/// A usage observer that counts checkpoints so override tests can tell it
/// apart from the no-op default.
type StubUsageObserver() =
    let mutable checkpoints = 0

    interface IUsageObserver with
        member _.OnCheckpoint(_usage: UsageCheckpoint) = checkpoints <- checkpoints + 1
        member _.OnSettled(_usage: UsageSettlement) = ()

    /// How many checkpoints the stub observed.
    member _.Checkpoints = checkpoints

/// A model policy that allows every call.
type StubModelPolicy() =

    interface IModelPolicy with
        member _.Authorize(_tenant: TenantId, _provider: string, _model: string) = ModelPolicyDecision.Allow

/// An artifact quota that grants every reservation under one id.
type StubQuota() =

    interface IArtifactQuota with
        member _.Reserve(_request: ArtifactQuotaRequest, _cancellationToken: CancellationToken) =
            Task.FromResult(ArtifactQuotaDecision.Grant "stub")

        member _.Commit(_reservationId: string, _cancellationToken: CancellationToken) = Task.CompletedTask
        member _.Release(_reservationId: string, _cancellationToken: CancellationToken) = Task.CompletedTask

/// An audit sink that acknowledges every change.
type StubAuditSink() =

    interface IAgentAuditSink with
        member _.OnAgentChanged(_agent: Agent, _cancellationToken: CancellationToken) = Task.CompletedTask

/// A tool source that contributes no tools.
type StubToolSource() =

    interface IToolSource with
        member _.GetTools(_context: ToolSourceContext) =
            Task.FromResult(ResizeArray<AITool>() :> IReadOnlyList<AITool>)

/// A permission policy that allows every call.
type StubPermissionPolicy() =

    interface IPermissionPolicy with
        member _.Evaluate(_request: PermissionRequest) = PermissionVerdict.Allow

// ──────────────────────────────────────────────────────────────────────────
// Helpers

/// Configures the builder the F# way: a plain fun over LegateBuilder. The
/// named optional argument pins the fun overload over the Action one.
let private addLegateFSharp (services: IServiceCollection) (configure: LegateBuilder -> unit) =
    LegateServiceCollectionExtensions.AddLegate(services, ?configure = Some configure)
    |> ignore

/// Runs every registered hosted service start hook, the way a host start
/// runs startup validation and options validation.
let private startHosted (provider: IServiceProvider) : unit =
    for service in provider.GetServices<IHostedService>() do
        service.StartAsync(CancellationToken.None).GetAwaiter().GetResult()

/// Builds the Legate configuration section from in-memory pairs, the same
/// helper shape as LegateOptionsBindingTests.
let private buildSection (pairs: (string * string) seq) : IConfigurationSection =
    let keyValues =
        pairs
        |> Seq.map (fun (key, value) -> KeyValuePair<string, string>(key, value))
        |> Seq.toArray

    ConfigurationBuilder().AddInMemoryCollection(keyValues).Build().GetSection("Legate")

/// Registers the three required registrations through the sub-builders.
let private registerRequired (builder: LegateBuilder) : unit =
    builder.Llm.AddProvider(StubLlmProvider()) |> ignore

    builder.Storage.UseSessionStore(InMemorySessionStore(InMemoryDatabase()))
    |> ignore

    builder.Workspace.UseRuntime(StubWorkspaceRuntime()) |> ignore

// ──────────────────────────────────────────────────────────────────────────
// Defaults

[<Fact>]
let ``Defaults resolve the documented policies quotas and hooks`` () =
    let services = ServiceCollection()
    addLegateFSharp services (fun _ -> ())
    use provider = services.BuildServiceProvider()

    provider.GetService<ISessionAdmissionPolicy>() :? AllowAllAdmissionPolicy
    |> should equal true

    provider.GetService<IModelPolicy>() :? AllowAllModelPolicy |> should equal true

    provider.GetService<IArtifactQuota>() :? UnlimitedArtifactQuota
    |> should equal true

    provider.GetService<IUsageObserver>() :? NoopUsageObserver |> should equal true

    provider.GetService<IAgentAuditSink>() :? NoopAgentAuditSink
    |> should equal true

    provider.GetService<TimeProvider>() |> should not' (be null)
    provider.GetService<ILlmDelay>() |> should not' (be null)
    provider.GetService<ILlmRandom>() |> should not' (be null)

    // Nothing optional is registered by default: no providers, no stores,
    // no sources, no permission policy.
    isNull (box (provider.GetService<ILlmProvider>())) |> should equal true
    isNull (box (provider.GetService<ISessionStore>())) |> should equal true
    isNull (box (provider.GetService<IWorkspaceRuntime>())) |> should equal true
    isNull (box (provider.GetService<IAgentStore>())) |> should equal true
    isNull (box (provider.GetService<IToolSource>())) |> should equal true
    isNull (box (provider.GetService<IPermissionPolicy>())) |> should equal true

    // The default options are the single-node LegateOptions defaults.
    let options = provider.GetRequiredService<IOptions<LegateOptions>>().Value
    options.Sessions.Capacity |> should equal 4
    options.Validate() |> should equal null

[<Fact>]
let ``No-op observer and audit sink accept deliveries without effects`` () =
    let services = ServiceCollection()
    addLegateFSharp services (fun _ -> ())
    use provider = services.BuildServiceProvider()

    let checkpoint: UsageCheckpoint =
        {
            Tenant = TenantId.Create "acme"
            SessionId = SessionId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"
            TurnId = TurnId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"
            Attempt = 1
            Provider = "stub"
            Model = "stub-model"
            InputTokens = 120L
            OutputTokens = 45L
            IdempotencyKey = "checkpoint-1"
        }

    let observer = provider.GetRequiredService<IUsageObserver>()
    observer.OnCheckpoint(checkpoint)

    let settlement: UsageSettlement =
        {
            Tenant = TenantId.Create "acme"
            SessionId = SessionId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"
            TurnId = TurnId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"
            Attempt = 1
            Provider = "stub"
            Model = "stub-model"
            InputTokens = 120L
            OutputTokens = 45L
            IdempotencyKey = "settlement-1"
        }

    observer.OnSettled(settlement)

    let sink = provider.GetRequiredService<IAgentAuditSink>()

    sink.OnAgentChanged(Unchecked.defaultof<Agent>, CancellationToken.None).GetAwaiter().GetResult()

// ──────────────────────────────────────────────────────────────────────────
// Overrides

[<Fact>]
let ``Host registrations through the sub-builders win over the defaults`` () =
    let admission = StubAdmissionPolicy()
    let observer = StubUsageObserver()
    let model = StubModelPolicy()
    let quota = StubQuota()
    let sink = StubAuditSink()
    let source = StubToolSource()
    let policy = StubPermissionPolicy()
    let runtime = StubWorkspaceRuntime()
    let store = InMemorySessionStore(InMemoryDatabase())
    let llm = StubLlmProvider()

    let services = ServiceCollection()

    addLegateFSharp services (fun builder ->
        builder.Policies.UseAdmissionPolicy(admission) |> ignore
        builder.Policies.UseUsageObserver(observer) |> ignore
        builder.Policies.UseModelPolicy(model) |> ignore
        builder.Storage.UseQuota(quota) |> ignore
        builder.Storage.UseSessionStore(store) |> ignore
        builder.Agents.UseAuditSink(sink) |> ignore
        builder.Tools.AddSource(source) |> ignore
        builder.Permissions.UsePolicy(policy) |> ignore
        builder.Workspace.UseRuntime(runtime) |> ignore
        builder.Llm.AddProvider(llm) |> ignore)

    use provider = services.BuildServiceProvider()

    provider.GetService<ISessionAdmissionPolicy>() |> should equal admission
    provider.GetService<IUsageObserver>() |> should equal observer
    provider.GetService<IModelPolicy>() |> should equal model
    provider.GetService<IArtifactQuota>() |> should equal quota
    provider.GetService<ISessionStore>() |> should equal store
    provider.GetService<IAgentAuditSink>() |> should equal sink
    provider.GetService<IToolSource>() |> should equal source
    provider.GetService<IPermissionPolicy>() |> should equal policy
    provider.GetService<IWorkspaceRuntime>() |> should equal runtime
    provider.GetService<ILlmProvider>() |> should equal llm

[<Fact>]
let ``Host registrations before AddLegate win over the defaults`` () =
    let admission = StubAdmissionPolicy()
    let services = ServiceCollection()
    services.AddSingleton<ISessionAdmissionPolicy>(admission) |> ignore

    addLegateFSharp services (fun _ -> ())
    use provider = services.BuildServiceProvider()

    provider.GetService<ISessionAdmissionPolicy>() |> should equal admission

[<Fact>]
let ``Generic overloads construct container types`` () =
    let services = ServiceCollection()

    // The generic session store builds through the container, so its own
    // database dependency is registered first, like a host would.
    services.AddSingleton<InMemoryDatabase>(InMemoryDatabase()) |> ignore

    addLegateFSharp services (fun builder ->
        builder.Llm.AddProvider<StubLlmProvider>() |> ignore
        builder.Policies.UseAdmissionPolicy<StubAdmissionPolicy>() |> ignore
        builder.Storage.UseSessionStore<InMemorySessionStore>() |> ignore)

    use provider = services.BuildServiceProvider()

    provider.GetService<ILlmProvider>() :? StubLlmProvider |> should equal true

    provider.GetService<ISessionAdmissionPolicy>() :? StubAdmissionPolicy
    |> should equal true

    provider.GetService<ISessionStore>() :? InMemorySessionStore
    |> should equal true

// ──────────────────────────────────────────────────────────────────────────
// Startup validation

[<Fact>]
let ``Empty host fails startup with one message naming every missing registration`` () =
    let services = ServiceCollection()
    addLegateFSharp services (fun _ -> ())
    use provider = services.BuildServiceProvider()

    let ex = Assert.Throws<InvalidOperationException>(fun () -> startHosted provider)

    ex.Message.Contains("ILlmProvider") |> should equal true
    ex.Message.Contains("ISessionStore") |> should equal true
    ex.Message.Contains("IWorkspaceRuntime") |> should equal true

[<Fact>]
let ``Partial host fails startup naming only what is missing`` () =
    let services = ServiceCollection()

    addLegateFSharp services (fun builder -> builder.Llm.AddProvider(StubLlmProvider()) |> ignore)

    use provider = services.BuildServiceProvider()

    let ex = Assert.Throws<InvalidOperationException>(fun () -> startHosted provider)

    ex.Message.Contains("ILlmProvider") |> should equal false
    ex.Message.Contains("ISessionStore") |> should equal true
    ex.Message.Contains("IWorkspaceRuntime") |> should equal true

[<Fact>]
let ``Complete host passes startup`` () =
    let services = ServiceCollection()
    addLegateFSharp services registerRequired
    use provider = services.BuildServiceProvider()

    startHosted provider

// ──────────────────────────────────────────────────────────────────────────
// Configuration

[<Fact>]
let ``UseConfiguration binds the Legate section with durations and enums`` () =
    let section =
        buildSection
            [
                "Legate:Sessions:Capacity", "8"
                "Legate:Sessions:LeaseDuration", "30s"
                "Legate:Sessions:LeaseRenewalInterval", "5s"
                "Legate:Turns:DefaultDelivery", "Inject"
                "Legate:Llm:DefaultModel", "anthropic/claude-sonnet"
            ]

    let services = ServiceCollection()

    addLegateFSharp services (fun builder ->
        registerRequired builder
        builder.UseConfiguration(section) |> ignore)

    use provider = services.BuildServiceProvider()
    let options = provider.GetRequiredService<IOptions<LegateOptions>>().Value

    options.Sessions.Capacity |> should equal 8
    options.Sessions.LeaseDuration |> should equal (TimeSpan.FromSeconds 30.0)
    options.Sessions.LeaseRenewalInterval |> should equal (TimeSpan.FromSeconds 5.0)
    options.Turns.DefaultDelivery |> should equal DeliveryMode.Inject
    options.Llm.DefaultModel |> should equal "anthropic/claude-sonnet"
    options.Validate() |> should equal null

[<Fact>]
let ``UseConfiguration rejects unknown values at registration`` () =
    let section =
        buildSection
            [
                "Legate:Turns:DefaultDelivery", "Teleport"
            ]

    let services = ServiceCollection()

    let register () =
        addLegateFSharp services (fun builder -> builder.UseConfiguration(section) |> ignore)

    let ex = Assert.Throws<InvalidOperationException>(register)
    ex.Message.Contains("DefaultDelivery") |> should equal true

// ──────────────────────────────────────────────────────────────────────────
// Call styles

[<Fact>]
let ``C# Action style registers through the same builder`` () =
    let section = buildSection [ "Legate:Sessions:Capacity", "8" ]
    let llm = StubLlmProvider()
    let services = ServiceCollection()

    LegateServiceCollectionExtensions.AddLegate(
        services,
        Action<LegateBuilder>(fun builder ->
            registerRequired builder
            builder.Llm.AddProvider(llm) |> ignore
            builder.UseConfiguration(section) |> ignore)
    )
    |> ignore

    use provider = services.BuildServiceProvider()
    provider.GetService<ILlmProvider>() |> should equal llm

    let options = provider.GetRequiredService<IOptions<LegateOptions>>().Value
    options.Sessions.Capacity |> should equal 8

    startHosted provider

// ──────────────────────────────────────────────────────────────────────────
// Cluster bootstrap (issue 138)

/// A bootstrap hook that contributes no HOCON and starts nothing: the
/// builder tests only assert registration, never cluster formation.
type StubClusterBootstrap() =

    interface IClusterBootstrap with
        member _.BuildHocon() = ""
        member _.StartAsync(_system: obj, _cancellationToken: CancellationToken) = Task.CompletedTask

[<Fact>]
let ``Cluster UseBootstrap registers the hook instance`` () =
    let hook = StubClusterBootstrap()
    let services = ServiceCollection()

    addLegateFSharp services (fun builder -> builder.Cluster.UseBootstrap(hook :> IClusterBootstrap) |> ignore)

    use provider = services.BuildServiceProvider()
    provider.GetService<IClusterBootstrap>() |> should equal hook

    // No hook by default: Kubernetes keeps the singleton self-join.
    let plain = ServiceCollection()
    addLegateFSharp plain (fun _ -> ())
    use plainProvider = plain.BuildServiceProvider()

    isNull (box (plainProvider.GetService<IClusterBootstrap>()))
    |> should equal true

[<Fact>]
let ``Cluster UseBootstrap generic registers the hook type`` () =
    let services = ServiceCollection()

    addLegateFSharp services (fun builder -> builder.Cluster.UseBootstrap<StubClusterBootstrap>() |> ignore)

    use provider = services.BuildServiceProvider()

    provider.GetService<IClusterBootstrap>() :? StubClusterBootstrap
    |> should equal true

[<Fact>]
let ``Cluster UseKubernetes registers the bootstrap hook with shaped options`` () =
    let services = ServiceCollection()

    addLegateFSharp services (fun builder ->
        builder.Cluster.UseKubernetes(Action<KubernetesOptions>(fun options -> options.RequiredContactPoints <- 3))
        |> ignore)

    use provider = services.BuildServiceProvider()

    provider.GetService<IClusterBootstrap>() |> should not' (be null)

    let options = provider.GetRequiredService<IOptions<KubernetesOptions>>().Value
    options.RequiredContactPoints |> should equal 3
    options.Validate() |> should equal null

[<Fact>]
let ``Cluster UseKubernetes fails fast on invalid options`` () =
    let services = ServiceCollection()

    let register () =
        addLegateFSharp services (fun builder ->
            builder.Cluster.UseKubernetes(Action<KubernetesOptions>(fun options -> options.ManagementPort <- 0))
            |> ignore)

    let ex = Assert.Throws<InvalidOperationException>(register)
    ex.Message.Contains("ManagementPort") |> should equal true
