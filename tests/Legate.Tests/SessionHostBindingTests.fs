// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.SessionHostBindingTests

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open Akka.Actor
open Legate
open Legate.Storage.InMemory
open Legate.Testing
open Microsoft.Extensions.AI
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Options
open Xunit

let private executionServices tenant =
    let services = ServiceCollection()
    services.AddLegate() |> ignore
    let database = InMemoryDatabase()
    services.AddSingleton<ISessionStore>(InMemorySessionStore(database)) |> ignore

    services.AddSingleton<ISessionEventStore>(InMemorySessionEventStore(database))
    |> ignore

    let options = SessionClientOptions()
    options.Tenant <- tenant
    services.AddSingleton(options) |> ignore

    services.AddSingleton<IChatClient>(new ScriptedChatClient(Array.empty<ScriptStep> :> IReadOnlyList<_>))
    |> ignore

    services.AddSingleton<ILlmProvider>(
        { new ILlmProvider with
            member _.Id = "test"
            member _.DefaultModel = "test"

            member _.Capabilities =
                {
                    Streaming = false
                    Reasoning = false
                    ToolCalling = true
                }

            member _.CreateChatClient(_, _) =
                failwith "Context assembly must not invoke a model factory."
        }
    )
    |> ignore

    services.AddSingleton<IWorkspaceRuntime>(
        { new IWorkspaceRuntime with
            member _.Bind(_, _, _) =
                Task.FromException<IWorkspace>(
                    InvalidOperationException("No workspace effects during context assembly.")
                )

            member _.CheckReadiness(_) =
                Task.FromException<WorkspaceReadiness>(
                    InvalidOperationException("No workspace effects during context assembly.")
                )
        }
    )
    |> ignore

    services.AddSingleton<IHostedService>(
        Func<IServiceProvider, IHostedService>(fun _ ->
            failwith "A borrowed provider's hosted services must not be resolved.")
    )
    |> ignore

    services

let private executionProvider tenant =
    (executionServices tenant).BuildServiceProvider(ServiceProviderOptions(ValidateScopes = true))

let private localService (provider: IServiceProvider) =
    provider.GetServices<IHostedService>()
    |> Seq.pick (function
        | :? LocalActorSystemService as local -> Some local
        | _ -> None)

[<Fact>]
let ``issue415 background passes never resolve request scoped facade`` () =
    task {
        let services = executionServices TenantId.Default

        services.AddScoped<SessionClient>(
            Func<IServiceProvider, SessionClient>(fun _ -> invalidOp "No request context")
        )
        |> ignore

        use provider =
            services.BuildServiceProvider(ServiceProviderOptions(ValidateScopes = true))

        do! provider.GetRequiredService<ISessionHostContexts>().InitializeAsync(CancellationToken.None)
        let options = provider.GetRequiredService<IOptions<LegateOptions>>()
        let delay = provider.GetRequiredService<ILlmDelay>()
        let dispatcher = DispatcherService(provider, options, TimeProvider.System, delay)
        let! pass = dispatcher.RunOnceAsync(CancellationToken.None)
        Assert.Equal(0, pass.Started)

        let schedules =
            ScheduleEvaluatorService(provider, options, TimeProvider.System, delay)

        let! fired = schedules.RunOnceAsync(CancellationToken.None)
        Assert.Equal(0, fired)
    }

[<Fact>]
let ``issue415 receiving only root sweeps all initialized borrowed graphs without root execution defaults`` () =
    task {
        let a, b = TenantId.Create "background-a", TenantId.Create "background-b"
        use graphA = executionProvider a
        use graphB = executionProvider b
        let services = ServiceCollection()
        services.AddLegate() |> ignore

        services.AddLegateSessionBinding(
            SessionHostBinding(a, Func<IServiceProvider, IServiceProvider>(fun _ -> graphA))
        )
        |> ignore

        services.AddLegateSessionBinding(
            SessionHostBinding(b, Func<IServiceProvider, IServiceProvider>(fun _ -> graphB))
        )
        |> ignore

        use node =
            services.BuildServiceProvider(ServiceProviderOptions(ValidateScopes = true))

        let contexts = node.GetRequiredService<ISessionHostContexts>()
        do! contexts.InitializeAsync(CancellationToken.None)
        Assert.Null(node.GetService<ISessionStore>())

        let dispatcher =
            DispatcherService(
                node,
                node.GetRequiredService<IOptions<LegateOptions>>(),
                TimeProvider.System,
                node.GetRequiredService<ILlmDelay>()
            )

        let! result = dispatcher.RunOnceAsync(CancellationToken.None)
        Assert.Equal(0, result.Started)
        Assert.Equal(2, contexts.All.Length)
        contexts.CloseAdmission()
        let! stopped = dispatcher.RunOnceAsync(CancellationToken.None)
        Assert.Equal(0, stopped.Started)
    }

[<Fact>]
let ``issue395 initialization is single flight and client access is read only`` () =
    task {
        let tenant = TenantId.Create "tenant-a"
        use execution = executionProvider tenant
        let mutable callbacks = 0

        let binding =
            SessionHostBinding(
                tenant,
                Func<IServiceProvider, IServiceProvider>(fun _ ->
                    Interlocked.Increment(&callbacks) |> ignore
                    execution)
            )

        Assert.Throws<InvalidOperationException>(fun () -> binding.Client |> ignore)
        |> ignore

        let services = ServiceCollection()
        services.AddLegate().AddLegateSessionBinding(binding) |> ignore

        use node =
            services.BuildServiceProvider(ServiceProviderOptions(ValidateScopes = true))

        let contexts = node.GetRequiredService<ISessionHostContexts>()

        do!
            Task.WhenAll(
                [|
                    contexts.InitializeAsync CancellationToken.None
                    contexts.InitializeAsync CancellationToken.None
                |]
            )

        Assert.Equal(1, callbacks)
        Assert.Same(binding.Client, binding.Client)
        Assert.Single(contexts.All) |> ignore

        Assert.Throws<SessionScopeRejectedException>(fun () -> contexts.Get TenantId.Default |> ignore)
        |> ignore

        execution.GetRequiredService<SessionClientOptions>().Tenant <- TenantId.Create "retarget"
        Assert.Equal(tenant, binding.Client.Tenant)
        Assert.Equal(tenant, contexts.Get(tenant).Tenant)
    }

[<Theory>]
[<InlineData("test/facade", "test/config", "test/facade")>]
[<InlineData(null, "test/config", "test/config")>]
[<InlineData(null, null, "test/test")>]
let ``issue415 model preparation finalizes private copies with exact priority``
    (facade: string)
    (configured: string)
    expected
    =
    let services = executionServices TenantId.Default
    let hostOptions = SessionClientOptions(DefaultModel = facade)
    services.AddSingleton(hostOptions) |> ignore

    services.Configure<LegateOptions>(Action<LegateOptions>(fun o -> o.Llm.DefaultModel <- configured))
    |> ignore

    use provider = services.BuildServiceProvider()
    let prepared = SessionClientWiring.prepareOptions provider
    Assert.Equal(expected, prepared.Client.DefaultModel)
    Assert.Equal(facade, hostOptions.DefaultModel)
    Assert.Equal(configured, provider.GetRequiredService<IOptions<LegateOptions>>().Value.Llm.DefaultModel)
    hostOptions.DefaultModel <- "test/retarget"
    Assert.Equal(expected, prepared.Client.DefaultModel)

[<Fact>]
let ``issue415 invalid winning default fails without modifying host options`` () =
    let services = executionServices TenantId.Default
    let options = SessionClientOptions(DefaultModel = "not-qualified")
    services.AddSingleton(options) |> ignore
    use provider = services.BuildServiceProvider()

    Assert.Throws<InvalidOperationException>(fun () -> SessionClientWiring.prepareOptions provider |> ignore)
    |> ignore

    Assert.Equal("not-qualified", options.DefaultModel)

[<Fact>]
let ``issue415 explicit client with multiple providers keeps legacy fallback`` () =
    let services = executionServices TenantId.Default

    services.AddSingleton<ILlmProvider>(
        { new ILlmProvider with
            member _.Id = "other"
            member _.DefaultModel = "other"

            member _.Capabilities =
                {
                    Streaming = false
                    Reasoning = false
                    ToolCalling = false
                }

            member _.CreateChatClient(_, _) =
                failwith "Must preserve explicit client."
        }
    )
    |> ignore

    use provider = services.BuildServiceProvider()
    let prepared = SessionClientWiring.prepareOptions provider
    Assert.Equal(Agents.AgentFileParser.defaultModel.Value, prepared.Client.DefaultModel)

[<Fact>]
let ``issue415 factory lookup never rebinds validates or disposes graphs and closes on stop`` () =
    task {
        let a, b = TenantId.Create "a", TenantId.Create "b"
        use graphA = executionProvider a
        use graphB = executionProvider b
        let mutable binds = 0
        let mutable checks = 0
        let services = ServiceCollection()

        services.AddLegate(
            Action<LegateBuilder>(fun builder ->
                builder.AddExecutionValidation(
                    Action<IServiceProvider, ClusterMode>(fun graph mode ->
                        Assert.Equal(ClusterMode.Local, mode)
                        Assert.True(Object.ReferenceEquals(graph, graphA) || Object.ReferenceEquals(graph, graphB))
                        Interlocked.Increment(&checks) |> ignore)
                )
                |> ignore)
        )
        |> ignore

        for tenant, graph in [ a, graphA; b, graphB ] do
            services.AddLegateSessionBinding(
                SessionHostBinding(
                    tenant,
                    Func<IServiceProvider, IServiceProvider>(fun _ ->
                        Interlocked.Increment(&binds) |> ignore
                        graph)
                )
            )
            |> ignore

        use node = services.BuildServiceProvider()
        let factory = node.GetRequiredService<ISessionClientFactory>()

        Assert.Throws<InvalidOperationException>(fun () -> factory.GetClient(a) |> ignore)
        |> ignore

        let contexts = node.GetRequiredService<ISessionHostContexts>()
        do! contexts.InitializeAsync(CancellationToken.None)
        Assert.Equal(2, binds)
        Assert.Equal(2, checks)
        Assert.NotSame(factory.GetClient(a), factory.GetClient(b))
        Assert.Same(factory.GetClient(a), factory.GetClient(a))
        Assert.Equal("test/test", factory.GetClient(a).AutoTitle.Value.FacadeDefaultModel)
        Assert.Null(graphA.GetRequiredService<SessionClientOptions>().DefaultModel)
        contexts.CloseAdmission()

        let error =
            Assert.Throws<SessionScopeRejectedException>(fun () -> factory.GetClient(a) |> ignore)

        Assert.Equal(SessionScopeRejectionReason.NodeStopping, error.Reason)
        Assert.Equal(2, binds)
        Assert.Equal(2, checks)
    }

[<Fact>]
let ``issue415 incompatible later graph publishes none and never assembles event buses`` () =
    task {
        let a, b = TenantId.Create "a", TenantId.Create "b"
        let servicesA = executionServices a
        let servicesB = executionServices b

        servicesB.AddSingleton<ISessionEventStore>(InMemorySessionEventStore(InMemoryDatabase()))
        |> ignore

        let mutable buses = 0

        for services in [ servicesA; servicesB ] do
            services.AddSingleton<SessionEventBus>(
                Func<IServiceProvider, SessionEventBus>(fun _ ->
                    Interlocked.Increment(&buses) |> ignore
                    failwith "Must not assemble a bus.")
            )
            |> ignore

        use graphA = servicesA.BuildServiceProvider()
        use graphB = servicesB.BuildServiceProvider()

        let first =
            SessionHostBinding(a, Func<IServiceProvider, IServiceProvider>(fun _ -> graphA))

        let second =
            SessionHostBinding(b, Func<IServiceProvider, IServiceProvider>(fun _ -> graphB))

        let services = ServiceCollection()

        services.AddLegate().AddLegateSessionBinding(first).AddLegateSessionBinding(second)
        |> ignore

        use node = services.BuildServiceProvider()
        let contexts = node.GetRequiredService<ISessionHostContexts>()

        let! error =
            Assert.ThrowsAsync<InvalidOperationException>(fun () -> contexts.InitializeAsync(CancellationToken.None))

        Assert.Contains("incompatible", error.Message)
        Assert.Empty(contexts.All)
        Assert.Equal(0, buses)

        Assert.Throws<InvalidOperationException>(fun () -> first.Client |> ignore)
        |> ignore

        Assert.Null(graphA.GetRequiredService<SessionClientOptions>().DefaultModel)
    }

[<Fact>]
let ``issue395 duplicate bindings fail without published partial contexts`` () =
    task {
        let tenant = TenantId.Create "tenant-a"
        use execution = executionProvider tenant

        let first =
            SessionHostBinding(tenant, Func<IServiceProvider, IServiceProvider>(fun _ -> execution))

        let second =
            SessionHostBinding(tenant, Func<IServiceProvider, IServiceProvider>(fun _ -> execution))

        let services = ServiceCollection()

        services.AddLegate().AddLegateSessionBinding(first).AddLegateSessionBinding(second)
        |> ignore

        use node = services.BuildServiceProvider()
        let contexts = node.GetRequiredService<ISessionHostContexts>()

        let! _ =
            Assert.ThrowsAsync<InvalidOperationException>(fun () -> contexts.InitializeAsync CancellationToken.None)

        Assert.Empty contexts.All

        Assert.Throws<InvalidOperationException>(fun () -> first.Client |> ignore)
        |> ignore
    }

[<Fact>]
let ``issue395 receiving only local node eagerly serves authorized contexts and refuses before activation`` () =
    task {
        let a = TenantId.Create "tenant-a"
        let b = TenantId.Create "tenant-b"
        use providerA = executionProvider a
        use providerB = executionProvider b

        let bindingA =
            SessionHostBinding(a, Func<IServiceProvider, IServiceProvider>(fun _ -> providerA))

        let bindingB =
            SessionHostBinding(b, Func<IServiceProvider, IServiceProvider>(fun _ -> providerB))

        let services = ServiceCollection()

        services.AddLegate().AddLegateSessionBinding(bindingA).AddLegateSessionBinding(bindingB)
        |> ignore

        use node = services.BuildServiceProvider()
        let service = localService node
        do! (service :> IHostedService).StartAsync CancellationToken.None

        try
            let! original = bindingA.Client.OpenSessionAsync(AgentId.New(), null, CancellationToken.None)

            let copied =
                { original with
                    Tenant = b
                    Options = SessionOptions()
                }

            let! _ = providerB.GetRequiredService<ISessionStore>().CreateSession(b, copied, CancellationToken.None)

            let! routeA =
                (service :> ISessionResolver)
                    .ResolveSessionAsync(SessionAddress(a, original.Id), CancellationToken.None)

            let! routeB =
                (service :> ISessionResolver)
                    .ResolveSessionAsync(SessionAddress(b, original.Id), CancellationToken.None)

            Assert.NotEqual(routeA, routeB)

            let! beforeA =
                providerA.GetRequiredService<ISessionStore>().ReadPendingInbox(a, original.Id, CancellationToken.None)

            let! beforeB =
                providerB.GetRequiredService<ISessionStore>().ReadPendingInbox(b, original.Id, CancellationToken.None)

            let unknown = SessionAddress(TenantId.Create "unavailable", original.Id)

            let! error =
                Assert.ThrowsAsync<SessionScopeRejectedException>(fun () ->
                    (service :> ISessionResolver).ResolveSessionAsync(unknown, CancellationToken.None) :> Task)

            Assert.Equal(SessionScopeRejectionReason.ScopeUnavailable, error.Reason)

            let! afterA =
                providerA.GetRequiredService<ISessionStore>().ReadPendingInbox(a, original.Id, CancellationToken.None)

            let! afterB =
                providerB.GetRequiredService<ISessionStore>().ReadPendingInbox(b, original.Id, CancellationToken.None)

            Assert.Equal(beforeA.Count, afterA.Count)
            Assert.Equal(beforeB.Count, afterB.Count)
        finally
            (service :> IHostedService).StopAsync(CancellationToken.None).GetAwaiter().GetResult()
    }
