// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.SessionHostLifetimeTests

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open Legate
open Legate.Storage.InMemory
open Legate.Testing
open Microsoft.Extensions.AI
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Options
open Xunit

let private completeServices (tenant: TenantId) : IServiceCollection * ServiceProvider =
    let services = ServiceCollection()
    LegateServiceCollectionExtensions.AddLegate(services) |> ignore

    let database = InMemoryDatabase()
    services.AddSingleton<ISessionStore>(InMemorySessionStore(database)) |> ignore

    services.AddSingleton<ISessionEventStore>(InMemorySessionEventStore(database))
    |> ignore

    let options = SessionClientOptions()
    options.Tenant <- tenant
    services.AddSingleton(options) |> ignore

    services.AddSingleton<IChatClient>(new ScriptedChatClient(Array.empty<ScriptStep> :> IReadOnlyList<ScriptStep>))
    |> ignore

    services.AddSingleton<ILlmProvider>(BuilderTests.StubLlmProvider()) |> ignore

    services.AddSingleton<IWorkspaceRuntime>(BuilderTests.StubWorkspaceRuntime())
    |> ignore

    services, services.BuildServiceProvider()

let private startValidation (provider: IServiceProvider) =
    provider.GetServices<IHostedService>()
    |> Seq.pick (function
        | :? LegateStartupValidation as validation -> Some(validation :> IHostedService)
        | _ -> None)

[<Fact>]
let ``default root binding materializes once under concurrent initialization`` () : Task =
    task {
        let _, execution = completeServices TenantId.Default
        use execution = execution
        let contexts = execution.GetRequiredService<ISessionHostContexts>()
        let barrier = new Barrier(8)

        let calls =
            [|
                for _ in 1..8 do
                    yield
                        Task.Run(fun () ->
                            barrier.SignalAndWait()
                            contexts.InitializeAsync(CancellationToken.None).GetAwaiter().GetResult())
            |]

        do! Task.WhenAll calls
        Assert.Single(contexts.All) |> ignore
        Assert.Equal(TenantId.Default, contexts.DefaultTenant)
        Assert.NotNull(execution.GetRequiredService<SessionClient>())
    }

[<Fact>]
let ``failed binding never publishes partial contexts or a client`` () : Task =
    task {
        let tenant = TenantId.Create "lifetime-invalid"

        let broken = ServiceCollection()
        LegateServiceCollectionExtensions.AddLegate(broken) |> ignore
        let brokenOptions = SessionClientOptions()
        brokenOptions.Tenant <- tenant
        broken.AddSingleton(brokenOptions) |> ignore
        let brokenProvider = broken.BuildServiceProvider()
        use brokenProvider = brokenProvider

        let binding =
            SessionHostBinding(tenant, Func<IServiceProvider, IServiceProvider>(fun _ -> brokenProvider))

        let node = ServiceCollection()

        LegateServiceCollectionExtensions.AddLegate(node).AddLegateSessionBinding(binding)
        |> ignore

        use node = node.BuildServiceProvider()
        let contexts = node.GetRequiredService<ISessionHostContexts>()

        let! failure =
            Assert.ThrowsAsync<InvalidOperationException>(fun () -> contexts.InitializeAsync CancellationToken.None)

        Assert.Contains("IChatClient", failure.Message)
        Assert.Empty(contexts.All)

        Assert.Throws<InvalidOperationException>(fun () -> binding.Client |> ignore)
        |> ignore
    }

[<Fact>]
let ``a failed descriptor cannot be reused`` () : Task =
    task {
        let tenant = TenantId.Create "lifetime-reuse"
        use execution = snd (completeServices tenant)

        let binding =
            SessionHostBinding(tenant, Func<IServiceProvider, IServiceProvider>(fun _ -> execution))

        let first = ServiceCollection()

        LegateServiceCollectionExtensions.AddLegate(first).AddLegateSessionBinding(binding)
        |> ignore

        use first = first.BuildServiceProvider()
        let firstContexts = first.GetRequiredService<ISessionHostContexts>()
        do! firstContexts.InitializeAsync CancellationToken.None

        let second = ServiceCollection()

        LegateServiceCollectionExtensions.AddLegate(second).AddLegateSessionBinding(binding)
        |> ignore

        use second = second.BuildServiceProvider()
        let secondContexts = second.GetRequiredService<ISessionHostContexts>()

        let! failure =
            Assert.ThrowsAsync<InvalidOperationException>(fun () ->
                secondContexts.InitializeAsync CancellationToken.None)

        Assert.Contains("reused", failure.Message)
        Assert.Empty(secondContexts.All)
    }

[<Fact>]
let ``node subscription lifetime detaches only its enumerators and keeps the bus alive`` () =
    task {
        let database = InMemoryDatabase()
        let events = InMemorySessionEventStore(database) :> ISessionEventStore
        let options = SessionSubscriptionOptions()
        options.MaxSubscribersPerSession <- 2
        use bus = new SessionEventBus(events, options)
        use nodeLifetime = new SessionSubscriptionLifetime()
        let tenant = TenantId.Create "subscription-owner"
        let session = SessionId.New()

        let external =
            bus.Subscribe(tenant, session, 0L, CancellationToken.None).GetAsyncEnumerator()

        let owned =
            nodeLifetime.Wrap(fun token -> bus.Subscribe(tenant, session, 0L, token)).GetAsyncEnumerator()

        nodeLifetime.Close()

        let replacement =
            bus.Subscribe(tenant, session, 0L, CancellationToken.None).GetAsyncEnumerator()

        do! external.DisposeAsync().AsTask()
        do! owned.DisposeAsync().AsTask()
        do! replacement.DisposeAsync().AsTask()
    }

[<Fact>]
let ``production factory seams reject mutation`` () =
    let _, provider = completeServices TenantId.Default
    use provider = provider

    let local =
        provider.GetServices<IHostedService>()
        |> Seq.pick (function
            | :? LocalActorSystemService as service -> Some service
            | _ -> None)

    let factory = Some(fun _ _ _ -> Unchecked.defaultof<Akka.Actor.IActorRef>)

    Assert.Throws<InvalidOperationException>(fun () -> local.SessionChildFactory <- factory)
    |> ignore

[<Fact>]
let ``cancellation during local stop keeps the live actor references`` () : Task =
    task {
        let options = LegateOptions()
        options.Cluster.ShutdownGraceSeconds <- TimeSpan.FromSeconds 1.0

        let service =
            LocalActorSystemService(OptionsWrapper<LegateOptions>(options), TimeProvider.System)

        do! (service :> IHostedService).StartAsync(CancellationToken.None)
        let cancellation = new CancellationTokenSource()
        cancellation.Cancel()

        let! _ =
            Assert.ThrowsAnyAsync<OperationCanceledException>(fun () ->
                (service :> IHostedService).StopAsync(cancellation.Token))

        Assert.NotNull(service.System)
        do! (service :> IHostedService).StopAsync(CancellationToken.None)
        Assert.Null(service.System)
    }
