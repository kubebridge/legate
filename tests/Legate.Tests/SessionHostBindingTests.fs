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
open Xunit

let private executionProvider tenant =
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

    services.BuildServiceProvider(ServiceProviderOptions(ValidateScopes = true))

let private localService (provider: IServiceProvider) =
    provider.GetServices<IHostedService>()
    |> Seq.pick (function
        | :? LocalActorSystemService as local -> Some local
        | _ -> None)

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
