// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.ProductionSessionHostBindingTests

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
open Xunit

let private emptyChat () =
    new ScriptedChatClient(Array.empty<ScriptStep> :> IReadOnlyList<ScriptStep>) :> IChatClient

[<Fact>]
let ``production AddLegate without a binding fails with an explicit startup diagnostic`` () =
    task {
        let services = ServiceCollection()
        LegateServiceCollectionExtensions.AddLegate(services) |> ignore
        use provider = services.BuildServiceProvider()

        let validation =
            provider.GetServices<IHostedService>()
            |> Seq.pick (function
                | :? LegateStartupValidation as service -> Some(service :> IHostedService)
                | _ -> None)

        let! failure =
            Assert.ThrowsAsync<InvalidOperationException>(fun () -> validation.StartAsync(CancellationToken.None))

        Assert.Contains("IChatClient", failure.Message)
    }

[<Fact>]
let ``declared binding validates its complete execution provider at initialization`` () =
    task {
        let tenant = TenantId.Create "production-validation"
        let database = InMemoryDatabase()
        let execution = ServiceCollection()
        LegateServiceCollectionExtensions.AddLegate(execution) |> ignore
        execution.AddSingleton<ISessionStore>(InMemorySessionStore(database)) |> ignore

        execution.AddSingleton<ISessionEventStore>(InMemorySessionEventStore(database))
        |> ignore

        let options = SessionClientOptions()
        options.Tenant <- tenant
        execution.AddSingleton(options) |> ignore
        execution.AddSingleton<IChatClient>(emptyChat ()) |> ignore
        use executionProvider = execution.BuildServiceProvider()

        let binding =
            SessionHostBinding(tenant, Func<IServiceProvider, IServiceProvider>(fun _ -> executionProvider))

        let node = ServiceCollection()

        LegateServiceCollectionExtensions.AddLegate(node).AddLegateSessionBinding(binding)
        |> ignore

        use nodeProvider = node.BuildServiceProvider()
        let contexts = nodeProvider.GetRequiredService<ISessionHostContexts>()

        let! failure =
            Assert.ThrowsAsync<InvalidOperationException>(fun () -> contexts.InitializeAsync CancellationToken.None)

        Assert.Contains("IWorkspaceRuntime", failure.Message)
        Assert.Empty(contexts.All)

        Assert.Throws<InvalidOperationException>(fun () -> binding.Client |> ignore)
        |> ignore
    }

[<Fact>]
let ``scoped response owner survives current wire roundtrip`` () =
    let address = SessionAddress(TenantId.Create "wire-owner", SessionId.New())

    let response: SessionRouteResponse =
        {
            Address = address.Key
            Owner = "akka://legate@127.0.0.1:2551"
            Payload = SessionRouteAccepted
        }

    let wire = WireDtos.toWire response
    let rebuilt = WireDtos.ofWire wire :?> SessionRouteResponse
    Assert.Equal(response.Address, rebuilt.Address)
    Assert.Equal(response.Owner, rebuilt.Owner)
