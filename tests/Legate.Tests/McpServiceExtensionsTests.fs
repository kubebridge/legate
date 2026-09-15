// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.McpServiceExtensionsTests

open System
open System.Collections.Generic
open FsUnit.Xunit
open Legate
open Legate.Mcp
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Logging
open Microsoft.Extensions.Logging.Abstractions
open Microsoft.Extensions.Options
open Xunit

/// Builds the application configuration from in-memory pairs.
let private buildConfiguration (pairs: (string * string) seq) : IConfiguration =
    let keyValues =
        pairs
        |> Seq.map (fun (key, value) -> KeyValuePair<string, string>(key, value))
        |> Seq.toArray

    ConfigurationBuilder().AddInMemoryCollection(keyValues).Build() :> IConfiguration

/// Registers Legate with the F# fun overload, running AddMcp inside.
let private addLegateWithMcp
    (services: IServiceCollection)
    (configuration: IConfiguration)
    (configure: Action<McpOptions> | null)
    =
    let registration (builder: LegateBuilder) : unit =
        match configure with
        | null -> builder.AddMcp(configuration) |> ignore
        | callback -> builder.AddMcp(configuration, callback) |> ignore

    LegateServiceCollectionExtensions.AddLegate(services, ?configure = Some registration)
    |> ignore

[<Fact>]
let ``AddMcp binds Legate Tools Mcp and registers the source`` () =
    let configuration =
        buildConfiguration
            [
                "Legate:Tools:Mcp:CollisionPolicy", "Suffix"
                "Legate:Tools:Mcp:Servers:0:Name", "alpha"
                "Legate:Tools:Mcp:Servers:0:Command", "npx"
                "Legate:Tools:Mcp:Servers:1:Name", "beta"
                "Legate:Tools:Mcp:Servers:1:Url", "http://127.0.0.1:8080/mcp"
            ]

    let services = ServiceCollection()
    // AddLegate does not register logging (hosts do); the source's
    // ILogger comes from a NullLogger here.
    services.AddSingleton<ILogger<McpToolSource>>(NullLogger<McpToolSource>.Instance)
    |> ignore

    addLegateWithMcp services configuration null
    use provider = services.BuildServiceProvider()

    let options = provider.GetRequiredService<IOptions<McpOptions>>().Value
    options.CollisionPolicy |> should equal McpNameCollisionPolicy.Suffix
    options.Servers.Count |> should equal 2
    options.Servers[0].Name |> should equal "alpha"
    options.Servers[1].Url |> should equal "http://127.0.0.1:8080/mcp"
    options.Validate() |> should equal null

    provider.GetServices<IToolSource>()
    |> Seq.exists (fun source -> source :? McpToolSource)
    |> should equal true

[<Fact>]
let ``AddMcp applies the configure callback`` () =
    let configuration =
        buildConfiguration
            [
                "Legate:Tools:Mcp:Servers:0:Name", "alpha"
                "Legate:Tools:Mcp:Servers:0:Command", "npx"
            ]

    let services = ServiceCollection()

    services.AddSingleton<ILogger<McpToolSource>>(NullLogger<McpToolSource>.Instance)
    |> ignore

    addLegateWithMcp
        services
        configuration
        (Action<McpOptions>(fun options -> options.CollisionPolicy <- McpNameCollisionPolicy.Suffix))

    use provider = services.BuildServiceProvider()

    provider.GetRequiredService<IOptions<McpOptions>>().Value.CollisionPolicy
    |> should equal McpNameCollisionPolicy.Suffix

[<Fact>]
let ``AddMcp rejects invalid options`` () =
    let configuration =
        buildConfiguration
            [
                "Legate:Tools:Mcp:Servers:0:Name", "neither"
            ]

    let services = ServiceCollection()

    (fun () -> addLegateWithMcp services configuration null |> ignore)
    |> should throw typeof<InvalidOperationException>
