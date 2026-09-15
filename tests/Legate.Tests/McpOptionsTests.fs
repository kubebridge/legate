// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.McpOptionsTests

open System
open System.Collections.Generic
open FsUnit.Xunit
open Legate.Mcp
open Microsoft.Extensions.Configuration
open Xunit

/// Builds the Legate:Tools:Mcp section from in-memory pairs.
let private buildSection (pairs: (string * string) seq) : IConfigurationSection =
    let keyValues =
        pairs
        |> Seq.map (fun (key, value) -> KeyValuePair<string, string>(key, value))
        |> Seq.toArray

    ConfigurationBuilder().AddInMemoryCollection(keyValues).Build().GetSection(McpOptions.ConfigurationSectionPath)

/// One stdio server with the given name.
let private stdioServer (name: string) : McpServerOptions =
    McpServerOptions(Name = name, Command = "npx")

/// One HTTP server with the given name.
let private httpServer (name: string) : McpServerOptions =
    McpServerOptions(Name = name, Url = "http://127.0.0.1:8080/mcp")

[<Fact>]
let ``Section path binds at Legate Tools Mcp`` () =
    McpOptions.ConfigurationSectionPath |> should equal "Legate:Tools:Mcp"

[<Fact>]
let ``Defaults fail on collisions and serve no servers`` () =
    let options = McpOptions()
    options.CollisionPolicy |> should equal McpNameCollisionPolicy.Fail
    options.Servers.Count |> should equal 0
    options.Validate() |> should equal null

[<Fact>]
let ``Validate accepts stdio and HTTP servers`` () =
    let options = McpOptions()
    options.Servers.Add(stdioServer "alpha")
    options.Servers.Add(httpServer "beta")
    options.Validate() |> should equal null

[<Fact>]
let ``Validate rejects a blank server name`` () =
    let options = McpOptions()
    options.Servers.Add(stdioServer "")
    options.Validate() |> should not' (equal null)

[<Fact>]
let ``Validate requires exactly one transport`` () =
    let neither = McpOptions()
    neither.Servers.Add(McpServerOptions(Name = "neither"))
    neither.Validate() |> should not' (equal null)

    let both = McpOptions()
    both.Servers.Add(McpServerOptions(Name = "both", Command = "npx", Url = "http://127.0.0.1:8080/mcp"))
    both.Validate() |> should not' (equal null)

[<Fact>]
let ``Validate rejects a non-absolute HTTP url`` () =
    let options = McpOptions()
    options.Servers.Add(McpServerOptions(Name = "beta", Url = "not-a-uri"))
    options.Validate() |> should not' (equal null)

[<Fact>]
let ``Validate rejects duplicate server names`` () =
    let options = McpOptions()
    options.Servers.Add(stdioServer "alpha")
    options.Servers.Add(stdioServer "alpha")
    options.Validate() |> should not' (equal null)

[<Fact>]
let ``Validate rejects an undefined collision policy`` () =
    let options = McpOptions()
    options.CollisionPolicy <- enum<McpNameCollisionPolicy> 99
    options.Validate() |> should not' (equal null)

[<Fact>]
let ``Binds servers and the suffix policy from configuration`` () =
    let section =
        buildSection
            [
                "Legate:Tools:Mcp:CollisionPolicy", "Suffix"
                "Legate:Tools:Mcp:Servers:0:Name", "alpha"
                "Legate:Tools:Mcp:Servers:0:Command", "npx"
                "Legate:Tools:Mcp:Servers:0:Arguments:0", "--yes"
                "Legate:Tools:Mcp:Servers:0:EnvironmentVariables:TOKEN", "test-token"
                "Legate:Tools:Mcp:Servers:1:Name", "beta"
                "Legate:Tools:Mcp:Servers:1:Url", "http://127.0.0.1:8080/mcp"
                "Legate:Tools:Mcp:Servers:1:Headers:Authorization", "Bearer test"
            ]

    let bound: McpOptions | null = section.Get<McpOptions>()

    match bound with
    | null -> Assert.Fail("Configuration binding returned null.") |> ignore
    | options ->
        options.CollisionPolicy |> should equal McpNameCollisionPolicy.Suffix
        options.Servers.Count |> should equal 2
        options.Servers[0].Name |> should equal "alpha"
        options.Servers[0].Command |> should equal "npx"
        options.Servers[0].Arguments |> Seq.toList |> should equal [ "--yes" ]
        options.Servers[0].EnvironmentVariables["TOKEN"] |> should equal "test-token"
        options.Servers[1].Name |> should equal "beta"
        options.Servers[1].Url |> should equal "http://127.0.0.1:8080/mcp"
        options.Servers[1].Headers["Authorization"] |> should equal "Bearer test"
        options.Validate() |> should equal null

[<Fact>]
let ``Overrides default to empty collections`` () =
    let overrides = McpServerOverrides()
    overrides.DisabledTools.Count |> should equal 0
    overrides.DescriptionOverrides.Count |> should equal 0

[<Fact>]
let ``Servers default to no overrides and still validate`` () =
    let options = McpOptions()
    options.Servers.Add(stdioServer "alpha")
    options.Servers[0].Overrides |> should equal null
    options.Validate() |> should equal null

[<Fact>]
let ``Binds overrides from the server section`` () =
    let section =
        buildSection
            [
                "Legate:Tools:Mcp:Servers:0:Name", "alpha"
                "Legate:Tools:Mcp:Servers:0:Command", "npx"
                "Legate:Tools:Mcp:Servers:0:Overrides:DisabledTools:0", "secret"
                "Legate:Tools:Mcp:Servers:0:Overrides:DescriptionOverrides:read", "Rewritten."
            ]

    let bound: McpOptions | null = section.Get<McpOptions>()

    match bound with
    | null -> Assert.Fail("Configuration binding returned null.") |> ignore
    | options ->
        options.Servers.Count |> should equal 1
        options.Servers[0].Overrides |> should not' (equal null)

        match box options.Servers[0].Overrides with
        | :? McpServerOverrides as overrides ->
            overrides.DisabledTools |> Seq.toList |> should equal [ "secret" ]
            overrides.DescriptionOverrides["read"] |> should equal "Rewritten."
        | _ -> Assert.Fail("Overrides must bind.") |> ignore

        options.Validate() |> should equal null

[<Fact>]
let ``Overrides never invalidate a server`` () =
    let options = McpOptions()
    let server = stdioServer "alpha"
    let overrides = McpServerOverrides()
    overrides.DisabledTools.Add("unknown-tool")
    overrides.DescriptionOverrides["unknown-tool"] <- "Rewritten."
    server.Overrides <- overrides
    options.Servers.Add(server)
    options.Validate() |> should equal null
