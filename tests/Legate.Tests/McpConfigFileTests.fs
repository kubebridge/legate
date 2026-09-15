// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.McpConfigFileTests

open System
open System.Collections.Generic
open System.IO
open System.Text
open FsUnit.Xunit
open Legate
open Legate.Mcp
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Options
open Xunit

// Temp files only, never a live server: every loader case writes one
// mcp.json to the temp directory, loads it, and deletes it. Environment
// placeholders use suite-unique variable names restored after each read,
// so parallel test collections cannot observe each other.

// Unchecked.defaultof<string> rather than a bare null literal: under
// Nullable=enable the literal trips F# nullness checking on string-typed
// parameters. Deliberate: this is the null string value for rejection cases.
let nullString = Unchecked.defaultof<string>

// ──────────────────────────
// Helpers

/// Writes JSON to a unique temp file for one load.
let private writeTempFile (json: string) : string =
    let path =
        Path.Combine(Path.GetTempPath(), $"legate-mcp-config-{Guid.NewGuid():N}.json")

    File.WriteAllText(path, json, Encoding.UTF8)
    path

/// Loads JSON through a temp file, deleting the file afterwards.
let private loadJson (json: string) : McpServerOptions list =
    let path = writeTempFile json

    try
        McpConfigFile.loadServers path |> Seq.toList
    finally
        File.Delete(path) |> ignore

/// Loads JSON expecting an InvalidOperationException, returned for
/// message assertions.
let private loadThrows (json: string) : InvalidOperationException =
    let path = writeTempFile json

    try
        Assert.Throws<InvalidOperationException>(Action(fun () -> McpConfigFile.loadServers path |> ignore))
    finally
        File.Delete(path) |> ignore

/// Runs the action with one process environment variable set (null
/// deletes), restoring the prior value afterwards.
let private withEnv (name: string) (value: string | null) (action: unit -> 'T) : 'T =
    let prior = Environment.GetEnvironmentVariable(name)

    try
        Environment.SetEnvironmentVariable(name, value)
        action ()
    finally
        Environment.SetEnvironmentVariable(name, prior)

/// Builds the application configuration from in-memory pairs.
let private buildConfiguration (pairs: (string * string) seq) : IConfiguration =
    let keyValues =
        pairs
        |> Seq.map (fun (key, value) -> KeyValuePair<string, string>(key, value))
        |> Seq.toArray

    ConfigurationBuilder().AddInMemoryCollection(keyValues).Build() :> IConfiguration

/// Counts the registered IToolSource descriptors without resolving them,
/// so no server process or socket ever starts.
let private toolSourceCount (services: IServiceCollection) : int =
    services
    |> Seq.filter (fun descriptor -> descriptor.ServiceType = typeof<IToolSource>)
    |> Seq.length

// ──────────────────────────
// Mapping

[<Fact>]
let ``Stdio entry maps command args and env`` () =
    let servers =
        loadJson
            """{ "mcpServers": { "files": {
                "command": "npx",
                "args": ["-y", "@modelcontextprotocol/server-filesystem", "/tmp"],
                "env": { "HOME": "/home/agent" } } } }"""

    servers.Length |> should equal 1
    let server = servers[0]
    server.Name |> should equal "files"
    server.Command |> should equal "npx"

    server.Arguments
    |> Seq.toList
    |> should
        equal
        [
            "-y"
            "@modelcontextprotocol/server-filesystem"
            "/tmp"
        ]

    server.EnvironmentVariables["HOME"] |> should equal "/home/agent"
    server.IsStdio |> should equal true
    server.IsHttp |> should equal false
    server.Validate() |> should equal null

[<Fact>]
let ``Http entry maps url and headers`` () =
    let servers =
        loadJson
            """{ "mcpServers": { "remote": {
                "url": "http://127.0.0.1:8080/mcp",
                "headers": { "Authorization": "Bearer file-token" } } } }"""

    servers.Length |> should equal 1
    let server = servers[0]
    server.Name |> should equal "remote"
    server.Url |> should equal "http://127.0.0.1:8080/mcp"
    server.Headers["Authorization"] |> should equal "Bearer file-token"
    server.IsHttp |> should equal true
    server.IsStdio |> should equal false
    server.Validate() |> should equal null

[<Fact>]
let ``Transport is inferred by command versus url presence`` () =
    let servers =
        loadJson
            """{ "mcpServers": {
                "local": { "command": "npx" },
                "remote": { "url": "http://127.0.0.1:8080/mcp" } } }"""

    servers.Length |> should equal 2
    let local = servers |> List.find (fun server -> server.Name = "local")
    local.IsStdio |> should equal true
    local.IsHttp |> should equal false
    let remote = servers |> List.find (fun server -> server.Name = "remote")
    remote.IsHttp |> should equal true
    remote.IsStdio |> should equal false

// ──────────────────────────
// Placeholder expansion

[<Fact>]
let ``Placeholder expands in command args env url and headers`` () =
    withEnv "LEGATE_MCP_TEST_EXPAND_80" "expanded-value" (fun () ->
        let servers =
            loadJson
                """{ "mcpServers": {
                    "stdio": {
                        "command": "run-${LEGATE_MCP_TEST_EXPAND_80}",
                        "args": ["--token", "${LEGATE_MCP_TEST_EXPAND_80}"],
                        "env": { "TOKEN": "${LEGATE_MCP_TEST_EXPAND_80}" } },
                    "http": {
                        "url": "http://127.0.0.1:8080/${LEGATE_MCP_TEST_EXPAND_80}",
                        "headers": { "Authorization": "Bearer ${LEGATE_MCP_TEST_EXPAND_80}" } } } }"""

        servers.Length |> should equal 2
        let stdio = servers |> List.find (fun server -> server.Name = "stdio")
        stdio.Command |> should equal "run-expanded-value"
        stdio.Arguments |> Seq.toList |> should equal [ "--token"; "expanded-value" ]
        stdio.EnvironmentVariables["TOKEN"] |> should equal "expanded-value"

        let http = servers |> List.find (fun server -> server.Name = "http")
        http.Url |> should equal "http://127.0.0.1:8080/expanded-value"
        http.Headers["Authorization"] |> should equal "Bearer expanded-value")

[<Fact>]
let ``Unset placeholder fails naming the server key and variable`` () =
    Environment.SetEnvironmentVariable("LEGATE_MCP_TEST_MISSING_80", null)

    let ex =
        loadThrows """{ "mcpServers": { "worker": { "command": "run-${LEGATE_MCP_TEST_MISSING_80}" } } }"""

    ex.Message.Contains("worker") |> should equal true
    ex.Message.Contains("LEGATE_MCP_TEST_MISSING_80") |> should equal true

[<Fact>]
let ``Empty placeholder fails naming the server key and variable`` () =
    withEnv "LEGATE_MCP_TEST_EMPTY_80" "" (fun () ->
        let ex =
            loadThrows
                """{ "mcpServers": { "worker": { "url": "http://127.0.0.1:8080/${LEGATE_MCP_TEST_EMPTY_80}" } } }"""

        ex.Message.Contains("worker") |> should equal true
        ex.Message.Contains("LEGATE_MCP_TEST_EMPTY_80") |> should equal true)

// ──────────────────────────
// Invalid entries

[<Fact>]
let ``Entry with both transports fails naming the key`` () =
    let ex =
        loadThrows
            """{ "mcpServers": { "both": {
                "command": "npx",
                "url": "http://127.0.0.1:8080/mcp" } } }"""

    ex.Message.Contains("both") |> should equal true

[<Fact>]
let ``Entry with neither transport fails naming the key`` () =
    let ex = loadThrows """{ "mcpServers": { "neither": {} } } """
    ex.Message.Contains("neither") |> should equal true

[<Fact>]
let ``Entry with a non-absolute url fails naming the key`` () =
    let ex = loadThrows """{ "mcpServers": { "remote": { "url": "not-a-uri" } } }"""

    ex.Message.Contains("remote") |> should equal true

[<Fact>]
let ``Entry with a non-string command fails naming the key`` () =
    let ex = loadThrows """{ "mcpServers": { "broken": { "command": 42 } } }"""

    ex.Message.Contains("broken") |> should equal true

[<Fact>]
let ``Unknown transport marker without command or url fails naming the key`` () =
    let ex = loadThrows """{ "mcpServers": { "legacy": { "type": "sse" } } }"""

    ex.Message.Contains("legacy") |> should equal true

[<Fact>]
let ``Inert type and cwd fields never select a transport`` () =
    let servers =
        loadJson
            """{ "mcpServers": {
                "typed": { "type": "stdio", "command": "npx", "cwd": "/tmp" } } }"""

    servers.Length |> should equal 1
    servers[0].IsStdio |> should equal true
    servers[0].Validate() |> should equal null

// ──────────────────────────
// File errors

[<Fact>]
let ``Missing file fails naming the path`` () =
    let path =
        Path.Combine(Path.GetTempPath(), $"legate-mcp-absent-{Guid.NewGuid():N}.json")

    let ex =
        Assert.Throws<InvalidOperationException>(Action(fun () -> McpConfigFile.loadServers path |> ignore))

    ex.Message.Contains(path) |> should equal true

[<Fact>]
let ``Malformed JSON fails naming the path`` () =
    let path = writeTempFile "{not json"

    try
        let ex =
            Assert.Throws<InvalidOperationException>(Action(fun () -> McpConfigFile.loadServers path |> ignore))

        ex.Message.Contains(path) |> should equal true
    finally
        File.Delete(path) |> ignore

[<Fact>]
let ``Missing mcpServers map fails naming the requirement`` () =
    let ex = loadThrows """{ "servers": {} }"""
    ex.Message.Contains("mcpServers") |> should equal true

// ──────────────────────────
// Builder extension

[<Fact>]
let ``AddMcpServersFromConfig rejects a null path`` () =
    let builder = LegateBuilder(ServiceCollection() :> IServiceCollection)

    (fun () -> builder.Tools.AddMcpServersFromConfig(nullString) |> ignore)
    |> should throw typeof<ArgumentNullException>

[<Fact>]
let ``AddMcpServersFromConfig rejects an empty path`` () =
    let builder = LegateBuilder(ServiceCollection() :> IServiceCollection)

    (fun () -> builder.Tools.AddMcpServersFromConfig("") |> ignore)
    |> should throw typeof<ArgumentException>

[<Fact>]
let ``AddMcpServersFromConfig appends file servers onto section servers`` () =
    let configuration =
        buildConfiguration
            [
                "Legate:Tools:Mcp:Servers:0:Name", "alpha"
                "Legate:Tools:Mcp:Servers:0:Command", "npx"
            ]

    let services = ServiceCollection() :> IServiceCollection
    let builder = LegateBuilder(services)
    builder.AddMcp(configuration) |> ignore

    let path =
        writeTempFile
            """{ "mcpServers": { "beta": {
                "url": "http://127.0.0.1:8081/mcp",
                "headers": { "Authorization": "Bearer file-token" } } } }"""

    try
        builder.Tools.AddMcpServersFromConfig(path) |> ignore
        use provider = services.BuildServiceProvider()
        let options = provider.GetRequiredService<IOptions<McpOptions>>().Value
        options.Servers.Count |> should equal 2
        options.Servers[0].Name |> should equal "alpha"
        options.Servers[1].Name |> should equal "beta"
        options.Servers[1].Url |> should equal "http://127.0.0.1:8081/mcp"
        options.Servers[1].Headers["Authorization"] |> should equal "Bearer file-token"
        options.Validate() |> should equal null
        toolSourceCount services |> should equal 1
    finally
        File.Delete(path) |> ignore

[<Fact>]
let ``AddMcpServersFromConfig alone registers options and the source`` () =
    let services = ServiceCollection() :> IServiceCollection
    let builder = LegateBuilder(services)

    let path =
        writeTempFile """{ "mcpServers": { "solo": { "command": "npx", "args": ["--yes"] } } }"""

    try
        builder.Tools.AddMcpServersFromConfig(path) |> ignore
        use provider = services.BuildServiceProvider()
        let options = provider.GetRequiredService<IOptions<McpOptions>>().Value
        options.Servers.Count |> should equal 1
        options.Servers[0].Name |> should equal "solo"
        options.Servers[0].IsStdio |> should equal true
        options.Validate() |> should equal null
        toolSourceCount services |> should equal 1
    finally
        File.Delete(path) |> ignore
