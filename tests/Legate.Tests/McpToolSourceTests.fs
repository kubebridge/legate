// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.McpToolSourceTests

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Legate.Mcp
open Microsoft.Extensions.AI
open Xunit

// Scripted transports only: no subprocess, no socket. The scripted
// connector answers per server from memory and records connects, so lazy
// start, reuse, and degrade-to-zero are verified without live servers.

// ──────────────────────────
// Scripted transport

/// One scripted server session: scripted tools, counted lists and calls,
/// and an optional list failure.
type internal ScriptedSession(serverName: string, tools: McpDiscovery.McpDiscoveredTool list, failList: bool) =

    let mutable lists = 0
    let mutable calls: string list = []
    let mutable disposed = false

    interface IMcpServerSession with
        member _.ServerName: string = serverName

        member _.ListToolsAsync
            (_cancellationToken: CancellationToken)
            : Task<IReadOnlyList<McpDiscovery.McpDiscoveredTool>> =
            task {
                lists <- lists + 1

                if failList then
                    raise (InvalidOperationException($"list failed for {serverName}"))

                let listed = ResizeArray<McpDiscovery.McpDiscoveredTool>()

                for tool in tools do
                    listed.Add(tool)

                return listed :> IReadOnlyList<McpDiscovery.McpDiscoveredTool>
            }

        member _.CallToolAsync
            (toolName: string, _arguments: IReadOnlyDictionary<string, obj>, _cancellationToken: CancellationToken)
            : Task<string> =
            task {
                calls <- toolName :: calls
                return $"called:{toolName}"
            }

    interface IAsyncDisposable with
        member _.DisposeAsync() : ValueTask =
            disposed <- true
            ValueTask.CompletedTask

    /// How many times the source listed through this session.
    member _.Lists: int = lists

    /// The tool names called through this session, most recent first.
    member _.Calls: string list = calls

    /// Whether StopAsync disposed this session.
    member _.Disposed: bool = disposed

/// A scripted connector: connects from the scripted sessions, records
/// connects per server, and fails connects for the named servers.
type internal ScriptedConnector(sessions: Map<string, ScriptedSession>, failConnect: Set<string>) =

    let connects = Dictionary<string, int>(StringComparer.Ordinal)

    interface IMcpServerConnector with
        member _.ConnectAsync
            (server: McpServerOptions, _cancellationToken: CancellationToken)
            : Task<IMcpServerSession> =
            task {
                // Server names are always set in tests; unbox carries that
                // invariant past the nullness analysis.
                let name = unbox<string> (box server.Name)
                let mutable count = 0

                if connects.TryGetValue(name, &count) then
                    connects[name] <- count + 1
                else
                    connects[name] <- 1

                if failConnect.Contains(name) then
                    raise (InvalidOperationException($"connect failed for {name}"))

                return sessions[name] :> IMcpServerSession
            }

    /// Connects per server name.
    member _.Connects: Map<string, int> =
        connects |> Seq.map (fun pair -> pair.Key, pair.Value) |> Map.ofSeq

/// A degrade sink capturing every reason in order. The source hands the
/// sink the same reason the production logger logs, so asserting the sink
/// asserts the logged reason.
let private degradeSink (messages: ResizeArray<string>) : Action<string | null> =
    Action<string | null>(fun reason ->
        match box reason with
        | :? string as text -> messages.Add(text)
        | _ -> messages.Add("<null>"))

// ──────────────────────────
// Helpers

/// One discovered tool on the named server.
let private discovered (server: string) (tool: string) (destructive: bool) : McpDiscovery.McpDiscoveredTool =
    {
        ServerName = server
        ToolName = tool
        Title = null
        Description = $"Tool {tool} on {server}."
        InputSchema = McpDiscovery.DefaultInputSchema
        OutputSchema = None
        DestructiveHint = destructive
        ReadOnlyHint = None
        IdempotentHint = None
        OpenWorldHint = None
        AnnotationTitle = null
    }

/// One stdio server option.
let private stdioServer (name: string) : McpServerOptions =
    McpServerOptions(Name = name, Command = "npx")

/// One HTTP server option.
let private httpServer (name: string) : McpServerOptions =
    McpServerOptions(Name = name, Url = "http://127.0.0.1:8080/mcp")

/// The options over the given servers with the default Fail policy.
let private optionsFor (servers: McpServerOptions list) : McpOptions =
    let options = McpOptions()

    for server in servers do
        options.Servers.Add(server)

    options

/// The common resolution context.
let private sampleContext () : ToolSourceContext =
    {
        Tenant = TenantId.Create "acme"
        AgentId = AgentId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"
        SessionId = SessionId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"
    }

/// Builds a source over one scripted session per server, capturing
/// degrade reasons into messages.
let private sourceFor
    (options: McpOptions)
    (messages: ResizeArray<string>)
    (sessions: (string * McpDiscovery.McpDiscoveredTool list * bool) list)
    (failConnect: Set<string>)
    : McpToolSource * Map<string, ScriptedSession> * ScriptedConnector =
    let scripted =
        sessions
        |> List.map (fun (name, tools, failList) -> name, ScriptedSession(name, tools, failList))
        |> Map.ofList

    let connector = ScriptedConnector(scripted, failConnect)

    McpToolSource(options, degradeSink messages, connector :> IMcpServerConnector), scripted, connector

/// Resolves tools synchronously for tests.
let private resolve (source: McpToolSource) : IReadOnlyList<AITool> =
    (source :> IToolSource).GetTools(sampleContext ()).GetAwaiter().GetResult()

/// The tool names in resolution order.
let private namesOf (tools: IReadOnlyList<AITool>) : string list =
    tools |> Seq.map (fun tool -> tool.Name) |> Seq.toList

// ──────────────────────────
// Lazy start, reuse, degrade

[<Fact>]
let ``Starts lazily on first GetTools and reuses stdio and HTTP sessions`` () =
    let messages = ResizeArray<string>()

    let source, _scripted, connector =
        sourceFor
            (optionsFor
                [
                    stdioServer "alpha"
                    httpServer "beta"
                ])
            messages
            [
                "alpha", [ discovered "alpha" "read" false ], false
                "beta", [ discovered "beta" "read" false ], false
            ]
            Set.empty

    connector.Connects.Count |> should equal 0

    let first = resolve source
    namesOf first |> should equal [ "alpha_read"; "beta_read" ]

    let second = resolve source
    namesOf second |> should equal [ "alpha_read"; "beta_read" ]

    // One connect per server across both resolutions: sessions are reused.
    connector.Connects |> Map.toList |> should equal [ "alpha", 1; "beta", 1 ]
    source.Failure |> should equal null
    messages.Count |> should equal 0

[<Fact>]
let ``StartAsync connects once and GetTools reuses`` () =
    let messages = ResizeArray<string>()

    let source, _scripted, connector =
        sourceFor
            (optionsFor [ stdioServer "alpha" ])
            messages
            [
                "alpha", [ discovered "alpha" "read" false ], false
            ]
            Set.empty

    (source :> IToolSourceLifecycle).StartAsync(CancellationToken.None).GetAwaiter().GetResult()
    resolve source |> namesOf |> should equal [ "alpha_read" ]
    resolve source |> namesOf |> should equal [ "alpha_read" ]
    connector.Connects |> Map.toList |> should equal [ "alpha", 1 ]

[<Fact>]
let ``Degrades to zero tools with a logged reason when a server fails to start`` () =
    let messages = ResizeArray<string>()

    let source, _scripted, connector =
        sourceFor
            (optionsFor
                [
                    stdioServer "alpha"
                    httpServer "beta"
                ])
            messages
            [
                "alpha", [ discovered "alpha" "read" false ], false
            ]
            (Set.ofList [ "beta" ])

    let tools = resolve source
    tools |> should not' (be null)
    tools.Count |> should equal 0

    // The reason names the failed server.
    source.Failure |> should not' (equal null)

    match box source.Failure with
    | :? string as reason -> reason.Contains("beta") |> should equal true
    | _ -> Assert.Fail("Failure must carry the reason.") |> ignore

    messages
    |> Seq.exists (fun message -> message.Contains("beta"))
    |> should equal true

    // No retry on the next resolution: still zero tools, same connects.
    (resolve source).Count |> should equal 0
    connector.Connects |> Map.toList |> should equal [ "alpha", 1; "beta", 1 ]

[<Fact>]
let ``Degrades to zero tools with a logged reason when listing fails`` () =
    let messages = ResizeArray<string>()

    let source, _scripted, _connector =
        sourceFor
            (optionsFor [ stdioServer "alpha" ])
            messages
            [
                "alpha", [ discovered "alpha" "read" false ], true
            ]
            Set.empty

    (resolve source).Count |> should equal 0

    messages
    |> Seq.exists (fun message -> message.Contains("alpha"))
    |> should equal true

[<Fact>]
let ``Invalid options degrade without connecting`` () =
    let messages = ResizeArray<string>()
    let options = optionsFor [ McpServerOptions(Name = "neither") ]

    let source, _scripted, connector =
        sourceFor options messages [ "neither", [], false ] Set.empty

    (resolve source).Count |> should equal 0
    connector.Connects.Count |> should equal 0
    messages.Count |> should equal 1

// ──────────────────────────
// Naming and collisions

[<Fact>]
let ``Fail is the default collision policy`` () =
    let messages = ResizeArray<string>()

    // "s x"/"t" and "s"/"x_t" sanitize to the same s_x_t.
    let source, _scripted, _connector =
        sourceFor
            (optionsFor [ stdioServer "s x"; stdioServer "s" ])
            messages
            [
                "s x", [ discovered "s x" "t" false ], false
                "s", [ discovered "s" "x_t" false ], false
            ]
            Set.empty

    (resolve source).Count |> should equal 0

    messages
    |> Seq.exists (fun message -> message.Contains("s_x_t"))
    |> should equal true

[<Fact>]
let ``Suffix renames collisions deterministically`` () =
    let messages = ResizeArray<string>()
    let options = optionsFor [ stdioServer "s x"; stdioServer "s" ]
    options.CollisionPolicy <- McpNameCollisionPolicy.Suffix

    let source, _scripted, _connector =
        sourceFor
            options
            messages
            [
                "s x", [ discovered "s x" "t" false ], false
                "s", [ discovered "s" "x_t" false ], false
            ]
            Set.empty

    resolve source |> namesOf |> should equal [ "s_x_t"; "s_x_t_2" ]
    messages.Count |> should equal 0

// ──────────────────────────
// Projection and lifecycle

[<Fact>]
let ``Projected tools invoke through the owning session`` () =
    let messages = ResizeArray<string>()

    let source, scripted, _connector =
        sourceFor
            (optionsFor [ stdioServer "alpha" ])
            messages
            [
                "alpha", [ discovered "alpha" "read" false ], false
            ]
            Set.empty

    let tools = resolve source
    tools.Count |> should equal 1

    let result =
        (tools[0] :?> AIFunction).InvokeAsync(AIFunctionArguments(), CancellationToken.None).GetAwaiter().GetResult()

    match box result with
    | :? string as text -> text |> should equal "called:read"
    | _ -> Assert.Fail("Invoke must return the session answer.") |> ignore

    scripted["alpha"].Calls |> should equal [ "read" ]
    scripted["alpha"].Lists |> should equal 1

[<Fact>]
let ``StopAsync disposes sessions`` () =
    let messages = ResizeArray<string>()

    let source, scripted, _connector =
        sourceFor
            (optionsFor [ stdioServer "alpha" ])
            messages
            [
                "alpha", [ discovered "alpha" "read" false ], false
            ]
            Set.empty

    resolve source |> namesOf |> should equal [ "alpha_read" ]
    (source :> IToolSourceLifecycle).StopAsync(CancellationToken.None).GetAwaiter().GetResult()
    scripted["alpha"].Disposed |> should equal true
