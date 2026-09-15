// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.CustomToolSourceTests

open System
open System.Collections.Generic
open System.Net
open System.Text
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Microsoft.Extensions.AI
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Logging
open Xunit

// Tests for the custom tool source (issue 74, task 4): per-session wiring
// of enabled AgentCustomTool rows to signed functions, first-claimant-wins
// dedupe with built-ins keeping first claim, schema fallback reporting,
// degraded-conditions behaviour, and builder registration order.

// ───────────────────────────────────────────────────────────────────────────
// Doubles

/// A canned resolver seam answering loopback for every host.
type private FakeResolver() =

    interface IHostAddressResolver with
        member _.ResolveAsync(_, _) =
            Task.FromResult([| IPAddress.Loopback |] :> IReadOnlyList<IPAddress>)

/// A scripted custom tool store: lists the canned rows or fails on demand.
type private ScriptedToolStore(rows: AgentCustomTool list, fail: bool) =

    interface IAgentCustomToolStore with
        member _.ListCustomTools(_, _, _) =
            if fail then
                Task.FromException<IReadOnlyList<AgentCustomTool>>(InvalidOperationException("store down"))
            else
                Task.FromResult(rows :> IReadOnlyList<AgentCustomTool>)

        member _.GetCustomTool(_, _, _, _) =
            Task.FromResult(Unchecked.defaultof<AgentCustomTool>)

        member _.UpsertCustomTool(_, _, tool, _) = Task.FromResult(tool)
        member _.DeleteCustomTool(_, _, _, _) = Task.FromResult(false)

/// A recording logger capturing every formatted message with its level.
type private RecordingLogger() =
    let messages = ResizeArray<string * string>()

    let scope =
        { new IDisposable with
            member _.Dispose() = ()
        }

    interface ILogger with
        member _.BeginScope<'TState when 'TState: not null>(_: 'TState) : IDisposable = scope

        member _.IsEnabled(_) = true

        member _.Log<'TState>
            (logLevel: LogLevel, _eventId: EventId, state: 'TState, ex: exn, formatter: Func<'TState, exn, string>)
            : unit =
            messages.Add((logLevel.ToString(), formatter.Invoke(state, ex)))

    /// Every captured message, oldest first.
    member _.Messages: (string * string) list = messages |> List.ofSeq

    /// The captured Warning texts, oldest first.
    member this.Warnings: string list =
        this.Messages
        |> List.choose (fun (level, text) -> if level = "Warning" then Some text else None)

// ───────────────────────────────────────────────────────────────────────────
// Helpers

/// The guard options the tests run under: loopback explicitly allowed.
let private allowLocal () =
    let options = SsrfGuardOptions()
    options.AllowList.Add("127.0.0.1")
    options

/// Builds one enabled tool row with the given name and schema.
let private row (name: string) (schema: string | null) : AgentCustomTool =
    {
        Tenant = TenantId.Default
        AgentId = AgentId.New()
        Name = name
        Description = "Does a thing."
        Endpoint = Uri("http://127.0.0.1:9/tool")
        InputSchema = schema
        Headers = null
        SigningSecret = Encoding.UTF8.GetBytes("secret")
        Enabled = true
        RowVersion = 1UL
        CreatedAt = DateTimeOffset.UtcNow
        UpdatedAt = DateTimeOffset.UtcNow
    }

/// Builds the source under test over the canned rows.
let private makeSource (rows: AgentCustomTool list) (fail: bool) (logger: ILogger) =
    CustomToolSource(
        ScriptedToolStore(rows, fail) :> IAgentCustomToolStore,
        FakeResolver() :> IHostAddressResolver,
        allowLocal (),
        null,
        TimeSpan.FromSeconds(10.0),
        logger
    )

let private context () : ToolSourceContext =
    {
        Tenant = TenantId.Default
        AgentId = AgentId.New()
        SessionId = SessionId.New()
    }

/// Resolves the source's tools for one session.
let private getTools (source: CustomToolSource) : IReadOnlyList<AITool> =
    ((source :> IToolSource).GetTools(context ())).GetAwaiter().GetResult()

// ───────────────────────────────────────────────────────────────────────────
// Per-session wiring

[<Fact>]
let ``Enabled tools build one function each with valid names`` () =
    let logger = RecordingLogger()

    let tools =
        getTools (makeSource [ row "alpha" null; row "beta" null ] false (logger :> ILogger))

    tools.Count |> should equal 2
    let names = tools |> Seq.map (fun tool -> tool.Name) |> Set.ofSeq
    names |> should equal (Set.ofList [ "alpha"; "beta" ])

    for tool in tools do
        ToolNameRules.Validate(tool.Name) |> ignore

[<Fact>]
let ``A valid schema serves on the function`` () =
    let logger = RecordingLogger()

    let tools =
        getTools (
            makeSource
                [
                    row "alpha" """{"type":"object","properties":{"q":{"type":"string"}}}"""
                ]
                false
                (logger :> ILogger)
        )

    tools.Count |> should equal 1

    (tools[0] :?> AIFunction).JsonSchema.GetProperty("properties").GetProperty("q").GetProperty("type").GetString()
    |> should equal "string"

[<Fact>]
let ``An invalid schema falls back with the diagnostic and a reason-only warning`` () =
    let logger = RecordingLogger()
    let schemaMarker = "schema-marker-2b7d"
    let secretMarker = "secret-marker-8e1a"

    let bad =
        { row "alpha" ("{invalid " + schemaMarker) with
            SigningSecret = Encoding.UTF8.GetBytes(secretMarker)
        }

    let tools = getTools (makeSource [ bad ] false (logger :> ILogger))

    tools.Count |> should equal 1
    let fn = tools[0] :?> AIFunction
    fn.JsonSchema.GetRawText() |> should equal CustomToolSchema.PermissiveJson

    fn.Description.Contains(CustomToolSchema.FallbackDiagnostic)
    |> should equal true

    logger.Warnings.Length |> should be (greaterThan 0)

    for warning in logger.Warnings do
        warning.Contains(schemaMarker) |> should equal false
        warning.Contains(secretMarker) |> should equal false

[<Fact>]
let ``A duplicate sanitized name keeps the first claimant`` () =
    let logger = RecordingLogger()

    let tools =
        getTools (
            makeSource
                [
                    row "my tool" null
                    row "my_tool" null
                ]
                false
                (logger :> ILogger)
        )

    tools.Count |> should equal 1
    tools[0].Name |> should equal "my_tool"
    logger.Warnings |> should haveLength 2

[<Fact>]
let ``A built-in name loses to the built-in`` () =
    let logger = RecordingLogger()
    let tools = getTools (makeSource [ row "exec" null ] false (logger :> ILogger))

    tools.Count |> should equal 0
    logger.Warnings.Length |> should equal 1
    logger.Warnings[0].Contains("built-in") |> should equal true

[<Fact>]
let ``The built-in set pins every known tool name`` () =
    // Compile-order keeps the set literal; this test fails loudly when a
    // built-in is renamed without updating the set.
    for name in
        [
            ExecTool.ToolName
            EditFileInternals.toolName
            DownloadUrlTool.ToolName
            AskUserTool.ToolName
            SkillTool.ToolName
            TaskTool.ToolName
            BuiltinSearchTools.GlobToolName
            BuiltinSearchTools.GrepToolName
            TurnLoop.AskUserToolName
            TurnLoop.SkillToolName
            TurnLoop.TaskToolName
        ] do
        Set.contains name CustomToolBuiltIns.asSet |> should equal true

[<Fact>]
let ``Unusable names drop out with a report`` () =
    let logger = RecordingLogger()

    let nullName =
        { row "placeholder" null with
            Name = Unchecked.defaultof<string>
        }

    let tools =
        getTools (makeSource [ nullName; row "" null ] false (logger :> ILogger))

    tools.Count |> should equal 0
    logger.Warnings.Length |> should equal 2

[<Fact>]
let ``Rows without an endpoint or secret drop out`` () =
    let logger = RecordingLogger()

    let noEndpoint =
        { row "no_endpoint" null with
            Endpoint = Unchecked.defaultof<Uri>
        }

    let noSecret =
        { row "no_secret" null with
            SigningSecret = [||]
        }

    let tools = getTools (makeSource [ noEndpoint; noSecret ] false (logger :> ILogger))

    tools.Count |> should equal 0
    logger.Warnings.Length |> should equal 2

[<Fact>]
let ``A failing store degrades to the empty list`` () =
    let logger = RecordingLogger()
    let tools = getTools (makeSource [ row "alpha" null ] true (logger :> ILogger))

    tools.Count |> should equal 0
    logger.Warnings.Length |> should equal 1

[<Fact>]
let ``A null context is a host bug that throws`` () =
    let logger = RecordingLogger()
    let source = makeSource [ row "alpha" null ] false (logger :> ILogger)

    (fun () ->
        ((source :> IToolSource).GetTools(Unchecked.defaultof<ToolSourceContext>)).GetAwaiter().GetResult()
        |> ignore)
    |> should throw typeof<ArgumentNullException>

// ───────────────────────────────────────────────────────────────────────────
// Builder registration

[<Fact>]
let ``AddCustomTools registers the source in the tool-source chain`` () =
    let store = ScriptedToolStore([], false) :> IAgentCustomToolStore
    let services = ServiceCollection() :> IServiceCollection
    let builder = LegateBuilder(services)
    builder.Tools.AddCustomTools(store) |> ignore

    use provider = services.BuildServiceProvider()
    let sources = provider.GetServices<IToolSource>() |> List.ofSeq

    sources.Length |> should equal 1
    (sources[0].GetType().Name.Contains("CustomToolSource")) |> should equal true

[<Fact>]
let ``The default call bound is 30 seconds`` () =
    CustomToolFunction.DefaultTimeout |> should equal (TimeSpan.FromSeconds(30.0))
