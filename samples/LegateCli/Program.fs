// SPDX-License-Identifier: Apache-2.0
module LegateCli.Program

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open Legate
open Legate.Llm.OpenAI
open Legate.Mcp
open Legate.Storage.InMemory
open Legate.Workspace.Process
open LegateCli.Engine
open Microsoft.Extensions.AI
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Hosting

// LegateCli host: builds the container (InMemory stores, process
// workspace over the working directory, mcp.json attach, env-configured
// providers or scripted transports), resolves the session client facade,
// starts the local actor system, and runs the REPL engine. Keys come from
// the environment through configuration binding only; nothing is printed
// or persisted.

// ──────────────────────────────────────────────────────────────────────────
// Scripted transports (no keys, no external services)

// Asks for every tool call: the scripted and live default, so every agent
// tool call meets a console approval before it runs.
type private AskAllPolicy() =
    interface IPermissionPolicy with
        member _.Evaluate(_) = PermissionVerdict.Ask

/// One scripted model step: assistant text or one tool call.
type private ScriptStep =
    | Text of string
    | ToolCall of callId: string * toolName: string

/// Minimal inline scripted chat client: serves queued steps, throws
/// NotSupportedException for streaming (the loop falls back to a single
/// GetResponseAsync call, mirroring Legate.Testing).
type private ScriptedClient(steps: Queue<ScriptStep>) =
    do ArgumentNullException.ThrowIfNull(steps)

    interface IChatClient with
        member _.GetResponseAsync(_, _, cancellationToken) =
            task {
                cancellationToken.ThrowIfCancellationRequested()

                if steps.Count = 0 then
                    return raise (InvalidOperationException("The scripted client ran out of steps."))
                else
                    match steps.Dequeue() with
                    | Text text -> return ChatResponse(ChatMessage(ChatRole.Assistant, text))
                    | ToolCall(callId, toolName) ->
                        let call =
                            FunctionCallContent(callId, toolName, Dictionary<string, obj>() :> IDictionary<string, obj>)
                            :> AIContent

                        return ChatResponse(ChatMessage(ChatRole.Assistant, ResizeArray<AIContent>([| call |])))
            }

        member _.GetStreamingResponseAsync(_, _, _) =
            raise (
                NotSupportedException(
                    "The scripted client is non-streaming: the caller falls back to GetResponseAsync."
                )
            )

        member _.GetService(_, _) = null
        member _.Dispose() = ()

/// Stub provider backing scripted mode: satisfies startup validation with
/// no key and serves the scripted client for any model.
type private StubScriptedProvider(client: IChatClient) =
    do ArgumentNullException.ThrowIfNull(client)

    interface ILlmProvider with
        member _.Id = "scripted"
        member _.DefaultModel = "scripted"

        member _.Capabilities =
            {
                Streaming = false
                Reasoning = false
                ToolCalling = true
            }

        member _.CreateChatClient(_model: ModelReference, _options: LlmProviderOptions | null) = client

/// Fixed inline tool source: the scripted echo tool for scripted mode.
type private StaticSource(tools: IReadOnlyList<AITool>) =
    do ArgumentNullException.ThrowIfNull(tools)

    interface IToolSource with
        member _.GetTools(_) = Task.FromResult(tools)

// ──────────────────────────────────────────────────────────────────────────
// Host building

/// Creates the scripted chat client with the smoke script: a permission
/// gated echo call, then the fixture echo call, then plain answers.
let private scriptedClient () : ScriptedClient =
    let steps = Queue<ScriptStep>()
    steps.Enqueue(ToolCall("call-1", "scripted-echo"))
    steps.Enqueue(Text "scripted answer one")
    steps.Enqueue(ToolCall("call-2", "fixture_echo"))
    steps.Enqueue(Text "fixture says hi")
    steps.Enqueue(Text "scripted answer two")
    new ScriptedClient(steps)

/// Creates the scripted echo tool.
let private scriptedEchoTool () : AITool =
    let method = System.Func<string>(fun () -> "scripted tool ok")

    AIFunctionFactory.Create(
        method,
        "scripted-echo",
        "Echoes a canned acknowledgement for the scripted smoke run.",
        null
    )
    :> AITool

/// Resolves the chat client for real mode: the selected provider serving
/// the selected model reference.
let private selectClient (provider: IServiceProvider) (start: CliStart) : IChatClient =
    let providers =
        provider.GetServices<ILlmProvider>()
        |> Seq.filter (fun candidate -> not (isNull (box candidate)))
        |> List.ofSeq

    let selected =
        providers
        |> List.tryFind (fun candidate ->
            String.Equals(candidate.Id, start.Provider, StringComparison.OrdinalIgnoreCase))

    match selected with
    | None ->
        let known =
            providers |> List.map (fun candidate -> candidate.Id) |> String.concat ", "

        raise (
            InvalidOperationException(
                $"No provider '{start.Provider}' is registered (known: {known}). Register it through Legate:Llm:Providers or pass --provider."
            )
        )
    | Some live ->
        let defaultModel: string =
            match box live.DefaultModel with
            | null ->
                raise (
                    InvalidOperationException(
                        $"Provider '{live.Id}' has no default model: pass --model <provider/model>."
                    )
                )
            | _ -> live.DefaultModel

        let raw =
            match start.Model with
            | null -> null
            | model when String.IsNullOrWhiteSpace model -> null
            | model -> model.Trim()

        let reference =
            match raw with
            | null when defaultModel.Contains("/") -> ModelReference.Parse(defaultModel)
            | null -> ModelReference.Parse($"{live.Id}/{defaultModel}")
            | model -> ModelReference.Parse(model)

        live.CreateChatClient(reference, null)

/// Builds the container per the start plan.
let private buildServices
    (services: IServiceCollection)
    (configuration: IConfiguration)
    (database: InMemoryDatabase)
    (start: CliStart)
    : unit =
    LegateServiceCollectionExtensions.AddLegate(
        services,
        Action<LegateBuilder>(fun builder ->
            builder.Storage.UseSessionStore(InMemorySessionStore(database)) |> ignore

            let workspaceOptions = ProcessWorkspaceRuntimeOptions()
            workspaceOptions.Root <- Environment.CurrentDirectory

            builder.Workspace.UseRuntime(ProcessWorkspaceRuntime(workspaceOptions, null, null))
            |> ignore

            if start.Scripted then
                builder.Tools.AddSource(StaticSource(ResizeArray<AITool>([| scriptedEchoTool () |])))
                |> ignore
            else
                match start.Provider.ToLowerInvariant() with
                | "anthropic" -> builder.Llm.AddAnthropic(configuration) |> ignore
                | "openai" -> builder.Llm.AddOpenAI(configuration) |> ignore
                | "google" ->
                    Legate.Llm.GoogleServiceCollectionExtensions.AddGoogle(builder.Services, configuration)
                    |> ignore
                | unknown ->
                    raise (
                        InvalidOperationException(
                            $"Unknown --provider '{unknown}': expected anthropic, openai, or google."
                        )
                    )

            match start.McpPath with
            | null -> ()
            | path ->
                if System.IO.File.Exists(path) then
                    builder.Tools.AddMcpServersFromConfig(path) |> ignore
                else
                    raise (InvalidOperationException($"The --mcp path '{path}' does not exist.")))
    )
    |> ignore

    services.AddSingleton<ISessionEventStore>(InMemorySessionEventStore(database))
    |> ignore

    services.AddSingleton<IPermissionPolicy>(AskAllPolicy()) |> ignore

    if start.Scripted then
        let client = scriptedClient ()

        services.AddSingleton<ILlmProvider>(StubScriptedProvider(client) :> ILlmProvider)
        |> ignore

        services.AddSingleton<IChatClient>(client :> IChatClient) |> ignore
    else
        services.AddSingleton<IChatClient>(
            Func<IServiceProvider, IChatClient>(fun provider -> selectClient provider start)
        )
        |> ignore

/// Stops the MCP lifecycle sources so subprocess servers exit with the CLI.
let private stopToolSources (provider: IServiceProvider) : Task =
    task {
        for source in provider.GetServices<IToolSource>() do
            match source with
            | :? IToolSourceLifecycle as lifecycle ->
                try
                    do! lifecycle.StopAsync(CancellationToken.None)
                with _ ->
                    ()
            | _ -> ()
    }

[<EntryPoint>]
let main (argv: string[]) : int =
    let runAsync (start: CliStart) : Task<int> =
        task {
            let application = Host.CreateApplicationBuilder()
            let database = InMemoryDatabase()
            buildServices application.Services application.Configuration database start

            use host = application.Build()

            // Resolve before starting: the resolve triggers the session
            // router wiring, which must land before the actor system spawns
            // its router.
            let client = host.Services.GetRequiredService<SessionClient>()

            do! host.StartAsync(CancellationToken.None)

            let! exit =
                try
                    let engine =
                        Engine(client, Console.In, Console.Out, TimeSpan.FromMinutes(start.WaitMinutes))

                    engine.RunAsync(start.Resume, CancellationToken.None)
                with error ->
                    Console.Error.WriteLine($"legate engine: {error.Message}")
                    Task.FromResult(1)

            do! stopToolSources host.Services
            do! host.StopAsync(CancellationToken.None)
            return exit
        }

    try
        let start =
            try
                parseArgs argv
            with :? ArgumentException as usage ->
                Console.Error.WriteLine(usage.Message)
                Environment.Exit(2)
                Unchecked.defaultof<CliStart>

        runAsync start |> fun runner -> runner.GetAwaiter().GetResult()
    with error ->
        Console.Error.WriteLine($"legate: {error.Message}")
        1
