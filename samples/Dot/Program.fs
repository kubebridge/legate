// SPDX-License-Identifier: Apache-2.0
module Dot.Program

open System
open System.Threading
open System.Threading.Tasks
open Legate
open Legate.Storage.InMemory
open Legate.Workspace.Process
open Microsoft.Extensions.AI
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Hosting

// Dot skeleton: opens one ordinary session on in-repo packages (InMemory
// stores, process workspace over the working directory), runs a single
// scripted prompt through PromptAndWait, and maps the TurnResult to a
// process exit code. Mirrors the samples/Headless precedent; scripted
// support stays host-local, never a Legate.Testing reference (test-only
// package). Live-provider wiring belongs to later children: there are no
// live branches here.

// ──────────────────────────────────────────────────────────────────────────
// Arguments

/// How dot was asked to start.
type DotStart =
    {
        /// The single prompt text.
        Prompt: string
        /// Scripted transports instead of live providers (always true:
        /// live wiring belongs to later children, so the flag is accepted
        /// for forward compatibility with the smoke invocation).
        Scripted: bool
    }

/// Parses the dot arguments into a start plan. Unknown flags fail with a
/// usage error naming the flag; missing values fail naming the flag.
/// <param name="argv">The process arguments.</param>
/// <returns>The start plan.</returns>
let parseArgs (argv: string[]) : DotStart =
    let mutable prompt = "hi"
    let mutable scripted = true

    let mutable index = 0

    let take (flag: string) : string =
        index <- index + 1

        if index >= argv.Length then
            raise (ArgumentException($"The {flag} flag needs a value.", flag))

        argv[index]

    while index < argv.Length do
        match argv[index] with
        | "--prompt" -> prompt <- take "--prompt"
        | "--scripted" -> scripted <- true
        | "--help"
        | "-h" -> raise (ArgumentException("Usage: Dot [--prompt <text>] [--scripted]", "--help"))
        | unknown -> raise (ArgumentException($"Unknown flag '{unknown}'.", unknown))

        index <- index + 1

    if String.IsNullOrWhiteSpace prompt then
        raise (ArgumentException("The --prompt flag needs a non-empty prompt.", "--prompt"))

    {
        Prompt = prompt.Trim()
        Scripted = scripted
    }

// ──────────────────────────────────────────────────────────────────────────
// Scripted transports (no keys, no external services)

/// Minimal inline scripted chat client: serves one queued answer, then
/// fails loudly. Mirrors the samples/Headless precedent; scripted support
/// stays host-local, never a Legate.Testing reference (test-only package).
type private ScriptedClient(answer: string) =
    let mutable served = false

    interface IChatClient with
        member _.GetResponseAsync(_, _, cancellationToken) =
            task {
                cancellationToken.ThrowIfCancellationRequested()

                if served then
                    return raise (InvalidOperationException("The scripted client already served its answer."))
                else
                    served <- true
                    return ChatResponse(ChatMessage(ChatRole.Assistant, answer))
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

// ──────────────────────────────────────────────────────────────────────────
// Exit codes

/// The exit code for a settled turn result: 0 when the turn completed, 2
/// when the host aborted it, 1 when it failed, 3 for anything else.
let private exitFor (status: TurnStatus) : int =
    match status with
    | TurnStatus.Completed -> 0
    | TurnStatus.Aborted -> 2
    | TurnStatus.Failed -> 1
    | TurnStatus.Pending
    | TurnStatus.Running
    | TurnStatus.Suspended
    | _ -> 3

// ──────────────────────────────────────────────────────────────────────────
// Host building

/// Builds the container: InMemory stores, a process workspace over the
/// working directory, allow-all permissions (the facade runner reads the
/// container policy), and the scripted provider. No live branches: live
/// wiring belongs to later children.
let private buildServices (services: IServiceCollection) (database: InMemoryDatabase) : unit =
    LegateServiceCollectionExtensions.AddLegate(
        services,
        Action<LegateBuilder>(fun builder ->
            builder.Storage.UseSessionStore(InMemorySessionStore(database)) |> ignore

            let workspaceOptions = ProcessWorkspaceRuntimeOptions()
            workspaceOptions.Root <- Environment.CurrentDirectory

            builder.Workspace.UseRuntime(ProcessWorkspaceRuntime(workspaceOptions, null, null))
            |> ignore

            builder.Permissions.UsePolicy(AllowAllPermissionPolicy()) |> ignore)
    )
    |> ignore

    services.AddSingleton<ISessionEventStore>(InMemorySessionEventStore(database))
    |> ignore

    let client = new ScriptedClient("dot scripted answer")

    services.AddSingleton<ILlmProvider>(StubScriptedProvider(client) :> ILlmProvider)
    |> ignore

    services.AddSingleton<IChatClient>(client :> IChatClient) |> ignore

/// Opens the dot session: allow-all permissions with the smoke bound.
let private openDotAsync
    (client: SessionClient)
    (bound: TimeSpan)
    (cancellationToken: CancellationToken)
    : Task<Session> =
    task {
        let options = SessionOptions()
        options.Title <- "dot"
        options.Permissions <- AllowAllPermissionPolicy()
        options.Timeout <- Nullable<TimeSpan>(bound)

        return! SessionClientOperations.OpenSessionAsync(client, AgentId.New(), options, cancellationToken)
    }

[<EntryPoint>]
let main (argv: string[]) : int =
    let runAsync (start: DotStart) : Task<int> =
        task {
            let application = Host.CreateApplicationBuilder()
            let database = InMemoryDatabase()
            buildServices application.Services database

            use host = application.Build()

            // Resolve before starting: the resolve triggers the session
            // router wiring, which must land before the actor system spawns
            // its router.
            let client = host.Services.GetRequiredService<SessionClient>()

            do! host.StartAsync(CancellationToken.None)

            let! exit =
                task {
                    try
                        let bound = TimeSpan.FromMinutes(5.0)
                        let! session = openDotAsync client bound CancellationToken.None
                        Console.Out.WriteLine($"SESSION {session.Id}")

                        let! result =
                            SessionClientExtensions.PromptAndWaitAsync(
                                client,
                                session.Id,
                                UserMessage.Text start.Prompt,
                                CancellationToken.None
                            )

                        Console.Out.WriteLine($"RESULT {result.Status}")

                        if not (String.IsNullOrEmpty result.AssistantText) then
                            Console.Out.WriteLine($"TEXT {result.AssistantText}")

                        return exitFor result.Status
                    with error ->
                        Console.Error.WriteLine($"dot: {error.Message}")
                        return 1
                }

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
                Unchecked.defaultof<DotStart>

        runAsync start |> fun runner -> runner.GetAwaiter().GetResult()
    with error ->
        Console.Error.WriteLine($"dot: {error.Message}")
        1
