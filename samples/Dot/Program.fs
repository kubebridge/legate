// SPDX-License-Identifier: Apache-2.0
module Dot.Program

open System
open System.Collections.Generic
open System.IO
open System.Threading
open System.Threading.Tasks
open Legate
open Legate.Storage.Sqlite
open Legate.Workspace.Process
open Microsoft.Extensions.AI
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Hosting

// Dot host with durable SQLite sessions: one UseSqlite file under the
// per-user dot config dir (DOT_DB_PATH overrides), so transcripts survive
// process exit for list (newest-first) and resume. The interactive
// slash-command REPL (#304) drives each turn as waiter-queued-before-prompt
// plus Subscribe streaming with inline permission/question replies, over
// the /new, /sessions, /resume, /abort, /compact, and /quit commands.
// Mirrors the samples/Headless scripted precedent; scripted support stays
// host-local, never a Legate.Testing reference (test-only package).
// Live-provider wiring belongs to later children: there are no live
// branches here. Approval UX polish belongs to #306 (this host only wires
// the opt-in ask policy the REPL already answers inline); steering
// commands belong to #308.

// ──────────────────────────────────────────────────────────────────────────
// Database path

/// Resolves the SQLite file path: the DOT_DB_PATH override when set,
/// else the per-user dot config dir (%APPDATA%/dot/dot.db on Windows,
/// $XDG_CONFIG_HOME/dot/dot.db else ~/.config/dot/dot.db on Unix).
/// Directory creation rides on SqliteDatabase.Open.
/// <returns>The database file path.</returns>
let resolveDbPath () : string =
    let defaultPath () : string =
        if OperatingSystem.IsWindows() then
            Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), "dot", "dot.db")
        else
            let baseDir =
                match Environment.GetEnvironmentVariable("XDG_CONFIG_HOME") with
                | null -> Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), ".config")
                | xdg when String.IsNullOrWhiteSpace xdg ->
                    Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), ".config")
                | xdg -> xdg.Trim()

            Path.Combine(baseDir, "dot", "dot.db")

    match Environment.GetEnvironmentVariable("DOT_DB_PATH") with
    | null -> defaultPath ()
    | overridePath when String.IsNullOrWhiteSpace overridePath -> defaultPath ()
    | overridePath -> overridePath.Trim()

/// Describes where dot stores its sessions and how the second process
/// behaves, for --help and startup errors.
/// <param name="dbPath">The resolved database file path.</param>
/// <returns>The storage help lines.</returns>
let private storageHelp (dbPath: string) : string =
    $"Sessions live in SQLite at {dbPath} (set DOT_DB_PATH to override the file). "
    + "Only one dot process may open the file: a second process exits with a locked error naming the path."

// ──────────────────────────────────────────────────────────────────────────
// Arguments

/// How dot was asked to start.
type DotStart =
    {
        /// Scripted transports instead of live providers (always true:
        /// live wiring belongs to later children, so the flag is accepted
        /// for forward compatibility with the smoke invocation).
        Scripted: bool
        /// Session id to attach at REPL startup, or null to open.
        Resume: string | null
        /// List stored sessions newest-first instead of entering the REPL.
        ListSessions: bool
        /// Settle wait bound in minutes: must be positive. A turn that
        /// outruns it reports DEADLINE while the turn keeps running.
        WaitMinutes: float
        /// Ask for every tool call instead of allowing all: the opt-in
        /// ask policy the REPL answers inline.
        Ask: bool
    }

/// Builds the --help text over the resolved database path.
/// <param name="dbPath">The resolved database file path.</param>
/// <returns>The usage text.</returns>
let private helpText (dbPath: string) : string =
    "Usage: Dot [--resume <session-id>] [--sessions|--list] [--scripted] [--wait-minutes <n>] [--ask] "
    + storageHelp dbPath

/// Parses the dot arguments into a start plan. Unknown flags fail with a
/// usage error naming the flag; missing values fail naming the flag.
/// <param name="argv">The process arguments.</param>
/// <returns>The start plan.</returns>
let parseArgs (argv: string[]) : DotStart =
    let mutable scripted = true
    let mutable resume: string | null = null
    let mutable listSessions = false
    let mutable waitMinutes = 5.0
    let mutable ask = false

    let mutable index = 0

    let take (flag: string) : string =
        index <- index + 1

        if index >= argv.Length then
            raise (ArgumentException($"The {flag} flag needs a value.", flag))

        argv[index]

    while index < argv.Length do
        match argv[index] with
        | "--resume" -> resume <- take "--resume"
        | "--sessions"
        | "--list" -> listSessions <- true
        | "--scripted" -> scripted <- true
        | "--wait-minutes" ->
            let raw = take "--wait-minutes"

            match Double.TryParse(raw) with
            | true, minutes when minutes > 0.0 -> waitMinutes <- minutes
            | _ ->
                raise (
                    ArgumentException("The --wait-minutes flag needs a positive number of minutes.", "--wait-minutes")
                )
        | "--ask" -> ask <- true
        | "--help"
        | "-h" -> raise (ArgumentException(helpText (resolveDbPath ()), "--help"))
        | unknown -> raise (ArgumentException($"Unknown flag '{unknown}'.", unknown))

        index <- index + 1

    let resumeValue: string | null =
        match resume with
        | null -> null
        | raw when String.IsNullOrWhiteSpace raw ->
            raise (ArgumentException("The --resume flag needs a non-empty session id.", "--resume"))
        | raw -> raw.Trim()

    {
        Scripted = scripted
        Resume = resumeValue
        ListSessions = listSessions
        WaitMinutes = waitMinutes
        Ask = ask
    }

// ──────────────────────────────────────────────────────────────────────────
// Scripted transports (no keys, no external services)

// Asks for every tool call: the opt-in ask policy, so every agent tool
// call meets a console approval before it runs. Approval UX polish
// belongs to #306; this is only the policy the REPL answers inline.
type private AskAllPolicy() =
    interface IPermissionPolicy with
        member _.Evaluate(_) = PermissionVerdict.Ask

/// One scripted model step: assistant text or one tool call.
type private ScriptStep =
    | Text of string
    | ToolCall of callId: string * toolName: string

/// Minimal inline scripted chat client: serves the queued steps, then a
/// canned answer forever so the open-ended REPL never runs dry. Mirrors
/// the samples/Headless precedent; scripted support stays host-local,
/// never a Legate.Testing reference (test-only package). A prompt naming
/// "slow-turn" waits a second before answering: a scripted verification
/// aid for the settle-deadline path, so a tiny --wait-minutes bound
/// deterministically outruns the turn while it keeps running.
type private ScriptedClient(steps: Queue<ScriptStep>) =
    do ArgumentNullException.ThrowIfNull(steps)

    /// True when the incoming messages ask for the slow-turn probe.
    /// <param name="messages">The chat messages.</param>
    /// <returns>True when the turn should wait before answering.</returns>
    let isSlowTurn (messages: ChatMessage seq) : bool =
        messages
        |> Seq.exists (fun message ->
            not (isNull (box message))
            && not (isNull (box message.Text))
            && message.Text.Contains("slow-turn", StringComparison.Ordinal))

    interface IChatClient with
        member _.GetResponseAsync(messages, _, cancellationToken) =
            task {
                cancellationToken.ThrowIfCancellationRequested()

                if isSlowTurn messages then
                    do! Task.Delay(TimeSpan.FromSeconds(1.0), cancellationToken)

                if steps.Count = 0 then
                    return ChatResponse(ChatMessage(ChatRole.Assistant, "dot scripted answer"))
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

/// Creates the scripted chat client: one permission-gated echo call under
/// the ask policy, then plain answers. Under allow-all the echo call is
/// auto-approved and the turn completes with the first answer.
let private scriptedClient () : ScriptedClient =
    let steps = Queue<ScriptStep>()
    steps.Enqueue(ToolCall("call-1", "scripted-echo"))
    steps.Enqueue(Text "dot scripted answer one")
    steps.Enqueue(Text "dot scripted answer two")
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

/// Fixed inline tool source: the scripted echo tool for scripted mode.
type private StaticSource(tools: IReadOnlyList<AITool>) =
    do ArgumentNullException.ThrowIfNull(tools)

    interface IToolSource with
        member _.GetTools(_) = Task.FromResult(tools)

// ──────────────────────────────────────────────────────────────────────────
// Host building

/// Builds the container: one UseSqlite file plus every store over it
/// (migrations, WAL mode, and the busy timeout ride on the open), a
/// process workspace over the working directory, the allow-all policy by
/// default (the opt-in ask policy under --ask; the facade runner reads
/// the container policy), the scripted echo tool, and the scripted
/// provider. No live branches: live wiring belongs to later children.
/// <param name="services">The container to add the host to.</param>
/// <param name="dbPath">The SQLite file path. Created with its directory on first use.</param>
/// <param name="ask">True to ask for every tool call instead of allowing all.</param>
let private buildServices (services: IServiceCollection) (dbPath: string) (ask: bool) : unit =
    SqliteServiceCollectionExtensions.UseSqlite(services, dbPath) |> ignore

    LegateServiceCollectionExtensions.AddLegate(
        services,
        Action<LegateBuilder>(fun builder ->
            let workspaceOptions = ProcessWorkspaceRuntimeOptions()
            workspaceOptions.Root <- Environment.CurrentDirectory

            builder.Workspace.UseRuntime(ProcessWorkspaceRuntime(workspaceOptions, null, null))
            |> ignore

            if ask then
                builder.Permissions.UsePolicy(AskAllPolicy()) |> ignore
            else
                builder.Permissions.UsePolicy(AllowAllPermissionPolicy()) |> ignore

            builder.Tools.AddSource(StaticSource(ResizeArray<AITool>([| scriptedEchoTool () |])))
            |> ignore)
    )
    |> ignore

    let client = scriptedClient ()

    services.AddSingleton<ILlmProvider>(StubScriptedProvider(client) :> ILlmProvider)
    |> ignore

    services.AddSingleton<IChatClient>(client :> IChatClient) |> ignore

/// Lists the stored sessions newest-first with title and state.
/// <param name="client">The session client.</param>
/// <param name="cancellationToken">Abandons the list.</param>
/// <returns>The process exit code.</returns>
let private listSessionsAsync (client: SessionClient) (cancellationToken: CancellationToken) : Task<int> =
    task {
        let! page = SessionClientListingOperations.ListSessionsAsync(client, SessionListOptions(), cancellationToken)

        let items =
            if isNull (box page) || isNull (box page.Items) then
                ResizeArray<Session>() :> System.Collections.Generic.IReadOnlyList<Session>
            else
                page.Items

        Console.Out.WriteLine($"SESSIONS {items.Count}")

        for session in items do
            if not (isNull (box session)) then
                let title = if isNull (box session.Title) then "" else session.Title

                Console.Out.WriteLine($"{session.Id} {title} [{session.State}]")

        return 0
    }

/// Prints the second-process locked error naming the database path.
/// <param name="dbPath">The resolved database file path.</param>
/// <param name="locked">The typed locked error.</param>
let private reportLocked (dbPath: string) (locked: SqliteLockedException) : unit =
    let path =
        if isNull (box locked.Path) || String.IsNullOrWhiteSpace locked.Path then
            dbPath
        else
            locked.Path

    Console.Error.WriteLine(
        $"dot: the database is already open in another process (path {path}). Close the other dot process and try again."
    )

[<EntryPoint>]
let main (argv: string[]) : int =
    let runAsync (start: DotStart) (dbPath: string) : Task<int> =
        task {
            let application = Host.CreateApplicationBuilder()
            buildServices application.Services dbPath start.Ask

            use host = application.Build()

            // Resolve before starting: the resolve triggers the session
            // router wiring, which must land before the actor system spawns
            // its router.
            let client = host.Services.GetRequiredService<SessionClient>()
            let agents = host.Services.GetRequiredService<IAgentStore>()

            try
                do! host.StartAsync(CancellationToken.None)

                let! exit =
                    task {
                        try
                            if start.ListSessions then
                                return! listSessionsAsync client CancellationToken.None
                            else
                                let waitBound = TimeSpan.FromMinutes(start.WaitMinutes)

                                let engine = ReplEngine.Engine(client, agents, Console.In, Console.Out, waitBound)

                                return! engine.RunAsync(start.Resume, CancellationToken.None)
                        with
                        | :? SqliteLockedException as locked ->
                            reportLocked dbPath locked
                            return 1
                        | error ->
                            Console.Error.WriteLine($"dot: {error.Message}")
                            return 1
                    }

                do! host.StopAsync(CancellationToken.None)
                return exit
            with
            | :? SqliteLockedException as locked ->
                reportLocked dbPath locked
                return 1
            | error ->
                Console.Error.WriteLine($"dot: {error.Message} (path {dbPath}).")
                return 1
        }

    try
        let start =
            try
                parseArgs argv
            with :? ArgumentException as usage ->
                Console.Error.WriteLine(usage.Message)
                Environment.Exit(2)
                Unchecked.defaultof<DotStart>

        let dbPath = resolveDbPath ()
        runAsync start dbPath |> fun runner -> runner.GetAwaiter().GetResult()
    with
    | :? SqliteLockedException as locked ->
        let path =
            try
                resolveDbPath ()
            with _ ->
                ""

        let display =
            if isNull (box locked.Path) || String.IsNullOrWhiteSpace locked.Path then
                path
            else
                locked.Path

        Console.Error.WriteLine(
            $"dot: the database is already open in another process (path {display}). Close the other dot process and try again."
        )

        1
    | error ->
        Console.Error.WriteLine($"dot: {error.Message}")
        1
