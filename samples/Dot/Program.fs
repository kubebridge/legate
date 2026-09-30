// SPDX-License-Identifier: Apache-2.0
module Dot.Program

open System
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
// process exit for list (newest-first) and resume. Mirrors the
// samples/Headless scripted precedent; scripted support stays host-local,
// never a Legate.Testing reference (test-only package). Live-provider
// wiring belongs to later children: there are no live branches here. The
// interactive slash-command REPL itself belongs to #304: this host keeps
// only the minimal startup, list, and resume surface needed to verify
// cross-process durability.

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
        /// The single prompt text.
        Prompt: string
        /// Scripted transports instead of live providers (always true:
        /// live wiring belongs to later children, so the flag is accepted
        /// for forward compatibility with the smoke invocation).
        Scripted: bool
        /// Session id to resume and prompt, or null to open a new session.
        Resume: string | null
        /// List stored sessions newest-first instead of prompting.
        ListSessions: bool
    }

/// Builds the --help text over the resolved database path.
/// <param name="dbPath">The resolved database file path.</param>
/// <returns>The usage text.</returns>
let private helpText (dbPath: string) : string =
    "Usage: Dot [--prompt <text>] [--resume <session-id>] [--sessions|--list] [--scripted] "
    + storageHelp dbPath

/// Parses the dot arguments into a start plan. Unknown flags fail with a
/// usage error naming the flag; missing values fail naming the flag.
/// <param name="argv">The process arguments.</param>
/// <returns>The start plan.</returns>
let parseArgs (argv: string[]) : DotStart =
    let mutable prompt = "hi"
    let mutable scripted = true
    let mutable resume: string | null = null
    let mutable listSessions = false

    let mutable index = 0

    let take (flag: string) : string =
        index <- index + 1

        if index >= argv.Length then
            raise (ArgumentException($"The {flag} flag needs a value.", flag))

        argv[index]

    while index < argv.Length do
        match argv[index] with
        | "--prompt" -> prompt <- take "--prompt"
        | "--resume" -> resume <- take "--resume"
        | "--sessions"
        | "--list" -> listSessions <- true
        | "--scripted" -> scripted <- true
        | "--help"
        | "-h" -> raise (ArgumentException(helpText (resolveDbPath ()), "--help"))
        | unknown -> raise (ArgumentException($"Unknown flag '{unknown}'.", unknown))

        index <- index + 1

    if not listSessions && String.IsNullOrWhiteSpace prompt then
        raise (ArgumentException("The --prompt flag needs a non-empty prompt.", "--prompt"))

    let resumeValue: string | null =
        match resume with
        | null -> null
        | raw when String.IsNullOrWhiteSpace raw ->
            raise (ArgumentException("The --resume flag needs a non-empty session id.", "--resume"))
        | raw -> raw.Trim()

    {
        Prompt = prompt.Trim()
        Scripted = scripted
        Resume = resumeValue
        ListSessions = listSessions
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

/// Builds the container: one UseSqlite file plus every store over it
/// (migrations, WAL mode, and the busy timeout ride on the open), a
/// process workspace over the working directory, allow-all permissions
/// (the facade runner reads the container policy), and the scripted
/// provider. No live branches: live wiring belongs to later children.
/// <param name="services">The container to add the host to.</param>
/// <param name="dbPath">The SQLite file path. Created with its directory on first use.</param>
let private buildServices (services: IServiceCollection) (dbPath: string) : unit =
    SqliteServiceCollectionExtensions.UseSqlite(services, dbPath) |> ignore

    LegateServiceCollectionExtensions.AddLegate(
        services,
        Action<LegateBuilder>(fun builder ->
            let workspaceOptions = ProcessWorkspaceRuntimeOptions()
            workspaceOptions.Root <- Environment.CurrentDirectory

            builder.Workspace.UseRuntime(ProcessWorkspaceRuntime(workspaceOptions, null, null))
            |> ignore

            builder.Permissions.UsePolicy(AllowAllPermissionPolicy()) |> ignore)
    )
    |> ignore

    let client = new ScriptedClient("dot scripted answer")

    services.AddSingleton<ILlmProvider>(StubScriptedProvider(client) :> ILlmProvider)
    |> ignore

    services.AddSingleton<IChatClient>(client :> IChatClient) |> ignore

/// Ensures the session's agent exists in the SQLite agent store: the
/// runtime's authority check rejects turns for missing agents, while a
/// null catalog (the InMemory skeleton precedent) authorizes. Dot is the
/// first SQLite host, so it provisions one enabled scripted agent per
/// session on open; resumed sessions reuse the stored agent row.
/// <param name="agents">The SQLite agent store.</param>
/// <param name="tenant">The tenant the session belongs to.</param>
/// <param name="agentId">The agent the session opens with.</param>
/// <param name="cancellationToken">Abandons the upsert.</param>
let private ensureDotAgentAsync
    (agents: IAgentStore)
    (tenant: TenantId)
    (agentId: AgentId)
    (cancellationToken: CancellationToken)
    : Task =
    task {
        let now = DateTimeOffset.UtcNow

        let agent: Agent =
            {
                Id = agentId
                Tenant = tenant
                Name = "dot"
                Description = null
                Model = ModelReference.Parse("scripted/scripted")
                SystemPrompt = ""
                EnvironmentVariables = null
                PermissionDefaults = null
                ToolSelection = null
                PackageReference = null
                Enabled = true
                Schedule = null
                RowVersion = 0UL
                CreatedAt = now
                UpdatedAt = now
            }

        let! _ = agents.UpdateIfUnchanged(tenant, agent, 0UL, cancellationToken)
        ()
    }

/// Opens the dot session with the smoke bound. Permissions stay null so
/// the session uses the container allow-all policy: a concrete policy on
/// SessionOptions does not survive the SQLite JSON round-trip.
let private openDotAsync
    (client: SessionClient)
    (agents: IAgentStore)
    (bound: TimeSpan)
    (cancellationToken: CancellationToken)
    : Task<Session> =
    task {
        let agentId = AgentId.New()
        do! ensureDotAgentAsync agents TenantId.Default agentId cancellationToken

        let options = SessionOptions()
        options.Title <- "dot"
        options.Timeout <- Nullable<TimeSpan>(bound)

        return! SessionClientOperations.OpenSessionAsync(client, agentId, options, cancellationToken)
    }

/// Prints one settled turn result and maps it to a process exit code.
/// <param name="result">The settled turn result.</param>
/// <returns>The process exit code.</returns>
let private reportResult (result: TurnResult) : int =
    Console.Out.WriteLine($"RESULT {result.Status}")

    if not (String.IsNullOrEmpty result.AssistantText) then
        Console.Out.WriteLine($"TEXT {result.AssistantText}")

    exitFor result.Status

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

/// Resumes one stored session by id and prompts it, so the follow-up turn
/// observes the prior transcript. Unknown ids print a clean error naming
/// the id, never a stack trace.
/// <param name="client">The session client.</param>
/// <param name="rawId">The session id text from --resume.</param>
/// <param name="promptText">The follow-up prompt text.</param>
/// <param name="cancellationToken">Abandons the resume.</param>
/// <returns>The process exit code.</returns>
let private resumeAndPromptAsync
    (client: SessionClient)
    (rawId: string)
    (promptText: string)
    (cancellationToken: CancellationToken)
    : Task<int> =
    task {
        let mutable parsed = Unchecked.defaultof<SessionId>

        if not (SessionId.TryParse(rawId, &parsed)) then
            Console.Error.WriteLine($"dot: no session {rawId}.")
            return 1
        else
            try
                // Probe the journal: unknown sessions throw
                // SessionNotFoundException here.
                let! _ = SessionClientOperations.ReadEventsAsync(client, parsed, 0L, 1, cancellationToken)
                Console.Out.WriteLine($"RESUMED {parsed}")

                let! result =
                    SessionClientExtensions.PromptAndWaitAsync(
                        client,
                        parsed,
                        UserMessage.Text promptText,
                        cancellationToken
                    )

                return reportResult result
            with :? SessionNotFoundException ->
                Console.Error.WriteLine($"dot: no session {parsed}.")
                return 1
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
            buildServices application.Services dbPath

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
                                match start.Resume with
                                | null ->
                                    let bound = TimeSpan.FromMinutes(5.0)

                                    let! session = openDotAsync client agents bound CancellationToken.None

                                    Console.Out.WriteLine($"SESSION {session.Id}")

                                    let! result =
                                        SessionClientExtensions.PromptAndWaitAsync(
                                            client,
                                            session.Id,
                                            UserMessage.Text start.Prompt,
                                            CancellationToken.None
                                        )

                                    return reportResult result
                                | raw ->
                                    return! resumeAndPromptAsync client (raw.Trim()) start.Prompt CancellationToken.None
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
