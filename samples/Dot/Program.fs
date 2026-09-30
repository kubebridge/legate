// SPDX-License-Identifier: Apache-2.0
module Dot.Program

open System
open System.Collections.Generic
open System.IO
open System.Threading
open System.Threading.Tasks
open Legate
open Legate.Llm.OpenAI
open Legate.Mcp
open Legate.Storage.Sqlite
open Legate.Workspace.HostDirectory
open Microsoft.Extensions.AI
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Hosting

// Dot host with durable SQLite sessions: one UseSqlite file under the
// per-user dot config dir (DOT_DB_PATH overrides), so transcripts survive
// process exit for list (newest-first) and resume. The interactive
// slash-command REPL (#304) drives each turn as waiter-queued-before-prompt
// plus Subscribe streaming with inline permission/question replies, over
// the /new, /sessions, /resume, /model, /steer, /follow, /abort, /compact,
// /tree, /fork, /clone, /session, /export, /<template>, and /quit commands. Live providers (#307) register
// only when their env key is present (Anthropic through the OpenAI-compatible
// preset under id anthropic, OpenAI, Google), with keys flowing from the
// environment through IConfiguration binding only, never printed or
// persisted; with exactly one key set dot just works, with several
// --provider picks, else the default order anthropic, openai, google wins,
// and --model overrides the model. Mid-session /model switches through
// SetAgentAsync against a model-carrying agent row, so the transcript and
// workspace binding survive. Steering (#308) stays foreground while one
// turn runs in flight: /steer interrupts (Interrupt) and /follow folds in
// (Inject) with plain input queuing (Queue), /tree lists journal positions
// and /fork branches the prefix through ForkAsync. Mirrors the
// samples/Headless scripted precedent; scripted support stays host-local,
// never a Legate.Testing reference (test-only package). The workspace is
// the working directory through the host-directory runtime with the
// dot-local coding tools (#306): read_file, write_file, list_files,
// edit_file, glob, grep, and exec under a pi-faithful allow-all default,
// with the opt-in --ask policy the REPL answers inline (allow-once /
// allow-for-session / deny). Root fencing stays on in every policy mode.

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
        /// Force scripted transports even when live keys are set. Live mode
        /// registers when a provider key is present; with no keys dot falls
        /// back to scripted, so the flag is only needed to pin scripted.
        Scripted: bool
        /// Provider id pick for live mode, or null for automatic: the single
        /// registered provider, else the default order.
        Provider: string | null
        /// Model reference override in <provider/model> form, or null for
        /// the provider default.
        Model: string | null
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
        /// MCP config file to attach extra tools from, or null for none.
        McpPath: string | null
    }

/// Builds the --help text over the resolved database path: usage, session
/// storage, the provider and model flags with the default order, the
/// unsandboxed workspace with its container-sandboxing expectation, and
/// the --ask escape.
/// <param name="dbPath">The resolved database file path.</param>
/// <returns>The usage text.</returns>
let private helpText (dbPath: string) : string =
    "Usage: Dot [--provider <id>] [--model <provider/model>] [--resume <session-id>] [--sessions|--list] [--scripted] [--wait-minutes <n>] [--ask] [--mcp <path>] "
    + storageHelp dbPath
    + " Providers anthropic, openai, and google register only when ANTHROPIC_API_KEY, OPENAI_API_KEY, or GOOGLE_API_KEY is set (keys flow from the environment through Legate:Llm:Providers:<id>:ApiKey binding only, never printed or persisted). With exactly one key set dot just works; with several, --provider picks, else the default order anthropic, openai, google wins; --model <provider/model> overrides the model (--provider/--model apply to live mode; --scripted pins the scripted transport). /model lists the options mid-session and switches without losing the transcript. "
    + " Tools run against the working directory through the host-directory runtime with no sandbox: sandbox the run in a container (as pi does) or pass --ask for per-call approval (allow once, allow for session, deny). The workspace root fence stays on in every mode."

/// Parses the dot arguments into a start plan. Unknown flags fail with a
/// usage error naming the flag; missing values fail naming the flag.
/// <param name="argv">The process arguments.</param>
/// <returns>The start plan.</returns>
let parseArgs (argv: string[]) : DotStart =
    let mutable scripted = false
    let mutable provider: string | null = null
    let mutable model: string | null = null
    let mutable resume: string | null = null
    let mutable listSessions = false
    let mutable waitMinutes = 5.0
    let mutable ask = false
    let mutable mcp: string | null = null

    let mutable index = 0

    let take (flag: string) : string =
        index <- index + 1

        if index >= argv.Length then
            raise (ArgumentException($"The {flag} flag needs a value.", flag))

        argv[index]

    while index < argv.Length do
        match argv[index] with
        | "--provider" -> provider <- take "--provider"
        | "--model" -> model <- take "--model"
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
        | "--mcp" -> mcp <- take "--mcp"
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

    let providerValue: string | null =
        match provider with
        | null -> null
        | raw when String.IsNullOrWhiteSpace raw ->
            raise (ArgumentException("The --provider flag needs a non-empty provider id.", "--provider"))
        | raw -> raw.Trim()

    let modelValue: string | null =
        match model with
        | null -> null
        | raw when String.IsNullOrWhiteSpace raw ->
            raise (ArgumentException("The --model flag needs a non-empty <provider/model> reference.", "--model"))
        | raw -> raw.Trim()

    let mcpValue: string | null =
        match mcp with
        | null -> null
        | raw when String.IsNullOrWhiteSpace raw ->
            raise (ArgumentException("The --mcp flag needs a non-empty config path.", "--mcp"))
        | raw -> raw.Trim()

    {
        Scripted = scripted
        Provider = providerValue
        Model = modelValue
        Resume = resumeValue
        ListSessions = listSessions
        WaitMinutes = waitMinutes
        Ask = ask
        McpPath = mcpValue
    }

// ──────────────────────────────────────────────────────────────────────────
// Scripted transports (no keys, no external services)

// Asks for every tool call: the opt-in ask policy, so every agent tool
// call (reads and lists included, exec always) meets a console approval
// before it runs. A deny runs nothing and the turn continues; the REPL
// answers each request inline (allow-once / allow-for-session / deny).
type private AskAllPolicy() =
    interface IPermissionPolicy with
        member _.Evaluate(_) = PermissionVerdict.Ask

/// One scripted model step: assistant text or one tool call with its
/// named arguments.
type private ScriptStep =
    | Text of string
    | ToolCall of callId: string * toolName: string * args: (string * obj) list

/// Probe scripts keyed by the prompt marker that selects them: scripted
/// acceptances for the coding tools over the working directory, the skill
/// tool over the sample review package (skill-load, skill-missing), and
/// the context-file rewrite (ctx-edit). Each probe replaces the default
/// queue once per process, so the shared steps below stay byte-identical
/// for the existing smoke runs. ctx-check is dynamic instead (see
/// serveProbe): every mention answers with the markers the current system
/// prompt carries, so an edit between turns steers the next answer.
/// <param name="marker">The prompt marker selecting the probe.</param>
/// <returns>The probe's model steps.</returns>
let private probeScript (marker: string) : ScriptStep list =
    match marker with
    | "coding-write" ->
        [
            ToolCall(
                "call-write",
                "write_file",
                [
                    "path", "hello.html" :> obj
                    "content", "<!DOCTYPE html>\n<html>\n<body>\n<h1>hello from dot</h1>\n</body>\n</html>\n" :> obj
                ]
            )
            Text "coding-write done"
        ]
    | "coding-edit" ->
        [
            ToolCall(
                "call-edit-write",
                "write_file",
                [
                    "path", "site/index.html" :> obj
                    "content", "<h1>hello</h1>\n" :> obj
                ]
            )
            ToolCall(
                "call-edit",
                "edit_file",
                [
                    "path", "site/index.html" :> obj
                    "old_string", "<h1>hello</h1>" :> obj
                    "new_string", "<h1>hello, edited</h1>" :> obj
                ]
            )
            Text "coding-edit done"
        ]
    | "coding-two" ->
        [
            ToolCall(
                "call-two-a",
                "write_file",
                [
                    "path", "probe-a.txt" :> obj
                    "content", "alpha\n" :> obj
                ]
            )
            ToolCall(
                "call-two-b",
                "write_file",
                [
                    "path", "probe-b.txt" :> obj
                    "content", "beta\n" :> obj
                ]
            )
            Text "coding-two done"
        ]
    | "coding-outside" ->
        [
            ToolCall(
                "call-outside",
                "write_file",
                [
                    "path", "../outside-evil.txt" :> obj
                    "content", "must not land\n" :> obj
                ]
            )
            Text "coding-outside done"
        ]
    | "coding-exec" ->
        [
            ToolCall(
                "call-exec",
                "exec",
                [
                    "command", "dotnet --version" :> obj
                    "timeout_seconds", 60 :> obj
                ]
            )
            Text "coding-exec done"
        ]
    | "slow-steer" ->
        [
            ToolCall("call-slow-steer", "slow-echo", [])
            Text "slow-steer done"
        ]
    | "skill-load" ->
        [
            ToolCall("call-skill-load", "skill", [ "name", "review" :> obj ])
            Text "SKILL-LOAD-DONE-309"
        ]
    | "skill-missing" ->
        [
            ToolCall("call-skill-missing", "skill", [ "name", "nosuchskill309" :> obj ])
            Text "SKILL-MISSING-DONE-309"
        ]
    | "ctx-edit" ->
        [
            ToolCall(
                "call-ctx-edit",
                "write_file",
                [
                    "path", "AGENTS.md" :> obj
                    "content", "# dot context probe\n\nCTX-AGENTS-BETA-309 steering active.\n" :> obj
                ]
            )
            Text "CTX-EDIT-DONE-309"
        ]
    | _ -> []

/// Minimal inline scripted chat client: serves the queued steps, then a
/// canned answer forever so the open-ended REPL never runs dry. Mirrors
/// the samples/Headless precedent; scripted support stays host-local,
/// never a Legate.Testing reference (test-only package). A prompt naming
/// "slow-turn" waits a second before answering: a scripted verification
/// aid for the settle-deadline path, so a tiny --wait-minutes bound
/// deterministically outruns the turn while it keeps running.
type private ScriptedClient(steps: Queue<ScriptStep>) =
    do ArgumentNullException.ThrowIfNull(steps)

    // Probe markers served so far: history repeats the marker on later
    // calls, so a served probe never refills the queue twice.
    let served = HashSet<string>(StringComparer.Ordinal)

    /// The probe markers in match order: no marker contains another, so
    /// the first mention wins. ctx-check stays out: it answers dynamically
    /// from the current system prompt markers on every mention (see
    /// serveProbe), so an edit between turns steers the next answer.
    let probeMarkers =
        [
            "coding-write"
            "coding-edit"
            "coding-two"
            "coding-outside"
            "coding-exec"
            "slow-steer"
            "skill-load"
            "skill-missing"
            "ctx-edit"
        ]

    /// True when any incoming message mentions the marker.
    /// <param name="messages">The chat messages.</param>
    /// <param name="marker">The probe marker.</param>
    /// <returns>True when the marker is mentioned.</returns>
    let mentions (messages: ChatMessage seq) (marker: string) : bool =
        messages
        |> Seq.exists (fun message ->
            not (isNull (box message))
            && not (isNull (box message.Text))
            && message.Text.Contains(marker, StringComparison.Ordinal))

    /// True when any incoming system message mentions the marker: the
    /// context-file proof scans the composed system prompt only, never the
    /// transcript, so an edit between turns steers the next answer only
    /// when the runtime re-read the files this turn.
    /// <param name="messages">The chat messages.</param>
    /// <param name="marker">The context marker.</param>
    /// <returns>True when a system message mentions the marker.</returns>
    let mentionsSystem (messages: ChatMessage seq) (marker: string) : bool =
        messages
        |> Seq.exists (fun message ->
            not (isNull (box message))
            && not (isNull (box message.Text))
            && message.Role = ChatRole.System
            && message.Text.Contains(marker, StringComparison.Ordinal))

    /// Refills the queue with the first unserved probe the messages
    /// mention, if any. ctx-check refills on every mention (never marked
    /// served): the answer names the context markers the current system
    /// prompt carries, proving the runtime re-read the files this turn.
    /// <param name="messages">The chat messages.</param>
    let serveProbe (messages: ChatMessage seq) : unit =
        if mentions messages "ctx-check" then
            let agentsPart =
                if mentionsSystem messages "CTX-AGENTS-BETA-309" then
                    "beta"
                elif mentionsSystem messages "CTX-AGENTS-ALPHA-309" then
                    "alpha"
                else
                    "none"

            let systemPart =
                if mentionsSystem messages "CTX-SYSTEM-ONE-309" then
                    "one"
                else
                    "none"

            steps.Clear()
            steps.Enqueue(Text $"CTX-SEEN-309 agents={agentsPart} system={systemPart}")
        else
            match
                probeMarkers
                |> List.tryFind (fun marker -> not (served.Contains marker) && mentions messages marker)
            with
            | None -> ()
            | Some marker ->
                served.Add marker |> ignore
                steps.Clear()

                for step in probeScript marker do
                    steps.Enqueue step

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

                serveProbe messages

                if steps.Count = 0 then
                    return ChatResponse(ChatMessage(ChatRole.Assistant, "dot scripted answer"))
                else
                    match steps.Dequeue() with
                    | Text text -> return ChatResponse(ChatMessage(ChatRole.Assistant, text))
                    | ToolCall(callId, toolName, args) ->
                        let arguments = Dictionary<string, obj>()

                        for key, value in args do
                            arguments[key] <- value

                        let call =
                            FunctionCallContent(callId, toolName, arguments :> IDictionary<string, obj>) :> AIContent

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
/// auto-approved and the turn completes with the first answer. Prompts
/// naming a coding probe (coding-write, coding-edit, coding-two,
/// coding-outside, coding-exec), a skill probe (skill-load, skill-missing),
/// or the context rewrite (ctx-edit) swap the queue for that probe's tool
/// steps instead; prompts naming ctx-check answer with the context markers
/// the current system prompt carries.
let private scriptedClient () : ScriptedClient =
    let steps = Queue<ScriptStep>()
    steps.Enqueue(ToolCall("call-1", "scripted-echo", []))
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

/// Creates the slow echo tool for the steering probe: one three-second
/// call the mid-turn /steer, /follow, /abort, and /compact lines overlap,
/// so piped stdin deterministically steers a still-running turn under the
/// default --wait-minutes bound.
let private slowEchoTool () : AITool =
    let method =
        System.Func<string>(fun () ->
            System.Threading.Thread.Sleep(3000)
            "slow tool ok")

    AIFunctionFactory.Create(
        method,
        "slow-echo",
        "Slow echo for the steering smoke run: sleeps before answering.",
        null
    )
    :> AITool

/// Fixed inline tool source: the scripted echo tool for scripted mode.
type private StaticSource(tools: IReadOnlyList<AITool>) =
    do ArgumentNullException.ThrowIfNull(tools)

    interface IToolSource with
        member _.GetTools(_) = Task.FromResult(tools)

// ──────────────────────────────────────────────────────────────────────────
// Provider keys (issue 307)

/// Whether the environment carries the provider key: the registration
/// gate, mirroring the samples/MinimalHost precedent.
let private hasKey (name: string) : bool =
    not (String.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(name)))

/// Whether any live provider key is present.
let private hasLiveKey () : bool =
    hasKey "ANTHROPIC_API_KEY" || hasKey "OPENAI_API_KEY" || hasKey "GOOGLE_API_KEY"

/// Bridges the plain provider env keys into the Legate provider sections
/// before the host builds its configuration, so `export ANTHROPIC_API_KEY`
/// satisfies the Legate:Llm:Providers:anthropic:ApiKey binding the Add*
/// registration reads (AGENTS.md Agent Login names both sources). A
/// Legate:Llm:Providers:<id>:ApiKey value already in the environment wins:
/// the bridge only fills blanks. Process memory only: nothing is printed
/// or persisted.
let private bridgeProviderKeys () : unit =
    let bridge (plain: string) (compound: string) : unit =
        if String.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(compound)) then
            let value = Environment.GetEnvironmentVariable(plain)

            if not (String.IsNullOrWhiteSpace value) then
                Environment.SetEnvironmentVariable(compound, value)

    bridge "ANTHROPIC_API_KEY" "Legate__Llm__Providers__anthropic__ApiKey"
    bridge "OPENAI_API_KEY" "Legate__Llm__Providers__openai__ApiKey"
    bridge "GOOGLE_API_KEY" "Legate__Llm__Providers__Google__ApiKey"

// ──────────────────────────────────────────────────────────────────────────
// Host building

/// Resolves the live chat client: the selected provider serving the
/// selected model reference. Unknown providers and unparsable references
/// fail naming the known ids; a missing key names its env var.
/// <param name="provider">The built container.</param>
/// <param name="start">The start plan: provider and model picks.</param>
/// <returns>The chat client for the selected model.</returns>
let private selectLiveClient (provider: IServiceProvider) (start: DotStart) : IChatClient =
    let registered =
        provider.GetServices<ILlmProvider>()
        |> Seq.filter (fun candidate -> not (isNull (box candidate)))
        |> List.ofSeq

    let options = ReplEngine.describeProviders registered
    let reference = ReplEngine.selectReference options start.Provider start.Model

    match
        registered
        |> List.tryFind (fun candidate ->
            String.Equals(candidate.Id, reference.Provider, StringComparison.OrdinalIgnoreCase))
    with
    | None ->
        let known = options |> List.map (fun option -> option.Id) |> String.concat ", "

        raise (InvalidOperationException($"No provider '{reference.Provider}' is registered (known: {known})."))
    | Some live -> live.CreateChatClient(reference, null)

/// Builds the container: one UseSqlite file plus every store over it
/// (migrations, WAL mode, and the busy timeout ride on the open), a
/// host-directory workspace over the working directory, the allow-all
/// policy by default (the every-call ask policy under --ask; the facade
/// runner reads the container policy), the scripted echo tool plus the
/// dot-local coding tools bound to the session workspace, the optional
/// mcp.json attach, and the scripted provider or the live providers. Live
/// providers register only when their env key is present (Anthropic rides
/// the OpenAI-compatible preset under id anthropic: the native AddAnthropic
/// never registers alongside it, last registration wins); keys flow from
/// the environment through IConfiguration binding only (see
/// bridgeProviderKeys), never printed or persisted.
/// <param name="services">The container to add the host to.</param>
/// <param name="dbPath">The SQLite file path. Created with its directory on first use.</param>
/// <param name="start">The start plan: ask policy, MCP config path, and provider/model picks.</param>
/// <param name="configuration">The application configuration providers bind from.</param>
/// <param name="useScripted">True for the scripted transport: no live branches.</param>
let private buildServices
    (services: IServiceCollection)
    (dbPath: string)
    (start: DotStart)
    (configuration: IConfiguration)
    (useScripted: bool)
    : unit =
    ArgumentNullException.ThrowIfNull(configuration)

    SqliteServiceCollectionExtensions.UseSqlite(services, dbPath) |> ignore

    LegateServiceCollectionExtensions.AddLegate(
        services,
        Action<LegateBuilder>(fun builder ->
            let workspaceOptions = HostDirectoryWorkspaceRuntimeOptions()
            workspaceOptions.Root <- Environment.CurrentDirectory

            builder.Workspace.UseRuntime(HostDirectoryWorkspaceRuntime(workspaceOptions, null, null))
            |> ignore

            if start.Ask then
                builder.Permissions.UsePolicy(AskAllPolicy()) |> ignore
            else
                builder.Permissions.UsePolicy(AllowAllPermissionPolicy()) |> ignore

            builder.Tools.AddSource(
                StaticSource(
                    ResizeArray<AITool>(
                        [|
                            scriptedEchoTool ()
                            slowEchoTool ()
                        |]
                    )
                )
            )
            |> ignore

            builder.Tools.AddSource<CodingTools.CodingToolSource>() |> ignore

            builder.Tools.AddSource<DotSkills.SkillToolSource>() |> ignore

            if not useScripted then
                if hasKey "ANTHROPIC_API_KEY" then
                    builder.Llm.AddAnthropicCompatible(configuration) |> ignore

                if hasKey "OPENAI_API_KEY" then
                    builder.Llm.AddOpenAI(configuration) |> ignore

                if hasKey "GOOGLE_API_KEY" then
                    Legate.Llm.GoogleServiceCollectionExtensions.AddGoogle(builder.Services, configuration)
                    |> ignore

            match start.McpPath with
            | null -> ()
            | path ->
                if File.Exists(path) then
                    builder.Tools.AddMcpServersFromConfig(path) |> ignore
                else
                    raise (InvalidOperationException($"The --mcp path '{path}' does not exist.")))
    )
    |> ignore

    if useScripted then
        let client = scriptedClient ()

        services.AddSingleton<ILlmProvider>(StubScriptedProvider(client) :> ILlmProvider)
        |> ignore

        services.AddSingleton<IChatClient>(client :> IChatClient) |> ignore
    else
        services.AddSingleton<IChatClient>(
            Func<IServiceProvider, IChatClient>(fun provider -> selectLiveClient provider start)
        )
        |> ignore

/// Stops the MCP lifecycle sources so subprocess servers exit with dot.
/// Mirrors the samples/LegateCli precedent; host-local, like the scripted
/// client.
/// <param name="provider">The built container.</param>
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
            // Scripted unless live mode was asked for with keys to serve
            // it: --scripted pins scripted, and no keys fall back to it.
            let useScripted = start.Scripted || not (hasLiveKey ())

            let application = Host.CreateApplicationBuilder()
            buildServices application.Services dbPath start application.Configuration useScripted

            use host = application.Build()

            // Resolve before starting: the resolve triggers the session
            // router wiring, which must land before the actor system spawns
            // its router.
            let client = host.Services.GetRequiredService<SessionClient>()
            let agents = host.Services.GetRequiredService<IAgentStore>()
            let packages = host.Services.GetRequiredService<IAgentPackageStore>()

            try
                do! host.StartAsync(CancellationToken.None)

                let! exit =
                    task {
                        try
                            if start.ListSessions then
                                return! listSessionsAsync client CancellationToken.None
                            else
                                let registered =
                                    host.Services.GetServices<ILlmProvider>()
                                    |> Seq.filter (fun candidate -> not (isNull (box candidate)))
                                    |> List.ofSeq

                                let initialModel, options =
                                    if useScripted then
                                        ModelReference.Parse("scripted/scripted"),
                                        ([
                                            {
                                                Id = "scripted"
                                                DefaultModel = "scripted"
                                                EnvVar = null
                                            }
                                        ]
                                        : ReplEngine.ProviderOption list)
                                    else
                                        let opts = ReplEngine.describeProviders registered
                                        ReplEngine.selectReference opts start.Provider start.Model, opts

                                let waitBound = TimeSpan.FromMinutes(start.WaitMinutes)

                                let engine =
                                    ReplEngine.Engine(
                                        client,
                                        agents,
                                        packages,
                                        Console.In,
                                        Console.Out,
                                        waitBound,
                                        initialModel,
                                        options
                                    )

                                return! engine.RunAsync(start.Resume, CancellationToken.None)
                        with
                        | :? SqliteLockedException as locked ->
                            reportLocked dbPath locked
                            return 1
                        | error ->
                            Console.Error.WriteLine($"dot: {error.Message}")
                            return 1
                    }

                do! stopToolSources host.Services
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
        bridgeProviderKeys ()
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
