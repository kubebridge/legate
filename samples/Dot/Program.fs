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
// /tree, /fork, /clone, /session, /export, /<template>, and /quit commands. Live providers (#307, #329) register
// only when their env key is present (Anthropic through the OpenAI-compatible
// preset under id anthropic, OpenAI, Google, Ollama Cloud), with keys flowing from the
// environment through IConfiguration binding only, never printed or
// persisted; with exactly one key set dot just works, with several
// --provider picks, else the default order anthropic, openai, google, ollamacloud wins,
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
/// else the per-user default (DotConfig.defaultDbPath:
/// %APPDATA%/dot/dot.db on Windows, DOT_CONFIG_HOME/dot else
/// $XDG_CONFIG_HOME/dot else ~/.config/dot plus dot.db on Unix).
/// Directory creation rides on SqliteDatabase.Open.
/// <returns>The database file path.</returns>
let resolveDbPath () : string =
    let defaultPath () : string = DotConfig.defaultDbPath ()

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
        /// Wins over same-name Dot:Mcp:Servers YAML entries.
        McpPath: string | null
        /// One-shot query for -p|--print, or null for the REPL. Runs one
        /// Headless-shaped turn (PromptAndWaitAsync with the Headless exit
        /// map) instead of entering the REPL.
        Print: string | null
        /// Output shape for the one-shot path: "text" prints only the final
        /// answer to stdout, "json" streams session events as JSONL to
        /// stdout with diagnostics on stderr. Ignored without -p|--print.
        Mode: string
        /// Print the redacted effective configuration (Dot plus Legate
        /// sections, secret values masked) to stdout and exit 0 without
        /// opening the database or the network.
        PrintConfig: bool
        /// Force plain console output even on a real terminal: skips the
        /// fullscreen TUI shell and runs the REPL or one-shot path.
        NoTui: bool
    }

/// Builds the --help text over the resolved database path: usage, session
/// storage, the provider and model flags with the default order, the
/// unsandboxed workspace with its container-sandboxing expectation,
/// the --ask escape, and the one-shot print/JSON modes.
/// <param name="dbPath">The resolved database file path.</param>
/// <returns>The usage text.</returns>
let private helpText (dbPath: string) : string =
    "Usage: Dot [--provider <id>] [--model <provider/model>] [--resume <session-id>] [--sessions|--list] [-p|--print <query>] [--mode text|json] [--scripted] [--wait-minutes <n>] [--ask] [--mcp <path>] [--print-config] [--no-tui] "
    + storageHelp dbPath
    + " Providers anthropic, openai, google, and ollamacloud register only when ANTHROPIC_API_KEY, OPENAI_API_KEY, GOOGLE_API_KEY, or OLLAMA_API_KEY is set (keys flow from the environment through Legate:Llm:Providers:<id>:ApiKey binding only, never printed or persisted). With exactly one key set dot just works; with several, --provider picks, else the default order anthropic, openai, google, ollamacloud wins; --model <provider/model> overrides the model (--provider/--model apply to live mode; --scripted pins the scripted transport). /model lists the options mid-session and switches without losing the transcript. "
    + " Tools run against the working directory through the host-directory runtime with no sandbox: sandbox the run in a container (as pi does) or pass --ask for per-call approval (allow once, allow for session, deny). The workspace root fence stays on in every mode. "
    + " One-shot print: -p|--print <query> runs one Headless-shaped turn (PromptAndWaitAsync under --wait-minutes with the Dot agent) instead of the REPL, printing only the final answer to stdout and exiting 0 completed, 2 aborted, 1 failed, 3 anything else (--resume attaches instead of opening; --sessions still lists). JSON stream: --mode json with -p streams session events as JSONL to stdout (one $type-polymorphic event per line, DotExport options) for scripting as dot --mode json -p \"<query>\" | <jsonl-parser>, with every human diagnostic on stderr; without -p --mode is ignored and the REPL runs in text."
    + " Configuration: dot reads optional YAML files in order user appsettings.yaml < user appsettings.local.yaml < project ./.dot/appsettings.yaml < project ./.dot/appsettings.local.yaml (missing files are never errors), then environment variables, then CLI flags strongest; --print-config prints the effective Dot plus Legate configuration with secret values masked and exits 0. MCP servers declare under Dot:Mcp:Servers (stdio command/args/env, remote url/headers) or attach via --mcp <path>; a same-name YAML entry loses to the file. See samples/Dot/README.md for the schema."
    + " Display mode: dot opens the fullscreen TUI shell on a real terminal; --no-tui, piped (redirected) stdout, NO_COLOR, TERM=dumb (empty/unknown TERM forces plain only off Windows: Windows consoles rarely set TERM but support fullscreen), any CI marker, -p/--print, --mode json with -p, --sessions/--list, or --print-config run the plain output instead (the piped path loads no TUI code and stays byte-identical with no alternate-screen escapes). The plain REPL names its mode on startup as plain (<reason>) with <reason> one of --no-tui, redirected-stdout, NO_COLOR, TERM=dumb, CI; fullscreen means the TUI owns the screen. Tiny terminals (narrower than 40 columns or shorter than 12 rows) show a minimum-size notice and keep running; quit, abort, error, and Ctrl+C paths restore the terminal."

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
    let mutable print: string | null = null
    let mutable mode = "text"
    let mutable printConfig = false
    let mutable noTui = false

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
        | "-p"
        | "--print" -> print <- take "-p"
        | "--mode" ->
            let raw = take "--mode"

            if String.Equals(raw, "text", StringComparison.OrdinalIgnoreCase) then
                mode <- "text"
            elif String.Equals(raw, "json", StringComparison.OrdinalIgnoreCase) then
                mode <- "json"
            else
                raise (ArgumentException("The --mode flag needs text or json (--mode text|json).", "--mode"))
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
        | "--print-config" -> printConfig <- true
        | "--no-tui" -> noTui <- true
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

    let printValue: string | null =
        match print with
        | null -> null
        | raw when String.IsNullOrWhiteSpace raw ->
            raise (ArgumentException("The -p flag needs a non-empty query.", "-p"))
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
        Print = printValue
        Mode = mode
        PrintConfig = printConfig
        NoTui = noTui
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
/// tool over the sample review package (skill-load, skill-missing), the
/// context-file rewrite (ctx-edit), and the YAML MCP fixture attach
/// (mcp-echo, served by the Dot:Mcp:Servers fixture server). Each probe replaces the default
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
    | "mcp-echo" ->
        [
            ToolCall("call-mcp-echo", "fixture_echo", [ "text", "hello-fixture-336" :> obj ])
            Text "mcp-echo done"
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
            "mcp-echo"
        ]

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

    /// The turn's current input: the latest user message in the received
    /// history. Probes trigger on current intent, never on stale history:
    /// restored conversation context repeats earlier turns' text, so
    /// scanning every message would re-fire served probes and shadow the
    /// live prompt (a prior ctx-check would swallow a current ctx-edit).
    /// <param name="messages">The chat messages.</param>
    /// <returns>The latest user message, or None when the history holds none.</returns>
    let latestUser (messages: ChatMessage seq) : ChatMessage option =
        if isNull (box messages) then
            None
        else
            messages
            |> Seq.filter (fun message ->
                not (isNull (box message))
                && not (isNull (box message.Text))
                && message.Role = ChatRole.User)
            |> Seq.tryLast

    /// True when the turn's current input mentions the marker.
    /// <param name="messages">The chat messages.</param>
    /// <param name="marker">The probe marker.</param>
    /// <returns>True when the latest user message mentions the marker.</returns>
    let mentionsCurrent (messages: ChatMessage seq) (marker: string) : bool =
        match latestUser messages with
        | None -> false
        | Some current -> current.Text.Contains(marker, StringComparison.Ordinal)

    /// Refills the queue with the first unserved probe the current input
    /// mentions, if any. ctx-check refills on every mention (never marked
    /// served): the answer names the context markers the current system
    /// prompt carries, proving the runtime re-read the files this turn.
    /// <param name="messages">The chat messages.</param>
    let serveProbe (messages: ChatMessage seq) : unit =
        if mentionsCurrent messages "ctx-check" then
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
                |> List.tryFind (fun marker -> not (served.Contains marker) && mentionsCurrent messages marker)
            with
            | None -> ()
            | Some marker ->
                served.Add marker |> ignore
                steps.Clear()

                for step in probeScript marker do
                    steps.Enqueue step

    /// True when the turn's current input asks for the slow-turn probe.
    /// Scoped to the latest user message like the probe triggers: a stale
    /// history mention must not slow every later turn.
    /// <param name="messages">The chat messages.</param>
    /// <returns>True when the turn should wait before answering.</returns>
    let isSlowTurn (messages: ChatMessage seq) : bool = mentionsCurrent messages "slow-turn"

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
/// the context rewrite (ctx-edit), or the MCP fixture probe (mcp-echo,
/// served by the Dot:Mcp:Servers fixture server) swap the queue for that probe's tool
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
// Provider keys (issues 307, 328)

/// Whether any live provider key is present in the effective config
/// (YAML files, Dot-section keys, or plain env keys).
/// <param name="config">The merged configuration.</param>
/// <returns>True when live mode can serve.</returns>
let private hasLiveKey (config: IConfiguration) : bool = DotConfig.hasLiveKey config

/// Bridges the plain provider env keys and Dot-section file keys into the
/// Legate provider sections, so `export ANTHROPIC_API_KEY` satisfies the
/// Legate:Llm:Providers:anthropic:ApiKey binding the Add* registration
/// reads (AGENTS.md Agent Login names both sources). An explicit compound
/// value already in the environment or files wins: the bridge only fills
/// blanks. Process memory only: nothing is printed or persisted.
/// <param name="config">The merged configuration.</param>
let private bridgeProviderKeys (config: IConfiguration) : unit = DotConfig.bridgeProviderKeys config

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
/// mcp.json attach plus the Dot:Mcp:Servers YAML section (a same-name YAML
/// entry loses to the file with a logged skip), and the scripted provider or the live providers. Live
/// providers register only when their env key is present (Anthropic rides
/// the OpenAI-compatible preset under id anthropic: the native AddAnthropic
/// never registers alongside it, last registration wins; Ollama Cloud rides
/// the OpenAI-compatible preset under id ollamacloud); keys flow from
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
            builder.UseConfiguration(configuration.GetSection("Legate")) |> ignore

            let workspaceOptions = HostDirectoryWorkspaceRuntimeOptions()
            workspaceOptions.Root <- DotConfig.resolveWorkspaceRoot configuration

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
                if DotConfig.hasProviderKey configuration "anthropic" then
                    builder.Llm.AddAnthropicCompatible(configuration) |> ignore

                if DotConfig.hasProviderKey configuration "openai" then
                    builder.Llm.AddOpenAI(configuration) |> ignore

                if DotConfig.hasProviderKey configuration "google" then
                    Legate.Llm.GoogleServiceCollectionExtensions.AddGoogle(builder.Services, configuration)
                    |> ignore

                if DotConfig.hasProviderKey configuration "ollamacloud" then
                    builder.Llm.AddOllamaCloud(configuration) |> ignore

            // MCP servers (issue 336): the --mcp file loads first (CLI
            // strongest), then the Dot:Mcp:Servers YAML section appends
            // through AddMcp, so the Legate:Tools:Mcp section, the YAML
            // section, and the file compose through the deferred Configure
            // path. A same-name YAML entry loses to the file with a logged
            // skip. Startup logs server names plus the winning source on
            // stderr, never values; a server that will not start keeps the
            // whole-source degrade (zero MCP tools with a logged reason
            // naming it) and dot continues.
            let fileKeys, hasFile =
                match start.McpPath with
                | null -> Set.empty, false
                | path ->
                    if File.Exists(path) then
                        builder.Tools.AddMcpServersFromConfig(path) |> ignore
                        let keys = DotMcp.fileServerKeys path

                        for name in keys do
                            Console.Error.WriteLine($"dot: MCP server '{name}' attached from --mcp file.")

                        keys, true
                    else
                        raise (InvalidOperationException($"The --mcp path '{path}' does not exist."))

            let yamlServers = DotMcp.loadYamlServers configuration

            let freshYaml =
                yamlServers
                |> List.filter (fun server ->
                    // loadYamlServers validates names non-empty, so the
                    // fallback keeps a server rather than dropping it.
                    match box server.Name with
                    | :? string as raw when not (String.IsNullOrWhiteSpace raw) ->
                        if fileKeys.Contains(raw) then
                            Console.Error.WriteLine(
                                $"dot: MCP server '{raw}' from Dot:Mcp:Servers shadowed by --mcp file (file wins)."
                            )

                            false
                        else
                            true
                    | _ -> true)

            for server in freshYaml do
                Console.Error.WriteLine($"dot: MCP server '{server.Name}' attached from Dot:Mcp:Servers.")

            let legateServers =
                configuration.GetSection(McpOptions.ConfigurationSectionPath + ":Servers")

            for child in legateServers.GetChildren() do
                match Option.ofObj child["Name"] with
                | None -> ()
                | Some raw when String.IsNullOrWhiteSpace raw -> ()
                | Some raw ->
                    Console.Error.WriteLine($"dot: MCP server '{raw.Trim()}' attached from Legate:Tools:Mcp.")

            if
                hasFile
                || not freshYaml.IsEmpty
                || not (Seq.isEmpty (legateServers.GetChildren()))
            then
                builder.AddMcp(
                    configuration,
                    Action<McpOptions>(fun options ->
                        for server in freshYaml do
                            options.Servers.Add(server))
                )
                |> ignore)
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

        // The runtime keeps the host pipeline unless the host explicitly
        // supplies a model-aware client factory. Dot owns this opt-in.
        services.AddSingleton<Func<ModelReference, IChatClient>>(
            Func<IServiceProvider, Func<ModelReference, IChatClient>>(fun provider ->
                Func<ModelReference, IChatClient>(fun reference ->
                    let selected =
                        provider.GetServices<ILlmProvider>()
                        |> Seq.find (fun candidate -> candidate.Id = reference.Provider)

                    selected.CreateChatClient(reference, null)))
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

// ──────────────────────────────────────────────────────────────────────────
// One-shot print/JSON (issue 310)

/// The exit code for a settled turn result: 0 when the turn completed, 2
/// when the host aborted it, 1 when it failed, 3 for anything else. Copies
/// the samples/Headless map verbatim, including the 3-else branch, so
/// exit-code drift is a copy-paste diff.
/// <param name="status">The settled turn status.</param>
/// <returns>The process exit code.</returns>
let private exitFor (status: TurnStatus) : int =
    match status with
    | TurnStatus.Completed -> 0
    | TurnStatus.Aborted -> 2
    | TurnStatus.Failed -> 1
    | TurnStatus.Pending
    | TurnStatus.Running
    | TurnStatus.Suspended
    | _ -> 3

/// Opens the one-shot session: the Dot agent row, durable SQLite, the
/// resolved host instruction files, and the --wait-minutes timeout so
/// PromptAndWaitAsync runs under the flag. No AutoClose: the session stays
/// durable for --sessions/--resume like the REPL.
/// <param name="client">The session client.</param>
/// <param name="agentId">The model-carrying agent the session converses with.</param>
/// <param name="waitBound">The settle wait bound from --wait-minutes.</param>
/// <param name="cancellationToken">Abandons the open.</param>
/// <returns>The opened session.</returns>
let private openPrintSessionAsync
    (client: SessionClient)
    (agentId: AgentId)
    (waitBound: TimeSpan)
    (cancellationToken: CancellationToken)
    : Task<Session> =
    task {
        let options = SessionOptions()
        options.Title <- "dot -p"
        options.HostInstructionFiles <- DotContext.resolveHostInstructionFiles Environment.CurrentDirectory
        options.Timeout <- Nullable<TimeSpan>(waitBound)
        return! SessionClientOperations.OpenSessionAsync(client, agentId, options, cancellationToken)
    }

/// Resolves the one-shot session: opens a new print session, or attaches
/// the --resume session after probing its journal. The probe throws
/// SessionNotFoundException for unknown ids, so scripting fails instead of
/// silently opening a new session.
/// <param name="client">The session client.</param>
/// <param name="agentId">The model-carrying agent new sessions converse with.</param>
/// <param name="start">The start plan: resume pick and wait bound.</param>
/// <param name="waitBound">The settle wait bound from --wait-minutes.</param>
/// <param name="cancellationToken">Abandons the resolve.</param>
/// <returns>The session id and the exclusive event cursor before the turn.</returns>
let private resolvePrintSessionAsync
    (client: SessionClient)
    (agentId: AgentId)
    (start: DotStart)
    (waitBound: TimeSpan)
    (cancellationToken: CancellationToken)
    : Task<SessionId * int64> =
    task {
        match start.Resume with
        | null ->
            let! created = openPrintSessionAsync client agentId waitBound cancellationToken
            return created.Id, 0L
        | resumeText ->
            let trimmed = resumeText.Trim()
            let mutable parsed = Unchecked.defaultof<SessionId>

            if not (SessionId.TryParse(trimmed, &parsed)) then
                raise (ArgumentException($"The --resume value '{trimmed}' is not a session id.", "--resume"))

            let! events = DotExport.readAllEventsAsync client parsed cancellationToken
            let mutable cursor = 0L

            if not (isNull (box events)) then
                for evt in events do
                    if not (isNull (box evt)) && evt.Sequence.HasValue && evt.Sequence.Value > cursor then
                        cursor <- evt.Sequence.Value

            return parsed, cursor
    }

/// True when the event terminates the one-shot turn.
let private isPrintTerminal (evt: SessionEvent) : bool =
    not (isNull (box evt))
    && (evt :? TurnCompletedEvent || evt :? TurnAbortedEvent || evt :? TurnFailedEvent)

/// Runs the -p one-shot in text mode: one PromptAndWaitAsync turn, only the
/// final answer on stdout, SESSION/RESULT diagnostics on stderr, Headless
/// exit map.
/// <param name="client">The session client.</param>
/// <param name="sessionId">The session to prompt.</param>
/// <param name="query">The one-shot query. Must not be empty.</param>
/// <returns>The process exit code.</returns>
let private runPrintTextAsync (client: SessionClient) (sessionId: SessionId) (query: string) : Task<int> =
    task {
        try
            let! result =
                SessionClientExtensions.PromptAndWaitAsync(
                    client,
                    sessionId,
                    UserMessage.Text query,
                    CancellationToken.None
                )

            Console.Error.WriteLine($"SESSION {sessionId}")
            Console.Error.WriteLine($"RESULT {result.Status}")

            if not (isNull (box result.AssistantText)) then
                Console.Out.WriteLine(result.AssistantText)
                Console.Out.Flush()

            return exitFor result.Status
        with
        | :? DeadlineExceededException as exceeded ->
            Console.Error.WriteLine($"dot: DEADLINE {exceeded.Message} (the turn keeps running).")
            return 1
        | :? PermissionApprovalRequiredException as suspended ->
            Console.Error.WriteLine(
                $"dot: the turn needs approval for tool '{suspended.ToolName}' (request {suspended.RequestId}): answer in the REPL or rerun without --ask."
            )

            return 1
        | error ->
            Console.Error.WriteLine($"dot: {error.Message}")
            return 1
    }

/// Runs the -p one-shot in JSON mode: streams session events from the
/// pre-turn cursor as JSONL on stdout (one $type-polymorphic event per
/// line, DotExport options) while PromptAndWaitAsync settles the turn, with
/// every human diagnostic on stderr. Stdout stays pure JSONL so
/// dot --mode json -p "<query>" | <jsonl-parser> proves scriptability.
/// <param name="client">The session client.</param>
/// <param name="sessionId">The session to prompt.</param>
/// <param name="startCursor">The exclusive cursor before the turn.</param>
/// <param name="query">The one-shot query. Must not be empty.</param>
/// <returns>The process exit code.</returns>
let private runPrintJsonAsync
    (client: SessionClient)
    (sessionId: SessionId)
    (startCursor: int64)
    (query: string)
    : Task<int> =
    task {
        use streamCts = new CancellationTokenSource()
        let collected = ResizeArray<SessionEvent>()
        // Consumer dedup (issue 385): durable identities already streamed
        // as JSONL print once; duplicate delivery repeats the identical
        // pair and never a second logical event.
        let seen = HashSet<string>()

        let streamTask =
            task {
                try
                    let stream =
                        SessionClientOperations.Subscribe(client, sessionId, startCursor, streamCts.Token)

                    let enumerator = stream.GetAsyncEnumerator(streamCts.Token)

                    try
                        let mutable go = true

                        while go do
                            try
                                let! has = enumerator.MoveNextAsync().AsTask()

                                if not has then
                                    go <- false
                                else
                                    let evt = enumerator.Current

                                    if not (isNull (box evt)) then
                                        let duplicate =
                                            match DotDedup.durableKeyOf evt with
                                            | None -> false
                                            | Some identity ->
                                                if seen.Contains identity then
                                                    true
                                                else
                                                    seen.Add identity |> ignore
                                                    false

                                        if not duplicate then
                                            collected.Add(evt)
                                            Console.Out.WriteLine(DotExport.toJsonLine evt)
                                            Console.Out.Flush()
                            with :? OperationCanceledException ->
                                go <- false
                    finally
                        try
                            enumerator.DisposeAsync().AsTask() |> ignore
                        with _ ->
                            ()
                with
                | :? OperationCanceledException -> ()
                | error -> Console.Error.WriteLine($"dot: json stream failed: {error.Message}")
            }

        let! settled, failure =
            task {
                try
                    let! result =
                        SessionClientExtensions.PromptAndWaitAsync(
                            client,
                            sessionId,
                            UserMessage.Text query,
                            CancellationToken.None
                        )

                    return Some result, None
                with error ->
                    return None, Some error
            }

        try
            let deadline = DateTimeOffset.UtcNow.AddSeconds(10.0)
            let mutable waited = false

            while not waited do
                let terminal = lock collected (fun () -> collected |> Seq.exists isPrintTerminal)

                if terminal || DateTimeOffset.UtcNow >= deadline then
                    waited <- true
                else
                    do! Task.Delay(50)
        with _ ->
            ()

        try
            streamCts.Cancel()
        with _ ->
            ()

        try
            do! streamTask.WaitAsync(TimeSpan.FromSeconds(5.0))
        with _ ->
            ()

        Console.Error.WriteLine($"SESSION {sessionId}")

        match settled, failure with
        | Some result, _ ->
            Console.Error.WriteLine($"RESULT {result.Status}")

            // Prefix-aware settlement text (issue 385): streamed-then-success
            // reports only the genuinely unrendered suffix, and
            // failure/abort reports no text (the truthful terminal outcome
            // travels on the RESULT line and the journaled terminal event).
            if
                result.Status = TurnStatus.Completed
                && not (String.IsNullOrEmpty result.AssistantText)
            then
                let prefix =
                    lock collected (fun () ->
                        collected
                        |> Seq.filter (fun evt -> evt :? TextDeltaEvent)
                        |> Seq.choose (fun evt ->
                            let raw: string | null = (evt :?> TextDeltaEvent).Text

                            match raw with
                            | null -> None
                            | text -> Some text)
                        |> String.concat "")

                let suffix = DotDedup.settlementSuffix prefix result.AssistantText

                if suffix <> "" then
                    Console.Error.WriteLine($"TEXT {suffix}")

            return exitFor result.Status
        | None, Some(:? DeadlineExceededException as exceeded) ->
            Console.Error.WriteLine($"dot: DEADLINE {exceeded.Message} (the turn keeps running).")
            return 1
        | None, Some(:? PermissionApprovalRequiredException as suspended) ->
            Console.Error.WriteLine(
                $"dot: the turn needs approval for tool '{suspended.ToolName}' (request {suspended.RequestId}): answer in the REPL or rerun without --ask."
            )

            return 1
        | None, Some error ->
            Console.Error.WriteLine($"dot: {error.Message}")
            return 1
        | None, None ->
            Console.Error.WriteLine("dot: the one-shot turn resolved with no outcome.")
            return 1
    }

/// Runs the -p one-shot: opens (or --resume attaches) one SQLite-backed
/// session with the Dot agent, runs the single prompt through
/// PromptAndWaitAsync under --wait-minutes, and exits with the Headless map
/// (0 completed / 2 aborted / 1 failed, 3 anything else). Text mode prints
/// only the final answer to stdout; JSON mode streams Subscribe-from-cursor
/// JSONL to stdout with diagnostics on stderr.
/// <param name="client">The session client.</param>
/// <param name="agents">The agent store.</param>
/// <param name="packages">The package store the sample skill uploads to.</param>
/// <param name="start">The start plan: print query, mode, resume, and wait bound.</param>
/// <param name="initialModel">The model the Dot agent carries.</param>
/// <param name="cancellationToken">Abandons the turn.</param>
/// <returns>The process exit code.</returns>
let private runPrintAsync
    (client: SessionClient)
    (agents: IAgentStore)
    (packages: IAgentPackageStore)
    (start: DotStart)
    (initialModel: ModelReference)
    (cancellationToken: CancellationToken)
    : Task<int> =
    task {
        ArgumentNullException.ThrowIfNull(client)
        ArgumentNullException.ThrowIfNull(agents)
        ArgumentNullException.ThrowIfNull(packages)
        ArgumentNullException.ThrowIfNull(start)

        let query =
            match start.Print with
            | null -> raise (ArgumentException("The -p flag needs a non-empty query.", "-p"))
            | raw when String.IsNullOrWhiteSpace raw ->
                raise (ArgumentException("The -p flag needs a non-empty query.", "-p"))
            | raw -> raw.Trim()

        let waitBound = TimeSpan.FromMinutes(start.WaitMinutes)

        let isJson = String.Equals(start.Mode, "json", StringComparison.OrdinalIgnoreCase)

        try
            let! agentId = ReplEngine.ensureModelAgentAsync agents packages initialModel cancellationToken
            let! sessionId, startCursor = resolvePrintSessionAsync client agentId start waitBound cancellationToken

            if isJson then
                return! runPrintJsonAsync client sessionId startCursor query
            else
                return! runPrintTextAsync client sessionId query
        with
        | :? SqliteLockedException as locked -> return raise locked
        | :? ArgumentException as invalid ->
            Console.Error.WriteLine(invalid.Message)
            return 2
        | :? SessionNotFoundException as missing ->
            Console.Error.WriteLine($"dot: {missing.Message}")
            return 1
        | :? InvalidSessionStateException as invalid ->
            Console.Error.WriteLine($"dot: {invalid.Message}")
            return 1
        | error ->
            Console.Error.WriteLine($"dot: {error.Message}")
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
    let runAsync (start: DotStart) : Task<int> =
        task {
            let application = Host.CreateApplicationBuilder()

            // The fullscreen renderer must be the sole terminal writer. The
            // default asynchronous console logger otherwise writes into the
            // composer after a frame has been painted.
            if
                not start.ListSessions
                && isNull (box start.Print)
                && DotTuiMode.shouldUseTui (DotTuiMode.readRequest start.NoTui)
            then
                Microsoft.Extensions.Logging.LoggingBuilderExtensions.ClearProviders(application.Logging)
                |> ignore

            // YAML scopes weakest first, then the environment again so env
            // wins over files; CLI flags win through the effective resolve
            // below (never through the configuration itself).
            let configBuilder = application.Configuration :> IConfigurationBuilder

            for path in DotConfig.orderedPaths Environment.CurrentDirectory do
                configBuilder.Add(DotConfig.yamlSource path) |> ignore

            configBuilder.AddEnvironmentVariables() |> ignore

            let configuration = application.Configuration :> IConfiguration

            try
                DotConfig.bridgeProviderKeys configuration
                // The bridge fills compound bindings into process memory
                // after the builder already loaded the environment, so
                // reload: the Add* registration snapshots keys from this
                // same configuration object.
                (application.Configuration :> IConfigurationRoot).Reload()

                let provider = DotConfig.resolveProvider configuration start.Provider
                let model = DotConfig.resolveModel configuration start.Model
                let ask = DotConfig.resolveAsk configuration start.Ask

                let effective =
                    { start with
                        Provider = provider
                        Model = model
                        Ask = ask
                    }

                if effective.PrintConfig then
                    let dbPath = DotConfig.resolveDbPath configuration
                    let workspaceRoot = DotConfig.resolveWorkspaceRoot configuration
                    DotConfig.printEffective configuration provider model dbPath workspaceRoot ask
                    return 0
                else
                    // Scripted unless live mode was asked for with keys to
                    // serve it: --scripted pins scripted, and no keys fall
                    // back to it.
                    let useScripted = effective.Scripted || not (hasLiveKey configuration)
                    let dbPath = DotConfig.resolveDbPath configuration
                    buildServices application.Services dbPath effective configuration useScripted

                    use host = application.Build()

                    let agents = host.Services.GetRequiredService<IAgentStore>()
                    let packages = host.Services.GetRequiredService<IAgentPackageStore>()

                    try
                        // Validate the selected model client before host
                        // startup so command-line configuration errors remain
                        // deterministic and do not start background workers.
                        host.Services.GetRequiredService<IChatClient>() |> ignore
                        do! host.StartAsync(CancellationToken.None)
                        let client = host.Services.GetRequiredService<SessionClient>()

                        let! exit =
                            task {
                                try
                                    if effective.ListSessions then
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

                                                ReplEngine.selectReference opts effective.Provider effective.Model, opts

                                        if not (isNull (box effective.Print)) then
                                            return!
                                                runPrintAsync
                                                    client
                                                    agents
                                                    packages
                                                    effective
                                                    initialModel
                                                    CancellationToken.None
                                        elif DotTuiMode.isHeadlessSmokeRequested () then
                                            // CI-only headless TUI smoke (issue 335): the
                                            // DOT_TUI_SMOKE environment trigger runs the
                                            // boot/render-one-turn/exit proof with piped
                                            // stdin instead of the fullscreen loop, so CI
                                            // proves the shell without a TTY. Never a CLI
                                            // flag: --help and the pipe contract never
                                            // change.
                                            let waitBound = TimeSpan.FromMinutes(effective.WaitMinutes)

                                            return!
                                                DotTui.runHeadlessSmokeAsync
                                                    client
                                                    agents
                                                    packages
                                                    initialModel
                                                    options
                                                    waitBound
                                                    CancellationToken.None
                                        else
                                            // Fullscreen shell (issues 330-332): the one-shot (-p), list,
                                            // and config paths never touch the TUI so piped stdout stays
                                            // byte-identical; the interactive REPL opens the stdlib-only
                                            // fullscreen input loop only when the pure selector clears
                                            // (real TTY, no NO_COLOR/TERM=dumb/CI/--no-tui), else it runs
                                            // the current plain REPL and names the reason.
                                            let request = DotTuiMode.readRequest effective.NoTui

                                            if DotTuiMode.shouldUseTui request then
                                                let waitBound = TimeSpan.FromMinutes(effective.WaitMinutes)

                                                return!
                                                    DotTui.runSpikeAsync
                                                        client
                                                        agents
                                                        packages
                                                        initialModel
                                                        options
                                                        configuration
                                                        waitBound
                                                        CancellationToken.None
                                            else
                                                let waitBound = TimeSpan.FromMinutes(effective.WaitMinutes)

                                                Console.Error.WriteLine(
                                                    $"dot: running {DotTuiMode.describeSelection request} REPL (--no-tui forces plain; see --help for the mode list)."
                                                )

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

                                                return! engine.RunAsync(effective.Resume, CancellationToken.None)
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
            with error ->
                Console.Error.WriteLine($"dot: {error.Message}")
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

        runAsync start |> fun runner -> runner.GetAwaiter().GetResult()
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
