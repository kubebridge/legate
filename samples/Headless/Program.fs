// SPDX-License-Identifier: Apache-2.0
module Headless.Program

open System
open System.Collections.Generic
open System.Security.Cryptography
open System.Threading
open System.Threading.Tasks
open Headless.Loopback
open Legate
open Legate.Llm.OpenAI
open Legate.Storage.InMemory
open Legate.Workspace.Process
open Microsoft.Extensions.AI
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Hosting

// Headless host: opens one ordinary session with headless options
// (AutoClose, Structured outcome, allow-all permissions, a
// WebhookCompletionSink), runs a single prompt through PromptAndWait,
// maps the TurnResult to a process exit code, and proves the completion
// webhook reached the loopback receiver with a valid X-Legate-Signature:
// sha256=<hex>. Never a one-shot verb: headless stays sessions-with-options.
// Scripted transports by default (CI); live providers read keys from the
// environment through configuration binding only when --live is passed.

// ──────────────────────────────────────────────────────────────────────────
// Arguments

/// How the headless run was asked to start.
type HeadlessStart =
    {
        /// The single prompt text.
        Prompt: string
        /// Scripted transports instead of live providers.
        Scripted: bool
        /// Provider id for live mode; ignored scripted.
        Provider: string
        /// Model reference (provider/model) or null for the provider default.
        Model: string | null
        /// PromptAndWait bound in minutes.
        WaitMinutes: float
    }

/// Parses the headless arguments into a start plan. Unknown flags fail
/// with a usage error naming the flag; missing values fail naming the flag.
/// <param name="argv">The process arguments.</param>
/// <returns>The start plan.</returns>
let parseArgs (argv: string[]) : HeadlessStart =
    let mutable prompt = "headless smoke prompt"
    let mutable scripted = true
    let mutable provider = "openai"
    let mutable model: string | null = null
    let mutable waitMinutes = 5.0

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
        | "--live" -> scripted <- false
        | "--provider" -> provider <- take "--provider"
        | "--model" -> model <- take "--model"
        | "--wait-minutes" ->
            let raw = take "--wait-minutes"

            match Double.TryParse(raw) with
            | true, minutes when minutes > 0.0 -> waitMinutes <- minutes
            | _ ->
                raise (
                    ArgumentException("The --wait-minutes flag needs a positive number of minutes.", "--wait-minutes")
                )
        | "--help"
        | "-h" ->
            raise (
                ArgumentException(
                    "Usage: Headless [--prompt <text>] [--scripted|--live] [--provider <id>] [--model <provider/model>] [--wait-minutes <n>]",
                    "--help"
                )
            )
        | unknown -> raise (ArgumentException($"Unknown flag '{unknown}'.", unknown))

        index <- index + 1

    if String.IsNullOrWhiteSpace prompt then
        raise (ArgumentException("The --prompt flag needs a non-empty prompt.", "--prompt"))

    {
        Prompt = prompt.Trim()
        Scripted = scripted
        Provider = provider
        Model = model
        WaitMinutes = waitMinutes
    }

// ──────────────────────────────────────────────────────────────────────────
// Scripted transports (no keys, no external services)

/// Minimal inline scripted chat client: serves one queued answer, then
/// fails loudly. Mirrors the samples/LegateCli precedent; scripted support
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

/// Resolves the chat client for live mode: the selected provider serving
/// the selected model reference. Keys come from the environment through
/// configuration binding only.
let private selectClient (provider: IServiceProvider) (start: HeadlessStart) : IChatClient =
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

/// Builds the container per the start plan: InMemory stores, a process
/// workspace over the working directory, allow-all permissions (the facade
/// runner reads the container policy), and scripted or live providers.
let private buildServices
    (services: IServiceCollection)
    (configuration: IConfiguration)
    (database: InMemoryDatabase)
    (store: InMemorySessionStore)
    (start: HeadlessStart)
    : unit =
    LegateServiceCollectionExtensions.AddLegate(
        services,
        Action<LegateBuilder>(fun builder ->
            builder.Storage.UseSessionStore(store) |> ignore

            let workspaceOptions = ProcessWorkspaceRuntimeOptions()
            workspaceOptions.Root <- Environment.CurrentDirectory

            builder.Workspace.UseRuntime(ProcessWorkspaceRuntime(workspaceOptions, null, null))
            |> ignore

            builder.Permissions.UsePolicy(AllowAllPermissionPolicy()) |> ignore

            if start.Scripted then
                ()
            else
                match start.Provider.ToLowerInvariant() with
                | "anthropic" -> builder.Llm.AddAnthropicCompatible(configuration) |> ignore
                | "openai" -> builder.Llm.AddOpenAI(configuration) |> ignore
                | "google" ->
                    Legate.Llm.GoogleServiceCollectionExtensions.AddGoogle(builder.Services, configuration)
                    |> ignore
                | unknown ->
                    raise (
                        InvalidOperationException(
                            $"Unknown --provider '{unknown}': expected anthropic, openai, or google."
                        )
                    ))
    )
    |> ignore

    services.AddSingleton<ISessionEventStore>(InMemorySessionEventStore(database))
    |> ignore

    if start.Scripted then
        let client = new ScriptedClient("headless smoke answer")

        services.AddSingleton<ILlmProvider>(StubScriptedProvider(client) :> ILlmProvider)
        |> ignore

        services.AddSingleton<IChatClient>(client :> IChatClient) |> ignore
    else
        services.AddSingleton<IChatClient>(
            Func<IServiceProvider, IChatClient>(fun provider -> selectClient provider start)
        )
        |> ignore

/// Opens the headless session: AutoClose, Structured outcome, allow-all
/// permissions, the wait bound, and the webhook completion sink
/// snapshotted at open.
let private openHeadlessAsync
    (client: SessionClient)
    (sink: WebhookCompletionSink)
    (bound: TimeSpan)
    (cancellationToken: CancellationToken)
    : Task<Session> =
    task {
        let options = SessionOptions()
        options.AutoClose <- true
        options.Outcome <- SessionOutcomeMode.Structured
        options.Permissions <- AllowAllPermissionPolicy()
        options.CompletionSink <- sink
        options.Timeout <- Nullable<TimeSpan>(bound)

        return! SessionClientOperations.OpenSessionAsync(client, AgentId.New(), options, cancellationToken)
    }

/// Waits, without sleeping, for AutoClose to land: the close runs on the
/// actor after the settle the waiter already observed, so the store read
/// spins (Task.Yield) until Closed or the bound lapses.
/// <param name="store">The session store the run persists through.</param>
/// <param name="sessionId">The session that should close itself.</param>
/// <param name="bound">How long to wait for the Closed write.</param>
/// <returns>True when the session read Closed in bound.</returns>
let private waitForClosedAsync (store: InMemorySessionStore) (sessionId: SessionId) (bound: TimeSpan) : Task<bool> =
    let deadline = DateTimeOffset.UtcNow.Add(bound)

    let rec loop () : Task<bool> =
        task {
            let! current = (store :> ISessionStore).GetSession(TenantId.Default, sessionId, CancellationToken.None)

            let closed =
                match current with
                | null -> false
                | live -> live.State = SessionState.Closed

            if closed then
                return true
            elif DateTimeOffset.UtcNow >= deadline then
                return false
            else
                do! Task.Yield()
                return! loop ()
        }

    loop ()

[<EntryPoint>]
let main (argv: string[]) : int =
    let runAsync (start: HeadlessStart) : Task<int> =
        task {
            // The receiver starts first: the sink needs its URL at open, and
            // both share one random secret generated per run (never
            // committed, never logged).
            let secret = RandomNumberGenerator.GetBytes(32)
            use receiver = new LoopbackReceiver(secret)

            let sinkOptions = WebhookCompletionSinkOptions()
            sinkOptions.Endpoint <- Uri(receiver.Url("/hook"))
            sinkOptions.SigningSecret <- secret

            let guard = SsrfGuardOptions()
            guard.AllowList.Add("127.0.0.1") |> ignore

            use sink =
                new WebhookCompletionSink(sinkOptions, SystemHostAddressResolver() :> IHostAddressResolver, guard)

            let application = Host.CreateApplicationBuilder()
            let database = InMemoryDatabase()
            let store = InMemorySessionStore(database)
            buildServices application.Services application.Configuration database store start

            use host = application.Build()

            // Resolve before starting: the resolve triggers the session
            // router wiring, which must land before the actor system spawns
            // its router.
            let client = host.Services.GetRequiredService<SessionClient>()

            do! host.StartAsync(CancellationToken.None)

            let! exit =
                task {
                    try
                        let bound = TimeSpan.FromMinutes(start.WaitMinutes)
                        let! session = openHeadlessAsync client sink bound CancellationToken.None
                        Console.Out.WriteLine($"SESSION {session.Id}")

                        let! result =
                            SessionClientExtensions.PromptAndWaitAsync(
                                client,
                                session.Id,
                                UserMessage.Text start.Prompt,
                                CancellationToken.None
                            )

                        Console.Out.WriteLine($"RESULT {result.Status}")

                        let outcomeName =
                            match result.Outcome with
                            | null -> "none"
                            | outcome -> outcome.GetType().Name

                        Console.Out.WriteLine($"OUTCOME {outcomeName}")

                        if not (String.IsNullOrEmpty result.AssistantText) then
                            Console.Out.WriteLine($"TEXT {result.AssistantText}")

                        let code = exitFor result.Status

                        // The sink delivers in the background: wait
                        // event-driven (never a sleep) for the first
                        // signature-valid delivery.
                        let! verified = receiver.WaitForVerifiedAsync(TimeSpan.FromSeconds(60.0))

                        match verified with
                        | None ->
                            Console.Error.WriteLine(
                                $"headless: no signature-valid webhook delivery arrived (observed {receiver.ObservedCount})."
                            )

                            return 1
                        | Some _ ->
                            Console.Out.WriteLine(
                                $"WEBHOOK verified deliveries={receiver.VerifiedCount} observed={receiver.ObservedCount}"
                            )

                            // AutoClose assertion: the close runs on the actor
                            // after the settle, store-first with no journaled
                            // event, so the store read proves it landed.
                            let! closed = waitForClosedAsync store session.Id (TimeSpan.FromSeconds(10.0))

                            if closed then
                                Console.Out.WriteLine("AUTOCLOSE session closed")
                            else
                                Console.Error.WriteLine("headless: the session did not close itself.")

                            if closed then return code else return 1
                    with error ->
                        Console.Error.WriteLine($"headless: {error.Message}")
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
                Unchecked.defaultof<HeadlessStart>

        runAsync start |> fun runner -> runner.GetAwaiter().GetResult()
    with error ->
        Console.Error.WriteLine($"headless: {error.Message}")
        1
