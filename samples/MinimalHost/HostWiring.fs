// SPDX-License-Identifier: Apache-2.0
module MinimalHost.HostWiring

open System
open Giraffe
open Legate
open Legate.Cluster
open Legate.Coordination
open Legate.Llm.OpenAI
open Legate.Storage
open Legate.Storage.InMemory
open Legate.Workspace.Process
open Microsoft.Extensions.AI
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.DependencyInjection
open MinimalHost.ScriptedTransport

// Container wiring: AddLegate with InMemory stores, the process workspace
// over the working directory, and provider selection that defaults to the
// scripted transport. A live provider registers only when its env key is
// present; keys come from the environment through configuration binding
// only and are never printed or persisted.

// ──────────────────────────────────────────────────────────────────────────
// Provider selection

/// Whether the environment carries the provider key.
let private hasKey (name: string) : bool =
    not (String.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(name)))

/// Whether any live provider key is present.
let private hasLiveKey () : bool =
    hasKey "ANTHROPIC_API_KEY" || hasKey "OPENAI_API_KEY" || hasKey "GOOGLE_API_KEY"

/// Whether the configuration carries a Postgres connection string: when
/// set the host registers the Postgres stores, otherwise it keeps the
/// offline InMemory default. Reads the bound configuration value (env
/// Legate__Storage__Postgres__ConnectionString flows through here), so
/// compose and appsettings both opt in without code changes.
let private hasPostgres (configuration: IConfiguration) : bool =
    not (String.IsNullOrWhiteSpace(configuration["Legate:Storage:Postgres:ConnectionString"]))

/// Whether the configuration carries a Redis connection string: when set
/// the host registers the Redis admission client, otherwise coordination
/// stays local. Reads the bound value (env
/// Legate__Llm__DistributedCoordination__ConnectionString flows through
/// here); the Legate:Llm:DistributedCoordination switch itself arrives
/// from configuration like every other knob.
let private hasRedis (configuration: IConfiguration) : bool =
    not (String.IsNullOrWhiteSpace(configuration["Legate:Llm:DistributedCoordination:ConnectionString"]))

/// Resolves the live chat client: the provider named by
/// LEGATE_MINIMALHOST_PROVIDER, else anthropic, openai, google in that
/// order, serving LEGATE_MODEL or its own default model.
let private selectLiveClient (provider: IServiceProvider) : IChatClient =
    let providers =
        provider.GetServices<ILlmProvider>()
        |> Seq.filter (fun candidate -> not (isNull (box candidate)))
        |> List.ofSeq

    let wanted = Environment.GetEnvironmentVariable("LEGATE_MINIMALHOST_PROVIDER")

    let findById (id: string) : ILlmProvider option =
        providers
        |> List.tryFind (fun candidate -> String.Equals(candidate.Id, id, StringComparison.OrdinalIgnoreCase))

    let selected =
        let prefer () : ILlmProvider option =
            [ "anthropic"; "openai"; "google" ]
            |> List.tryPick findById
            |> Option.orElse (List.tryHead providers)

        match wanted with
        | null -> prefer ()
        | candidate when String.IsNullOrWhiteSpace(candidate) -> prefer ()
        | candidate -> findById (candidate.Trim())

    match selected with
    | None ->
        let known =
            providers |> List.map (fun candidate -> candidate.Id) |> String.concat ", "

        raise (
            InvalidOperationException(
                $"No live provider is registered (known: {known}). Set ANTHROPIC_API_KEY, OPENAI_API_KEY, or GOOGLE_API_KEY."
            )
        )
    | Some live ->
        let defaultText =
            if String.IsNullOrWhiteSpace(live.DefaultModel) then
                raise (
                    InvalidOperationException(
                        $"Provider '{live.Id}' has no default model: set LEGATE_MODEL to <provider/model>."
                    )
                )
            elif live.DefaultModel.Contains("/") then
                live.DefaultModel.Trim()
            else
                $"{live.Id}/{live.DefaultModel.Trim()}"

        let modelEnv = Environment.GetEnvironmentVariable("LEGATE_MODEL")

        let reference =
            match modelEnv with
            | null -> ModelReference.Parse(defaultText)
            | candidate when String.IsNullOrWhiteSpace(candidate) -> ModelReference.Parse(defaultText)
            | candidate -> ModelReference.Parse(candidate.Trim())

        live.CreateChatClient(reference, null)

// ──────────────────────────────────────────────────────────────────────────
// Smoke agent (cluster-compose crash-resume harness, issue 271)

// The stable cross-node id of the code-defined smoke agent: every node
// registers the same id, so a session opened with it resolves on any
// survivor after the kill. The cluster-compose smoke hardcodes the same
// literal; keep the two in sync.
let private smokeAgentIdDefault = "01ARZ3NDEKTSV4RRFFQ69G5FAV"

/// Resolves the smoke agent id: LEGATE_SMOKE_AGENT_ID when it parses as
/// an agent id, else the fixed default. An unparsable override falls back
/// instead of failing startup so a typo never strands every node.
let private smokeAgentId () : AgentId =
    match Environment.GetEnvironmentVariable("LEGATE_SMOKE_AGENT_ID") with
    | null -> AgentId.Parse(smokeAgentIdDefault)
    | raw when String.IsNullOrWhiteSpace(raw) -> AgentId.Parse(smokeAgentIdDefault)
    | raw ->
        let mutable parsed = Unchecked.defaultof<AgentId>

        if AgentId.TryParse(raw.Trim(), &parsed) then
            parsed
        else
            AgentId.Parse(smokeAgentIdDefault)

/// Reads the smoke-scoped session claim lease in seconds: the
/// SessionClientOptions.LeaseDuration default (one hour) when the
/// environment carries no positive integer. A short lease (e.g. 60s) lets
/// the cluster-compose crash-resume smoke observe survivor recovery inside
/// its observation window; absent or invalid preserves production behavior.
let private sessionLeaseDuration () : TimeSpan =
    match Environment.GetEnvironmentVariable("LEGATE_SESSION_LEASE_SECONDS") with
    | null -> TimeSpan.FromHours 1.0
    | raw when String.IsNullOrWhiteSpace(raw) -> TimeSpan.FromHours 1.0
    | raw ->
        let mutable value = 0

        if Int32.TryParse(raw.Trim(), &value) && value > 0 then
            TimeSpan.FromSeconds(float value)
        else
            TimeSpan.FromHours 1.0

/// Builds the container: InMemory session and event stores over one shared
/// database by default, the process workspace, scripted-by-default
/// providers, and the chat client the facade opts into suspendable
/// children with. Binds the Legate configuration section (cluster mode,
/// remoting port/hostname, minimum members, and join timeout arrive from
/// the environment) and registers the Kubernetes bootstrap hook with 3
/// required contact points; the hook stays idle unless Cluster:Mode
/// selects Kubernetes. When the configuration carries a Postgres
/// connection string the Postgres stores replace the InMemory ones, and
/// when it carries a Redis connection string the Redis admission client
/// registers; otherwise the host stays fully offline.
/// <param name="services">The container to add Legate services to.</param>
/// <param name="configuration">The application configuration providers bind from.</param>
/// <param name="database">The shared in-memory database behind every store.</param>
let buildServices (services: IServiceCollection) (configuration: IConfiguration) (database: InMemoryDatabase) : unit =
    ArgumentNullException.ThrowIfNull(services)
    ArgumentNullException.ThrowIfNull(configuration)
    ArgumentNullException.ThrowIfNull(database)

    // Health checks first: AddLegate registers the legate-cluster
    // readiness check only when a HealthCheckService is already present,
    // so reversing this order would silently skip it.
    services.AddHealthChecks() |> ignore

    let usePostgres = hasPostgres configuration
    let useRedis = hasRedis configuration

    LegateServiceCollectionExtensions.AddLegate(
        services,
        Action<LegateBuilder>(fun builder ->
            builder.UseConfiguration(configuration.GetSection("Legate")) |> ignore

            builder.Cluster.UseKubernetes(Action<KubernetesOptions>(fun options -> options.RequiredContactPoints <- 3))
            |> ignore

            builder.Storage.UseSessionStore(InMemorySessionStore(database)) |> ignore

            let workspaceOptions = ProcessWorkspaceRuntimeOptions()
            workspaceOptions.Root <- Environment.CurrentDirectory

            builder.Workspace.UseRuntime(ProcessWorkspaceRuntime(workspaceOptions, null, null))
            |> ignore

            if usePostgres then
                builder.UsePostgres(configuration) |> ignore

            if useRedis then
                builder.UseRedisCoordination(configuration) |> ignore

            if hasKey "ANTHROPIC_API_KEY" then
                builder.Llm.AddAnthropicCompatible(configuration) |> ignore

            if hasKey "OPENAI_API_KEY" then
                builder.Llm.AddOpenAI(configuration) |> ignore

            if hasKey "GOOGLE_API_KEY" then
                Legate.Llm.GoogleServiceCollectionExtensions.AddGoogle(builder.Services, configuration)
                |> ignore

            // The crash-resume smoke agent: a code-defined entry with the
            // stable id above, the scripted model (the echo transport serves
            // every call, holding the reply past the kill via
            // LEGATE_MINIMALHOST_REPLY_DELAY_MS), and an empty tool
            // selection. The harness registers no tool sources, so empty
            // means no tools and no side-effect surface to dedup.
            builder.Agents.Add(
                "smoke",
                Func<Agent, Agent>(fun template ->
                    { template with
                        Id = smokeAgentId ()
                        Description = "Stable smoke agent for the cluster-compose crash-resume harness."
                        Model = ModelReference.Parse("scripted/scripted")
                        SystemPrompt = "You are the cluster-compose crash-resume smoke agent. Answer briefly."
                        ToolSelection = ToolSelection()
                    })
            )
            |> ignore)
    )
    |> ignore

    // Smoke-scoped claim lease: a host-owned SessionClientOptions wins
    // over the facade TryAdd default (plain Add appends and resolves
    // last), so the compose smoke can expire the victim claim inside its
    // window without touching the one-hour src default.
    let leaseOptions = SessionClientOptions()
    leaseOptions.LeaseDuration <- sessionLeaseDuration ()
    services.AddSingleton(leaseOptions) |> ignore

    // The Postgres registration above replaces the session store; the
    // event store is registered here, so it is only InMemory when Postgres
    // did not already claim it.
    if not usePostgres then
        services.AddSingleton<ISessionEventStore>(InMemorySessionEventStore(database))
        |> ignore

    // Giraffe needs its serializer in the container for the bindJson and
    // json handlers the routes use.
    services.AddGiraffe() |> ignore

    if hasLiveKey () then
        services.AddSingleton<IChatClient>(
            Func<IServiceProvider, IChatClient>(fun provider -> selectLiveClient provider)
        )
        |> ignore
    else
        let client = new EchoChatClient()

        services.AddSingleton<ILlmProvider>(StubScriptedProvider(client) :> ILlmProvider)
        |> ignore

        services.AddSingleton<IChatClient>(client :> IChatClient) |> ignore
