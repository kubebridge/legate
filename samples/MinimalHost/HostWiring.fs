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
                builder.Llm.AddAnthropic(configuration) |> ignore

            if hasKey "OPENAI_API_KEY" then
                builder.Llm.AddOpenAI(configuration) |> ignore

            if hasKey "GOOGLE_API_KEY" then
                Legate.Llm.GoogleServiceCollectionExtensions.AddGoogle(builder.Services, configuration)
                |> ignore)
    )
    |> ignore

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
