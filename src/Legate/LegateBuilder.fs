// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Generic
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.DependencyInjection.Extensions
open Microsoft.Extensions.Logging

// Microsoft-style builder for services.AddLegate(configure). The builder and
// its seven sub-builders live in Legate; only the registered interfaces live
// in Legate.Abstractions. Sub-builder methods return their own builder for
// chaining; Use* replaces any previous registration for that contract while
// Add* appends (providers and tool sources are multi-registration).
// Generic overloads register through ServiceDescriptor with typeof so the
// container constructs the type; instance overloads register the value.

// ──────────────────────────────────────────────────────────────────────────
// Llm

/// Configures the LLM providers the coordinator resolves models through.
/// At least one provider is required: startup fails listing it when none is
/// registered.
[<Sealed>]
type LlmBuilder internal (services: IServiceCollection) as this =
    do ArgumentNullException.ThrowIfNull(services)

    /// Registers one LLM provider instance.
    /// <param name="provider">The provider to serve its provider id.</param>
    /// <returns>This builder, for chaining.</returns>
    member _.AddProvider(provider: ILlmProvider) : LlmBuilder =
        ArgumentNullException.ThrowIfNull(provider)
        services.AddSingleton<ILlmProvider>(provider) |> ignore
        this

    /// Registers an LLM provider type for container construction.
    /// <typeparam name="T">The provider type to construct per container.</typeparam>
    /// <returns>This builder, for chaining.</returns>
    member _.AddProvider<'T when 'T :> ILlmProvider>() : LlmBuilder =
        services.Add(ServiceDescriptor(typeof<ILlmProvider>, typeof<'T>, ServiceLifetime.Singleton))
        |> ignore

        this

// ──────────────────────────────────────────────────────────────────────────
// Storage

/// Configures session storage and the artifact quota. A session store is
/// required: startup fails listing it when none is registered.
[<Sealed>]
type StorageBuilder internal (services: IServiceCollection) as this =
    do ArgumentNullException.ThrowIfNull(services)

    /// Registers the session store, replacing any previous one.
    /// <param name="store">The session store to persist sessions with.</param>
    /// <returns>This builder, for chaining.</returns>
    member _.UseSessionStore(store: ISessionStore) : StorageBuilder =
        ArgumentNullException.ThrowIfNull(store)
        services.Replace(ServiceDescriptor.Singleton<ISessionStore>(store)) |> ignore
        this

    /// Registers a session store type for container construction, replacing
    /// any previous one.
    /// <typeparam name="T">The session store type to construct per container.</typeparam>
    /// <returns>This builder, for chaining.</returns>
    member _.UseSessionStore<'T when 'T :> ISessionStore>() : StorageBuilder =
        services.Replace(ServiceDescriptor(typeof<ISessionStore>, typeof<'T>, ServiceLifetime.Singleton))
        |> ignore

        this

    /// Registers the artifact quota, replacing the unlimited default.
    /// <param name="quota">The quota to bound artifact storage with.</param>
    /// <returns>This builder, for chaining.</returns>
    member _.UseQuota(quota: IArtifactQuota) : StorageBuilder =
        ArgumentNullException.ThrowIfNull(quota)
        services.Replace(ServiceDescriptor.Singleton<IArtifactQuota>(quota)) |> ignore
        this

    /// Registers an artifact quota type for container construction,
    /// replacing the unlimited default.
    /// <typeparam name="T">The quota type to construct per container.</typeparam>
    /// <returns>This builder, for chaining.</returns>
    member _.UseQuota<'T when 'T :> IArtifactQuota>() : StorageBuilder =
        services.Replace(ServiceDescriptor(typeof<IArtifactQuota>, typeof<'T>, ServiceLifetime.Singleton))
        |> ignore

        this

// ──────────────────────────────────────────────────────────────────────────
// Workspace

/// Configures the workspace runtime sessions bind workspaces through. A
/// runtime is required: startup fails listing it when none is registered.
/// There is no implicit fallback: a runtime that cannot bind throws.
[<Sealed>]
type WorkspaceBuilder internal (services: IServiceCollection) as this =
    do ArgumentNullException.ThrowIfNull(services)

    /// Registers the workspace runtime, replacing any previous one.
    /// <param name="runtime">The runtime to bind session workspaces with.</param>
    /// <returns>This builder, for chaining.</returns>
    member _.UseRuntime(runtime: IWorkspaceRuntime) : WorkspaceBuilder =
        ArgumentNullException.ThrowIfNull(runtime)

        services.Replace(ServiceDescriptor.Singleton<IWorkspaceRuntime>(runtime))
        |> ignore

        this

    /// Registers a workspace runtime type for container construction,
    /// replacing any previous one.
    /// <typeparam name="T">The runtime type to construct per container.</typeparam>
    /// <returns>This builder, for chaining.</returns>
    member _.UseRuntime<'T when 'T :> IWorkspaceRuntime>() : WorkspaceBuilder =
        services.Replace(ServiceDescriptor(typeof<IWorkspaceRuntime>, typeof<'T>, ServiceLifetime.Singleton))
        |> ignore

        this

// ──────────────────────────────────────────────────────────────────────────
// Tools

/// Configures the tool sources sessions resolve tools through. Sources are
/// optional: a session with no sources simply offers no tools.
[<Sealed>]
type ToolsBuilder internal (services: IServiceCollection) as this =
    do ArgumentNullException.ThrowIfNull(services)

    /// The container the builder registers into.
    member _.Services: IServiceCollection = services

    /// Registers one tool source instance.
    /// <param name="source">The source contributing tools to sessions.</param>
    /// <returns>This builder, for chaining.</returns>
    member _.AddSource(source: IToolSource) : ToolsBuilder =
        ArgumentNullException.ThrowIfNull(source)
        services.AddSingleton<IToolSource>(source) |> ignore
        this

    /// Registers a tool source type for container construction.
    /// <typeparam name="T">The source type to construct per container.</typeparam>
    /// <returns>This builder, for chaining.</returns>
    member _.AddSource<'T when 'T :> IToolSource>() : ToolsBuilder =
        services.Add(ServiceDescriptor(typeof<IToolSource>, typeof<'T>, ServiceLifetime.Singleton))
        |> ignore

        this

    /// Registers the agent custom HTTP tool source: per session it builds
    /// one signed function per enabled custom tool, losing name collisions
    /// to built-ins so built-ins keep first claim. Uses system DNS, the
    /// default SSRF posture (reserved addresses denied), the 30 s call
    /// bound, and the container's logger.
    /// <param name="toolStore">The store listing the agent's enabled custom tools. Must not be null.</param>
    /// <returns>This builder, for chaining.</returns>
    member _.AddCustomTools(toolStore: IAgentCustomToolStore) : ToolsBuilder =
        ArgumentNullException.ThrowIfNull(toolStore)
        this.AddCustomTools(toolStore, null, null)

    /// Registers the agent custom HTTP tool source with explicit
    /// networking posture: per session it builds one signed function per
    /// enabled custom tool, losing name collisions to built-ins so
    /// built-ins keep first claim. Uses the 30 s call bound and the
    /// container's logger.
    /// <param name="toolStore">The store listing the agent's enabled custom tools. Must not be null.</param>
    /// <param name="resolver">The address resolver the call-time SSRF guard resolves through, or null for system DNS.</param>
    /// <param name="options">The host-level SSRF allow/deny lists, or null for deny-reserved only.</param>
    /// <returns>This builder, for chaining.</returns>
    member _.AddCustomTools
        (toolStore: IAgentCustomToolStore, resolver: IHostAddressResolver | null, options: SsrfGuardOptions | null)
        : ToolsBuilder =
        ArgumentNullException.ThrowIfNull(toolStore)

        let build (provider: IServiceProvider) : IToolSource =
            let effectiveResolver =
                match Option.ofObj resolver with
                | Some live -> live
                | None -> SystemHostAddressResolver() :> IHostAddressResolver

            let effectiveOptions =
                match Option.ofObj options with
                | Some live -> live
                | None -> SsrfGuardOptions()

            let logger = provider.GetService<ILogger<CustomToolSource>>()

            CustomToolSource(
                toolStore,
                effectiveResolver,
                effectiveOptions,
                null,
                CustomToolFunction.DefaultTimeout,
                logger
            )
            :> IToolSource

        services.AddSingleton<IToolSource>(Func<IServiceProvider, IToolSource>(build))
        |> ignore

        this

// ──────────────────────────────────────────────────────────────────────────
// Policies

/// Configures the admission, usage, and model policies. Each carries a
/// builder default (allow-all admission, no-op usage observer, allow-all
/// model policy) that a host registration replaces.
[<Sealed>]
type PoliciesBuilder internal (services: IServiceCollection) as this =
    do ArgumentNullException.ThrowIfNull(services)

    /// Registers the session admission policy, replacing the allow-all
    /// default.
    /// <param name="policy">The policy gating session opens and prompts.</param>
    /// <returns>This builder, for chaining.</returns>
    member _.UseAdmissionPolicy(policy: ISessionAdmissionPolicy) : PoliciesBuilder =
        ArgumentNullException.ThrowIfNull(policy)

        services.Replace(ServiceDescriptor.Singleton<ISessionAdmissionPolicy>(policy))
        |> ignore

        this

    /// Registers a session admission policy type for container
    /// construction, replacing the allow-all default.
    /// <typeparam name="T">The policy type to construct per container.</typeparam>
    /// <returns>This builder, for chaining.</returns>
    member _.UseAdmissionPolicy<'T when 'T :> ISessionAdmissionPolicy>() : PoliciesBuilder =
        services.Replace(ServiceDescriptor(typeof<ISessionAdmissionPolicy>, typeof<'T>, ServiceLifetime.Singleton))
        |> ignore

        this

    /// Registers the usage observer, replacing the no-op default.
    /// <param name="observer">The observer receiving usage checkpoints and settlements.</param>
    /// <returns>This builder, for chaining.</returns>
    member _.UseUsageObserver(observer: IUsageObserver) : PoliciesBuilder =
        ArgumentNullException.ThrowIfNull(observer)

        services.Replace(ServiceDescriptor.Singleton<IUsageObserver>(observer))
        |> ignore

        this

    /// Registers a usage observer type for container construction,
    /// replacing the no-op default.
    /// <typeparam name="T">The observer type to construct per container.</typeparam>
    /// <returns>This builder, for chaining.</returns>
    member _.UseUsageObserver<'T when 'T :> IUsageObserver>() : PoliciesBuilder =
        services.Replace(ServiceDescriptor(typeof<IUsageObserver>, typeof<'T>, ServiceLifetime.Singleton))
        |> ignore

        this

    /// Registers the model policy, replacing the allow-all default.
    /// <param name="policy">The policy authorising tenant-provider-model calls.</param>
    /// <returns>This builder, for chaining.</returns>
    member _.UseModelPolicy(policy: IModelPolicy) : PoliciesBuilder =
        ArgumentNullException.ThrowIfNull(policy)
        services.Replace(ServiceDescriptor.Singleton<IModelPolicy>(policy)) |> ignore
        this

    /// Registers a model policy type for container construction, replacing
    /// the allow-all default.
    /// <typeparam name="T">The policy type to construct per container.</typeparam>
    /// <returns>This builder, for chaining.</returns>
    member _.UseModelPolicy<'T when 'T :> IModelPolicy>() : PoliciesBuilder =
        services.Replace(ServiceDescriptor(typeof<IModelPolicy>, typeof<'T>, ServiceLifetime.Singleton))
        |> ignore

        this

// ──────────────────────────────────────────────────────────────────────────
// Agents

/// Configures the agent store and the agent audit sink. Neither is required:
/// sessions can run without a managed agent catalog, and the audit sink
/// defaults to a builder-scoped no-op.
///
/// File layers (<c>AddFromDirectory</c> plus <c>Add</c>) merge with later
/// definitions winning: the backing store from <c>UseStore</c>, then the
/// directory files in sorted file order, then code-defined entries in call
/// order, all keyed by agent name. The directory and code-defined layers
/// serve <see cref="F:Legate.TenantId.Default" /> only; other tenants see
/// the backing store alone. A missing directory reads as empty, and every
/// store read re-reads the directories from disk with no watcher.
[<Sealed>]
type AgentsBuilder internal (services: IServiceCollection) as this =
    do ArgumentNullException.ThrowIfNull(services)

    let directories = ResizeArray<string>()
    let codeDefined = ResizeArray<Agent>()

    /// The container the builder registers into.
    member internal _.Services: IServiceCollection = services

    /// Whether the host registered at least one file layer.
    member internal _.HasFileLayers: bool = directories.Count > 0 || codeDefined.Count > 0

    /// The registered agent directories, in call order.
    member internal _.FileDirectories: IReadOnlyList<string> =
        directories :> IReadOnlyList<string>

    /// The code-defined agent templates, in call order.
    member internal _.FileAgents: IReadOnlyList<Agent> =
        codeDefined :> IReadOnlyList<Agent>

    /// Registers the agent store, replacing any previous one.
    /// <param name="store">The store managing host agents.</param>
    /// <returns>This builder, for chaining.</returns>
    member _.UseStore(store: IAgentStore) : AgentsBuilder =
        ArgumentNullException.ThrowIfNull(store)
        services.Replace(ServiceDescriptor.Singleton<IAgentStore>(store)) |> ignore
        this

    /// Registers an agent store type for container construction, replacing
    /// any previous one.
    /// <typeparam name="T">The store type to construct per container.</typeparam>
    /// <returns>This builder, for chaining.</returns>
    member _.UseStore<'T when 'T :> IAgentStore>() : AgentsBuilder =
        services.Replace(ServiceDescriptor(typeof<IAgentStore>, typeof<'T>, ServiceLifetime.Singleton))
        |> ignore

        this

    /// Registers a directory of agent definition files (`.agent/agents`
    /// for CLI hosts): every top-level `*.md` file contributes one agent
    /// whose frontmatter (`name`, `description`, `model`, `tools`,
    /// `enabled`) plus body-as-system-prompt merge over the backing store
    /// with later files winning in sorted file order. The directory is re-read on every
    /// store read, so disk changes apply on the next open with no watcher;
    /// a missing directory reads as empty. Writes always throw
    /// <see cref="T:Legate.ReadOnlyAgentStoreException" />.
    /// <param name="path">The directory holding the `*.md` agent files.</param>
    /// <returns>This builder, for chaining.</returns>
    member _.AddFromDirectory(path: string) : AgentsBuilder =
        if isNull (box path) then
            raise (ArgumentNullException(nameof path))

        if String.IsNullOrWhiteSpace path then
            raise (ArgumentException("An agent directory path must be a non-empty string.", nameof path))

        directories.Add(path)
        this

    /// Registers a code-defined agent on
    /// <see cref="F:Legate.TenantId.Default" />: the template starts with
    /// the registered name, a fresh id, the default model, an empty system
    /// prompt, and enabled, and <paramref name="configure" /> shapes the
    /// rest. F# hosts copy with <c>{ template with ... }</c>; C# hosts
    /// mutate the template's properties and return it. Code-defined entries
    /// merge last, so they win over the same name from the backing store or
    /// a directory file; the registered name and the default tenant always
    /// key the entry, even when <paramref name="configure" /> replaces them.
    /// <param name="name">The agent name keying the entry.</param>
    /// <param name="configure">The transform shaping the agent from its template. Must not return null.</param>
    /// <returns>This builder, for chaining.</returns>
    member _.Add(name: string, configure: Func<Agent, Agent>) : AgentsBuilder =
        if isNull (box name) then
            raise (ArgumentNullException(nameof name))

        if String.IsNullOrWhiteSpace name then
            raise (ArgumentException("An agent name must be a non-empty string.", nameof name))

        ArgumentNullException.ThrowIfNull(configure)

        let template: Agent =
            {
                Id = AgentId.New()
                Tenant = TenantId.Default
                Name = name.Trim()
                Description = null
                Model = Agents.AgentFileParser.defaultModel
                SystemPrompt = ""
                EnvironmentVariables = null
                PermissionDefaults = null
                ToolSelection = null
                PackageReference = null
                Enabled = true
                Schedule = null
                RowVersion = 0UL
                CreatedAt = DateTimeOffset.UtcNow
                UpdatedAt = DateTimeOffset.UtcNow
            }

        let customized = configure.Invoke(template)

        if isNull (box customized) then
            raise (ArgumentException("The agent configure function must return an agent.", nameof configure))

        codeDefined.Add(
            { customized with
                Name = name.Trim()
                Tenant = TenantId.Default
            }
        )

        this

    /// Registers the agent audit sink, replacing the no-op default.
    /// <param name="sink">The sink notified when a managed agent changes.</param>
    /// <returns>This builder, for chaining.</returns>
    member _.UseAuditSink(sink: IAgentAuditSink) : AgentsBuilder =
        ArgumentNullException.ThrowIfNull(sink)
        services.Replace(ServiceDescriptor.Singleton<IAgentAuditSink>(sink)) |> ignore
        this

    /// Registers an agent audit sink type for container construction,
    /// replacing the no-op default.
    /// <typeparam name="T">The sink type to construct per container.</typeparam>
    /// <returns>This builder, for chaining.</returns>
    member _.UseAuditSink<'T when 'T :> IAgentAuditSink>() : AgentsBuilder =
        services.Replace(ServiceDescriptor(typeof<IAgentAuditSink>, typeof<'T>, ServiceLifetime.Singleton))
        |> ignore

        this

// ──────────────────────────────────────────────────────────────────────────
// Permissions

/// Configures the permission policy gating tool calls. Optional: without a
/// host policy the runtime falls back to its configured permission defaults.
[<Sealed>]
type PermissionsBuilder internal (services: IServiceCollection) as this =
    do ArgumentNullException.ThrowIfNull(services)

    /// Registers the permission policy, replacing any previous one.
    /// <param name="policy">The policy deciding what happens to tool calls.</param>
    /// <returns>This builder, for chaining.</returns>
    member _.UsePolicy(policy: IPermissionPolicy) : PermissionsBuilder =
        ArgumentNullException.ThrowIfNull(policy)

        services.Replace(ServiceDescriptor.Singleton<IPermissionPolicy>(policy))
        |> ignore

        this

    /// Registers a permission policy type for container construction,
    /// replacing any previous one.
    /// <typeparam name="T">The policy type to construct per container.</typeparam>
    /// <returns>This builder, for chaining.</returns>
    member _.UsePolicy<'T when 'T :> IPermissionPolicy>() : PermissionsBuilder =
        services.Replace(ServiceDescriptor(typeof<IPermissionPolicy>, typeof<'T>, ServiceLifetime.Singleton))
        |> ignore

        this

// ──────────────────────────────────────────────────────────────────────────
// Root builder

/// The Microsoft-style builder hosts configure inside
/// <c>services.AddLegate(configure)</c>: seven sub-builders plus
/// <see cref="M:Legate.LegateBuilder.UseConfiguration(Microsoft.Extensions.Configuration.IConfigurationSection)" />.
/// The local actor system registers through AddLegate (Local mode only)
/// with the session client facade (SessionClient over the suspendable
/// router wiring for hosts that registered an IChatClient): registration,
/// options, and startup validation only.
[<Sealed>]
type LegateBuilder internal (services: IServiceCollection) as this =
    do ArgumentNullException.ThrowIfNull(services)

    let llm = LlmBuilder(services)
    let storage = StorageBuilder(services)
    let workspace = WorkspaceBuilder(services)
    let tools = ToolsBuilder(services)
    let policies = PoliciesBuilder(services)
    let agents = AgentsBuilder(services)
    let permissions = PermissionsBuilder(services)

    /// The container the builder registers into.
    member _.Services: IServiceCollection = services

    /// Configures the LLM providers the coordinator resolves models through.
    member _.Llm: LlmBuilder = llm

    /// Configures session storage and the artifact quota.
    member _.Storage: StorageBuilder = storage

    /// Configures the workspace runtime sessions bind workspaces through.
    member _.Workspace: WorkspaceBuilder = workspace

    /// Configures the tool sources sessions resolve tools through.
    member _.Tools: ToolsBuilder = tools

    /// Configures the admission, usage, and model policies.
    member _.Policies: PoliciesBuilder = policies

    /// Configures the agent store and the agent audit sink.
    member _.Agents: AgentsBuilder = agents

    /// Configures the permission policy gating tool calls.
    member _.Permissions: PermissionsBuilder = permissions

    /// Binds the <c>Legate:Artifacts</c> configuration section onto
    /// <see cref="T:Legate.ArtifactOptions" /> with a snapshot copy, failing
    /// fast on the first violation like the root binder. Durations bind in
    /// the canonical TimeSpan form (for example "7.00:00:00" for 7 days).
    /// <param name="section">The <c>Legate</c> configuration section.</param>
    /// <param name="services">The container receiving the configured options.</param>
    static member private BindArtifacts(section: IConfigurationSection, services: IServiceCollection) : unit =
        ArgumentNullException.ThrowIfNull(section)
        ArgumentNullException.ThrowIfNull(services)

        let probe = ArtifactOptions()
        section.GetSection("Artifacts").Bind(probe)

        match probe.Validate() with
        | null -> ()
        | violation ->
            raise (
                InvalidOperationException(
                    $"Invalid Legate configuration at '%s{section.Path}:Artifacts': %s{violation}"
                )
            )

        services.Configure<ArtifactOptions>(
            Action<ArtifactOptions>(fun target ->
                target.MaxEncodedBytes <- probe.MaxEncodedBytes
                target.MaxDecodedBytes <- probe.MaxDecodedBytes
                target.MaxWidth <- probe.MaxWidth
                target.MaxHeight <- probe.MaxHeight
                target.MaxPixels <- probe.MaxPixels
                target.PreviewThresholdBytes <- probe.PreviewThresholdBytes
                target.PreviewMaxDimension <- probe.PreviewMaxDimension
                target.AllowedImageMediaTypes <- List<string>(probe.AllowedImageMediaTypes)
                target.AllowedVideoMediaTypes <- List<string>(probe.AllowedVideoMediaTypes)
                target.PresignedExpiry <- probe.PresignedExpiry
                target.ReservationReclaimAfter <- probe.ReservationReclaimAfter)
        )
        |> ignore

    /// Binds the <c>Legate</c> configuration section onto
    /// <see cref="T:Legate.LegateOptions" /> with the shared binder
    /// (durations and flat-choice enums parse from raw strings, unknown
    /// values fail binding), replacing the defaults section by section.
    /// <param name="section">The <c>Legate</c> configuration section to bind.</param>
    /// <returns>This builder, for chaining.</returns>
    member _.UseConfiguration(section: IConfigurationSection) : LegateBuilder =
        ArgumentNullException.ThrowIfNull(section)
        let bound = LegateOptionsBinding.bind section

        services.Configure<LegateOptions>(
            Action<LegateOptions>(fun target ->
                target.Sessions <- bound.Sessions
                target.Turns <- bound.Turns
                target.Permissions <- bound.Permissions
                target.Llm <- bound.Llm
                target.Workspace <- bound.Workspace
                target.Completion <- bound.Completion
                target.AskUser <- bound.AskUser
                target.Cluster <- bound.Cluster)
        )
        |> ignore

        LegateBuilder.BindArtifacts(section, services) |> ignore

        this
