// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.DependencyInjection.Extensions

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
[<Sealed>]
type AgentsBuilder internal (services: IServiceCollection) as this =
    do ArgumentNullException.ThrowIfNull(services)

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
/// The local actor system registers through AddLegate (Local mode only);
/// no session client facade yet (later cycles own it): registration,
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

        this
