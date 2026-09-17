// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Runtime.CompilerServices
open Legate.Agents
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.DependencyInjection.Extensions

// Minimal service registration for hosts that build their own container.
// Future hosting work extends this method; it does not re-register, so hosts
// that call it twice keep their own overrides.
// All overloads live on LegateServiceCollectionExtensions because F# let
// bindings cannot overload and optional arguments are only permitted on type
// members. The module below keeps the original single-argument function so
// existing F# call sites are untouched. The type comes first: F# resolves
// names top to bottom within a file.

// ──────────────────────────────────────────────────────────────────────────
// Overloads

/// C# and F# entry points for the Legate builder:
/// <c>services.AddLegate()</c>, <c>services.AddLegate(b =&gt; ...)</c>, and
/// <c>LegateServiceCollectionExtensions.AddLegate(services, fun builder -&gt; ...)</c>.
/// Defaults (allow-all admission and model policy, unlimited artifact quota,
/// no-op usage observer and audit sink, system clock, delay, and random)
/// apply with <c>TryAdd</c> after the host configuration runs, so host
/// registrations always win. Missing required registrations (no provider, no
/// session store, no workspace runtime) fail at host start with one message
/// listing everything missing. Registers the local actor system hosted
/// service (Local cluster mode only; Clustered mode is untouched) and the
/// session client facade (SessionClient over the suspendable router wiring
/// for hosts that registered an IChatClient, identity children otherwise).
[<Sealed; AbstractClass; Extension>]
type LegateServiceCollectionExtensions =

    /// Registers the Legate runtime in <paramref name="services" /> and
    /// applies the host's builder configuration: sub-builders for providers,
    /// storage, workspaces, tools, policies, agents, and permissions, plus
    /// <c>UseConfiguration</c> for the <c>Legate</c> section.
    /// <param name="services">The container to add Legate services to.</param>
    /// <param name="configure">The host configuration over the Legate builder, or omitted for seams and defaults only.</param>
    /// <returns>The same container, for call chaining.</returns>
    [<Extension>]
    static member AddLegate(services: IServiceCollection, ?configure: LegateBuilder -> unit) : IServiceCollection =
        ArgumentNullException.ThrowIfNull(services)
        LegateServiceCollectionExtensions.RegisterRuntime(services, configure)

    /// Registers the Legate runtime in <paramref name="services" /> and
    /// applies the host's builder configuration, the C# callable form of the
    /// sibling overload: same defaults, configuration binding, and startup
    /// validation.
    /// <param name="services">The container to add Legate services to.</param>
    /// <param name="configure">The host configuration over the Legate builder.</param>
    /// <returns>The same container, for call chaining.</returns>
    [<Extension>]
    static member AddLegate(services: IServiceCollection, configure: Action<LegateBuilder>) : IServiceCollection =
        ArgumentNullException.ThrowIfNull(services)
        ArgumentNullException.ThrowIfNull(configure)

        let hostConfigure: LegateBuilder -> unit = fun builder -> configure.Invoke(builder)

        LegateServiceCollectionExtensions.RegisterRuntime(services, Some hostConfigure)

    /// Shared registration both overloads delegate to: seams, the optional
    /// host configuration, then the builder-owned defaults, options, and
    /// startup checks.
    /// <param name="services">The container to add Legate services to.</param>
    /// <param name="configure">The host configuration over the Legate builder, or None for seams and defaults only.</param>
    /// <returns>The same container, for call chaining.</returns>
    static member private RegisterRuntime
        (services: IServiceCollection, configure: (LegateBuilder -> unit) option)
        : IServiceCollection =
        services.TryAddSingleton(TimeProvider.System) |> ignore
        services.TryAddSingleton<ILlmDelay, SystemLlmDelay>() |> ignore
        services.TryAddSingleton<ILlmRandom, SystemLlmRandom>() |> ignore

        match configure with
        | Some configureHost ->
            let builder = LegateBuilder(services)
            configureHost builder
            FileAgentStoreRegistration.compose builder.Agents
        | _ -> ()

        LegateDefaultRegistration.register services
        LegateStartupChecks.register services
        LocalActorSystemRegistration.register services
        SessionClientRegistration.register services
        CompletionRedriverRegistration.register services
        JournalArchiveRegistration.register services
        services

// ──────────────────────────────────────────────────────────────────────────
// Original F# surface

/// Extension methods that register the Legate runtime in a service container.
[<AutoOpen>]
module AddLegateExtensions =

    /// Registers the Legate runtime seams in <paramref name="services" />:
    /// the machine clock as <see cref="T:System.TimeProvider" /> plus the
    /// system-backed delay and random defaults, each only when the host has
    /// not already supplied its own, along with the builder-owned policy,
    /// quota, and hook defaults, the options pipeline, and the startup
    /// checks. Future Legate hosting work registers its remaining services on
    /// top of this method.
    /// <param name="services">The container to add Legate services to.</param>
    /// <returns>The same container, for call chaining.</returns>
    let AddLegate (services: IServiceCollection) : IServiceCollection =
        ArgumentNullException.ThrowIfNull(services)
        LegateServiceCollectionExtensions.AddLegate(services)
