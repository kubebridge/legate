// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.DependencyInjection.Extensions

// Minimal service registration for hosts that build their own container.
// Future hosting work extends this method; it does not re-register, so hosts
// that call it twice keep their own overrides.

/// Extension methods that register the Legate runtime in a service container.
[<AutoOpen>]
module AddLegateExtensions =

    /// Registers the Legate runtime seams in <paramref name="services" />:
    /// the machine clock as <see cref="T:System.TimeProvider" /> plus the
    /// system-backed delay and random defaults, each only when the host has
    /// not already supplied its own. Future Legate hosting work registers its
    /// remaining services on top of this method.
    /// <param name="services">The container to add Legate services to.</param>
    /// <returns>The same container, for call chaining.</returns>
    let AddLegate (services: IServiceCollection) : IServiceCollection =
        ArgumentNullException.ThrowIfNull(services)

        services.TryAddSingleton(TimeProvider.System) |> ignore
        services.TryAddSingleton<ILlmDelay, SystemLlmDelay>() |> ignore
        services.TryAddSingleton<ILlmRandom, SystemLlmRandom>() |> ignore
        services
