// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.Packages

open System
open System.Runtime.CompilerServices
open Legate
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.DependencyInjection.Extensions

// Registration for the shared package lease service: one call wires the
// in-process IAgentPackageLeaseService beside the FileSystem or S3
// package stores, so hosts compose coordination without touching a
// store. The clock and the delay seam resolve from the container when
// the host registered them (AddLegate registers both) and fall back to
// the machine clock with system waits otherwise, so registration never
// throws for a missing seam.

/// <summary>
/// Entry point for the shared package lease service:
/// <c>services.AddAgentPackageLeaseService()</c>. Registers the
/// in-process <see cref="T:Legate.IAgentPackageLeaseService" /> over the
/// container's <see cref="T:System.TimeProvider" /> and
/// <see cref="T:Legate.ILlmDelay" /> when present, the machine clock with
/// system waits otherwise. File-system and S3 hosts compose this beside
/// their stores; no store changes.
/// </summary>
[<Sealed; AbstractClass; Extension>]
type PackageLeaseServiceCollectionExtensions =

    /// <summary>
    /// Registers the shared in-process package lease service, keeping a
    /// host registration when one already exists.
    /// </summary>
    /// <param name="services">The container to add the service to.</param>
    /// <returns>The same container, for call chaining.</returns>
    [<Extension>]
    static member AddAgentPackageLeaseService(services: IServiceCollection) : IServiceCollection =
        ArgumentNullException.ThrowIfNull(services)

        services.TryAddSingleton<IAgentPackageLeaseService>(
            Func<IServiceProvider, IAgentPackageLeaseService>(fun provider ->
                let clock =
                    match provider.GetService<TimeProvider>() with
                    | null -> TimeProvider.System
                    | running -> running

                let delay =
                    match provider.GetService<ILlmDelay>() with
                    | null -> PackageLeaseSystemDelay(clock) :> ILlmDelay
                    | seam -> seam

                AgentPackageLeaseService(clock, delay) :> IAgentPackageLeaseService)
        )
        |> ignore

        services

/// The F# call-chain surface for the package lease service registration.
module PackageLeaseServiceExtensions =

    /// Registers the shared in-process package lease service, keeping a
    /// host registration when one already exists.
    /// <param name="services">The container to add the service to.</param>
    /// <returns>The same container, for call chaining.</returns>
    let AddAgentPackageLeaseService (services: IServiceCollection) : IServiceCollection =
        PackageLeaseServiceCollectionExtensions.AddAgentPackageLeaseService(services)
