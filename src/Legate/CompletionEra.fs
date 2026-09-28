// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.DependencyInjection.Extensions

/// Completion-era gate (issue 289): one additive migration
/// (TurnCompletionEra, 202609281200) records which sessions opened after
/// terminal completion events started journaling, so the live-turn orphan
/// trigger tells orphaned marker-only turns (era-marked, no terminal row)
/// from healthy pre-era restarts (marker-only by construction, never
/// marked). Delegates, not an interface: no InternalsVisibleTo exists in
/// the Abstractions sources, so delegates avoid cross-assembly internal
/// visibility entirely. Internal: storage providers replace the default
/// gate through the internal registration below.
module internal CompletionEra =

    /// Reads whether the session opened in the completion era: true once
    /// Open or Fork marked it, false for pre-era sessions (absent row) and
    /// unknown sessions. Never throws for a missing row: absence reads
    /// false (pre-era quiet).
    type CompletionEraReader = TenantId -> SessionId -> CancellationToken -> Task<bool>

    /// Marks the session era-marked after a successful Open or Fork.
    /// Best-effort: a marking failure degrades to pre-era (quiet) and
    /// never fails the open or fork.
    type CompletionEraMarker = TenantId -> SessionId -> CancellationToken -> Task

    /// The era gate the runtime closes over: the reader the dispatcher
    /// sweep and the entity-start predicate consult, plus the marker the
    /// session client calls on open and fork.
    type CompletionEraGate =
        {
            /// Reads the era mark. Never null.
            Reader: CompletionEraReader
            /// Writes the era mark. Never null.
            Marker: CompletionEraMarker
        }

    /// The pre-era gate: nothing is ever marked and marking is a no-op,
    /// so the orphan trigger stays quiet until a storage provider
    /// replaces it. The AddLegate root registers this default.
    let preEraGate: CompletionEraGate =
        {
            Reader = fun _ _ _ -> Task.FromResult false
            Marker = fun _ _ _ -> Task.CompletedTask
        }

/// Registers the completion-era gate.
module internal CompletionEraRegistration =

    /// Registers the pre-era default gate, keeping a host registration:
    /// storage providers replace it with a backed gate.
    /// <param name="services">The container to add the gate to.</param>
    let register (services: IServiceCollection) : unit =
        ArgumentNullException.ThrowIfNull(services)

        services.TryAddSingleton<CompletionEra.CompletionEraGate>(CompletionEra.preEraGate)
        |> ignore

    /// Replaces the gate with a backed one. Storage providers call this
    /// from their registration after wiring their stores.
    /// <param name="services">The container holding the gate.</param>
    /// <param name="gate">The backed gate. Must not be null.</param>
    let replace (services: IServiceCollection) (gate: CompletionEra.CompletionEraGate) : unit =
        ArgumentNullException.ThrowIfNull(services)

        if isNull (box gate) then
            raise (ArgumentNullException(nameof gate))

        if isNull (box gate.Reader) then
            raise (ArgumentNullException(nameof gate))

        if isNull (box gate.Marker) then
            raise (ArgumentNullException(nameof gate))

        services.Replace(ServiceDescriptor.Singleton<CompletionEra.CompletionEraGate>(gate))
        |> ignore
