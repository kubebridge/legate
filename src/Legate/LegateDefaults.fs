// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.DependencyInjection.Extensions

// Builder-scoped no-op defaults for the observe-and-audit hooks. The
// contracts deliberately promise "no default" (Policies.fs documents no
// default observer, HostHooks.fs documents no default audit sink and no
// registration seam), so these types stay internal and are registered only
// by the AddLegate builder below: hosts that need real observation replace
// them through the Policies and Agents sub-builders, and the contract docs
// are untouched.

// ──────────────────────────────────────────────────────────────────────────
// No-op hooks

/// A usage observer that drops every checkpoint and settlement. Internal to
/// the builder: hosts that need real observation replace it through
/// <c>Policies.UseUsageObserver</c>.
type internal NoopUsageObserver() =

    interface IUsageObserver with
        member _.OnCheckpoint(_usage: UsageCheckpoint) = ()
        member _.OnSettled(_usage: UsageSettlement) = ()

/// An agent audit sink that acknowledges every change without recording
/// anything. Internal to the builder: hosts that need real auditing replace
/// it through <c>Agents.UseAuditSink</c>.
type internal NoopAgentAuditSink() =

    interface IAgentAuditSink with
        member _.OnAgentChanged(_agent: Agent, _cancellationToken: CancellationToken) = Task.CompletedTask

// ──────────────────────────────────────────────────────────────────────────
// Default registration

/// Registers the builder-owned defaults with TryAdd so host registrations
/// always win, whenever the host registered (before or after AddLegate).
module internal LegateDefaultRegistration =

    /// Registers the policy, quota, and hook defaults: allow-all admission,
    /// the no-op usage observer, allow-all model policy, unlimited artifact
    /// quota, and the no-op audit sink. Each only when the host has not
    /// already supplied its own.
    /// <param name="services">The container to add the defaults to.</param>
    let register (services: IServiceCollection) : unit =
        ArgumentNullException.ThrowIfNull(services)

        services.TryAddSingleton<ISessionAdmissionPolicy, AllowAllAdmissionPolicy>()
        |> ignore

        services.TryAddSingleton<IUsageObserver, NoopUsageObserver>() |> ignore
        services.TryAddSingleton<IModelPolicy, AllowAllModelPolicy>() |> ignore
        services.TryAddSingleton<IArtifactQuota, UnlimitedArtifactQuota>() |> ignore
        services.TryAddSingleton<IAgentAuditSink, NoopAgentAuditSink>() |> ignore
