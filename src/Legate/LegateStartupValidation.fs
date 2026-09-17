// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.DependencyInjection.Extensions
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Options

// Startup-time checks for the AddLegate builder. Missing required
// registrations (no provider, no session store, no workspace runtime) fail
// at host start with one InvalidOperationException listing everything
// missing; the check runs at startup rather than at the AddLegate call
// because hosts register stores after AddLegate. LegateOptions rides the
// standard options pipeline with ValidateOnStart so invalid options fail at
// host start too.

// ──────────────────────────────────────────────────────────────────────────
// Options validation

/// Validates <see cref="T:Legate.LegateOptions" /> through the options
/// pipeline, surfacing the first composite violation as the failure message.
type internal LegateOptionsValidation() =

    interface IValidateOptions<LegateOptions> with
        member _.Validate(_name: string, options: LegateOptions) =
            if isNull (box options) then
                ValidateOptionsResult.Fail "LegateOptions must not be null."
            else
                let violation = options.Validate()

                if isNull (box violation) then
                    ValidateOptionsResult.Success
                else
                    ValidateOptionsResult.Fail $"Invalid LegateOptions: %s{violation}"

// ──────────────────────────────────────────────────────────────────────────
// Expiry store gate

/// Fails startup fast when session expiry is enabled without a durable
/// blob store. Expiry closes idle sessions and re-bind restores workspace
/// input/output from the blob store, so an InMemory blob store (or no blob
/// store at all) would silently lose workspace files: that is a host
/// misconfiguration, and the single InvalidOperationException below names
/// it. The InMemory check is name-based because Legate never references a
/// storage package: a host-owned store whose type name contains "InMemory"
/// is treated as non-durable and documented as such.
module internal SessionExpiryStartup =

    /// Whether expiry is enabled in the options: the Expiry knob set to a
    /// positive bound. A set-but-non-positive value is left to the options
    /// validation failure, never to this gate.
    /// <param name="sessions">The session knobs, or null for defaults.</param>
    /// <returns>True when the sweeper will close idle sessions.</returns>
    let isEnabled (sessions: SessionsOptions | null) : bool =
        match sessions with
        | null -> false
        | present -> present.Expiry.HasValue && present.Expiry.Value > TimeSpan.Zero

    /// Whether the registered blob store survives a restart: present and
    /// not an in-memory backend.
    /// <param name="blobStore">The registered blob store, or null when none is registered.</param>
    /// <returns>True when expiry may rely on the store.</returns>
    let isDurable (blobStore: IBlobStore | null) : bool =
        match blobStore with
        | null -> false
        | present ->
            match present.GetType().FullName with
            | null -> true
            | typeName -> not (typeName.Contains("InMemory", StringComparison.Ordinal))

    /// Raises the single startup failure when expiry is enabled without a
    /// durable blob store; otherwise returns.
    /// <param name="serviceProvider">The container to resolve options and the blob store from.</param>
    /// <exception cref="T:System.InvalidOperationException">Expiry is enabled without a durable blob store.</exception>
    let requireDurableBlobStore (serviceProvider: IServiceProvider) : unit =
        ArgumentNullException.ThrowIfNull(serviceProvider)

        let sessions =
            match serviceProvider.GetService<IOptions<LegateOptions>>() with
            | null -> null
            | options when isNull (box options.Value) -> null
            | options when isNull (box options.Value.Sessions) -> null
            | options -> options.Value.Sessions

        if isEnabled sessions then
            let blobStore = serviceProvider.GetService<IBlobStore>()

            if not (isDurable blobStore) then
                raise (
                    InvalidOperationException(
                        "Legate sessions expiry (Sessions:Expiry) is enabled but no durable blob store is registered: "
                        + "expiry closes idle sessions and re-bind restores workspace input/output from the blob store, "
                        + "so the InMemory blob store (or no blob store) loses workspace files on restart. "
                        + "Register a durable IBlobStore (SQLite, file-system, or S3) or disable expiry by leaving Sessions:Expiry empty."
                    )
                )

// ──────────────────────────────────────────────────────────────────────────
// Required-registration check

/// Fails host startup with one message listing every missing required
/// registration: no <see cref="T:Legate.ILlmProvider" />, no
/// <see cref="T:Legate.ISessionStore" />, or no
/// <see cref="T:Legate.IWorkspaceRuntime" />. The local actor system hosted
/// service registers separately; this service only validates.
type internal LegateStartupValidation(serviceProvider: IServiceProvider) =

    do ArgumentNullException.ThrowIfNull(serviceProvider)

    interface IHostedService with
        member _.StartAsync(_cancellationToken: CancellationToken) =
            let missing = ResizeArray<string>()

            if Seq.isEmpty (serviceProvider.GetServices<ILlmProvider>()) then
                missing.Add "an LLM provider (ILlmProvider): call Llm.AddProvider"

            if isNull (box (serviceProvider.GetService<ISessionStore>())) then
                missing.Add "a session store (ISessionStore): call Storage.UseSessionStore"

            if isNull (box (serviceProvider.GetService<IWorkspaceRuntime>())) then
                missing.Add "a workspace runtime (IWorkspaceRuntime): call Workspace.UseRuntime"

            if missing.Count = 0 then
                SessionExpiryStartup.requireDurableBlobStore serviceProvider

                Task.CompletedTask
            else
                let listed = String.Join("; ", missing)

                raise (
                    InvalidOperationException(
                        $"Legate is missing required registrations: %s{listed}. Register them through the AddLegate builder before the host starts."
                    )
                )

        member _.StopAsync(_cancellationToken: CancellationToken) = Task.CompletedTask

// ──────────────────────────────────────────────────────────────────────────
// Registration

/// Wires the startup checks: the default <see cref="T:Legate.LegateOptions" />
/// with validation on start, plus the required-registration check above.
module internal LegateStartupChecks =

    /// Registers the options pipeline (defaults, validation, start-time
    /// validation) and the required-registration hosted service.
    /// <param name="services">The container to add the checks to.</param>
    let register (services: IServiceCollection) : unit =
        ArgumentNullException.ThrowIfNull(services)

        services.AddOptions<LegateOptions>().ValidateOnStart() |> ignore

        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IValidateOptions<LegateOptions>, LegateOptionsValidation>()
        )
        |> ignore

        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, LegateStartupValidation>())
        |> ignore
