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

    let requireForSnapshot (provider: IServiceProvider) (sessions: SessionsOptions) =
        if isEnabled sessions && not (isDurable (provider.GetService<IBlobStore>())) then
            invalidOp
                "Session expiry requires a durable IBlobStore in this execution binding, or disable Sessions:Expiry."

// ──────────────────────────────────────────────────────────────────────────
// Required-registration check

type internal ExecutionValidation(callback: Action<IServiceProvider, ClusterMode>) =
    member _.Validate(provider, nodeMode) = callback.Invoke(provider, nodeMode)

module internal ExecutionGraphValidation =
    let resolve (provider: IServiceProvider) (contract: Type) =
        try
            provider.GetService(contract)
        with _ ->
            invalidOp (
                $"Legate execution binding could not resolve {contract.Name}; check its registration and settings. Cluster hosts require explicit execution providers."
            )

    let providers (provider: IServiceProvider) =
        try
            match resolve provider typeof<IEnumerable<ILlmProvider>> with
            | null -> Array.empty
            | :? IEnumerable<ILlmProvider> as values -> values |> Seq.toArray
            | _ -> invalidOp "ILlmProvider registrations are incompatible."
        with _ ->
            invalidOp "Legate execution binding could not resolve ILlmProvider registrations."

    let require (provider: IServiceProvider) (binding: string) =
        let missing = ResizeArray<string>()
        let store = resolve provider typeof<ISessionStore> :?> ISessionStore | null

        let journal =
            resolve provider typeof<ISessionEventStore> :?> ISessionEventStore | null

        if isNull (box store) then
            missing.Add("ISessionStore")
        elif not (store :? ISessionAbortControlStore) then
            missing.Add("ISessionAbortControlStore")

        let settlement =
            match resolve provider typeof<ISessionSettlementStore> with
            | null ->
                match box store with
                | :? ISessionSettlementStore as capable -> Some capable
                | _ -> None
            | registered -> Some(registered :?> ISessionSettlementStore)

        match settlement with
        | None -> missing.Add("ISessionSettlementStore")
        | Some capable ->
            match journal with
            | null -> ()
            | actual ->
                let compatible =
                    try
                        capable.SupportsSettlementJournal actual
                    with _ ->
                        invalidOp (
                            $"Legate {binding} could not validate ISessionSettlementStore/ISessionEventStore compatibility."
                        )

                if not compatible then
                    missing.Add("ISessionSettlementStore(ISessionEventStore incompatible)")

        if isNull (box journal) then
            missing.Add("ISessionEventStore")

        if isNull (resolve provider typeof<Microsoft.Extensions.AI.IChatClient>) then
            missing.Add("IChatClient")

        if isNull (resolve provider typeof<IWorkspaceRuntime>) then
            missing.Add("IWorkspaceRuntime")

        if (providers provider).Length = 0 then
            missing.Add("ILlmProvider")

        if missing.Count > 0 then
            invalidOp (
                $"Legate {binding} is missing required registrations: "
                + String.Join(", ", missing)
            )

/// Fails host startup with one message listing every missing required
/// registration: no <see cref="T:Legate.ILlmProvider" />, no
/// <see cref="T:Legate.ISessionStore" />, or no
/// <see cref="T:Legate.IWorkspaceRuntime" />. The local actor system hosted
/// service registers separately; this service only validates.
type internal LegateStartupValidation(serviceProvider: IServiceProvider) =

    do ArgumentNullException.ThrowIfNull(serviceProvider)

    interface IHostedService with
        member _.StartAsync(cancellationToken: CancellationToken) =
            let hasBindings =
                match serviceProvider.GetService<ISessionHostContexts>() with
                | null -> false
                | contexts -> contexts.HasDeclaredBindings

            // Explicit bindings are independent execution boundaries; their
            // providers are validated atomically by the context registry.
            // Without explicit bindings the root is the deferred self-binding
            // and must report the complete aggregate dependency set.
            if not hasBindings then
                ExecutionGraphValidation.require serviceProvider "startup"

            SessionExpiryStartup.requireDurableBlobStore serviceProvider

            match serviceProvider.GetService<ISessionHostContexts>() with
            | null -> Task.CompletedTask
            | contexts -> contexts.InitializeAsync(cancellationToken)

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
