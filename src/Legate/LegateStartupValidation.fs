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
// Required-registration check

/// Fails host startup with one message listing every missing required
/// registration: no <see cref="T:Legate.ILlmProvider" />, no
/// <see cref="T:Legate.ISessionStore" />, or no
/// <see cref="T:Legate.IWorkspaceRuntime" />. Issue 30 owns the local actor
/// system hosted service; this service only validates.
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
