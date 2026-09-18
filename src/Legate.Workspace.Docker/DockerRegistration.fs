// SPDX-License-Identifier: Apache-2.0
namespace Legate.Workspace.Docker

open System
open System.Runtime.CompilerServices
open Legate
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.DependencyInjection.Extensions
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Logging
open Microsoft.Extensions.Options

// Registration over LegateBuilder: UseDocker binds the
// Legate:Workspace:Docker section onto DockerWorkspaceOptions, applies the
// optional configure callback, validates eagerly, and registers the
// container-per-session runtime over the bound options. Uses only the
// public builder surface: the options through IOptions, the runtime
// through the container. Registration never contacts the daemon: the
// container materialises on first Bind.

// Wiring shared by the overloads. Internal.
module internal DockerRegistration =

    /// Binds the section, applies the callback, validates eagerly, and
    /// registers the Docker workspace runtime.
    /// <param name="builder">The Legate builder receiving the runtime.</param>
    /// <param name="configuration">The application configuration root.</param>
    /// <param name="configure">Adjusts the bound options, or null to keep them.</param>
    /// <returns>The same builder, for chaining.</returns>
    let useDocker
        (builder: LegateBuilder, configuration: IConfiguration, configure: Action<DockerWorkspaceOptions> | null)
        : LegateBuilder =
        ArgumentNullException.ThrowIfNull(builder)
        ArgumentNullException.ThrowIfNull(configuration)

        // Fail fast before registering: a missing root or image, a bad
        // limit, or a dead exec timeout surfaces here, not on first Bind.
        let probe = DockerWorkspaceOptions()
        configuration.GetSection(DockerWorkspaceOptions.ConfigurationSectionPath).Bind(probe)

        match configure with
        | null -> ()
        | callback -> callback.Invoke(probe)

        match probe.Validate() with
        | null -> ()
        | violation ->
            raise (InvalidOperationException($"Invalid {DockerWorkspaceOptions.ConfigurationSectionPath}: {violation}"))

        let services = builder.Services
        services.AddOptions<DockerWorkspaceOptions>() |> ignore

        let section =
            configuration.GetSection(DockerWorkspaceOptions.ConfigurationSectionPath)

        services.Configure<DockerWorkspaceOptions>(Action<DockerWorkspaceOptions>(fun options -> section.Bind(options)))
        |> ignore

        match configure with
        | null -> ()
        | callback -> services.Configure<DockerWorkspaceOptions>(callback) |> ignore

        services.Replace(
            ServiceDescriptor.Singleton<IWorkspaceRuntime>(
                Func<IServiceProvider, IWorkspaceRuntime>(fun provider ->
                    let options = provider.GetRequiredService<IOptions<DockerWorkspaceOptions>>().Value

                    let environment = provider.GetService<IHostEnvironment>()
                    let typedLogger = provider.GetService<ILogger<DockerWorkspaceRuntime>>()

                    let logger: ILogger | null =
                        if isNull (box typedLogger) then
                            null
                        else
                            upcast typedLogger

                    DockerWorkspaceRuntime(options, environment, logger) :> IWorkspaceRuntime)
            )
        )
        |> ignore

        builder

/// Extension methods registering the Docker workspace runtime on
/// <see cref="T:Legate.LegateBuilder" /> (<c>builder.UseDocker(...)</c>).
/// Binds the <c>Legate:Workspace:Docker</c> section (host root, image,
/// network, CPU and memory limits, user, and the default exec timeout)
/// and registers the container-per-session runtime the dispatcher binds
/// through. Registration never contacts the daemon.
[<Sealed; AbstractClass; Extension>]
type DockerLegateBuilderExtensions =

    /// Binds <c>Legate:Workspace:Docker</c> and registers the Docker
    /// workspace runtime.
    /// <param name="builder">The Legate builder receiving the runtime.</param>
    /// <param name="configuration">The application configuration root holding the workspace section.</param>
    /// <returns>The same builder, for chaining.</returns>
    [<Extension>]
    static member UseDocker(builder: LegateBuilder, configuration: IConfiguration) : LegateBuilder =
        DockerRegistration.useDocker (builder, configuration, null)

    /// Binds <c>Legate:Workspace:Docker</c>, adjusts it with the callback,
    /// and registers the Docker workspace runtime.
    /// <param name="builder">The Legate builder receiving the runtime.</param>
    /// <param name="configuration">The application configuration root holding the workspace section.</param>
    /// <param name="configure">Adjusts the bound options before registration.</param>
    /// <returns>The same builder.</returns>
    [<Extension>]
    static member UseDocker
        (builder: LegateBuilder, configuration: IConfiguration, configure: Action<DockerWorkspaceOptions>)
        : LegateBuilder =
        ArgumentNullException.ThrowIfNull(configure)
        DockerRegistration.useDocker (builder, configuration, configure)
