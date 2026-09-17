// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage

open System
open System.Runtime.CompilerServices
open Legate
open Legate.Storage.S3
open Amazon.S3
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.DependencyInjection.Extensions
open Microsoft.Extensions.Options

// Registration over LegateBuilder: UseS3 binds the Legate:Storage:S3
// section onto S3StorageOptions, applies the optional configure callback,
// validates eagerly, and registers the client plus every store over the
// bound options: the blob store and the agent package store. Uses only the
// public builder surface: the options through IOptions, the client and the
// stores through the container. The client constructs without touching the
// network and the bucket materialises on first write, so registration
// never contacts the endpoint.

// Wiring shared by the overloads. Internal.
module internal S3Registration =

    /// Binds the section, applies the callback, validates eagerly, and
    /// registers the client plus every store.
    /// <param name="builder">The Legate builder receiving the stores.</param>
    /// <param name="configuration">The application configuration root.</param>
    /// <param name="configure">Adjusts the bound options, or null to keep them.</param>
    /// <returns>The same builder, for chaining.</returns>
    let useS3
        (builder: LegateBuilder, configuration: IConfiguration, configure: Action<S3StorageOptions> | null)
        : LegateBuilder =
        ArgumentNullException.ThrowIfNull(builder)
        ArgumentNullException.ThrowIfNull(configuration)

        // Fail fast before registering: an empty bucket, a bad endpoint,
        // half-set credentials, or a non-positive presign expiry surfaces
        // here, not on first use. The signing key itself never leaves the
        // options: it is never logged and never embedded in messages.
        let probe = S3StorageOptions()
        configuration.GetSection(S3StorageOptions.ConfigurationSectionPath).Bind(probe)

        match configure with
        | null -> ()
        | callback -> callback.Invoke(probe)

        match probe.Validate() with
        | null -> ()
        | violation ->
            raise (InvalidOperationException($"Invalid {S3StorageOptions.ConfigurationSectionPath}: {violation}"))

        // Deferred and composable: the section binds at resolve time, so a
        // later Configure joins the section no matter which runs first.
        let services = builder.Services
        services.AddOptions<S3StorageOptions>() |> ignore

        let section = configuration.GetSection(S3StorageOptions.ConfigurationSectionPath)

        services.Configure<S3StorageOptions>(Action<S3StorageOptions>(fun options -> section.Bind(options)))
        |> ignore

        match configure with
        | null -> ()
        | callback -> services.Configure<S3StorageOptions>(callback) |> ignore

        let resolveOptions (provider: IServiceProvider) =
            provider.GetRequiredService<IOptions<S3StorageOptions>>().Value

        let resolveClock (provider: IServiceProvider) =
            match provider.GetService<TimeProvider>() with
            | null -> TimeProvider.System
            | clock -> clock

        services.AddSingleton<AmazonS3Client>(fun provider -> S3Client.buildClient (resolveOptions provider))
        |> ignore

        services.Replace(
            ServiceDescriptor.Singleton<IBlobStore>(
                Func<IServiceProvider, IBlobStore>(fun provider ->
                    S3BlobStore(resolveOptions provider, provider.GetRequiredService<AmazonS3Client>()) :> IBlobStore)
            )
        )
        |> ignore

        services.Replace(
            ServiceDescriptor.Singleton<IAgentPackageStore>(
                Func<IServiceProvider, IAgentPackageStore>(fun provider ->
                    S3AgentPackageStore(
                        resolveOptions provider,
                        provider.GetRequiredService<AmazonS3Client>(),
                        resolveClock provider
                    )
                    :> IAgentPackageStore)
            )
        )
        |> ignore

        builder

/// Extension methods registering the S3 stores on
/// <see cref="T:Legate.LegateBuilder" /> (<c>builder.UseS3(...)</c>).
/// Binds the <c>Legate:Storage:S3</c> section (endpoint, region, bucket,
/// credentials, path-style switch, and presign expiry) and registers the
/// client plus the blob and agent package stores the runtime resolves.
/// AWS hosts leave the endpoint empty and path-style off; Hetzner and
/// MinIO hosts set the endpoint and switch path-style on: see
/// <see cref="T:Legate.Storage.S3.S3StorageOptions" />.
[<Sealed; AbstractClass; Extension>]
type S3LegateBuilderExtensions =

    /// Binds <c>Legate:Storage:S3</c> and registers the S3 client and
    /// stores.
    /// <param name="builder">The Legate builder receiving the stores.</param>
    /// <param name="configuration">The application configuration root holding the storage section.</param>
    /// <returns>The same builder, for chaining.</returns>
    [<Extension>]
    static member UseS3(builder: LegateBuilder, configuration: IConfiguration) : LegateBuilder =
        S3Registration.useS3 (builder, configuration, null)

    /// Binds <c>Legate:Storage:S3</c>, adjusts it with the callback, and
    /// registers the S3 client and stores.
    /// <param name="builder">The Legate builder receiving the stores.</param>
    /// <param name="configuration">The application configuration root holding the storage section.</param>
    /// <param name="configure">Adjusts the bound options before registration.</param>
    /// <returns>The same builder.</returns>
    [<Extension>]
    static member UseS3
        (builder: LegateBuilder, configuration: IConfiguration, configure: Action<S3StorageOptions>)
        : LegateBuilder =
        ArgumentNullException.ThrowIfNull(configure)
        S3Registration.useS3 (builder, configuration, configure)
