// SPDX-License-Identifier: Apache-2.0
namespace Legate.Coordination

open System
open System.Runtime.CompilerServices
open Legate
open Legate.Coordination.Redis
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.DependencyInjection.Extensions
open Microsoft.Extensions.Options
open StackExchange.Redis

// Registration over LegateBuilder: UseRedisCoordination binds the
// Legate:Llm:DistributedCoordination section onto
// DistributedCoordinationOptions, applies the optional configure callback,
// validates eagerly, and registers the shared connection plus the
// IDistributedLlmAdmission client. Uses only the public builder surface.
// The connection constructs lazily at resolve time, so registration never
// contacts Redis. No coordinator wiring and no health check: those belong
// to the wiring issue.

// Wiring shared by the overloads. Internal.
module internal RedisCoordinationRegistration =

    /// Binds the section, applies the callback, validates eagerly, and
    /// registers the connection plus the admission client.
    /// <param name="builder">The Legate builder receiving the client.</param>
    /// <param name="configuration">The application configuration root.</param>
    /// <param name="configure">Adjusts the bound options, or null to keep them.</param>
    /// <returns>The same builder, for chaining.</returns>
    let useRedisCoordination
        (builder: LegateBuilder, configuration: IConfiguration, configure: Action<DistributedCoordinationOptions> | null) : LegateBuilder =
        ArgumentNullException.ThrowIfNull(builder)
        ArgumentNullException.ThrowIfNull(configuration)

        let probe = DistributedCoordinationOptions()
        configuration.GetSection(DistributedCoordinationOptions.ConfigurationSectionPath).Bind(probe)

        match configure with
        | null -> ()
        | callback -> callback.Invoke(probe)

        match probe.Validate() with
        | null -> ()
        | violation ->
            raise (
                InvalidOperationException(
                    $"Invalid {DistributedCoordinationOptions.ConfigurationSectionPath}: {violation}"
                )
            )

        let services = builder.Services
        services.AddOptions<DistributedCoordinationOptions>() |> ignore

        let section =
            configuration.GetSection(DistributedCoordinationOptions.ConfigurationSectionPath)

        services.Configure<DistributedCoordinationOptions>(
            Action<DistributedCoordinationOptions>(fun options -> section.Bind(options))
        )
        |> ignore

        match configure with
        | null -> ()
        | callback -> services.Configure<DistributedCoordinationOptions>(callback) |> ignore

        let resolveOptions (provider: IServiceProvider) =
            provider.GetRequiredService<IOptions<DistributedCoordinationOptions>>().Value

        services.AddSingleton<IConnectionMultiplexer>(fun provider ->
            let options = resolveOptions provider
            ConnectionMultiplexer.Connect(options.ConnectionString) :> IConnectionMultiplexer)
        |> ignore

        services.Replace(
            ServiceDescriptor.Singleton<IDistributedLlmAdmission>(
                Func<IServiceProvider, IDistributedLlmAdmission>(fun provider ->
                    RedisDistributedLlmAdmission(
                        resolveOptions provider,
                        provider.GetRequiredService<IConnectionMultiplexer>()
                    )
                    :> IDistributedLlmAdmission)
            )
        )
        |> ignore

        builder

/// Extension methods registering distributed admission on
/// <see cref="T:Legate.LegateBuilder" /> (<c>builder.UseRedisCoordination(...)</c>).
/// Binds the <c>Legate:Llm:DistributedCoordination</c> section (mode,
/// connection, key prefix, startup-required, and fail-closed) and registers
/// the shared Redis connection plus the admission client the wiring issue
/// consumes. Disabled mode registers the client without contacting Redis
/// until first use.
[<Sealed; AbstractClass; Extension>]
type RedisLegateBuilderExtensions =

    /// Binds <c>Legate:Llm:DistributedCoordination</c> and registers the
    /// Redis connection and admission client.
    /// <param name="builder">The Legate builder receiving the client.</param>
    /// <param name="configuration">The application configuration root holding the coordination section.</param>
    /// <returns>The same builder, for chaining.</returns>
    [<Extension>]
    static member UseRedisCoordination(builder: LegateBuilder, configuration: IConfiguration) : LegateBuilder =
        RedisCoordinationRegistration.useRedisCoordination (builder, configuration, null)

    /// Binds <c>Legate:Llm:DistributedCoordination</c>, adjusts it with the
    /// callback, and registers the Redis connection and admission client.
    /// <param name="builder">The Legate builder receiving the client.</param>
    /// <param name="configuration">The application configuration root holding the coordination section.</param>
    /// <param name="configure">Adjusts the bound options before registration.</param>
    /// <returns>The same builder.</returns>
    [<Extension>]
    static member UseRedisCoordination
        (builder: LegateBuilder, configuration: IConfiguration, configure: Action<DistributedCoordinationOptions>)
        : LegateBuilder =
        ArgumentNullException.ThrowIfNull(configure)
        RedisCoordinationRegistration.useRedisCoordination (builder, configuration, configure)
