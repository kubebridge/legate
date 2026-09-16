// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage

open System
open System.Runtime.CompilerServices
open Legate
open Legate.Storage.Migrations
open Legate.Storage.Postgres
open FluentMigrator.Runner
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.DependencyInjection.Extensions
open Microsoft.Extensions.Options

// Registration over LegateBuilder: UsePostgres binds the
// Legate:Storage:Postgres section onto PostgresOptions, applies the
// optional configure callback, validates eagerly, and registers every
// store over the bound options: the session store, the event store, the
// agent store, and the custom-tool store. Uses only the public builder
// surface: the options through IOptions, the stores through the storage
// and agents sub-builders' containers, and the migrations through the
// shared AddLegateMigrations scan plus the Npgsql processor when
// RunMigrations is true.

// Wiring shared by the overloads. Internal.
module internal PostgresRegistration =

    /// Binds the section, applies the callback, validates eagerly, and
    /// registers deferred options plus every store.
    /// <param name="builder">The Legate builder receiving the stores.</param>
    /// <param name="configuration">The application configuration root.</param>
    /// <param name="configure">Adjusts the bound options, or null to keep them.</param>
    /// <returns>The same builder, for chaining.</returns>
    let usePostgres
        (builder: LegateBuilder, configuration: IConfiguration, configure: Action<PostgresOptions> | null)
        : LegateBuilder =
        ArgumentNullException.ThrowIfNull(builder)
        ArgumentNullException.ThrowIfNull(configuration)

        // Fail fast before registering: an empty connection string or a bad
        // schema, prefix, or limit surfaces here, not on first use.
        let probe = PostgresOptions()
        configuration.GetSection(PostgresOptions.ConfigurationSectionPath).Bind(probe)

        match configure with
        | null -> ()
        | callback -> callback.Invoke(probe)

        match probe.Validate() with
        | null -> ()
        | violation ->
            raise (InvalidOperationException($"Invalid {PostgresOptions.ConfigurationSectionPath}: {violation}"))

        // Deferred and composable: the section binds at resolve time, so a
        // later Configure joins the section no matter which runs first.
        let services = builder.Services
        services.AddOptions<PostgresOptions>() |> ignore

        let section = configuration.GetSection(PostgresOptions.ConfigurationSectionPath)

        services.Configure<PostgresOptions>(Action<PostgresOptions>(fun options -> section.Bind(options)))
        |> ignore

        match configure with
        | null -> ()
        | callback -> services.Configure<PostgresOptions>(callback) |> ignore

        let resolveOptions (provider: IServiceProvider) =
            provider.GetRequiredService<IOptions<PostgresOptions>>().Value

        let resolveClock (provider: IServiceProvider) =
            match provider.GetService<TimeProvider>() with
            | null -> TimeProvider.System
            | clock -> clock

        services.Replace(
            ServiceDescriptor.Singleton<ISessionStore>(
                Func<IServiceProvider, ISessionStore>(fun provider ->
                    PostgresSessionStore(resolveOptions provider, resolveClock provider) :> ISessionStore)
            )
        )
        |> ignore

        services.Replace(
            ServiceDescriptor.Singleton<ISessionEventStore>(
                Func<IServiceProvider, ISessionEventStore>(fun provider ->
                    PostgresSessionEventStore(resolveOptions provider, resolveClock provider) :> ISessionEventStore)
            )
        )
        |> ignore

        services.Replace(
            ServiceDescriptor.Singleton<IAgentStore>(
                Func<IServiceProvider, IAgentStore>(fun provider ->
                    PostgresAgentStore(resolveOptions provider, resolveClock provider) :> IAgentStore)
            )
        )
        |> ignore

        services.Replace(
            ServiceDescriptor.Singleton<IAgentCustomToolStore>(
                Func<IServiceProvider, IAgentCustomToolStore>(fun provider ->
                    PostgresAgentStore(resolveOptions provider, resolveClock provider) :> IAgentCustomToolStore)
            )
        )
        |> ignore

        // The shared migrations scan plus the Npgsql processor, so hosts
        // resolving IMigrationRunner migrate the same baseline the stores
        // ensure on first use. The stores themselves migrate lazily, so
        // directly constructed stores honour RunMigrations too.
        if probe.RunMigrations then
            LegateMigrationsServiceCollectionExtensions.AddLegateMigrations(
                services,
                ?configure =
                    Some(fun (migration: MigrationOptions) ->
                        migration.Schema <- probe.Schema
                        migration.TablePrefix <- probe.TablePrefix)
            )
            |> ignore

            services.ConfigureRunner(fun runner ->
                runner
                    .AddPostgres()
                    .WithGlobalConnectionString(probe.ConnectionString)
                    .ScanIn(typeof<MigrationOptions>.Assembly)
                    .For.Migrations()
                |> ignore)
            |> ignore

        builder

/// Extension methods registering the PostgreSQL stores on
/// <see cref="T:Legate.LegateBuilder" /> (<c>builder.UsePostgres(...)</c>).
/// Binds the <c>Legate:Storage:Postgres</c> section (connection string,
/// schema, table prefix, migration switch, and journal limits) and
/// registers the session, event, agent, and custom-tool stores the runtime
/// resolves.
[<Sealed; AbstractClass; Extension>]
type PostgresLegateBuilderExtensions =

    /// Binds <c>Legate:Storage:Postgres</c> and registers the PostgreSQL
    /// stores.
    /// <param name="builder">The Legate builder receiving the stores.</param>
    /// <param name="configuration">The application configuration root holding the storage section.</param>
    /// <returns>The same builder, for chaining.</returns>
    [<Extension>]
    static member UsePostgres(builder: LegateBuilder, configuration: IConfiguration) : LegateBuilder =
        PostgresRegistration.usePostgres (builder, configuration, null)

    /// Binds <c>Legate:Storage:Postgres</c>, adjusts it with the callback,
    /// and registers the PostgreSQL stores.
    /// <param name="builder">The Legate builder receiving the stores.</param>
    /// <param name="configuration">The application configuration root holding the storage section.</param>
    /// <param name="configure">Adjusts the bound options before registration.</param>
    /// <returns>The same builder, for chaining.</returns>
    [<Extension>]
    static member UsePostgres
        (builder: LegateBuilder, configuration: IConfiguration, configure: Action<PostgresOptions>)
        : LegateBuilder =
        ArgumentNullException.ThrowIfNull(configure)
        PostgresRegistration.usePostgres (builder, configuration, configure)
