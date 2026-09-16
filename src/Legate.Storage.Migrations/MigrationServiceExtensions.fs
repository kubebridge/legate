// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.Migrations

open System
open System.Runtime.CompilerServices
open FluentMigrator.Runner
open Microsoft.Extensions.DependencyInjection

// Runner wiring for the shared Legate migrations. Providers (SQLite,
// Postgres) call AddLegateMigrations when RunMigrations is true and add
// their own processor and connection string with a second ConfigureRunner;
// hosts with their own runner reference this package and do the same. The
// wiring is processor-free on purpose: this assembly must not drag a
// database driver into hosts that use the other provider.

// ──────────────────────────────────────────────────────────────────────────
// Overloads

/// <summary>
/// Entry points for the shared Legate migrations:
/// <c>services.AddLegateMigrations()</c>,
/// <c>services.AddLegateMigrations(fun o -&gt; ...)</c>, and the C# form
/// <c>services.AddLegateMigrations(o =&gt; ...)</c>. Registers the
/// FluentMigrator core, scans this assembly for migrations, and binds
/// <see cref="T:Legate.Storage.Migrations.MigrationOptions" /> through the
/// options pipeline so the migrations resolve schema and prefix from the
/// runner's service provider. The database processor and connection string
/// stay with the caller (a provider or a host with its own runner).
/// </summary>
[<Sealed; AbstractClass; Extension>]
type LegateMigrationsServiceCollectionExtensions =

    /// <summary>
    /// Registers the shared Legate migrations in
    /// <paramref name="services" /> and applies the host's options
    /// configuration, the F# form (a function over the options, omitted
    /// for defaults: schema <c>legate</c>, no table prefix). F# callers
    /// pin this overload with the named optional argument
    /// (<c>?configure = Some ...</c>); the C# sibling takes an
    /// <see cref="T:System.Action`1" />.
    /// </summary>
    /// <param name="services">The container to add the migrations to.</param>
    /// <param name="configure">The configuration over the migration options, or omitted for defaults.</param>
    /// <returns>The same container, for call chaining.</returns>
    [<Extension>]
    static member AddLegateMigrations
        (services: IServiceCollection, ?configure: MigrationOptions -> unit)
        : IServiceCollection =
        ArgumentNullException.ThrowIfNull(services)

        LegateMigrationsServiceCollectionExtensions.RegisterMigrations(services, configure)

    /// <summary>
    /// Registers the shared Legate migrations in
    /// <paramref name="services" /> and applies the host's options
    /// configuration, the C# callable form of the sibling overload.
    /// </summary>
    /// <param name="services">The container to add the migrations to.</param>
    /// <param name="configure">The configuration over the migration options.</param>
    /// <returns>The same container, for call chaining.</returns>
    [<Extension>]
    static member AddLegateMigrations
        (services: IServiceCollection, configure: Action<MigrationOptions>)
        : IServiceCollection =
        ArgumentNullException.ThrowIfNull(services)
        ArgumentNullException.ThrowIfNull(configure)

        let hostConfigure: MigrationOptions -> unit =
            fun options -> configure.Invoke(options)

        LegateMigrationsServiceCollectionExtensions.RegisterMigrations(services, Some hostConfigure)

    /// <summary>
    /// Shared registration both overloads delegate to: options validation
    /// fails fast at registration, then the FluentMigrator core and the
    /// migration scan.
    /// </summary>
    /// <param name="services">The container to add the migrations to.</param>
    /// <param name="configure">The options configuration, or None for defaults.</param>
    /// <returns>The same container, for call chaining.</returns>
    static member private RegisterMigrations
        (services: IServiceCollection, configure: (MigrationOptions -> unit) option)
        : IServiceCollection =
        let probe = MigrationOptions()

        match configure with
        | Some configureHost -> configureHost probe
        | _ -> ()

        match probe.Validate() with
        | null -> ()
        | reason -> raise (ArgumentException(reason, nameof configure))

        services.AddOptions() |> ignore

        match configure with
        | Some configureHost ->
            services.Configure(fun (options: MigrationOptions) -> configureHost options)
            |> ignore
        | _ -> services.Configure(fun (_: MigrationOptions) -> ()) |> ignore

        services.AddFluentMigratorCore() |> ignore

        services.ConfigureRunner(fun builder ->
            builder.ScanIn(typeof<MigrationOptions>.Assembly).For.Migrations() |> ignore)
        |> ignore

        services

// ──────────────────────────────────────────────────────────────────────────
// Original F# surface

/// <summary>
/// Functions that register the shared Legate migrations in a service
/// container.
/// </summary>
[<AutoOpen>]
module MigrationServiceExtensions =

    /// <summary>
    /// Registers the shared Legate migrations in
    /// <paramref name="services" /> with default options (schema
    /// <c>legate</c>, no table prefix). The database processor and
    /// connection string stay with the caller.
    /// </summary>
    /// <param name="services">The container to add the migrations to.</param>
    /// <returns>The same container, for call chaining.</returns>
    let AddLegateMigrations (services: IServiceCollection) : IServiceCollection =
        ArgumentNullException.ThrowIfNull(services)
        LegateMigrationsServiceCollectionExtensions.AddLegateMigrations(services)
