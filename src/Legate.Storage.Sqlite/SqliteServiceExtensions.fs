// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.Sqlite

open System
open System.Runtime.CompilerServices
open Legate
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Options

// Registration for the SQLite stores: UseSqlite wires one shared database
// file plus every store over it, so a host registers durable storage with
// one call. The database opens (creating the file, applying WAL mode and
// the busy timeout, running the shared baseline migrations plus the local
// blob, package, and journal-archive tables) on first resolve, reading the
// clock from the container when the host registered one.

// ──────────────────────────────────────────────────────────────────────────
// Overloads

/// <summary>
/// Entry points for the SQLite stores:
/// <c>services.UseSqlite(path)</c>,
/// <c>services.UseSqlite(fun o -&gt; ...)</c>, and the C# forms
/// <c>services.UseSqlite(path, o =&gt; ...)</c> and
/// <c>services.UseSqlite(o =&gt; ...)</c>. Registers one shared
/// <see cref="T:Legate.Storage.Sqlite.SqliteDatabase" /> plus every store
/// over it: <see cref="T:Legate.ISessionStore" />,
/// <see cref="T:Legate.ISessionEventStore" />,
/// <see cref="T:Legate.IAgentStore" />,
/// <see cref="T:Legate.IAgentCustomToolStore" />,
/// <see cref="T:Legate.IBlobStore" />, and
/// <see cref="T:Legate.IAgentPackageStore" />. The agent store serves both
/// agent contracts from one instance, so custom-tool writes resolve agents
/// through the same state.
/// </summary>
[<Sealed; AbstractClass; Extension>]
type SqliteServiceCollectionExtensions =

    /// <summary>
    /// Registers the SQLite stores over the database file at
    /// <paramref name="path" /> with default tuning (five-second busy
    /// timeout, no table prefix, migrations on, system clock unless the
    /// container holds a <see cref="T:System.TimeProvider" />).
    /// </summary>
    /// <param name="services">The container to add the stores to.</param>
    /// <param name="path">The database file path. Created with its directory on first use.</param>
    /// <returns>The same container, for call chaining.</returns>
    [<Extension>]
    static member UseSqlite(services: IServiceCollection, path: string) : IServiceCollection =
        ArgumentNullException.ThrowIfNull(services)

        if String.IsNullOrWhiteSpace path then
            raise (ArgumentException("The database path must be a non-empty file path.", nameof path))

        SqliteServiceCollectionExtensions.RegisterStores(
            services,
            Some(fun (options: SqliteStorageOptions) -> options.Path <- path)
        )

    /// <summary>
    /// Registers the SQLite stores, the C# callable form of the path
    /// overload: the file path plus host configuration over
    /// <see cref="T:Legate.Storage.Sqlite.SqliteStorageOptions" />.
    /// </summary>
    /// <param name="services">The container to add the stores to.</param>
    /// <param name="path">The database file path. Created with its directory on first use.</param>
    /// <param name="configure">The configuration over the storage options.</param>
    /// <returns>The same container, for call chaining.</returns>
    [<Extension>]
    static member UseSqlite
        (services: IServiceCollection, path: string, configure: Action<SqliteStorageOptions>)
        : IServiceCollection =
        ArgumentNullException.ThrowIfNull(services)

        if String.IsNullOrWhiteSpace path then
            raise (ArgumentException("The database path must be a non-empty file path.", nameof path))

        ArgumentNullException.ThrowIfNull(configure)

        SqliteServiceCollectionExtensions.RegisterStores(
            services,
            Some(fun options ->
                options.Path <- path
                configure.Invoke(options))
        )

    /// <summary>
    /// Registers the SQLite stores in <paramref name="services" /> and
    /// applies the host's options configuration, the F# form (a function
    /// over the options, carrying the file path). F# callers pin this
    /// overload with the named optional argument
    /// (<c>?configure = Some ...</c>); the C# siblings take an
    /// <see cref="T:System.Action`1" />.
    /// </summary>
    /// <param name="services">The container to add the stores to.</param>
    /// <param name="configure">The configuration over the storage options, or omitted when the options are bound elsewhere.</param>
    /// <returns>The same container, for call chaining.</returns>
    [<Extension>]
    static member UseSqlite
        (services: IServiceCollection, ?configure: SqliteStorageOptions -> unit)
        : IServiceCollection =
        ArgumentNullException.ThrowIfNull(services)
        SqliteServiceCollectionExtensions.RegisterStores(services, configure)

    /// <summary>
    /// Registers the SQLite stores in <paramref name="services" /> and
    /// applies the host's options configuration, the C# callable form of
    /// the sibling overload.
    /// </summary>
    /// <param name="services">The container to add the stores to.</param>
    /// <param name="configure">The configuration over the storage options.</param>
    /// <returns>The same container, for call chaining.</returns>
    [<Extension>]
    static member UseSqlite
        (services: IServiceCollection, configure: Action<SqliteStorageOptions>)
        : IServiceCollection =
        ArgumentNullException.ThrowIfNull(services)
        ArgumentNullException.ThrowIfNull(configure)

        let hostConfigure: SqliteStorageOptions -> unit =
            fun options -> configure.Invoke(options)

        SqliteServiceCollectionExtensions.RegisterStores(services, Some hostConfigure)

    /// <summary>
    /// Shared registration every overload delegates to: options validation
    /// fails fast at registration, then the shared database and every store
    /// over it.
    /// </summary>
    /// <param name="services">The container to add the stores to.</param>
    /// <param name="configure">The options configuration, or None when the options are bound elsewhere.</param>
    /// <returns>The same container, for call chaining.</returns>
    static member private RegisterStores
        (services: IServiceCollection, configure: (SqliteStorageOptions -> unit) option)
        : IServiceCollection =
        let probe = SqliteStorageOptions()

        match configure with
        | Some configureHost -> configureHost probe
        | _ -> ()

        // Validate only what is already known at registration: the busy
        // timeout and prefix fail fast here, while an empty path (bound
        // later through configuration) fails at first resolve.
        if probe.BusyTimeout <= TimeSpan.Zero then
            raise (ArgumentException("SqliteStorageOptions.BusyTimeout must be positive.", nameof configure))

        if isNull (box probe.TablePrefix) then
            raise (ArgumentException("SqliteStorageOptions.TablePrefix must not be null.", nameof configure))

        services.AddOptions() |> ignore

        match configure with
        | Some configureHost ->
            services.Configure(fun (options: SqliteStorageOptions) -> configureHost options)
            |> ignore
        | _ -> services.Configure(fun (_: SqliteStorageOptions) -> ()) |> ignore

        services.AddSingleton<SqliteDatabase>(fun provider ->
            let options = provider.GetRequiredService<IOptions<SqliteStorageOptions>>().Value

            match options.Validate() with
            | null -> ()
            | reason -> raise (ArgumentException(reason, "SqliteStorageOptions"))

            let clock =
                match provider.GetService<TimeProvider>() with
                | null -> TimeProvider.System
                | resolved -> resolved

            SqliteDatabase.Open(options, clock))
        |> ignore

        services.AddSingleton<ISessionStore>(fun provider ->
            SqliteSessionStore(provider.GetRequiredService<SqliteDatabase>()) :> ISessionStore)
        |> ignore

        services.AddSingleton<ISessionEventStore>(fun provider ->
            SqliteSessionEventStore(provider.GetRequiredService<SqliteDatabase>()) :> ISessionEventStore)
        |> ignore

        services.AddSingleton<SqliteAgentStore>(fun provider ->
            SqliteAgentStore(provider.GetRequiredService<SqliteDatabase>()))
        |> ignore

        services.AddSingleton<IAgentStore>(fun provider ->
            provider.GetRequiredService<SqliteAgentStore>() :> IAgentStore)
        |> ignore

        services.AddSingleton<IAgentCustomToolStore>(fun provider ->
            provider.GetRequiredService<SqliteAgentStore>() :> IAgentCustomToolStore)
        |> ignore

        services.AddSingleton<IBlobStore>(fun provider ->
            SqliteBlobStore(provider.GetRequiredService<SqliteDatabase>()) :> IBlobStore)
        |> ignore

        services.AddSingleton<IAgentPackageStore>(fun provider ->
            SqliteAgentPackageStore(provider.GetRequiredService<SqliteDatabase>()) :> IAgentPackageStore)
        |> ignore

        services

// ──────────────────────────────────────────────────────────────────────────
// Original F# surface

/// <summary>
/// Functions that register the SQLite stores in a service container.
/// </summary>
[<AutoOpen>]
module SqliteServiceExtensions =

    /// <summary>
    /// Registers the SQLite stores over the database file at
    /// <paramref name="path" /> with default tuning.
    /// </summary>
    /// <param name="services">The container to add the stores to.</param>
    /// <param name="path">The database file path. Created with its directory on first use.</param>
    /// <returns>The same container, for call chaining.</returns>
    let UseSqlite (services: IServiceCollection) (path: string) : IServiceCollection =
        ArgumentNullException.ThrowIfNull(services)
        SqliteServiceCollectionExtensions.UseSqlite(services, path)

/// <summary>
/// The store factory over one database: the composition root tests and
/// hosts consume, so one database instance backs every store.
/// </summary>
module SqliteStoreFactory =

    /// <summary>
    /// Constructs the session store over the database.
    /// </summary>
    /// <param name="database">The shared database. Must not be null.</param>
    /// <returns>The session store.</returns>
    let sessionStore (database: SqliteDatabase) =
        SqliteSessionStore(database) :> ISessionStore

    /// <summary>
    /// Constructs the event store over the database.
    /// </summary>
    /// <param name="database">The shared database. Must not be null.</param>
    /// <returns>The event store.</returns>
    let eventStore (database: SqliteDatabase) =
        SqliteSessionEventStore(database) :> ISessionEventStore

    /// <summary>
    /// Constructs the agent store (both interfaces) over the database.
    /// </summary>
    /// <param name="database">The shared database. Must not be null.</param>
    /// <returns>The agent store.</returns>
    let agentStore (database: SqliteDatabase) =
        SqliteAgentStore(database) :> IAgentStore

    /// <summary>
    /// Constructs the custom-tool store over the database.
    /// </summary>
    /// <param name="database">The shared database. Must not be null.</param>
    /// <returns>The custom-tool store.</returns>
    let customToolStore (database: SqliteDatabase) =
        SqliteAgentStore(database) :> IAgentCustomToolStore

    /// <summary>
    /// Constructs the blob store over the database.
    /// </summary>
    /// <param name="database">The shared database. Must not be null.</param>
    /// <returns>The blob store.</returns>
    let blobStore (database: SqliteDatabase) = SqliteBlobStore(database) :> IBlobStore

    /// <summary>
    /// Constructs the package store over the database.
    /// </summary>
    /// <param name="database">The shared database. Must not be null.</param>
    /// <returns>The package store.</returns>
    let packageStore (database: SqliteDatabase) =
        SqliteAgentPackageStore(database) :> IAgentPackageStore
