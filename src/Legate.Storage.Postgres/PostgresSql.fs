// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.Postgres

open System
open System.Collections.Concurrent
open System.Data.Common
open System.Globalization
open System.Text.Json
open FluentMigrator.Runner
open Legate.Storage.Migrations
open Microsoft.Extensions.DependencyInjection
open Npgsql
open NpgsqlTypes

// Shared plumbing for the PostgreSQL stores: table-name resolution over
// PostgresOptions, JSON serialisation, UTC instant stamps, connection and
// transaction handling, typed parameters, and the once-per-options
// migration ensure. Internal: hosts configure PostgresOptions and resolve
// the stores, never these helpers.
module internal PostgresSql =

    /// The JSON settings the stores serialise payloads with: the defaults,
    /// matching the in-memory byte estimates, so limit accounting agrees
    /// across implementations.
    let jsonOptions = JsonSerializerOptions()

    /// Serialises a payload to its JSON text.
    let serialize<'T> (value: 'T) : string =
        JsonSerializer.Serialize(value, jsonOptions)

    /// Stamps an instant as UTC ISO-8601 text, the portable form the
    /// baseline stores timestamps in. Fixed-width and chronological under
    /// ordinal comparison while every stamp shares the UTC offset.
    let stamp (instant: DateTimeOffset) : string =
        instant.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture)

    /// Parses a stamp written by <see cref="M:Legate.Storage.Postgres.PostgresSql.stamp" />.
    let parseStamp (text: string) : DateTimeOffset =
        DateTimeOffset.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind)

    /// The physical table name for a logical name: the configured prefix
    /// prepended verbatim, mirroring the migration naming.
    let tableName (options: PostgresOptions) (logicalName: string) : string = options.TablePrefix + logicalName

    /// The quoted, schema-qualified table reference for SQL text: quoted
    /// because the names are validated portable identifiers, qualified
    /// unless the schema is empty (unqualified DDL).
    let qualified (options: PostgresOptions) (logicalName: string) : string =
        let name = tableName options logicalName

        if options.Schema = "" then
            sprintf "\"%s\"" name
        else
            sprintf "\"%s\".\"%s\"" options.Schema name

    /// Opens a connection on the options' connection string.
    let openConnection (options: PostgresOptions) : NpgsqlConnection =
        let connection = new NpgsqlConnection(options.ConnectionString)
        connection.Open()
        connection

    /// Runs the work inside one transaction on a fresh connection: every
    /// store call lands in one transaction, so claims, renewals, and
    /// settlements are atomic. A thrown exception rolls the transaction
    /// back on dispose.
    let transact<'T> (options: PostgresOptions) (work: NpgsqlConnection -> NpgsqlTransaction -> 'T) : 'T =
        use connection = openConnection options
        use transaction = connection.BeginTransaction()
        let result = work connection transaction
        transaction.Commit()
        result

    /// Creates a command on the connection and transaction.
    let command (connection: NpgsqlConnection) (transaction: NpgsqlTransaction | null) (text: string) : NpgsqlCommand =
        match box transaction with
        | null -> new NpgsqlCommand(text, connection)
        | _ -> new NpgsqlCommand(text, connection, transaction)

    /// Adds a typed parameter, mapping null to DBNull so nullable columns
    /// round-trip.
    let private addParam (cmd: NpgsqlCommand) (name: string) (dbType: NpgsqlDbType) (value: obj | null) : unit =
        let parameter = NpgsqlParameter(name, dbType)
        parameter.Value <- if isNull (box value) then box DBNull.Value else box value
        cmd.Parameters.Add(parameter) |> ignore

    /// Adds a text parameter (null sends NULL).
    let textParam (cmd: NpgsqlCommand) (name: string) (value: string | null) : unit =
        addParam cmd name NpgsqlDbType.Text (box value)

    /// Adds a 32-bit integer parameter.
    let intParam (cmd: NpgsqlCommand) (name: string) (value: int) : unit =
        addParam cmd name NpgsqlDbType.Integer (box value)

    /// Adds a 64-bit integer parameter.
    let longParam (cmd: NpgsqlCommand) (name: string) (value: int64) : unit =
        addParam cmd name NpgsqlDbType.Bigint (box value)

    /// Adds a boolean parameter.
    let boolParam (cmd: NpgsqlCommand) (name: string) (value: bool) : unit =
        addParam cmd name NpgsqlDbType.Boolean (box value)

    /// Reads a nullable text column.
    let getTextOrNull (reader: DbDataReader) (ordinal: int) : string | null =
        if reader.IsDBNull(ordinal) then
            null
        else
            reader.GetString(ordinal)

// ──────────────────────────────────────────────────────────────────────────
// Migration ensure

/// Applies the shared baseline migrations to the options' database once
/// per process and options shape, so directly constructed stores honour
/// RunMigrations without host wiring. Internal: UsePostgres registers the
/// same wiring in the host container for hosts resolving IMigrationRunner
/// themselves.
module internal PostgresMigrations =

    let private ensured = ConcurrentDictionary<string, unit>()

    let private keyOf (options: PostgresOptions) : string =
        String.Concat(options.ConnectionString, "\u0000", options.Schema, "\u0000", options.TablePrefix)

    /// Migrates the options' database to the baseline unless RunMigrations
    /// is false or this shape was already migrated in this process.
    /// Idempotent: the runner applies pending versions only.
    let ensure (options: PostgresOptions) : unit =
        if not options.RunMigrations then
            ()
        else
            ensured.GetOrAdd(
                keyOf options,
                fun _ ->
                    let services = ServiceCollection()

                    LegateMigrationsServiceCollectionExtensions.AddLegateMigrations(
                        services,
                        ?configure =
                            Some(fun (migration: MigrationOptions) ->
                                migration.Schema <- options.Schema
                                migration.TablePrefix <- options.TablePrefix)
                    )
                    |> ignore

                    services.ConfigureRunner(fun builder ->
                        builder
                            .AddPostgres()
                            .WithGlobalConnectionString(options.ConnectionString)
                            .ScanIn(typeof<MigrationOptions>.Assembly)
                            .For.Migrations()
                        |> ignore)
                    |> ignore

                    use provider = services.BuildServiceProvider()
                    use scope = provider.CreateScope()

                    let runner = scope.ServiceProvider.GetRequiredService<IMigrationRunner>()
                    runner.MigrateUp()
            )
            |> ignore
