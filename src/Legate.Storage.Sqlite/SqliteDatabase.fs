// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.Sqlite

open System
open System.IO
open System.Text.Json
open FluentMigrator.Runner
open Legate.Storage.Migrations
open Microsoft.Data.Sqlite
open Microsoft.Extensions.DependencyInjection

// The shared SQLite database behind every store: one file, one gate lock,
// the injected clock lease expiry reads, and the single-process file guard.
// Every store opens short-lived connections against this database; claims
// and fenced writes run as single transactions under SQLite's single-writer
// lock with a busy timeout, and every SqliteException funnels through the
// SqliteErrors boundary. The baseline ten tables come from the shared
// AddLegateMigrations baseline (empty schema, configured prefix); the blob
// and package tables the baseline does not cover are created here with
// plain DDL over the same file, so one path holds every store.

// ──────────────────────────────────────────────────────────────────────────
// Shared JSON

/// <summary>
/// Shared storage JSON: the options every store serialises payload columns
/// with. Internal: the JSON is opaque storage, not a wire contract.
/// </summary>
module internal SqliteJson =

    /// <summary>
    /// The shared serializer options for storage payload columns.
    /// </summary>
    let options = JsonSerializerOptions()

    /// <summary>
    /// Serialises a value to its storage JSON form, preserving its static
    /// type so polymorphic discriminators round-trip.
    /// </summary>
    /// <param name="value">The value to serialise.</param>
    /// <returns>The JSON text.</returns>
    let inline serialize (value: 'T) : string =
        JsonSerializer.Serialize((box value), typeof<'T>, options)

    /// <summary>
    /// Deserialises a storage JSON column back to its value.
    /// </summary>
    /// <param name="json">The JSON text. Must not be null.</param>
    /// <returns>The deserialised value.</returns>
    let deserialize<'T> (json: string) : 'T =
        match JsonSerializer.Deserialize(json, typeof<'T>, options) with
        | null -> raise (JsonException "Storage JSON must not be null.")
        | boxed -> unbox<'T> boxed

// ──────────────────────────────────────────────────────────────────────────
// The database

/// <summary>
/// The shared SQLite database behind every store: one WAL-mode file, one
/// in-process gate, and the single-process file guard. Construct once per
/// file and hand to every store, so the event store fences appends on the
/// session store's claim tokens and the custom-tool store resolves agents
/// through the agent rows over the same state.
/// </summary>
type SqliteDatabase
    private (path: string, timeProvider: TimeProvider, prefix: string, busyMs: int, lockStream: FileStream) =

    do
        if String.IsNullOrWhiteSpace path then
            raise (ArgumentException("The database path must be a non-empty file path.", nameof path))

        if isNull (box timeProvider) then
            raise (ArgumentNullException(nameof timeProvider))

    let gate = obj ()
    let mutable disposed = false

    // ── construction ──

    /// <summary>
    /// Opens (creating with its directory when absent) the database at
    /// <paramref name="path" />, acquires the single-process guard, applies
    /// WAL mode and the busy timeout, and runs the shared baseline
    /// migrations plus the local blob and package tables when
    /// <paramref name="runMigrations" /> is true.
    /// </summary>
    /// <param name="path">The database file path. Created with its directory on first use.</param>
    /// <param name="timeProvider">The clock lease expiry reads. Must not be null.</param>
    /// <param name="prefix">The table prefix prepended to every table name.</param>
    /// <param name="busyTimeout">How long writers wait on the single-writer lock.</param>
    /// <param name="runMigrations">Whether to run the baseline migrations and local tables on open.</param>
    /// <returns>The open shared database.</returns>
    /// <exception cref="T:Legate.Storage.Sqlite.SqliteLockedException">The file is already open in another process.</exception>
    static member Open
        (path: string, timeProvider: TimeProvider, prefix: string, busyTimeout: TimeSpan, runMigrations: bool)
        : SqliteDatabase =
        if String.IsNullOrWhiteSpace path then
            raise (ArgumentException("The database path must be a non-empty file path.", nameof path))

        if isNull (box timeProvider) then
            raise (ArgumentNullException(nameof timeProvider))

        let safePrefix = if isNull (box prefix) then "" else prefix

        let busyMs = int (max 0.0 busyTimeout.TotalMilliseconds)

        let directory = Path.GetDirectoryName(Path.GetFullPath path)

        match directory with
        | null -> ()
        | dir -> Directory.CreateDirectory dir |> ignore

        // Single-process guard: a lock sidecar held with no sharing, so a
        // second process opening the same file fails here with the typed
        // locked error instead of silently sharing the file.
        let lockPath = path + ".lock"

        let acquired =
            try
                let stream =
                    new FileStream(lockPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, 4096, false)

                stream.SetLength 0L
                Some stream
            with
            | :? IOException -> None
            | :? UnauthorizedAccessException -> None

        let lockStream =
            match acquired with
            | Some stream -> stream
            | None -> raise (SqliteErrors.locked path "lock-held")

        let database =
            new SqliteDatabase(path, timeProvider, safePrefix, busyMs, lockStream)

        try
            database.ApplyPragmas()

            if runMigrations then
                database.RunBaselineMigrations()
                database.EnsureLocalTables()

            database
        with ex ->
            (database :> IDisposable).Dispose()

            match ex with
            | :? Legate.LegateException -> raise ex
            | :? SqliteException as sql -> raise (SqliteErrors.ofSqliteException path sql)
            | _ -> raise ex

    /// <summary>
    /// Opens the database at <paramref name="path" /> with the system clock
    /// and default tuning (five-second busy timeout, no prefix, migrations on).
    /// </summary>
    /// <param name="path">The database file path.</param>
    /// <returns>The open shared database.</returns>
    static member Open(path: string) : SqliteDatabase =
        SqliteDatabase.Open(path, TimeProvider.System, "", TimeSpan.FromSeconds 5.0, true)

    /// <summary>
    /// Opens the database at <paramref name="path" /> with the given clock
    /// and default tuning.
    /// </summary>
    /// <param name="path">The database file path.</param>
    /// <param name="timeProvider">The clock lease expiry reads. Must not be null.</param>
    /// <returns>The open shared database.</returns>
    static member Open(path: string, timeProvider: TimeProvider) : SqliteDatabase =
        SqliteDatabase.Open(path, timeProvider, "", TimeSpan.FromSeconds 5.0, true)

    /// <summary>
    /// Opens the database at the options path with the given clock.
    /// </summary>
    /// <param name="options">The storage options. Must not be null and must validate.</param>
    /// <param name="timeProvider">The clock lease expiry reads. Must not be null.</param>
    /// <returns>The open shared database.</returns>
    static member Open(options: SqliteStorageOptions, timeProvider: TimeProvider) : SqliteDatabase =
        if isNull (box options) then
            raise (ArgumentNullException(nameof options))

        match options.Validate() with
        | null -> ()
        | reason -> raise (ArgumentException(reason, nameof options))

        SqliteDatabase.Open(options.Path, timeProvider, options.TablePrefix, options.BusyTimeout, options.RunMigrations)

    // ── state ──

    /// <summary>
    /// The database file path.
    /// </summary>
    member _.Path = path

    /// <summary>
    /// The clock lease expiry reads.
    /// </summary>
    member _.TimeProvider = timeProvider

    /// <summary>
    /// The table prefix prepended to every table name.
    /// </summary>
    member _.TablePrefix = prefix

    /// <summary>
    /// The one gate every store locks. Stores take it for multi-statement
    /// transitions; single-statement calls rely on SQLite autocommit
    /// atomicity plus this same gate for in-process serialisation.
    /// </summary>
    member internal _.Gate = gate

    /// <summary>
    /// The instant the stores read now from; exposed so stores on the same
    /// database stamp with one clock.
    /// </summary>
    member _.UtcNow = timeProvider.GetUtcNow()

    /// <summary>
    /// The physical table name for a logical name: the configured prefix
    /// prepended verbatim.
    /// </summary>
    /// <param name="logical">The unprefixed table name, for example "sessions".</param>
    /// <returns>The prefixed table name.</returns>
    member _.Table(logical: string) : string = prefix + logical

    /// <summary>
    /// The connection string for the file.
    /// </summary>
    member _.ConnectionString = $"Data Source=%s{path}"

    /// <summary>
    /// The busy timeout in milliseconds applied to every connection.
    /// </summary>
    member _.BusyTimeoutMs = busyMs

    // ── connections ──

    /// <summary>
    /// Opens a tuned connection to the file: WAL mode, the busy timeout,
    /// and no foreign keys (the baseline carries none). The caller owns
    /// the connection.
    /// </summary>
    /// <returns>The open connection.</returns>
    /// <exception cref="T:Legate.Storage.Sqlite.SqliteLockedException">The file is locked by another process.</exception>
    /// <exception cref="T:Legate.Storage.Sqlite.SqliteStorageException">The open failed for a non-lock reason.</exception>
    member internal this.OpenConnection() : SqliteConnection =
        try
            let connection = new SqliteConnection(this.ConnectionString)
            connection.Open()

            use pragma = connection.CreateCommand()
            pragma.CommandText <- "PRAGMA journal_mode=WAL;"
            pragma.ExecuteNonQuery() |> ignore
            pragma.CommandText <- $"PRAGMA busy_timeout=%d{busyMs};"
            pragma.ExecuteNonQuery() |> ignore
            pragma.CommandText <- "PRAGMA foreign_keys=OFF;"
            pragma.ExecuteNonQuery() |> ignore
            pragma.CommandText <- "PRAGMA synchronous=NORMAL;"
            pragma.ExecuteNonQuery() |> ignore

            connection
        with
        | :? Legate.LegateException -> reraise ()
        | :? SqliteException as sql -> raise (SqliteErrors.ofSqliteException path sql)

    // ── schema ──

    /// <summary>
    /// Applies the connection pragmas to the file without opening the
    /// guard: the first touch that creates the file when absent.
    /// </summary>
    member private this.ApplyPragmas() =
        use connection = this.OpenConnection()
        ()

    /// <summary>
    /// Runs the shared baseline migrations (empty schema, configured
    /// prefix) through the FluentMigrator SQLite processor.
    /// </summary>
    member private this.RunBaselineMigrations() =
        try
            let services = ServiceCollection()

            LegateMigrationsServiceCollectionExtensions.AddLegateMigrations(
                services,
                ?configure =
                    Some(fun (options: MigrationOptions) ->
                        options.Schema <- ""
                        options.TablePrefix <- prefix)
            )
            |> ignore

            services.ConfigureRunner(fun builder ->
                builder
                    .AddSQLite()
                    .WithGlobalConnectionString(this.ConnectionString)
                    .ScanIn(typeof<MigrationOptions>.Assembly)
                    .For.Migrations()
                |> ignore)
            |> ignore

            use provider = services.BuildServiceProvider()
            use scope = provider.CreateScope()
            let runner = scope.ServiceProvider.GetRequiredService<IMigrationRunner>()
            runner.MigrateUp()
        with
        | :? Legate.LegateException -> reraise ()
        | :? SqliteException as sql -> raise (SqliteErrors.ofSqliteException path sql)

    /// <summary>
    /// Creates the local blob and package tables the shared baseline does
    /// not cover, over the same file: one blob row per key, one package
    /// state row per agent, and one row per package version carrying its
    /// files as JSON.
    /// </summary>
    member private this.EnsureLocalTables() =
        try
            use connection = this.OpenConnection()
            use command = connection.CreateCommand()

            let blobs = this.Table "blobs"
            let packages = this.Table "packages"
            let versions = this.Table "package_versions"
            let archive = this.Table "journal_archive"

            command.CommandText <-
                $"""CREATE TABLE IF NOT EXISTS "%s{blobs}" (
  key TEXT NOT NULL PRIMARY KEY,
  content BLOB NOT NULL,
  content_type TEXT NOT NULL,
  etag TEXT NOT NULL,
  size_bytes INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS "%s{packages}" (
  tenant TEXT NOT NULL,
  agent_id TEXT NOT NULL,
  active TEXT NULL,
  source TEXT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS "IX_%s{packages}_tenant_agent" ON "%s{packages}" (tenant, agent_id);
CREATE TABLE IF NOT EXISTS "%s{versions}" (
  tenant TEXT NOT NULL,
  agent_id TEXT NOT NULL,
  version TEXT NOT NULL,
  files_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS "IX_%s{versions}_tenant_agent_version" ON "%s{versions}" (tenant, agent_id, version);
CREATE TABLE IF NOT EXISTS "%s{archive}" (
  session_id TEXT NOT NULL PRIMARY KEY,
  tenant TEXT NOT NULL,
  archived_at TEXT NOT NULL
);"""

            command.ExecuteNonQuery() |> ignore
        with
        | :? Legate.LegateException -> reraise ()
        | :? SqliteException as sql -> raise (SqliteErrors.ofSqliteException path sql)

    // ── lifecycle ──

    /// <summary>
    /// Releases the single-process guard. Stores must not be used after
    /// the database is disposed.
    /// </summary>
    interface IDisposable with
        member _.Dispose() =
            if not disposed then
                disposed <- true

                try
                    lockStream.Dispose()
                with _ ->
                    ()

    /// <summary>
    /// Formats an instant as ISO-8601 text: the portable TEXT column form
    /// the baseline uses, which sorts chronologically for UTC instants.
    /// </summary>
    /// <param name="instant">The instant to format.</param>
    /// <returns>The ISO-8601 text.</returns>
    static member ToIso(instant: DateTimeOffset) : string = instant.ToString "O"

    /// <summary>
    /// Parses an ISO-8601 text column back to its instant.
    /// </summary>
    /// <param name="text">The ISO-8601 text. Must not be null.</param>
    /// <returns>The parsed instant.</returns>
    static member OfIso(text: string) : DateTimeOffset =
        DateTimeOffset.Parse(text, null, System.Globalization.DateTimeStyles.RoundtripKind)
