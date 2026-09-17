// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.MigrationTests

open System
open FluentMigrator.Runner
open FsUnit.Xunit
open Legate.Storage.Migrations
open Microsoft.Data.Sqlite
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Options
open Xunit

// Round-trip tests for the shared baseline migration on SQLite: no
// external services, one isolated shared-cache memory database per test
// (the runner opens and closes its own connections, so a keep-alive
// connection holds the database for the test's lifetime).

// ──────────────────────────────────────────────────────────────────────────
// Harness

/// <summary>
/// Opens a keep-alive connection to a uniquely named shared-cache memory
/// database and returns it with the connection string the runner uses.
/// </summary>
let private openDatabase () : SqliteConnection * string =
    let suffix = Guid.NewGuid().ToString("N")
    let name = $"legate-mig-%s{suffix}"
    let connectionString = $"Data Source=%s{name};Mode=Memory;Cache=Shared"
    let connection = new SqliteConnection(connectionString)
    connection.Open()
    connection, connectionString

/// <summary>
/// Builds the runner-wired provider: the shared AddLegateMigrations scan
/// plus the SQLite processor and connection string the providers supply.
/// </summary>
let private buildProvider (connectionString: string) (configure: MigrationOptions -> unit) : ServiceProvider =
    let services = ServiceCollection()

    // The named optional argument pins the fun overload over the Action one,
    // the same convention BuilderTests uses for AddLegate.
    LegateMigrationsServiceCollectionExtensions.AddLegateMigrations(services, ?configure = Some configure)
    |> ignore

    services.ConfigureRunner(fun builder ->
        builder
            .AddSQLite()
            .WithGlobalConnectionString(connectionString)
            .ScanIn(typeof<MigrationOptions>.Assembly)
            .For.Migrations()
        |> ignore)
    |> ignore

    services.BuildServiceProvider()

/// <summary>
/// Runs the runner's MigrateUp inside a scope and returns the provider for
/// option assertions.
/// </summary>
let private migrateUp (provider: IServiceProvider) : unit =
    use scope = provider.CreateScope()
    let runner = scope.ServiceProvider.GetRequiredService<IMigrationRunner>()
    runner.MigrateUp()

/// <summary>
/// Runs the runner's MigrateDown to version 0 (rolls the baseline back).
/// </summary>
let private migrateDown (provider: IServiceProvider) : unit =
    use scope = provider.CreateScope()
    let runner = scope.ServiceProvider.GetRequiredService<IMigrationRunner>()
    runner.MigrateDown(0L)

/// <summary>
/// The user-table names in the database (excludes SQLite internals).
/// </summary>
let private tableNames (connection: SqliteConnection) : string list =
    use command = connection.CreateCommand()

    command.CommandText <-
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name"

    use reader = command.ExecuteReader()

    [
        while reader.Read() do
            reader.GetString(0)
    ]

/// <summary>
/// The index names in the database (excludes SQLite auto-indexes).
/// </summary>
let private indexNames (connection: SqliteConnection) : string list =
    use command = connection.CreateCommand()

    command.CommandText <-
        "SELECT name FROM sqlite_master WHERE type = 'index' AND name NOT LIKE 'sqlite_%' ORDER BY name"

    use reader = command.ExecuteReader()

    [
        while reader.Read() do
            reader.GetString(0)
    ]

/// <summary>
/// The column names of one table.
/// </summary>
let private columnNames (connection: SqliteConnection) (table: string) : string list =
    use command = connection.CreateCommand()
    command.CommandText <- $"PRAGMA table_info(\"%s{table}\")"

    use reader = command.ExecuteReader()

    [
        while reader.Read() do
            reader.GetString(1)
    ]

// ──────────────────────────────────────────────────────────────────────────
// Options

[<Fact>]
let ``Migration defaults describe the legate schema with no prefix`` () =
    let options = MigrationOptions()
    options.Schema |> should equal "legate"
    options.TablePrefix |> should equal ""
    options.Validate() |> should equal null

[<Fact>]
let ``Migration options resolve through the runner-wired provider`` () =
    let keepAlive, connectionString = openDatabase ()
    use _keep = keepAlive

    use provider =
        buildProvider connectionString (fun options -> options.TablePrefix <- "lb_")

    let resolved = provider.GetRequiredService<IOptions<MigrationOptions>>().Value
    resolved.Schema |> should equal "legate"
    resolved.TablePrefix |> should equal "lb_"

[<Fact>]
let ``AddLegateMigrations rejects a null schema`` () =
    let services = ServiceCollection()

    (fun () ->
        LegateMigrationsServiceCollectionExtensions.AddLegateMigrations(
            services,
            ?configure = Some(fun (options: MigrationOptions) -> options.Schema <- Unchecked.defaultof<string>)
        )
        |> ignore)
    |> should throw typeof<ArgumentException>

[<Fact>]
let ``An empty schema validates for unqualified DDL`` () =
    let options = MigrationOptions()
    options.Schema <- ""
    options.Validate() |> should equal null

[<Fact>]
let ``Migration options reject a null prefix`` () =
    let options = MigrationOptions()
    options.TablePrefix <- Unchecked.defaultof<string>
    options.Validate() |> should not' (equal null)

// ──────────────────────────────────────────────────────────────────────────
// Baseline round-trip

[<Fact>]
let ``Baseline creates all ten tables with their indexes`` () =
    let keepAlive, connectionString = openDatabase ()
    use _keep = keepAlive

    use provider =
        buildProvider connectionString (fun (options: MigrationOptions) -> options.Schema <- "")

    migrateUp provider

    tableNames keepAlive
    |> should
        equal
        [
            // SQLite orders BINARY: the runner's VersionInfo sorts first.
            "VersionInfo"
            "agents"
            "cleanup_claims"
            "custom_tools"
            "events"
            "inbox"
            "outbox"
            "schedule_occurrences"
            "session_grants"
            "sessions"
            "turns"
        ]

    let indexes = indexNames keepAlive

    for expected in
        [
            "IX_sessions_tenant_updated"
            "IX_sessions_tenant_agent"
            "IX_inbox_session_pending"
            "IX_inbox_tenant_pending"
            "IX_inbox_uq_session_position"
            "IX_turns_session"
            "IX_turns_tenant_status"
            "IX_events_uq_session_sequence"
            "IX_agents_tenant_schedule"
            "IX_agents_uq_tenant_agent"
            "IX_custom_tools_agent_enabled"
            "IX_custom_tools_uq_tool"
            "IX_schedule_occurrences_uq_occurrence"
            "IX_outbox_pending_created"
            "IX_outbox_delivered_at"
            "IX_session_grants_uq_grant"
        ] do
        indexes |> should contain expected

[<Fact>]
let ``Baseline columns carry the contract shapes`` () =
    let keepAlive, connectionString = openDatabase ()
    use _keep = keepAlive

    use provider =
        buildProvider connectionString (fun (options: MigrationOptions) -> options.Schema <- "")

    migrateUp provider

    columnNames keepAlive "sessions"
    |> should
        equal
        [
            "id"
            "tenant"
            "agent_id"
            "title"
            "state"
            "current_turn_id"
            "created_at"
            "updated_at"
            "closed_at"
            "workspace_binding"
            "options_json"
            "permission_grants_json"
        ]

    columnNames keepAlive "inbox"
    |> should
        equal
        [
            "session_id"
            "position"
            "tenant"
            "payload_json"
            "delivery_mode"
            "consumed"
            "appended_at"
        ]

    columnNames keepAlive "turns"
    |> should
        equal
        [
            "turn_id"
            "session_id"
            "tenant"
            "attempt"
            "status"
            "claim_token"
            "claim_owner"
            "claim_expires_at"
            "started_at"
            "completed_at"
            "iterations"
            "input_tokens"
            "output_tokens"
            "error"
            "stop_cause"
            "outcome_json"
        ]

    columnNames keepAlive "events"
    |> should
        equal
        [
            "session_id"
            "sequence"
            "tenant"
            "turn_id"
            "event_type"
            "payload_json"
            "timestamp"
        ]

    columnNames keepAlive "cleanup_claims"
    |> should
        equal
        [
            "session_id"
            "tenant"
            "token"
            "owner"
            "expires_at"
        ]

    columnNames keepAlive "agents"
    |> should
        equal
        [
            "tenant"
            "agent_id"
            "name"
            "row_version"
            "definition_json"
            "schedule_enabled"
            "schedule_cron"
            "schedule_timezone"
            "created_at"
            "updated_at"
        ]

    columnNames keepAlive "custom_tools"
    |> should
        equal
        [
            "tenant"
            "agent_id"
            "name"
            "row_version"
            "enabled"
            "definition_json"
            "created_at"
            "updated_at"
        ]

    columnNames keepAlive "schedule_occurrences"
    |> should
        equal
        [
            "tenant"
            "agent_id"
            "occurrence_utc"
            "consumed"
            "consumed_at"
            "created_at"
        ]

    columnNames keepAlive "outbox"
    |> should
        equal
        [
            "idempotency_key"
            "tenant"
            "session_id"
            "completion_json"
            "created_at"
            "delivered"
            "delivered_at"
            "lease_owner"
            "lease_expires_at"
        ]

    columnNames keepAlive "session_grants"
    |> should
        equal
        [
            "tenant"
            "session_id"
            "tool_name"
            "granted_at"
        ]

[<Fact>]
let ``Baseline down removes every table it created`` () =
    let keepAlive, connectionString = openDatabase ()
    use _keep = keepAlive

    use provider =
        buildProvider connectionString (fun (options: MigrationOptions) -> options.Schema <- "")

    migrateUp provider
    migrateDown provider

    tableNames keepAlive
    |> List.filter (fun name -> name <> "VersionInfo")
    |> should be Empty

[<Fact>]
let ``The Postgres-only archive migration skips SQLite`` () =
    let keepAlive, connectionString = openDatabase ()
    use _keep = keepAlive

    use provider =
        buildProvider connectionString (fun (options: MigrationOptions) -> options.Schema <- "")

    migrateUp provider

    // The SQLite store keeps its archive pointer in its local
    // journal_archive table: the shared migration must leave the baseline
    // cleanup_claims shape exactly as it found it.
    columnNames keepAlive "cleanup_claims"
    |> should not' (contain "archive_location")

[<Fact>]
let ``Table prefix renames every baseline table and index`` () =
    let keepAlive, connectionString = openDatabase ()
    use _keep = keepAlive

    use provider =
        buildProvider connectionString (fun (options: MigrationOptions) ->
            options.Schema <- ""
            options.TablePrefix <- "lb_")

    migrateUp provider

    let tables = tableNames keepAlive

    for expected in
        [
            "lb_agents"
            "lb_cleanup_claims"
            "lb_custom_tools"
            "lb_events"
            "lb_inbox"
            "lb_outbox"
            "lb_schedule_occurrences"
            "lb_session_grants"
            "lb_sessions"
            "lb_turns"
        ] do
        tables |> should contain expected

    tables |> should not' (contain "sessions")

    indexNames keepAlive |> should contain "IX_lb_sessions_tenant_updated"
