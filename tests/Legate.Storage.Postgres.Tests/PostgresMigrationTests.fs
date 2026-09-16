// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.Postgres.Tests

open Npgsql
open Xunit

// First live proof of the shared baseline's schema-qualified DDL path on
// real PostgreSQL: the Testcontainers database migrates with the default
// options (schema legate, no prefix) and carries the schema plus all ten
// baseline tables. The SQLite MigrationTests only ever exercised
// unqualified DDL.
module PostgresMigrationTests =

    /// The schema-qualified table names the baseline owns.
    let private expectedTables =
        [
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

    let private tableNames (connection: NpgsqlConnection) : string list =
        use command =
            new NpgsqlCommand(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'legate' ORDER BY table_name",
                connection
            )

        use reader = command.ExecuteReader()

        [
            while reader.Read() do
                reader.GetString(0)
        ]

    let private schemaExists (connection: NpgsqlConnection) : bool =
        use command =
            new NpgsqlCommand("SELECT 1 FROM information_schema.schemata WHERE schema_name = 'legate'", connection)

        use reader = command.ExecuteReader()
        reader.Read()

    [<Fact>]
    let ``Baseline migrates the legate schema with all ten tables`` () =
        let connectionString = PostgresTestDatabase.ensureReady ()

        use connection = new NpgsqlConnection(connectionString)
        connection.Open()

        Assert.True(schemaExists connection, "The legate schema is missing.")
        Assert.Equal<string list>(expectedTables, tableNames connection)
