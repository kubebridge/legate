// SPDX-License-Identifier: Apache-2.0
namespace Legate.Tests

open System
open Legate.Storage.Sqlite
open Legate.Testing

/// The SQLite agent store and custom-tool store derive the shared
/// conformance suite over one database, so custom-tool upserts resolve
/// agents through the same rows the agent store writes.
type SqliteAgentStoreTests private (agentStore, toolStore, database: SqliteDatabase, path: string) =
    inherit AgentStoreConformance(agentStore, toolStore)

    new() =
        let clock = TestClock()
        let database, path = SqliteTestFixture.openTestDatabase clock

        new SqliteAgentStoreTests(
            SqliteStoreFactory.agentStore database,
            SqliteStoreFactory.customToolStore database,
            database,
            path
        )

    interface IDisposable with
        member _.Dispose() =
            (database :> IDisposable).Dispose()
            SqliteTestFixture.deleteDatabaseFiles path
