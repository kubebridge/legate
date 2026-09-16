// SPDX-License-Identifier: Apache-2.0
namespace Legate.Tests

open System
open Legate.Storage.Sqlite
open Legate.Testing

/// The SQLite agent package store derives the shared conformance suite over
/// a temp-file database.
type SqliteAgentPackageStoreTests private (store, database: SqliteDatabase, path: string) =
    inherit AgentPackageStoreConformance(store)

    new() =
        let clock = TestClock()
        let database, path = SqliteTestFixture.openTestDatabase clock

        new SqliteAgentPackageStoreTests(SqliteStoreFactory.packageStore database, database, path)

    interface IDisposable with
        member _.Dispose() =
            (database :> IDisposable).Dispose()
            SqliteTestFixture.deleteDatabaseFiles path
