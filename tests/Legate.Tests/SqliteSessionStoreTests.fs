// SPDX-License-Identifier: Apache-2.0
namespace Legate.Tests

open System
open Legate.Storage.Sqlite
open Legate.Testing

/// The SQLite session store derives the shared conformance suite over a
/// temp-file database, against the deterministic TestClock: one clock drives
/// both the store's lease stamps and the suite's time advances.
type SqliteSessionStoreTests private (store, clock, database: SqliteDatabase, path: string) =
    inherit SessionStoreConformance(store, clock)

    new() =
        let clock = TestClock()
        let database, path = SqliteTestFixture.openTestDatabase clock

        new SqliteSessionStoreTests(SqliteStoreFactory.sessionStore database, clock, database, path)

    interface IDisposable with
        member _.Dispose() =
            (database :> IDisposable).Dispose()
            SqliteTestFixture.deleteDatabaseFiles path
