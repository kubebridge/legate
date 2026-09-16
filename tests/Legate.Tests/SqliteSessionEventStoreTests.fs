// SPDX-License-Identifier: Apache-2.0
namespace Legate.Tests

open System
open Legate.Storage.Sqlite
open Legate.Testing

/// The SQLite event store derives the shared conformance suite, paired with
/// the session store over the same database so appends fence on the session
/// store's live claim token, against the deterministic TestClock.
type SqliteSessionEventStoreTests private (eventStore, sessionStore, clock, database: SqliteDatabase, path: string) =
    inherit SessionEventStoreConformance(eventStore, sessionStore, clock)

    new() =
        let clock = TestClock()
        let database, path = SqliteTestFixture.openTestDatabase clock

        new SqliteSessionEventStoreTests(
            SqliteStoreFactory.eventStore database,
            SqliteStoreFactory.sessionStore database,
            clock,
            database,
            path
        )

    interface IDisposable with
        member _.Dispose() =
            (database :> IDisposable).Dispose()
            SqliteTestFixture.deleteDatabaseFiles path
