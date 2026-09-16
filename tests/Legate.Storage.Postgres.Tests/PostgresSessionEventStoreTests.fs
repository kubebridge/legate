// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.Postgres.Tests

open System
open Legate
open Legate.Testing

/// The PostgreSQL event store derives the shared conformance suite over
/// the Testcontainers database: the session store over the same database
/// is the fence source appends verify against. Truncates after every
/// fact, like the session suite.
type PostgresSessionEventStoreTests
    private (eventStore: ISessionEventStore, sessionStore: ISessionStore, clock: TestClock) =
    inherit SessionEventStoreConformance(eventStore, sessionStore, clock)

    new() =
        let clock, sessions, events, _ = PostgresTestDatabase.createStores ()
        new PostgresSessionEventStoreTests(events, sessions, clock)

    interface IDisposable with
        member _.Dispose() = PostgresTestDatabase.truncate ()
