// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.Postgres.Tests

open System
open Legate
open Legate.Storage.Postgres
open Legate.Testing

/// The PostgreSQL session store derives the shared conformance suite over
/// the Testcontainers database, with the suite's deterministic TestClock
/// driving both the store's lease stamps and the suite's time advances.
/// Truncates after every fact: the suite reuses tenants and keys across
/// facts, assuming per-test isolation.
type PostgresSessionStoreTests private (store: ISessionStore, clock: TestClock) =
    inherit SessionStoreConformance(store, clock)

    new() =
        let clock, store, _, _ = PostgresTestDatabase.createStores ()
        new PostgresSessionStoreTests(store, clock)

    interface IDisposable with
        member _.Dispose() = PostgresTestDatabase.truncate ()
