// SPDX-License-Identifier: Apache-2.0
namespace Legate.Tests

open Legate.Storage.InMemory
open Legate.Testing

/// The InMemory session store derives the shared conformance suite, in the
/// primary tenant, against the deterministic TestClock: one clock drives
/// both the store's lease stamps and the suite's time advances.
type InMemorySessionStoreTests private (store, clock) =
    inherit SessionStoreConformance(store, clock)

    new() =
        let clock = TestClock()

        let store = InMemoryStoreFactory.sessionStore (InMemoryDatabase(clock))

        InMemorySessionStoreTests(store, clock)
