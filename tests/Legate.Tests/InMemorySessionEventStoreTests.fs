// SPDX-License-Identifier: Apache-2.0
namespace Legate.Tests

open Legate.Storage.InMemory
open Legate.Testing

/// The InMemory event store derives the shared conformance suite, paired
/// with the session store over the same database so appends fence on the
/// session store's live claim token, against the deterministic TestClock.
type InMemorySessionEventStoreTests private (eventStore, sessionStore, clock) =
    inherit SessionEventStoreConformance(eventStore, sessionStore, clock)

    new() =
        let clock = TestClock()
        let database = InMemoryDatabase(clock)

        InMemorySessionEventStoreTests(
            InMemoryStoreFactory.eventStore database,
            InMemoryStoreFactory.sessionStore database,
            clock
        )
