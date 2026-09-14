// SPDX-License-Identifier: Apache-2.0
namespace Legate.Tests

open System
open Legate.Storage.InMemory
open Legate.Testing
open Xunit

module InMemoryDatabaseTests =

    [<Fact>]
    let ``The default clock is the system provider`` () =
        let database = InMemoryDatabase()

        Assert.Equal(TimeProvider.System.GetUtcNow().UtcDateTime.Date, database.UtcNow.UtcDateTime.Date)

    [<Fact>]
    let ``The injected clock drives the store stamps`` () =
        let clock = TestClock()
        let database = InMemoryDatabase(clock)

        Assert.Equal(clock.Instant, database.UtcNow)

    [<Fact>]
    let ``Options default to unbounded`` () =
        let database = InMemoryDatabase()

        Assert.Equal(0L, database.Options.MaxEventBytes)
        Assert.Equal(0L, database.Options.MaxEventsPerSession)
        Assert.Equal(0L, database.Options.MaxJournalBytesPerSession)
        Assert.Equal(0, database.Options.MaxAppendBatchSize)

    [<Fact>]
    let ``The factory hands out every store over one database`` () =
        let database = InMemoryDatabase()

        let sessionStore, eventStore, agentStore, toolStore, blobStore, packageStore =
            InMemoryStoreFactory.allStores database

        Assert.NotNull(box sessionStore)
        Assert.NotNull(box eventStore)
        Assert.NotNull(box agentStore)
        Assert.NotNull(box toolStore)
        Assert.NotNull(box blobStore)
        Assert.NotNull(box packageStore)
