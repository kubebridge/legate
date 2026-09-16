// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.Postgres.Tests

open System
open Legate
open Legate.Storage.Postgres
open Legate.Testing

/// The PostgreSQL agent store (both interfaces over one instance family)
/// derives the shared conformance suite over the Testcontainers database.
/// Truncates after every fact, like the session suite.
type PostgresAgentStoreTests private (agentStore: IAgentStore, toolStore: IAgentCustomToolStore) =
    inherit AgentStoreConformance(agentStore, toolStore)

    new() =
        let connectionString = PostgresTestDatabase.ensureReady ()
        let clock = TestClock()
        let options = PostgresTestDatabase.testOptions connectionString
        let agents = PostgresAgentStore(options, clock)

        new PostgresAgentStoreTests(
            agents :> IAgentStore,
            PostgresAgentStore(options, clock) :> IAgentCustomToolStore
        )

    interface IDisposable with
        member _.Dispose() = PostgresTestDatabase.truncate ()
