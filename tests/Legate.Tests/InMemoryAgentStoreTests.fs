// SPDX-License-Identifier: Apache-2.0
namespace Legate.Tests

open Legate.Storage.InMemory
open Legate.Testing

/// The InMemory agent store and custom-tool store derive the shared
/// conformance suite over one database, so custom-tool upserts resolve
/// agents through the same rows the agent store writes.
type InMemoryAgentStoreTests private (agentStore, toolStore) =
    inherit AgentStoreConformance(agentStore, toolStore)

    new() =
        let database = InMemoryDatabase()

        InMemoryAgentStoreTests(InMemoryStoreFactory.agentStore database, InMemoryStoreFactory.customToolStore database)
