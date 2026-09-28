// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.InMemory

open System
open System.Collections.Generic
open System.Runtime.CompilerServices
open System.Threading
open System.Threading.Tasks
open Legate

// Completion-era marks over one database (issue 289): the in-memory
// parity for the turn_completion_era table. One mark per session, held in
// a table keyed weakly off the database instance, so a fresh
// InMemoryDatabase per test gives a fresh era map (no cross-test
// pollution) and an absent key reads false (pre-era quiet). The reader
// and marker match the runtime's CompletionEra delegates structurally,
// so callers pass them without naming cross-assembly internals.

/// <summary>
/// Completion-era marks over one <see cref="T:Legate.Storage.InMemory.InMemoryDatabase" />:
/// the in-memory parity for the relational turn_completion_era table.
/// Internal: tests and the session harness close over these.
/// </summary>
module internal InMemoryCompletionEra =

    let private marks =
        ConditionalWeakTable<InMemoryDatabase, Dictionary<TenantId * SessionId, DateTimeOffset>>()

    let private table (database: InMemoryDatabase) : Dictionary<TenantId * SessionId, DateTimeOffset> =
        ArgumentNullException.ThrowIfNull(database)
        marks.GetValue(database, fun _ -> Dictionary<TenantId * SessionId, DateTimeOffset>())

    /// Reads whether the session is era-marked: true once marked over
    /// this database, false for absent keys (pre-era quiet).
    let isMarked
        (database: InMemoryDatabase)
        (tenant: TenantId)
        (sessionId: SessionId)
        (_cancellationToken: CancellationToken)
        : Task<bool> =
        let stamped = table database

        let found = lock stamped (fun () -> stamped.ContainsKey((tenant, sessionId)))

        Task.FromResult found

    /// Marks the session era-marked over this database. Idempotent:
    /// re-marking refreshes the stamp.
    let mark
        (database: InMemoryDatabase)
        (tenant: TenantId)
        (sessionId: SessionId)
        (_cancellationToken: CancellationToken)
        : Task =
        let stamped = table database

        lock stamped (fun () -> stamped[(tenant, sessionId)] <- DateTimeOffset.UtcNow)
        |> ignore

        Task.CompletedTask
