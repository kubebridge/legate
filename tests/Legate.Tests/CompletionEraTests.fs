// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.CompletionEraTests

open System
open System.IO
open System.Threading
open System.Threading.Tasks
open Legate
open Legate.Storage.InMemory
open Legate.Storage.Sqlite
open Legate.Testing
open FsUnit.Xunit
open Xunit

// Completion-era gate (issue 289): the InMemory parity map and the
// relational turn_completion_era table agree that absent rows read
// pre-era (quiet) and marked sessions read marked, with per-database
// isolation.

let private tenant = TenantId.Create "acme"

[<Fact>]
let ``InMemory absent sessions read pre-era`` () =
    task {
        let database = InMemoryDatabase()

        let! marked = InMemoryCompletionEra.isMarked database tenant (SessionId.New()) CancellationToken.None

        marked |> should equal false
    }

[<Fact>]
let ``InMemory mark then read round-trips`` () =
    task {
        let database = InMemoryDatabase()
        let sessionId = SessionId.New()

        do! InMemoryCompletionEra.mark database tenant sessionId CancellationToken.None

        let! marked = InMemoryCompletionEra.isMarked database tenant sessionId CancellationToken.None

        marked |> should equal true

        // Re-marking stays marked.
        do! InMemoryCompletionEra.mark database tenant sessionId CancellationToken.None

        let! again = InMemoryCompletionEra.isMarked database tenant sessionId CancellationToken.None

        again |> should equal true
    }

[<Fact>]
let ``InMemory era maps isolate per database`` () =
    task {
        let first = InMemoryDatabase()
        let second = InMemoryDatabase()
        let sessionId = SessionId.New()

        do! InMemoryCompletionEra.mark first tenant sessionId CancellationToken.None

        let! other = InMemoryCompletionEra.isMarked second tenant sessionId CancellationToken.None

        other |> should equal false
    }

/// Opens a temp-file SQLite database with migrations applied, running the
/// work before disposing it and deleting the files.
let private withSqliteDatabase (work: SqliteDatabase -> Task<'T>) : Task<'T> =
    task {
        let path =
            Path.Combine(Path.GetTempPath(), "legate-era-" + Guid.NewGuid().ToString("N") + ".db")

        let database = SqliteDatabase.Open(path, TimeProvider.System)

        try
            return! work database
        finally
            (database :> IDisposable).Dispose()

            for suffix in [ ""; ".lock"; "-wal"; "-shm" ] do
                try
                    File.Delete(path + suffix)
                with _ ->
                    ()
    }

[<Fact>]
let ``SQLite absent sessions read pre-era`` () : Task =
    withSqliteDatabase (fun database ->
        task {
            let store = SqliteSessionStore(database)
            let sessionId = SessionId.New()

            let! marked = store.IsCompletionEraMarkedAsync(tenant, sessionId, CancellationToken.None)

            marked |> should equal false
        })

[<Fact>]
let ``SQLite mark then read round-trips`` () : Task =
    withSqliteDatabase (fun database ->
        task {
            let store = SqliteSessionStore(database)
            let sessionId = SessionId.New()

            do! store.MarkCompletionEraAsync(tenant, sessionId, CancellationToken.None)

            let! marked = store.IsCompletionEraMarkedAsync(tenant, sessionId, CancellationToken.None)

            marked |> should equal true

            // Re-marking stays marked.
            do! store.MarkCompletionEraAsync(tenant, sessionId, CancellationToken.None)

            let! again = store.IsCompletionEraMarkedAsync(tenant, sessionId, CancellationToken.None)

            again |> should equal true
        })
