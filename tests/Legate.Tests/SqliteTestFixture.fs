// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.SqliteTestFixture

open System
open System.IO
open Legate.Storage.Sqlite
open Legate.Testing

// Shared helpers for the SQLite-backed tests: isolated temp-file databases
// (no external services) plus best-effort cleanup of the file and the
// engine and guard sidecars beside it.

/// <summary>
/// A fresh temp-file database path, unique per call.
/// </summary>
/// <returns>The database file path.</returns>
let tempDatabasePath () : string =
    Path.Combine(Path.GetTempPath(), "legate-sqlite-" + Guid.NewGuid().ToString("N") + ".db")

/// <summary>
/// Opens a test database at a fresh temp path over the given clock.
/// </summary>
/// <param name="clock">The clock the database reads.</param>
/// <returns>The open database and its path.</returns>
let openTestDatabase (clock: TestClock) : SqliteDatabase * string =
    let path = tempDatabasePath ()
    SqliteDatabase.Open(path, clock), path

/// <summary>
/// Deletes the database file and its sidecars (the lock guard plus the
/// engine WAL files), ignoring failures: cleanup must never fail a test.
/// </summary>
/// <param name="path">The database file path.</param>
let deleteDatabaseFiles (path: string) : unit =
    for suffix in
        [
            ""
            ".lock"
            "-wal"
            "-shm"
            "-journal"
        ] do
        try
            File.Delete(path + suffix)
        with _ ->
            ()
