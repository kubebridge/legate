// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.Sqlite

open System
open Microsoft.Data.Sqlite

// The package-local typed error boundary. Every SQLite failure surfaces as
// one of these two LegateException subtypes, never as a raw
// SqliteException: the locked error for the second-process (or otherwise
// locked) case, and the storage error for every other database failure.
// Messages carry the database path and the lock or SQLite error context,
// never secrets or tool arguments.

// ──────────────────────────────────────────────────────────────────────────
// Public typed errors

/// <summary>
/// The SQLite database file is already open in another process (or is
/// otherwise locked): the single-process guard refused the open or a
/// lock-family SQLite error arrived mid-operation. Hosts branch on the type,
/// never on the message.
/// </summary>
/// <param name="path">The database file path that could not be locked. Never null.</param>
/// <param name="message">The exception message, without secrets or tool arguments.</param>
[<Sealed>]
type SqliteLockedException(path: string, message: string) =
    inherit Legate.LegateException(message)

    /// <summary>
    /// The database file path that could not be locked.
    /// </summary>
    member _.Path = path

/// <summary>
/// A SQLite operation failed for a non-lock reason: the single boundary
/// every store funnels <see cref="T:Microsoft.Data.Sqlite.SqliteException" />
/// through. Hosts branch on the type, never on the message.
/// </summary>
/// <param name="path">The database file path the failing operation targeted. Never null.</param>
/// <param name="message">The exception message, without secrets or tool arguments.</param>
[<Sealed>]
type SqliteStorageException(path: string, message: string) =
    inherit Legate.LegateException(message)

    /// <summary>
    /// The database file path the failing operation targeted.
    /// </summary>
    member _.Path = path

// ──────────────────────────────────────────────────────────────────────────
// Boundary mapping

/// <summary>
/// Maps data-access failures at one boundary to the package-local typed
/// errors. Internal: stores call this, hosts catch the typed errors.
/// </summary>
module internal SqliteErrors =

    /// <summary>
    /// Whether the SQLite error code belongs to the lock family a second
    /// opener or a contended writer produces (busy, locked, readonly, or
    /// busy-recovery): those map to
    /// <see cref="T:Legate.Storage.Sqlite.SqliteLockedException" />, every
    /// other SQLite failure to
    /// <see cref="T:Legate.Storage.Sqlite.SqliteStorageException" />.
    /// </summary>
    /// <param name="code">The raw SQLite result code.</param>
    /// <returns>True when the code is a lock-family code; otherwise false.</returns>
    let isLockCode (code: int) : bool =
        // Primary codes: 5 BUSY, 6 LOCKED, 8 READONLY. Extended busy codes
        // live in the low byte 5 with high bits set, so mask to the
        // primary code as well as matching the bare values.
        code = 5 || code = 6 || code = 8 || (code &&& 0xFF) = 5 || (code &&& 0xFF) = 6

    /// <summary>
    /// Maps one SQLite exception to the typed error: lock-family codes to
    /// the locked error, everything else to the storage error.
    /// </summary>
    /// <param name="path">The database file path the failing operation targeted.</param>
    /// <param name="ex">The SQLite exception to map. Must not be null.</param>
    /// <returns>The typed error carrying the path and SQLite context.</returns>
    let ofSqliteException (path: string) (ex: SqliteException) : Legate.LegateException =
        let safePath = if isNull (box path) then "" else path

        let context =
            sprintf "SQLite error %d failed: %s (path %s)." ex.SqliteErrorCode ex.Message safePath

        if isLockCode ex.SqliteErrorCode then
            SqliteLockedException(safePath, context) :> Legate.LegateException
        else
            SqliteStorageException(safePath, context) :> Legate.LegateException

    /// <summary>
    /// Maps a lock-file acquisition failure (the second-process guard) to
    /// the locked error.
    /// </summary>
    /// <param name="path">The database file path that is already open elsewhere.</param>
    /// <param name="reason">The short reason the guard refused, for example "lock-held".</param>
    /// <returns>The locked error carrying the path and reason.</returns>
    let locked (path: string) (reason: string) : Legate.LegateException =
        let safePath = if isNull (box path) then "" else path

        SqliteLockedException(
            safePath,
            sprintf "The SQLite database file is already open in another process (%s): %s." reason safePath
        )
        :> Legate.LegateException
