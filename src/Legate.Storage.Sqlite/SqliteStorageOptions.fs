// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.Sqlite

open System

// Options for the SQLite stores: the file path plus the engine tuning the
// plan pins (WAL mode, busy timeout, create on first use) and the table
// prefix shared with the baseline migration. A reference type with mutable
// properties so absent configuration keeps the defaults and C# object
// initialisers work.

/// <summary>
/// Where the SQLite stores live and how the engine is tuned: the database
/// file path, the busy-timeout writers wait under the single-writer lock,
/// the table prefix prepended to every table name (shared with
/// <see cref="T:Legate.Storage.Migrations.MigrationOptions" />), and
/// whether opening the database runs the shared baseline migrations.
/// </summary>
/// <remarks>
/// Defaults describe a fresh single-process host: a busy timeout of five
/// seconds, no table prefix, migrations on. An empty table prefix means
/// unprefixed names such as <c>sessions</c>. One file is one database plus
/// the engine's WAL sidecars (<c>-wal</c>, <c>-shm</c>) and the
/// single-process lock sidecar beside it.
/// </remarks>
type SqliteStorageOptions() =

    /// <summary>
    /// The SQLite database file path. Created with its directory on first
    /// use when absent. Must be a non-empty string.
    /// </summary>
    member val Path: string = "" with get, set

    /// <summary>
    /// How long a writer waits on the single-writer lock before the engine
    /// reports busy. Defaults to five seconds. Must be positive.
    /// </summary>
    member val BusyTimeout: TimeSpan = TimeSpan.FromSeconds 5.0 with get, set

    /// <summary>
    /// Prepended to every table and index name. Defaults to the empty
    /// string (unprefixed names such as <c>sessions</c>). Empty is valid;
    /// otherwise letters, digits, and underscores only, starting with a
    /// letter or underscore, so the name stays a portable identifier on
    /// every engine.
    /// </summary>
    member val TablePrefix: string = "" with get, set

    /// <summary>
    /// Whether opening the database runs the shared baseline migrations
    /// before any store reads or writes. Defaults to true; disable only
    /// when the host runs its own migrator over the same options.
    /// </summary>
    member val RunMigrations: bool = true with get, set

    /// <summary>
    /// Checks the options: the path must be non-empty, the busy timeout
    /// positive, and the table prefix empty or a portable identifier.
    /// </summary>
    /// <returns>Null when the options are valid; otherwise the reason they are not.</returns>
    member this.Validate() : string | null =
        if String.IsNullOrWhiteSpace this.Path then
            "SqliteStorageOptions.Path must be a non-empty file path."
        elif this.BusyTimeout <= TimeSpan.Zero then
            "SqliteStorageOptions.BusyTimeout must be positive."
        elif isNull (box this.TablePrefix) then
            "SqliteStorageOptions.TablePrefix must not be null: use the empty string for unprefixed table names."
        elif
            this.TablePrefix <> ""
            && not (SqliteStorageOptions.IsIdentifier this.TablePrefix)
        then
            "SqliteStorageOptions.TablePrefix must be empty or a portable identifier: letters, digits, and underscores, starting with a letter or underscore."
        else
            null

    /// <summary>
    /// The portable-identifier rule
    /// <see cref="M:Legate.Storage.Sqlite.SqliteStorageOptions.Validate" />
    /// enforces, shared so the message and the check stay in one place.
    /// </summary>
    /// <param name="value">The value to test. Must not be null.</param>
    /// <returns>True when the value is a portable identifier; otherwise false.</returns>
    static member IsIdentifier(value: string) : bool =
        System.Text.RegularExpressions.Regex.IsMatch(
            value,
            @"\A[A-Za-z_][A-Za-z0-9_]*\z",
            System.Text.RegularExpressions.RegexOptions.CultureInvariant
        )
