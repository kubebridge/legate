// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.Migrations

open System
open System.Text.RegularExpressions

// Options controlling where the shared Legate relational migrations land.
// Both relational providers (SQLite, Postgres) read these through the
// FluentMigrator runner's service provider when RunMigrations is true, and
// hosts with their own runner reference this package and set the same
// options. A reference type with mutable properties so absent configuration
// keeps the defaults and C# object initialisers work.

// ──────────────────────────────────────────────────────────────────────────
// Validation

/// <summary>
/// Where the Legate relational tables live: the schema that owns them and
/// the prefix prepended to every table and index name.
/// </summary>
/// <remarks>
/// Defaults describe a fresh Postgres host: schema <c>legate</c>, no
/// prefix. The empty schema means unqualified DDL (the engine's default
/// schema): providers without schema support (SQLite: one file is one
/// database) configure the empty schema and only vary
/// <see cref="P:Legate.Storage.Migrations.MigrationOptions.TablePrefix" />.
/// A non-empty schema on such a provider fails loudly in the engine, so
/// misconfiguration surfaces at migrate time, not silently.
/// </remarks>
type MigrationOptions() =

    /// <summary>
    /// The schema that owns the Legate tables. Defaults to
    /// <c>legate</c>. Empty means unqualified DDL: the tables land in the
    /// engine's default schema, which is how providers without schema
    /// support (SQLite) apply this migration.
    /// </summary>
    member val Schema: string = "legate" with get, set

    /// <summary>
    /// Prepended to every Legate table and index name. Defaults to the
    /// empty string (unprefixed names such as <c>sessions</c>). Empty is
    /// valid; otherwise letters, digits, and underscores only, starting
    /// with a letter or underscore, so the name stays a portable
    /// identifier on every engine.
    /// </summary>
    member val TablePrefix: string = "" with get, set

    /// <summary>
    /// Checks the options: the schema must not be null (empty means
    /// unqualified DDL) and non-empty values must be portable identifiers
    /// (letters, digits, underscores; the prefix may be empty).
    /// </summary>
    /// <returns>Null when the options are valid; otherwise the reason they are not.</returns>
    member this.Validate() : string | null =
        if isNull (box this.Schema) then
            "MigrationOptions.Schema must not be null: use \"legate\" (the default) or the empty string for unqualified DDL."
        elif this.Schema <> "" && not (MigrationOptions.IsIdentifier this.Schema) then
            "MigrationOptions.Schema must be empty or a portable identifier: letters, digits, and underscores, starting with a letter or underscore."
        elif isNull (box this.TablePrefix) then
            "MigrationOptions.TablePrefix must not be null: use the empty string for unprefixed table names."
        elif this.TablePrefix <> "" && not (MigrationOptions.IsIdentifier this.TablePrefix) then
            "MigrationOptions.TablePrefix must be empty or a portable identifier: letters, digits, and underscores, starting with a letter or underscore."
        else
            null

    /// <summary>
    /// The portable-identifier rule <see cref="M:Legate.Storage.Migrations.MigrationOptions.Validate" />
    /// enforces, shared so the message and the check stay in one place.
    /// </summary>
    /// <param name="value">The value to test. Must not be null.</param>
    /// <returns>True when the value is a portable identifier; otherwise false.</returns>
    static member IsIdentifier(value: string) : bool =
        Regex.IsMatch(value, @"\A[A-Za-z_][A-Za-z0-9_]*\z", RegexOptions.CultureInvariant)
