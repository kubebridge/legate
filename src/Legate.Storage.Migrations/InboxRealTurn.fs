// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.Migrations

open System
open FluentMigrator
open Microsoft.Extensions.Options

// Real-turn identity per accepted inbox entry (issue 374): one nullable
// turn id per inbox row, stamped at accept for user messages. Reply entries
// and legacy rows stay null (the default sentinel); the stores bind legacy
// pending rows once at first claim without rewriting history. Additive-only:
// landed migrations are untouched, no backfill, no synthetic-history rewrite.

/// <summary>
/// Adds the real-turn identity column to the inbox table.
/// Additive, never a landed-migration edit; runs on every engine.
/// </summary>
/// <param name="options">The resolved migration options. Must not be null and must validate.</param>
[<Migration(202610041200L)>]
type InboxRealTurn(options: IOptions<MigrationOptions>) =
    inherit Migration()
    let resolved = options.Value

    do
        match resolved.Validate() with
        | null -> ()
        | error -> raise (ArgumentException(error, nameof options))

    /// <summary>
    /// Applies the migration: adds the nullable turn_id column to inbox.
    /// Old rows remain null and read as the default sentinel.
    /// </summary>
    override this.Up() =
        let column =
            this.Create.Column("turn_id").OnTable(MigrationNaming.tableName resolved "inbox")

        if resolved.Schema = "" then
            column.AsString(26).Nullable() |> ignore
        else
            column.InSchema(resolved.Schema).AsString(26).Nullable() |> ignore

    /// <summary>
    /// Removes only this migration's column on explicit rollback.
    /// </summary>
    override this.Down() =
        let column =
            this.Delete.Column("turn_id").FromTable(MigrationNaming.tableName resolved "inbox")

        if resolved.Schema = "" then
            column |> ignore
        else
            column.InSchema(resolved.Schema) |> ignore
