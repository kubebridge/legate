// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.Migrations

open System
open FluentMigrator
open Microsoft.Extensions.Options

/// Adds immutable routing for new deliveries without rebinding or backfilling old rows.
[<Migration(202610031200L)>]
type CompletionDestination(options: IOptions<MigrationOptions>) =
    inherit Migration()
    let resolved = options.Value

    do
        match resolved.Validate() with
        | null -> ()
        | error -> raise (ArgumentException(error, nameof options))

    /// Old rows remain null and unsupported; no default destination exists.
    override this.Up() =
        let column =
            this.Create.Column("destination_id").OnTable(MigrationNaming.tableName resolved "outbox")

        if resolved.Schema = "" then
            column.AsString(128).Nullable() |> ignore
        else
            column.InSchema(resolved.Schema).AsString(128).Nullable() |> ignore

    /// Removes only this migration's column on explicit rollback.
    override this.Down() =
        let column =
            this.Delete.Column("destination_id").FromTable(MigrationNaming.tableName resolved "outbox")

        if resolved.Schema = "" then
            column |> ignore
        else
            column.InSchema(resolved.Schema) |> ignore
