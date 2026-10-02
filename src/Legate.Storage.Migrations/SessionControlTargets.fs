// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.Migrations

open System
open FluentMigrator
open FluentMigrator.Builders.Create.Table
open FluentMigrator.Builders.Create.Index
open Microsoft.Extensions.Options

/// Additive versioned control attribution, intent and decision storage. No legacy backfill.
[<Migration(202610021800L)>]
type SessionControlTargets(options: IOptions<MigrationOptions>) =
    inherit Migration()

    let resolved = options.Value

    do
        match resolved.Validate() with
        | null -> ()
        | error -> raise (ArgumentException(error, nameof options))

    /// Creates one serialized control record per tenant/session, locked with its session row.
    override this.Up() =
        let table = MigrationNaming.tableName resolved "session_control"
        let root = this.Create.Table(table)

        let columns: ICreateTableWithColumnSyntax =
            if resolved.Schema = "" then
                root :> ICreateTableWithColumnSyntax
            else
                root.InSchema(resolved.Schema)

        columns
            .WithColumn("tenant")
            .AsString()
            .NotNullable()
            .WithColumn("session_id")
            .AsString()
            .NotNullable()
            .WithColumn("format_version")
            .AsInt32()
            .NotNullable()
            .WithColumn("control_json")
            .AsString(Int32.MaxValue)
            .NotNullable()
        |> ignore

        let index =
            this.Create.Index(MigrationNaming.indexName resolved "session_control" "uq_session").OnTable(table)

        let keys: ICreateIndexOnColumnSyntax =
            if resolved.Schema = "" then
                index :> ICreateIndexOnColumnSyntax
            else
                index.InSchema(resolved.Schema)

        keys.OnColumn("tenant").Ascending().OnColumn("session_id").Ascending().WithOptions().Unique()
        |> ignore

    /// Removes only the new control table on explicit rollback.
    override this.Down() =
        let table = MigrationNaming.tableName resolved "session_control"

        if resolved.Schema = "" then
            this.Delete.Table(table) |> ignore
        else
            this.Delete.Table(table).InSchema(resolved.Schema) |> ignore
