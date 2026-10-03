// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.Migrations

open System
open FluentMigrator
open FluentMigrator.Builders.Create.Table
open FluentMigrator.Builders.Create.Index
open Microsoft.Extensions.Options

// Terminal settlement bookkeeping (issue 363): one minimal receipt per inbox entry position,
// keyed by tenant, session, and immutable entry position. Existing versions at numbering time:
// 202609161200 (BaselineV1), 202609171200 (CleanupClaimsArchiveLocation), 202609191200
// (ScheduleOccurrenceKey), 202609281200 (TurnCompletionEra), 202610021800 (SessionControlTargets),
// 202610031200 (CompletionDestination); this stamp is 2026-10-03 18:00 UTC. Additive-only:
// landed migrations are untouched, no legacy backfill, retention follows inbox cleanup.

/// <summary>
/// Creates the execution-settlements table: one terminal receipt per settled inbox entry.
/// Additive, never a landed-migration edit; runs on every engine.
/// </summary>
/// <param name="options">The resolved migration options. Must not be null and must validate.</param>
[<Migration(202610031800L)>]
type ExecutionSettlements(options: IOptions<MigrationOptions>) =
    inherit Migration()

    let resolved = options.Value

    do
        match resolved.Validate() with
        | null -> ()
        | error -> raise (ArgumentException(error, nameof options))

    /// <summary>
    /// Applies the migration: creates the execution-settlements table with its
    /// (tenant, session_id, position) unique key.
    /// </summary>
    override this.Up() : unit =
        let table = MigrationNaming.tableName resolved "execution_settlements"
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
            .WithColumn("position")
            .AsInt64()
            .NotNullable()
            .WithColumn("claim_turn_id")
            .AsString()
            .NotNullable()
            .WithColumn("claim_token")
            .AsString()
            .NotNullable()
            .WithColumn("claim_attempt")
            .AsInt32()
            .NotNullable()
            .WithColumn("request_json")
            .AsString(Int32.MaxValue)
            .Nullable()
            .WithColumn("outcome_json")
            .AsString(Int32.MaxValue)
            .Nullable()
        |> ignore

        let index =
            this.Create.Index(MigrationNaming.indexName resolved "execution_settlements" "pk_entry").OnTable(table)

        let keys: ICreateIndexOnColumnSyntax =
            if resolved.Schema = "" then
                index :> ICreateIndexOnColumnSyntax
            else
                index.InSchema(resolved.Schema)

        keys
            .OnColumn("tenant")
            .Ascending()
            .OnColumn("session_id")
            .Ascending()
            .OnColumn("position")
            .Ascending()
            .WithOptions()
            .Unique()
        |> ignore

    /// <summary>
    /// Rolls the migration back: drops the execution-settlements table.
    /// </summary>
    override this.Down() : unit =
        let table = MigrationNaming.tableName resolved "execution_settlements"

        if resolved.Schema = "" then
            this.Delete.Table(table) |> ignore
        else
            this.Delete.Table(table).InSchema(resolved.Schema) |> ignore
