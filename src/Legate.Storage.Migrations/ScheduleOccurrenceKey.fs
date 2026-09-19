// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.Migrations

open System
open FluentMigrator
open Microsoft.Extensions.Options

// Schedule occurrence key (issue 121): the occurrence_key column on
// schedule_occurrences carrying the full {agentId}:{cron}:{occurrenceTicks}
// key the evaluator consumes exactly once through IAgentStore, with a
// unique index on (tenant, occurrence_key) so the relational consume is an
// atomic insert-or-ignore. Additive-only: the landed baseline is untouched.
// Runs on every engine: both the Postgres and the SQLite agent stores
// consume through this table. First live exercise of occurrence-key
// consumption; the baseline's provisional (tenant, agent_id,
// occurrence_utc) unique key stays for observability. Existing migrations
// at numbering time: 202609161200 (BaselineV1), 202609171200
// (CleanupClaimsArchiveLocation).

/// <summary>
/// Adds the occurrence-key column and its unique key to the
/// schedule-occurrences table: the full occurrence key the schedule
/// evaluator consumes exactly once. Additive, never a baseline edit; runs
/// on every engine.
/// </summary>
/// <param name="options">The resolved migration options. Must not be null and must validate.</param>
[<Migration(202609191200L)>]
type ScheduleOccurrenceKey(options: IOptions<MigrationOptions>) =
    inherit Migration()

    do
        if isNull (box options) then
            raise (ArgumentNullException(nameof options))

    let resolved =
        let value = options.Value

        if isNull (box value) then
            raise (
                InvalidOperationException(
                    "MigrationOptions are not registered: call AddLegateMigrations before running the Legate migrations."
                )
            )

        match value.Validate() with
        | null -> value
        | reason -> raise (ArgumentException(reason, nameof options))

    /// <summary>
    /// Applies the migration: adds the non-nullable occurrence-key column
    /// to the schedule-occurrences table with a unique index on
    /// (tenant, occurrence_key).
    /// </summary>
    override this.Up() : unit =
        let schema = resolved.Schema
        let target = MigrationNaming.tableName resolved "schedule_occurrences"

        let index =
            MigrationNaming.indexName resolved "schedule_occurrences" "uq_occurrence_key"

        if schema = "" then
            this.Alter.Table(target).AddColumn("occurrence_key").AsString().NotNullable()
            |> ignore

            this.Create
                .Index(index)
                .OnTable(target)
                .OnColumn("tenant")
                .Ascending()
                .OnColumn("occurrence_key")
                .Ascending()
                .WithOptions()
                .Unique()
            |> ignore
        else
            this.Alter.Table(target).InSchema(schema).AddColumn("occurrence_key").AsString().NotNullable()
            |> ignore

            this.Create
                .Index(index)
                .OnTable(target)
                .InSchema(schema)
                .OnColumn("tenant")
                .Ascending()
                .OnColumn("occurrence_key")
                .Ascending()
                .WithOptions()
                .Unique()
            |> ignore

    /// <summary>
    /// Rolls the migration back: drops the unique index and the
    /// occurrence-key column.
    /// </summary>
    override this.Down() : unit =
        let schema = resolved.Schema
        let target = MigrationNaming.tableName resolved "schedule_occurrences"

        let index =
            MigrationNaming.indexName resolved "schedule_occurrences" "uq_occurrence_key"

        if schema = "" then
            this.Delete.Index(index).OnTable(target) |> ignore
            this.Delete.Column("occurrence_key").FromTable(target) |> ignore
        else
            this.Delete.Index(index).OnTable(target).InSchema(schema) |> ignore

            this.Delete.Column("occurrence_key").FromTable(target).InSchema(schema)
            |> ignore
