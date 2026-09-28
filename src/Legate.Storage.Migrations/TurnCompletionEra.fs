// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.Migrations

open System
open FluentMigrator
open Microsoft.Extensions.Options

// Turn completion era (issue 289): the turn_completion_era table carrying
// one row per session opened after terminal completion events started
// journaling, so the live-turn orphan trigger tells orphaned marker-only
// turns (era-marked, no terminal row) from healthy pre-era restarts
// (marker-only by construction, never marked). Additive-only: the landed
// migrations are untouched. Runs on every engine: the relational session
// stores read and write it, the in-memory stores keep parity in code, and
// an absent row reads pre-era (quiet) with no backfill. Existing
// migrations at numbering time: 202609161200 (BaselineV1),
// 202609171200 (CleanupClaimsArchiveLocation), 202609191200
// (ScheduleOccurrenceKey); this stamp is 2026-09-28 12:00 UTC.

// Composite keys are unique indexes, not PRIMARY KEY constraints (the
// BaselineV1 convention: SQLite cannot add a PK constraint after
// creation, so composite keys ride unique indexes on both engines with
// NotNullable key columns). The (tenant, session_id) unique index is
// the table's key.

/// <summary>
/// Creates the turn-completion-era table: one row per session opened in
/// the completion era. Additive, never a landed-migration edit; runs on
/// every engine.
/// </summary>
/// <param name="options">The resolved migration options. Must not be null and must validate.</param>
[<Migration(202609281200L)>]
type TurnCompletionEra(options: IOptions<MigrationOptions>) =
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
    /// Applies the migration: creates the turn-completion-era table with
    /// its (tenant, session_id) unique key.
    /// </summary>
    override this.Up() : unit =
        let schema = resolved.Schema
        let target = MigrationNaming.tableName resolved "turn_completion_era"
        let key = MigrationNaming.indexName resolved "turn_completion_era" "pk_session"

        if schema = "" then
            this.Create
                .Table(target)
                .WithColumn("tenant")
                .AsString()
                .NotNullable()
                .WithColumn("session_id")
                .AsString()
                .NotNullable()
                .WithColumn("marked_at")
                .AsString()
                .NotNullable()
            |> ignore

            this.Create
                .Index(key)
                .OnTable(target)
                .OnColumn("tenant")
                .Ascending()
                .OnColumn("session_id")
                .Ascending()
                .WithOptions()
                .Unique()
            |> ignore
        else
            this.Create
                .Table(target)
                .InSchema(schema)
                .WithColumn("tenant")
                .AsString()
                .NotNullable()
                .WithColumn("session_id")
                .AsString()
                .NotNullable()
                .WithColumn("marked_at")
                .AsString()
                .NotNullable()
            |> ignore

            this.Create
                .Index(key)
                .OnTable(target)
                .InSchema(schema)
                .OnColumn("tenant")
                .Ascending()
                .OnColumn("session_id")
                .Ascending()
                .WithOptions()
                .Unique()
            |> ignore

    /// <summary>
    /// Rolls the migration back: drops the turn-completion-era table.
    /// </summary>
    override this.Down() : unit =
        let schema = resolved.Schema
        let target = MigrationNaming.tableName resolved "turn_completion_era"

        if schema = "" then
            this.Delete.Table(target) |> ignore
        else
            this.Delete.Table(target).InSchema(schema) |> ignore
