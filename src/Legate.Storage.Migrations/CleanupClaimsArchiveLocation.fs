// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.Migrations

open System
open FluentMigrator
open Microsoft.Extensions.Options

// Journal archive pointer (issue 111): the nullable archive_location
// column on cleanup_claims carrying where the archived journal lives, so
// the expired replay reports its pointer beside the marker. Additive-only:
// the landed baseline is untouched. Postgres-only: the SQLite event store
// keeps its pointer in its local journal_archive table (guarded alter),
// and the SQLite MigrationTests pin the baseline column shapes exactly,
// so this migration skips every other engine. First live exercise of the
// AGENTS.md yyyymmddHHMM UTC numbering rule after baseline 202609161200.

/// <summary>
/// Adds the nullable archive-location pointer to the cleanup-claims table:
/// the marker row <c>CompleteCleanup</c> leaves behind carries where the
/// archived journal lives. Postgres-only (the SQLite store keeps its
/// pointer locally); additive, never a baseline edit.
/// </summary>
/// <param name="options">The resolved migration options. Must not be null and must validate.</param>
[<Migration(202609171200L)>]
type CleanupClaimsArchiveLocation(options: IOptions<MigrationOptions>) =
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

    /// The engine names this migration runs on: both Postgres processor
    /// names FluentMigrator has used, so the match holds whichever the
    /// runner reports. SQLite ("SQLite") never matches and keeps its exact
    /// baseline column shapes.
    let engines = [| "Postgres"; "PostgreSQL" |]

    /// <summary>
    /// Applies the migration: adds the nullable archive-location column to
    /// the cleanup-claims table on Postgres; other engines skip it.
    /// </summary>
    override this.Up() : unit =
        let schema = resolved.Schema
        let target = MigrationNaming.tableName resolved "cleanup_claims"

        if schema = "" then
            (this.IfDatabase(engines).Alter.Table(target))
                .AddColumn("archive_location")
                .AsString(Int32.MaxValue)
                .Nullable()
            |> ignore
        else
            (this.IfDatabase(engines).Alter.Table(target).InSchema(schema))
                .AddColumn("archive_location")
                .AsString(Int32.MaxValue)
                .Nullable()
            |> ignore

    /// <summary>
    /// Rolls the migration back: drops the archive-location column on
    /// Postgres; other engines skip it.
    /// </summary>
    override this.Down() : unit =
        let schema = resolved.Schema
        let target = MigrationNaming.tableName resolved "cleanup_claims"

        if schema = "" then
            (this.IfDatabase(engines).Delete.Column("archive_location").FromTable(target))
            |> ignore
        else
            (this.IfDatabase(engines).Delete.Column("archive_location").FromTable(target).InSchema(schema))
            |> ignore
