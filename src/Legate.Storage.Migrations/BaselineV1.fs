// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.Migrations

open System
open FluentMigrator
open FluentMigrator.Builders.Create.Index
open FluentMigrator.Builders.Create.Table
open Microsoft.Extensions.Options

// Baseline v1 relational schema for the Legate durable stores. One shared
// migration both relational providers (SQLite, Postgres) apply: the only
// migration in this assembly until the stores grow a second version (see
// AGENTS.md for the numbering rule). Conventions: enums as text, ids as
// text, timestamps as ISO-8601 text (the SQLite spike showed the SQLite
// processor has no DateTimeOffset mapping, so one portable TEXT column
// carries instants on both engines; stores write UTC, which sorts
// chronologically, and the full offset round-trips), payloads as unlimited
// text carrying System.Text.Json, secrets as opaque bytes inside the JSON,
// never logged. An empty MigrationOptions.Schema means unqualified DDL
// (the SQLite path: one file is one database, and its generator rejects
// schema-qualified names); otherwise every table, key, and index is
// schema-qualified (Postgres). No foreign keys in v1: retention janitors
// delete selectively per store contract, so joins ride the indexes
// ARCHITECTURE.md justifies. The three provisional tables
// (schedule_occurrences, outbox, session_grants) have no consumer yet
// (issues 119-128 own them) and are marked as such; later changes are
// additive-only, never baseline edits.

/// <summary>
/// Baseline v1 of the Legate relational schema: the seven table groups the
/// store contracts need (sessions, inbox, turns with claims, events,
/// cleanup claims, agents, custom tools) plus the three provisional tables
/// (schedule occurrences, completion outbox, session grants). Options arrive
/// through the runner's service provider, so each provider and host
/// configures schema and prefix once via
/// <see cref="T:Legate.Storage.Migrations.LegateMigrationsServiceCollectionExtensions" />.
/// </summary>
/// <param name="options">The resolved migration options. Must not be null and must validate.</param>
[<Migration(202609161200L)>]
type BaselineV1(options: IOptions<MigrationOptions>) =
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
    /// Applies the baseline: creates the schema (when the options name
    /// one), then the ten tables with their keys and indexes.
    /// </summary>
    override this.Up() : unit =
        let create = base.Create
        let schema = resolved.Schema
        let table = MigrationNaming.tableName resolved
        let index = MigrationNaming.indexName resolved

        // One DDL definition serves both engines: an empty schema means
        // unqualified DDL, otherwise everything is schema-qualified.
        let startTable (name: string) : ICreateTableWithColumnSyntax =
            let root = create.Table(name)

            if schema = "" then
                root :> ICreateTableWithColumnSyntax
            else
                root.InSchema(schema)

        let startIndex (ixName: string) (tableName: string) : ICreateIndexOnColumnSyntax =
            let root = create.Index(ixName).OnTable(tableName)

            if schema = "" then
                root :> ICreateIndexOnColumnSyntax
            else
                root.InSchema(schema)

        // Composite keys are unique indexes, not PRIMARY KEY constraints:
        // SQLite cannot add a PK constraint after creation (its generator
        // emits ALTER TABLE ADD CONSTRAINT, which SQLite rejects), while
        // unique indexes enforce the same uniqueness on both engines. All
        // key columns stay NotNullable, so the semantics match.
        let uniqueKey (ixName: string) (tableName: string) (columns: string list) : unit =
            match columns with
            | [ first; second ] ->
                (startIndex ixName tableName)
                    .OnColumn(first)
                    .Ascending()
                    .OnColumn(second)
                    .Ascending()
                    .WithOptions()
                    .Unique()
                |> ignore
            | [ first; second; third ] ->
                (startIndex ixName tableName)
                    .OnColumn(first)
                    .Ascending()
                    .OnColumn(second)
                    .Ascending()
                    .OnColumn(third)
                    .Ascending()
                    .WithOptions()
                    .Unique()
                |> ignore
            | _ -> invalidArg (nameof columns) "Composite keys in the baseline hold two or three columns."

        if schema <> "" then
            create.Schema(schema) |> ignore

        // ── sessions ──
        let sessions = table "sessions"

        (startTable sessions)
            .WithColumn("id")
            .AsString()
            .NotNullable()
            .PrimaryKey()
            .WithColumn("tenant")
            .AsString()
            .NotNullable()
            .WithColumn("agent_id")
            .AsString()
            .NotNullable()
            .WithColumn("title")
            .AsString(Int32.MaxValue)
            .NotNullable()
            .WithColumn("state")
            .AsString()
            .NotNullable()
            .WithColumn("current_turn_id")
            .AsString()
            .Nullable()
            .WithColumn("created_at")
            .AsString()
            .NotNullable()
            .WithColumn("updated_at")
            .AsString()
            .NotNullable()
            .WithColumn("closed_at")
            .AsString()
            .Nullable()
            .WithColumn("workspace_binding")
            .AsString()
            .Nullable()
            .WithColumn("options_json")
            .AsString(Int32.MaxValue)
            .NotNullable()
            .WithColumn("permission_grants_json")
            .AsString(Int32.MaxValue)
            .NotNullable()
            .WithDefaultValue("[]")
        |> ignore

        (startIndex (index "sessions" "tenant_updated") sessions)
            .OnColumn("tenant")
            .Ascending()
            .OnColumn("updated_at")
            .Ascending()
        |> ignore

        (startIndex (index "sessions" "tenant_agent") sessions)
            .OnColumn("tenant")
            .Ascending()
            .OnColumn("agent_id")
            .Ascending()
        |> ignore

        // ── inbox ──
        let inbox = table "inbox"

        (startTable inbox)
            .WithColumn("session_id")
            .AsString()
            .NotNullable()
            .WithColumn("position")
            .AsInt64()
            .NotNullable()
            .WithColumn("tenant")
            .AsString()
            .NotNullable()
            .WithColumn("payload_json")
            .AsString(Int32.MaxValue)
            .NotNullable()
            .WithColumn("delivery_mode")
            .AsString()
            .NotNullable()
            .WithColumn("consumed")
            .AsBoolean()
            .NotNullable()
            .WithDefaultValue(false)
            .WithColumn("appended_at")
            .AsString()
            .NotNullable()
        |> ignore

        uniqueKey (index "inbox" "uq_session_position") inbox [ "session_id"; "position" ]

        (startIndex (index "inbox" "session_pending") inbox)
            .OnColumn("session_id")
            .Ascending()
            .OnColumn("consumed")
            .Ascending()
            .OnColumn("position")
            .Ascending()
        |> ignore

        (startIndex (index "inbox" "tenant_pending") inbox)
            .OnColumn("tenant")
            .Ascending()
            .OnColumn("consumed")
            .Ascending()
        |> ignore

        // ── turns (with the claim lease on the row) ──
        let turns = table "turns"

        (startTable turns)
            .WithColumn("turn_id")
            .AsString()
            .NotNullable()
            .PrimaryKey()
            .WithColumn("session_id")
            .AsString()
            .NotNullable()
            .WithColumn("tenant")
            .AsString()
            .NotNullable()
            .WithColumn("attempt")
            .AsInt32()
            .NotNullable()
            .WithColumn("status")
            .AsString()
            .NotNullable()
            .WithColumn("claim_token")
            .AsString()
            .Nullable()
            .WithColumn("claim_owner")
            .AsString()
            .Nullable()
            .WithColumn("claim_expires_at")
            .AsString()
            .Nullable()
            .WithColumn("started_at")
            .AsString()
            .NotNullable()
            .WithColumn("completed_at")
            .AsString()
            .Nullable()
            .WithColumn("iterations")
            .AsInt32()
            .NotNullable()
            .WithDefaultValue(0)
            .WithColumn("input_tokens")
            .AsInt64()
            .NotNullable()
            .WithDefaultValue(0L)
            .WithColumn("output_tokens")
            .AsInt64()
            .NotNullable()
            .WithDefaultValue(0L)
            .WithColumn("error")
            .AsString(Int32.MaxValue)
            .Nullable()
            .WithColumn("stop_cause")
            .AsString()
            .Nullable()
            .WithColumn("outcome_json")
            .AsString(Int32.MaxValue)
            .Nullable()
        |> ignore

        (startIndex (index "turns" "session") turns).OnColumn("session_id").Ascending()
        |> ignore

        (startIndex (index "turns" "tenant_status") turns).OnColumn("tenant").Ascending().OnColumn("status").Ascending()
        |> ignore

        // ── events (PK order serves the replay cursors) ──
        let events = table "events"

        (startTable events)
            .WithColumn("session_id")
            .AsString()
            .NotNullable()
            .WithColumn("sequence")
            .AsInt64()
            .NotNullable()
            .WithColumn("tenant")
            .AsString()
            .NotNullable()
            .WithColumn("turn_id")
            .AsString()
            .NotNullable()
            .WithColumn("event_type")
            .AsString()
            .NotNullable()
            .WithColumn("payload_json")
            .AsString(Int32.MaxValue)
            .NotNullable()
            .WithColumn("timestamp")
            .AsString()
            .NotNullable()
        |> ignore

        uniqueKey (index "events" "uq_session_sequence") events [ "session_id"; "sequence" ]

        // ── cleanup_claims ──
        let cleanupClaims = table "cleanup_claims"

        (startTable cleanupClaims)
            .WithColumn("session_id")
            .AsString()
            .NotNullable()
            .PrimaryKey()
            .WithColumn("tenant")
            .AsString()
            .NotNullable()
            .WithColumn("token")
            .AsString()
            .NotNullable()
            .WithColumn("owner")
            .AsString()
            .NotNullable()
            .WithColumn("expires_at")
            .AsString()
            .NotNullable()
        |> ignore

        // ── agents ──
        let agents = table "agents"

        (startTable agents)
            .WithColumn("tenant")
            .AsString()
            .NotNullable()
            .WithColumn("agent_id")
            .AsString()
            .NotNullable()
            .WithColumn("name")
            .AsString()
            .NotNullable()
            .WithColumn("row_version")
            .AsInt64()
            .NotNullable()
            .WithColumn("definition_json")
            .AsString(Int32.MaxValue)
            .NotNullable()
            .WithColumn("schedule_enabled")
            .AsBoolean()
            .NotNullable()
            .WithDefaultValue(false)
            .WithColumn("schedule_cron")
            .AsString()
            .Nullable()
            .WithColumn("schedule_timezone")
            .AsString()
            .Nullable()
            .WithColumn("created_at")
            .AsString()
            .NotNullable()
            .WithColumn("updated_at")
            .AsString()
            .NotNullable()
        |> ignore

        uniqueKey (index "agents" "uq_tenant_agent") agents [ "tenant"; "agent_id" ]

        (startIndex (index "agents" "tenant_schedule") agents)
            .OnColumn("tenant")
            .Ascending()
            .OnColumn("schedule_enabled")
            .Ascending()
        |> ignore

        // ── custom_tools ──
        let customTools = table "custom_tools"

        (startTable customTools)
            .WithColumn("tenant")
            .AsString()
            .NotNullable()
            .WithColumn("agent_id")
            .AsString()
            .NotNullable()
            .WithColumn("name")
            .AsString()
            .NotNullable()
            .WithColumn("row_version")
            .AsInt64()
            .NotNullable()
            .WithColumn("enabled")
            .AsBoolean()
            .NotNullable()
            .WithDefaultValue(true)
            .WithColumn("definition_json")
            .AsString(Int32.MaxValue)
            .NotNullable()
            .WithColumn("created_at")
            .AsString()
            .NotNullable()
            .WithColumn("updated_at")
            .AsString()
            .NotNullable()
        |> ignore

        uniqueKey (index "custom_tools" "uq_tool") customTools [ "tenant"; "agent_id"; "name" ]

        (startIndex (index "custom_tools" "agent_enabled") customTools)
            .OnColumn("tenant")
            .Ascending()
            .OnColumn("agent_id")
            .Ascending()
            .OnColumn("enabled")
            .Ascending()
        |> ignore

        // ── schedule_occurrences (PROVISIONAL: no consumer until the scheduling issues) ──
        let occurrences = table "schedule_occurrences"

        (startTable occurrences)
            .WithColumn("tenant")
            .AsString()
            .NotNullable()
            .WithColumn("agent_id")
            .AsString()
            .NotNullable()
            .WithColumn("occurrence_utc")
            .AsString()
            .NotNullable()
            .WithColumn("consumed")
            .AsBoolean()
            .NotNullable()
            .WithDefaultValue(false)
            .WithColumn("consumed_at")
            .AsString()
            .Nullable()
            .WithColumn("created_at")
            .AsString()
            .NotNullable()
        |> ignore

        uniqueKey
            (index "schedule_occurrences" "uq_occurrence")
            occurrences
            [
                "tenant"
                "agent_id"
                "occurrence_utc"
            ]

        // ── outbox (PROVISIONAL: no consumer until the completion-sink issues) ──
        let outbox = table "outbox"

        (startTable outbox)
            .WithColumn("idempotency_key")
            .AsString()
            .NotNullable()
            .PrimaryKey()
            .WithColumn("tenant")
            .AsString()
            .NotNullable()
            .WithColumn("session_id")
            .AsString()
            .NotNullable()
            .WithColumn("completion_json")
            .AsString(Int32.MaxValue)
            .NotNullable()
            .WithColumn("created_at")
            .AsString()
            .NotNullable()
            .WithColumn("delivered")
            .AsBoolean()
            .NotNullable()
            .WithDefaultValue(false)
            .WithColumn("delivered_at")
            .AsString()
            .Nullable()
            .WithColumn("lease_owner")
            .AsString()
            .Nullable()
            .WithColumn("lease_expires_at")
            .AsString()
            .Nullable()
        |> ignore

        (startIndex (index "outbox" "pending_created") outbox)
            .OnColumn("delivered")
            .Ascending()
            .OnColumn("created_at")
            .Ascending()
        |> ignore

        (startIndex (index "outbox" "delivered_at") outbox)
            .OnColumn("delivered")
            .Ascending()
            .OnColumn("delivered_at")
            .Ascending()
        |> ignore

        // ── session_grants (PROVISIONAL: execution-authority shape owned by the grants issue) ──
        let grants = table "session_grants"

        (startTable grants)
            .WithColumn("tenant")
            .AsString()
            .NotNullable()
            .WithColumn("session_id")
            .AsString()
            .NotNullable()
            .WithColumn("tool_name")
            .AsString()
            .NotNullable()
            .WithColumn("granted_at")
            .AsString()
            .NotNullable()
        |> ignore

        uniqueKey (index "session_grants" "uq_grant") grants [ "tenant"; "session_id"; "tool_name" ]

    /// <summary>
    /// Rolls the baseline back: drops the ten tables in reverse order,
    /// then the schema (when the options name one).
    /// </summary>
    override this.Down() : unit =
        let delete = base.Delete
        let schema = resolved.Schema
        let table = MigrationNaming.tableName resolved

        for logical in
            [
                "session_grants"
                "outbox"
                "schedule_occurrences"
                "custom_tools"
                "agents"
                "cleanup_claims"
                "events"
                "turns"
                "inbox"
                "sessions"
            ] do
            let target = delete.Table(table logical)

            if schema = "" then
                target |> ignore
            else
                target.InSchema(schema) |> ignore

        if schema <> "" then
            delete.Schema(schema) |> ignore
