// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.Postgres

open System
open Legate.Storage.Migrations

// Settings for the PostgreSQL stores, bound from the
// Legate:Storage:Postgres configuration section. A plain class with mutable
// properties and defaults, so hosts can set properties before registering
// and the configuration binder only overrides the keys the host sets. The
// journal limits mirror InMemoryStoreOptions: 0 means unbounded, a positive
// value bounds the dimension, and the event store enforces them in the
// store before any part of a batch lands.
type PostgresOptions() =

    /// The configuration section the stores bind from:
    /// <c>Legate:Storage:Postgres</c>.
    static member ConfigurationSectionPath = "Legate:Storage:Postgres"

    /// The Npgsql connection string of the database holding the Legate
    /// tables. The database must exist; the schema and tables are created
    /// by the shared migrations when <see cref="P:Legate.Storage.Postgres.PostgresOptions.RunMigrations" />
    /// is true. Must be a non-empty string.
    member val ConnectionString: string = "" with get, set

    /// The schema owning the Legate tables. Defaults to <c>legate</c>;
    /// empty means unqualified DDL in the engine's default schema.
    /// Otherwise letters, digits, and underscores only, starting with a
    /// letter or underscore.
    member val Schema: string = "legate" with get, set

    /// Prepended to every Legate table and index name. Defaults to the
    /// empty string (unprefixed names such as <c>sessions</c>). Empty is
    /// valid; otherwise letters, digits, and underscores only, starting
    /// with a letter or underscore.
    member val TablePrefix: string = "" with get, set

    /// Whether the stores apply the shared migrations on first use.
    /// Defaults to true: the first store call migrates an empty database
    /// to the baseline before acting. Hosts with their own runner set
    /// this to false and run the migrations themselves.
    member val RunMigrations: bool = true with get, set

    /// The maximum size of one journaled event, in bytes, estimated as the
    /// UTF-8 encoded length of the event's serialised JSON form. 0 means
    /// unbounded.
    member val MaxEventBytes: int64 = 0L with get, set

    /// The maximum number of events one session's journal may hold. 0 means
    /// unbounded.
    member val MaxEventsPerSession: int64 = 0L with get, set

    /// The maximum total bytes one session's journal may hold. 0 means
    /// unbounded.
    member val MaxJournalBytesPerSession: int64 = 0L with get, set

    /// The maximum number of events one append may carry. 0 means unbounded.
    member val MaxAppendBatchSize: int = 0 with get, set

    /// Checks the options: the connection string must be non-empty, the
    /// schema and prefix follow the migration identifier rule, and the
    /// limits must not be negative (0 means unbounded).
    /// <returns>Null when the options are valid; otherwise the reason they are not.</returns>
    member this.Validate() : string | null =
        if String.IsNullOrWhiteSpace(this.ConnectionString) then
            "PostgresOptions.ConnectionString must be a non-empty Npgsql connection string."
        elif isNull (box this.Schema) then
            "PostgresOptions.Schema must not be null: use \"legate\" (the default) or the empty string for unqualified DDL."
        elif this.Schema <> "" && not (MigrationOptions.IsIdentifier this.Schema) then
            "PostgresOptions.Schema must be empty or a portable identifier: letters, digits, and underscores, starting with a letter or underscore."
        elif isNull (box this.TablePrefix) then
            "PostgresOptions.TablePrefix must not be null: use the empty string for unprefixed table names."
        elif this.TablePrefix <> "" && not (MigrationOptions.IsIdentifier this.TablePrefix) then
            "PostgresOptions.TablePrefix must be empty or a portable identifier: letters, digits, and underscores, starting with a letter or underscore."
        elif this.MaxEventBytes < 0L then
            "PostgresOptions.MaxEventBytes must not be negative: 0 means unbounded."
        elif this.MaxEventsPerSession < 0L then
            "PostgresOptions.MaxEventsPerSession must not be negative: 0 means unbounded."
        elif this.MaxJournalBytesPerSession < 0L then
            "PostgresOptions.MaxJournalBytesPerSession must not be negative: 0 means unbounded."
        elif this.MaxAppendBatchSize < 0 then
            "PostgresOptions.MaxAppendBatchSize must not be negative: 0 means unbounded."
        else
            null
