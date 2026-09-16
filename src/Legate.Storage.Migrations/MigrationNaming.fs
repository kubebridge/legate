// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.Migrations

// Internal naming helpers shared by the baseline migration (and the tests
// through InternalsVisibleTo): one place turns MigrationOptions into the
// schema-qualified, prefixed names the DDL uses, so the migration and the
// assertions cannot drift apart.

/// <summary>
/// Turns <see cref="T:Legate.Storage.Migrations.MigrationOptions" /> into
/// the physical names the migrations declare. Internal: hosts configure
/// the options, never these helpers.
/// </summary>
module internal MigrationNaming =

    /// <summary>
    /// The physical table name for a logical name: the configured prefix
    /// prepended verbatim (empty by default, so logical names stand).
    /// </summary>
    /// <param name="options">The resolved migration options. Must not be null.</param>
    /// <param name="logicalName">The unprefixed table name, for example "sessions".</param>
    /// <returns>The prefixed table name.</returns>
    let tableName (options: MigrationOptions) (logicalName: string) : string = options.TablePrefix + logicalName

    /// <summary>
    /// The physical index name for a table and column set: prefixed with
    /// the table so concurrent configurations never collide.
    /// </summary>
    /// <param name="options">The resolved migration options. Must not be null.</param>
    /// <param name="logicalTable">The unprefixed table name.</param>
    /// <param name="suffix">The index suffix, for example "tenant_updated".</param>
    /// <returns>The prefixed index name.</returns>
    let indexName (options: MigrationOptions) (logicalTable: string) (suffix: string) : string =
        $"IX_%s{tableName options logicalTable}_%s{suffix}"
