// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.Sqlite

open System
open System.Collections.Generic
open System.IO
open System.Threading
open System.Threading.Tasks
open Legate
open Microsoft.Data.Sqlite

// The SQLite IAgentPackageStore: versioned package contents per agent over
// one shared database. Uploads publish and activate atomically (a failed or
// duplicate-path upload leaves the previous state intact, and re-uploads
// preserve CreatedAt); replaces never change active status and throw on
// unknown versions; deleting the active version clears it to null with no
// auto-promotion; GetPackageInfo derives from the active version's stored
// layout. Paths normalise and validate through AgentPackagePaths and
// versions through PackageVersions. Every SqliteException funnels through
// the SqliteErrors boundary.

/// <summary>
/// The SQLite <see cref="T:Legate.IAgentPackageStore" /> over one shared
/// database file.
/// </summary>
/// <param name="database">The shared database every store uses. Must not be null.</param>
type SqliteAgentPackageStore(database: SqliteDatabase) =

    do
        if isNull (box database) then
            raise (ArgumentNullException(nameof database))

    let path = database.Path
    let mapSql (ex: SqliteException) : LegateException = SqliteErrors.ofSqliteException path ex
    let toIso = SqliteDatabase.ToIso
    let ofIso = SqliteDatabase.OfIso

    let packagesTable () = database.Table "packages"
    let versionsTable () = database.Table "package_versions"

    let staticSkillsPrefix = ".agent/skills/"
    let staticAgentsPrefix = ".agent/agents/"

    /// Materialises the entries into path-keyed bytes, validating paths and
    /// rejecting duplicate normalised paths before anything is stored.
    let materialise (entries: IAsyncEnumerable<AgentPackageEntry>) (cancellationToken: CancellationToken) =
        task {
            let files = Dictionary<string, byte[]>()
            use memory = new MemoryStream()
            let enumerator = entries.GetAsyncEnumerator cancellationToken
            let mutable more = true
            let mutable failure: exn | null = null

            try
                while more do
                    let! moved = enumerator.MoveNextAsync()

                    if moved then
                        let entry = enumerator.Current
                        let normalised = AgentPackagePaths.Normalise entry.Path

                        if files.ContainsKey normalised then
                            raise (
                                ArgumentException(
                                    sprintf "Two package entries normalise to the same path %s." normalised,
                                    nameof entries
                                )
                            )

                        do! entry.Content.CopyToAsync memory
                        files[normalised] <- memory.ToArray()
                        memory.SetLength 0L
                    else
                        more <- false
            with ex ->
                failure <- ex

            do! enumerator.DisposeAsync()

            match failure with
            | null -> return files
            | ex -> return raise ex
        }

    let namesUnder (files: Dictionary<string, byte[]>) (prefix: string) (suffix: string) : List<string> =
        files.Keys
        |> Seq.filter (fun filePath -> filePath.StartsWith(prefix, StringComparison.Ordinal))
        |> Seq.filter (fun filePath -> filePath.EndsWith(suffix, StringComparison.Ordinal))
        |> Seq.map (fun filePath -> filePath.Substring(prefix.Length, filePath.Length - prefix.Length - suffix.Length))
        |> Seq.filter (fun name -> name.Length > 0 && not (name.Contains '/'))
        |> Seq.sortWith (fun left right -> String.CompareOrdinal(left, right))
        |> fun names -> List<string>(names)

    let infoOf (files: Dictionary<string, byte[]>) (active: string) (source: string | null) : AgentPackageInfo =
        let skills = namesUnder files staticSkillsPrefix "/SKILL.md"
        let subAgents = namesUnder files staticAgentsPrefix "/AGENT.md"

        let instructions: string | null =
            match files.TryGetValue "AGENTS.md" with
            | true, bytes -> Text.Encoding.UTF8.GetString bytes
            | false, _ ->
                (match files.TryGetValue "agents.md" with
                 | true, bytes -> Text.Encoding.UTF8.GetString bytes
                 | false, _ -> null)

        AgentPackageInfo(
            instructions,
            (skills :> IReadOnlyList<string>),
            (subAgents :> IReadOnlyList<string>),
            source,
            active
        )

    let readState
        (connection: SqliteConnection)
        (transaction: SqliteTransaction | null)
        (tenant: TenantId)
        (agentId: AgentId)
        : (string option * string | null) option =
        use command = connection.CreateCommand()

        if not (isNull (box transaction)) then
            command.Transaction <- transaction

        command.CommandText <-
            $"SELECT active, source FROM \"%s{packagesTable ()}\" WHERE tenant = $tenant AND agent_id = $agent"

        command.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore
        command.Parameters.AddWithValue("$agent", agentId.Value) |> ignore

        use reader = command.ExecuteReader()

        if reader.Read() then
            let active =
                if reader.IsDBNull(0) then
                    None
                else
                    Some(reader.GetString(0))

            let source = if reader.IsDBNull(1) then null else reader.GetString(1)

            Some(active, source)
        else
            None

    let readVersion
        (connection: SqliteConnection)
        (transaction: SqliteTransaction | null)
        (tenant: TenantId)
        (agentId: AgentId)
        (version: string)
        : (Dictionary<string, byte[]> * DateTimeOffset) option =
        use command = connection.CreateCommand()

        if not (isNull (box transaction)) then
            command.Transaction <- transaction

        command.CommandText <-
            $"SELECT files_json, created_at FROM \"%s{versionsTable ()}\" WHERE tenant = $tenant AND agent_id = $agent AND version = $version"

        command.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore
        command.Parameters.AddWithValue("$agent", agentId.Value) |> ignore
        command.Parameters.AddWithValue("$version", version) |> ignore

        use reader = command.ExecuteReader()

        if reader.Read() then
            let files = SqliteJson.deserialize<Dictionary<string, byte[]>> (reader.GetString(0))
            Some(files, ofIso (reader.GetString(1)))
        else
            None

    interface IAgentPackageStore with

        member _.GetPackageInfo(tenant, agentId, _) =
            task {
                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()

                            match readState connection null tenant agentId with
                            | None -> Unchecked.defaultof<AgentPackageInfo>
                            | Some(None, _) -> Unchecked.defaultof<AgentPackageInfo>
                            | Some(Some active, source) ->
                                match readVersion connection null tenant agentId active with
                                | None -> Unchecked.defaultof<AgentPackageInfo>
                                | Some(files, _) -> infoOf files active source)
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }

        member _.ReadFile(tenant, agentId, version, filePath, _) =
            task {
                PackageVersions.Validate version |> ignore
                let canonicalPath = AgentPackagePaths.Normalise filePath

                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()

                            match readVersion connection null tenant agentId version with
                            | None -> Unchecked.defaultof<Stream>
                            | Some(files, _) ->
                                match files.TryGetValue canonicalPath with
                                | true, bytes -> new MemoryStream(bytes, false) :> Stream
                                | false, _ -> Unchecked.defaultof<Stream>)
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }

        member _.ListFiles(tenant, agentId, version, prefix, _) =
            task {
                PackageVersions.Validate version |> ignore

                if isNull (box prefix) then
                    raise (ArgumentNullException(nameof prefix))

                let canonicalPrefix = AgentPackagePaths.Normalise prefix

                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()

                            match readVersion connection null tenant agentId version with
                            | None -> [] :> IReadOnlyList<string>
                            | Some(files, _) ->
                                files.Keys
                                |> Seq.filter (fun filePath ->
                                    filePath = canonicalPrefix
                                    || filePath.StartsWith(canonicalPrefix + "/", StringComparison.Ordinal))
                                |> Seq.sortWith (fun left right -> String.CompareOrdinal(left, right))
                                |> Seq.toList
                                :> IReadOnlyList<string>)
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }

        member _.UploadPackage(tenant, agentId, version, source, entries, cancellationToken) =
            task {
                if isNull (box source) then
                    raise (ArgumentNullException(nameof source))

                PackageVersions.Validate version |> ignore

                // Materialise and validate outside the gate: the entries are
                // caller-supplied streams, and a failure must leave every
                // table untouched.
                let! files = materialise entries cancellationToken

                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()
                            use transaction = connection.BeginTransaction()
                            let filesJson = SqliteJson.serialize files

                            let createdAt =
                                match readVersion connection transaction tenant agentId version with
                                | Some(_, created) ->
                                    use update = connection.CreateCommand()
                                    update.Transaction <- transaction

                                    update.CommandText <-
                                        $"UPDATE \"%s{versionsTable ()}\" SET files_json = $files WHERE tenant = $tenant AND agent_id = $agent AND version = $version"

                                    update.Parameters.AddWithValue("$files", filesJson) |> ignore
                                    update.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore
                                    update.Parameters.AddWithValue("$agent", agentId.Value) |> ignore
                                    update.Parameters.AddWithValue("$version", version) |> ignore
                                    update.ExecuteNonQuery() |> ignore
                                    created
                                | None ->
                                    let created = database.UtcNow

                                    use insert = connection.CreateCommand()
                                    insert.Transaction <- transaction

                                    insert.CommandText <-
                                        $"INSERT INTO \"%s{versionsTable ()}\" (tenant, agent_id, version, files_json, created_at) VALUES ($tenant, $agent, $version, $files, $created)"

                                    insert.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore
                                    insert.Parameters.AddWithValue("$agent", agentId.Value) |> ignore
                                    insert.Parameters.AddWithValue("$version", version) |> ignore
                                    insert.Parameters.AddWithValue("$files", filesJson) |> ignore
                                    insert.Parameters.AddWithValue("$created", toIso created) |> ignore
                                    insert.ExecuteNonQuery() |> ignore
                                    created

                            // Publish activates: deploy semantics.
                            use state = connection.CreateCommand()
                            state.Transaction <- transaction

                            state.CommandText <-
                                $"INSERT INTO \"%s{packagesTable ()}\" (tenant, agent_id, active, source) VALUES ($tenant, $agent, $active, $source) ON CONFLICT (tenant, agent_id) DO UPDATE SET active = $active, source = $source"

                            state.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore
                            state.Parameters.AddWithValue("$agent", agentId.Value) |> ignore
                            state.Parameters.AddWithValue("$active", version) |> ignore
                            state.Parameters.AddWithValue("$source", source) |> ignore
                            state.ExecuteNonQuery() |> ignore
                            transaction.Commit()
                            AgentPackageVersion(version, createdAt))
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }

        member _.ReplacePackageVersion(tenant, agentId, version, source, entries, cancellationToken) =
            task {
                if isNull (box source) then
                    raise (ArgumentNullException(nameof source))

                PackageVersions.Validate version |> ignore
                let! files = materialise entries cancellationToken

                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()
                            use transaction = connection.BeginTransaction()

                            match readVersion connection transaction tenant agentId version with
                            | None -> raise (ArgumentException("The version to replace is unknown.", nameof version))
                            | Some(_, created) ->
                                let filesJson = SqliteJson.serialize files

                                use update = connection.CreateCommand()
                                update.Transaction <- transaction

                                update.CommandText <-
                                    $"UPDATE \"%s{versionsTable ()}\" SET files_json = $files WHERE tenant = $tenant AND agent_id = $agent AND version = $version"

                                update.Parameters.AddWithValue("$files", filesJson) |> ignore
                                update.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore
                                update.Parameters.AddWithValue("$agent", agentId.Value) |> ignore
                                update.Parameters.AddWithValue("$version", version) |> ignore
                                update.ExecuteNonQuery() |> ignore

                                // The source label of the replacing write is recorded;
                                // active status never changes here.
                                use state = connection.CreateCommand()
                                state.Transaction <- transaction

                                state.CommandText <-
                                    $"UPDATE \"%s{packagesTable ()}\" SET source = $source WHERE tenant = $tenant AND agent_id = $agent"

                                state.Parameters.AddWithValue("$source", source) |> ignore
                                state.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore
                                state.Parameters.AddWithValue("$agent", agentId.Value) |> ignore
                                state.ExecuteNonQuery() |> ignore
                                transaction.Commit()
                                AgentPackageVersion(version, created))
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }

        member _.DeletePackageVersion(tenant, agentId, version, _) =
            task {
                PackageVersions.Validate version |> ignore

                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()
                            use transaction = connection.BeginTransaction()

                            use delete = connection.CreateCommand()
                            delete.Transaction <- transaction

                            delete.CommandText <-
                                $"DELETE FROM \"%s{versionsTable ()}\" WHERE tenant = $tenant AND agent_id = $agent AND version = $version"

                            delete.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore
                            delete.Parameters.AddWithValue("$agent", agentId.Value) |> ignore
                            delete.Parameters.AddWithValue("$version", version) |> ignore

                            let removed = delete.ExecuteNonQuery() > 0

                            // Deleting the active version clears it: nothing else
                            // auto-promotes.
                            use clear = connection.CreateCommand()
                            clear.Transaction <- transaction

                            clear.CommandText <-
                                $"UPDATE \"%s{packagesTable ()}\" SET active = NULL WHERE tenant = $tenant AND agent_id = $agent AND active = $version"

                            clear.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore
                            clear.Parameters.AddWithValue("$agent", agentId.Value) |> ignore
                            clear.Parameters.AddWithValue("$version", version) |> ignore
                            clear.ExecuteNonQuery() |> ignore
                            transaction.Commit()
                            removed)
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }

        member _.ListVersions(tenant, agentId, _) =
            task {
                try
                    return
                        lock database.Gate (fun () ->
                            use connection = database.OpenConnection()

                            use command = connection.CreateCommand()

                            command.CommandText <-
                                $"SELECT version, created_at FROM \"%s{versionsTable ()}\" WHERE tenant = $tenant AND agent_id = $agent ORDER BY version"

                            command.Parameters.AddWithValue("$tenant", tenant.Value) |> ignore
                            command.Parameters.AddWithValue("$agent", agentId.Value) |> ignore

                            use reader = command.ExecuteReader()
                            let versions = List<AgentPackageVersion>()

                            while reader.Read() do
                                versions.Add(AgentPackageVersion(reader.GetString(0), ofIso (reader.GetString(1))))

                            versions :> IReadOnlyList<AgentPackageVersion>)
                with
                | :? LegateException as ex -> return raise ex
                | :? SqliteException as sql -> return raise (mapSql sql)
            }
