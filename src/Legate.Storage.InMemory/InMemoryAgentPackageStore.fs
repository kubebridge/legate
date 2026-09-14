// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.InMemory

open System
open System.Collections.Generic
open System.IO
open System.Threading
open System.Threading.Tasks
open Legate

/// The in-memory <see cref="T:Legate.IAgentPackageStore" />: versioned
/// package contents per agent over one shared database. Uploads publish
/// and activate atomically (a failed or duplicate-path upload leaves the
/// previous state intact, and re-uploads preserve CreatedAt); replaces
/// never change active status and throw on unknown versions; deleting the
/// active version clears it to null with no auto-promotion;
/// <see cref="M:Legate.IAgentPackageStore.GetPackageInfo*" /> derives from
/// the active version's stored layout. Paths normalise and validate
/// through <see cref="T:Legate.AgentPackagePaths" /> and versions through
/// <see cref="T:Legate.PackageVersions" />.
type InMemoryAgentPackageStore(database: InMemoryDatabase) =

    do
        if isNull (box database) then
            raise (ArgumentNullException(nameof database))

    let ok value = Task.FromResult value

    let packageRow (tenant: TenantId) (agentId: AgentId) =
        match database.Packages.TryGetValue((tenant, agentId)) with
        | true, row -> Some row
        | false, _ -> None

    let ensureRow (tenant: TenantId) (agentId: AgentId) =
        match packageRow tenant agentId with
        | Some row -> row
        | None ->
            let row = PackageRow(Dictionary<string, StoredVersion>(), null)
            database.Packages[(tenant, agentId)] <- row
            row

    /// Materialises the entries into path-keyed bytes, validating paths and
    /// rejecting duplicate normalised paths before anything is stored.
    let materialise (entries: IAsyncEnumerable<AgentPackageEntry>) (cancellationToken: CancellationToken) =
        task {
            let files = Dictionary<string, byte[]>()
            use memory = new MemoryStream()
            let! enumerator = Task.FromResult(entries.GetAsyncEnumerator cancellationToken)

            let mutable more = true

            let mutable failure: exn | null = null

            try
                while more do
                    let! moved = enumerator.MoveNextAsync()

                    if moved then
                        let entry = enumerator.Current
                        let path = AgentPackagePaths.Normalise entry.Path

                        if files.ContainsKey path then
                            raise (
                                ArgumentException(
                                    sprintf "Two package entries normalise to the same path %s." path,
                                    nameof entries
                                )
                            )

                        do! entry.Content.CopyToAsync memory
                        files[path] <- memory.ToArray()
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

    /// The layout-derivation constants of the normative package layout.
    let staticSkillsPrefix = ".agent/skills/"

    let staticAgentsPrefix = ".agent/agents/"

    /// Derives the package info from the active version's stored layout.
    let infoOf (row: PackageRow) : AgentPackageInfo | null =
        let activeVersion =
            match row.Active with
            | null -> None
            | active ->
                match row.Versions.TryGetValue active with
                | true, stored -> Some stored
                | false, _ -> None

        match activeVersion with
        | None -> null
        | Some version ->
            let namesUnder prefix suffix =
                version.Files.Keys
                |> Seq.filter (fun path -> path.StartsWith(prefix, StringComparison.Ordinal))
                |> Seq.filter (fun path -> path.EndsWith(suffix, StringComparison.Ordinal))
                |> Seq.map (fun path -> path.Substring(prefix.Length, path.Length - prefix.Length - suffix.Length))
                |> Seq.filter (fun name -> name.Length > 0 && not (name.Contains '/'))
                |> Seq.sortWith (fun left right -> String.CompareOrdinal(left, right))
                |> Seq.toList

            let skills = namesUnder staticSkillsPrefix "/SKILL.md"
            let subAgents = namesUnder staticAgentsPrefix "/AGENT.md"

            let instructions: string | null =
                match version.Files.TryGetValue("AGENTS.md") with
                | true, bytes -> Text.Encoding.UTF8.GetString bytes
                | false, _ ->
                    (match version.Files.TryGetValue("agents.md") with
                     | true, bytes -> Text.Encoding.UTF8.GetString bytes
                     | false, _ -> null)

            AgentPackageInfo(
                instructions,
                (skills :> IReadOnlyList<string>),
                (subAgents :> IReadOnlyList<string>),
                row.Source,
                row.Active
            )

    interface IAgentPackageStore with

        member _.GetPackageInfo(tenant, agentId, _) =
            lock database.Gate (fun () ->
                (match packageRow tenant agentId with
                 | None -> Unchecked.defaultof<AgentPackageInfo>
                 | Some row -> infoOf row)
                : AgentPackageInfo | null)
            |> ok

        member _.ReadFile(tenant, agentId, version, path, _) =
            PackageVersions.Validate version |> ignore
            let canonicalPath = AgentPackagePaths.Normalise path

            lock database.Gate (fun () ->
                (match packageRow tenant agentId with
                 | None -> Unchecked.defaultof<Stream>
                 | Some row ->
                     match row.Versions.TryGetValue version with
                     | false, _ -> Unchecked.defaultof<Stream>
                     | true, stored ->
                         (match stored.Files.TryGetValue canonicalPath with
                          | true, bytes -> new MemoryStream(bytes, false) :> Stream
                          | false, _ -> Unchecked.defaultof<Stream>)))
            |> ok

        member _.UploadPackage(tenant, agentId, version, source, entries, cancellationToken) =
            if isNull (box source) then
                raise (ArgumentNullException(nameof source))

            PackageVersions.Validate version |> ignore

            // Materialise and validate outside the gate: the entries are
            // caller-supplied streams, and a failure must leave every
            // table untouched.
            let files = (materialise entries cancellationToken).GetAwaiter().GetResult()

            lock database.Gate (fun () ->
                let row = ensureRow tenant agentId

                let stored =
                    match row.Versions.TryGetValue version with
                    | true, existing ->
                        existing.Files.Clear()

                        for pair in files do
                            existing.Files[pair.Key] <- pair.Value

                        existing
                    | false, _ ->
                        let fresh = StoredVersion(Dictionary<string, byte[]>(), database.UtcNow)

                        for pair in files do
                            fresh.Files[pair.Key] <- pair.Value

                        row.Versions[version] <- fresh
                        fresh

                // Publish activates: deploy semantics.
                row.Active <- version
                row.Source <- source

                AgentPackageVersion(version, stored.CreatedAt))
            |> ok

        member _.ReplacePackageVersion(tenant, agentId, version, source, entries, cancellationToken) =
            if isNull (box source) then
                raise (ArgumentNullException(nameof source))

            PackageVersions.Validate version |> ignore

            let files = (materialise entries cancellationToken).GetAwaiter().GetResult()

            lock database.Gate (fun () ->
                let row =
                    match packageRow tenant agentId with
                    | None -> raise (ArgumentException("The version to replace is unknown.", nameof version))
                    | Some row -> row

                match row.Versions.TryGetValue version with
                | false, _ -> raise (ArgumentException("The version to replace is unknown.", nameof version))
                | true, stored ->
                    stored.Files.Clear()

                    for pair in files do
                        stored.Files[pair.Key] <- pair.Value

                    // The source label of the replacing write is recorded;
                    // active status never changes here.
                    row.Source <- source

                    AgentPackageVersion(version, stored.CreatedAt))
            |> ok

        member _.DeletePackageVersion(tenant, agentId, version, _) =
            PackageVersions.Validate version |> ignore

            lock database.Gate (fun () ->
                match packageRow tenant agentId with
                | None -> false
                | Some row ->
                    let removed = row.Versions.Remove version

                    // Deleting the active version clears it: nothing else
                    // auto-promotes.
                    if String.Equals(row.Active, version, StringComparison.Ordinal) then
                        row.Active <- Unchecked.defaultof<string>

                    removed)
            |> ok

        member _.ListVersions(tenant, agentId, _) =
            lock database.Gate (fun () ->
                match packageRow tenant agentId with
                | None -> [] :> IReadOnlyList<AgentPackageVersion>
                | Some row ->
                    row.Versions
                    |> Seq.map (fun pair -> AgentPackageVersion(pair.Key, pair.Value.CreatedAt))
                    |> Seq.sortBy (fun packageVersion -> packageVersion.Version)
                    |> Seq.toList
                    :> IReadOnlyList<AgentPackageVersion>)
            |> ok

/// The store factory over one database: the composition root the runtime's
/// registration consumes, so one database instance backs every store.
module InMemoryStoreFactory =

    /// Constructs the session store over the database.
    let sessionStore (database: InMemoryDatabase) =
        InMemorySessionStore(database) :> ISessionStore

    /// Constructs the event store over the database.
    let eventStore (database: InMemoryDatabase) =
        InMemorySessionEventStore(database) :> ISessionEventStore

    /// Constructs the agent store (both interfaces) over the database.
    let agentStore (database: InMemoryDatabase) =
        InMemoryAgentStore(database) :> IAgentStore

    /// Constructs the custom-tool store over the database.
    let customToolStore (database: InMemoryDatabase) =
        InMemoryAgentStore(database) :> IAgentCustomToolStore

    /// Constructs the blob store over the database.
    let blobStore (database: InMemoryDatabase) =
        InMemoryBlobStore(database) :> IBlobStore

    /// Constructs the package store over the database.
    let packageStore (database: InMemoryDatabase) =
        InMemoryAgentPackageStore(database) :> IAgentPackageStore

    /// Every store over the database, as the composition root registers
    /// them: the session store, the event store, the agent store and the
    /// custom-tool store, the blob store, and the package store, all over
    /// one shared database.
    let allStores (database: InMemoryDatabase) =
        (sessionStore database,
         eventStore database,
         agentStore database,
         customToolStore database,
         blobStore database,
         packageStore database)
