// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.FileSystem

open System
open System.Collections.Generic
open System.IO
open System.Text
open System.Threading
open System.Threading.Tasks
open Legate

// The file-system IAgentPackageStore: versioned package contents per agent
// over one rooted directory. Layout per agent:
// packages/{tenant}/{agent}/versions/{version}/ plus a stamp file
// versions/{version}.json carrying the creation instant, an active.txt
// pointer naming the active version, and a source.txt label with the last
// write's source. Uploads publish and activate atomically (entries
// materialise and validate before anything lands, then a staged tree is
// renamed into place under a per-store gate; re-uploads preserve
// CreatedAt); replaces never change active status and throw on unknown
// versions; deleting the active version clears it to null with no
// auto-promotion; GetPackageInfo derives from the active version's stored
// layout, mirroring the in-memory store. Single-node scope.

// ──────────────────────────────────────────────────────────────────────────
// Layout derivation shared with #107

/// <summary>
/// The normative package-layout derivation over stored entry paths:
/// instructions from AGENTS.md (agents.md fallback), skill names under
/// .agent/skills, sub-agent names under .agent/agents. Internal so the
/// versioned package-manifest work in #107 reuses the derivation.
/// </summary>
module internal FileSystemPackageLayout =

    /// <summary>
    /// The skills prefix of the normative package layout.
    /// </summary>
    let skillsPrefix = ".agent/skills/"

    /// <summary>
    /// The sub-agents prefix of the normative package layout.
    /// </summary>
    let agentsPrefix = ".agent/agents/"

    /// <summary>
    /// Derives the directory names directly under a prefix carrying the
    /// suffix file, sorted lexicographically: the skill and sub-agent
    /// discovery rule.
    /// </summary>
    /// <param name="paths">The stored canonical entry paths.</param>
    /// <param name="prefix">The layout prefix to scan.</param>
    /// <param name="suffix">The marker file ending the scan.</param>
    /// <returns>The discovered names, sorted lexicographically.</returns>
    let namesUnder (paths: string seq) (prefix: string) (suffix: string) : List<string> =
        paths
        |> Seq.filter (fun path -> path.StartsWith(prefix, StringComparison.Ordinal))
        |> Seq.filter (fun path -> path.EndsWith(suffix, StringComparison.Ordinal))
        |> Seq.map (fun path -> path.Substring(prefix.Length, path.Length - prefix.Length - suffix.Length))
        |> Seq.filter (fun name -> name.Length > 0 && not (name.Contains '/'))
        |> Seq.sortWith (fun left right -> String.CompareOrdinal(left, right))
        |> fun names -> List<string>(names)

    /// <summary>
    /// Reads the instructions text from stored files: AGENTS.md first, then
    /// the agents.md fallback, else null.
    /// </summary>
    /// <param name="read">Reads a canonical path to its text, or null when absent.</param>
    /// <returns>The instructions text, or null when the package has none.</returns>
    let instructionsOf (read: string -> string | null) : string | null =
        match read "AGENTS.md" with
        | null ->
            match read "agents.md" with
            | null -> null
            | fallback -> fallback
        | instructions -> instructions

/// <summary>
/// The file-system <see cref="T:Legate.IAgentPackageStore" /> over one
/// rooted directory: versioned entry trees plus stamp files, an
/// active-version pointer, and a source label per agent, with staged
/// atomic uploads and replaces under a per-store gate. Single-node scope.
/// </summary>
/// <param name="options">The storage options: the shared root. Must not be null.</param>
type FileSystemAgentPackageStore(options: FileSystemStorageOptions) =

    do
        ArgumentNullException.ThrowIfNull(options)

        match options.Validate() with
        | null -> ()
        | reason -> raise (ArgumentException(reason, nameof options))

    let rootFull = FileSystemPaths.ensureRoot options.RootDirectory
    let stagingRootFull = FileSystemPaths.stagingRoot rootFull
    let gate = obj ()

    /// Materialises the entries into path-keyed bytes, validating paths and
    /// rejecting duplicate normalised paths before anything is stored.
    /// <param name="entries">The entries to materialise.</param>
    /// <param name="cancellationToken">Token that abandons the read.</param>
    /// <returns>The files keyed by canonical path.</returns>
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

    /// Reads a version's creation stamp: the stamp file when present and
    /// well-formed, else the directory's creation time.
    /// <param name="tenant">The tenant that owns the package.</param>
    /// <param name="agentId">The agent the package belongs to.</param>
    /// <param name="version">The version to stamp.</param>
    /// <param name="versionDirFull">The version content directory.</param>
    /// <returns>The creation stamp.</returns>
    let createdAtOf (tenant: TenantId) (agentId: AgentId) (version: string) (versionDirFull: string) : DateTimeOffset =
        let metaPath =
            FileSystemPaths.packageVersionMetaPath rootFull tenant agentId version

        if File.Exists metaPath then
            match FileSystemManifests.tryReadVersionCreatedAt (File.ReadAllBytes metaPath) with
            | Some created -> created
            | None -> DateTimeOffset(Directory.GetCreationTimeUtc versionDirFull, TimeSpan.Zero)
        else
            DateTimeOffset(Directory.GetCreationTimeUtc versionDirFull, TimeSpan.Zero)

    /// Stages validated files into a fresh staging tree, mirroring their
    /// canonical relative paths.
    /// <param name="files">The files keyed by canonical path.</param>
    /// <returns>The full staging-directory path.</returns>
    let stageFiles (files: Dictionary<string, byte[]>) : string =
        let staged = FileSystemAtomic.freshStagingDir stagingRootFull

        for pair in files do
            match FileSystemPaths.tryResolveUnderRoot staged (List.ofArray (pair.Key.Split('/'))) with
            | None ->
                raise (
                    InvalidPackagePathException(
                        pair.Key,
                        "A package entry path must stay inside its version directory."
                    )
                )
            | Some _ ->
                let platformed =
                    Path.Combine(Array.ofList (staged :: List.ofArray (pair.Key.Split('/'))))

                match Path.GetDirectoryName platformed with
                | null -> ()
                | parent -> Directory.CreateDirectory parent |> ignore

                File.WriteAllBytes(platformed, pair.Value)

        staged

    /// Swaps a staged tree into its version directory: the previous tree
    /// moves aside first, so a failure restores it best-effort instead of
    /// leaving a partial version.
    /// <param name="stagedFull">The full staging-directory path.</param>
    /// <param name="versionDirFull">The full version-directory path.</param>
    let swapStaged (stagedFull: string) (versionDirFull: string) : unit =
        // Directory.Move creates nothing: the versions parent must exist first.
        match Path.GetDirectoryName versionDirFull with
        | null -> ()
        | parent -> Directory.CreateDirectory parent |> ignore

        if Directory.Exists versionDirFull then
            let backup = Path.Combine(stagingRootFull, "backup-" + Guid.NewGuid().ToString("N"))
            Directory.Move(versionDirFull, backup)

            try
                Directory.Move(stagedFull, versionDirFull)
                FileSystemAtomic.tryDeleteDirectory backup
            with _ ->
                try
                    if not (Directory.Exists versionDirFull) then
                        Directory.Move(backup, versionDirFull)
                with _ ->
                    ()

                reraise ()
        else
            Directory.Move(stagedFull, versionDirFull)

    /// Writes one version's stored state: the stamp plus the agent-level
    /// pointer and label. Replaces skip the pointer: they never change
    /// active status.
    /// <param name="tenant">The tenant that owns the package.</param>
    /// <param name="agentId">The agent the package belongs to.</param>
    /// <param name="version">The version just stored.</param>
    /// <param name="createdAt">The version's creation stamp.</param>
    /// <param name="source">The write's opaque source label.</param>
    /// <param name="activate">True to point active at the version; false to leave it.</param>
    let writeState
        (tenant: TenantId)
        (agentId: AgentId)
        (version: string)
        (createdAt: DateTimeOffset)
        (source: string)
        (activate: bool)
        : unit =
        let agentDir = FileSystemPaths.packageAgentDir rootFull tenant agentId
        Directory.CreateDirectory agentDir |> ignore

        FileSystemAtomic.writeFileAtomic
            stagingRootFull
            (FileSystemPaths.packageVersionMetaPath rootFull tenant agentId version)
            (FileSystemManifests.versionMetaJson createdAt)

        if activate then
            FileSystemAtomic.writeFileAtomic
                stagingRootFull
                (FileSystemPaths.packageActivePath agentDir)
                (Encoding.UTF8.GetBytes version)

        FileSystemAtomic.writeFileAtomic
            stagingRootFull
            (FileSystemPaths.packageSourcePath agentDir)
            (Encoding.UTF8.GetBytes source)

    interface IAgentPackageStore with

        member _.GetPackageInfo(tenant, agentId, _) =
            task {
                return
                    lock gate (fun () ->
                        let agentDir = FileSystemPaths.packageAgentDir rootFull tenant agentId
                        let activePath = FileSystemPaths.packageActivePath agentDir

                        let active =
                            if File.Exists activePath then
                                File.ReadAllText(activePath, Encoding.UTF8).Trim()
                            else
                                ""

                        if String.IsNullOrEmpty active then
                            Unchecked.defaultof<AgentPackageInfo>
                        else
                            let versionDir = FileSystemPaths.packageVersionDir rootFull tenant agentId active

                            if not (Directory.Exists versionDir) then
                                Unchecked.defaultof<AgentPackageInfo>
                            else
                                let paths =
                                    Directory.EnumerateFiles(versionDir, "*", SearchOption.AllDirectories)
                                    |> Seq.map (FileSystemPaths.entryRelativePath versionDir)
                                    |> Seq.toList

                                let read (path: string) : string | null =
                                    let full = FileSystemPaths.entryPathUnder versionDir path

                                    if File.Exists full then
                                        File.ReadAllText(full, Encoding.UTF8)
                                    else
                                        Unchecked.defaultof<string>

                                let skills =
                                    FileSystemPackageLayout.namesUnder
                                        paths
                                        FileSystemPackageLayout.skillsPrefix
                                        "/SKILL.md"

                                let subAgents =
                                    FileSystemPackageLayout.namesUnder
                                        paths
                                        FileSystemPackageLayout.agentsPrefix
                                        "/AGENT.md"

                                let sourcePath = FileSystemPaths.packageSourcePath agentDir

                                let source =
                                    if File.Exists sourcePath then
                                        File.ReadAllText(sourcePath, Encoding.UTF8)
                                    else
                                        Unchecked.defaultof<string>

                                AgentPackageInfo(
                                    FileSystemPackageLayout.instructionsOf read,
                                    (skills :> IReadOnlyList<string>),
                                    (subAgents :> IReadOnlyList<string>),
                                    source,
                                    active
                                ))
            }

        member _.ReadFile(tenant, agentId, version, path, _) =
            task {
                PackageVersions.Validate version |> ignore
                let canonicalPath = AgentPackagePaths.Normalise path

                return
                    lock gate (fun () ->
                        let versionDir = FileSystemPaths.packageVersionDir rootFull tenant agentId version
                        let full = FileSystemPaths.entryPathUnder versionDir canonicalPath

                        if File.Exists full then
                            File.OpenRead full :> Stream
                        else
                            Unchecked.defaultof<Stream>)
            }

        member _.ListFiles(tenant, agentId, version, prefix, _) =
            task {
                PackageVersions.Validate version |> ignore

                if isNull (box prefix) then
                    raise (ArgumentNullException(nameof prefix))

                let canonicalPrefix = AgentPackagePaths.Normalise prefix

                return
                    lock gate (fun () ->
                        let versionDir = FileSystemPaths.packageVersionDir rootFull tenant agentId version

                        if not (Directory.Exists versionDir) then
                            [] :> IReadOnlyList<string>
                        else
                            Directory.EnumerateFiles(versionDir, "*", SearchOption.AllDirectories)
                            |> Seq.map (FileSystemPaths.entryRelativePath versionDir)
                            |> Seq.filter (fun path ->
                                path = canonicalPrefix
                                || path.StartsWith(canonicalPrefix + "/", StringComparison.Ordinal))
                            |> Seq.sortWith (fun left right -> String.CompareOrdinal(left, right))
                            |> Seq.toList
                            :> IReadOnlyList<string>)
            }

        member _.UploadPackage(tenant, agentId, version, source, entries, cancellationToken) =
            task {
                if isNull (box source) then
                    raise (ArgumentNullException(nameof source))

                PackageVersions.Validate version |> ignore

                // Materialise and validate outside the gate: the entries
                // are caller-supplied streams, and a failure must leave
                // every directory untouched.
                let! files = materialise entries cancellationToken

                return
                    lock gate (fun () ->
                        let versionDir = FileSystemPaths.packageVersionDir rootFull tenant agentId version

                        let createdAt =
                            if Directory.Exists versionDir then
                                createdAtOf tenant agentId version versionDir
                            else
                                DateTimeOffset.UtcNow

                        let staged = stageFiles files

                        try
                            swapStaged staged versionDir
                            writeState tenant agentId version createdAt source true
                            AgentPackageVersion(version, createdAt)
                        with _ ->
                            FileSystemAtomic.tryDeleteDirectory staged
                            reraise ())
            }

        member _.ReplacePackageVersion(tenant, agentId, version, source, entries, cancellationToken) =
            task {
                if isNull (box source) then
                    raise (ArgumentNullException(nameof source))

                PackageVersions.Validate version |> ignore
                let! files = materialise entries cancellationToken

                return
                    lock gate (fun () ->
                        let versionDir = FileSystemPaths.packageVersionDir rootFull tenant agentId version

                        if not (Directory.Exists versionDir) then
                            raise (ArgumentException("The version to replace is unknown.", nameof version))

                        let createdAt = createdAtOf tenant agentId version versionDir
                        let staged = stageFiles files

                        try
                            swapStaged staged versionDir
                            writeState tenant agentId version createdAt source false
                            AgentPackageVersion(version, createdAt)
                        with _ ->
                            FileSystemAtomic.tryDeleteDirectory staged
                            reraise ())
            }

        member _.DeletePackageVersion(tenant, agentId, version, _) =
            task {
                PackageVersions.Validate version |> ignore

                return
                    lock gate (fun () ->
                        let versionDir = FileSystemPaths.packageVersionDir rootFull tenant agentId version
                        let removed = Directory.Exists versionDir

                        if removed then
                            Directory.Delete(versionDir, true)

                            FileSystemAtomic.tryDeleteFile (
                                FileSystemPaths.packageVersionMetaPath rootFull tenant agentId version
                            )

                        // Deleting the active version clears it: nothing
                        // else auto-promotes.
                        let agentDir = FileSystemPaths.packageAgentDir rootFull tenant agentId
                        let activePath = FileSystemPaths.packageActivePath agentDir

                        if
                            File.Exists activePath
                            && String.Equals(
                                File.ReadAllText(activePath, Encoding.UTF8).Trim(),
                                version,
                                StringComparison.Ordinal
                            )
                        then
                            FileSystemAtomic.tryDeleteFile activePath

                        removed)
            }

        member _.ListVersions(tenant, agentId, _) =
            task {
                return
                    lock gate (fun () ->
                        let agentDir = FileSystemPaths.packageAgentDir rootFull tenant agentId
                        let versionsRoot = Path.Combine(agentDir, FileSystemPaths.versionsDirectoryName)

                        if not (Directory.Exists versionsRoot) then
                            [] :> IReadOnlyList<AgentPackageVersion>
                        else
                            Directory.EnumerateDirectories versionsRoot
                            |> Seq.map Path.GetFileName
                            |> Seq.choose Option.ofObj
                            |> Seq.filter PackageVersions.TryValidate
                            |> Seq.map (fun version ->
                                let versionDir = Path.Combine(versionsRoot, version)
                                AgentPackageVersion(version, createdAtOf tenant agentId version versionDir))
                            |> Seq.sortBy (fun packageVersion -> packageVersion.Version)
                            |> Seq.toList
                            :> IReadOnlyList<AgentPackageVersion>)
            }
