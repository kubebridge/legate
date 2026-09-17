// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.FileSystem

open System
open System.IO
open System.Text
open System.Text.Json
open Legate

// Layout, containment, and atomic-write helpers shared by the file-system
// stores: every key, path, and version funnels through here before touching
// disk, so the root-containment rule and the temp-plus-rename discipline live
// in one place. Internal and small on purpose so the versioned
// package-manifest work in #107 can reuse the seam without rebuilding it.

// ──────────────────────────────────────────────────────────────────────────
// Path layout and containment

/// <summary>
/// The on-disk layout derivation, root-containment checks, atomic file
/// helpers, and sidecar manifest readers shared by the file-system stores.
/// Internal: hosts reach the layout only through the stores.
/// </summary>
module internal FileSystemPaths =

    /// <summary>
    /// Blob bytes live under this child of the root, one file per validated key.
    /// </summary>
    let blobsDirectoryName = "blobs"

    /// <summary>
    /// Blob sidecars (etag plus content-type JSON) live under this child of
    /// the root, mirroring the key with a JSON suffix, so listings over the
    /// data tree never see metadata.
    /// </summary>
    let blobMetaDirectoryName = "blob-meta"

    /// <summary>
    /// Agent packages live under this child of the root.
    /// </summary>
    let packagesDirectoryName = "packages"

    /// <summary>
    /// One version's content lives in a directory of this name under its
    /// agent directory; its creation stamp lives beside it with a JSON
    /// suffix, so content scans never see metadata.
    /// </summary>
    let versionsDirectoryName = "versions"

    /// <summary>
    /// Atomic-write staging lives under this child of the root: temp files
    /// and staged trees are created here (same volume, so the final rename
    /// stays atomic) and renamed into place; crash orphans stay invisible
    /// here instead of beside live data.
    /// </summary>
    let stagingDirectoryName = "tmp"

    /// <summary>
    /// The file inside an agent directory naming the active version.
    /// </summary>
    let activeFileName = "active.txt"

    /// <summary>
    /// The file inside an agent directory carrying the last write's opaque
    /// source label.
    /// </summary>
    let sourceFileName = "source.txt"

    /// <summary>
    /// The suffix appended to a blob key for its sidecar path.
    /// </summary>
    let blobMetaSuffix = ".json"

    /// <summary>
    /// The suffix appended to a version directory name for its stamp file.
    /// </summary>
    let versionMetaSuffix = ".json"

    /// <summary>
    /// Percent-escapes every character of a path segment outside
    /// [A-Za-z0-9_-] as a fixed-width uppercase <c>%XXXX</c> hex escape per
    /// UTF-16 code unit, mirroring the tenant rule in
    /// <see cref="T:Legate.BlobKeys" />, so distinct tenants always map to
    /// distinct segments (the escape is injective) and no escaped segment
    /// can form a traversal or introduce a separator.
    /// </summary>
    /// <param name="value">The segment to escape. Must not be null.</param>
    /// <returns>The escaped segment.</returns>
    let escapeSegment (value: string) : string =
        let builder = StringBuilder()

        let safe (c: char) =
            Char.IsAsciiLetterOrDigit c || c = '_' || c = '-'

        for c in value do
            if safe c then
                builder.Append(c) |> ignore
            else
                builder.Append('%') |> ignore

                builder.Append((int c).ToString("X4", Globalization.CultureInfo.InvariantCulture))
                |> ignore

        builder.ToString()

    /// <summary>
    /// Resolves the root to a full path and creates the store directories
    /// (blobs, blob sidecars, packages, staging) under it.
    /// </summary>
    /// <param name="root">The configured root directory.</param>
    /// <returns>The root as a full path.</returns>
    let ensureRoot (root: string) : string =
        let rootFull = Path.GetFullPath root

        for child in
            [
                blobsDirectoryName
                blobMetaDirectoryName
                packagesDirectoryName
                stagingDirectoryName
            ] do
            Directory.CreateDirectory(Path.Combine(rootFull, child)) |> ignore

        rootFull

    /// <summary>
    /// The staging directory under the root, created when absent.
    /// </summary>
    /// <param name="rootFull">The full root path.</param>
    /// <returns>The full staging path.</returns>
    let stagingRoot (rootFull: string) : string =
        let staging = Path.Combine(rootFull, stagingDirectoryName)
        Directory.CreateDirectory staging |> ignore
        staging

    /// <summary>
    /// Reports whether a full path stays inside the root: the path itself
    /// is the root or starts with the root plus a separator, compared
    /// ordinally.
    /// </summary>
    /// <param name="rootFull">The full root path.</param>
    /// <param name="full">The full path to test.</param>
    /// <returns>True when the path stays inside the root; otherwise false.</returns>
    let isUnderRoot (rootFull: string) (full: string) : bool =
        let rooted =
            rootFull.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)

        full = rootFull
        || full.StartsWith(rooted + string Path.DirectorySeparatorChar, StringComparison.Ordinal)

    /// <summary>
    /// Tries to resolve path parts under the root, rejecting any resolved
    /// path that escapes it.
    /// </summary>
    /// <param name="rootFull">The full root path.</param>
    /// <param name="parts">The path parts to join under the root.</param>
    /// <returns>The full path, or None when it escapes the root.</returns>
    let tryResolveUnderRoot (rootFull: string) (parts: string list) : string option =
        let full = Path.GetFullPath(Path.Combine(Array.ofList (rootFull :: parts)))

        if isUnderRoot rootFull full then Some full else None

    /// <summary>
    /// Resolves path parts under the root, raising when the resolved path
    /// escapes it. Part validation (keys, versions, entry paths) runs before
    /// this; the check is the last-moment containment fence.
    /// </summary>
    /// <param name="rootFull">The full root path.</param>
    /// <param name="parts">The path parts to join under the root.</param>
    /// <returns>The full path.</returns>
    let resolveUnderRoot (rootFull: string) (parts: string list) : string =
        match tryResolveUnderRoot rootFull parts with
        | Some full -> full
        | None ->
            raise (InvalidOperationException(sprintf "A store path escaped its root: %s." (String.Join("/", parts))))

    /// <summary>
    /// The data-file path for a validated blob key: the key segments joined
    /// under the blobs directory.
    /// </summary>
    /// <param name="rootFull">The full root path.</param>
    /// <param name="key">The validated blob key.</param>
    /// <returns>The full data-file path.</returns>
    let blobDataPath (rootFull: string) (key: string) : string =
        let segments = blobsDirectoryName :: List.ofArray (key.Split('/'))
        resolveUnderRoot rootFull segments

    /// <summary>
    /// The sidecar path for a validated blob key: the key segments joined
    /// under the sidecar directory with a JSON suffix on the last segment.
    /// </summary>
    /// <param name="rootFull">The full root path.</param>
    /// <param name="key">The validated blob key.</param>
    /// <returns>The full sidecar path.</returns>
    let blobMetaPath (rootFull: string) (key: string) : string =
        let segments = List.ofArray (key.Split('/'))

        let suffixed =
            segments
            |> List.mapi (fun index segment ->
                if index = segments.Length - 1 then
                    segment + blobMetaSuffix
                else
                    segment)

        resolveUnderRoot rootFull (blobMetaDirectoryName :: suffixed)

    /// <summary>
    /// Maps a data file back to its blob key: the path relative to the
    /// blobs directory with separators folded to slashes.
    /// </summary>
    /// <param name="rootFull">The full root path.</param>
    /// <param name="fileFull">The full data-file path.</param>
    /// <returns>The blob key.</returns>
    let blobKeyOfFile (rootFull: string) (fileFull: string) : string =
        let blobsRoot = Path.Combine(rootFull, blobsDirectoryName)

        String.Join(
            "/",
            Path.GetRelativePath(blobsRoot, fileFull).Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)
        )

    /// <summary>
    /// The agent directory for a tenant and agent: the escaped tenant and
    /// the canonical agent id joined under the packages directory.
    /// </summary>
    /// <param name="rootFull">The full root path.</param>
    /// <param name="tenant">The tenant that owns the package.</param>
    /// <param name="agentId">The agent the package belongs to.</param>
    /// <returns>The full agent-directory path.</returns>
    let packageAgentDir (rootFull: string) (tenant: TenantId) (agentId: AgentId) : string =
        resolveUnderRoot
            rootFull
            [
                packagesDirectoryName
                escapeSegment tenant.Value
                agentId.Value
            ]

    /// <summary>
    /// One version's content directory: the validated version joined under
    /// the agent's versions directory.
    /// </summary>
    /// <param name="rootFull">The full root path.</param>
    /// <param name="tenant">The tenant that owns the package.</param>
    /// <param name="agentId">The agent the package belongs to.</param>
    /// <param name="version">The validated version string.</param>
    /// <returns>The full version-directory path.</returns>
    let packageVersionDir (rootFull: string) (tenant: TenantId) (agentId: AgentId) (version: string) : string =
        resolveUnderRoot
            rootFull
            [
                packagesDirectoryName
                escapeSegment tenant.Value
                agentId.Value
                versionsDirectoryName
                version
            ]

    /// <summary>
    /// One version's stamp file: the validated version plus a JSON suffix
    /// beside its content directory, carrying the creation stamp.
    /// </summary>
    /// <param name="rootFull">The full root path.</param>
    /// <param name="tenant">The tenant that owns the package.</param>
    /// <param name="agentId">The agent the package belongs to.</param>
    /// <param name="version">The validated version string.</param>
    /// <returns>The full stamp-file path.</returns>
    let packageVersionMetaPath (rootFull: string) (tenant: TenantId) (agentId: AgentId) (version: string) : string =
        resolveUnderRoot
            rootFull
            [
                packagesDirectoryName
                escapeSegment tenant.Value
                agentId.Value
                versionsDirectoryName
                version + versionMetaSuffix
            ]

    /// <summary>
    /// The active-version pointer inside an agent directory.
    /// </summary>
    /// <param name="agentDirFull">The full agent-directory path.</param>
    /// <returns>The full pointer path.</returns>
    let packageActivePath (agentDirFull: string) : string =
        Path.Combine(agentDirFull, activeFileName)

    /// <summary>
    /// The source-label file inside an agent directory.
    /// </summary>
    /// <param name="agentDirFull">The full agent-directory path.</param>
    /// <returns>The full source-label path.</returns>
    let packageSourcePath (agentDirFull: string) : string =
        Path.Combine(agentDirFull, sourceFileName)

    /// <summary>
    /// Resolves a canonical entry path under a version directory, rejecting
    /// any resolved path that escapes it.
    /// </summary>
    /// <param name="versionDirFull">The full version-directory path.</param>
    /// <param name="canonicalPath">The normalised entry path.</param>
    /// <returns>The full entry path.</returns>
    let entryPathUnder (versionDirFull: string) (canonicalPath: string) : string =
        match tryResolveUnderRoot versionDirFull (List.ofArray (canonicalPath.Split('/'))) with
        | Some full -> full
        | None ->
            raise (
                InvalidPackagePathException(
                    canonicalPath,
                    "A package entry path must stay inside its version directory."
                )
            )

    /// <summary>
    /// Maps a version file back to its canonical entry path: the path
    /// relative to the version directory with separators folded to slashes.
    /// </summary>
    /// <param name="versionDirFull">The full version-directory path.</param>
    /// <param name="fileFull">The full entry-file path.</param>
    /// <returns>The canonical entry path.</returns>
    let entryRelativePath (versionDirFull: string) (fileFull: string) : string =
        String.Join(
            "/",
            Path
                .GetRelativePath(versionDirFull, fileFull)
                .Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)
        )

// ──────────────────────────────────────────────────────────────────────────
// Atomic file helpers

/// <summary>
/// Temp-plus-rename file helpers: every write lands in a uniquely named
/// temp file under staging (same volume, so the final rename stays atomic)
/// and is renamed into place; failures leave the previous content intact
/// and only invisible staging orphans behind.
/// </summary>
module internal FileSystemAtomic =

    /// <summary>
    /// Creates a uniquely named staging directory under the staging root.
    /// </summary>
    /// <param name="stagingRootFull">The full staging path.</param>
    /// <returns>The full staging-directory path, created.</returns>
    let freshStagingDir (stagingRootFull: string) : string =
        let staged = Path.Combine(stagingRootFull, "stage-" + Guid.NewGuid().ToString("N"))
        Directory.CreateDirectory staged |> ignore
        staged

    /// <summary>
    /// Best-effort file deletion: cleanup must never fail a store call.
    /// </summary>
    /// <param name="path">The file to delete.</param>
    let tryDeleteFile (path: string) : unit =
        try
            File.Delete path
        with _ ->
            ()

    /// <summary>
    /// Best-effort recursive directory deletion: cleanup must never fail a
    /// store call.
    /// </summary>
    /// <param name="path">The directory to delete.</param>
    let tryDeleteDirectory (path: string) : unit =
        try
            if Directory.Exists path then
                Directory.Delete(path, true)
        with _ ->
            ()

    /// <summary>
    /// Writes bytes atomically: a uniquely named temp file under staging,
    /// then a rename over the final path (creating its parent). A failure
    /// leaves the previous content intact.
    /// </summary>
    /// <param name="stagingRootFull">The full staging path.</param>
    /// <param name="finalPath">The full final path.</param>
    /// <param name="bytes">The bytes to write.</param>
    let writeFileAtomic (stagingRootFull: string) (finalPath: string) (bytes: byte[]) : unit =
        let temp =
            Path.Combine(stagingRootFull, "file-" + Guid.NewGuid().ToString("N") + ".tmp")

        File.WriteAllBytes(temp, bytes)

        try
            match Path.GetDirectoryName finalPath with
            | null -> ()
            | parent -> Directory.CreateDirectory parent |> ignore

            File.Move(temp, finalPath, true)
        with _ ->
            tryDeleteFile temp
            reraise ()

    /// <summary>
    /// Removes now-empty parent directories up to (excluding) the stop
    /// directory, so prefix deletions do not leave empty husks behind.
    /// Best-effort throughout.
    /// </summary>
    /// <param name="stopExclusiveFull">The full directory to stop at (never removed).</param>
    /// <param name="startFull">The full file or directory to start from.</param>
    let removeEmptyParents (stopExclusiveFull: string) (startFull: string) : unit =
        let rec prune (current: string | null) : unit =
            match current with
            | null -> ()
            | dir when String.Equals(dir, stopExclusiveFull, StringComparison.Ordinal) -> ()
            | dir ->
                if Directory.Exists dir && Directory.GetFileSystemEntries(dir).Length = 0 then
                    try
                        Directory.Delete dir
                        prune (Path.GetDirectoryName dir)
                    with _ ->
                        ()
                else
                    ()

        try
            prune (
                if Directory.Exists startFull then
                    startFull
                else
                    Path.GetDirectoryName startFull
            )
        with _ ->
            ()

// ──────────────────────────────────────────────────────────────────────────
// Sidecar manifests

/// <summary>
/// The JSON sidecar shapes: blob etag plus content type, and package
/// version creation stamps. Small records serialised with
/// <see cref="T:System.Text.Json.JsonSerializer" /> so #107 can reuse the
/// manifest shape for versioned leases.
/// </summary>
module internal FileSystemManifests =

    /// <summary>
    /// Serialises a blob sidecar: the opaque etag plus the stored content type.
    /// </summary>
    /// <param name="etag">The opaque version token.</param>
    /// <param name="contentType">The stored content type.</param>
    /// <returns>The sidecar JSON bytes (UTF-8).</returns>
    let blobMetaJson (etag: string) (contentType: string) : byte[] =
        JsonSerializer.SerializeToUtf8Bytes(
            {|
                etag = etag
                contentType = contentType
            |}
        )

    /// <summary>
    /// Reads a blob sidecar back to its etag and content type.
    /// </summary>
    /// <param name="bytes">The sidecar bytes.</param>
    /// <returns>The etag and content type, or None when the sidecar is missing or corrupt.</returns>
    let tryReadBlobMeta (bytes: byte[]) : (string * string) option =
        try
            use document = JsonDocument.Parse bytes
            let root = document.RootElement
            let etag = root.GetProperty("etag").GetString()
            let contentType = root.GetProperty("contentType").GetString()

            match etag, contentType with
            | null, _ -> None
            | _, null -> None
            | etag, contentType -> Some(etag, contentType)
        with _ ->
            None

    /// <summary>
    /// Serialises a version stamp: when the version was first published.
    /// </summary>
    /// <param name="createdAt">The creation stamp.</param>
    /// <returns>The stamp JSON bytes (UTF-8).</returns>
    let versionMetaJson (createdAt: DateTimeOffset) : byte[] =
        JsonSerializer.SerializeToUtf8Bytes(
            {|
                createdAt = createdAt.ToString("O", Globalization.CultureInfo.InvariantCulture)
            |}
        )

    /// <summary>
    /// Reads a version stamp back to its creation instant.
    /// </summary>
    /// <param name="bytes">The stamp bytes.</param>
    /// <returns>The creation stamp, or None when the stamp is missing or corrupt.</returns>
    let tryReadVersionCreatedAt (bytes: byte[]) : DateTimeOffset option =
        try
            use document = JsonDocument.Parse bytes
            let raw = document.RootElement.GetProperty("createdAt").GetString()

            match raw with
            | null -> None
            | raw ->
                let mutable parsed = DateTimeOffset.MinValue

                if
                    DateTimeOffset.TryParse(
                        raw,
                        Globalization.CultureInfo.InvariantCulture,
                        Globalization.DateTimeStyles.RoundtripKind,
                        &parsed
                    )
                then
                    Some parsed
                else
                    None
        with _ ->
            None

// ──────────────────────────────────────────────────────────────────────────
// Async enumeration

/// <summary>
/// Turns a synchronous list into an
/// <see cref="T:System.Collections.Generic.IAsyncEnumerable`1" /> that
/// yields without asynchrony.
/// </summary>
module internal FileSystemAsync =

    /// <summary>
    /// Adapts a list into an IAsyncEnumerable, in order.
    /// </summary>
    /// <param name="items">The items to yield.</param>
    /// <returns>The items as an async enumerable.</returns>
    let ofList (items: 'T list) : Collections.Generic.IAsyncEnumerable<'T> =
        { new Collections.Generic.IAsyncEnumerable<'T> with
            member _.GetAsyncEnumerator(_: Threading.CancellationToken) : Collections.Generic.IAsyncEnumerator<'T> =
                let items = items |> List.toArray
                let mutable index = -1

                { new Collections.Generic.IAsyncEnumerator<'T> with
                    member this.MoveNextAsync() : Threading.Tasks.ValueTask<bool> =
                        let mutable next = false

                        if index + 1 < items.Length then
                            index <- index + 1
                            next <- true

                        Threading.Tasks.ValueTask<bool>(next)

                    member _.Current: 'T = items[index]

                    member _.DisposeAsync() : Threading.Tasks.ValueTask = Threading.Tasks.ValueTask()
                }
        }
