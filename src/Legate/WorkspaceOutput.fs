// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.IO
open System.Threading
open System.Threading.Tasks

// Per-attempt workspace output persistence (issue 115). Before a turn
// settles, the runtime persists the bound workspace's output/ files to the
// artifact blob scope under attempts/{n}/output/{rel} keys (derived through
// BlobKeys.ForArtifact over BlobKeys.AttemptOutputName, the same derivation
// get_download_url and re-bind restore use). Every Put is fenced by the
// turn's claim token at the last moment through ClaimFence.verifyAsync: a
// lost or missing claim skips with zero writes, so a stale attempt can
// never overwrite. There is no turn-host or actor wiring here (no per-turn
// Bind caller exists yet); the dispatcher and session-ops waves own the
// end-to-end per-turn binding as a follow-up.

// ──────────────────────────────────────────────────────────────────────────
// Persist

/// Persists a bound workspace's outputs to the artifact blob scope, one key
/// per attempt. Internal so no store type ever crosses the public API; tests
/// drive <c>persistAsync</c> directly against the in-memory blob store.
module internal WorkspaceOutput =

    /// The workspace root output files persist from.
    [<Literal>]
    let OutputWorkspacePrefix = "output/"

    /// The content type stored with persisted outputs: workspace files carry
    /// no mime metadata, so every output lands as opaque bytes (the
    /// artifact-store fallback precedent).
    [<Literal>]
    let OutputContentType = "application/octet-stream"

    /// Resolves the workspace's output/ directory on disk. Fails loudly
    /// when the runtime exposes no root path (the search-tools precedent:
    /// a null path cannot enumerate, so it throws instead of persisting a
    /// silent zero).
    /// <param name="workspace">The bound workspace. Must not be null.</param>
    /// <returns>The absolute output/ directory, which may not exist yet.</returns>
    let private outputDirectory (workspace: IWorkspace) : string =
        if isNull (box workspace) then
            raise (ArgumentNullException(nameof workspace))

        match workspace.Root.Path with
        | null ->
            raise (
                WorkspaceException(
                    OutputWorkspacePrefix,
                    "The workspace does not expose a root path; output persistence requires a path-exposing runtime."
                )
            )
        | rootPath -> Path.Combine(Path.GetFullPath rootPath, "output")

    /// Lists the slash-separated workspace-relative paths of every file
    /// under the workspace's output/ root, in lexicographic order. A
    /// missing output/ directory lists as empty: an agent that produced no
    /// outputs persists nothing and still succeeds.
    /// <param name="workspace">The bound workspace. Must not be null.</param>
    /// <returns>The output-relative file paths, slash-separated.</returns>
    let private listOutputFiles (workspace: IWorkspace) : string list =
        let outputDir = outputDirectory workspace

        if not (Directory.Exists outputDir) then
            []
        else
            Directory.EnumerateFiles(outputDir, "*", SearchOption.AllDirectories)
            |> Seq.map (fun full -> Path.GetRelativePath(outputDir, full).Replace(Path.DirectorySeparatorChar, '/'))
            |> List.ofSeq
            |> List.sort

    /// Persists every file under the workspace's output/ root to the
    /// artifact blob scope under the attempt's keys
    /// (<c>attempts/{n}/output/{rel}</c> through
    /// <see cref="T:Legate.BlobKeys" />). The claim is verified at the last
    /// moment before each Put: a null claim reads as fenced out
    /// (fail-closed, mirroring <c>ClaimFence.isLiveState</c>), and the first lost
    /// or missing verification skips every remaining write, so a stale
    /// attempt produces zero further effects.
    /// <param name="sessionStore">The durable store verifying the claim. Must not be null.</param>
    /// <param name="blobStore">The blob primitives to write through. Must not be null.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session whose outputs persist.</param>
    /// <param name="claim">The claim fencing the writes, or null when ownership cannot be proven (writes zero).</param>
    /// <param name="attempt">The 1-based attempt the outputs persist under. Must be positive.</param>
    /// <param name="workspace">The bound workspace to persist from. Must not be null.</param>
    /// <param name="cancellationToken">Abandons the persistence.</param>
    /// <returns>How many output files were persisted.</returns>
    let persistAsync
        (sessionStore: ISessionStore)
        (blobStore: IBlobStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (claim: TurnClaim)
        (attempt: int)
        (workspace: IWorkspace)
        (cancellationToken: CancellationToken)
        : Task<int> =
        task {
            ArgumentNullException.ThrowIfNull(sessionStore)
            ArgumentNullException.ThrowIfNull(blobStore)

            if isNull (box workspace) then
                raise (ArgumentNullException(nameof workspace))

            if attempt < 1 then
                raise (ArgumentOutOfRangeException(nameof attempt, "The attempt number must be positive."))

            if isNull (box claim) then
                return 0
            else
                let outputDir = outputDirectory workspace
                let files = listOutputFiles workspace

                let mutable persisted = 0
                let mutable fencedOut = false

                for rel in files do
                    if not fencedOut then
                        let! state = ClaimFence.verifyAsync sessionStore tenant claim cancellationToken

                        if ClaimFence.isLiveState state then
                            let fullPath =
                                Path.Combine(outputDir, rel.Replace('/', Path.DirectorySeparatorChar))

                            let! bytes = File.ReadAllBytesAsync(fullPath, cancellationToken)

                            let key =
                                BlobKeys.ForArtifact(tenant, sessionId, BlobKeys.AttemptOutputName(attempt, rel))

                            let! _ = blobStore.Put(key, BlobContent(bytes, OutputContentType), cancellationToken)
                            persisted <- persisted + 1
                        else
                            fencedOut <- true

                return persisted
        }
