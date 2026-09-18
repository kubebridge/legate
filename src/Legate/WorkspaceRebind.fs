// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks

// Workspace idle teardown and blob re-bind (issue 110, attempt-aware
// re-stage in issue 115). The runtime owns idle teardown per
// IWorkspaceRuntime: disposing a bound IWorkspace destroys only the
// execution vehicle, never the workspace's files, and the next Bind
// re-binds over the same state honouring Session.WorkspaceBinding. This
// file holds the two halves the expiry service drives: a clock-seamed
// tracker of bound workspaces whose idle entries the sweeper disposes, and
// a restore step that re-stages a freshly bound workspace's input/ and
// output/ areas from the blob store (session scope for input, artifact
// scope for output) through the same BlobKeys derivation the DownloadUrl
// tool uses. Attempt-scoped outputs (attempts/{n}/output/{rel}, written by
// WorkspaceOutput.persistAsync) re-stage latest-attempt-wins: only the
// highest attempt lands under output/, prior attempts stay
// blob-addressable; every other artifact blob restores at its legacy path.
// Restoring output files is what output persistence feeds; a null payload
// means absent, never an error.

// ──────────────────────────────────────────────────────────────────────────
// Restore

/// Re-stages workspace files from the blob store onto a freshly bound
/// workspace. Internal so no store type ever crosses the public API; tests
/// drive <c>restoreAsync</c> directly against the in-memory blob store.
module internal WorkspaceRebind =

    /// The session-scope relative prefix input files restore from: blobs
    /// named under <c>input/</c> in the session scope land under the
    /// workspace's <c>input/</c> root at the same relative path.
    [<Literal>]
    let InputBlobPrefix = "input/"

    /// The workspace root input files restore under.
    [<Literal>]
    let InputWorkspacePrefix = "input/"

    /// The workspace root output files restore under: every artifact-scope
    /// blob lands under <c>output/</c> at its scope-relative name, because
    /// the artifact scope is output-dedicated.
    [<Literal>]
    let OutputWorkspacePrefix = "output/"

    /// Collects one blob listing into a list, skipping null entries so a
    /// store quirk never fails a restore.
    /// <param name="source">The key enumeration to collect.</param>
    /// <param name="cancellationToken">Abandons the collection.</param>
    /// <returns>The listed keys, in the store's order.</returns>
    let private collectAsync
        (source: IAsyncEnumerable<string>)
        (cancellationToken: CancellationToken)
        : Task<string list> =
        task {
            ArgumentNullException.ThrowIfNull(source)
            let found = ResizeArray<string>()
            use enumerator = source.GetAsyncEnumerator(cancellationToken)
            let mutable go = true

            while go do
                let! has = enumerator.MoveNextAsync().AsTask()

                if has then
                    let current = enumerator.Current

                    if not (isNull (box current)) then
                        found.Add current
                else
                    go <- false

            return found |> List.ofSeq
        }

    /// Restores one blob into the workspace when it is present: a null
    /// payload means absent and is skipped, never an error.
    /// <param name="blobStore">The blob primitives to read through.</param>
    /// <param name="key">The full blob key to read.</param>
    /// <param name="destination">The workspace-relative path to write.</param>
    /// <param name="workspace">The freshly bound workspace to stage into.</param>
    /// <param name="cancellationToken">Abandons the restore.</param>
    /// <returns>True when a blob was restored; false when it was absent.</returns>
    let private restoreOneAsync
        (blobStore: IBlobStore)
        (key: string)
        (destination: string)
        (workspace: IWorkspace)
        (cancellationToken: CancellationToken)
        : Task<bool> =
        task {
            let! bytes = blobStore.Get(key, cancellationToken)

            match bytes with
            | null -> return false
            | data ->
                do! workspace.WriteFile(destination, data, cancellationToken)
                return true
        }

    /// Splits an artifact-scope relative name into its attempt and
    /// output-relative path when it is a well-formed attempt-output name
    /// (<c>attempts/{n}/output/{rel}</c> with a positive <c>n</c> and a
    /// non-empty <c>rel</c>, the shape
    /// <c>WorkspaceOutput.persistAsync</c> writes through
    /// <c>BlobKeys.AttemptOutputName</c>); anything else reads as legacy
    /// and restores at its full relative path, so pre-attempt scopes
    /// restore exactly as before.
    /// <param name="relative">The artifact-scope relative name.</param>
    /// <returns>The attempt and output-relative path, or None for legacy names.</returns>
    let private tryParseAttemptOutput (relative: string) : (int * string) option =
        if isNull (box relative) then
            None
        else
            let segments = relative.Split('/')

            if segments.Length >= 4 && segments[0] = "attempts" && segments[2] = "output" then
                match Int32.TryParse segments[1] with
                | true, attempt when attempt >= 1 ->
                    let rel = String.Join("/", segments[3..])

                    if rel = "" then None else Some(attempt, rel)
                | _ -> None
            else
                None

    /// Re-stages a freshly bound workspace's <c>input/</c> area from the
    /// session blob scope and its <c>output/</c> area from the artifact blob
    /// scope. Input lists the session scope under the <c>input/</c> relative
    /// prefix (the prefix form without a trailing slash, which
    /// <see cref="T:Legate.BlobKeys" /> validation accepts) and writes each
    /// present blob back at its scope-relative path; output lists the whole
    /// artifact scope (the scope prefix without its trailing slash, which
    /// keeps validation happy while session ids stay fixed-width, so no
    /// sibling session can share the prefix). Legacy artifact blobs (any
    /// name that is not a well-formed <c>attempts/{n}/output/{rel}</c> name)
    /// write under <c>output/</c> at their scope-relative name, exactly as
    /// before; attempt-scoped outputs re-stage latest-attempt-wins on top,
    /// so the highest attempt wins on collision while prior attempts stay
    /// blob-addressable but never land in the workspace. Absent blobs are
    /// skipped; an empty scope restores nothing and still succeeds.
    /// <param name="blobStore">The blob primitives to read through.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session being re-bound.</param>
    /// <param name="workspace">The freshly bound workspace to stage into.</param>
    /// <param name="cancellationToken">Abandons the restore.</param>
    /// <returns>The restored input and output file counts.</returns>
    let restoreAsync
        (blobStore: IBlobStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (workspace: IWorkspace)
        (cancellationToken: CancellationToken)
        : Task<int * int> =
        task {
            ArgumentNullException.ThrowIfNull(blobStore)

            if isNull (box workspace) then
                raise (ArgumentNullException(nameof workspace))

            let sessionPrefix = BlobKeys.SessionPrefix(tenant, sessionId)
            let inputListPrefix = sessionPrefix + "input"
            let! inputKeys = collectAsync (blobStore.List(inputListPrefix, cancellationToken)) cancellationToken

            let mutable inputCount = 0

            for key in inputKeys do
                if key.StartsWith(sessionPrefix, StringComparison.Ordinal) then
                    let relative = key.Substring(sessionPrefix.Length)

                    if relative.StartsWith(InputBlobPrefix, StringComparison.Ordinal) then
                        let! restored = restoreOneAsync blobStore key relative workspace cancellationToken

                        if restored then
                            inputCount <- inputCount + 1

            let artifactPrefix = BlobKeys.ArtifactPrefix(tenant, sessionId)
            let artifactListPrefix = artifactPrefix.TrimEnd('/')
            let! artifactKeys = collectAsync (blobStore.List(artifactListPrefix, cancellationToken)) cancellationToken

            let mutable outputCount = 0
            let legacy = ResizeArray<string * string>()
            let attempts = Dictionary<int, ResizeArray<string * string>>()

            for key in artifactKeys do
                if key.StartsWith(artifactPrefix, StringComparison.Ordinal) then
                    let relative = key.Substring(artifactPrefix.Length)

                    match tryParseAttemptOutput relative with
                    | Some(attempt, rel) ->
                        match attempts.TryGetValue attempt with
                        | true, bucket -> bucket.Add(key, rel)
                        | false, _ -> attempts[attempt] <- ResizeArray([ (key, rel) ])
                    | None ->
                        if not (String.IsNullOrEmpty relative) then
                            legacy.Add(key, relative)

            // Legacy first: every non-attempt blob lands under output/ at
            // its scope-relative name, exactly as before.
            for key, relative in legacy do
                let destination = OutputWorkspacePrefix + relative
                let! restored = restoreOneAsync blobStore key destination workspace cancellationToken

                if restored then
                    outputCount <- outputCount + 1

            // Then the latest attempt on top, so it wins on collision;
            // prior attempts stay blob-addressable but never stage.
            if attempts.Count > 0 then
                let latest = attempts.Keys |> Seq.max

                for key, rel in attempts[latest] do
                    let destination = OutputWorkspacePrefix + rel
                    let! restored = restoreOneAsync blobStore key destination workspace cancellationToken

                    if restored then
                        outputCount <- outputCount + 1

            return inputCount, outputCount
        }

// ──────────────────────────────────────────────────────────────────────────
// Tracker

/// One tracked binding: the bound workspace plus when it was last active
/// on the tracker's clock.
type internal TrackedWorkspace =
    {
        /// The bound workspace the tracker may dispose once idle.
        Workspace: IWorkspace
        /// When the binding last saw activity (track or touch).
        LastActive: DateTimeOffset
    }

/// Tracks bound workspaces per session on the injected clock so the expiry
/// service can tear down idle execution vehicles. Track on bind, Touch on
/// every prompt, and sweep with TeardownIdleAsync: disposing destroys only
/// the execution vehicle (a container or process), never the workspace's
/// files, and the next Bind re-binds over the same state. A racing Track
/// wins over a concurrent teardown: the entry is removed before its
/// workspace is disposed, so a freshly tracked binding is never disposed
/// by a pass that already passed it.
type internal SessionWorkspaceTracker(clock: TimeProvider) =

    do ArgumentNullException.ThrowIfNull(clock)

    let entries = ConcurrentDictionary<TenantId * SessionId, TrackedWorkspace>()

    /// Tracks the workspace bound for a session, stamping now as its last
    /// activity. Replaces any previous binding without disposing it: the
    /// caller owns the replaced workspace.
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session the workspace was bound for.</param>
    /// <param name="workspace">The bound workspace. Must not be null.</param>
    member _.Track(tenant: TenantId, sessionId: SessionId, workspace: IWorkspace) : unit =
        if isNull (box workspace) then
            raise (ArgumentNullException(nameof workspace))

        entries[(tenant, sessionId)] <-
            {
                Workspace = workspace
                LastActive = clock.GetUtcNow()
            }

    /// Marks a tracked session active now; untracked sessions are ignored
    /// so prompts for sessions with no bound workspace stay a no-op.
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to mark active.</param>
    member _.Touch(tenant: TenantId, sessionId: SessionId) : unit =
        match entries.TryGetValue((tenant, sessionId)) with
        | true, tracked ->
            entries[(tenant, sessionId)] <-
                { tracked with
                    LastActive = clock.GetUtcNow()
                }
        | false, _ -> ()

    /// How many workspaces are currently tracked.
    member _.Count: int = entries.Count

    /// Disposes every workspace idle at least <paramref name="idleAfter" />
    /// on the tracker's clock and untracks it. A non-positive idle span
    /// tears down nothing: the configured
    /// <see cref="P:Legate.WorkspaceOptions.IdleTeardownAfter" /> is always
    /// positive, so such a call is a host bug, not an eager sweep.
    /// <param name="idleAfter">How long a binding may sit idle before its vehicle is torn down.</param>
    /// <param name="cancellationToken">Abandons the sweep.</param>
    /// <returns>How many workspaces were disposed.</returns>
    member _.TeardownIdleAsync(idleAfter: TimeSpan, cancellationToken: CancellationToken) : Task<int> =
        task {
            cancellationToken.ThrowIfCancellationRequested()

            if idleAfter <= TimeSpan.Zero then
                return 0
            else
                let now = clock.GetUtcNow()
                let mutable tornDown = 0

                for pair in entries.ToArray() do
                    cancellationToken.ThrowIfCancellationRequested()

                    if now - pair.Value.LastActive >= idleAfter then
                        match entries.TryRemove(pair.Key) with
                        | true, tracked ->
                            do! tracked.Workspace.DisposeAsync().AsTask()
                            tornDown <- tornDown + 1
                        | false, _ -> ()

                return tornDown
        }
