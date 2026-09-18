// SPDX-License-Identifier: Apache-2.0
namespace Legate.Workspace.Docker

open System
open System.Collections.Generic
open System.IO
open System.Threading
open System.Threading.Tasks
open Legate

// The bound Docker workspace: confined file operations over the mounted
// host directory and exec through docker exec into the session's
// container. Every file member validates the workspace-relative path
// against the blob-key rules first (aligned with the HostDirectory rule
// set, not a rival one), then canonicalises post-symlink and requires
// containment within the canonical host root. Dispose is idempotent,
// destroys only the container (best-effort stop/rm under a bounded
// timeout), never deletes the workspace's files, and makes every member
// throw ObjectDisposedException once disposed. Every Docker failure
// surfaces as WorkspaceException: there is no host fallback.
//
// Residuals documented, not fixed in scope: validate-then-act TOCTOU
// between the containment check and the IO, and unbounded captured exec
// output.

module internal DockerWorkspacePaths =

    /// How many symbolic-link hops the canonicaliser follows before it
    /// rejects the path: loops and runaway chains surface as
    /// <see cref="T:Legate.WorkspaceException" /> instead of hanging.
    let maxLinkSteps = 40

    /// Validates a workspace-relative path against the blob-key rules and
    /// returns it unchanged: non-null, non-empty, no rooted paths or drive
    /// prefixes, no leading or trailing slash, no '.' or '..' segments, no
    /// backslashes, and no NUL characters. All violations are
    /// <see cref="T:Legate.WorkspaceException" /> so callers cannot reach
    /// outside the workspace root.
    /// <param name="path">The path to validate.</param>
    /// <returns>The validated path, unchanged.</returns>
    /// <exception cref="T:Legate.WorkspaceException">The path violates the workspace path rules.</exception>
    let validate (path: string) : string =
        if isNull (box path) then
            raise (ArgumentNullException(nameof path))

        if path = "" then
            raise (WorkspaceException(path, "A workspace path must not be empty."))

        if path.Length >= 2 && path[1] = ':' then
            raise (WorkspaceException(path, "A workspace path must not be a rooted path or carry a drive prefix."))

        if path.StartsWith('/') || path.StartsWith('\\') then
            raise (WorkspaceException(path, "A workspace path must be relative, without a leading slash."))

        if path.EndsWith('/') then
            raise (WorkspaceException(path, "A workspace path must not end with a slash."))

        let segments = path.Split('/')

        for segment in segments do
            if segment = "" then
                raise (WorkspaceException(path, "A workspace path must not contain empty segments."))

            if segment = "." || segment = ".." then
                raise (WorkspaceException(path, "A workspace path must not contain '.' or '..' segments."))

            if segment.Contains('\\') then
                raise (WorkspaceException(path, "A workspace path must not contain backslashes."))

            if segment.Contains('\u0000') then
                raise (WorkspaceException(path, "A workspace path must not contain NUL characters."))

        path

    /// Validates a workspace binding against the confined-relative-directory
    /// rules and returns it unchanged: non-null, non-empty, no rooted paths
    /// or drive prefixes, no leading or trailing slash, no '.' or '..'
    /// segments, no backslashes, and no NUL characters.
    /// <param name="binding">The binding to validate.</param>
    /// <returns>The validated binding, unchanged.</returns>
    /// <exception cref="T:Legate.WorkspaceException">The binding violates the confinement rules.</exception>
    let validateBinding (binding: string) : string =
        if isNull (box binding) then
            raise (ArgumentNullException(nameof binding))

        if String.IsNullOrWhiteSpace binding then
            raise (WorkspaceException(binding, "A workspace binding must not be empty."))

        if binding.Length >= 2 && binding[1] = ':' then
            raise (
                WorkspaceException(binding, "A workspace binding must not be a rooted path or carry a drive prefix.")
            )

        if binding.StartsWith('/') || binding.StartsWith('\\') then
            raise (WorkspaceException(binding, "A workspace binding must be relative, without a leading slash."))

        if binding.EndsWith('/') then
            raise (WorkspaceException(binding, "A workspace binding must not end with a slash."))

        let segments = binding.Split('/')

        for segment in segments do
            if segment = "" then
                raise (WorkspaceException(binding, "A workspace binding must not contain empty segments."))

            if segment = "." || segment = ".." then
                raise (WorkspaceException(binding, "A workspace binding must not contain '.' or '..' segments."))

            if segment.Contains('\\') then
                raise (WorkspaceException(binding, "A workspace binding must not contain backslashes."))

            if segment.Contains('\u0000') then
                raise (WorkspaceException(binding, "A workspace binding must not contain NUL characters."))

        binding

    /// Canonicalises a workspace root once, at construction: the absolute
    /// path without a trailing separator (except a filesystem root, which
    /// keeps its own form), so the containment check compares exact paths
    /// on a separator boundary instead of string prefixes.
    /// <param name="root">The absolute workspace root directory.</param>
    /// <returns>The canonical root, without a trailing separator.</returns>
    let canonicalizeRoot (root: string) : string =
        let full = Path.GetFullPath root

        let trimmed =
            full.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)

        if trimmed = "" then full else trimmed

    let private pathComparison =
        if OperatingSystem.IsWindows() then
            StringComparison.OrdinalIgnoreCase
        else
            StringComparison.Ordinal

    /// Reports whether a canonical candidate path is the canonical root
    /// itself or sits under it on a directory-separator boundary, so a
    /// sibling such as "<c>root-evil</c>" never counts as contained.
    /// <param name="canonicalRoot">The canonical workspace root.</param>
    /// <param name="candidate">The canonical candidate path.</param>
    /// <returns>true when the candidate is the root or under it; otherwise false.</returns>
    let isWithin (canonicalRoot: string) (candidate: string) : bool =
        if String.Equals(canonicalRoot, candidate, pathComparison) then
            true
        else
            candidate.StartsWith(canonicalRoot + string Path.DirectorySeparatorChar, pathComparison)

    let private linkTargetOf (path: string) : string | null =
        let probe (info: FileSystemInfo) : string | null =
            try
                info.LinkTarget
            with
            | :? FileNotFoundException -> null
            | :? DirectoryNotFoundException -> null
            | error ->
                raise (
                    WorkspaceException(
                        path,
                        sprintf "The workspace path could not be resolved; the host reported: %s." error.Message
                    )
                )

        let directoryTarget = probe (DirectoryInfo(path) :> FileSystemInfo)

        if isNull (box directoryTarget) then
            probe (FileInfo(path) :> FileSystemInfo)
        else
            directoryTarget

    /// Canonicalises a combined absolute path post-symlink: the deepest
    /// existing-or-link ancestor is resolved through its link chain, then
    /// each remaining segment is appended and resolved in turn. Loops,
    /// over-long chains, and unresolvable paths raise
    /// <see cref="T:Legate.WorkspaceException" />.
    /// <param name="combined">The absolute path to canonicalise.</param>
    /// <returns>The canonical absolute path, with all links resolved.</returns>
    /// <exception cref="T:Legate.WorkspaceException">The path loops, exceeds the hop cap, or cannot be resolved.</exception>
    let resolveCanonical (combined: string) : string =
        let visited = HashSet<string>()
        let mutable steps = 0

        let normaliseKey (path: string) =
            if OperatingSystem.IsWindows() then
                path.ToUpperInvariant()
            else
                path

        let rec hop (current: string) : string =
            match linkTargetOf current with
            | null -> Path.GetFullPath current
            | target ->
                steps <- steps + 1

                if steps > maxLinkSteps then
                    raise (
                        WorkspaceException(
                            current,
                            "The workspace path could not be resolved; too many symbolic-link levels."
                        )
                    )

                if not (visited.Add(normaliseKey current)) then
                    raise (
                        WorkspaceException(
                            current,
                            "The workspace path could not be resolved; symbolic links form a loop."
                        )
                    )

                let parent =
                    match Path.GetDirectoryName current with
                    | null -> current
                    | directory -> directory

                let next =
                    if Path.IsPathRooted target then
                        target
                    else
                        Path.Combine(parent, target)

                hop next

        let anchor =
            match Path.GetPathRoot combined with
            | null -> raise (WorkspaceException(combined, "The workspace path could not be resolved; it has no root."))
            | root -> root

        let remainder = combined.Substring(anchor.Length)

        let segments =
            remainder.Split(
                [|
                    Path.DirectorySeparatorChar
                    Path.AltDirectorySeparatorChar
                |],
                StringSplitOptions.RemoveEmptyEntries
            )

        let mutable current = Path.GetFullPath anchor

        for segment in segments do
            current <- hop (Path.Combine(current, segment))

        Path.GetFullPath current

    /// Validates a workspace-relative path, canonicalises it post-symlink,
    /// and requires containment within the canonical root. Violations raise
    /// <see cref="T:Legate.WorkspaceException" /> before any IO runs.
    /// <param name="canonicalRoot">The canonical workspace root.</param>
    /// <param name="root">The absolute workspace root directory.</param>
    /// <param name="path">The workspace-relative path.</param>
    /// <returns>The canonical absolute path to act on.</returns>
    /// <exception cref="T:Legate.WorkspaceException">The path is invalid, unresolvable, or escapes the root.</exception>
    let resolveContained (canonicalRoot: string) (root: string) (path: string) : string =
        let validated = validate path

        let combined =
            Path.GetFullPath(Path.Combine(root, validated.Replace('/', Path.DirectorySeparatorChar)))

        let canonical = resolveCanonical combined

        if isWithin canonicalRoot canonical then
            canonical
        else
            raise (WorkspaceException(validated, "The workspace path escapes the workspace root."))

    /// Validates a workspace binding as a confined relative sub-path,
    /// canonicalises it post-symlink, and requires containment within the
    /// canonical root.
    /// <param name="canonicalRoot">The canonical workspace root.</param>
    /// <param name="root">The absolute workspace root directory.</param>
    /// <param name="binding">The validated workspace binding.</param>
    /// <returns>The canonical absolute directory to bind.</returns>
    /// <exception cref="T:Legate.WorkspaceException">The binding is invalid, unresolvable, or escapes the root.</exception>
    let resolveBinding (canonicalRoot: string) (root: string) (binding: string) : string =
        let validated = validateBinding binding

        let combined =
            Path.GetFullPath(Path.Combine(root, validated.Replace('/', Path.DirectorySeparatorChar)))

        let canonical = resolveCanonical combined

        if isWithin canonicalRoot canonical then
            canonical
        else
            raise (WorkspaceException(validated, "The session's workspace binding escapes the workspace root."))

    /// Ensures host directory traversal for the session's container user
    /// (for example <c>65534:65534</c>): on non-Windows, every directory
    /// from the start up to the filesystem root that lacks other-execute
    /// gains it (search-only), never other-read, so the chain is
    /// traversable but not listable. Host-written content files keep only
    /// the read bits the write path already adds; the 0600 env file never
    /// flows through here. Per-directory failures are swallowed so a
    /// non-owned system ancestor can never break a bind: any residual gap
    /// surfaces later through the container's own error.
    /// <param name="startDirectory">The deepest host directory to fix.</param>
    let ensureTraversableChain (startDirectory: string) : unit =
        if not (OperatingSystem.IsWindows()) then
            try
                let mutable current: string | null = Path.GetFullPath startDirectory

                while not (isNull current) do
                    match current with
                    | null -> ()
                    | directory ->
                        try
                            let mode = File.GetUnixFileMode directory

                            if not (mode.HasFlag UnixFileMode.OtherExecute) then
                                File.SetUnixFileMode(directory, mode ||| UnixFileMode.OtherExecute)
                        with _ ->
                            ()

                        current <- Path.GetDirectoryName directory
            with _ ->
                ()

module internal DockerWorkspaceWrite =

    /// The host IO failure translated into
    /// <see cref="T:Legate.WorkspaceException" /> for the write path.
    let writeFailed (path: string) (error: exn) : exn =
        WorkspaceException(path, sprintf "The workspace could not write the file; the host reported: %s." error.Message)

    /// Writes content to the destination through a sibling temp file with
    /// an atomic replace, translating host IO failures into
    /// <see cref="T:Legate.WorkspaceException" />. Cancellation propagates
    /// as-is, the temp file is cleaned up on every failure path. On Linux
    /// the final file gains read for all so the session's container user
    /// (for example <c>65534:65534</c>) can read host-written files through
    /// the mount; the 0600 env file path never flows through here.
    let atomically (path: string) (fullPath: string) (content: byte[]) (cancellationToken: CancellationToken) : Task =
        task {
            match Path.GetDirectoryName fullPath with
            | null -> ()
            | directory ->
                if not (Directory.Exists directory) then
                    Directory.CreateDirectory directory |> ignore

                // Later-created parents inherit the runner umask too: the
                // container user needs search on them exactly as on the
                // session directory the bind fixed.
                DockerWorkspacePaths.ensureTraversableChain directory

            let tempPath = sprintf "%s.legate-tmp-%s" fullPath (Ulid.NewUlid().ToString())

            let mutable failure: exn | null = null

            try
                use stream =
                    new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None)

                do! stream.WriteAsync(content, 0, content.Length, cancellationToken)
                do! stream.FlushAsync(cancellationToken)
            with
            | :? OperationCanceledException as cancelled -> failure <- cancelled
            | error -> failure <- writeFailed path error

            if isNull (box failure) then
                try
                    File.Move(tempPath, fullPath, overwrite = true)

                    if not (OperatingSystem.IsWindows()) then
                        let current = File.GetUnixFileMode fullPath

                        let readable =
                            current
                            ||| (UnixFileMode.UserRead ||| UnixFileMode.GroupRead ||| UnixFileMode.OtherRead)

                        File.SetUnixFileMode(fullPath, readable)
                with error ->
                    failure <- writeFailed path error

            if not (isNull (box failure)) then
                try
                    File.Delete tempPath
                with _ ->
                    ()

            match failure with
            | null -> ()
            | error -> return raise error
        }

/// <summary>
/// The workspace bound by
/// <see cref="T:Legate.Workspace.Docker.DockerWorkspaceRuntime" />:
/// confined file operations over the mounted host directory and
/// <see cref="M:Legate.IWorkspace.Exec*" /> through <c>docker exec</c>
/// into the session's container. Disposing the workspace stops and
/// removes only the container (best-effort, under a bounded timeout),
/// never the workspace's files: the runtime may re-bind over the same
/// directory later.
/// </summary>
/// <remarks>
/// <para>Standard output and standard error are captured without a cap, so
/// a command that prints without bound can balloon memory; the runtime
/// never logs captured output, command lines, or environment values.</para>
/// <para>Confinement is validate-then-act: the containment check and the IO
/// are two steps, so a concurrent rename or link swap between them can move
/// the target. The workspace never takes locks; hosts that need stronger
/// guarantees must quiesce writers first.</para>
/// </remarks>
[<Sealed>]
type DockerWorkspace
    internal
    (
        runtimeId: string,
        containerName: string,
        hostDirectory: string,
        runner: IDockerCommandRunner,
        defaultExecTimeout: Nullable<TimeSpan>
    ) =

    do
        if String.IsNullOrWhiteSpace runtimeId then
            raise (ArgumentException("A workspace root must name its owning runtime.", nameof runtimeId))

        if String.IsNullOrWhiteSpace containerName then
            raise (ArgumentException("A Docker workspace must name its container.", nameof containerName))

        if isNull (box hostDirectory) then
            raise (ArgumentNullException(nameof hostDirectory))

        if not (Path.IsPathRooted hostDirectory) then
            raise (ArgumentException("A workspace host directory must be an absolute path.", nameof hostDirectory))

        if isNull (box runner) then
            raise (ArgumentNullException(nameof runner))

    let root = Path.GetFullPath hostDirectory
    let canonicalRoot = DockerWorkspacePaths.canonicalizeRoot root
    let defaultExecTimeout = defaultExecTimeout

    let mutable disposed = 0
    let gate = obj ()

    let throwIfDisposed (operation: string) =
        lock gate (fun () -> disposed) |> ignore

        if disposed <> 0 then
            raise (
                ObjectDisposedException(
                    nameof DockerWorkspace,
                    sprintf "The workspace is disposed; %s is no longer available." operation
                )
            )

    let isMissingContainer (stderr: string) =
        stderr.Contains("No such container", StringComparison.OrdinalIgnoreCase)
        || stderr.Contains("is not running", StringComparison.OrdinalIgnoreCase)

    let execAsync
        (command: string)
        (timeout: Nullable<TimeSpan>)
        (env: IReadOnlyDictionary<string, string> | null)
        (cancellationToken: CancellationToken)
        : Task<WorkspaceExecResult> =
        throwIfDisposed "exec"

        if isNull (box command) then
            raise (ArgumentNullException(nameof command))

        if timeout.HasValue && timeout.Value <= TimeSpan.Zero then
            raise (ArgumentOutOfRangeException(nameof timeout, "The exec timeout must be positive when set."))

        task {
            let effectiveTimeout =
                if timeout.HasValue then
                    Nullable timeout.Value
                else
                    defaultExecTimeout

            let args = ResizeArray<string>()
            args.Add "exec" |> ignore

            let pairs =
                match env with
                | null -> Seq.empty
                | present -> present :> seq<KeyValuePair<string, string>>

            for KeyValue(key, value) in pairs do
                if isNull (box key) then
                    raise (ArgumentException("An environment variable name must not be null.", nameof env))

                // Values ride as one argv element each; they are never
                // logged and never embedded in exception messages.
                args.Add "-e" |> ignore
                args.Add(sprintf "%s=%s" key value) |> ignore

            args.Add containerName |> ignore
            args.Add "/bin/sh" |> ignore
            args.Add "-c" |> ignore
            args.Add command |> ignore

            let! outcome = runner.RunAsync(args :> IReadOnlyList<string>, effectiveTimeout, cancellationToken)

            if outcome.ExitCode <> 0 && isMissingContainer outcome.Stderr then
                raise (
                    WorkspaceException(
                        root,
                        "The workspace command could not run: the session's container is missing or stopped."
                    )
                )

            return WorkspaceExecResult(outcome.ExitCode, outcome.Stdout, outcome.Stderr, outcome.TimedOut)
        }

    // Best-effort, bounded container teardown: stop, then remove. Every
    // failure is swallowed so dispose never throws; the session-scoped
    // name bounds the blast radius to this session's container.
    let destroyContainerAsync () : Task =
        task {
            let stopArgs: IReadOnlyList<string> = [| "stop"; "-t"; "5"; containerName |]

            try
                let! _ = runner.RunAsync(stopArgs, Nullable(TimeSpan.FromSeconds 15.0), CancellationToken.None)
                ()
            with _ ->
                ()

            let rmArgs: IReadOnlyList<string> = [| "rm"; containerName |]

            try
                let! _ = runner.RunAsync(rmArgs, Nullable(TimeSpan.FromSeconds 15.0), CancellationToken.None)
                ()
            with _ ->
                ()
        }

    /// The session-scoped container this workspace executes in.
    member internal _.ContainerName: string = containerName

    /// The mounted host directory file operations act through.
    member internal _.HostDirectory: string = root

    interface IWorkspace with

        member _.Root: WorkspaceRoot = WorkspaceRoot(runtimeId, "/workspace")

        member workspace.Exec(command, timeout, env, cancellationToken) =
            execAsync command timeout env cancellationToken

        member workspace.Exists(path, cancellationToken) =
            throwIfDisposed "Exists"

            let fullPath = DockerWorkspacePaths.resolveContained canonicalRoot root path

            task {
                cancellationToken.ThrowIfCancellationRequested()

                try
                    return File.Exists fullPath
                with error ->
                    return
                        raise (
                            WorkspaceException(
                                root,
                                sprintf "The workspace could not probe the path; the host reported: %s." error.Message
                            )
                        )
            }

        member workspace.ReadFile(path, cancellationToken) =
            throwIfDisposed "ReadFile"

            let fullPath = DockerWorkspacePaths.resolveContained canonicalRoot root path

            task {
                cancellationToken.ThrowIfCancellationRequested()

                if not (File.Exists fullPath) then
                    raise (FileNotFoundException("No file exists at this workspace path.", path))

                try
                    return new FileStream(fullPath, FileMode.Open, FileAccess.Read, FileShare.Read) :> Stream
                with error ->
                    return
                        raise (
                            WorkspaceException(
                                root,
                                sprintf "The workspace could not open the file; the host reported: %s." error.Message
                            )
                        )
            }

        member workspace.WriteFile(path, content, cancellationToken) =
            throwIfDisposed "WriteFile"

            let fullPath = DockerWorkspacePaths.resolveContained canonicalRoot root path

            if isNull (box content) then
                raise (ArgumentNullException(nameof content))

            task {
                cancellationToken.ThrowIfCancellationRequested()
                do! DockerWorkspaceWrite.atomically path fullPath content cancellationToken
            }

        member workspace.DeleteFile(path, cancellationToken) =
            throwIfDisposed "DeleteFile"

            let fullPath = DockerWorkspacePaths.resolveContained canonicalRoot root path

            task {
                cancellationToken.ThrowIfCancellationRequested()

                try
                    if File.Exists fullPath then
                        File.Delete fullPath
                        return true
                    else
                        return false
                with error ->
                    return
                        raise (
                            WorkspaceException(
                                root,
                                sprintf "The workspace could not delete the file; the host reported: %s." error.Message
                            )
                        )
            }

        member _.DisposeAsync() =
            let already =
                lock gate (fun () ->
                    let seen = disposed
                    disposed <- 1
                    seen)

            if already <> 0 then
                ValueTask.CompletedTask
            else
                destroyContainerAsync () |> ValueTask

    interface IDisposable with
        member this.Dispose() =
            (this :> IAsyncDisposable).DisposeAsync().AsTask().Wait()
