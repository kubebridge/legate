// SPDX-License-Identifier: Apache-2.0
namespace Legate.Workspace.Process

open System
open System.Collections.Generic
open System.Diagnostics
open System.IO
open System.Runtime.InteropServices
open System.Threading
open System.Threading.Tasks
open Legate

// The bound process workspace: confined file operations over one scratch
// directory and exec through the platform shell. The path validator
// reimplements the blob-key rules (they are private to Legate.Abstractions
// by design, so the package cannot reach them) and every member validates
// before touching the file system, so a call can never reach outside the
// workspace root. Dispose is idempotent, never deletes the workspace's
// files (the runtime may re-bind over the same directory later), and makes
// every member throw ObjectDisposedException once disposed.

module internal ProcessWorkspacePaths =

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

    /// Resolves a validated workspace-relative path under the root. The
    /// validator already rejected rooted, traversing, and backslash paths,
    /// so the join stays inside the root on every platform.
    /// <param name="root">The absolute workspace root directory.</param>
    /// <param name="path">The validated workspace-relative path.</param>
    /// <returns>The absolute path under the root.</returns>
    let resolve (root: string) (path: string) : string =
        Path.GetFullPath(Path.Combine(root, path.Replace('/', Path.DirectorySeparatorChar)))

/// The workspace bound by <see cref="T:Legate.Workspace.Process.ProcessWorkspaceRuntime" />:
/// confined file operations over the session's scratch directory and
/// <see cref="M:Legate.IWorkspace.Exec" /> through the platform shell
/// (<c>cmd.exe /d /s /c</c> on Windows, <c>/bin/sh -c</c> elsewhere), with
/// the inherited environment plus any injected variables and
/// <see cref="M:System.Diagnostics.Process.Kill(System.Boolean)" /> of the
/// shell's entire process tree on timeout or cancellation.
///
/// <para>Not a sandbox: the shell runs as the host process's user with the
/// host's environment, so the runtime is unsafe for untrusted agents. The
/// constructor logs a warning outside Development.</para>
///
/// <para>Standard output and standard error are captured without a cap, so a
/// command that prints without bound can balloon memory; the runtime never
/// logs captured output, command lines, or environment values.</para>
[<Sealed>]
type ProcessWorkspace internal (runtimeId: string, rootDirectory: string) =

    do
        if String.IsNullOrWhiteSpace runtimeId then
            raise (ArgumentException("A workspace root must name its owning runtime.", nameof runtimeId))

        if isNull (box rootDirectory) then
            raise (ArgumentNullException(nameof rootDirectory))

        if not (Path.IsPathRooted rootDirectory) then
            raise (ArgumentException("A workspace root directory must be an absolute path.", nameof rootDirectory))

    let root = Path.GetFullPath rootDirectory

    let mutable disposed = 0

    // One lock serialises the dispose transition against in-flight members;
    // file operations themselves rely on the OS for mutual exclusion.
    let gate = obj ()

    let throwIfDisposed (operation: string) =
        lock gate (fun () -> disposed) |> ignore

        if disposed <> 0 then
            raise (
                ObjectDisposedException(
                    nameof ProcessWorkspace,
                    sprintf "The workspace is disposed; %s is no longer available." operation
                )
            )

    /// Starts the shell with the workspace as its working directory and both
    /// streams redirected. The shell inherits the process's environment; the
    /// caller's injected variables are added on top, overriding on name
    /// collisions. Values may carry secrets and are never logged.
    let startShell (command: string) (env: IReadOnlyDictionary<string, string> | null) : Process =
        let startInfo = ProcessStartInfo()

        if RuntimeInformation.IsOSPlatform(OSPlatform.Windows) then
            let comspec = Environment.GetEnvironmentVariable "ComSpec"

            startInfo.FileName <-
                (match comspec with
                 | null -> "cmd.exe"
                 | value -> value)

            startInfo.Arguments <- sprintf "/d /s /c %s" command
        else
            startInfo.FileName <- "/bin/sh"
            startInfo.Arguments <- sprintf "-c %s" (command.Replace("'", "'\\''"))

        startInfo.WorkingDirectory <- root
        startInfo.UseShellExecute <- false
        startInfo.RedirectStandardOutput <- true
        startInfo.RedirectStandardError <- true
        startInfo.CreateNoWindow <- true

        if not (isNull (box env)) then
            for KeyValue(key, value) in env do
                startInfo.Environment[key] <- value

        try
            match Process.Start startInfo with
            | null ->
                raise (
                    WorkspaceException(
                        root,
                        "The workspace could not start a shell for the command; the host returned no process."
                    )
                )
            | started -> started
        with error ->
            raise (
                WorkspaceException(
                    root,
                    sprintf
                        "The workspace could not start a shell for the command; the shell reported: %s."
                        error.Message
                )
            )

    let killTree (shell: Process) =
        try
            shell.Kill(entireProcessTree = true)
        with :? InvalidOperationException ->
            // The shell already exited between the wait and the kill;
            // nothing left to terminate.
            ()

    /// Runs the shell to completion under the timeout and the cancellation
    /// token. The timeout and the caller's cancellation share one linked
    /// source: either path kills the shell's entire process tree, and the
    /// result reports the OS exit value of the killed shell with the
    /// timed-out flag set for the timeout path.
    let runShell
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
            let shell = startShell command env

            use _shell = shell

            // Drain both streams concurrently: a full pipe blocks a shell
            // writing more output, which would deadlock the wait.
            let stdoutTask: Task<string> = shell.StandardOutput.ReadToEndAsync()
            let stderrTask: Task<string> = shell.StandardError.ReadToEndAsync()

            let mutable timedOut = false

            use linked =
                let source = CancellationTokenSource.CreateLinkedTokenSource cancellationToken

                if timeout.HasValue then
                    let delay = Task.Delay(timeout.Value, CancellationToken.None)

                    delay.ContinueWith(fun (_: Task) ->
                        if delay.Status = TaskStatus.RanToCompletion then
                            timedOut <- true
                            source.Cancel())
                    |> ignore

                source

            use _killRegistration = linked.Token.Register(fun () -> killTree shell)

            try
                do! shell.WaitForExitAsync(linked.Token)
            with :? OperationCanceledException ->
                // The shell was killed by the timeout or the caller's
                // cancellation; the exit code below is the OS-reported
                // value of the killed process.
                ()

            // Belt and braces: the tree is down whatever the exit path.
            killTree shell

            let! stdout = stdoutTask
            let! stderr = stderrTask

            let! _ = shell.WaitForExitAsync(CancellationToken.None)

            return WorkspaceExecResult(shell.ExitCode, stdout, stderr, timedOut)
        }

    interface IWorkspace with

        member _.Root: WorkspaceRoot = WorkspaceRoot(runtimeId, root)

        member workspace.Exec(command, timeout, env, cancellationToken) =
            runShell command timeout env cancellationToken

        member workspace.Exists(path, cancellationToken) =
            throwIfDisposed "Exists"

            let path = ProcessWorkspacePaths.validate path

            task {
                cancellationToken.ThrowIfCancellationRequested()
                return File.Exists(ProcessWorkspacePaths.resolve root path)
            }

        member workspace.ReadFile(path, cancellationToken) =
            throwIfDisposed "ReadFile"

            let path = ProcessWorkspacePaths.validate path

            task {
                cancellationToken.ThrowIfCancellationRequested()

                let fullPath = ProcessWorkspacePaths.resolve root path

                if not (File.Exists fullPath) then
                    raise (FileNotFoundException("No file exists at this workspace path.", path))

                return new FileStream(fullPath, FileMode.Open, FileAccess.Read, FileShare.Read) :> Stream
            }

        member workspace.WriteFile(path, content, cancellationToken) =
            throwIfDisposed "WriteFile"

            let path = ProcessWorkspacePaths.validate path

            if isNull (box content) then
                raise (ArgumentNullException(nameof content))

            task {
                cancellationToken.ThrowIfCancellationRequested()

                let fullPath = ProcessWorkspacePaths.resolve root path

                // Directories first so the atomic replace below never races
                // a missing parent.
                match Path.GetDirectoryName fullPath with
                | null -> ()
                | directory when not (Directory.Exists directory) -> Directory.CreateDirectory directory |> ignore
                | _ -> ()

                // Atomic replace: write to a sibling temp file, close it,
                // then swap over the destination, so readers never see a
                // partial file and an existing file is always overwritten.
                let tempPath = sprintf "%s.legate-tmp-%s" fullPath (Ulid.NewUlid().ToString())

                do!
                    task {
                        use stream =
                            new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None)

                        do! stream.WriteAsync(content, 0, content.Length, cancellationToken)
                        do! stream.FlushAsync(cancellationToken)
                    }

                File.Move(tempPath, fullPath, overwrite = true)
            }

        member workspace.DeleteFile(path, cancellationToken) =
            throwIfDisposed "DeleteFile"

            let path = ProcessWorkspacePaths.validate path

            task {
                cancellationToken.ThrowIfCancellationRequested()

                let fullPath = ProcessWorkspacePaths.resolve root path

                if File.Exists fullPath then
                    File.Delete fullPath
                    return true
                else
                    return false
            }

        member workspace.DisposeAsync() =
            lock gate (fun () -> disposed <- 1)

            // No execution vehicle of our own to destroy (each exec's shell
            // is killed on timeout or cancellation), and the workspace's
            // files always survive dispose: the runtime may re-bind over
            // the same directory.
            ValueTask.CompletedTask

    interface IDisposable with
        member this.Dispose() =
            (this :> IAsyncDisposable).DisposeAsync().AsTask().Wait()
