// SPDX-License-Identifier: Apache-2.0
namespace Legate.Workspace.Docker

open System
open System.Collections.Generic
open System.Diagnostics
open System.Threading
open System.Threading.Tasks

// The injectable seam over the docker CLI: every docker invocation flows
// through IDockerCommandRunner, so unit tests assert arg shapes and failure
// mapping against a fake with no daemon. The default implementation spawns
// the docker CLI via ProcessStartInfo + ArgumentList (the HostDirectory
// startShell pattern), captures both streams, honours the timeout and the
// caller's cancellation by killing the CLI process tree, and never logs
// argument values (environment values may ride on docker exec -e).

/// The outcome of one docker CLI invocation.
type internal DockerCliResult =
    {
        /// The CLI process exit code.
        ExitCode: int
        /// Everything the CLI wrote to stdout; never null.
        Stdout: string
        /// Everything the CLI wrote to stderr; never null.
        Stderr: string
        /// Whether the runner killed the CLI because it exceeded the
        /// invocation timeout (false when the caller's cancellation did).
        TimedOut: bool
    }

/// Runs one docker CLI invocation: the argument vector (without the
/// leading docker executable), the invocation timeout (or empty for
/// unbounded), and the caller's cancellation token.
type internal IDockerCommandRunner =
    abstract RunAsync:
        args: IReadOnlyList<string> * timeout: Nullable<TimeSpan> * cancellationToken: CancellationToken ->
            Task<DockerCliResult>

/// The default runner: spawns the docker CLI with ArgumentList, drains
/// both streams concurrently (a full pipe would deadlock the wait), and
/// kills the CLI process tree on timeout or cancellation. Start failures
/// surface as WorkspaceException; a non-zero exit is a plain result the
/// caller maps (callers decide which exits are failures).
type internal DockerCommandRunner() =

    static let dockerFileName = "docker"

    static let killTree (cli: Process) =
        try
            cli.Kill(entireProcessTree = true)
        with :? InvalidOperationException ->
            // The CLI already exited between the wait and the kill;
            // nothing left to terminate.
            ()

    interface IDockerCommandRunner with
        member _.RunAsync(args, timeout, cancellationToken) =
            if isNull (box args) then
                raise (ArgumentNullException(nameof args))

            if timeout.HasValue && timeout.Value <= TimeSpan.Zero then
                raise (
                    ArgumentOutOfRangeException(
                        nameof timeout,
                        "The docker invocation timeout must be positive when set."
                    )
                )

            task {
                let startInfo = ProcessStartInfo()
                startInfo.FileName <- dockerFileName

                for arg in args do
                    startInfo.ArgumentList.Add arg

                startInfo.UseShellExecute <- false
                startInfo.RedirectStandardOutput <- true
                startInfo.RedirectStandardError <- true
                startInfo.CreateNoWindow <- true

                let cli =
                    try
                        match Process.Start startInfo with
                        | null ->
                            raise (
                                Legate.WorkspaceException(
                                    dockerFileName,
                                    "The workspace could not start the docker CLI; the host returned no process."
                                )
                            )
                        | started -> started
                    with
                    | :? Legate.WorkspaceException -> reraise ()
                    | error ->
                        raise (
                            Legate.WorkspaceException(
                                dockerFileName,
                                sprintf
                                    "The workspace could not start the docker CLI; the host reported: %s."
                                    error.Message
                            )
                        )

                use _cli = cli

                let stdoutTask: Task<string> = cli.StandardOutput.ReadToEndAsync()
                let stderrTask: Task<string> = cli.StandardError.ReadToEndAsync()

                use linked = CancellationTokenSource.CreateLinkedTokenSource cancellationToken

                if timeout.HasValue then
                    linked.CancelAfter timeout.Value

                use _killRegistration = linked.Token.Register(fun () -> killTree cli)

                try
                    do! cli.WaitForExitAsync(linked.Token)
                with :? OperationCanceledException ->
                    // The CLI was killed by the timeout or the caller's
                    // cancellation; the exit code below is the OS-reported
                    // value of the killed process.
                    ()

                let timedOut =
                    linked.IsCancellationRequested && not cancellationToken.IsCancellationRequested

                // Belt and braces: the tree is down whatever the exit path.
                killTree cli

                let! stdout = stdoutTask
                let! stderr = stderrTask

                let! _ = cli.WaitForExitAsync(CancellationToken.None)

                // A caller cancellation (not a timeout) propagates as
                // cancellation, matching the HostDirectory exec contract.
                if linked.IsCancellationRequested && cancellationToken.IsCancellationRequested then
                    cancellationToken.ThrowIfCancellationRequested()

                return
                    {
                        ExitCode = cli.ExitCode
                        Stdout = stdout
                        Stderr = stderr
                        TimedOut = timedOut
                    }
            }
