// SPDX-License-Identifier: Apache-2.0
namespace Legate.Workspace.Docker

open System
open System.Collections.Generic
open System.IO
open System.Threading
open System.Threading.Tasks
open Legate
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Logging

// The container-per-session workspace runtime over the docker CLI: Bind
// creates (or re-binds over) the session's host directory, starts one
// session-scoped container with the configured image and applied limits,
// and returns a DockerWorkspace that execs through docker exec and reads
// files through the mounted host directory. Every failure surfaces as
// WorkspaceException with no host fallback.

/// The warning the constructor logs when the host environment is absent
/// or not Development.
[<Sealed>]
module internal DockerUnsafeLabelModule =

    /// The label the constructor logs when the host environment is absent
    /// or not Development.
    let label =
        "Legate.Workspace.Docker executes commands inside containers; ensure the configured image and mounts are trusted."

module internal DockerContainerNames =

    /// The longest container name the runtime mints: session-scoped names
    /// stay short enough for the daemon to accept.
    let maxLength = 63

    /// Sanitises one name fragment: lowercase, alphanumerics kept, every
    /// other character a dash, leading and trailing dashes trimmed.
    /// <param name="value">The fragment to sanitise.</param>
    /// <returns>The sanitised fragment, or "session" when nothing survives.</returns>
    let sanitize (value: string) : string =
        let lowered = if isNull (box value) then "" else value.ToLowerInvariant()

        let chars =
            lowered.ToCharArray()
            |> Array.map (fun c -> if Char.IsLetterOrDigit c then c else '-')
            |> String

        let trimmed = chars.Trim('-')

        if String.IsNullOrEmpty trimmed then "session" else trimmed

    /// Mints the session-scoped container name: legate, the tenant, and
    /// the session id, sanitised and bounded so parallel sessions never
    /// share a container and a failure cleans up only its own vehicle.
    /// <param name="tenant">The tenant value the session belongs to.</param>
    /// <param name="sessionId">The session id value.</param>
    /// <returns>The session-scoped container name.</returns>
    let containerNameFor (tenant: string) (sessionId: string) : string =
        let combined = sprintf "legate-%s-%s" (sanitize tenant) (sanitize sessionId)

        let bounded =
            if combined.Length > maxLength then
                combined.Substring(0, maxLength)
            else
                combined

        let trimmed = bounded.Trim('-')

        if String.IsNullOrEmpty trimmed then
            "legate-session"
        else
            trimmed

/// <summary>
/// The workspace runtime that binds one container per session over the
/// <c>docker</c> CLI: <see cref="M:Legate.IWorkspaceRuntime.Bind*" />
/// binds the session's confined host directory, starts the session-scoped
/// container from the configured image with the applied network, CPU,
/// memory, and user limits, and returns a
/// <see cref="T:Legate.Workspace.Docker.DockerWorkspace" />. There is no
/// implicit fallback: every failure throws
/// <see cref="T:Legate.WorkspaceException" />.
/// </summary>
[<Sealed>]
type DockerWorkspaceRuntime
    internal
    (
        options: DockerWorkspaceOptions,
        runner: IDockerCommandRunner,
        environment: IHostEnvironment | null,
        logger: ILogger | null
    ) =

    do
        if isNull (box options) then
            raise (ArgumentNullException(nameof options))

        if isNull (box runner) then
            raise (ArgumentNullException(nameof runner))

        match options.Validate() with
        | null -> ()
        | violation -> raise (ArgumentException(violation, nameof options))

    do
        let isDevelopment =
            match environment with
            | null -> false
            | hostEnvironment -> hostEnvironment.IsDevelopment()

        if not isDevelopment then
            match logger with
            | null -> ()
            | log -> log.LogWarning DockerUnsafeLabelModule.label

    let root =
        match options.Root with
        | null -> raise (ArgumentException("The Docker workspace runtime requires a root directory.", nameof options))
        | value -> Path.GetFullPath value

    let canonicalRoot = DockerWorkspacePaths.canonicalizeRoot root
    let runtimeId = "workspace.docker"

    static let readinessTimeout = Nullable(TimeSpan.FromSeconds 10.0)
    static let stopTimeout = Nullable(TimeSpan.FromSeconds 15.0)
    static let rmTimeout = Nullable(TimeSpan.FromSeconds 15.0)
    static let runTimeout = Nullable(TimeSpan.FromMinutes 2.0)

    // Writes the container env file under the session's host directory:
    // empty content, owner-only on Linux, never logged, always deleted by
    // the caller in a finally. A permission failure throws before any
    // container starts.
    let writeEnvFile (directory: string) : string =
        let path =
            Path.Combine(directory, sprintf ".legate-env-%s" (Ulid.NewUlid().ToString()))

        File.WriteAllBytes(path, [||])

        if not (OperatingSystem.IsWindows()) then
            File.SetUnixFileMode(path, UnixFileMode.UserRead ||| UnixFileMode.UserWrite)

        path

    let deleteQuietly (path: string) =
        try
            if File.Exists path then
                File.Delete path
        with _ ->
            ()

    // Best-effort, bounded container teardown after a failed bind: stop,
    // then remove. Every failure is swallowed; the session-scoped name
    // bounds the blast radius to this session's container.
    let cleanupContainerAsync (containerName: string) : Task =
        task {
            let stopArgs: IReadOnlyList<string> = [| "stop"; "-t"; "5"; containerName |]

            try
                let! _ = runner.RunAsync(stopArgs, stopTimeout, CancellationToken.None)
                ()
            with _ ->
                ()

            let rmArgs: IReadOnlyList<string> = [| "rm"; containerName |]

            try
                let! _ = runner.RunAsync(rmArgs, rmTimeout, CancellationToken.None)
                ()
            with _ ->
                ()
        }

    let runArgsOrThrow
        (args: IReadOnlyList<string>)
        (containerName: string)
        (operation: string)
        : Task<DockerCliResult> =
        task {
            let! outcome = runner.RunAsync(args, runTimeout, CancellationToken.None)

            if outcome.ExitCode <> 0 then
                let detail =
                    if String.IsNullOrWhiteSpace outcome.Stderr then
                        sprintf "the docker CLI exited with code %d." outcome.ExitCode
                    else
                        sprintf
                            "the docker CLI exited with code %d; it reported: %s."
                            outcome.ExitCode
                            (outcome.Stderr.Trim())

                raise (
                    WorkspaceException(containerName, sprintf "The Docker workspace could not %s; %s" operation detail)
                )

            return outcome
        }

    /// <summary>
    /// Creates the runtime over the default docker CLI runner.
    /// </summary>
    /// <param name="options">The container options. Must validate.</param>
    /// <param name="environment">The host environment, or null when unavailable.</param>
    /// <param name="logger">The logger receiving the non-Development warning, or null for no logging.</param>
    new(options: DockerWorkspaceOptions, environment: IHostEnvironment | null, logger: ILogger | null) =
        DockerWorkspaceRuntime(options, DockerCommandRunner() :> IDockerCommandRunner, environment, logger)

    /// <summary>
    /// The workspace directory for one session: the configured host
    /// directory itself when the session carries no workspace binding,
    /// otherwise the binding as a confined relative sub-path under it. A
    /// binding that escapes the root fails loudly with
    /// <see cref="T:Legate.WorkspaceException" />.
    /// </summary>
    /// <param name="session">The session to resolve the directory for.</param>
    /// <returns>The canonical absolute host directory to mount.</returns>
    member _.DirectoryOf(session: Session) : string =
        if isNull (box session) then
            raise (ArgumentNullException(nameof session))

        match session.WorkspaceBinding with
        | null -> canonicalRoot
        | binding -> DockerWorkspacePaths.resolveBinding canonicalRoot root binding

    /// <summary>
    /// The session-scoped container name for one session: derived from the
    /// tenant and session id, sanitised and bounded, stable across
    /// re-binds so workspace state survives between the session's turns.
    /// </summary>
    /// <param name="session">The session to name the container for.</param>
    /// <returns>The session-scoped container name.</returns>
    member _.ContainerNameOf(session: Session) : string =
        if isNull (box session) then
            raise (ArgumentNullException(nameof session))

        DockerContainerNames.containerNameFor session.Tenant.Value session.Id.Value

    /// <summary>
    /// Binds the session's host directory, starts (or reuses) its
    /// session-scoped container with the configured image and applied
    /// limits, and returns the bound workspace. The failure surfaces as
    /// <see cref="T:Legate.WorkspaceException" /> with no fallback; a
    /// failed bind stops and removes the session's container
    /// (best-effort, bounded) while the host directory's files survive.
    /// </summary>
    /// <param name="session">The session to bind a workspace for.</param>
    /// <param name="cancellationToken">Token that abandons the bind.</param>
    /// <returns>The bound workspace.</returns>
    member runtime.CreateContainerLayout(session: Session, cancellationToken: CancellationToken) : Task<IWorkspace> =
        if isNull (box session) then
            raise (ArgumentNullException(nameof session))

        task {
            cancellationToken.ThrowIfCancellationRequested()

            let directory = runtime.DirectoryOf session
            let containerName = runtime.ContainerNameOf session

            try
                Directory.CreateDirectory directory |> ignore
            with error ->
                raise (
                    WorkspaceException(
                        directory,
                        sprintf
                            "The Docker workspace could not bind the session's directory; the host reported: %s."
                            error.Message
                    )
                )

            let envFile =
                try
                    writeEnvFile directory
                with error ->
                    raise (
                        WorkspaceException(
                            directory,
                            sprintf
                                "The Docker workspace could not prepare the session's environment; the host reported: %s."
                                error.Message
                        )
                    )

            try
                try
                    // Reuse when the session's container already exists:
                    // a running one is kept, a stopped one is started.
                    let inspectArgs: IReadOnlyList<string> = [| "inspect"; containerName |]
                    let! inspect = runner.RunAsync(inspectArgs, runTimeout, cancellationToken)

                    if inspect.ExitCode = 0 then
                        let runningArgs: IReadOnlyList<string> =
                            [|
                                "inspect"
                                "-f"
                                "{{.State.Running}}"
                                containerName
                            |]

                        let! running = runner.RunAsync(runningArgs, runTimeout, cancellationToken)

                        if running.ExitCode <> 0 then
                            raise (
                                WorkspaceException(
                                    containerName,
                                    "The Docker workspace could not inspect the session's container; the docker CLI reported an error."
                                )
                            )

                        if not (running.Stdout.Trim().Equals("true", StringComparison.OrdinalIgnoreCase)) then
                            let startArgs: IReadOnlyList<string> = [| "start"; containerName |]
                            let! _ = runArgsOrThrow startArgs containerName "start the session's container"
                            ()
                    else
                        let create = ResizeArray<string>()
                        create.Add "run" |> ignore
                        create.Add "-d" |> ignore
                        create.Add "--name" |> ignore
                        create.Add containerName |> ignore

                        match options.Network with
                        | null -> ()
                        | network ->
                            create.Add "--network" |> ignore
                            create.Add network |> ignore

                        if options.CpuLimit.HasValue then
                            create.Add "--cpus" |> ignore

                            create.Add(
                                options.CpuLimit.Value.ToString(System.Globalization.CultureInfo.InvariantCulture)
                            )
                            |> ignore

                        match options.MemoryLimit with
                        | null -> ()
                        | memory ->
                            create.Add "--memory" |> ignore
                            create.Add memory |> ignore

                        match options.User with
                        | null -> ()
                        | user ->
                            create.Add "--user" |> ignore
                            create.Add user |> ignore

                        create.Add "-v" |> ignore
                        create.Add(sprintf "%s:/workspace" directory) |> ignore
                        create.Add "-w" |> ignore
                        create.Add "/workspace" |> ignore
                        create.Add "--env-file" |> ignore
                        create.Add envFile |> ignore
                        create.Add options.Image |> ignore
                        create.Add "sleep" |> ignore
                        create.Add "infinity" |> ignore

                        let! _ =
                            runArgsOrThrow
                                (create :> IReadOnlyList<string>)
                                containerName
                                "create the session's container"

                        ()

                    return
                        new DockerWorkspace(runtimeId, containerName, directory, runner, options.DefaultExecTimeout)
                        :> IWorkspace
                with error ->
                    // A failed bind never leaks the vehicle: stop and remove
                    // best-effort under a bounded timeout, while the host
                    // directory's files survive. Cancellation propagates
                    // as-is; every other failure surfaces as
                    // WorkspaceException with no fallback.
                    do! cleanupContainerAsync containerName

                    match error with
                    | :? OperationCanceledException -> return raise error
                    | :? WorkspaceException -> return raise error
                    | other ->
                        return
                            raise (
                                WorkspaceException(
                                    containerName,
                                    sprintf
                                        "The Docker workspace could not bind the session; the host reported: %s."
                                        other.Message
                                )
                            )
            finally
                // The env file served its single creation: it is deleted on
                // every path (best-effort) so no 0600 file lingers and no
                // value it ever carried survives the bind.
                deleteQuietly envFile
        }

    interface IWorkspaceRuntime with

        member runtime.Bind(session, _, cancellationToken) =
            if isNull (box session) then
                raise (ArgumentNullException(nameof session))

            runtime.CreateContainerLayout(session, cancellationToken)

        member _.CheckReadiness(cancellationToken) =
            task {
                let probeArgs: IReadOnlyList<string> = [| "info" |]

                try
                    let! outcome = runner.RunAsync(probeArgs, readinessTimeout, cancellationToken)

                    if outcome.ExitCode = 0 then
                        return WorkspaceReadiness(true, null)
                    else
                        let detail =
                            if String.IsNullOrWhiteSpace outcome.Stderr then
                                sprintf "the docker CLI exited with code %d." outcome.ExitCode
                            else
                                outcome.Stderr.Trim()

                        return WorkspaceReadiness(false, sprintf "The Docker daemon is unreachable; %s" detail)
                with
                | :? OperationCanceledException as cancelled -> return raise cancelled
                | error ->
                    return
                        WorkspaceReadiness(
                            false,
                            sprintf "The Docker daemon is unreachable; the host reported: %s." error.Message
                        )
            }
