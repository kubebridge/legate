// SPDX-License-Identifier: Apache-2.0
namespace Legate.Workspace.Process

open System
open System.IO
open System.Threading
open System.Threading.Tasks
open Legate
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Logging

/// Options for the process workspace runtime: where sessions' scratch
/// directories live and how long an exec may run when the caller passes no
/// timeout. A mutable options class, mirroring
/// <see cref="T:Legate.SessionOptions" />, so C# hosts configure through
/// object initialisers.
type ProcessWorkspaceRuntimeOptions() =

    /// The absolute directory the runtime creates sessions' scratch
    /// directories under. Must not be null or empty.
    member val Root: string | null = null with get, set

    /// How long <see cref="M:Legate.IWorkspace.Exec*" /> may run a command
    /// when the caller passes no timeout, or empty (HasValue is false)
    /// meaning commands may run unbounded.
    member val DefaultExecTimeout: Nullable<TimeSpan> = Nullable<TimeSpan>() with get, set

/// The workspace runtime on a per-session scratch directory with host-shell
/// exec and no sandbox: <see cref="M:Legate.IWorkspaceRuntime.Bind*" />
/// creates (or re-binds over) the session's directory under the configured
/// root, creating <c>input/</c>, <c>output/</c>, and <c>scratch/</c>, and
/// returns a <see cref="T:Legate.Workspace.Process.ProcessWorkspace" />.
/// There is no implicit fallback: a bind failure throws
/// <see cref="T:Legate.WorkspaceException" />.
///
/// <para>This runtime is unsafe for untrusted agents: the shell runs as the
/// host process's user with the host's environment. The constructor logs a
/// warning when the injected
/// <see cref="T:Microsoft.Extensions.Hosting.IHostEnvironment" /> is absent
/// or names an environment other than Development; hosts that construct the
/// runtime directly receive that warning only, so the XML docs repeat the
/// label.</para>
[<Sealed>]
module internal UnsafeLabelModule =

    /// The unsafe label the constructor logs when the host environment is
    /// absent or not Development.
    let unsafeLabel =
        "Legate.Workspace.Process runs commands through the host shell without a sandbox; it is unsafe for untrusted agents."

/// The workspace runtime on a per-session scratch directory with host-shell
/// exec and no sandbox: Bind creates (or re-binds over) the session's
/// directory under the configured root and returns a ProcessWorkspace.
[<Sealed>]
type ProcessWorkspaceRuntime
    (options: ProcessWorkspaceRuntimeOptions, environment: IHostEnvironment | null, logger: ILogger | null) =

    do
        if isNull (box options) then
            raise (ArgumentNullException(nameof options))

        if String.IsNullOrWhiteSpace options.Root then
            raise (ArgumentException("The process workspace runtime requires a root directory.", nameof options))

    do
        let isDevelopment =
            match environment with
            | null -> false
            | hostEnvironment -> hostEnvironment.IsDevelopment()

        if not isDevelopment then
            match logger with
            | null ->
                // No logger: nothing to log into; the XML docs and this
                // label carry the warning.
                ()
            | log -> log.LogWarning UnsafeLabelModule.unsafeLabel

    let root =
        match options.Root with
        | null -> raise (ArgumentException("The process workspace runtime requires a root directory.", nameof options))
        | value -> Path.GetFullPath value

    let runtimeId = "workspace.process"

    /// The workspace directory for one session: the session's opaque
    /// <see cref="P:Legate.Session.WorkspaceBinding" /> when it names a
    /// confined relative directory, otherwise the derived directory name.
    /// A binding that escapes the root fails loudly with
    /// <see cref="T:Legate.WorkspaceException" />.
    member runtime.DirectoryOf(session: Session) : string =
        match session.WorkspaceBinding with
        | null ->
            // Derived: tenant- and session-keyed, one directory per
            // session, stable across re-binds so workspace state survives
            // between the session's turns.
            sprintf "%s_%s" (session.Tenant.Value.Replace('/', '-')) session.Id.Value
        | binding ->
            if
                String.IsNullOrWhiteSpace binding
                || binding.StartsWith('/')
                || binding.StartsWith('\\')
                || (binding.Length >= 2 && binding[1] = ':')
                || binding.Contains("..")
                || binding.Contains('\\')
                || binding.Contains('\u0000')
                || binding = "."
                || binding.EndsWith('/')
            then
                raise (
                    WorkspaceException(
                        binding,
                        "The session's workspace binding must be a confined relative directory name."
                    )
                )

            binding
        |> fun name -> Path.GetFullPath(Path.Combine(root, name))

    /// Creates the session's directory layout, or re-binds over existing
    /// state; the failure surfaces as <see cref="T:Legate.WorkspaceException" />
    /// with no fallback.
    member runtime.CreateDirectoryLayout(session: Session) : IWorkspace =
        let directory = runtime.DirectoryOf session

        try
            // The layout the file tools expect; existing state
            // survives, so re-binding over a used directory keeps
            // the session's files.
            Directory.CreateDirectory directory |> ignore
            Directory.CreateDirectory(Path.Combine(directory, "input")) |> ignore
            Directory.CreateDirectory(Path.Combine(directory, "output")) |> ignore
            Directory.CreateDirectory(Path.Combine(directory, "scratch")) |> ignore

            new ProcessWorkspace(runtimeId, directory, options.DefaultExecTimeout) :> IWorkspace
        with
        | :? WorkspaceException -> reraise ()
        | error ->
            raise (
                WorkspaceException(
                    directory,
                    sprintf
                        "The process workspace could not bind the session's directory; the host reported: %s."
                        error.Message
                )
            )

    interface IWorkspaceRuntime with

        member runtime.Bind(session, _, cancellationToken) =
            if isNull (box session) then
                raise (ArgumentNullException(nameof session))

            cancellationToken.ThrowIfCancellationRequested()

            Task.FromResult(runtime.CreateDirectoryLayout session)

        member _.CheckReadiness(_) =
            task {
                try
                    let probe =
                        Path.Combine(root, sprintf ".legate-ready-%s" (Ulid.NewUlid().ToString()))

                    do! File.WriteAllTextAsync(probe, "ready")
                    File.Delete probe

                    return WorkspaceReadiness(true, null)
                with error ->
                    return
                        WorkspaceReadiness(
                            false,
                            sprintf "The workspace root is not writable; the host reported: %s." error.Message
                        )
            }
