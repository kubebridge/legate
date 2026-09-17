// SPDX-License-Identifier: Apache-2.0
namespace Legate.Workspace.HostDirectory

open System
open System.IO
open System.Threading
open System.Threading.Tasks
open Legate
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Logging

/// Options for the host-directory workspace runtime: which host directory
/// sessions bind to, whether file operations may resolve outside it, and
/// how long an exec may run when the caller passes no timeout. A mutable
/// options class, mirroring <see cref="T:Legate.SessionOptions" />, so C#
/// hosts configure through object initialisers.
type HostDirectoryWorkspaceRuntimeOptions() =

    /// The absolute host directory sessions bind to. Must not be null or
    /// empty. Without a session binding the workspace is this directory
    /// itself; with one it is the binding as a confined relative sub-path.
    member val Root: string | null = null with get, set

    /// Whether file operations may act on paths that canonicalise outside
    /// the root (through a symbolic link, for example). An explicit opt-in
    /// for hosts that want it; default false. Blob-key validation still
    /// applies, and the session's workspace binding stays confined whatever
    /// this flag says.
    member val AllowOutsideRoot: bool = false with get, set

    /// How long <see cref="M:Legate.IWorkspace.Exec*" /> may run a command
    /// when the caller passes no timeout, or empty (HasValue is false)
    /// meaning commands may run unbounded.
    member val DefaultExecTimeout: Nullable<TimeSpan> = Nullable<TimeSpan>() with get, set

/// The workspace runtime bound to one host directory with host-shell exec
/// and no sandbox: <see cref="M:Legate.IWorkspaceRuntime.Bind*" /> binds
/// the configured directory itself (or the session's binding as a confined
/// relative sub-path under it) and returns a
/// <see cref="T:Legate.Workspace.HostDirectory.HostDirectoryWorkspace" />.
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
        "Legate.Workspace.HostDirectory runs commands through the host shell without a sandbox; it is unsafe for untrusted agents."

/// The workspace runtime bound to one host directory with host-shell exec
/// and no sandbox: Bind binds the configured directory (or the session's
/// confined binding under it) and returns a HostDirectoryWorkspace.
[<Sealed>]
type HostDirectoryWorkspaceRuntime
    (options: HostDirectoryWorkspaceRuntimeOptions, environment: IHostEnvironment | null, logger: ILogger | null) =

    do
        if isNull (box options) then
            raise (ArgumentNullException(nameof options))

        if String.IsNullOrWhiteSpace options.Root then
            raise (ArgumentException("The host directory workspace runtime requires a root directory.", nameof options))

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
        | null ->
            raise (ArgumentException("The host directory workspace runtime requires a root directory.", nameof options))
        | value -> Path.GetFullPath value

    let canonicalRoot = HostDirectoryWorkspacePaths.canonicalizeRoot root
    let allowOutsideRoot = options.AllowOutsideRoot
    let runtimeId = "workspace.host-directory"

    /// The workspace directory for one session: the configured host
    /// directory itself when the session carries no workspace binding,
    /// otherwise the binding as a confined relative sub-path under it. A
    /// binding that escapes the root (including through a symbolic link)
    /// fails loudly with <see cref="T:Legate.WorkspaceException" />, even
    /// when <c>AllowOutsideRoot</c> opts the file operations out.
    member runtime.DirectoryOf(session: Session) : string =
        if isNull (box session) then
            raise (ArgumentNullException(nameof session))

        match session.WorkspaceBinding with
        | null -> canonicalRoot
        | binding -> HostDirectoryWorkspacePaths.resolveBinding canonicalRoot root binding

    /// Binds the session's directory, creating it when missing and
    /// re-binding over existing state otherwise; the failure surfaces as
    /// <see cref="T:Legate.WorkspaceException" /> with no fallback.
    member runtime.CreateDirectoryLayout(session: Session) : IWorkspace =
        let directory = runtime.DirectoryOf session

        try
            // Existing state survives, so re-binding over a used directory
            // keeps the session's files. The binding was
            // containment-checked before this create, so a pre-existing
            // escaping link cannot divert the directory creation.
            Directory.CreateDirectory directory |> ignore

            new HostDirectoryWorkspace(runtimeId, directory, allowOutsideRoot, options.DefaultExecTimeout) :> IWorkspace
        with
        | :? WorkspaceException -> reraise ()
        | error ->
            raise (
                WorkspaceException(
                    directory,
                    sprintf
                        "The host directory workspace could not bind the session's directory; the host reported: %s."
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
