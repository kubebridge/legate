// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Generic
open System.IO
open System.Threading
open System.Threading.Tasks

// Workspace contracts. IWorkspaceRuntime is how a session binds a workspace
// and how a host asks whether the runtime can serve; IWorkspace is the bound
// workspace a turn's tools run in: a root path abstraction, command execution
// with timeout and environment, and the confined file operations the file
// tools consume (they never touch the file system directly). The contracts
// are BCL-only; no runtime lives here — the host directory, Docker, and
// scratch process runtimes are their own packages. Semantics pinned on the
// interface docs: binding is stable per session (workspace state survives
// between turns of the same session, and the session's opaque workspace
// binding is honoured when present), there is no implicit fallback (a bind
// failure throws WorkspaceException), and idle teardown destroys only the
// execution vehicle, never the workspace's files, with the next bind
// re-binding over the same state.

/// Per-runtime workspace options. A mutable base class so hosts derive
/// per-runtime subclasses in their own packages (a future
/// <c>DockerWorkspaceOptions</c>, for instance) and configure them through
/// object initialisers, mirroring <see cref="T:Legate.SessionOptions" />.
/// <see cref="M:Legate.IWorkspaceRuntime.Bind*" /> accepts null options
/// meaning the runtime's configured defaults.
type WorkspaceOptions() =

    /// How long a workspace may sit idle before the runtime tears it down,
    /// or empty (HasValue is false) meaning the runtime default from
    /// configuration, mirroring <see cref="P:Legate.SessionOptions.Timeout" />.
    /// Idle teardown destroys only the execution vehicle, never the
    /// workspace's files; the next bind re-binds over the same state.
    member val IdleTeardownAfter: Nullable<TimeSpan> = Nullable<TimeSpan>() with get, set

/// The root of a bound workspace: the abstraction
/// <see cref="T:Legate.IWorkspace" /> hands out instead of a host filesystem
/// path. <see cref="P:Legate.WorkspaceRoot.Path" /> is the path inside the
/// runtime's execution context (a host directory, a container path, or a
/// scratch directory) and is null when the runtime cannot expose one;
/// <see cref="P:Legate.WorkspaceRoot.RuntimeId" /> names the owning runtime
/// for diagnostics. Never build host paths from
/// <see cref="P:Legate.WorkspaceRoot.Path" />: file access goes through
/// <see cref="T:Legate.IWorkspace" />.
/// <param name="runtimeId">The id of the runtime that owns the workspace. Must be non-null and non-empty.</param>
/// <param name="path">The workspace root path inside the runtime's execution context, or null when the runtime cannot expose one.</param>
[<Sealed>]
type WorkspaceRoot(runtimeId: string, path: string | null) =

    do
        if isNull (box runtimeId) then
            raise (ArgumentNullException(nameof runtimeId))

        if String.IsNullOrWhiteSpace runtimeId then
            raise (ArgumentException("A workspace root must name its owning runtime.", nameof runtimeId))

    /// The id of the runtime that owns the workspace; for diagnostics and
    /// reporting, never for dispatch.
    member _.RuntimeId: string = runtimeId

    /// The workspace root path inside the runtime's execution context (a
    /// host directory, a container path, or a scratch directory), or null
    /// when the runtime cannot expose one.
    member _.Path: string | null = path

/// Whether a workspace runtime can serve sessions right now, reported by
/// <see cref="M:Legate.IWorkspaceRuntime.CheckReadiness*" />. An unready
/// runtime is a result, not an exception: the cause travels on
/// <see cref="P:Legate.WorkspaceReadiness.Reason" /> (the Docker daemon is
/// unreachable, the directory is not writable) and never embeds secrets.
/// <param name="isReady">Whether the runtime can serve sessions.</param>
/// <param name="reason">A human-readable cause for being unready, or null when ready.</param>
[<Sealed>]
type WorkspaceReadiness(isReady: bool, reason: string | null) =

    /// Whether the runtime can serve sessions right now.
    member _.IsReady: bool = isReady

    /// A human-readable cause for being unready (an unreachable Docker
    /// daemon, a non-writable directory), or null when the runtime is
    /// ready. Never embeds secrets.
    member _.Reason: string | null = reason

/// The outcome of one command execution in a workspace, returned by
/// <see cref="M:Legate.IWorkspace.Exec*" />. Standard output and standard
/// error are never null: pass the empty string when the runtime captured
/// nothing; null arguments are rejected.
/// <param name="exitCode">The process exit code the runtime observed.</param>
/// <param name="standardOutput">Everything the process wrote to stdout. Must not be null; pass the empty string when nothing was captured.</param>
/// <param name="standardError">Everything the process wrote to stderr. Must not be null; pass the empty string when nothing was captured.</param>
/// <param name="timedOut">Whether the runtime killed the process because it exceeded the execution timeout.</param>
[<Sealed>]
type WorkspaceExecResult(exitCode: int, standardOutput: string, standardError: string, timedOut: bool) =

    do
        if isNull (box standardOutput) then
            raise (ArgumentNullException(nameof standardOutput))

        if isNull (box standardError) then
            raise (ArgumentNullException(nameof standardError))

    /// The process exit code the runtime observed. Not meaningful when
    /// <see cref="P:Legate.WorkspaceExecResult.TimedOut" /> is true: it is
    /// then the OS-reported value for the killed process and must not be
    /// used to decide anything.
    member _.ExitCode: int = exitCode

    /// Everything the process wrote to stdout; never null (the empty string
    /// when nothing was captured).
    member _.StandardOutput: string = standardOutput

    /// Everything the process wrote to stderr; never null (the empty string
    /// when nothing was captured).
    member _.StandardError: string = standardError

    /// Whether the runtime killed the process because it exceeded the
    /// execution timeout.
    member _.TimedOut: bool = timedOut

// ───────────────────────────────────────────────────────────────────────────
// The workspace contracts

/// One bound workspace: the execution context a turn's tools run in. File
/// tools consume this interface; they never touch the file system directly,
/// and every path they pass is relative to
/// <see cref="P:Legate.IWorkspace.Root" />.
///
/// Every path argument to the file operations is a workspace-relative,
/// slash-separated path: no rooted paths or drive prefixes, no leading or
/// trailing slash, no <c>.</c> or <c>..</c> segments, no backslashes, and no
/// NUL characters (the blob-key rules); the empty string is not a path.
/// Implementations validate before touching storage and throw
/// <see cref="T:Legate.WorkspaceException" /> on violation, so a call can
/// never reach outside the workspace.
///
/// Disposing the workspace ends the binding and tears down the execution
/// vehicle (a container or process) without deleting the workspace's files.
/// Dispose is idempotent; every member throws
/// <see cref="T:System.ObjectDisposedException" /> once disposed.
type IWorkspace =
    inherit IAsyncDisposable

    /// The root of the workspace the runtime bound.
    abstract Root: WorkspaceRoot

    /// Executes a command in the workspace and waits for it to finish.
    /// <param name="command">The command line to execute, resolved inside the workspace's execution context.</param>
    /// <param name="timeout">How long the command may run before the runtime kills it, or empty (HasValue is false) meaning the runtime's configured default.</param>
    /// <param name="env">Extra environment variables for the command, or null to inherit the execution context's environment. Values may contain secrets and are never logged.</param>
    /// <param name="cancellationToken">Token that abandons the execution.</param>
    /// <returns>The command's exit code, output streams, and whether it timed out.</returns>
    /// <exception cref="T:Legate.WorkspaceException">The command could not be started or the execution failed at the infrastructure level.</exception>
    abstract Exec:
        command: string *
        timeout: Nullable<TimeSpan> *
        env: IReadOnlyDictionary<string, string> | null *
        cancellationToken: CancellationToken ->
            Task<WorkspaceExecResult>

    /// Reports whether a file exists in the workspace.
    /// <param name="path">The workspace-relative file path.</param>
    /// <param name="cancellationToken">Token that abandons the probe.</param>
    /// <returns>true when the file exists; otherwise false.</returns>
    /// <exception cref="T:Legate.WorkspaceException">The path violates the workspace path rules.</exception>
    abstract Exists: path: string * cancellationToken: CancellationToken -> Task<bool>

    /// Opens a read stream over a workspace file. Throws
    /// <see cref="T:System.IO.FileNotFoundException" /> when the file does
    /// not exist (the <see cref="M:Legate.IBlobStore.OpenRead*" /> precedent).
    /// <param name="path">The workspace-relative file path.</param>
    /// <param name="cancellationToken">Token that abandons the open.</param>
    /// <returns>A stream over the file's bytes.</returns>
    /// <exception cref="T:System.IO.FileNotFoundException">The file does not exist.</exception>
    /// <exception cref="T:Legate.WorkspaceException">The path violates the workspace path rules.</exception>
    abstract ReadFile: path: string * cancellationToken: CancellationToken -> Task<Stream>

    /// Writes a workspace file, atomically replacing any previous content:
    /// readers never see a partial file, and an existing file is always
    /// overwritten (there is no create-only mode).
    /// <param name="path">The workspace-relative file path.</param>
    /// <param name="content">The bytes to write.</param>
    /// <param name="cancellationToken">Token that abandons the write.</param>
    /// <returns>A task that completes once the file is fully written.</returns>
    /// <exception cref="T:Legate.WorkspaceException">The path violates the workspace path rules or the write failed.</exception>
    abstract WriteFile: path: string * content: byte[] * cancellationToken: CancellationToken -> Task

    /// Deletes a workspace file.
    /// <param name="path">The workspace-relative file path.</param>
    /// <param name="cancellationToken">Token that abandons the deletion.</param>
    /// <returns>true when the file was deleted; false when it did not exist.</returns>
    /// <exception cref="T:Legate.WorkspaceException">The path violates the workspace path rules or the deletion failed.</exception>
    abstract DeleteFile: path: string * cancellationToken: CancellationToken -> Task<bool>

/// Binds and tears down workspaces for sessions; the contract the runtime
/// packages implement (host directory, Docker, scratch process). A runtime
/// is a host hook: hosts select it explicitly, and there is no implicit
/// fallback — a runtime that cannot bind throws
/// <see cref="T:Legate.WorkspaceException" />.
///
/// Binding is stable per session: workspace state survives between turns of
/// the same session. When the session carries a workspace binding (the
/// opaque <see cref="P:Legate.Session.WorkspaceBinding" /> reference) it is
/// honoured: Bind binds over that binding rather than minting a fresh
/// workspace.
///
/// Idle teardown is the runtime's job, driven by
/// <see cref="P:Legate.WorkspaceOptions.IdleTeardownAfter" />: the runtime may
/// dispose an idle <see cref="T:Legate.IWorkspace" /> after that span, which
/// destroys only the execution vehicle (a container or process), never the
/// workspace root's files; the next
/// <see cref="M:Legate.IWorkspaceRuntime.Bind*" /> re-binds over the same
/// state.
type IWorkspaceRuntime =

    /// Binds the workspace the session's turns execute in. Null
    /// <paramref name="options" /> means the runtime's configured defaults.
    /// <param name="session">The session to bind a workspace for. Its <see cref="P:Legate.Session.WorkspaceBinding" /> is honoured when present.</param>
    /// <param name="options">The per-runtime options, or null for the runtime's configured defaults.</param>
    /// <param name="cancellationToken">Token that abandons the bind.</param>
    /// <returns>The bound workspace.</returns>
    /// <exception cref="T:Legate.WorkspaceException">The runtime cannot bind: the failure surfaces, it never falls back to another runtime.</exception>
    abstract Bind:
        session: Session * options: WorkspaceOptions | null * cancellationToken: CancellationToken -> Task<IWorkspace>

    /// Reports whether the runtime can serve sessions right now (for the
    /// Docker runtime, whether the daemon is reachable; for a directory
    /// runtime, whether the directory is writable). An unready runtime is a
    /// result, not an exception.
    /// <param name="cancellationToken">Token that abandons the probe.</param>
    /// <returns>The readiness snapshot.</returns>
    abstract CheckReadiness: cancellationToken: CancellationToken -> Task<WorkspaceReadiness>
