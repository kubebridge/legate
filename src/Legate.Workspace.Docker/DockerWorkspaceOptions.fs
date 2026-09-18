// SPDX-License-Identifier: Apache-2.0
namespace Legate.Workspace.Docker

open System

// Settings for the Docker workspace runtime, bound from the
// Legate:Workspace:Docker configuration section. A plain class with mutable
// properties and defaults, so hosts set properties before registering and
// the configuration binder only overrides the keys the host sets. Secrets
// never travel on these options: per-command environment rides on
// IWorkspace.Exec, never on the container options, and is never logged.

/// <summary>
/// Where the Docker workspace runtime binds sessions and how it shapes
/// each session's container: the host directory sessions bind under, the
/// image every container starts from, and the applied container limits
/// (network, CPUs, memory, user) plus the default exec timeout. Bound
/// from the <c>Legate:Workspace:Docker</c> configuration section.
/// </summary>
type DockerWorkspaceOptions() =

    /// <summary>
    /// The configuration section the runtime binds from:
    /// <c>Legate:Workspace:Docker</c>.
    /// </summary>
    static member ConfigurationSectionPath = "Legate:Workspace:Docker"

    /// <summary>
    /// The absolute host directory sessions bind under. Each session binds
    /// a confined sub-path of this directory (or the directory itself when
    /// the session carries no workspace binding), and that host path is
    /// mounted into the session's container. Must not be null or empty.
    /// </summary>
    member val Root: string | null = null with get, set

    /// <summary>
    /// The image every session container starts from, for example
    /// <c>alpine:3.20</c>. Must be a non-empty image reference.
    /// </summary>
    member val Image: string = "" with get, set

    /// <summary>
    /// The Docker network the session containers attach to, for example
    /// <c>none</c> or <c>bridge</c>, or null to leave the daemon default
    /// in place. When set it must be non-empty and is passed as
    /// <c>--network</c> on container creation.
    /// </summary>
    member val Network: string | null = null with get, set

    /// <summary>
    /// The CPU limit passed as <c>--cpus</c> on container creation, or
    /// empty (HasValue is false) meaning no CPU limit. When set it must
    /// be positive.
    /// </summary>
    member val CpuLimit: Nullable<float> = Nullable<float>() with get, set

    /// <summary>
    /// The memory limit passed as <c>--memory</c> on container creation
    /// (for example <c>512m</c>), or null meaning no memory limit. When
    /// set it must be non-empty.
    /// </summary>
    member val MemoryLimit: string | null = null with get, set

    /// <summary>
    /// The user the session containers run as (for example
    /// <c>1000:1000</c>), or null to run as the image default. When set
    /// it must be non-empty and is passed as <c>--user</c> on container
    /// creation.
    /// </summary>
    member val User: string | null = null with get, set

    /// <summary>
    /// How long <see cref="M:Legate.IWorkspace.Exec*" /> may run a command
    /// when the caller passes no timeout, or empty (HasValue is false)
    /// meaning commands may run unbounded.
    /// </summary>
    member val DefaultExecTimeout: Nullable<TimeSpan> = Nullable<TimeSpan>() with get, set

    /// <summary>
    /// Checks the options: the root must be non-empty, the image must be
    /// a non-empty reference, the network/user/memory limits must be
    /// non-empty when set, the CPU limit must be positive when set, and
    /// the default exec timeout must be positive when set.
    /// </summary>
    /// <returns>Null when the options are valid; otherwise the reason they are not.</returns>
    member this.Validate() : string | null =
        if String.IsNullOrWhiteSpace this.Root then
            "DockerWorkspaceOptions.Root must be a non-empty host directory."
        elif String.IsNullOrWhiteSpace this.Image then
            "DockerWorkspaceOptions.Image must be a non-empty image reference."
        elif not (isNull (box this.Network)) && String.IsNullOrWhiteSpace this.Network then
            "DockerWorkspaceOptions.Network must be non-empty when set: use null for the daemon default."
        elif this.CpuLimit.HasValue && not (this.CpuLimit.Value > 0.0) then
            "DockerWorkspaceOptions.CpuLimit must be positive when set."
        elif
            not (isNull (box this.MemoryLimit))
            && String.IsNullOrWhiteSpace this.MemoryLimit
        then
            "DockerWorkspaceOptions.MemoryLimit must be non-empty when set: use null for no limit."
        elif not (isNull (box this.User)) && String.IsNullOrWhiteSpace this.User then
            "DockerWorkspaceOptions.User must be non-empty when set: use null for the image default."
        elif
            this.DefaultExecTimeout.HasValue
            && this.DefaultExecTimeout.Value <= TimeSpan.Zero
        then
            "DockerWorkspaceOptions.DefaultExecTimeout must be positive when set."
        else
            null
