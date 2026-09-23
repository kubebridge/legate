// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Threading
open System.Threading.Tasks

// Cluster bootstrap hook: the seam through which a discovery package forms
// the Akka cluster when ClusterMode.Kubernetes is selected. The core
// runtime owns the actor system lifecycle and the session shard region; the
// hook owns the management plus discovery fragment merged into the cluster
// HOCON and the extension start that joins the node through discovery
// instead of the singleton self-join. Public signatures stay BCL-only (no
// Akka types cross the boundary): the running actor system travels as
// obj and the implementation casts it. When no hook is registered the
// Kubernetes start branch keeps today's singleton behavior, which the
// hermetic cluster tests cover.

// How a clustered Legate host joins its Akka cluster through discovery.
// Implemented by the Kubernetes bootstrap package
// (Legate.Cluster.Kubernetes); hosts with their own discovery mechanism
// may implement it directly. Small on purpose: hosts never implement
// cluster internals through it.
type IClusterBootstrap =

    /// Builds the management plus discovery HOCON fragment the cluster
    /// start merges into the node configuration before the actor system
    /// is created, so invalid options fail before the cluster forms.
    /// <returns>The HOCON fragment merged into the cluster configuration.</returns>
    abstract BuildHocon: unit -> string

    /// Starts cluster formation against the running actor system: the
    /// management endpoint plus the discovery bootstrap join. Called once
    /// per start after the system is created; the runtime awaits it before
    /// hosting the session shard region.
    /// <param name="system">The running Akka actor system, as an untyped reference keeping Akka off the public surface.</param>
    /// <param name="cancellationToken">Abandons the bootstrap start.</param>
    /// <returns>A task that completes once cluster formation has started.</returns>
    abstract StartAsync: system: obj * cancellationToken: CancellationToken -> Task
