// SPDX-License-Identifier: Apache-2.0
namespace Legate.Cluster.Kubernetes

open System
open System.Threading
open System.Threading.Tasks
open Akka.Actor
open Akka.Management.Cluster.Bootstrap
open Akka.Management.Dsl
open Legate
open Legate.Cluster
open Microsoft.Extensions.Options

// The IClusterBootstrap implementation UseKubernetes registers: renders
// the management plus discovery HOCON for the cluster start to merge, and
// starts Akka.Management plus the discovery bootstrap join against the
// running actor system. Internal so no Akka type ever crosses the public
// API; hosts only see IClusterBootstrap through the Cluster builder.

// Starts Akka.Management plus the Kubernetes discovery bootstrap against
// the running actor system. Internal so no Akka type ever crosses the
// public API.
// <param name="options">The Kubernetes bootstrap options.</param>
type internal KubernetesClusterBootstrap(options: IOptions<KubernetesOptions>) =

    do ArgumentNullException.ThrowIfNull(options)

    /// Reads the current options or fails fast before the cluster forms.
    /// <returns>The validated Kubernetes options.</returns>
    member private _.currentOptions() : KubernetesOptions =
        let current = options.Value

        ArgumentNullException.ThrowIfNull(current)

        match current.Validate() with
        | null -> current
        | violation -> raise (InvalidOperationException($"Invalid Kubernetes bootstrap options: %s{violation}"))

    interface IClusterBootstrap with

        member this.BuildHocon() : string =
            KubernetesHocon.buildFragment (this.currentOptions ())

        member this.StartAsync(system: obj, cancellationToken: CancellationToken) : Task =
            ArgumentNullException.ThrowIfNull(system)

            let typed =
                match system with
                | :? ActorSystem as actorSystem -> actorSystem
                | _ ->
                    raise (
                        ArgumentException("The bootstrap system must be the running Akka actor system.", nameof system)
                    )

            task {
                this.currentOptions () |> ignore
                let! _ = AkkaManagement.Get(typed).Start()
                do! ClusterBootstrap.Get(typed).Start()
                cancellationToken.ThrowIfCancellationRequested()
            }
            :> Task
