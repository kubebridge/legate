// SPDX-License-Identifier: Apache-2.0
namespace Legate.Cluster

open System
open System.Runtime.CompilerServices
open Legate
open Legate.Cluster.Kubernetes
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.DependencyInjection.Extensions
open Microsoft.Extensions.Options

// Host entry point for the Kubernetes bootstrap package. The extension
// methods live on the core Cluster builder so C# hosts chain them after
// AddLegate (F# hosts open this namespace first): registration, options,
// and fail-fast validation only. Starting cluster formation stays with
// the cluster hosted service, which consults the registered hook.

// Extension methods registering the Kubernetes cluster bootstrap on the
// Legate Cluster builder: <c>builder.Cluster.UseKubernetes(...)</c>.
[<Sealed; AbstractClass; Extension>]
type ClusterBuilderExtensions =

    /// Registers the Kubernetes bootstrap hook with default options:
    /// management on 8558, pods labelled app=legate in the ambient
    /// namespace, 2 required contact points. Fails fast listing the
    /// first invalid knob.
    /// <param name="builder">The Cluster builder to register the bootstrap on.</param>
    /// <returns>The same builder, for call chaining.</returns>
    [<Extension>]
    static member UseKubernetes(builder: ClusterBuilder) : ClusterBuilder =
        ArgumentNullException.ThrowIfNull(builder)
        ClusterBuilderExtensions.Register(KubernetesOptions(), builder.Services)
        builder

    /// Registers the Kubernetes bootstrap hook shaped by
    /// <paramref name="configure" />: the management endpoint plus the pod
    /// discovery knobs locating peer pods. Fails fast listing the first
    /// invalid knob, before the cluster forms.
    /// <param name="builder">The Cluster builder to register the bootstrap on.</param>
    /// <param name="configure">The transform shaping the bootstrap options. Must not be null.</param>
    /// <returns>The same builder, for call chaining.</returns>
    [<Extension>]
    static member UseKubernetes(builder: ClusterBuilder, configure: Action<KubernetesOptions>) : ClusterBuilder =
        ArgumentNullException.ThrowIfNull(builder)
        ArgumentNullException.ThrowIfNull(configure)

        let options = KubernetesOptions()
        configure.Invoke(options)
        ClusterBuilderExtensions.Register(options, builder.Services)
        builder

    /// Validates the options and registers them plus the bootstrap hook,
    /// replacing any previous bootstrap registration.
    /// <param name="options">The shaped bootstrap options.</param>
    /// <param name="services">The container receiving the registrations.</param>
    static member private Register(options: KubernetesOptions, services: IServiceCollection) : unit =
        ArgumentNullException.ThrowIfNull(options)
        ArgumentNullException.ThrowIfNull(services)

        match options.Validate() with
        | null -> ()
        | violation -> raise (InvalidOperationException($"Invalid Kubernetes bootstrap options: %s{violation}"))

        services.Replace(ServiceDescriptor.Singleton<IOptions<KubernetesOptions>>(Options.Create(options)))
        |> ignore

        services.Replace(
            ServiceDescriptor(typeof<IClusterBootstrap>, typeof<KubernetesClusterBootstrap>, ServiceLifetime.Singleton)
        )
        |> ignore
