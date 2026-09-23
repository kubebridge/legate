// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.KubernetesHoconTests

open System
open Akka.Configuration
open FsUnit.Xunit
open Legate
open Legate.Cluster
open Legate.Cluster.Kubernetes
open Xunit

// Kubernetes bootstrap HOCON (issue 138): the management plus discovery
// fragment the cluster start merges into the node configuration. Hermetic:
// fragments only parse; nothing contacts Kubernetes.

// ──────────────────────────────────────────────────────────────────────────
// Fragment

[<Fact>]
let ``Fragment renders the documented management and discovery keys`` () =
    let options = KubernetesOptions()
    options.PodLabelSelector <- "app=legate"
    options.Namespace <- "legate-prod"
    options.PortName <- "management"
    options.ManagementPort <- 8558
    options.RequiredContactPoints <- 3

    let config = ConfigurationFactory.ParseString(KubernetesHocon.buildFragment options)

    config.GetString("akka.discovery.method") |> should equal "kubernetes-api"

    config.GetString("akka.discovery.kubernetes-api.class")
    |> should equal "Akka.Discovery.KubernetesApi.KubernetesApiServiceDiscovery, Akka.Discovery.KubernetesApi"

    config.GetString("akka.discovery.kubernetes-api.pod-label-selector")
    |> should equal "app=legate"

    config.GetString("akka.discovery.kubernetes-api.pod-namespace")
    |> should equal "legate-prod"

    config.GetString("akka.management.http.bind-hostname") |> should equal "0.0.0.0"
    config.GetInt("akka.management.http.port") |> should equal 8558

    config.GetString("akka.management.cluster.bootstrap.contact-point-discovery.discovery-method")
    |> should equal "kubernetes-api"

    config.GetString("akka.management.cluster.bootstrap.contact-point-discovery.port-name")
    |> should equal "management"

    config.GetInt("akka.management.cluster.bootstrap.contact-point-discovery.required-contact-point-nr")
    |> should equal 3

[<Fact>]
let ``Fragment omits the namespace when unset so discovery auto-detects it`` () =
    let raw = KubernetesHocon.buildFragment (KubernetesOptions())

    raw.Contains("pod-namespace") |> should equal false

    // Still parses: the omitted line leaves valid HOCON behind.
    let config = ConfigurationFactory.ParseString(raw)

    config.GetString("akka.discovery.kubernetes-api.pod-label-selector")
    |> should equal "app=legate"

[<Fact>]
let ``Fragment escapes quoted values instead of breaking HOCON`` () =
    let options = KubernetesOptions()
    options.PodLabelSelector <- "app=\"legate\""

    let raw = KubernetesHocon.buildFragment options

    let config = ConfigurationFactory.ParseString(raw)

    config.GetString("akka.discovery.kubernetes-api.pod-label-selector")
    |> should equal "app=\"legate\""

[<Fact>]
let ``Fragment rejects invalid options before the cluster forms`` () =
    let options = KubernetesOptions()
    options.ManagementPort <- 0

    let render () =
        KubernetesHocon.buildFragment options |> ignore

    let ex = Assert.Throws<InvalidOperationException>(render)
    ex.Message.Contains("ManagementPort") |> should equal true

[<Fact>]
let ``Public-hostname line renders the pod IP override`` () =
    let line = KubernetesHocon.buildPublicHostnameLine "10.0.0.7"

    let config = ConfigurationFactory.ParseString(line)

    config.GetString("akka.remote.dot-netty.tcp.public-hostname")
    |> should equal "10.0.0.7"

[<Fact>]
let ``Public-hostname line is empty when the pod IP is blank`` () =
    KubernetesHocon.buildPublicHostnameLine "   " |> should equal ""

[<Fact>]
let ``Public-hostname line is empty when the pod IP is unset`` () =
    KubernetesHocon.buildPublicHostnameLine null |> should equal ""

[<Fact>]
let ``Fragment merges with the core cluster HOCON into one parseable config`` () =
    let clusterOptions = ClusterOptions(Mode = ClusterMode.Kubernetes)
    clusterOptions.Roles.Add("session") |> ignore

    let raw =
        ClusterActorSystem.buildClusterHocon clusterOptions 0
        + "\n"
        + KubernetesHocon.buildFragment (KubernetesOptions())

    let config = ConfigurationFactory.ParseString(raw)

    config.GetString("akka.actor.provider") |> should equal "cluster"
    config.GetInt("akka.management.http.port") |> should equal 8558

    config.GetString("akka.discovery.kubernetes-api.pod-label-selector")
    |> should equal "app=legate"

[<Fact>]
let ``Merged config binds all interfaces while advertising the pod IP`` () =
    let clusterOptions = ClusterOptions(Mode = ClusterMode.Kubernetes)
    clusterOptions.Roles.Add("session") |> ignore

    let raw =
        ClusterActorSystem.buildClusterHoconFor clusterOptions 0 ClusterActorSystem.kubernetesRemotingHostname
        + "\n"
        + KubernetesHocon.buildPublicHostnameLine "10.0.0.7"

    let config = ConfigurationFactory.ParseString(raw)

    config.GetString("akka.remote.dot-netty.tcp.hostname") |> should equal "0.0.0.0"

    config.GetString("akka.remote.dot-netty.tcp.public-hostname")
    |> should equal "10.0.0.7"
