// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.KubernetesBootstrapTests

open Akka.Actor
open Akka.Configuration
open Akka.Discovery.KubernetesApi
open FsUnit.Xunit
open Legate.Cluster
open Legate.Cluster.Kubernetes
open Xunit

// Kubernetes bootstrap discovery defaults (issue 267): touching the
// discovery extension injects its reference.conf fallback so the
// hand-rendered fragment keys override real defaults. Hermetic: the
// fragment-alone system is disposed and never starts management,
// bootstrap, or cluster contact.

// ──────────────────────────────────────────────────────────────────────────
// Discovery defaults

[<Fact>]
let ``Discovery Get injects reference defaults with explicit fragment keys overriding`` () =
    let fragment = KubernetesHocon.buildFragment (KubernetesOptions())

    let system =
        ActorSystem.Create("legate-kubernetes-bootstrap-test", ConfigurationFactory.ParseString(fragment))

    try
        // Mirrors KubernetesBootstrap.StartAsync without starting anything:
        // touching the extension injects the reference.conf fallback.
        KubernetesDiscovery.Get(system) |> ignore

        let config = system.Settings.Config

        // Injected reference.conf defaults absent from the fragment.
        config.GetString("akka.discovery.kubernetes-api.api-ca-path")
        |> should equal "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"

        config.GetString("akka.discovery.kubernetes-api.api-token-path")
        |> should equal "/var/run/secrets/kubernetes.io/serviceaccount/token"

        config.GetString("akka.discovery.kubernetes-api.api-service-host-env-name")
        |> should equal "KUBERNETES_SERVICE_HOST"

        config.GetString("akka.discovery.kubernetes-api.api-service-port-env-name")
        |> should equal "KUBERNETES_SERVICE_PORT"

        config.GetString("akka.discovery.kubernetes-api.pod-namespace-path")
        |> should equal "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

        config.GetString("akka.discovery.kubernetes-api.pod-domain")
        |> should equal "cluster.local"

        config.GetBoolean("akka.discovery.kubernetes-api.use-raw-ip")
        |> should equal true

        // Explicit fragment keys still override the injected defaults.
        config.GetString("akka.discovery.kubernetes-api.class")
        |> should equal "Akka.Discovery.KubernetesApi.KubernetesApiServiceDiscovery, Akka.Discovery.KubernetesApi"

        config.GetString("akka.discovery.kubernetes-api.pod-label-selector")
        |> should equal "app=legate"
    finally
        system.Terminate().GetAwaiter().GetResult() |> ignore
