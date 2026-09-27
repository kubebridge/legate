// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.KubernetesOptionsTests

open System
open FsUnit.Xunit
open Legate.Cluster
open Xunit

// Kubernetes bootstrap options (issue 138): defaults plus fail-fast
// validation. Hermetic: no actor system, no cluster, no Kubernetes.

// ──────────────────────────────────────────────────────────────────────────
// Defaults

[<Fact>]
let ``Kubernetes options default to the documented management endpoint and discovery knobs`` () =
    let options = KubernetesOptions()

    options.ManagementPort |> should equal 8558
    options.PodLabelSelector |> should equal "app=legate"
    isNull (box options.Namespace) |> should equal true
    options.PortName |> should equal "management"
    options.RequiredContactPoints |> should equal 2
    options.Validate() |> should equal null

// ──────────────────────────────────────────────────────────────────────────
// Validation

[<Fact>]
let ``Kubernetes Validate flags a bad management port`` () =
    KubernetesOptions(ManagementPort = 0).Validate()
    |> should equal "ManagementPort must be between 1 and 65535."

    KubernetesOptions(ManagementPort = 65536).Validate()
    |> should equal "ManagementPort must be between 1 and 65535."

[<Fact>]
let ``Kubernetes Validate flags a blank pod label selector`` () =
    let blank = KubernetesOptions()
    blank.PodLabelSelector <- "  "
    blank.Validate() |> should equal "PodLabelSelector must be a non-empty string."

[<Fact>]
let ``Kubernetes Validate accepts an unset namespace but flags a blank one`` () =
    KubernetesOptions().Validate() |> should equal null

    let blank = KubernetesOptions()
    blank.Namespace <- "  "

    blank.Validate()
    |> should equal "Namespace must be a non-empty string when set."

    let named = KubernetesOptions()
    named.Namespace <- "legate-prod"
    named.Validate() |> should equal null

[<Fact>]
let ``Kubernetes Validate flags a blank port name`` () =
    let blank = KubernetesOptions()
    blank.PortName <- ""
    blank.Validate() |> should equal "PortName must be a non-empty string."

[<Fact>]
let ``Kubernetes Validate flags too few required contact points`` () =
    KubernetesOptions(RequiredContactPoints = 0).Validate()
    |> should equal "RequiredContactPoints must be at least 1."
