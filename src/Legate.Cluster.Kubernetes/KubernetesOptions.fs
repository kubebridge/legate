// SPDX-License-Identifier: Apache-2.0
namespace Legate.Cluster

open System

// Kubernetes bootstrap options: the management endpoint plus the pod
// discovery knobs the bootstrap HOCON renders. Options bound through
// IOptions<KubernetesOptions> are plain classes with mutable properties
// and defaults; Validate returns null when valid or the first violation
// otherwise, mirroring ClusterOptions. Validation runs both at
// UseKubernetes registration and at cluster start, so missing or invalid
// options fail before the cluster forms, never mid-bootstrap.

// Kubernetes bootstrap settings: the management endpoint the pod exposes
// and the discovery knobs locating peer pods. Bound by the host (code or
// configuration); mutable so hosts can set properties before registering.
// Defaults expose management on 8558, select pods labelled app=legate in
// the ambient namespace, and wait for 2 contact points before bootstrap
// decides.
type KubernetesOptions() =

    /// The port the Akka.Management endpoint binds inside the pod (bound
    /// on all interfaces). The pod manifest must expose and name this
    /// port (see <see cref="P:Legate.Cluster.KubernetesOptions.PortName" />)
    /// so discovery can probe it. Default 8558.
    member val ManagementPort: int = 8558 with get, set

    /// The Kubernetes label selector locating peer pods, for example
    /// app=legate. Rendered verbatim into
    /// <c>akka.discovery.kubernetes-api.pod-label-selector</c>. Default
    /// app=legate. Must be a non-empty string.
    member val PodLabelSelector: string = "app=legate" with get, set

    /// The namespace queried for pods, or null (the default) to
    /// auto-detect it from the service account secret, falling back to
    /// default when the secret is absent (plain-process development).
    /// Rendered into
    /// <c>akka.discovery.kubernetes-api.pod-namespace</c> only when set.
    /// When set, must be a non-empty string.
    member val Namespace: string | null = null with get, set

    /// The name of the pod port carrying the management endpoint, for
    /// example management. Passed to discovery as the port name and to
    /// bootstrap as
    /// <c>akka.management.cluster.bootstrap.contact-point-discovery.port-name</c>,
    /// so it must match the port name the pod manifest declares. Default
    /// management. Must be a non-empty string.
    member val PortName: string = "management" with get, set

    /// How many contact points bootstrap waits for before deciding,
    /// rendered as
    /// <c>akka.management.cluster.bootstrap.contact-point-discovery.required-contact-point-nr</c>.
    /// Set this to the replica count the workload runs (the sibling
    /// manifests own that number). Default 2, the Akka default. Must be
    /// at least 1.
    member val RequiredContactPoints: int = 2 with get, set

    /// Returns null when every knob is in range, otherwise a message for the
    /// first violation.
    /// <returns>The first violation's message, or null when the settings are valid.</returns>
    member this.Validate() : string | null =
        let violations =
            [|
                if this.ManagementPort < 1 || this.ManagementPort > 65535 then
                    "ManagementPort must be between 1 and 65535."
                if String.IsNullOrWhiteSpace this.PodLabelSelector then
                    "PodLabelSelector must be a non-empty string."
                if not (isNull (box this.Namespace)) && String.IsNullOrWhiteSpace this.Namespace then
                    "Namespace must be a non-empty string when set."
                if String.IsNullOrWhiteSpace this.PortName then
                    "PortName must be a non-empty string."
                if this.RequiredContactPoints < 1 then
                    "RequiredContactPoints must be at least 1."
            |]

        if violations.Length = 0 then
            null
        else
            Array.head violations
