// SPDX-License-Identifier: Apache-2.0
namespace Legate.Cluster.Kubernetes

open System
open Legate.Cluster

// Renders the Akka.Management plus Akka.Discovery.KubernetesApi HOCON the
// cluster start merges into the node configuration. Only documented 1.5.x
// keys are emitted, so an Akka upgrade that renames a key fails loudly at
// review (the HOCON unit tests pin every key) instead of silently
// ignoring an unknown key: Akka never reports unknown HOCON. Values are
// HOCON-escaped on render; options are validated before rendering so
// missing or invalid options fail before the cluster forms.

// Builds the management plus discovery HOCON fragment from validated
// options. Internal so only the fragment text (through IClusterBootstrap)
// is visible outside the package.
module internal KubernetesHocon =

    /// The Akka.Discovery method selecting the Kubernetes API mechanism.
    let discoveryMethod = "kubernetes-api"

    /// Escapes a value for double-quoted HOCON: backslashes and quotes.
    /// <param name="value">The raw option value.</param>
    /// <returns>The value safe to embed in double quotes.</returns>
    let escape (value: string) : string =
        if isNull (box value) then
            ""
        else
            value.Replace("\\", "\\\\").Replace("\"", "\\\"")

    /// Renders the management plus discovery HOCON fragment for validated
    /// options. The namespace line is omitted when no namespace is set so
    /// discovery auto-detects it from the service account secret.
    /// <param name="options">The validated Kubernetes options. Must not be null.</param>
    /// <returns>The HOCON fragment merged into the cluster configuration.</returns>
    let buildFragment (options: KubernetesOptions) : string =
        ArgumentNullException.ThrowIfNull(options)

        match options.Validate() with
        | null -> ()
        | violation -> raise (InvalidOperationException($"Invalid Kubernetes bootstrap options: %s{violation}"))

        let selector = escape (options.PodLabelSelector.Trim())
        let portName = escape (options.PortName.Trim())

        let podNamespace =
            match Option.ofObj options.Namespace with
            | Some ns when not (String.IsNullOrWhiteSpace ns) -> $"    pod-namespace = \"%s{escape (ns.Trim())}\"\n"
            | _ -> ""

        $"""akka.discovery {{
  method = %s{discoveryMethod}
  kubernetes-api {{
    pod-label-selector = "%s{selector}"
%s{podNamespace}  }}
}}
akka.management {{
  http {{
    bind-hostname = "0.0.0.0"
    port = %d{options.ManagementPort}
  }}
  cluster.bootstrap {{
    contact-point-discovery {{
      discovery-method = %s{discoveryMethod}
      port-name = "%s{portName}"
      required-contact-point-nr = %d{options.RequiredContactPoints}
    }}
  }}
}}"""
