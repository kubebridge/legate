// SPDX-License-Identifier: Apache-2.0
module Legate.Cluster.Kubernetes.AssemblyInfo

open System.Runtime.CompilerServices

// Everything outside the public contract stays internal; the test project
// is the only assembly allowed to see it.
[<assembly: InternalsVisibleTo("Legate.Tests")>]
do ()
