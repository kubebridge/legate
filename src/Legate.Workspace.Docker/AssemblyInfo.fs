// SPDX-License-Identifier: Apache-2.0
module Legate.Workspace.Docker.AssemblyInfo

open System.Runtime.CompilerServices

// Everything outside the public contract stays internal; the Docker test
// project is the only assembly allowed to see it.
[<assembly: InternalsVisibleTo("Legate.Workspace.Docker.Tests")>]
do ()
