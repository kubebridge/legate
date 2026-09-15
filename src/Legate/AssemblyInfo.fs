// SPDX-License-Identifier: Apache-2.0
module Legate.AssemblyInfo

open System.Runtime.CompilerServices

// Everything outside the public contract stays internal; the test project
// and the test-support package (which drives the session actor) are the
// only assemblies allowed to see it.
[<assembly: InternalsVisibleTo("Legate.Tests")>]
[<assembly: InternalsVisibleTo("Legate.Testing")>]
do ()
