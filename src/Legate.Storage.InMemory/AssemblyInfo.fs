// SPDX-License-Identifier: Apache-2.0
module Legate.Storage.InMemory.AssemblyInfo

open System.Runtime.CompilerServices

// Everything outside the public contract stays internal; the test project
// and the test-support package (which passes the era reader into the
// session harness) are the only assemblies allowed to see it.
[<assembly: InternalsVisibleTo("Legate.Tests")>]
[<assembly: InternalsVisibleTo("Legate.Testing")>]
do ()
