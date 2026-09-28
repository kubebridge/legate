// SPDX-License-Identifier: Apache-2.0
module Legate.AssemblyInfo

open System.Runtime.CompilerServices

// Everything outside the public contract stays internal; the test project,
// the test-support package (which drives the session actor), and the
// dedicated Anthropic provider tests (which register through LlmBuilder)
// are the only assemblies allowed to see it. The storage providers also
// see it (issue 289): they replace the internal completion-era gate with
// a backed one from their registration.
[<assembly: InternalsVisibleTo("Legate.Tests")>]
[<assembly: InternalsVisibleTo("Legate.Testing")>]
[<assembly: InternalsVisibleTo("Legate.Llm.Anthropic.Tests")>]
[<assembly: InternalsVisibleTo("Legate.Storage.InMemory")>]
[<assembly: InternalsVisibleTo("Legate.Storage.Sqlite")>]
[<assembly: InternalsVisibleTo("Legate.Storage.Postgres")>]
do ()
