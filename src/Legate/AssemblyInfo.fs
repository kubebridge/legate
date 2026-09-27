// SPDX-License-Identifier: Apache-2.0
module Legate.AssemblyInfo

open System.Runtime.CompilerServices

// Everything outside the public contract stays internal; the test project,
// the test-support package (which drives the session actor), and the
// dedicated Anthropic provider tests (which register through LlmBuilder)
// are the only assemblies allowed to see it.
[<assembly: InternalsVisibleTo("Legate.Tests")>]
[<assembly: InternalsVisibleTo("Legate.Testing")>]
[<assembly: InternalsVisibleTo("Legate.Llm.Anthropic.Tests")>]
do ()
