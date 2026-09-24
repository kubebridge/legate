// SPDX-License-Identifier: Apache-2.0
module Legate.Llm.AssemblyInfo

open System.Runtime.CompilerServices

// Everything outside the public contract stays internal; the dedicated test
// project is the only assembly allowed to see it (scripted-transport and
// constructed-exception tests drive the internals without live calls).
[<assembly: InternalsVisibleTo("Legate.Llm.Anthropic.Tests")>]
do ()
