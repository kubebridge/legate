// SPDX-License-Identifier: Apache-2.0
module Legate.Llm.OpenAI.AssemblyInfo

open System.Runtime.CompilerServices

// Everything outside the public contract stays internal; the test project is
// the only assembly allowed to see it (scripted transports and Retry-After
// unit tests drive the internals without live network calls).
[<assembly: InternalsVisibleTo("Legate.Tests")>]
do ()
