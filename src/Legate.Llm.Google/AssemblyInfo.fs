// SPDX-License-Identifier: Apache-2.0
module Legate.Llm.AssemblyInfo

open System.Runtime.CompilerServices

// Everything outside the public contract stays internal; the test project is
// the only assembly allowed to see it (the error-mapping and round-trip
// tests drive the internal chat-client wrapper directly).
[<assembly: InternalsVisibleTo("Legate.Tests")>]
do ()
