// SPDX-License-Identifier: Apache-2.0
module Legate.Storage.Sqlite.AssemblyInfo

open System.Runtime.CompilerServices

// Everything outside the public contract stays internal; the test project
// is the only assembly allowed to see it.
[<assembly: InternalsVisibleTo("Legate.Tests")>]
do ()
