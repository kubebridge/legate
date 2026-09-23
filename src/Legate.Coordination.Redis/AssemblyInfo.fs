// SPDX-License-Identifier: Apache-2.0
module Legate.Coordination.Redis.AssemblyInfo

open System.Runtime.CompilerServices

// Everything outside the public contract stays internal; the Redis
// coordination test project is the only assembly allowed to see it.
[<assembly: InternalsVisibleTo("Legate.Coordination.Redis.Tests")>]
do ()
