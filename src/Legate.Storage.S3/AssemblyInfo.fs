// SPDX-License-Identifier: Apache-2.0
module Legate.Storage.S3.AssemblyInfo

open System.Runtime.CompilerServices

// Everything outside the public contract stays internal; the S3 test
// project is the only assembly allowed to see it.
[<assembly: InternalsVisibleTo("Legate.Storage.S3.Tests")>]
do ()
