// SPDX-License-Identifier: Apache-2.0
module Legate.Testing.AssemblyInfo

open System.Runtime.CompilerServices

// The test project drives the harness internals (actor, stores, journal)
// through PromptAndWait-style clients without exposing Akka types on the
// support package's public surface.
[<assembly: InternalsVisibleTo("Legate.Tests")>]
do ()
