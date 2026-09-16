// SPDX-License-Identifier: Apache-2.0
module Legate.Storage.Postgres.Tests.TestAssembly

open Xunit

// The Docker suites share one database and truncate it after every
// conformance fact (see PostgresTestDatabase.truncate): run everything
// sequentially so a truncation never lands mid-fact in another suite.
[<assembly: CollectionBehavior(DisableTestParallelization = true)>]
do ()
