// SPDX-License-Identifier: Apache-2.0
module Legate.Storage.S3.Tests.TestAssembly

open Xunit

// The Docker suites share one MinIO container and mint a fresh bucket per
// test-class instance (see S3TestEnvironment): run everything sequentially
// so no two suites race container startup.
[<assembly: CollectionBehavior(DisableTestParallelization = true)>]
do ()
