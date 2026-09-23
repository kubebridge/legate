// SPDX-License-Identifier: Apache-2.0
module Legate.Coordination.Redis.Tests.TestAssembly

open Xunit

// The Docker suites share one Redis container and mint a fresh identity per
// fact (see RedisTestEnvironment): run everything sequentially so no two
// suites race container startup.
[<assembly: CollectionBehavior(DisableTestParallelization = true)>]
do ()
