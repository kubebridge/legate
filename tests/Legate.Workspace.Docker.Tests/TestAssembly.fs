// SPDX-License-Identifier: Apache-2.0
module Legate.Workspace.Docker.Tests.TestAssembly

open Xunit

// The daemon-backed suites share one Docker daemon and mint a
// session-scoped container per fact (see DockerTestDaemon): run everything
// sequentially so no two suites race container creation.
[<assembly: CollectionBehavior(DisableTestParallelization = true)>]
do ()
