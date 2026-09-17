// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.S3.Tests

open System.Threading
open System.Threading.Tasks
open Legate
open Legate.Storage.S3
open Legate.Testing
open Xunit

// The S3 package store derives the shared conformance suite over a fresh
// MinIO bucket per test-class instance. The pointer commit rides the same
// conditional write the blob fail-fast fact pins, so no separate
// Docker-backed negative lives here.
type S3AgentPackageStoreTests() =
    inherit AgentPackageStoreConformance(S3TestEnvironment.createPackageStore () |> fst)
