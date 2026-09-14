// SPDX-License-Identifier: Apache-2.0
namespace Legate.Tests

open Legate.Storage.InMemory
open Legate.Testing

/// The InMemory blob store derives the shared conformance suite.
type InMemoryBlobStoreTests() =
    inherit BlobStoreConformance(InMemoryStoreFactory.blobStore (InMemoryDatabase()))
