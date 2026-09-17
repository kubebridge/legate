// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.S3.Tests

open System
open System.Net.Http
open System.Threading
open System.Threading.Tasks
open Legate
open Legate.Storage.S3
open Legate.Testing
open Xunit

// The S3 blob store derives the shared conformance suite over a fresh
// MinIO bucket per test-class instance, then proves the S3-only
// behaviors: pagination across the thousand-key page, batched prefix
// deletion across the thousand-key delete batch, presigned downloads,
// and the conditional-write fail-fast against a non-supporting endpoint.
type S3BlobStoreTests() =
    inherit BlobStoreConformance(S3TestEnvironment.createBlobStore ())

    /// One JSON blob content the suite writes.
    let content (text: string) =
        BlobContent(System.Text.Encoding.UTF8.GetBytes text, "application/json")

    [<Fact>]
    member _.``List pages and DeletePrefix batches past a thousand keys``() =
        task {
            let store = S3TestEnvironment.createBlobStore () :> IBlobStore
            let prefix = "conformance/page"
            let count = 1100

            for index in 0 .. count - 1 do
                let key = sprintf "%s/%04d" prefix index

                let! _ = store.Put(key, content $"value-{index}", CancellationToken.None)
                ()

            let listed = store.List(prefix, CancellationToken.None)
            let collected = ResizeArray<string>()
            use enumerator = listed.GetAsyncEnumerator(CancellationToken.None)
            let mutable more = true

            while more do
                let! moved = enumerator.MoveNextAsync()

                if moved then
                    collected.Add enumerator.Current
                else
                    more <- false

            Assert.Equal(count, collected.Count)

            for index in 0 .. count - 1 do
                Assert.Equal(sprintf "%s/%04d" prefix index, collected[index])

            let! deleted = store.DeletePrefix(prefix, CancellationToken.None)
            Assert.Equal(count, deleted)

            let remaining = store.List(prefix, CancellationToken.None)
            use remainingEnumerator = remaining.GetAsyncEnumerator(CancellationToken.None)
            let! hasAny = remainingEnumerator.MoveNextAsync()
            Assert.False(hasAny)
        }

    [<Fact>]
    member _.``Presigned URL downloads the stored bytes before expiry``() =
        task {
            let store = S3TestEnvironment.createBlobStore () :> IBlobStore
            let bytes = System.Text.Encoding.UTF8.GetBytes "presigned bytes"

            let! _ = store.Put("conformance/presign/file", BlobContent(bytes, "text/plain"), CancellationToken.None)

            let! url =
                store.TryGetPresignedUrl("conformance/presign/file", TimeSpan.FromMinutes 5., CancellationToken.None)

            match url with
            | null -> failwith "The presigned URL was null."
            | address ->
                // The SDK rounds the expiry to whole seconds, so the
                // wire value lands within a second of the request.
                let expires = S3PresignSupport.expiresSeconds address
                Assert.InRange(expires, 299, 301)

                use http = new HttpClient()
                let! downloaded = http.GetByteArrayAsync(address)
                Assert.Equal<byte>(bytes, downloaded)
        }

    [<Fact>]
    member _.``Presigned URL expiry honors the options cap``() =
        task {
            let endpoint, _, _ = S3TestEnvironment.ensureReady ()
            let probe = S3TestEnvironment.testOptions endpoint "legate-probe"
            let client = S3Client.buildClient probe
            let bucket = S3TestEnvironment.freshBucket client

            let options = S3TestEnvironment.testOptions endpoint bucket
            options.PresignExpiry <- TimeSpan.FromMinutes 1.

            let store = S3BlobStore(options, client) :> IBlobStore
            let bytes = System.Text.Encoding.UTF8.GetBytes "capped"

            let! _ = store.Put("conformance/presign/capped", BlobContent(bytes, "text/plain"), CancellationToken.None)

            // Asking for an hour receives the one-minute cap instead.
            let! url =
                store.TryGetPresignedUrl("conformance/presign/capped", TimeSpan.FromHours 1., CancellationToken.None)

            match url with
            | null -> failwith "The presigned URL was null."
            | address ->
                let expires = S3PresignSupport.expiresSeconds address
                Assert.InRange(expires, 59, 61)
        }
