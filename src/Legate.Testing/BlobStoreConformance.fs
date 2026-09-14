// SPDX-License-Identifier: Apache-2.0
namespace Legate.Testing

open System
open System.IO
open System.Threading
open System.Threading.Tasks
open Legate
open Xunit

/// The shared conformance suite for <see cref="T:Legate.IBlobStore" />
/// implementations: etag uniqueness, the compare-exchange matrix, the
/// commit-on-dispose write stream, listing order, prefix deletion, key
/// validation, and the cannot-presign outcome. Keys here are raw store
/// keys; the typed scopes derive through
/// <see cref="T:Legate.BlobKeys" /> and are exercised in the runtime's own
/// suites.
[<AbstractClass>]
type BlobStoreConformance(store: IBlobStore) =

    do
        if isNull (box store) then
            raise (ArgumentNullException(nameof store))

    /// The store under test.
    member this.Store = store

    /// One JSON blob content the suite writes.
    member this.Content(text: string) =
        BlobContent(System.Text.Encoding.UTF8.GetBytes text, "application/json")

    [<Fact>]
    member this.``Etags are unique per write and never reused``() =
        task {
            let! first = store.Put("conformance/etag", this.Content "one", CancellationToken.None)

            let! _ = Task.Delay(1)

            let! second = store.Put("conformance/etag", this.Content "two", CancellationToken.None)

            Assert.NotEqual<string>(first.Etag, second.Etag)
        }

    [<Fact>]
    member this.``CompareExchange honours create-if-absent and etag matching``() =
        task {
            let! created = store.CompareExchange("conformance/cx", this.Content "v1", null, CancellationToken.None)

            Assert.NotNull(box created)

            // A second create-if-absent fails: the blob exists.
            let! occupied = store.CompareExchange("conformance/cx", this.Content "v2", null, CancellationToken.None)

            Assert.Null(occupied)

            // The observed etag matches.
            let createdEtag =
                match created with
                | null -> failwith "create-if-absent should have written"
                | written -> written.Etag

            let! exchanged =
                store.CompareExchange("conformance/cx", this.Content "v3", createdEtag, CancellationToken.None)

            Assert.NotNull(box exchanged)

            // The stale etag no longer matches.
            let! stale = store.CompareExchange("conformance/cx", this.Content "v4", createdEtag, CancellationToken.None)

            Assert.Null(stale)
        }

    [<Fact>]
    member this.``OpenWrite commits on dispose and abandons never commit``() =
        task {
            let! stream = store.OpenWrite("conformance/commit", "text/plain", CancellationToken.None)

            let bytes = System.Text.Encoding.UTF8.GetBytes "committed"
            let! _ = stream.WriteAsync(bytes, 0, bytes.Length, CancellationToken.None)
            stream.Dispose()

            let! metadata = store.GetMetadata("conformance/commit", CancellationToken.None)

            Assert.NotNull(box metadata)

            // Round-trip: the committed bytes read back identically,
            // which catches a store that commits the wrong buffer
            // (empty or partial content) while metadata still lands.
            let! read = store.Get("conformance/commit", CancellationToken.None)

            Assert.Equal<byte>(bytes, read)

            // A second committed write replaces the first content.
            let! rewrite = store.OpenWrite("conformance/commit", "text/plain", CancellationToken.None)

            let replacement = System.Text.Encoding.UTF8.GetBytes "replaced"
            let! _ = rewrite.WriteAsync(replacement, 0, replacement.Length, CancellationToken.None)
            rewrite.Dispose()

            let! replaced = store.Get("conformance/commit", CancellationToken.None)

            Assert.Equal<byte>(replacement, replaced)

            // An abandoned stream never commits: no key, no metadata.
            let! abandoned = store.OpenWrite("conformance/abandoned", "text/plain", CancellationToken.None)

            Assert.NotNull(abandoned)

            let! absent = store.GetMetadata("conformance/abandoned", CancellationToken.None)

            Assert.Null(absent)
        }

    [<Fact>]
    member this.``List returns matching keys in lexicographic order``() =
        task {
            let _ =
                store.Put("conformance/list/b", this.Content "b", CancellationToken.None).Result

            let _ =
                store.Put("conformance/list/a", this.Content "a", CancellationToken.None).Result

            let listed = store.List("conformance/list", CancellationToken.None)

            let mutable collected = []

            let mutable awaiter = listed.GetAsyncEnumerator(CancellationToken.None)

            try
                let mutable more = true

                while more do
                    let mutable move = awaiter.MoveNextAsync().AsTask().GetAwaiter().GetResult()

                    if move then
                        collected <- awaiter.Current :: collected
                    else
                        more <- false
            finally
                awaiter.DisposeAsync().AsTask().GetAwaiter().GetResult()

            Assert.Equal<string list>(
                [
                    "conformance/list/a"
                    "conformance/list/b"
                ],
                List.rev collected
            )
        }

    [<Fact>]
    member this.``DeletePrefix removes everything under the prefix``() =
        task {
            let _ =
                store.Put("conformance/del/x", this.Content "x", CancellationToken.None).Result

            let _ =
                store.Put("conformance/del/y", this.Content "y", CancellationToken.None).Result

            let _ =
                store.Put("conformance/keep/z", this.Content "z", CancellationToken.None).Result

            let! deleted = store.DeletePrefix("conformance/del", CancellationToken.None)

            Assert.Equal(2, deleted)

            let! survivor = store.Get("conformance/keep/z", CancellationToken.None)

            Assert.NotNull(survivor)
        }

    [<Fact>]
    member this.``Keys validate through BlobKeys``() =
        task {
            Assert.Throws<InvalidBlobKeyException>(fun () ->
                store.Put("../escape", this.Content "x", CancellationToken.None).GetAwaiter().GetResult()
                |> ignore)
            |> ignore

            Assert.Throws<InvalidBlobKeyException>(fun () ->
                store.List("/rooted/", CancellationToken.None).GetAsyncEnumerator() |> ignore)
            |> ignore
        }

    [<Fact>]
    member this.``Presigning returns null when the backend cannot``() =
        task {
            let! url = store.TryGetPresignedUrl("conformance/etag", TimeSpan.FromMinutes 5., CancellationToken.None)

            Assert.Null(url)
        }
