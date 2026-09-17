// SPDX-License-Identifier: Apache-2.0
namespace Legate.Tests

open System
open System.IO
open System.Threading
open System.Threading.Tasks
open Legate
open Legate.Storage.FileSystem
open Legate.Testing
open Xunit

// Shared helpers for the file-system tests: isolated temp-directory roots
// (real filesystem, no external services) plus best-effort cleanup.

/// <summary>
/// Isolated temp-directory roots for the file-system store tests.
/// </summary>
module FileSystemTestRoot =

    /// <summary>
    /// A fresh temp-directory root path, unique per call.
    /// </summary>
    /// <returns>The root path (not yet created).</returns>
    let fresh () : string =
        Path.Combine(Path.GetTempPath(), "legate-fs-" + Guid.NewGuid().ToString("N"))

    /// <summary>
    /// Options over a root, plus extra host configuration.
    /// </summary>
    /// <param name="root">The root directory.</param>
    /// <param name="configure">Extra configuration over the options.</param>
    /// <returns>The configured options.</returns>
    let optionsFor (root: string) (configure: FileSystemStorageOptions -> unit) : FileSystemStorageOptions =
        let options = FileSystemStorageOptions()
        options.RootDirectory <- root
        configure options
        options

    /// <summary>
    /// Best-effort recursive root deletion: cleanup must never fail a test.
    /// </summary>
    /// <param name="root">The root to delete.</param>
    let deleteRoot (root: string) : unit =
        try
            if Directory.Exists root then
                Directory.Delete(root, true)
        with _ ->
            ()

/// The file-system blob store derives the shared conformance suite over a
/// temp-directory root.
type FileSystemBlobStoreTests private (store: IBlobStore, root: string) =
    inherit BlobStoreConformance(store)

    new() =
        let root = FileSystemTestRoot.fresh ()

        new FileSystemBlobStoreTests(FileSystemBlobStore(FileSystemTestRoot.optionsFor root ignore), root)

    interface IDisposable with
        member _.Dispose() = FileSystemTestRoot.deleteRoot root

/// Local-only edge tests the shared suite cannot pin: root containment,
/// atomic write visibility, sidecar cleanup, and presigned-URL gating.
module FileSystemBlobStoreEdgeTests =

    let private content (text: string) =
        BlobContent(Text.Encoding.UTF8.GetBytes text, "application/json")

    /// Runs work over a file-system blob store on a fresh temp root,
    /// deleting the root afterwards.
    let private useStore (configure: FileSystemStorageOptions -> unit) (work: IBlobStore -> string -> Task) : Task =
        task {
            let root = FileSystemTestRoot.fresh ()

            let store =
                FileSystemBlobStore(FileSystemTestRoot.optionsFor root configure) :> IBlobStore

            try
                do! work store root
            finally
                FileSystemTestRoot.deleteRoot root
        }

    [<Fact>]
    let ``Get returns null and OpenRead throws when absent`` () =
        useStore ignore (fun store _ ->
            task {
                let! absent = store.Get("edge/missing", CancellationToken.None)
                Assert.Null(absent)

                let! metadata = store.GetMetadata("edge/missing", CancellationToken.None)
                Assert.Null(metadata)

                do!
                    Assert.ThrowsAsync<FileNotFoundException>(fun () ->
                        store.OpenRead("edge/missing", CancellationToken.None))
                    :> Task
            })

    [<Fact>]
    let ``Traversal keys are rejected and containment holds`` () =
        useStore ignore (fun store root ->
            task {
                Assert.Throws<InvalidBlobKeyException>(fun () ->
                    store.Put("../escape", content "x", CancellationToken.None).GetAwaiter().GetResult()
                    |> ignore)
                |> ignore

                Assert.Throws<InvalidBlobKeyException>(fun () ->
                    store.Get("a/../../escape", CancellationToken.None).GetAwaiter().GetResult()
                    |> ignore)
                |> ignore

                // The last-moment fence, proved directly: a raw traversal
                // resolves to None instead of a path outside the root.
                Assert.Null(FileSystemPaths.tryResolveUnderRoot root [ ".."; "evil" ] |> Option.toObj)

                Assert.NotNull(FileSystemPaths.tryResolveUnderRoot root [ "edge"; "inside" ] |> Option.toObj)

                // Nothing escaped: the parent of the temp root holds no new entries.
                match Option.ofObj (Path.GetDirectoryName root) with
                | None -> failwith "expected the temp root to have a parent directory"
                | Some parent -> Assert.Empty(Directory.GetFileSystemEntries(parent, "evil"))
            })

    [<Fact>]
    let ``Writes land atomically with sidecars beside data and no staging residue`` () =
        useStore ignore (fun store root ->
            task {
                let! _ = store.Put("edge/atomic/b", content "b", CancellationToken.None)
                let! _ = store.Put("edge/atomic/a", content "a", CancellationToken.None)

                // Listings see exactly the committed keys, ordered.
                let listed = store.List("edge/atomic", CancellationToken.None)

                let mutable collected = []

                use enumerator = listed.GetAsyncEnumerator(CancellationToken.None)

                let mutable more = true

                while more do
                    let! moved = enumerator.MoveNextAsync()

                    if moved then
                        collected <- enumerator.Current :: collected
                    else
                        more <- false

                Assert.Equal<string list>([ "edge/atomic/a"; "edge/atomic/b" ], List.rev collected)

                // Data and sidecar land side by side; staging holds no residue.
                Assert.True(File.Exists(Path.Combine(root, "blobs", "edge", "atomic", "a")))
                Assert.True(File.Exists(Path.Combine(root, "blob-meta", "edge", "atomic", "a.json")))

                Assert.Empty(
                    Directory.EnumerateFiles(
                        Path.Combine(root, FileSystemPaths.stagingDirectoryName),
                        "*",
                        SearchOption.AllDirectories
                    )
                )
            })

    [<Fact>]
    let ``Content type round-trips through metadata`` () =
        useStore ignore (fun store _ ->
            task {
                let! written =
                    store.Put(
                        "edge/typed",
                        BlobContent(Text.Encoding.UTF8.GetBytes "typed", "text/plain"),
                        CancellationToken.None
                    )

                Assert.Equal("text/plain", written.ContentType)
                Assert.Equal(5L, written.SizeBytes)

                let! metadata = store.GetMetadata("edge/typed", CancellationToken.None)

                match metadata with
                | null -> failwith "expected blob metadata"
                | metadata ->
                    Assert.Equal("text/plain", metadata.ContentType)
                    Assert.Equal(5L, metadata.SizeBytes)
                    Assert.Equal(written.Etag, metadata.Etag)
            })

    [<Fact>]
    let ``Stale CompareExchange writes nothing and keeps the old bytes`` () =
        useStore ignore (fun store _ ->
            task {
                let! first = store.Put("edge/cx", content "one", CancellationToken.None)

                let! rejected = store.CompareExchange("edge/cx", content "two", "stale-etag", CancellationToken.None)

                Assert.Null(rejected)

                let! kept = store.Get("edge/cx", CancellationToken.None)
                Assert.Equal<byte>(Text.Encoding.UTF8.GetBytes "one", kept)

                // The observed etag still exchanges.
                let! exchanged = store.CompareExchange("edge/cx", content "two", first.Etag, CancellationToken.None)
                Assert.NotNull(box exchanged)

                // Absent plus a non-null etag never creates.
                let! noCreate = store.CompareExchange("edge/absent", content "x", "some-etag", CancellationToken.None)
                Assert.Null(noCreate)

                let! stillAbsent = store.Get("edge/absent", CancellationToken.None)
                Assert.Null(stillAbsent)
            })

    [<Fact>]
    let ``DeletePrefix removes data and sidecars together`` () =
        useStore ignore (fun store root ->
            task {
                let! _ = store.Put("edge/del/x", content "x", CancellationToken.None)
                let! _ = store.Put("edge/del/y", content "y", CancellationToken.None)
                let! _ = store.Put("edge/keep/z", content "z", CancellationToken.None)

                let! deleted = store.DeletePrefix("edge/del", CancellationToken.None)
                Assert.Equal(2, deleted)

                Assert.False(File.Exists(Path.Combine(root, "blobs", "edge", "del", "x")))
                Assert.False(File.Exists(Path.Combine(root, "blob-meta", "edge", "del", "x.json")))

                let! survivor = store.Get("edge/keep/z", CancellationToken.None)
                Assert.NotNull(survivor)
            })

    [<Fact>]
    let ``Half-configured presigning fails validation and full config signs`` () =
        task {
            // Half a pair is a configuration error, caught before any store exists.
            let half = FileSystemStorageOptions()
            half.RootDirectory <- FileSystemTestRoot.fresh ()
            half.PresignedBaseUrl <- "https://files.example/files"

            Assert.NotNull(box (half.Validate()))

            Assert.Throws<ArgumentException>(fun () -> FileSystemBlobStore(half) |> ignore)
            |> ignore

            FileSystemTestRoot.deleteRoot half.RootDirectory

            // Both set: URLs mint; every expiry signs differently.
            do!
                useStore
                    (fun options ->
                        options.PresignedBaseUrl <- "https://files.example/files"
                        options.PresignedSigningKey <- "test-signing-key")
                    (fun store _ ->
                        task {
                            let! first =
                                store.TryGetPresignedUrl(
                                    "edge/signed",
                                    TimeSpan.FromMinutes 5.,
                                    CancellationToken.None
                                )

                            match first with
                            | null -> failwith "expected a presigned URL"
                            | first ->
                                Assert.StartsWith("https://files.example/files/edge/signed", first.AbsoluteUri)
                                Assert.Contains("exp=", first.AbsoluteUri)
                                Assert.Contains("sig=", first.AbsoluteUri)

                                let! second =
                                    store.TryGetPresignedUrl(
                                        "edge/signed",
                                        TimeSpan.FromMinutes 6.,
                                        CancellationToken.None
                                    )

                                match second with
                                | null -> failwith "expected a second presigned URL"
                                | second -> Assert.NotEqual<string>(first.AbsoluteUri, second.AbsoluteUri)
                        })
        }

    [<Fact>]
    let ``Options reject an empty root`` () =
        task {
            let options = FileSystemStorageOptions()
            Assert.NotNull(box (options.Validate()))

            Assert.Throws<ArgumentNullException>(fun () ->
                FileSystemBlobStore(Unchecked.defaultof<FileSystemStorageOptions>) |> ignore)
            |> ignore

            Assert.Throws<ArgumentException>(fun () -> FileSystemBlobStore(options) |> ignore)
            |> ignore
        }
