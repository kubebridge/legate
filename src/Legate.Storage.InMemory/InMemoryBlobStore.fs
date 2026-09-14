// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.InMemory

open System
open System.Collections.Generic
open System.IO
open System.Linq
open System.Threading
open System.Threading.Tasks
open Legate

/// Turns a synchronous list into an <see cref="T:System.Collections.Generic.IAsyncEnumerable`1" />
/// that yields without asynchrony.
module internal SyncAsync =

    /// Adapts a list into an IAsyncEnumerable, in order.
    let ofList (items: 'T list) : IAsyncEnumerable<'T> =
        { new IAsyncEnumerable<'T> with
            member _.GetAsyncEnumerator(_: CancellationToken) : IAsyncEnumerator<'T> =
                let items = items |> List.toArray
                let mutable index = -1

                { new IAsyncEnumerator<'T> with
                    member this.MoveNextAsync() : ValueTask<bool> =
                        let mutable next = false

                        if index + 1 < items.Length then
                            index <- index + 1
                            next <- true

                        ValueTask<bool>(next)

                    member _.Current: 'T = items[index]

                    member _.DisposeAsync() : ValueTask = ValueTask()
                }
        }

/// A write stream that buffers locally and commits its bytes to the
/// database when disposed: an abandoned stream that is never disposed
/// commits nothing, and a disposed stream lands atomically under the
/// database's gate lock.
type internal CommitOnDisposeStream(database: InMemoryDatabase, key: string, contentType: string) =
    inherit MemoryStream()

    override this.Dispose(disposing: bool) =
        if disposing then
            let bytes = this.ToArray()

            lock database.Gate (fun () ->
                let etag = Ulid.NewUlid().ToString()
                let metadata = BlobMetadata(contentType, int64 bytes.Length, etag)
                database.Blobs[key] <- BlobRow(BlobContent(bytes, contentType), metadata))
            |> ignore

        base.Dispose disposing

    override this.DisposeAsync() : ValueTask =
        this.Dispose(true)
        ValueTask()

/// The in-memory <see cref="T:Legate.IBlobStore" />: the blob primitives
/// over one shared database dictionary. Etags are fresh ULIDs per write and
/// never reused; <see cref="M:Legate.IBlobStore.OpenWrite*" /> buffers and
/// commits atomically on dispose, so an abandoned stream never commits and
/// readers never see a partial blob; presigning returns null (the backend
/// cannot presign); keys and prefixes validate through
/// <see cref="T:Legate.BlobKeys" />.
type InMemoryBlobStore(database: InMemoryDatabase) =

    do
        if isNull (box database) then
            raise (ArgumentNullException(nameof database))

    let ok value = Task.FromResult value

    let row (key: string) =
        match database.Blobs.TryGetValue key with
        | true, blobRow -> Some blobRow
        | false, _ -> None

    let bytesOf (content: BlobContent) =
        if isNull (box content.Bytes) then
            Array.empty<byte>
        else
            content.Bytes

    let contentTypeOf (content: BlobContent) =
        if isNull (box content.ContentType) then
            "application/octet-stream"
        else
            content.ContentType

    let write (key: string) (bytes: byte[]) (contentType: string) =
        let etag = Ulid.NewUlid().ToString()
        let metadata = BlobMetadata(contentType, int64 bytes.Length, etag)
        database.Blobs[key] <- BlobRow(BlobContent(bytes, contentType), metadata)
        metadata

    interface IBlobStore with

        member _.Get(key, _) =
            BlobKeys.Validate key |> ignore

            lock database.Gate (fun () ->
                match row key with
                | None -> null
                | Some blobRow -> blobRow.Content.Bytes)
            |> ok

        member _.Put(key, content, _) =
            if isNull (box content.Bytes) then
                raise (ArgumentNullException(nameof content))

            if isNull (box content.ContentType) then
                raise (ArgumentNullException(nameof content))

            BlobKeys.Validate key |> ignore

            lock database.Gate (fun () -> write key (bytesOf content) (contentTypeOf content))
            |> ok

        member _.CompareExchange(key, content, expectedEtag, _) =
            if isNull (box content.Bytes) then
                raise (ArgumentNullException(nameof content))

            if isNull (box content.ContentType) then
                raise (ArgumentNullException(nameof content))

            BlobKeys.Validate key |> ignore

            lock database.Gate (fun () ->
                let matches =
                    match expectedEtag, row key with
                    | null, None -> true
                    | null, Some _ -> false
                    | _, None -> false
                    | etag, Some blobRow -> String.Equals(etag, blobRow.Metadata.Etag, StringComparison.Ordinal)

                if matches then
                    write key (bytesOf content) (contentTypeOf content) |> Some
                else
                    None)
            |> Option.toObj
            |> ok

        member _.OpenRead(key, _) =
            BlobKeys.Validate key |> ignore

            lock database.Gate (fun () ->
                match row key with
                | None -> raise (FileNotFoundException(sprintf "No blob exists under key %s." key, key))
                | Some blobRow -> new MemoryStream(blobRow.Content.Bytes, false) :> Stream)
            |> ok

        member _.OpenWrite(key, contentType, _) =
            if isNull (box contentType) then
                raise (ArgumentNullException(nameof contentType))

            BlobKeys.Validate key |> ignore

            new CommitOnDisposeStream(database, key, contentType) :> Stream |> ok

        member _.List(prefix, _) =
            BlobKeys.ValidatePrefix prefix |> ignore

            lock database.Gate (fun () ->
                database.Blobs.Keys
                |> Seq.filter (fun key -> key.StartsWith(prefix, StringComparison.Ordinal))
                |> Seq.sortWith (fun left right -> String.CompareOrdinal(left, right))
                |> Seq.toList)
            |> SyncAsync.ofList

        member _.DeletePrefix(prefix, _) =
            BlobKeys.ValidatePrefix prefix |> ignore

            lock database.Gate (fun () ->
                let doomed =
                    database.Blobs.Keys
                    |> Seq.filter (fun key -> key.StartsWith(prefix, StringComparison.Ordinal))
                    |> Seq.toList

                for key in doomed do
                    database.Blobs.Remove key |> ignore

                doomed.Length)
            |> ok

        member _.GetMetadata(key, _) =
            BlobKeys.Validate key |> ignore

            lock database.Gate (fun () ->
                match row key with
                | None -> null
                | Some blobRow -> blobRow.Metadata)
            |> ok

        member _.TryGetPresignedUrl(_, _, _) =
            // The in-memory backend cannot presign.
            ok null
