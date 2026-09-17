// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.FileSystem

open System
open System.IO
open System.Threading.Tasks
open Legate

// The file-system IBlobStore: the blob primitives over one rooted directory.
// One file per validated key under blobs/, one JSON sidecar (etag plus
// content type) mirroring it under blob-meta/, so listings over the data
// tree never see metadata. Every write is temp-plus-rename under a
// per-store gate: readers never see a partial blob, and CompareExchange
// checks the sidecar etag under the same gate before swapping (single-node
// scope; the cluster uses the S3 backend). Presigned URLs are HMAC-SHA256
// signed URLs built only when the options carry both a base URL and a
// signing key; otherwise TryGetPresignedUrl returns null.

/// <summary>
/// A write stream that buffers locally and commits its bytes to the
/// file-system store when disposed: an abandoned stream that is never
/// disposed commits nothing, and a disposed stream lands atomically under
/// the store's gate.
/// </summary>
type internal FileSystemCommitStream(onCommit: byte[] -> unit) =
    inherit MemoryStream()

    let mutable committed = false

    /// <summary>
    /// Commits the buffered bytes exactly once, then releases the buffer.
    /// </summary>
    /// <param name="disposing">True when called from Dispose.</param>
    override this.Dispose(disposing: bool) =
        if disposing && not committed then
            committed <- true
            onCommit (this.ToArray())

        base.Dispose disposing

    /// <summary>
    /// Commits the buffered bytes exactly once, then releases the buffer.
    /// </summary>
    /// <returns>A task completing when the commit lands.</returns>
    override this.DisposeAsync() : ValueTask =
        this.Dispose(true)
        ValueTask()

/// <summary>
/// The file-system <see cref="T:Legate.IBlobStore" /> over one rooted
/// directory: file-per-key blob bytes plus JSON sidecar metadata, atomic
/// temp-plus-rename writes under a per-store gate, and options-bound
/// HMAC presigning only when configured. Single-node scope.
/// </summary>
/// <param name="options">The storage options: the shared root plus the optional presigning pair. Must not be null.</param>
type FileSystemBlobStore(options: FileSystemStorageOptions) =

    do
        ArgumentNullException.ThrowIfNull(options)

        match options.Validate() with
        | null -> ()
        | reason -> raise (ArgumentException(reason, nameof options))

    let rootFull = FileSystemPaths.ensureRoot options.RootDirectory
    let stagingRootFull = FileSystemPaths.stagingRoot rootFull
    let gate = obj ()

    // Snapshotted at construction; the key only ever feeds HMAC-SHA256 and
    // is never logged. Option.ofObj narrows the pair once, so the signing
    // call below takes plain non-null strings.
    let presignPair =
        match Option.ofObj options.PresignedBaseUrl, Option.ofObj options.PresignedSigningKey with
        | Some baseUrl, Some signingKey when
            not (String.IsNullOrWhiteSpace baseUrl)
            && not (String.IsNullOrWhiteSpace signingKey)
            ->
            Some(baseUrl, signingKey)
        | _ -> None

    /// Reads the current etag for a key: None when the data file is absent
    /// or its sidecar is missing or corrupt (a corrupt sidecar safely fails
    /// every exchange; Put still overwrites it).
    /// <param name="key">The validated blob key.</param>
    /// <returns>The current etag, or None when absent or unreadable.</returns>
    let currentEtag (key: string) : string option =
        let dataPath = FileSystemPaths.blobDataPath rootFull key

        if not (File.Exists dataPath) then
            None
        else
            let metaPath = FileSystemPaths.blobMetaPath rootFull key

            if not (File.Exists metaPath) then
                None
            else
                match FileSystemManifests.tryReadBlobMeta (File.ReadAllBytes metaPath) with
                | Some(etag, _) -> Some etag
                | None -> None

    /// Writes blob bytes plus a fresh sidecar under the gate: the sidecar
    /// lands first so a crash leaves at most an invisible orphan sidecar,
    /// never data without metadata.
    /// <param name="key">The validated blob key.</param>
    /// <param name="bytes">The blob payload.</param>
    /// <param name="contentType">The content type to store.</param>
    /// <returns>The metadata of the blob after the write.</returns>
    let writeLocked (key: string) (bytes: byte[]) (contentType: string) : BlobMetadata =
        let metadata =
            BlobMetadata(contentType, int64 bytes.Length, Ulid.NewUlid().ToString())

        FileSystemAtomic.writeFileAtomic
            stagingRootFull
            (FileSystemPaths.blobMetaPath rootFull key)
            (FileSystemManifests.blobMetaJson metadata.Etag metadata.ContentType)

        FileSystemAtomic.writeFileAtomic stagingRootFull (FileSystemPaths.blobDataPath rootFull key) bytes
        metadata

    interface IBlobStore with

        member _.Get(key, _) =
            BlobKeys.Validate key |> ignore

            lock gate (fun () ->
                let path = FileSystemPaths.blobDataPath rootFull key

                if File.Exists path then
                    File.ReadAllBytes path
                else
                    Unchecked.defaultof<byte[]>)
            |> Task.FromResult

        member _.Put(key, content, _) =
            if isNull (box content.Bytes) then
                raise (ArgumentNullException(nameof content))

            if isNull (box content.ContentType) then
                raise (ArgumentNullException(nameof content))

            BlobKeys.Validate key |> ignore

            lock gate (fun () -> writeLocked key content.Bytes content.ContentType)
            |> Task.FromResult

        member _.CompareExchange(key, content, expectedEtag, _) =
            if isNull (box content.Bytes) then
                raise (ArgumentNullException(nameof content))

            if isNull (box content.ContentType) then
                raise (ArgumentNullException(nameof content))

            BlobKeys.Validate key |> ignore

            lock gate (fun () ->
                let matches =
                    match expectedEtag, currentEtag key with
                    | null, None -> true
                    | null, Some _ -> false
                    | _, None -> false
                    | etag, Some stored -> String.Equals(etag, stored, StringComparison.Ordinal)

                if matches then
                    writeLocked key content.Bytes content.ContentType |> Some
                else
                    None)
            |> Option.toObj
            |> Task.FromResult

        member _.OpenRead(key, _) =
            task {
                BlobKeys.Validate key |> ignore

                return
                    lock gate (fun () ->
                        let path = FileSystemPaths.blobDataPath rootFull key

                        if File.Exists path then
                            File.OpenRead path :> Stream
                        else
                            raise (FileNotFoundException(sprintf "No blob exists under key %s." key, key)))
            }

        member _.OpenWrite(key, contentType, _) =
            task {
                if isNull (box contentType) then
                    raise (ArgumentNullException(nameof contentType))

                BlobKeys.Validate key |> ignore

                return
                    new FileSystemCommitStream(fun bytes ->
                        lock gate (fun () -> writeLocked key bytes contentType |> ignore))
                    :> Stream
            }

        member _.List(prefix, _) =
            BlobKeys.ValidatePrefix prefix |> ignore

            lock gate (fun () ->
                let blobsRoot = Path.Combine(rootFull, FileSystemPaths.blobsDirectoryName)

                Directory.EnumerateFiles(blobsRoot, "*", SearchOption.AllDirectories)
                |> Seq.map (FileSystemPaths.blobKeyOfFile rootFull)
                |> Seq.filter (fun key -> key.StartsWith(prefix, StringComparison.Ordinal))
                |> Seq.sortWith (fun left right -> String.CompareOrdinal(left, right))
                |> Seq.toList)
            |> FileSystemAsync.ofList

        member _.DeletePrefix(prefix, _) =
            task {
                BlobKeys.ValidatePrefix prefix |> ignore

                return
                    lock gate (fun () ->
                        let blobsRoot = Path.Combine(rootFull, FileSystemPaths.blobsDirectoryName)

                        let doomed =
                            Directory.EnumerateFiles(blobsRoot, "*", SearchOption.AllDirectories)
                            |> Seq.map (FileSystemPaths.blobKeyOfFile rootFull)
                            |> Seq.filter (fun key -> key.StartsWith(prefix, StringComparison.Ordinal))
                            |> Seq.toList

                        for key in doomed do
                            let dataPath = FileSystemPaths.blobDataPath rootFull key
                            let metaPath = FileSystemPaths.blobMetaPath rootFull key
                            FileSystemAtomic.tryDeleteFile dataPath
                            FileSystemAtomic.tryDeleteFile metaPath
                            FileSystemAtomic.removeEmptyParents blobsRoot dataPath

                            FileSystemAtomic.removeEmptyParents
                                (Path.Combine(rootFull, FileSystemPaths.blobMetaDirectoryName))
                                metaPath

                        doomed.Length)
            }

        member _.GetMetadata(key, _) =
            task {
                BlobKeys.Validate key |> ignore

                return
                    lock gate (fun () ->
                        let dataPath = FileSystemPaths.blobDataPath rootFull key

                        if not (File.Exists dataPath) then
                            Unchecked.defaultof<BlobMetadata>
                        else
                            let size = (FileInfo dataPath).Length
                            let metaPath = FileSystemPaths.blobMetaPath rootFull key

                            let etag, contentType =
                                if File.Exists metaPath then
                                    match FileSystemManifests.tryReadBlobMeta (File.ReadAllBytes metaPath) with
                                    | Some(etag, contentType) -> etag, contentType
                                    | None -> Ulid.NewUlid().ToString(), "application/octet-stream"
                                else
                                    Ulid.NewUlid().ToString(), "application/octet-stream"

                            BlobMetadata(contentType, size, etag))
            }

        member _.TryGetPresignedUrl(key, expiry, _) =
            task {
                BlobKeys.Validate key |> ignore

                match presignPair with
                | None -> return Unchecked.defaultof<Uri>
                | Some(baseUrl, signingKey) ->
                    return FileSystemSigning.signUrl baseUrl signingKey key (DateTimeOffset.UtcNow.Add expiry)
            }
