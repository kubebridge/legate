// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.S3

open System
open System.Collections.Generic
open System.IO
open System.Threading
open System.Threading.Tasks
open Amazon.S3
open Amazon.S3.Model
open Legate

// A write stream that buffers locally and commits its bytes to the bucket
// when disposed: an abandoned stream that is never disposed commits
// nothing, and a disposed stream lands atomically through one unconditional
// put. Disposing synchronously blocks on the commit; DisposeAsync commits
// without blocking.

/// <summary>
/// Buffers a blob write and commits it on dispose. Internal: OpenWrite
/// hands these out.
/// </summary>
type internal S3CommitOnDisposeStream
    (client: AmazonS3Client, gate: S3BucketGate, bucket: string, key: string, contentType: string) =
    inherit MemoryStream()

    let mutable committed = false

    /// Releases the buffer without committing: the shared tail of both
    /// dispose paths, so no closure ever touches the base directly.
    member private this.Release() = base.Dispose(true)

    /// <summary>
    /// Disposes the stream, committing the buffered bytes through one
    /// unconditional put. Blocks on the commit.
    /// </summary>
    /// <param name="disposing">Whether managed resources dispose.</param>
    override this.Dispose(disposing: bool) =
        if disposing && not committed then
            committed <- true
            let bytes = this.ToArray()

            gate.EnsureForWriteAsync(CancellationToken.None).GetAwaiter().GetResult()
            |> ignore

            S3Client.putAsync client bucket key bytes contentType CancellationToken.None
            |> fun put -> put.GetAwaiter().GetResult()
            |> ignore

        this.Release()

    /// <summary>
    /// Disposes the stream asynchronously, committing the buffered bytes
    /// through one unconditional put without blocking.
    /// </summary>
    /// <returns>A task completing when the commit and dispose finish.</returns>
    override this.DisposeAsync() : ValueTask =
        if committed then
            ValueTask()
        else
            committed <- true

            ValueTask(
                task {
                    let bytes = this.ToArray()
                    do! gate.EnsureForWriteAsync(CancellationToken.None)

                    let! _ = S3Client.putAsync client bucket key bytes contentType CancellationToken.None

                    this.Release()
                }
            )

// The S3 IBlobStore: the blob primitives over one bucket. Keys map 1:1 to
// object keys and validate through BlobKeys; compare-exchange rides the
// service preconditions (If-None-Match: * for create-if-absent, If-Match
// for etag match) with etags echoed opaquely exactly as the service
// returned them; lists page through continuation tokens and prefix
// deletions batch into service-side deletes; presigning is options-bound
// and returns null for credentialless configuration and for absent keys.

/// <summary>
/// The S3 <see cref="T:Legate.IBlobStore" /> over one bucket: keys map 1:1
/// to object keys, compare-exchange rides the service preconditions with
/// no read-modify-write fallback, and presigned URLs come from the SDK
/// presigner capped by the configured expiry.
/// </summary>
/// <param name="options">The validated storage options. Must not be null.</param>
/// <param name="client">The S3 client the store drives. Must not be null.</param>
type S3BlobStore(options: S3StorageOptions, client: AmazonS3Client) =

    do
        ArgumentNullException.ThrowIfNull(options)
        ArgumentNullException.ThrowIfNull(client)

        match options.Validate() with
        | null -> ()
        | reason -> raise (ArgumentException(reason, nameof options))

    let bucket = options.Bucket

    let endpoint: string | null =
        if options.ServiceUrl = "" then null else options.ServiceUrl

    let gate = S3BucketGate(client, bucket)

    /// <summary>
    /// The bucket the store reads and writes.
    /// </summary>
    member _.Bucket = bucket

    interface IBlobStore with

        member _.Get(key, cancellationToken) =
            task {
                BlobKeys.Validate key |> ignore

                try
                    let! bytes = S3Client.getAsync client bucket key cancellationToken
                    return bytes
                with :? LegateException as ex ->
                    return raise ex
            }

        member _.Put(key, content, cancellationToken) =
            task {
                if isNull (box content.Bytes) then
                    raise (ArgumentNullException(nameof content))

                if isNull (box content.ContentType) then
                    raise (ArgumentNullException(nameof content))

                BlobKeys.Validate key |> ignore

                try
                    do! gate.EnsureForWriteAsync cancellationToken

                    let! metadata =
                        S3Client.putAsync client bucket key content.Bytes content.ContentType cancellationToken

                    return metadata
                with :? LegateException as ex ->
                    return raise ex
            }

        member _.CompareExchange(key, content, expectedEtag, cancellationToken) =
            task {
                if isNull (box content.Bytes) then
                    raise (ArgumentNullException(nameof content))

                if isNull (box content.ContentType) then
                    raise (ArgumentNullException(nameof content))

                BlobKeys.Validate key |> ignore

                try
                    do! gate.EnsureForWriteAsync cancellationToken

                    let! exchanged =
                        S3Client.putConditionalAsync
                            client
                            endpoint
                            bucket
                            key
                            content.Bytes
                            content.ContentType
                            expectedEtag
                            cancellationToken

                    return exchanged
                with :? LegateException as ex ->
                    return raise ex
            }

        member _.OpenRead(key, cancellationToken) =
            task {
                BlobKeys.Validate key |> ignore

                try
                    let! response =
                        client.GetObjectAsync(GetObjectRequest(BucketName = bucket, Key = key), cancellationToken)

                    return new S3OwnedReadStream(response) :> Stream
                with
                | :? AmazonS3Exception as ex when S3Errors.isMissing ex ->
                    return raise (FileNotFoundException(sprintf "No blob exists under key %s." key, key))
                | :? AmazonS3Exception as ex -> return raise (S3Errors.ofS3Exception "OpenRead" bucket key ex)
            }

        member _.OpenWrite(key, contentType, _) =
            task {
                if isNull (box contentType) then
                    raise (ArgumentNullException(nameof contentType))

                BlobKeys.Validate key |> ignore

                return new S3CommitOnDisposeStream(client, gate, bucket, key, contentType) :> Stream
            }

        member _.List(prefix, cancellationToken) =
            BlobKeys.ValidatePrefix prefix |> ignore

            { new IAsyncEnumerable<string> with
                member _.GetAsyncEnumerator(innerCancellationToken: CancellationToken) : IAsyncEnumerator<string> =
                    let linked =
                        CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, innerCancellationToken)

                    let mutable fetched: string list option = None
                    let mutable index = -1

                    { new IAsyncEnumerator<string> with
                        member _.MoveNextAsync() : ValueTask<bool> =
                            task {
                                match fetched with
                                | None ->
                                    try
                                        let! keys = S3Client.listAllAsync client bucket prefix linked.Token

                                        fetched <- Some keys
                                    with :? LegateException as ex ->
                                        raise ex

                                | Some _ -> ()

                                let keys = fetched |> Option.defaultValue []

                                if index + 1 < keys.Length then
                                    index <- index + 1
                                    return true
                                else
                                    return false
                            }
                            |> fun move -> ValueTask<bool>(move)

                        member _.Current: string =
                            match fetched with
                            | Some keys -> keys[index]
                            | None -> raise (InvalidOperationException("The listing has not advanced."))

                        member _.DisposeAsync() : ValueTask =
                            linked.Dispose()
                            ValueTask()
                    }
            }

        member _.DeletePrefix(prefix, cancellationToken) =
            task {
                BlobKeys.ValidatePrefix prefix |> ignore

                try
                    let! keys = S3Client.listAllAsync client bucket prefix cancellationToken
                    let! deleted = S3Client.deleteBatchAsync client bucket keys cancellationToken
                    return deleted
                with :? LegateException as ex ->
                    return raise ex
            }

        member _.GetMetadata(key, cancellationToken) =
            task {
                BlobKeys.Validate key |> ignore

                try
                    let! metadata = S3Client.getMetadataAsync client bucket key cancellationToken
                    return metadata
                with :? LegateException as ex ->
                    return raise ex
            }

        member _.TryGetPresignedUrl(key, expiry, cancellationToken) =
            task {
                BlobKeys.Validate key |> ignore

                if expiry <= TimeSpan.Zero then
                    raise (ArgumentOutOfRangeException(nameof expiry, "The presigned-URL expiry must be positive."))

                if not (S3Client.hasCredentials options) then
                    // Credentialless configuration cannot sign: null, never an exception.
                    return Unchecked.defaultof<Uri>
                else
                    try
                        let! metadata = S3Client.getMetadataAsync client bucket key cancellationToken

                        match metadata with
                        | null ->
                            // No blob, no URL: presigning a missing key
                            // would hand out a signed 404.
                            return Unchecked.defaultof<Uri>
                        | _ ->
                            let effective = min expiry options.PresignExpiry

                            // The presigner ignores the client's HTTP
                            // switch: the request carries the endpoint's
                            // own scheme, so plain-HTTP endpoints presign
                            // plain-HTTP URLs.
                            let protocol =
                                if options.ServiceUrl.StartsWith("http://", StringComparison.OrdinalIgnoreCase) then
                                    Protocol.HTTP
                                else
                                    Protocol.HTTPS

                            let request =
                                GetPreSignedUrlRequest(
                                    BucketName = bucket,
                                    Key = key,
                                    Verb = HttpVerb.GET,
                                    Expires = DateTime.UtcNow.Add effective,
                                    Protocol = protocol
                                )

                            return new Uri(client.GetPreSignedURL(request))
                    with :? LegateException as ex ->
                        return raise ex
            }
