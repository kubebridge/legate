// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.S3

open System
open System.Collections.Generic
open System.IO
open System.Net
open System.Threading
open System.Threading.Tasks
open Amazon
open Amazon.Runtime
open Amazon.S3
open Amazon.S3.Model
open Legate

// The shared SDK boundary behind both stores: client construction from
// options, create-on-first-write bucket handling, the conditional put
// behind compare-exchange and the pointer commit, and the paged list and
// batched delete behind the prefix operations. Internal: stores call
// this, hosts see only the typed errors. Reads never create the bucket:
// a missing bucket reads as absent, and only a write ensures it.

// ──────────────────────────────────────────────────────────────────────────
// Client construction

/// <summary>
/// Builds and drives the S3 client behind the stores. Internal: the
/// registration and the test harness build through this so the credential
/// and endpoint wiring lives in one place.
/// </summary>
module internal S3Client =

    /// <summary>
    /// Whether the options carry a signing key: both parts set. A
    /// credentialless configuration presigns to null and fails data
    /// operations server-side.
    /// </summary>
    /// <param name="options">The options to inspect. Must not be null.</param>
    /// <returns>True when both credential parts are set; otherwise false.</returns>
    let hasCredentials (options: S3StorageOptions) =
        not (String.IsNullOrEmpty options.AccessKeyId)
        && not (String.IsNullOrEmpty options.SecretAccessKey)

    /// <summary>
    /// Builds the S3 client the stores drive: explicit credentials when
    /// the options carry them, anonymous otherwise; the custom service
    /// URL with the path-style switch when set, the AWS endpoint chain
    /// for the configured region otherwise. Construction never touches
    /// the network.
    /// </summary>
    /// <param name="options">The validated options to build from. Must not be null.</param>
    /// <returns>The client.</returns>
    let buildClient (options: S3StorageOptions) : AmazonS3Client =
        ArgumentNullException.ThrowIfNull(options)

        let credentials =
            if hasCredentials options then
                BasicAWSCredentials(options.AccessKeyId, options.SecretAccessKey) :> AWSCredentials
            else
                AnonymousAWSCredentials() :> AWSCredentials

        let config = new AmazonS3Config()

        if String.IsNullOrWhiteSpace options.ServiceUrl then
            config.RegionEndpoint <- RegionEndpoint.GetBySystemName options.Region
            config.ForcePathStyle <- options.ForcePathStyle
        else
            config.ServiceURL <- options.ServiceUrl
            config.ForcePathStyle <- options.ForcePathStyle

            // Plain-HTTP endpoints (local MinIO) presign plain-HTTP
            // URLs: the SDK defaults presigned URLs to HTTPS.
            if options.ServiceUrl.StartsWith("http://", StringComparison.OrdinalIgnoreCase) then
                config.UseHttp <- true

            if not (String.IsNullOrWhiteSpace options.Region) then
                config.AuthenticationRegion <- options.Region

        new AmazonS3Client(credentials, config)

    /// <summary>
    /// Adapts a list into an IAsyncEnumerable, in order.
    /// </summary>
    /// <param name="items">The items to yield.</param>
    /// <returns>The enumerable.</returns>
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

    /// <summary>
    /// Lists every key under the prefix, following continuation tokens
    /// until the service reports the listing complete, in the order the
    /// service returns (lexicographic for standard listings).
    /// </summary>
    /// <param name="client">The client to list through.</param>
    /// <param name="bucket">The bucket to list in.</param>
    /// <param name="prefix">The prefix to list under.</param>
    /// <param name="cancellationToken">Token that abandons the listing.</param>
    /// <returns>Every matching key.</returns>
    let listAllAsync
        (client: AmazonS3Client)
        (bucket: string)
        (prefix: string)
        (cancellationToken: CancellationToken)
        : Task<string list> =
        // One page behind its own error boundary: the outer loop below
        // holds no try/with, so no try encloses a loop containing let!.
        // A missing bucket surfaces as a null page: reads never create it.
        let fetchPageAsync (token: string | null) : Task<ListObjectsV2Response> =
            task {
                let request =
                    ListObjectsV2Request(BucketName = bucket, Prefix = prefix, MaxKeys = 1000)

                if not (isNull token) then
                    request.ContinuationToken <- token

                try
                    let! response = client.ListObjectsV2Async(request, cancellationToken)
                    return response
                with
                | :? AmazonS3Exception as ex when S3Errors.isMissing ex ->
                    // A missing bucket lists as empty: reads never create it.
                    return Unchecked.defaultof<ListObjectsV2Response>
                | :? AmazonS3Exception as ex -> return raise (S3Errors.ofS3Exception "List" bucket prefix ex)
            }

        task {
            let collected = ResizeArray<string>()
            let mutable token: string | null = null
            let mutable more = true

            while more do
                let! response = fetchPageAsync token

                // A null page is the missing-bucket sentinel: keep what
                // the earlier pages collected and stop.
                if isNull (box response) then
                    more <- false
                else
                    // The SDK leaves the entries null on an empty page.
                    if not (isNull (box response.S3Objects)) then
                        for entry in response.S3Objects do
                            collected.Add entry.Key

                    if response.IsTruncated.GetValueOrDefault() then
                        token <- response.NextContinuationToken
                    else
                        more <- false

            return collected |> Seq.toList
        }

    /// <summary>
    /// Deletes the keys in service-side batches of at most a thousand,
    /// returning how many the service confirms deleted. A batch the
    /// service partially rejects throws the typed storage error naming
    /// the rejected keys.
    /// </summary>
    /// <param name="client">The client to delete through.</param>
    /// <param name="bucket">The bucket to delete in.</param>
    /// <param name="keys">The keys to delete.</param>
    /// <param name="cancellationToken">Token that abandons the deletion.</param>
    /// <returns>How many keys the service confirms deleted.</returns>
    let deleteBatchAsync
        (client: AmazonS3Client)
        (bucket: string)
        (keys: string list)
        (cancellationToken: CancellationToken)
        : Task<int> =
        // One chunk behind its own error boundary: the outer loop below
        // holds no try/with, so no try encloses a loop containing let!.
        // (A try wrapping a for-loop with let! inside task emits IL the
        // Linux JIT rejects with InvalidProgramException.)
        let deleteChunkAsync (chunk: string list) : Task<int> =
            task {
                let request = DeleteObjectsRequest(BucketName = bucket)
                request.Objects <- new List<KeyVersion>(chunk |> Seq.map (fun key -> KeyVersion(Key = key)))

                try
                    let! response = client.DeleteObjectsAsync(request, cancellationToken)

                    // The SDK leaves result collections null when empty:
                    // a quiet service reports nothing to reject and
                    // nothing to confirm beyond the request itself.
                    let rejected =
                        if isNull (box response.DeleteErrors) then
                            []
                        else
                            response.DeleteErrors
                            |> Seq.map (fun error -> sprintf "%s (%s)" error.Key error.Code)
                            |> Seq.toList

                    if not rejected.IsEmpty then
                        raise (
                            S3StorageException(
                                bucket,
                                null,
                                sprintf
                                    "S3 DeletePrefix failed on bucket %s: the service rejected %s."
                                    bucket
                                    (rejected |> String.concat ", ")
                            )
                        )

                    if isNull (box response.DeletedObjects) then
                        return chunk.Length - rejected.Length
                    else
                        return response.DeletedObjects.Count
                with
                | :? AmazonS3Exception as ex when S3Errors.isMissing ex ->
                    // A missing bucket deletes nothing: reads never create it.
                    return 0
                | :? AmazonS3Exception as ex -> return raise (S3Errors.ofS3Exception "DeletePrefix" bucket null ex)
            }

        task {
            let mutable deleted = 0

            for chunk in keys |> List.chunkBySize 1000 do
                let! confirmed = deleteChunkAsync chunk
                deleted <- deleted + confirmed

            return deleted
        }

    /// <summary>
    /// Deletes one key, swallowing the missing key or bucket: deletes
    /// are idempotent, and reads never create the bucket.
    /// </summary>
    /// <param name="client">The client to delete through.</param>
    /// <param name="bucket">The bucket to delete in.</param>
    /// <param name="key">The key to delete.</param>
    /// <param name="cancellationToken">Token that abandons the deletion.</param>
    /// <returns>A task completing when the deletion finishes.</returns>
    let deleteOneAsync
        (client: AmazonS3Client)
        (bucket: string)
        (key: string)
        (cancellationToken: CancellationToken)
        : Task =
        task {
            try
                let! _ =
                    client.DeleteObjectAsync(DeleteObjectRequest(BucketName = bucket, Key = key), cancellationToken)

                ()
            with
            | :? AmazonS3Exception as ex when S3Errors.isMissing ex -> ()
            | :? AmazonS3Exception as ex -> raise (S3Errors.ofS3Exception "Delete" bucket key ex)
        }

    /// <summary>
    /// Writes the bytes unconditionally, creating or overwriting the key.
    /// </summary>
    /// <param name="client">The client to write through.</param>
    /// <param name="bucket">The bucket to write in.</param>
    /// <param name="key">The key to write under.</param>
    /// <param name="bytes">The payload.</param>
    /// <param name="contentType">The content type to store with the blob.</param>
    /// <param name="cancellationToken">Token that abandons the write.</param>
    /// <returns>The metadata of the blob after the write.</returns>
    let putAsync
        (client: AmazonS3Client)
        (bucket: string)
        (key: string)
        (bytes: byte[])
        (contentType: string)
        (cancellationToken: CancellationToken)
        : Task<BlobMetadata> =
        task {
            use stream = new MemoryStream(bytes, false)

            let request =
                PutObjectRequest(BucketName = bucket, Key = key, InputStream = stream, ContentType = contentType)

            try
                let! response = client.PutObjectAsync(request, cancellationToken)
                return BlobMetadata(contentType, int64 bytes.Length, response.ETag)
            with :? AmazonS3Exception as ex ->
                return raise (S3Errors.ofS3Exception "Put" bucket key ex)
        }

    /// <summary>
    /// Writes the bytes only when the precondition holds: a null expected
    /// etag sends <c>If-None-Match: *</c> (create-if-absent), otherwise the
    /// observed etag travels as <c>If-Match</c>. The etag is opaque to the
    /// store: whatever the service returned on the last write is echoed
    /// back verbatim. A failed precondition writes nothing and returns
    /// null; an endpoint that rejects the precondition fails fast with
    /// the typed conditional-write error and writes nothing. There is no
    /// read-modify-write fallback.
    /// </summary>
    /// <param name="client">The client to write through.</param>
    /// <param name="endpoint">The service endpoint for error context, or null for the AWS default chain.</param>
    /// <param name="bucket">The bucket to write in.</param>
    /// <param name="key">The key to write under.</param>
    /// <param name="bytes">The payload.</param>
    /// <param name="contentType">The content type to store with the blob.</param>
    /// <param name="expectedEtag">The etag the caller last observed, or null to require absence.</param>
    /// <param name="cancellationToken">Token that abandons the exchange.</param>
    /// <returns>The metadata written, or null when the precondition did not hold.</returns>
    let putConditionalAsync
        (client: AmazonS3Client)
        (endpoint: string | null)
        (bucket: string)
        (key: string)
        (bytes: byte[])
        (contentType: string)
        (expectedEtag: string | null)
        (cancellationToken: CancellationToken)
        : Task<BlobMetadata | null> =
        task {
            use stream = new MemoryStream(bytes, false)

            let request =
                PutObjectRequest(BucketName = bucket, Key = key, InputStream = stream, ContentType = contentType)

            match expectedEtag with
            | null -> request.IfNoneMatch <- "*"
            | etag -> request.IfMatch <- etag

            try
                let! response = client.PutObjectAsync(request, cancellationToken)
                return BlobMetadata(contentType, int64 bytes.Length, response.ETag)
            with
            | :? AmazonS3Exception as ex when S3Errors.isPreconditionFailure ex ->
                return Unchecked.defaultof<BlobMetadata>
            | :? AmazonS3Exception as ex when S3Errors.isUnsupportedConditional ex ->
                let endpointText =
                    match endpoint with
                    | null -> "the AWS default endpoint chain"
                    | value -> value

                return
                    raise (
                        S3ConditionalWriteNotSupportedException(
                            endpoint,
                            bucket,
                            sprintf
                                "The S3 endpoint %s rejected the conditional write on bucket %s (%O %s): conditional writes are not supported and no fallback applies."
                                endpointText
                                bucket
                                ex.StatusCode
                                ex.ErrorCode
                        )
                    )
        }

    /// <summary>
    /// Reads the whole blob. Returns null when the key (or bucket) is
    /// absent.
    /// </summary>
    /// <param name="client">The client to read through.</param>
    /// <param name="bucket">The bucket to read in.</param>
    /// <param name="key">The key to read.</param>
    /// <param name="cancellationToken">Token that abandons the read.</param>
    /// <returns>The payload, or null when absent.</returns>
    let getAsync
        (client: AmazonS3Client)
        (bucket: string)
        (key: string)
        (cancellationToken: CancellationToken)
        : Task<byte[] | null> =
        task {
            try
                use! response =
                    client.GetObjectAsync(GetObjectRequest(BucketName = bucket, Key = key), cancellationToken)

                use memory = new MemoryStream()
                do! response.ResponseStream.CopyToAsync(memory, cancellationToken)
                return memory.ToArray()
            with
            | :? AmazonS3Exception as ex when S3Errors.isMissing ex -> return Unchecked.defaultof<byte[]>
            | :? AmazonS3Exception as ex -> return raise (S3Errors.ofS3Exception "Get" bucket key ex)
        }

    /// <summary>
    /// Reads the blob metadata. Returns null when the key (or bucket) is
    /// absent.
    /// </summary>
    /// <param name="client">The client to read through.</param>
    /// <param name="bucket">The bucket to read in.</param>
    /// <param name="key">The key to describe.</param>
    /// <param name="cancellationToken">Token that abandons the read.</param>
    /// <returns>The metadata, or null when absent.</returns>
    let getMetadataAsync
        (client: AmazonS3Client)
        (bucket: string)
        (key: string)
        (cancellationToken: CancellationToken)
        : Task<BlobMetadata | null> =
        task {
            try
                let! response =
                    client.GetObjectMetadataAsync(
                        GetObjectMetadataRequest(BucketName = bucket, Key = key),
                        cancellationToken
                    )

                return BlobMetadata(response.Headers.ContentType, response.ContentLength, response.ETag)
            with
            | :? AmazonS3Exception as ex when S3Errors.isMissing ex -> return Unchecked.defaultof<BlobMetadata>
            | :? AmazonS3Exception as ex -> return raise (S3Errors.ofS3Exception "GetMetadata" bucket key ex)
        }

// ──────────────────────────────────────────────────────────────────────────
// Create-on-first-write bucket handling

/// <summary>
/// Ensures the bucket once per store instance, on the write path only:
/// a head naming a missing bucket creates it, a forbidden head proceeds
/// (the operation itself then reports the truth), and reads never create
/// anything. Internal: every store holds one.
/// </summary>
/// <param name="client">The client to ensure through.</param>
/// <param name="bucket">The bucket to ensure.</param>
type internal S3BucketGate(client: AmazonS3Client, bucket: string) =

    do
        ArgumentNullException.ThrowIfNull(client)

        if String.IsNullOrWhiteSpace bucket then
            raise (ArgumentException("The bucket must be a non-empty name.", nameof bucket))

    let gate = obj ()
    let mutable ensured = false

    /// <summary>
    /// Ensures the bucket exists, at most once per gate: heads a missing
    /// bucket into existence and lets every other outcome through to the
    /// operation.
    /// </summary>
    /// <param name="cancellationToken">Token that abandons the ensure.</param>
    /// <returns>A task completing when the bucket is ensured.</returns>
    member _.EnsureForWriteAsync(cancellationToken: CancellationToken) : Task =
        task {
            let attempt =
                lock gate (fun () ->
                    if ensured then
                        false
                    else
                        ensured <- true
                        true)

            if attempt then
                try
                    let! _ = client.HeadBucketAsync(HeadBucketRequest(BucketName = bucket), cancellationToken)
                    ()
                with
                | :? AmazonS3Exception as ex when ex.StatusCode = HttpStatusCode.NotFound ->
                    let! _ = client.PutBucketAsync(PutBucketRequest(BucketName = bucket), cancellationToken)
                    ()
                | :? AmazonS3Exception ->
                    // Exists-but-forbidden, or a transient head failure: the
                    // write itself reports the truth, wrapped at its own
                    // boundary.
                    ()
        }

// ──────────────────────────────────────────────────────────────────────────
// Owned read stream

/// <summary>
/// A read stream over one S3 object that owns its get response: disposing
/// the stream disposes the response, so the connection cannot leak past
/// the read. Internal: OpenRead hands these out.
/// </summary>
/// <param name="response">The get response owning the stream. Must not be null.</param>
type internal S3OwnedReadStream(response: GetObjectResponse) =
    inherit Stream()

    do ArgumentNullException.ThrowIfNull(response)

    let backing = response.ResponseStream

    /// <summary>
    /// Whether the stream supports reading: always true.
    /// </summary>
    override _.CanRead = true

    /// <summary>
    /// Whether the stream supports seeking: whatever the response stream reports.
    /// </summary>
    override _.CanSeek = backing.CanSeek

    /// <summary>
    /// Whether the stream supports writing: always false.
    /// </summary>
    override _.CanWrite = false

    /// <summary>
    /// The stream length: whatever the response stream reports.
    /// </summary>
    override _.Length = backing.Length

    /// <summary>
    /// The position in the stream.
    /// </summary>
    override _.Position
        with get () = backing.Position
        and set (value) = backing.Position <- value

    /// <summary>
    /// Flushes the stream: a read stream flush is a no-op over the backing stream.
    /// </summary>
    override _.Flush() = backing.Flush()

    /// <summary>
    /// Reads into the buffer.
    /// </summary>
    /// <param name="buffer">The buffer to read into.</param>
    /// <param name="offset">The offset into the buffer.</param>
    /// <param name="count">How many bytes to read at most.</param>
    /// <returns>How many bytes were read.</returns>
    override _.Read(buffer: byte[], offset: int, count: int) : int = backing.Read(buffer, offset, count)

    /// <summary>
    /// Reads into the buffer asynchronously.
    /// </summary>
    /// <param name="buffer">The buffer to read into.</param>
    /// <param name="offset">The offset into the buffer.</param>
    /// <param name="count">How many bytes to read at most.</param>
    /// <param name="cancellationToken">Token that abandons the read.</param>
    /// <returns>How many bytes were read.</returns>
    override _.ReadAsync(buffer: byte[], offset: int, count: int, cancellationToken: CancellationToken) : Task<int> =
        backing.ReadAsync(buffer, offset, count, cancellationToken)

    /// <summary>
    /// Copies to the destination asynchronously.
    /// </summary>
    /// <param name="destination">The stream to copy into.</param>
    /// <param name="bufferSize">The buffer size.</param>
    /// <param name="cancellationToken">Token that abandons the copy.</param>
    /// <returns>A task completing when the copy finishes.</returns>
    override _.CopyToAsync(destination: Stream, bufferSize: int, cancellationToken: CancellationToken) : Task =
        backing.CopyToAsync(destination, bufferSize, cancellationToken)

    /// <summary>
    /// Seeks within the stream.
    /// </summary>
    /// <param name="offset">The offset.</param>
    /// <param name="origin">The origin.</param>
    /// <returns>The new position.</returns>
    override _.Seek(offset: int64, origin: SeekOrigin) : int64 = backing.Seek(offset, origin)

    /// <summary>
    /// Sets the stream length: unsupported on a read stream.
    /// </summary>
    override _.SetLength(_: int64) : unit =
        raise (NotSupportedException("An S3 read stream cannot change length."))

    /// <summary>
    /// Writes into the stream: unsupported on a read stream.
    /// </summary>
    override _.Write(_: byte[], _: int, _: int) : unit =
        raise (NotSupportedException("An S3 read stream is read-only."))

    /// <summary>
    /// Disposes the stream and the get response owning it.
    /// </summary>
    /// <param name="disposing">Whether managed resources dispose.</param>
    override _.Dispose(disposing: bool) =
        if disposing then
            response.Dispose()

        base.Dispose disposing
