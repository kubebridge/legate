// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.S3.Tests

open System
open System.Collections.Generic
open System.Net
open System.Threading
open System.Threading.Tasks
open Amazon.Runtime
open Amazon.S3
open Amazon.S3.Model
open Legate.Storage.S3
open Xunit

// Docker-free pins for the delete mapping behind S3Client: SDK-subclass
// stubs returning canned DeleteObjects/DeleteObject responses (plus
// missing-bucket and denied throws) driven through
// S3Client.deleteBatchAsync/deleteOneAsync directly. The Docker-backed
// conformance facts carry the live fail-before proof on Linux; these
// facts pin the confirmed-count, rejected-keys, and idempotent-missing
// mapping on every leg, including the Windows leg that never attempts
// a container.

// ──────────────────────────────────────────────────────────────────────────
// The stubs

/// <summary>
/// The base client behind the delete stubs: explicit credentials against
/// a black-hole endpoint. Construction never touches the network; every
/// fact overrides only the delete operations it pins.
/// </summary>
type DeleteStubBase() =
    inherit
        AmazonS3Client(
            BasicAWSCredentials("test-key", "test-secret"),
            AmazonS3Config(ServiceURL = "http://127.0.0.1:9", ForcePathStyle = true, AuthenticationRegion = "us-east-1")
        )

/// <summary>
/// Confirms every requested key: the batch response carries one deleted
/// entry per key, and the single delete succeeds. Test-only.
/// </summary>
type ConfirmDeleteStub() =
    inherit DeleteStubBase()

    override _.DeleteObjectsAsync
        (request: DeleteObjectsRequest, _cancellationToken: CancellationToken)
        : Task<DeleteObjectsResponse> =
        let entries = List<DeletedObject>()

        for keyVersion in request.Objects do
            let entry = DeletedObject()
            entry.Key <- keyVersion.Key
            entries.Add entry

        let response = DeleteObjectsResponse()
        response.DeletedObjects <- entries
        response.DeleteErrors <- List<DeleteError>()
        Task.FromResult(response)

    override _.DeleteObjectAsync
        (_request: DeleteObjectRequest, _cancellationToken: CancellationToken)
        : Task<DeleteObjectResponse> =
        Task.FromResult(new DeleteObjectResponse())

/// <summary>
/// Rejects the batch: the response carries one delete error naming the
/// locked key. Test-only.
/// </summary>
type RejectedDeleteStub() =
    inherit DeleteStubBase()

    override _.DeleteObjectsAsync
        (_request: DeleteObjectsRequest, _cancellationToken: CancellationToken)
        : Task<DeleteObjectsResponse> =
        let failure = DeleteError()
        failure.Key <- "blobs/locked"
        failure.Code <- "AccessDenied"
        failure.Message <- "Access denied."

        let errors = List<DeleteError>()
        errors.Add failure

        let response = DeleteObjectsResponse()
        response.DeletedObjects <- List<DeletedObject>()
        response.DeleteErrors <- errors
        Task.FromResult(response)

    override _.DeleteObjectAsync
        (_request: DeleteObjectRequest, _cancellationToken: CancellationToken)
        : Task<DeleteObjectResponse> =
        Task.FromResult(new DeleteObjectResponse())

/// <summary>
/// Reports the bucket missing on batch deletes and the key missing on
/// single deletes: deletes are idempotent, so both complete as zero or
/// unit. Test-only.
/// </summary>
type MissingDeleteStub() =
    inherit DeleteStubBase()

    override _.DeleteObjectsAsync
        (_request: DeleteObjectsRequest, _cancellationToken: CancellationToken)
        : Task<DeleteObjectsResponse> =
        Task.FromException<DeleteObjectsResponse>(
            AmazonS3Exception(
                "No such bucket.",
                ErrorType.Sender,
                "NoSuchBucket",
                "request-id",
                HttpStatusCode.NotFound
            )
        )

    override _.DeleteObjectAsync
        (_request: DeleteObjectRequest, _cancellationToken: CancellationToken)
        : Task<DeleteObjectResponse> =
        Task.FromException<DeleteObjectResponse>(
            AmazonS3Exception("No such key.", ErrorType.Sender, "NoSuchKey", "request-id", HttpStatusCode.NotFound)
        )

/// <summary>
/// Denies every delete with a non-missing failure: the mapping wraps it
/// in the typed storage error. Test-only.
/// </summary>
type DeniedDeleteStub() =
    inherit DeleteStubBase()

    override _.DeleteObjectsAsync
        (_request: DeleteObjectsRequest, _cancellationToken: CancellationToken)
        : Task<DeleteObjectsResponse> =
        Task.FromException<DeleteObjectsResponse>(
            AmazonS3Exception(
                "Access denied.",
                ErrorType.Sender,
                "AccessDenied",
                "request-id",
                HttpStatusCode.Forbidden
            )
        )

    override _.DeleteObjectAsync
        (_request: DeleteObjectRequest, _cancellationToken: CancellationToken)
        : Task<DeleteObjectResponse> =
        Task.FromException<DeleteObjectResponse>(
            AmazonS3Exception(
                "Access denied.",
                ErrorType.Sender,
                "AccessDenied",
                "request-id",
                HttpStatusCode.Forbidden
            )
        )

// ──────────────────────────────────────────────────────────────────────────
// The facts

/// Docker-free delete-mapping pins: confirmed counts, rejected-key
/// errors, and idempotent missing handling through the client boundary
/// directly.
module S3DeleteStubTests =

    [<Fact>]
    let ``DeleteBatch returns the service confirmed count`` () =
        task {
            use client = new ConfirmDeleteStub()

            let! confirmed =
                S3Client.deleteBatchAsync client "legate-bucket" [ "blobs/a"; "blobs/b" ] CancellationToken.None

            Assert.Equal(2, confirmed)
        }

    [<Fact>]
    let ``DeleteBatch surfaces rejected keys as a storage error`` () =
        task {
            use client = new RejectedDeleteStub()

            let! ex =
                Assert.ThrowsAsync<S3StorageException>(fun () ->
                    S3Client.deleteBatchAsync client "legate-bucket" [ "blobs/locked" ] CancellationToken.None :> Task)

            Assert.Contains("blobs/locked", ex.Message)
        }

    [<Fact>]
    let ``DeleteBatch on a missing bucket deletes nothing`` () =
        task {
            use client = new MissingDeleteStub()

            let! confirmed = S3Client.deleteBatchAsync client "legate-missing" [ "blobs/a" ] CancellationToken.None

            Assert.Equal(0, confirmed)
        }

    [<Fact>]
    let ``DeleteOne completes on success`` () =
        task {
            use client = new ConfirmDeleteStub()
            do! S3Client.deleteOneAsync client "legate-bucket" "blobs/a" CancellationToken.None
        }

    [<Fact>]
    let ``DeleteOne swallows a missing key`` () =
        task {
            use client = new MissingDeleteStub()
            do! S3Client.deleteOneAsync client "legate-bucket" "blobs/absent" CancellationToken.None
        }

    [<Fact>]
    let ``DeleteOne wraps unexpected failures as a storage error`` () =
        task {
            use client = new DeniedDeleteStub()

            let! _ =
                Assert.ThrowsAsync<S3StorageException>(fun () ->
                    S3Client.deleteOneAsync client "legate-bucket" "blobs/a" CancellationToken.None)

            ()
        }
