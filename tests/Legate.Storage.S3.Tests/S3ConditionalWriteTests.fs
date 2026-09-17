// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.S3.Tests

open System
open System.Net
open System.Threading
open System.Threading.Tasks
open Amazon.Runtime
open Amazon.S3
open Amazon.S3.Model
open Legate
open Legate.Storage.S3
open Xunit

// An S3 client whose endpoint rejects conditional writes, throwing
// exactly what a non-supporting endpoint returns for a conditional put
// (a 400 carrying InvalidRequest) and reporting every other key absent.
// MinIO, like most S3-compatible endpoints, evaluates the preconditions
// instead of rejecting them, so no real endpoint deterministically
// produces this rejection: the stub pins the fail-fast mapping the live
// suites cannot.

// ──────────────────────────────────────────────────────────────────────────
// The stub

/// <summary>
/// An S3 client stub behind the fail-fast fact: conditional puts throw
/// the rejection, reads report absent. Test-only.
/// </summary>
type NotSupportingS3Client() =
    inherit
        AmazonS3Client(
            BasicAWSCredentials("test-key", "test-secret"),
            AmazonS3Config(ServiceURL = "http://127.0.0.1:9", ForcePathStyle = true, AuthenticationRegion = "us-east-1")
        )

    /// Reports the bucket missing, so the store attempts a create
    /// before the conditional put.
    override _.HeadBucketAsync
        (_request: HeadBucketRequest, _cancellationToken: CancellationToken)
        : Task<HeadBucketResponse> =
        Task.FromException<HeadBucketResponse>(
            AmazonS3Exception(
                "No such bucket.",
                ErrorType.Sender,
                "NoSuchBucket",
                "request-id",
                HttpStatusCode.NotFound
            )
        )

    /// Accepts the bucket create the missing head triggers.
    override _.PutBucketAsync
        (_request: PutBucketRequest, _cancellationToken: CancellationToken)
        : Task<PutBucketResponse> =
        Task.FromResult(new PutBucketResponse())

    /// Rejects the conditional put the way a non-supporting endpoint
    /// does: a 400 carrying InvalidRequest.
    override _.PutObjectAsync
        (_request: PutObjectRequest, _cancellationToken: CancellationToken)
        : Task<PutObjectResponse> =
        Task.FromException<PutObjectResponse>(
            AmazonS3Exception(
                "The S3 endpoint does not support conditional writes.",
                ErrorType.Sender,
                "InvalidRequest",
                "request-id",
                HttpStatusCode.BadRequest
            )
        )

    /// Reports every key absent: nothing was ever written.
    override _.GetObjectAsync
        (_request: GetObjectRequest, _cancellationToken: CancellationToken)
        : Task<GetObjectResponse> =
        Task.FromException<GetObjectResponse>(
            AmazonS3Exception("No such key.", ErrorType.Sender, "NoSuchKey", "request-id", HttpStatusCode.NotFound)
        )

// ──────────────────────────────────────────────────────────────────────────
// The fact

/// Docker-free fail-fast behavior: a conditional write against an
/// endpoint that rejects the precondition throws the typed error and
/// writes nothing, with no read-modify-write fallback.
module S3ConditionalWriteTests =

    [<Fact>]
    let ``Conditional write against a non-supporting endpoint fails fast without writing`` () =
        task {
            use client = new NotSupportingS3Client()

            let options = S3StorageOptions()
            options.ServiceUrl <- "http://127.0.0.1:9"
            options.Bucket <- "legate-probe"
            options.AccessKeyId <- "test-key"
            options.SecretAccessKey <- "test-secret"
            options.ForcePathStyle <- true

            let store = S3BlobStore(options, client) :> IBlobStore

            let content =
                BlobContent(System.Text.Encoding.UTF8.GetBytes "v1", "application/json")

            let! _ =
                Assert.ThrowsAsync<S3ConditionalWriteNotSupportedException>(fun () ->
                    store.CompareExchange("cx/legacy", content, null, CancellationToken.None) :> Task)

            let! read = store.Get("cx/legacy", CancellationToken.None)
            Assert.Null(read)
        }
