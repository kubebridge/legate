// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.S3

open System
open System.Net
open Amazon.S3

// The package-local typed error boundary. Every S3 failure surfaces as one
// of these two LegateException subtypes, never as a raw
// AmazonS3Exception: the conditional-write error when the endpoint lacks
// conditional-write support, and the storage error for every other S3
// failure. Messages carry the endpoint, bucket, key, and S3 error context,
// never credentials or blob payloads.

// ──────────────────────────────────────────────────────────────────────────
// Public typed errors

/// <summary>
/// The S3 endpoint does not support conditional writes
/// (<c>If-Match</c>/<c>If-None-Match</c> preconditions): a
/// compare-exchange, or the pointer commit behind a package publish,
/// arrived at an endpoint that rejects the precondition instead of
/// evaluating it. Nothing was written. Hosts branch on the type, never on
/// the message; there is no read-modify-write fallback.
/// </summary>
/// <param name="endpoint">The service endpoint the failing call targeted, or null for the AWS default endpoint chain. Never contains credentials.</param>
/// <param name="bucket">The bucket the failing call targeted.</param>
/// <param name="message">The exception message, without secrets or blob payloads.</param>
[<Sealed>]
type S3ConditionalWriteNotSupportedException(endpoint: string | null, bucket: string, message: string) =
    inherit Legate.LegateException(message)

    /// <summary>
    /// The service endpoint the failing call targeted, or null for the AWS
    /// default endpoint chain. Never contains credentials.
    /// </summary>
    member _.Endpoint: string | null = endpoint

    /// <summary>
    /// The bucket the failing call targeted.
    /// </summary>
    member _.Bucket = bucket

/// <summary>
/// An S3 operation failed for a non-conditional-write reason: the single
/// boundary every store funnels <see cref="T:Amazon.S3.AmazonS3Exception" />
/// through. Hosts branch on the type, never on the message.
/// </summary>
/// <param name="bucket">The bucket the failing operation targeted.</param>
/// <param name="key">The key the failing operation targeted, or null when the failure is not tied to a specific key.</param>
/// <param name="message">The exception message, without secrets or blob payloads.</param>
[<Sealed>]
type S3StorageException(bucket: string, key: string | null, message: string) =
    inherit Legate.LegateException(message)

    /// <summary>
    /// The bucket the failing operation targeted.
    /// </summary>
    member _.Bucket = bucket

    /// <summary>
    /// The key the failing operation targeted, or null when the failure is
    /// not tied to a specific key.
    /// </summary>
    member _.Key: string | null = key

// ──────────────────────────────────────────────────────────────────────────
// Boundary mapping

/// <summary>
/// Maps data-access failures at one boundary to the package-local typed
/// errors. Internal: stores call this, hosts catch the typed errors.
/// </summary>
module internal S3Errors =

    /// <summary>
    /// Whether the S3 failure means the key is absent: a 404 with one of
    /// the missing-key codes. Missing-bucket reads report absent too: an
    /// absent bucket holds no blobs.
    /// </summary>
    /// <param name="ex">The S3 failure to test.</param>
    /// <returns>True when the failure reports a missing key or bucket; otherwise false.</returns>
    let isMissing (ex: AmazonS3Exception) =
        ex.StatusCode = HttpStatusCode.NotFound
        && (ex.ErrorCode = "NoSuchKey"
            || ex.ErrorCode = "NoSuchBucket"
            || ex.ErrorCode = "NotFound")

    /// <summary>
    /// Whether the S3 failure means our precondition did not hold: a 412,
    /// or a 409 conflict where a concurrent writer won the race. Either
    /// way nothing we sent landed.
    /// </summary>
    /// <param name="ex">The S3 failure to test.</param>
    /// <returns>True when the failure reports an unmet precondition or a lost write race; otherwise false.</returns>
    let isPreconditionFailure (ex: AmazonS3Exception) =
        ex.StatusCode = HttpStatusCode.PreconditionFailed
        || ex.StatusCode = HttpStatusCode.Conflict

    /// <summary>
    /// Whether the S3 failure means the endpoint rejects conditional
    /// writes instead of evaluating them: an explicit not-implemented or
    /// method-not-allowed status, or a bad-request carrying the invalid
    /// conditional-request code. Only ever consulted for calls that
    /// carried a precondition.
    /// </summary>
    /// <param name="ex">The S3 failure to test.</param>
    /// <returns>True when the failure reports rejected conditional-write support; otherwise false.</returns>
    let isUnsupportedConditional (ex: AmazonS3Exception) =
        ex.StatusCode = HttpStatusCode.NotImplemented
        || ex.StatusCode = HttpStatusCode.MethodNotAllowed
        || ex.StatusCode = HttpStatusCode.HttpVersionNotSupported
        || (ex.StatusCode = HttpStatusCode.BadRequest && ex.ErrorCode = "InvalidRequest")

    /// <summary>
    /// Wraps an unexpected S3 failure in the storage error, carrying the
    /// operation, bucket, key, and S3 status and code. Never embeds
    /// credentials or blob payloads.
    /// </summary>
    /// <param name="operation">The store operation that failed, for example "Put" or "UploadPackage".</param>
    /// <param name="bucket">The bucket the failing operation targeted.</param>
    /// <param name="key">The key the failing operation targeted, or null when not key-scoped.</param>
    /// <param name="ex">The S3 failure to wrap.</param>
    /// <returns>The typed storage error.</returns>
    let ofS3Exception (operation: string) (bucket: string) (key: string | null) (ex: AmazonS3Exception) =
        let keyText =
            match key with
            | null -> "no key"
            | value -> sprintf "key %s" value

        S3StorageException(
            bucket,
            key,
            sprintf "S3 %s failed on bucket %s (%s): %O %s." operation bucket keyText ex.StatusCode ex.ErrorCode
        )
