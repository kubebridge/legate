// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.S3.Tests

open System
open System.Threading
open System.Threading.Tasks
open Legate
open Legate.Storage.S3
open Xunit

// Docker-free presign behavior: a credentialless configuration presigns
// to null without touching the network, and a non-positive expiry is a
// caller error before any signing. Positive presigning runs against
// MinIO in the Docker-backed blob suites.
module S3PresignTests =

    /// Builds a store over an endpoint nothing listens on: any test here
    /// must pass without a single packet leaving the process.
    let private unreachableStore (configure: S3StorageOptions -> unit) : S3BlobStore =
        let options = S3StorageOptions()
        options.ServiceUrl <- "http://127.0.0.1:9"
        options.Bucket <- "legate-probe"
        options.ForcePathStyle <- true
        configure options
        S3BlobStore(options, S3Client.buildClient options)

    [<Fact>]
    let ``Credentialless configuration presigns to null`` () =
        task {
            let store = unreachableStore (fun _ -> ())

            let! url =
                (store :> IBlobStore)
                    .TryGetPresignedUrl("conformance/etag", TimeSpan.FromMinutes 5., CancellationToken.None)

            Assert.Null(url)
        }

    [<Fact>]
    let ``Non-positive presign expiry throws before signing`` () =
        task {
            let store = unreachableStore (fun _ -> ())

            let! _ =
                Assert.ThrowsAsync<ArgumentOutOfRangeException>(fun () ->
                    (store :> IBlobStore).TryGetPresignedUrl("conformance/etag", TimeSpan.Zero, CancellationToken.None)
                    :> Task)

            ()
        }

// ──────────────────────────────────────────────────────────────────────────
// Shared presign assertions

/// Helpers behind the Docker-backed presign facts: the SDK rounds the
/// expiry to whole seconds on the wire, so facts assert ranges, not
/// exact instants.
module S3PresignSupport =

    /// Reads the X-Amz-Expires seconds off a presigned URL.
    /// <param name="address">The presigned URL.</param>
    /// <returns>The expiry seconds the URL carries.</returns>
    let expiresSeconds (address: Uri) =
        address.Query.TrimStart('?').Split('&')
        |> Array.pick (fun part ->
            if part.StartsWith("X-Amz-Expires=", StringComparison.Ordinal) then
                Some(Int32.Parse(part.Substring("X-Amz-Expires=".Length), Globalization.CultureInfo.InvariantCulture))
            else
                None)
