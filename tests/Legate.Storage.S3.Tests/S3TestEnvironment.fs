// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.S3.Tests

open System
open System.Runtime.InteropServices
open System.Threading
open System.Threading.Tasks
open Amazon.S3
open Amazon.S3.Model
open DotNet.Testcontainers.Images
open Legate
open Legate.Storage.S3
open Legate.Testing
open Testcontainers.Minio

// Shared Testcontainers harness for the S3 suites: one MinIO container
// per image per test run, and one fresh bucket per test-class instance.
//
// xUnit 2.9 has no dynamic-skip mechanism (verified against the 2.9.3
// runner assemblies: no SkipException handling, inaccessible
// SkipException constructors, no Assert.Skip), so suites that need a
// container are compiled out on Windows (see the fsproj conditions) and
// the Windows CI leg never attempts them. The Linux CI leg runs
// everything for real against Docker. Where Docker is unavailable the
// gate fails loudly with the remedy instead of passing vacuously.
// Undisposed containers are reaped by the Testcontainers resource reaper
// when the test process exits.
//
// Isolation is per bucket, not per table: every test-class instance mints
// a fresh bucket, so facts never share keys even where the conformance
// suites reuse fixed key names.
module S3TestEnvironment =

    let private gate = obj ()

    let mutable private cached: (string * string * string) option = None
    let mutable private skipped: string option = None

    /// The pinned server image the suites run against. Recent enough to
    /// evaluate conditional writes. MinIO left Docker Hub in 2025, so the
    /// pin lives on Quay.
    let MinioImage = "quay.io/minio/minio:RELEASE.2025-04-22T22-12-26Z"

    /// The test credentials minted into every container.
    let AccessKey = "legate-test-key"

    /// The test secret minted into every container. Test-only.
    let SecretKey = "legate-test-secret-key"

    /// Starts one container for the image and returns its endpoint plus
    /// the credentials it was minted with. Pulls only when the image is
    /// absent locally, so cached pins never touch the registry.
    let private startContainer (image: string) : string * string * string =
        let container =
            MinioBuilder(image)
                .WithUsername(AccessKey)
                .WithPassword(SecretKey)
                .WithImagePullPolicy(PullPolicy.Missing)
                .Build()

        try
            container.StartAsync().GetAwaiter().GetResult() |> ignore
            (container.GetConnectionString(), container.GetAccessKey(), container.GetSecretKey())
        with ex ->
            raise (InvalidOperationException($"The MinIO test container failed to start: {ex.Message}", ex))

    /// Fails the calling fact with the reason: xUnit 2.9 cannot report a
    /// dynamic skip, so an unavailable container is a loud failure naming
    /// the remedy, never a vacuous pass.
    let private failGate (reason: string) : 'T =
        raise (InvalidOperationException(reason))

    /// Whether the host deliberately opts into containers on Windows
    /// (LEGATE_S3_DOCKER=1 with Linux-container Docker): the only way
    /// the suites attempt a container there.
    let private windowsOptIn () =
        match Environment.GetEnvironmentVariable("LEGATE_S3_DOCKER") with
        | null -> false
        | value ->
            let normalized = value.Trim().ToLowerInvariant()
            normalized = "1" || normalized = "true"

    /// Returns the shared main-container endpoint and credentials,
    /// starting it once. Fails loudly with the remedy where Docker is
    /// unavailable; Windows without the opt-in indicates a
    /// compile-exclusion fault (the Docker suites do not compile there
    /// by default).
    let ensureReady () : string * string * string =
        lock gate (fun () ->
            match skipped, cached with
            | Some reason, _ -> failGate reason
            | None, Some endpoint -> endpoint
            | None, None ->
                if RuntimeInformation.IsOSPlatform(OSPlatform.Windows) && not (windowsOptIn ()) then
                    let reason =
                        "S3 Testcontainers suites do not run on Windows without LEGATE_S3_DOCKER=1: the Windows CI leg never attempts them."

                    skipped <- Some reason
                    failGate reason
                else
                    try
                        let endpoint = startContainer MinioImage
                        cached <- Some endpoint
                        endpoint
                    with ex ->
                        let reason =
                            $"S3 Testcontainers suites need a running Docker daemon; start Docker and re-run. Cause: {ex.Message}"

                        skipped <- Some reason
                        failGate reason)

    /// Builds options on the endpoint for one bucket, path-style like
    /// every MinIO and Hetzner host.
    let testOptions (endpoint: string) (bucket: string) : S3StorageOptions =
        let options = S3StorageOptions()
        options.ServiceUrl <- endpoint
        options.Region <- "us-east-1"
        options.Bucket <- bucket
        options.AccessKeyId <- AccessKey
        options.SecretAccessKey <- SecretKey
        options.ForcePathStyle <- true
        options

    /// Mints a fresh bucket on the client and returns its name: the
    /// per-instance isolation every suite builds on.
    let freshBucket (client: AmazonS3Client) : string =
        let bucket = sprintf "legate-test-%s" (Guid.NewGuid().ToString("N"))

        client.PutBucketAsync(PutBucketRequest(BucketName = bucket), CancellationToken.None).GetAwaiter().GetResult()
        |> ignore

        bucket

    /// Creates the blob store over a fresh bucket on the shared
    /// container: the shape the blob conformance suite pins.
    let createBlobStore () : S3BlobStore =
        let endpoint, _, _ = ensureReady ()
        let client = S3Client.buildClient (testOptions endpoint "legate-probe")
        let bucket = freshBucket client
        S3BlobStore(testOptions endpoint bucket, client)

    /// Creates the package store over a fresh bucket on the shared
    /// container with a deterministic clock: the shape the package
    /// conformance suite pins.
    let createPackageStore () : S3AgentPackageStore * TestClock =
        let endpoint, _, _ = ensureReady ()
        let client = S3Client.buildClient (testOptions endpoint "legate-probe")
        let bucket = freshBucket client
        let clock = TestClock()
        (S3AgentPackageStore(testOptions endpoint bucket, client, clock), clock)

    /// A fresh tenant id per test, so suites never share package scopes
    /// even where agent ids repeat.
    let freshTenant (prefix: string) =
        let suffix = Guid.NewGuid().ToString("N")
        TenantId.Create($"{prefix}-{suffix}")
