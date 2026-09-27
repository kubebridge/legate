// SPDX-License-Identifier: Apache-2.0
namespace Legate.Coordination.Redis.Tests

open System
open System.Runtime.InteropServices
open System.Threading
open Legate
open Legate.Coordination.Redis
open StackExchange.Redis
open Testcontainers.Redis

// Shared Testcontainers harness for the Redis suites: one container per
// test run, and one fresh coordination identity per fact.
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
// Isolation is per identity, not per database: every fact mints a fresh
// identity and key prefix, so facts never share keys even where the
// suites reuse fixed owner names.
module RedisTestEnvironment =

    let private gate = obj ()

    let mutable private cached: string option = None
    let mutable private skipped: string option = None

    /// The pinned server image the suites run against.
    let RedisImage = "redis:7-alpine"

    /// Starts one container for the image and returns its connection
    /// string. Pulls only when the image is absent locally, so cached pins
    /// never touch the registry.
    let private startContainer (image: string) : string =
        let container = RedisBuilder(image).Build()

        try
            container.StartAsync().GetAwaiter().GetResult() |> ignore
            container.GetConnectionString()
        with ex ->
            raise (InvalidOperationException($"The Redis test container failed to start: {ex.Message}", ex))

    /// Fails the calling fact with the reason: xUnit 2.9 cannot report a
    /// dynamic skip, so an unavailable container is a loud failure naming
    /// the remedy, never a vacuous pass.
    let private failGate (reason: string) : 'T =
        raise (InvalidOperationException(reason))

    /// Whether the host deliberately opts into containers on Windows
    /// (LEGATE_REDIS_DOCKER=1 with Linux-container Docker): the only way
    /// the suites attempt a container there.
    let private windowsOptIn () =
        match Environment.GetEnvironmentVariable("LEGATE_REDIS_DOCKER") with
        | null -> false
        | value ->
            let normalized = value.Trim().ToLowerInvariant()
            normalized = "1" || normalized = "true"

    /// Returns the shared container connection string, starting it once.
    /// Fails loudly with the remedy where Docker is unavailable; Windows
    /// without the opt-in indicates a compile-exclusion fault (the Docker
    /// suites do not compile there by default).
    let ensureReady () : string =
        lock gate (fun () ->
            match skipped, cached with
            | Some reason, _ -> failGate reason
            | None, Some connection -> connection
            | None, None ->
                if RuntimeInformation.IsOSPlatform(OSPlatform.Windows) && not (windowsOptIn ()) then
                    let reason =
                        "Redis Testcontainers suites do not run on Windows without LEGATE_REDIS_DOCKER=1: the Windows CI leg never attempts them."

                    skipped <- Some reason
                    failGate reason
                else
                    try
                        let connection = startContainer RedisImage
                        cached <- Some connection
                        connection
                    with ex ->
                        let reason =
                            $"Redis Testcontainers suites need a running Docker daemon; start Docker and re-run. Cause: {ex.Message}"

                        skipped <- Some reason
                        failGate reason)

    /// Mints a fresh coordination identity per fact, so suites never share
    /// admission keys even where owner ids repeat.
    let freshIdentity (prefix: string) =
        let suffix = Guid.NewGuid().ToString("N")
        prefix + "-" + suffix

    /// Builds options on the shared container with a fresh key prefix per
    /// caller, so facts never share keys.
    let testOptions () : DistributedCoordinationOptions =
        let _ = ensureReady ()
        let options = DistributedCoordinationOptions()
        options.Mode <- DistributedCoordinationMode.Redis
        options.ConnectionString <- ensureReady ()
        let suffix = Guid.NewGuid().ToString("N")
        options.KeyPrefix <- "legate:test:" + suffix
        options.StartupRequired <- true
        options.FailClosed <- true
        options

    /// Connects one client over fresh options: the shape the real-Redis
    /// suites pin. Each client shares the container but isolates keys by
    /// prefix only when the caller shares the options; sharing one
    /// options instance shares one keyspace.
    let connectClient (options: DistributedCoordinationOptions) : IDistributedLlmAdmission =
        let multiplexer = ConnectionMultiplexer.Connect(options.ConnectionString)
        RedisDistributedLlmAdmission(options, multiplexer) :> IDistributedLlmAdmission

    /// Connects two clients sharing one keyspace on the container: the
    /// shape the combined-limit suite pins.
    let connectSharedPair () : DistributedCoordinationOptions * IDistributedLlmAdmission * IDistributedLlmAdmission =
        let options = testOptions ()
        let first = connectClient options
        let second = connectClient options
        (options, first, second)
