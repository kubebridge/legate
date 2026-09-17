// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.S3

open System
open System.Collections.Generic
open System.Globalization
open System.IO
open System.Threading
open System.Threading.Tasks
open Amazon.S3
open Amazon.S3.Model
open Legate

// The S3 IAgentPackageStore: versioned package contents per agent over one
// bucket. Uploads publish and activate through one pointer document
// committed with a conditional write (a failed or duplicate-path upload
// leaves the previous state intact, and re-uploads preserve CreatedAt);
// replaces never change active status and throw on unknown versions;
// deleting the active version clears the pointer with no auto-promotion;
// GetPackageInfo derives from the active version's stored layout. Paths
// normalise through AgentPackagePaths and versions through
// PackageVersions. Multi-object writes are ordered but not transactional:
// duplicate-path validation always precedes any write, while a crash
// mid-upload can leave entry objects the next upload or delete repairs.

/// <summary>
/// The S3 <see cref="T:Legate.IAgentPackageStore" /> over one bucket:
/// versions under a per-agent key layout with an active-version pointer
/// committed through a conditional write, so publishing and activation
/// stay one atomic step.
/// </summary>
/// <param name="options">The validated storage options. Must not be null.</param>
/// <param name="client">The S3 client the store drives. Must not be null.</param>
/// <param name="clock">The clock stamping first publication. Must not be null.</param>
type S3AgentPackageStore(options: S3StorageOptions, client: AmazonS3Client, clock: TimeProvider) =

    do
        ArgumentNullException.ThrowIfNull(options)
        ArgumentNullException.ThrowIfNull(client)
        ArgumentNullException.ThrowIfNull(clock)

        match options.Validate() with
        | null -> ()
        | reason -> raise (ArgumentException(reason, nameof options))

    let bucket = options.Bucket

    let endpoint: string | null =
        if options.ServiceUrl = "" then null else options.ServiceUrl

    let gate = S3BucketGate(client, bucket)

    let staticSkillsPrefix = ".agent/skills/"
    let staticAgentsPrefix = ".agent/agents/"

    /// Materialises the entries into path-keyed bytes, validating paths
    /// and rejecting duplicate normalised paths before anything is
    /// stored.
    let materialise (entries: IAsyncEnumerable<AgentPackageEntry>) (cancellationToken: CancellationToken) =
        let files = Dictionary<string, byte[]>()
        let memory = new MemoryStream()
        let enumerator = entries.GetAsyncEnumerator cancellationToken
        let mutable failure: exn | null = null

        // One entry behind its own error boundary: the drain loop below
        // holds no try/with, so no try encloses a loop containing let!.
        // A failure stops the drain; the loop body reports it through
        // the failure cell so the enumerator still disposes before the
        // error propagates.
        let readOneAsync () : Task<bool> =
            task {
                try
                    let! moved = enumerator.MoveNextAsync()

                    if moved then
                        let entry = enumerator.Current
                        let normalised = AgentPackagePaths.Normalise entry.Path

                        if files.ContainsKey normalised then
                            raise (
                                ArgumentException(
                                    sprintf "Two package entries normalise to the same path %s." normalised,
                                    nameof entries
                                )
                            )

                        do! entry.Content.CopyToAsync memory
                        files[normalised] <- memory.ToArray()
                        memory.SetLength 0L
                        return true
                    else
                        return false
                with ex ->
                    failure <- ex
                    return false
            }

        task {
            let mutable more = true

            while more do
                let! keepGoing = readOneAsync ()
                more <- keepGoing

            do! enumerator.DisposeAsync()

            let result =
                match failure with
                | null -> files
                | ex -> raise ex

            memory.Dispose()
            return result
        }

    let namesUnder (paths: string seq) (prefix: string) (suffix: string) : List<string> =
        paths
        |> Seq.filter (fun filePath -> filePath.StartsWith(prefix, StringComparison.Ordinal))
        |> Seq.filter (fun filePath -> filePath.EndsWith(suffix, StringComparison.Ordinal))
        |> Seq.map (fun filePath -> filePath.Substring(prefix.Length, filePath.Length - prefix.Length - suffix.Length))
        |> Seq.filter (fun name -> name.Length > 0 && not (name.Contains '/'))
        |> Seq.sortWith (fun left right -> String.CompareOrdinal(left, right))
        |> fun names -> List<string>(names)

    let infoOf
        (paths: string seq)
        (instructions: string | null)
        (active: string)
        (source: string | null)
        : AgentPackageInfo =
        let skills = namesUnder paths staticSkillsPrefix "/SKILL.md"
        let subAgents = namesUnder paths staticAgentsPrefix "/AGENT.md"

        AgentPackageInfo(
            instructions,
            (skills :> IReadOnlyList<string>),
            (subAgents :> IReadOnlyList<string>),
            source,
            active
        )

    /// Reads the pointer document: null when no version was ever
    /// published or the active version was deleted.
    let readPointer
        (tenant: TenantId)
        (agentId: AgentId)
        (cancellationToken: CancellationToken)
        : Task<S3Keys.ActivePointer | null> =
        task {
            let key = S3Keys.pointerKey tenant agentId

            try
                let! bytes = S3Client.getAsync client bucket key cancellationToken

                match bytes with
                | null -> return Unchecked.defaultof<S3Keys.ActivePointer | null>
                | document ->
                    try
                        return S3Keys.deserializePointer document
                    with :? System.Text.Json.JsonException as ex ->
                        return
                            raise (
                                S3StorageException(
                                    bucket,
                                    key,
                                    sprintf "The package pointer on bucket %s is corrupt: %s." bucket ex.Message
                                )
                            )
            with :? LegateException as ex ->
                return raise ex
        }

    /// Reads one version's metadata document: null when the version was
    /// never published.
    let readVersionMetadata
        (tenant: TenantId)
        (agentId: AgentId)
        (version: string)
        (cancellationToken: CancellationToken)
        : Task<S3Keys.VersionMetadata | null> =
        task {
            let key = S3Keys.versionMetadataKey tenant agentId version

            try
                let! bytes = S3Client.getAsync client bucket key cancellationToken

                match bytes with
                | null -> return Unchecked.defaultof<S3Keys.VersionMetadata | null>
                | document ->
                    try
                        return S3Keys.deserializeVersionMetadata document
                    with :? System.Text.Json.JsonException as ex ->
                        return
                            raise (
                                S3StorageException(
                                    bucket,
                                    key,
                                    sprintf "The version document on bucket %s is corrupt: %s." bucket ex.Message
                                )
                            )
            with :? LegateException as ex ->
                return raise ex
        }

    /// Commits the pointer through a conditional write, re-reading and
    /// retrying a bounded number of times when a concurrent publisher
    /// wins the race. Writers compose through the package lease, so a
    /// race is an anomaly, not a branch: exhaustion throws.
    let commitPointer
        (tenant: TenantId)
        (agentId: AgentId)
        (version: string)
        (source: string)
        (cancellationToken: CancellationToken)
        =
        task {
            let key = S3Keys.pointerKey tenant agentId
            let payload = S3Keys.serializePointer { Version = version; Source = source }
            let mutable attempts = 0
            let mutable committed = false

            while not committed do
                let! current = S3Client.getMetadataAsync client bucket key cancellationToken

                let expected: string | null =
                    match current with
                    | null -> null
                    | metadata -> metadata.Etag

                let! written =
                    S3Client.putConditionalAsync
                        client
                        endpoint
                        bucket
                        key
                        payload
                        "application/json"
                        expected
                        cancellationToken

                if not (isNull (box written)) then
                    committed <- true
                else
                    attempts <- attempts + 1

                    if attempts >= 8 then
                        raise (
                            InvalidOperationException(
                                sprintf
                                    "The package pointer for agent %O resisted %d conditional commits: another publisher is racing this upload."
                                    agentId
                                    attempts
                            )
                        )

            return ()
        }

    /// Requires a version the store knows: unknown versions fail the
    /// replace path before anything is written.
    let requireVersion (version: string) (existing: S3Keys.VersionMetadata | null) =
        match existing with
        | null -> raise (ArgumentException("The version to replace is unknown.", nameof version))
        | metadata -> metadata

    /// Best-effort batch delete that never throws: cleanup behind a
    /// failed publish must not mask the original failure.
    let tryDeleteBatchAsync (keys: string list) (cancellationToken: CancellationToken) =
        task {
            try
                let! _ = S3Client.deleteBatchAsync client bucket keys cancellationToken
                ()
            with _ ->
                ()
        }

    /// Best-effort single delete that never throws: cleanup behind a
    /// failed publish must not mask the original failure.
    let tryDeleteOneAsync (key: string) (cancellationToken: CancellationToken) =
        task {
            try
                let! _ = S3Client.deleteOneAsync client bucket key cancellationToken
                ()
            with _ ->
                ()
        }

    /// Writes the entries, the version document, then the activating
    /// pointer commit. A brand-new version that fails mid-upload removes
    /// what this attempt wrote so a half-published version never lists;
    /// a re-upload failure leaves the previous entries for the next
    /// upload to repair: multi-object writes are ordered, not
    /// transactional.
    let publishVersionAsync
        (tenant: TenantId)
        (agentId: AgentId)
        (version: string)
        (source: string)
        (files: Dictionary<string, byte[]>)
        (createdAt: DateTimeOffset)
        (prefix: string)
        (isNew: bool)
        (cancellationToken: CancellationToken)
        =
        task {
            let written = ResizeArray<string>()

            try
                for pair in files do
                    let key = prefix + pair.Key

                    let! _ = S3Client.putAsync client bucket key pair.Value "application/octet-stream" cancellationToken

                    written.Add key

                let metaKey = S3Keys.versionMetadataKey tenant agentId version

                let! _ =
                    S3Client.putAsync
                        client
                        bucket
                        metaKey
                        (S3Keys.serializeVersionMetadata
                            {
                                CreatedAt = createdAt.ToString("O", CultureInfo.InvariantCulture)
                                Source = source
                            })
                        "application/json"
                        cancellationToken

                // Publish activates: the pointer commit folds activation
                // into the publish through one conditional write.
                do! commitPointer tenant agentId version source cancellationToken
                return AgentPackageVersion(version, createdAt)
            with ex ->
                if isNew then
                    do! tryDeleteBatchAsync (written |> Seq.toList) cancellationToken
                    do! tryDeleteOneAsync (S3Keys.versionMetadataKey tenant agentId version) cancellationToken

                return raise ex
        }

    /// <summary>
    /// The bucket the store reads and writes.
    /// </summary>
    member _.Bucket = bucket

    interface IAgentPackageStore with

        member _.GetPackageInfo(tenant, agentId, cancellationToken) =
            task {
                try
                    let! pointer = readPointer tenant agentId cancellationToken

                    match pointer with
                    | null -> return Unchecked.defaultof<AgentPackageInfo>
                    | active ->
                        let! metadata = readVersionMetadata tenant agentId active.Version cancellationToken

                        match metadata with
                        | null -> return Unchecked.defaultof<AgentPackageInfo>
                        | _ ->
                            let prefix = S3Keys.versionPrefix tenant agentId active.Version
                            let! keys = S3Client.listAllAsync client bucket prefix cancellationToken

                            let paths = keys |> List.map (fun key -> key.Substring prefix.Length)

                            let! instructions =
                                task {
                                    if paths |> List.contains "AGENTS.md" then
                                        let! bytes =
                                            S3Client.getAsync client bucket (prefix + "AGENTS.md") cancellationToken

                                        match bytes with
                                        | null -> return Unchecked.defaultof<string>
                                        | document -> return Text.Encoding.UTF8.GetString document
                                    elif paths |> List.contains "agents.md" then
                                        let! bytes =
                                            S3Client.getAsync client bucket (prefix + "agents.md") cancellationToken

                                        match bytes with
                                        | null -> return Unchecked.defaultof<string>
                                        | document -> return Text.Encoding.UTF8.GetString document
                                    else
                                        return Unchecked.defaultof<string>
                                }

                            return infoOf paths instructions active.Version active.Source
                with :? LegateException as ex ->
                    return raise ex
            }

        member _.ReadFile(tenant, agentId, version, filePath, cancellationToken) =
            task {
                PackageVersions.Validate version |> ignore
                let canonicalPath = AgentPackagePaths.Normalise filePath

                try
                    let key = S3Keys.entryKey tenant agentId version canonicalPath
                    let! bytes = S3Client.getAsync client bucket key cancellationToken

                    match bytes with
                    | null -> return Unchecked.defaultof<Stream>
                    | document -> return new MemoryStream(document, false) :> Stream
                with :? LegateException as ex ->
                    return raise ex
            }

        member _.ListFiles(tenant, agentId, version, prefix, cancellationToken) =
            task {
                PackageVersions.Validate version |> ignore

                if isNull (box prefix) then
                    raise (ArgumentNullException(nameof prefix))

                let canonicalPrefix = AgentPackagePaths.Normalise prefix

                try
                    let versionDir = S3Keys.versionPrefix tenant agentId version
                    let! keys = S3Client.listAllAsync client bucket (versionDir + canonicalPrefix) cancellationToken

                    return
                        (keys
                         |> List.filter (fun key ->
                             let relative = key.Substring versionDir.Length

                             relative = canonicalPrefix
                             || relative.StartsWith(canonicalPrefix + "/", StringComparison.Ordinal))
                         |> List.map (fun key -> key.Substring versionDir.Length)
                         |> List.sortWith (fun left right -> String.CompareOrdinal(left, right))
                        :> IReadOnlyList<string>)
                with :? LegateException as ex ->
                    return raise ex
            }

        member _.UploadPackage(tenant, agentId, version, source, entries, cancellationToken) =
            task {
                if isNull (box source) then
                    raise (ArgumentNullException(nameof source))

                PackageVersions.Validate version |> ignore

                // Materialise and validate before any write: a duplicate
                // path or unreadable stream fails here, leaving every
                // previous state intact.
                let! files = materialise entries cancellationToken

                try
                    do! gate.EnsureForWriteAsync cancellationToken

                    let! existing = readVersionMetadata tenant agentId version cancellationToken

                    let createdAt =
                        match existing with
                        | null -> clock.GetUtcNow()
                        | metadata ->
                            DateTimeOffset.Parse(
                                metadata.CreatedAt,
                                CultureInfo.InvariantCulture,
                                DateTimeStyles.RoundtripKind
                            )

                    let prefix = S3Keys.versionPrefix tenant agentId version
                    let isNew = isNull (box existing)

                    let! published =
                        publishVersionAsync tenant agentId version source files createdAt prefix isNew cancellationToken

                    return published
                with :? LegateException as ex ->
                    return raise ex
            }

        member _.ReplacePackageVersion(tenant, agentId, version, source, entries, cancellationToken) =
            task {
                if isNull (box source) then
                    raise (ArgumentNullException(nameof source))

                PackageVersions.Validate version |> ignore
                let! files = materialise entries cancellationToken

                try
                    do! gate.EnsureForWriteAsync cancellationToken

                    let! existing = readVersionMetadata tenant agentId version cancellationToken
                    let metadata = requireVersion version existing

                    let createdAt =
                        DateTimeOffset.Parse(
                            metadata.CreatedAt,
                            CultureInfo.InvariantCulture,
                            DateTimeStyles.RoundtripKind
                        )

                    let prefix = S3Keys.versionPrefix tenant agentId version

                    for pair in files do
                        let! _ =
                            S3Client.putAsync
                                client
                                bucket
                                (prefix + pair.Key)
                                pair.Value
                                "application/octet-stream"
                                cancellationToken

                        ()

                    let! previous = S3Client.listAllAsync client bucket prefix cancellationToken

                    let stale =
                        previous
                        |> List.filter (fun key ->
                            let relative = key.Substring prefix.Length
                            not (files.ContainsKey relative))
                        |> List.filter (fun key -> key <> S3Keys.versionMetadataKey tenant agentId version)

                    let! _ = S3Client.deleteBatchAsync client bucket stale cancellationToken

                    let! _ =
                        S3Client.putAsync
                            client
                            bucket
                            (S3Keys.versionMetadataKey tenant agentId version)
                            (S3Keys.serializeVersionMetadata
                                {
                                    CreatedAt = createdAt.ToString("O", CultureInfo.InvariantCulture)
                                    Source = source
                                })
                            "application/json"
                            cancellationToken

                    // The source label of the replacing write is
                    // recorded; active status never changes here: a
                    // cleared pointer stays cleared.
                    let! pointer = readPointer tenant agentId cancellationToken

                    match pointer with
                    | null -> ()
                    | active -> do! commitPointer tenant agentId active.Version source cancellationToken

                    return AgentPackageVersion(version, createdAt)
                with :? LegateException as ex ->
                    return raise ex
            }

        member _.DeletePackageVersion(tenant, agentId, version, cancellationToken) =
            task {
                PackageVersions.Validate version |> ignore

                try
                    let! existing = readVersionMetadata tenant agentId version cancellationToken
                    let prefix = S3Keys.versionPrefix tenant agentId version
                    let! entries = S3Client.listAllAsync client bucket prefix cancellationToken
                    let existed = not (isNull (box existing)) || not entries.IsEmpty

                    if existed then
                        let! _ =
                            S3Client.deleteOneAsync
                                client
                                bucket
                                (S3Keys.versionMetadataKey tenant agentId version)
                                cancellationToken

                        let! _ = S3Client.deleteBatchAsync client bucket entries cancellationToken

                        // Deleting the active version clears it: nothing
                        // else auto-promotes.
                        let! pointer = readPointer tenant agentId cancellationToken

                        match pointer with
                        | null -> ()
                        | active when String.Equals(active.Version, version, StringComparison.Ordinal) ->
                            let! _ =
                                S3Client.deleteOneAsync
                                    client
                                    bucket
                                    (S3Keys.pointerKey tenant agentId)
                                    cancellationToken

                            ()
                        | _ -> ()

                    return existed
                with :? LegateException as ex ->
                    return raise ex
            }

        member _.ListVersions(tenant, agentId, cancellationToken) =
            task {
                try
                    let prefix = S3Keys.versionsPrefix tenant agentId
                    let! keys = S3Client.listAllAsync client bucket prefix cancellationToken
                    let versions = ResizeArray<AgentPackageVersion>()

                    for key in keys |> List.sortWith (fun left right -> String.CompareOrdinal(left, right)) do
                        match S3Keys.versionOfMetadataKey prefix key with
                        | null -> ()
                        | version ->
                            if PackageVersions.TryValidate version then
                                let! metadata = readVersionMetadata tenant agentId version cancellationToken

                                match metadata with
                                | null -> ()
                                | document ->
                                    let mutable createdAt = DateTimeOffset.MinValue

                                    if
                                        DateTimeOffset.TryParse(
                                            document.CreatedAt,
                                            CultureInfo.InvariantCulture,
                                            DateTimeStyles.RoundtripKind,
                                            &createdAt
                                        )
                                    then
                                        versions.Add(AgentPackageVersion(version, createdAt))

                    return versions :> IReadOnlyList<AgentPackageVersion>
                with :? LegateException as ex ->
                    return raise ex
            }
