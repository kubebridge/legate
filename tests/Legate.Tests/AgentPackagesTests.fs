// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.AgentPackagesTests

open System
open System.Collections.Generic
open System.IO
open System.Text
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Xunit

let tenant = TenantId.Create "acme"
let otherTenant = TenantId.Create "other"

let agent = AgentId.New()
let otherAgent = AgentId.New()

let stamp = DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero)

/// A read-only stream over a UTF-8 string, the common entry-content shape.
let streamOf (text: string) : Stream =
    let bytes = Encoding.UTF8.GetBytes text
    new MemoryStream(bytes, writable = false) :> Stream

/// Reads a stream to its end as bytes, the store-side consumption shape.
let readAllBytes (stream: Stream) : byte[] =
    use memory = new MemoryStream()
    stream.CopyTo memory
    memory.ToArray()

/// Collects an async-enumerable into a list, the consumer side of
/// IAsyncEnumerable that store implementations exercise.
let toListAsync (source: IAsyncEnumerable<AgentPackageEntry>) : Task<AgentPackageEntry list> =
    task {
        let collected = ResizeArray<AgentPackageEntry>()
        let enumerator = source.GetAsyncEnumerator CancellationToken.None

        let mutable more = true

        while more do
            let! next = enumerator.MoveNextAsync().AsTask() |> Async.AwaitTask
            more <- next

            if more then
                collected.Add enumerator.Current

        do! enumerator.DisposeAsync().AsTask() |> Async.AwaitTask |> Async.Ignore

        return List.ofSeq collected
    }

/// Builds an async-enumerable of entries from (rawPath, text) pairs, the
/// producer side of IAsyncEnumerable that uploads exercise.
let entriesOf (pairs: (string * string) list) : IAsyncEnumerable<AgentPackageEntry> =
    let items =
        pairs
        |> List.map (fun (path, text) -> AgentPackageEntry(path, streamOf text))
        |> Array.ofList

    { new IAsyncEnumerable<AgentPackageEntry> with
        member _.GetAsyncEnumerator(_cancellationToken: CancellationToken) =
            let mutable index = -1

            { new IAsyncEnumerator<AgentPackageEntry> with
                member _.Current = items[index]

                member _.MoveNextAsync() =
                    if index + 1 < items.Length then
                        index <- index + 1
                        ValueTask<bool> true
                    else
                        ValueTask<bool> false

                member _.DisposeAsync() = ValueTask()
            }
    }

/// An in-memory IAgentPackageStore implemented entirely from outside the
/// assembly: the C#-friendly-surface proof, mirroring AgentStoreTests.fs's
/// FakeAgentStore. It implements the documented semantics the tests pin:
/// normalised duplicate detection, atomic publish-and-activate on upload,
/// active-status-preserving replace, idempotent delete that clears the
/// active version, null-absence reads, lexicographic version lists, and
/// info derived from the documented package layout.
type FakePackageStore() =
    // key: tenant|agent|version|canonicalPath -> bytes
    let files = Dictionary<string, byte[]>()
    // key: tenant|agent|version -> CreatedAt, stamped on first publication
    let versions = Dictionary<string, DateTimeOffset>()
    // key: tenant|agent -> active version string, null when cleared
    let active = Dictionary<string, string>()
    // key: tenant|agent -> last-write source label
    let sources = Dictionary<string, string>()

    let agentKey (t: TenantId) (a: AgentId) = sprintf "%s|%s" t.Value a.Value

    let versionKey (t: TenantId) (a: AgentId) (v: string) = sprintf "%s|%s|%s" t.Value a.Value v

    let fileKey (t: TenantId) (a: AgentId) (v: string) (p: string) =
        sprintf "%s|%s|%s|%s" t.Value a.Value v p

    /// Collects and validates an upload's entries exactly as the contract
    /// documents, before any store mutation: version rule, per-entry path
    /// normalisation, and duplicate-normalised-path rejection.
    let validateEntries (entries: IAsyncEnumerable<AgentPackageEntry>) =
        let collected = toListAsync entries |> (fun pending -> pending.Result)
        let seen = HashSet<string>(StringComparer.Ordinal)

        for entry in collected do
            if not (seen.Add entry.Path) then
                raise (ArgumentException("Two entries normalise to the same path.", nameof entries))

        collected

    let directoryNames (t: TenantId) (a: AgentId) (version: string) (prefix: string) =
        let scopedPrefix = fileKey t a version prefix

        files.Keys
        |> Seq.choose (fun key ->
            if key.StartsWith scopedPrefix then
                let rest = key.Substring(scopedPrefix.Length)
                let slash = rest.IndexOf '/'

                if slash > 0 then Some(rest.Substring(0, slash)) else None
            else
                None)
        |> Seq.distinct
        |> Seq.sortWith (fun x y -> String.CompareOrdinal(x, y))
        |> Seq.toArray

    interface IAgentPackageStore with
        member _.GetPackageInfo(t, a, _) =
            task {
                match active.TryGetValue(agentKey t a) with
                | true, version when not (isNull (box version)) ->
                    let instructions =
                        match files.TryGetValue(fileKey t a version "AGENTS.md") with
                        | true, bytes -> Encoding.UTF8.GetString bytes
                        | false, _ -> Unchecked.defaultof<string>

                    let skills = directoryNames t a version ".agent/skills/"
                    let subAgents = directoryNames t a version ".agent/agents/"

                    let source =
                        match sources.TryGetValue(agentKey t a) with
                        | true, value -> value
                        | false, _ -> Unchecked.defaultof<string>

                    return
                        AgentPackageInfo(
                            instructions,
                            (skills :> IReadOnlyList<string>),
                            (subAgents :> IReadOnlyList<string>),
                            source,
                            version
                        )
                | _ -> return Unchecked.defaultof<AgentPackageInfo>
            }

        member _.ReadFile(t, a, v, path, _) =
            task {
                let validatedVersion = PackageVersions.Validate v
                let canonical = AgentPackagePaths.Validate path

                let! read =
                    if versions.ContainsKey(versionKey t a validatedVersion) then
                        match files.TryGetValue(fileKey t a validatedVersion canonical) with
                        | true, bytes -> Task.FromResult<Stream>(streamOf (Encoding.UTF8.GetString bytes))
                        | false, _ -> Task.FromResult<Stream> Unchecked.defaultof<Stream>
                    else
                        Task.FromResult<Stream> Unchecked.defaultof<Stream>

                return read
            }

        member _.ListFiles(t, a, v, prefix, _) =
            task {
                let validatedVersion = PackageVersions.Validate v

                if isNull (box prefix) then
                    raise (ArgumentNullException(nameof prefix))

                let canonicalPrefix = AgentPackagePaths.Normalise prefix
                let scope = versionKey t a validatedVersion + "|"

                let listed =
                    files.Keys
                    |> Seq.choose (fun key ->
                        if key.StartsWith scope then
                            let path = key.Substring(scope.Length)

                            if
                                path = canonicalPrefix
                                || path.StartsWith(canonicalPrefix + "/", StringComparison.Ordinal)
                            then
                                Some path
                            else
                                None
                        else
                            None)
                    |> Seq.toArray
                    |> Array.sortWith (fun x y -> String.CompareOrdinal(x, y))

                return listed :> IReadOnlyList<string>
            }

        member _.UploadPackage(t, a, v, source, entries, _) =
            task {
                if isNull (box source) then
                    raise (ArgumentNullException(nameof source))

                let validatedVersion = PackageVersions.Validate v
                let collected = validateEntries entries

                // Atomic publish: everything validated before the first
                // mutation, so a rejected upload leaves no partial version.
                for entry in collected do
                    files[fileKey t a validatedVersion entry.Path] <- readAllBytes entry.Content

                if not (versions.ContainsKey(versionKey t a validatedVersion)) then
                    versions[versionKey t a validatedVersion] <- stamp

                // Deploy semantics: activation folds into the publish.
                active[agentKey t a] <- validatedVersion
                sources[agentKey t a] <- source

                return AgentPackageVersion(validatedVersion, versions[versionKey t a validatedVersion])
            }

        member _.ReplacePackageVersion(t, a, v, source, entries, _) =
            task {
                if isNull (box source) then
                    raise (ArgumentNullException(nameof source))

                let validatedVersion = PackageVersions.Validate v
                let collected = validateEntries entries
                let vKey = versionKey t a validatedVersion

                if not (versions.ContainsKey vKey) then
                    raise (ArgumentException("The version is unknown.", nameof v))

                // Swap only this version's files; the active status is
                // never read or written here.
                for entry in collected do
                    files[fileKey t a validatedVersion entry.Path] <- readAllBytes entry.Content

                sources[agentKey t a] <- source

                return AgentPackageVersion(validatedVersion, versions[vKey])
            }

        member _.DeletePackageVersion(t, a, v, _) =
            task {
                let validatedVersion = PackageVersions.Validate v
                let vKey = versionKey t a validatedVersion

                let removed = versions.Remove vKey

                if removed then
                    let prefix = vKey + "|"
                    let dead = files.Keys |> Seq.filter (fun key -> key.StartsWith prefix) |> Seq.toList

                    for key in dead do
                        files.Remove key |> ignore

                    // Documented: deleting the active version clears it to
                    // null; deleting a non-active version leaves it
                    // untouched.
                    match active.TryGetValue(agentKey t a) with
                    | true, current when current = validatedVersion ->
                        active[agentKey t a] <- Unchecked.defaultof<string>
                    | _ -> ()

                return removed
            }

        member _.ListVersions(t, a, _) =
            task {
                let prefix = agentKey t a + "|"

                let listed =
                    versions.Keys
                    |> Seq.choose (fun key ->
                        if key.StartsWith prefix then
                            Some(AgentPackageVersion(key.Substring(prefix.Length), versions[key]))
                        else
                            None)
                    |> Seq.toArray
                    |> Array.sortWith (fun x y -> String.CompareOrdinal(x.Version, y.Version))

                return listed :> IReadOnlyList<AgentPackageVersion>
            }

/// A fake IAgentPackageLeaseService implemented from outside the assembly,
/// exercising every documented outcome branch without sleeps: acquire
/// grants or reports held, renew renews or loses, verify and release are
/// plain predicates, and WithLease acquires, runs the work, and releases
/// best-effort in a finally block. Renewal timing (interval, timeout,
/// cancel-on-failure) is pinned through PackageLeaseOptions' documented
/// defaults; a reference lease implementation with real renewal timing
/// belongs to the storage/coordination epics.
type FakeLeaseService() =
    // key: tenant|agent -> (token, expiry)
    let held = Dictionary<string, string * DateTimeOffset>()
    let mutable tokenCounter = 0

    let key (t: TenantId) (a: AgentId) = sprintf "%s|%s" t.Value a.Value

    let mintToken () =
        tokenCounter <- tokenCounter + 1
        sprintf "token-%d" tokenCounter

    let acquire (t: TenantId) (a: AgentId) (owner: string) (duration: TimeSpan) : PackageLeaseState =
        match held.TryGetValue(key t a) with
        | true, (_, expiresAt) when expiresAt > DateTimeOffset.UtcNow ->
            PackageLeaseUnavailable(t, a, "leaseHeld") :> PackageLeaseState
        | _ ->

            let token = mintToken ()
            let expiresAt = DateTimeOffset.UtcNow.Add duration
            held[key t a] <- (token, expiresAt)

            PackageLeaseAcquired(AgentPackageLease(t, a, owner, token, expiresAt)) :> PackageLeaseState

    let renew (lease: AgentPackageLease) (duration: TimeSpan) : PackageLeaseRenewal =
        match held.TryGetValue(key lease.Tenant lease.AgentId) with
        | true, (token, _) when token = lease.Token ->
            let fresh = mintToken ()
            let expiresAt = DateTimeOffset.UtcNow.Add duration
            held[key lease.Tenant lease.AgentId] <- (fresh, expiresAt)

            PackageLeaseRenewed(AgentPackageLease(lease.Tenant, lease.AgentId, lease.Owner, fresh, expiresAt))
            :> PackageLeaseRenewal
        | _ -> PackageLeaseLost("staleToken") :> PackageLeaseRenewal

    let release (lease: AgentPackageLease) : bool =
        match held.TryGetValue(key lease.Tenant lease.AgentId) with
        | true, (token, _) when token = lease.Token ->
            held.Remove(key lease.Tenant lease.AgentId) |> ignore
            true
        | _ -> false

    interface IAgentPackageLeaseService with
        member _.Acquire(t, a, owner, duration, _) =
            if isNull (box owner) then
                raise (ArgumentNullException(nameof owner))

            Task.FromResult(acquire t a owner duration)

        member _.Renew(lease, duration, _) = Task.FromResult(renew lease duration)

        member _.Verify(lease, _) =
            Task.FromResult(
                match held.TryGetValue(key lease.Tenant lease.AgentId) with
                | true, (token, _) -> token = lease.Token
                | false, _ -> false
            )

        member _.Release(lease, _) = Task.FromResult(release lease)

        member _.WithLease
            (
                t: TenantId,
                a: AgentId,
                owner: string,
                duration: TimeSpan,
                options: PackageLeaseOptions,
                work: Func<CancellationToken, Task<'T>>,
                _cancellationToken: CancellationToken
            ) : Task<'T> =
            if isNull (box owner) then
                raise (ArgumentNullException(nameof owner))

            // Null options resolve to the defaults; this fake reads them
            // so the option surface is exercised even without timing.
            let resolved =
                if isNull (box options) then
                    PackageLeaseOptions()
                else
                    options

            if resolved.RenewInterval <= TimeSpan.Zero then
                raise (ArgumentException("RenewInterval must be positive.", nameof options))

            let workFunc: Func<CancellationToken, Task<'T>> = work

            match acquire t a owner duration with
            | :? PackageLeaseAcquired as acquired ->
                let lease = acquired.Lease

                task {
                    try
                        return! workFunc.Invoke(CancellationToken.None)
                    finally
                        // Best-effort release in a finally block, also on
                        // failure paths.
                        release lease |> ignore
                }
            | _ ->
                Task.FromException<'T>(PackageLeaseException(a, owner, "acquire", "The lease could not be acquired."))

        member _.WithLease
            (
                t: TenantId,
                a: AgentId,
                owner: string,
                duration: TimeSpan,
                options: PackageLeaseOptions,
                work: Func<CancellationToken, Task>,
                _cancellationToken: CancellationToken
            ) : Task =
            let resolved =
                if isNull (box options) then
                    PackageLeaseOptions()
                else
                    options

            if resolved.RenewInterval <= TimeSpan.Zero then
                raise (ArgumentException("RenewInterval must be positive.", nameof options))

            match acquire t a owner duration with
            | :? PackageLeaseAcquired as acquired ->
                let lease = acquired.Lease

                task {
                    try
                        do! work.Invoke CancellationToken.None
                    finally
                        // Best-effort release in a finally block, also on
                        // failure paths.
                        release lease |> ignore
                }
            | _ ->
                Task.FromException<unit>(PackageLeaseException(a, owner, "acquire", "The lease could not be acquired."))
                |> fun failure -> failure :> Task

// ───────────────────────────────────────────────────────────────────────────
// AgentPackagePaths

[<Fact>]
let ``Normalise folds backslashes and collapses separators`` () =
    AgentPackagePaths.Normalise "AGENTS.md" |> should equal "AGENTS.md"

    AgentPackagePaths.Normalise @".agent\skills\deploy\SKILL.md"
    |> should equal ".agent/skills/deploy/SKILL.md"

    AgentPackagePaths.Normalise "//a///b//" |> should equal "a/b"
    AgentPackagePaths.Normalise @"\\a\\b\\" |> should equal "a/b"
    AgentPackagePaths.Normalise @"/\/mixed\/" |> should equal "mixed"

[<Fact>]
let ``Normalise rejects traversal and dot segments`` () =
    let throwsInvalid (raw: string) =
        try
            AgentPackagePaths.Normalise raw |> ignore
            failwithf "expected InvalidPackagePathException for %A" raw
        with :? InvalidPackagePathException as exn ->
            exn.Path |> should equal raw

    throwsInvalid ".."
    throwsInvalid "../escape"
    throwsInvalid @"..\escape"
    throwsInvalid "a/../b"
    throwsInvalid "a/./b"
    throwsInvalid "."

[<Fact>]
let ``Normalise rejects NUL and empty results`` () =
    let throwsInvalid (raw: string) =
        try
            AgentPackagePaths.Normalise raw |> ignore
            failwithf "expected InvalidPackagePathException for %A" raw
        with :? InvalidPackagePathException as exn ->
            exn.Path |> should equal raw

    throwsInvalid "a\u0000b"
    throwsInvalid ""
    throwsInvalid "/"
    throwsInvalid "//"
    throwsInvalid @"\\"

[<Fact>]
let ``Normalise keeps interior whitespace segments and collapses separators`` () =
    // Whitespace is a legal path character; only NUL and dot segments are
    // structural rejections.
    AgentPackagePaths.Normalise "docs/readme copy.md"
    |> should equal "docs/readme copy.md"

[<Fact>]
let ``Normalise rejects drive prefixes`` () =
    let throwsInvalid (raw: string) =
        try
            AgentPackagePaths.Normalise raw |> ignore
            failwithf "expected InvalidPackagePathException for %A" raw
        with :? InvalidPackagePathException as exn ->
            exn.Path |> should equal raw

    throwsInvalid "C:"
    throwsInvalid @"C:\work\agent"
    throwsInvalid "D:x"

[<Fact>]
let ``Normalise rejects null with ArgumentNullException`` () =
    (fun () -> AgentPackagePaths.Normalise(null |> box |> unbox<string>) |> ignore)
    |> should throw typeof<ArgumentNullException>

[<Fact>]
let ``Validate agrees with Normalise`` () =
    AgentPackagePaths.Validate @".agent\agents\reviewer\AGENT.md"
    |> should equal ".agent/agents/reviewer/AGENT.md"

// ───────────────────────────────────────────────────────────────────────────
// PackageVersions

[<Fact>]
let ``PackageVersions pattern is the pinned rule`` () =
    PackageVersions.Pattern |> should equal @"\A[A-Za-z0-9][A-Za-z0-9._-]{0,63}\z"

[<Fact>]
let ``PackageVersions accepts well-formed versions`` () =
    for version in
        [
            "1"
            "1.4.2"
            "v1_0-beta"
            "A"
            "0.0.1"
            "9x"
            "a.b-c_d"
        ] do
        PackageVersions.TryValidate version |> should equal true
        PackageVersions.Validate version |> should equal version

[<Fact>]
let ``PackageVersions rejects malformed versions`` () =
    let rejects (version: string) =
        PackageVersions.TryValidate version |> should equal false

    rejects ""
    rejects ".hidden"
    rejects "-leading"
    rejects "_leading"
    rejects "has space"
    rejects "with/slash"
    rejects "with:colon"
    rejects "tab\tchar"
    rejects "newline\n"
    rejects "non-ascii-\u00e9"
    rejects (String('a', 65))

    PackageVersions.TryValidate null |> should equal false

[<Fact>]
let ``PackageVersions accepts exactly 64 characters and rejects 65`` () =
    PackageVersions.TryValidate(String('a', 64)) |> should equal true
    PackageVersions.TryValidate(String('a', 65)) |> should equal false

[<Fact>]
let ``PackageVersions.Validate throws on invalid versions`` () =
    (fun () -> PackageVersions.Validate "/no/slashes" |> ignore)
    |> should throw typeof<ArgumentException>

    (fun () -> PackageVersions.Validate(null |> box |> unbox<string>) |> ignore)
    |> should throw typeof<ArgumentNullException>

// ───────────────────────────────────────────────────────────────────────────
// AgentPackageEntry

[<Fact>]
let ``AgentPackageEntry normalises its path`` () =
    let entry = AgentPackageEntry(@"AGENTS.md", streamOf "instructions")

    entry.Path |> should equal "AGENTS.md"
    isNull (box entry.Content) |> should equal false

[<Fact>]
let ``AgentPackageEntry rejects a null path or content`` () =
    (fun () -> AgentPackageEntry(null |> box |> unbox<string>, streamOf "x") |> ignore)
    |> should throw typeof<ArgumentNullException>

    (fun () -> AgentPackageEntry("AGENTS.md", Unchecked.defaultof<Stream>) |> ignore)
    |> should throw typeof<ArgumentNullException>

[<Fact>]
let ``AgentPackageEntry rejects an invalid path`` () =
    (fun () -> AgentPackageEntry("../escape", streamOf "x") |> ignore)
    |> should throw typeof<InvalidPackagePathException>

// ───────────────────────────────────────────────────────────────────────────
// IAgentPackageStore

[<Fact>]
let ``GetPackageInfo returns null when nothing was ever uploaded`` () =
    let store = FakePackageStore() :> IAgentPackageStore

    task {
        let! info = store.GetPackageInfo(tenant, agent, CancellationToken.None)
        info |> should equal null
    }
    |> (fun t -> t.Wait())

/// Asserts the fetched info reflects the just-published manual upload:
/// instructions, the deploy skill, the reviewer sub-agent, and the active
/// version. Pure so the resumable test stays a straight-line await plus a
/// return.
let private checkPublishedInfo (info: AgentPackageInfo | null) =
    match info with
    | null -> failwith "the uploaded package was not found"
    | found ->
        found.Instructions |> should equal "instructions"
        found.Skills.Count |> should equal 1
        found.Skills[0] |> should equal "deploy"
        found.SubAgents.Count |> should equal 1
        found.SubAgents[0] |> should equal "reviewer"
        found.Source |> should equal "manual-zip"
        found.ActiveVersion |> should equal "1.0.0"

[<Fact>]
let ``UploadPackage publishes the version and makes it active`` () =
    let store = FakePackageStore() :> IAgentPackageStore

    let entries =
        entriesOf
            [
                ("AGENTS.md", "instructions")
                (@".agent\skills\deploy\SKILL.md", "deploy skill")
                (".agent/agents/reviewer/AGENT.md", "reviewer sub-agent")
            ]

    task {
        let! published = store.UploadPackage(tenant, agent, "1.0.0", "manual-zip", entries, CancellationToken.None)

        published.Version |> should equal "1.0.0"
        published.CreatedAt |> should equal stamp

        let! info = store.GetPackageInfo(tenant, agent, CancellationToken.None)

        checkPublishedInfo info
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``UploadPackage rejects duplicate normalised paths and leaves no partial version`` () =
    let store = FakePackageStore() :> IAgentPackageStore

    let entries =
        entriesOf
            [
                ("AGENTS.md", "one")
                (@"\AGENTS.md", "duplicate via normalisation")
            ]

    task {
        try
            let! _ = store.UploadPackage(tenant, agent, "1.0.0", "manual-zip", entries, CancellationToken.None)
            failwith "expected ArgumentException"
        with :? ArgumentException ->
            ()

        let! versions = store.ListVersions(tenant, agent, CancellationToken.None)
        versions.Count |> should equal 0

        let! info = store.GetPackageInfo(tenant, agent, CancellationToken.None)
        info |> should equal null
    }
    |> (fun t -> t.Wait())

/// Asserts the replaced entry reads back the second upload's text,
/// taking ownership of the stream. Pure (synchronous) so the resumable
/// test stays a straight-line await plus a return.
let private checkReplacedEntry (read: Stream | null) =
    match read with
    | null -> failwith "the replaced entry was not found"
    | stream ->
        use stream = stream
        use reader = new StreamReader(stream)
        reader.ReadToEnd() |> should equal "second"

/// Asserts the info still points at the replaced version's contents and
/// source. Pure so the resumable test stays a straight-line await plus a
/// return.
let private checkReplacedInfo (info: AgentPackageInfo | null) =
    match info with
    | null -> failwith "the package was not found"
    | found ->
        found.Instructions |> should equal "second"
        found.Source |> should equal "github-sync"
        found.ActiveVersion |> should equal "1.0.0"

[<Fact>]
let ``Re-uploading an existing version replaces contents and stays active`` () =
    let store = FakePackageStore() :> IAgentPackageStore

    task {
        let! first =
            store.UploadPackage(
                tenant,
                agent,
                "1.0.0",
                "manual-zip",
                entriesOf [ ("AGENTS.md", "first") ],
                CancellationToken.None
            )

        let! second =
            store.UploadPackage(
                tenant,
                agent,
                "1.0.0",
                "github-sync",
                entriesOf [ ("AGENTS.md", "second") ],
                CancellationToken.None
            )

        second.Version |> should equal first.Version
        second.CreatedAt |> should equal first.CreatedAt

        let! read = store.ReadFile(tenant, agent, "1.0.0", "AGENTS.md", CancellationToken.None)

        checkReplacedEntry read

        let! info = store.GetPackageInfo(tenant, agent, CancellationToken.None)

        checkReplacedInfo info
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``UploadPackage rejects an invalid version without storing anything`` () =
    let store = FakePackageStore() :> IAgentPackageStore

    task {
        try
            let! _ =
                store.UploadPackage(
                    tenant,
                    agent,
                    "../bad",
                    "manual-zip",
                    entriesOf [ ("AGENTS.md", "x") ],
                    CancellationToken.None
                )

            failwith "expected ArgumentException"
        with :? ArgumentException ->
            ()

        let! versions = store.ListVersions(tenant, agent, CancellationToken.None)
        versions.Count |> should equal 0
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``ReplacePackageVersion swaps contents and preserves the active version`` () =
    let store = FakePackageStore() :> IAgentPackageStore

    task {
        let! _ =
            store.UploadPackage(
                tenant,
                agent,
                "1.0.0",
                "github-sync",
                entriesOf [ ("AGENTS.md", "old") ],
                CancellationToken.None
            )

        let! _ =
            store.UploadPackage(
                tenant,
                agent,
                "2.0.0",
                "github-sync",
                entriesOf [ ("AGENTS.md", "newest") ],
                CancellationToken.None
            )

        // Refresh the older version in place: the GitHub-sync refresh path.
        let! replaced =
            store.ReplacePackageVersion(
                tenant,
                agent,
                "1.0.0",
                "github-sync",
                entriesOf [ ("AGENTS.md", "refreshed") ],
                CancellationToken.None
            )

        replaced.Version |> should equal "1.0.0"
        replaced.CreatedAt |> should equal stamp

        let! read = store.ReadFile(tenant, agent, "1.0.0", "AGENTS.md", CancellationToken.None)

        match read with
        | null -> failwith "the replaced entry was not found"
        | stream ->
            use stream = stream
            use reader = new StreamReader(stream)
            reader.ReadToEnd() |> should equal "refreshed"

        // Active status is untouched by replace: 2.0.0 stays active.
        let! info = store.GetPackageInfo(tenant, agent, CancellationToken.None)

        match info with
        | null -> failwith "the package was not found"
        | found -> found.ActiveVersion |> should equal "2.0.0"
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``ReplacePackageVersion throws for an unknown version`` () =
    let store = FakePackageStore() :> IAgentPackageStore

    task {
        try
            let! _ =
                store.ReplacePackageVersion(
                    tenant,
                    agent,
                    "9.9.9",
                    "github-sync",
                    entriesOf [ ("AGENTS.md", "x") ],
                    CancellationToken.None
                )

            failwith "expected ArgumentException"
        with :? ArgumentException ->
            ()
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``ReadFile returns null when the version or path is absent`` () =
    let store = FakePackageStore() :> IAgentPackageStore

    task {
        let! _ =
            store.UploadPackage(
                tenant,
                agent,
                "1.0.0",
                "manual-zip",
                entriesOf [ ("AGENTS.md", "text") ],
                CancellationToken.None
            )

        let! missingPath = store.ReadFile(tenant, agent, "1.0.0", "missing.txt", CancellationToken.None)
        missingPath |> should equal null

        let! missingVersion = store.ReadFile(tenant, agent, "2.0.0", "AGENTS.md", CancellationToken.None)
        missingVersion |> should equal null

        // Reads key on the canonical path: the folded form finds the entry.
        let! folded = store.ReadFile(tenant, agent, "1.0.0", @"\AGENTS.md", CancellationToken.None)

        match folded with
        | null -> failwith "the folded read did not find the entry"
        | stream -> stream.Length |> should be (greaterThan 0L)
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``DeletePackageVersion is idempotent and clears the active version`` () =
    let store = FakePackageStore() :> IAgentPackageStore

    task {
        let! _ =
            store.UploadPackage(
                tenant,
                agent,
                "1.0.0",
                "manual-zip",
                entriesOf [ ("AGENTS.md", "only") ],
                CancellationToken.None
            )

        let! deleted = store.DeletePackageVersion(tenant, agent, "1.0.0", CancellationToken.None)
        deleted |> should equal true

        let! again = store.DeletePackageVersion(tenant, agent, "1.0.0", CancellationToken.None)
        again |> should equal false

        // Documented: deleting the active version clears ActiveVersion.
        let! info = store.GetPackageInfo(tenant, agent, CancellationToken.None)
        info |> should equal null

        // Deleting a non-active version leaves the active version intact.
        let! _ =
            store.UploadPackage(
                tenant,
                agent,
                "1.0.0",
                "manual-zip",
                entriesOf [ ("AGENTS.md", "v1") ],
                CancellationToken.None
            )

        let! _ =
            store.UploadPackage(
                tenant,
                agent,
                "2.0.0",
                "manual-zip",
                entriesOf [ ("AGENTS.md", "v2") ],
                CancellationToken.None
            )

        let! removedOlder = store.DeletePackageVersion(tenant, agent, "1.0.0", CancellationToken.None)
        removedOlder |> should equal true

        let! after = store.GetPackageInfo(tenant, agent, CancellationToken.None)

        match after with
        | null -> failwith "the package was not found"
        | found -> found.ActiveVersion |> should equal "2.0.0"
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``ListVersions returns versions lexicographically ordered`` () =
    let store = FakePackageStore() :> IAgentPackageStore

    task {
        for version in [ "2.0.0"; "1.0.0"; "10.0.0" ] do
            let! _ =
                store.UploadPackage(
                    tenant,
                    agent,
                    version,
                    "manual-zip",
                    entriesOf [ ("AGENTS.md", version) ],
                    CancellationToken.None
                )

            ()

        let! listed = store.ListVersions(tenant, agent, CancellationToken.None)

        listed.Count |> should equal 3
        listed[0].Version |> should equal "1.0.0"
        listed[1].Version |> should equal "10.0.0"
        listed[2].Version |> should equal "2.0.0"
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``Store operations are tenant-scoped and agent-scoped`` () =
    let store = FakePackageStore() :> IAgentPackageStore

    task {
        let! _ =
            store.UploadPackage(
                tenant,
                agent,
                "1.0.0",
                "manual-zip",
                entriesOf [ ("AGENTS.md", "mine") ],
                CancellationToken.None
            )

        let! crossTenant = store.GetPackageInfo(otherTenant, agent, CancellationToken.None)
        crossTenant |> should equal null

        let! crossAgent = store.GetPackageInfo(tenant, otherAgent, CancellationToken.None)
        crossAgent |> should equal null

        let! crossTenantRead = store.ReadFile(otherTenant, agent, "1.0.0", "AGENTS.md", CancellationToken.None)
        crossTenantRead |> should equal null

        let! crossAgentRead = store.ReadFile(tenant, otherAgent, "1.0.0", "AGENTS.md", CancellationToken.None)
        crossAgentRead |> should equal null

        let! crossTenantList = store.ListVersions(otherTenant, agent, CancellationToken.None)
        crossTenantList.Count |> should equal 0

        let! crossTenantDelete = store.DeletePackageVersion(otherTenant, agent, "1.0.0", CancellationToken.None)
        crossTenantDelete |> should equal false
    }
    |> (fun t -> t.Wait())

// ───────────────────────────────────────────────────────────────────────────
// IAgentPackageLeaseService

/// Asserts the first acquire granted worker-a the lease. Pure so the
/// resumable test stays a straight-line await plus a return.
let private checkFirstGrant (first: PackageLeaseState) =
    match first with
    | :? PackageLeaseAcquired as acquired ->
        acquired.Lease.Owner |> should equal "worker-a"
        acquired.Lease.AgentId |> should equal agent
        acquired.Lease.Tenant |> should equal tenant
        isNull (box acquired.Lease.Token) |> should equal false
    | _ -> failwith "expected PackageLeaseAcquired"

/// Asserts the second acquire reports the lease as held. Pure so the
/// resumable test stays a straight-line await plus a return.
let private checkHeldUnavailable (second: PackageLeaseState) =
    match second with
    | :? PackageLeaseUnavailable as unavailable ->
        unavailable.Reason |> should equal "leaseHeld"
        unavailable.AgentId |> should equal agent
    | _ -> failwith "expected PackageLeaseUnavailable"

[<Fact>]
let ``Acquire grants when unheld and reports leaseHeld when held`` () =
    let service = FakeLeaseService() :> IAgentPackageLeaseService

    task {
        let! first = service.Acquire(tenant, agent, "worker-a", TimeSpan.FromMinutes 5., CancellationToken.None)

        checkFirstGrant first

        let! second = service.Acquire(tenant, agent, "worker-b", TimeSpan.FromMinutes 5., CancellationToken.None)

        checkHeldUnavailable second
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``Acquire is tenant-scoped`` () =
    let service = FakeLeaseService() :> IAgentPackageLeaseService

    task {
        let! first = service.Acquire(tenant, agent, "worker-a", TimeSpan.FromMinutes 5., CancellationToken.None)

        match first with
        | :? PackageLeaseAcquired -> ()
        | _ -> failwith "expected PackageLeaseAcquired"

        let! crossTenant =
            service.Acquire(otherTenant, agent, "worker-b", TimeSpan.FromMinutes 5., CancellationToken.None)

        match crossTenant with
        | :? PackageLeaseAcquired -> ()
        | _ -> failwith "expected PackageLeaseAcquired"
    }
    |> (fun t -> t.Wait())

/// Asserts the renewal issued a fresh token with a later expiry. Pure so
/// the resumable test stays a straight-line await plus a return.
let private checkRenewedFresh (lease: AgentPackageLease) (renewed: PackageLeaseRenewal) =
    match renewed with
    | :? PackageLeaseRenewed as ok ->
        ok.Lease.Token |> should not' (equal lease.Token)
        ok.Lease.ExpiresAt |> should be (greaterThan DateTimeOffset.UtcNow)
    | _ -> failwith "expected PackageLeaseRenewed"

/// Asserts renewing the stale token reports the lease as lost. Pure so the
/// resumable test stays a straight-line await plus a return.
let private checkRenewedStale (lost: PackageLeaseRenewal) =
    match lost with
    | :? PackageLeaseLost as failure -> failure.Reason |> should equal "staleToken"
    | _ -> failwith "expected PackageLeaseLost"

[<Fact>]
let ``Renew returns a fresh lease or the lost branch`` () =
    let service = FakeLeaseService() :> IAgentPackageLeaseService

    task {
        let! state = service.Acquire(tenant, agent, "worker-a", TimeSpan.FromMinutes 5., CancellationToken.None)
        let lease = (state :?> PackageLeaseAcquired).Lease

        let! renewed = service.Renew(lease, TimeSpan.FromMinutes 5., CancellationToken.None)

        checkRenewedFresh lease renewed

        // The old token is stale after a renewal: the renewal is lost.
        let! lost = service.Renew(lease, TimeSpan.FromMinutes 5., CancellationToken.None)

        checkRenewedStale lost
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``Verify reports the current holder and Release frees the lease`` () =
    let service = FakeLeaseService() :> IAgentPackageLeaseService

    task {
        let! state = service.Acquire(tenant, agent, "worker-a", TimeSpan.FromMinutes 5., CancellationToken.None)
        let lease = (state :?> PackageLeaseAcquired).Lease

        let! held = service.Verify(lease, CancellationToken.None)
        held |> should equal true

        let! released = service.Release(lease, CancellationToken.None)
        released |> should equal true

        let! verifiedAfterRelease = service.Verify(lease, CancellationToken.None)
        verifiedAfterRelease |> should equal false

        // Release is best-effort: an already-released lease is a no-op.
        let! releasedAgain = service.Release(lease, CancellationToken.None)
        releasedAgain |> should equal false

        // The freed lease can be re-acquired by another owner.
        let! reacquired = service.Acquire(tenant, agent, "worker-b", TimeSpan.FromMinutes 5., CancellationToken.None)

        match reacquired with
        | :? PackageLeaseAcquired -> ()
        | _ -> failwith "expected PackageLeaseAcquired"
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``Release rejects a foreign token without dropping the held lease`` () =
    let service = FakeLeaseService() :> IAgentPackageLeaseService

    task {
        let! first = service.Acquire(tenant, agent, "worker-a", TimeSpan.FromMinutes 5., CancellationToken.None)
        let heldLease = (first :?> PackageLeaseAcquired).Lease

        let! second = service.Acquire(tenant, agent, "worker-b", TimeSpan.FromMinutes 5., CancellationToken.None)

        match second with
        | :? PackageLeaseUnavailable -> ()
        | _ -> failwith "expected PackageLeaseUnavailable"

        // A fabricated token cannot release someone else's lease.
        let forged =
            AgentPackageLease(tenant, agent, "worker-b", "forged", heldLease.ExpiresAt)

        let! released = service.Release(forged, CancellationToken.None)
        released |> should equal false

        let! stillHeld = service.Verify(heldLease, CancellationToken.None)
        stillHeld |> should equal true
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``PackageLeaseOptions defaults are the pinned values`` () =
    let options = PackageLeaseOptions()

    options.RenewInterval |> should equal (TimeSpan.FromSeconds 10.)
    options.RenewalTimeout |> should equal (TimeSpan.FromSeconds 5.)

[<Fact>]
let ``PackageLeaseOptions are mutable`` () =
    let options = PackageLeaseOptions()
    options.RenewInterval <- TimeSpan.FromSeconds 1.
    options.RenewalTimeout <- TimeSpan.FromSeconds 2.

    options.RenewInterval |> should equal (TimeSpan.FromSeconds 1.)
    options.RenewalTimeout |> should equal (TimeSpan.FromSeconds 2.)

[<Fact>]
let ``WithLease runs work and releases the lease afterwards`` () =
    let leased = FakeLeaseService() :> IAgentPackageLeaseService

    task {
        let! result =
            leased.WithLease(
                tenant,
                agent,
                "worker-a",
                TimeSpan.FromMinutes 5.,
                null,
                (fun (_: CancellationToken) -> Task.FromResult 42),
                CancellationToken.None
            )

        result |> should equal 42

        // WithLease released the lease, so another owner can acquire it.
        let! after = leased.Acquire(tenant, agent, "worker-b", TimeSpan.FromMinutes 5., CancellationToken.None)

        match after with
        | :? PackageLeaseAcquired -> ()
        | _ -> failwith "expected PackageLeaseAcquired"
    }
    |> (fun t -> t.Wait())

[<Fact>]
let ``WithLease releases the lease when the work fails`` () =
    let leased = FakeLeaseService() :> IAgentPackageLeaseService

    task {
        try
            let! _ =
                leased.WithLease(
                    tenant,
                    agent,
                    "worker-a",
                    TimeSpan.FromMinutes 5.,
                    null,
                    (fun (_: CancellationToken) -> Task.FromException<int>(InvalidOperationException "boom")),
                    CancellationToken.None
                )

            failwith "expected InvalidOperationException"
        with :? InvalidOperationException ->
            ()

        // The finally block released the lease despite the failure.
        let! after = leased.Acquire(tenant, agent, "worker-b", TimeSpan.FromMinutes 5., CancellationToken.None)

        match after with
        | :? PackageLeaseAcquired -> ()
        | _ -> failwith "expected PackageLeaseAcquired"
    }
    |> (fun t -> t.Wait())

/// Runs one package-service call and captures any exception instead of
/// raising, so the test's resumable body stays a straight-line await plus
/// a return. The call is deferred so synchronous throws are captured too.
let private captureCall (call: unit -> Task) : Task<exn option> =
    task {
        try
            do! call ()
            return None
        with ex ->
            return Some ex
    }

/// Asserts the captured outcome is the acquire refusal naming worker-a.
/// Pure so the resumable test stays a straight-line await plus a return.
let private checkLeaseDenied (captured: exn option) =
    match captured with
    | Some(:? PackageLeaseException as exn) ->
        exn.Operation |> should equal "acquire"
        exn.AgentId |> should equal agent
        exn.Owner |> should equal "worker-a"
    | Some unexpected -> failwith $"expected PackageLeaseException but got {unexpected.GetType().Name}"
    | None -> failwith "expected PackageLeaseException"

[<Fact>]
let ``WithLease throws PackageLeaseException when the acquire fails`` () =
    let leased = FakeLeaseService() :> IAgentPackageLeaseService

    task {
        // Hold the lease from another owner.
        let! _ = leased.Acquire(tenant, agent, "holder", TimeSpan.FromMinutes 5., CancellationToken.None)

        let! captured =
            captureCall (fun () ->
                leased.WithLease(
                    tenant,
                    agent,
                    "worker-a",
                    TimeSpan.FromMinutes 5.,
                    null,
                    (fun (_: CancellationToken) -> Task.FromResult 1),
                    CancellationToken.None
                )
                :> Task)

        checkLeaseDenied captured
    }
    |> (fun t -> t.Wait())
