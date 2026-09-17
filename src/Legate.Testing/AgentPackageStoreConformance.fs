// SPDX-License-Identifier: Apache-2.0
namespace Legate.Testing

open System
open System.Collections.Generic
open System.IO
open System.Threading
open System.Threading.Tasks
open Legate
open Xunit

/// The shared conformance suite for
/// <see cref="T:Legate.IAgentPackageStore" /> implementations: upload
/// publishes and activates atomically, replaces never change active
/// status, deleting the active version clears it, info derives from the
/// active version's layout, duplicate paths reject, versions validate, and
/// tenancy holds.
[<AbstractClass>]
type AgentPackageStoreConformance(store: IAgentPackageStore, tenant: TenantId) =

    do
        if isNull (box store) then
            raise (ArgumentNullException(nameof store))

    /// The store under test.
    member this.Store = store

    /// The primary tenant every row belongs to.
    member this.Tenant = tenant

    /// The second tenant proving isolation.
    member this.OtherTenant = TenantId.Create "other"

    /// Constructs the suite over the store, in the primary tenant.
    new(store) = AgentPackageStoreConformance(store, TenantId.Create "conformance")

    /// One agent id the suite packages for; the package store is decoupled
    /// from the agent rows, so any id serves.
    member this.AgentId = AgentId.New()

    /// One entry stream: a single file from a string.
    static member EntriesText(path: string, text: string) =
        let bytes = System.Text.Encoding.UTF8.GetBytes text

        let entry = AgentPackageEntry(path, new MemoryStream(bytes))

        let mutable current = Some entry
        let mutable doneOnce = false

        { new IAsyncEnumerable<AgentPackageEntry> with
            member this.GetAsyncEnumerator(_: CancellationToken) =
                { new IAsyncEnumerator<AgentPackageEntry> with
                    member this.MoveNextAsync() =
                        let mutable moved = false

                        if not doneOnce then
                            moved <- true
                            doneOnce <- true

                        ValueTask<bool>(moved)

                    member this.Current: AgentPackageEntry =
                        (match current with
                         | Some entry -> entry
                         | None -> failwith "enumerator exhausted")

                    member this.DisposeAsync() : ValueTask = ValueTask()
                }
        }

    [<Fact>]
    member this.``Upload publishes and activates atomically``() =
        task {
            let agentId = this.AgentId

            let! uploaded =
                store.UploadPackage(
                    tenant,
                    agentId,
                    "1.0.0",
                    "manual-zip",
                    AgentPackageStoreConformance.EntriesText("AGENTS.md", "Be helpful."),
                    CancellationToken.None
                )

            Assert.Equal("1.0.0", uploaded.Version)

            let! info = store.GetPackageInfo(tenant, agentId, CancellationToken.None)

            let info =
                match info with
                | null -> failwith "expected package info after upload"
                | nonNull -> nonNull

            Assert.Equal("Be helpful.", info.Instructions)
            Assert.Equal("1.0.0", info.ActiveVersion)
            Assert.Equal("manual-zip", info.Source)
        }

    [<Fact>]
    member this.``Replace never activates and preserves CreatedAt``() =
        task {
            let agentId = this.AgentId

            let! uploaded =
                store.UploadPackage(
                    tenant,
                    agentId,
                    "1.0.0",
                    "first",
                    AgentPackageStoreConformance.EntriesText("AGENTS.md", "one"),
                    CancellationToken.None
                )

            let! _next =
                store.UploadPackage(
                    tenant,
                    agentId,
                    "2.0.0",
                    "second",
                    AgentPackageStoreConformance.EntriesText("AGENTS.md", "two"),
                    CancellationToken.None
                )

            Assert.Equal("2.0.0", (this.InfoActiveOf store tenant agentId))

            let! replaced =
                store.ReplacePackageVersion(
                    tenant,
                    agentId,
                    "1.0.0",
                    "refresh",
                    AgentPackageStoreConformance.EntriesText("AGENTS.md", "one refreshed"),
                    CancellationToken.None
                )

            Assert.Equal(uploaded.CreatedAt, replaced.CreatedAt)

            // The active version is unchanged: replace never activates.
            Assert.Equal("2.0.0", (this.InfoActiveOf store tenant agentId))
        }

    /// Reads the active version through the info the store derives.
    member this.InfoActiveOf (store: IAgentPackageStore) (tenant: TenantId) (agentId: AgentId) =
        match store.GetPackageInfo(tenant, agentId, CancellationToken.None).Result with
        | null -> failwith "expected package info"
        | info -> info.ActiveVersion

    [<Fact>]
    member this.``Deleting the active version clears it``() =
        task {
            let agentId = this.AgentId

            let! _ =
                store.UploadPackage(
                    tenant,
                    agentId,
                    "1.0.0",
                    "only",
                    AgentPackageStoreConformance.EntriesText("AGENTS.md", "only"),
                    CancellationToken.None
                )

            let! deleted = store.DeletePackageVersion(tenant, agentId, "1.0.0", CancellationToken.None)

            Assert.True(deleted)

            let! info = store.GetPackageInfo(tenant, agentId, CancellationToken.None)

            Assert.Null(info)

            // Idempotent: deleting again reports false.
            let! again = store.DeletePackageVersion(tenant, agentId, "1.0.0", CancellationToken.None)

            Assert.False(again)
        }

    [<Fact>]
    member this.``Duplicate normalised paths reject before anything lands``() =
        task {
            let agentId = this.AgentId

            let duplicate =
                let first =
                    AgentPackageEntry("AGENTS.md", new MemoryStream(System.Text.Encoding.UTF8.GetBytes "dup"))

                let second = AgentPackageEntry("AGENTS.md", new MemoryStream())

                { new IAsyncEnumerable<AgentPackageEntry> with
                    member _.GetAsyncEnumerator(_: CancellationToken) =
                        let mutable step = 0

                        { new IAsyncEnumerator<AgentPackageEntry> with
                            member _.MoveNextAsync() =
                                step <- step + 1
                                ValueTask<bool>(step <= 2)

                            member _.Current: AgentPackageEntry = (if step = 1 then first else second)

                            member _.DisposeAsync() : ValueTask = ValueTask()
                        }
                }

            Assert.Throws<ArgumentException>(fun () ->
                store
                    .UploadPackage(tenant, agentId, "1.0.0", "dupes", duplicate, CancellationToken.None)
                    .GetAwaiter()
                    .GetResult()
                |> ignore)
            |> ignore

            let! info = store.GetPackageInfo(tenant, agentId, CancellationToken.None)

            Assert.Null(info)
        }

    [<Fact>]
    member this.``Tenancy holds across every read``() =
        task {
            let agentId = this.AgentId

            let! _ =
                store.UploadPackage(
                    tenant,
                    agentId,
                    "1.0.0",
                    "tenant",
                    AgentPackageStoreConformance.EntriesText("AGENTS.md", "tenant scoped"),
                    CancellationToken.None
                )

            let! wrongTenant = store.GetPackageInfo(this.OtherTenant, agentId, CancellationToken.None)

            Assert.Null(wrongTenant)

            let! wrongVersions = store.ListVersions(this.OtherTenant, agentId, CancellationToken.None)

            Assert.Equal(0, wrongVersions.Count)
        }

    /// One entry stream per pair: the multi-file uploads prefix scans need.
    static member EntriesTexts(entries: (string * string) list) =
        let prepared =
            entries
            |> List.map (fun (path, text) ->
                AgentPackageEntry(path, new MemoryStream(System.Text.Encoding.UTF8.GetBytes text)))
            |> List.toArray

        { new IAsyncEnumerable<AgentPackageEntry> with
            member _.GetAsyncEnumerator(_: CancellationToken) =
                let mutable index = -1

                { new IAsyncEnumerator<AgentPackageEntry> with
                    member _.MoveNextAsync() =
                        index <- index + 1
                        ValueTask<bool>(index < prepared.Length)

                    member _.Current: AgentPackageEntry = prepared[index]

                    member _.DisposeAsync() : ValueTask = ValueTask()
                }
        }

    [<Fact>]
    member this.``ListFiles scans one version by prefix in lexicographic order``() =
        task {
            let agentId = this.AgentId

            let! _ =
                store.UploadPackage(
                    tenant,
                    agentId,
                    "1.0.0",
                    "prefix",
                    AgentPackageStoreConformance.EntriesTexts(
                        [
                            "AGENTS.md", "Be helpful."
                            ".agent/skills/deploy/SKILL.md", "name: deploy"
                            ".agent/skills/deploy/refs/api.md", "api notes"
                            ".agent/skills/other/SKILL.md", "name: other"
                            ".agent/agents/helper/AGENT.md", "name: helper"
                        ]
                    ),
                    CancellationToken.None
                )

            // A skill directory scans with or without the trailing
            // separator, ordinally ordered and scoped to the directory.
            for prefix in
                [
                    ".agent/skills/deploy"
                    ".agent/skills/deploy/"
                ] do
                let! listed = store.ListFiles(tenant, agentId, "1.0.0", prefix, CancellationToken.None)

                Assert.Equal<string list>(
                    [
                        ".agent/skills/deploy/SKILL.md"
                        ".agent/skills/deploy/refs/api.md"
                    ],
                    listed |> Seq.toList
                )

            // A wider prefix sees every skill file but not AGENTS.md: the
            // top-level instructions file is not under the .agent prefix.
            let! skills = store.ListFiles(tenant, agentId, "1.0.0", ".agent/skills", CancellationToken.None)

            Assert.Equal<string list>(
                [
                    ".agent/skills/deploy/SKILL.md"
                    ".agent/skills/deploy/refs/api.md"
                    ".agent/skills/other/SKILL.md"
                ],
                skills |> Seq.toList
            )
        }

    [<Fact>]
    member this.``ListFiles is empty on absent data and validates its inputs``() =
        task {
            let agentId = this.AgentId

            let! _ =
                store.UploadPackage(
                    tenant,
                    agentId,
                    "1.0.0",
                    "prefix",
                    AgentPackageStoreConformance.EntriesText("AGENTS.md", "Be helpful."),
                    CancellationToken.None
                )

            // An unknown version and a foreign tenant scan empty: absent
            // data is an expected branch, never an exception.
            let! unknown = store.ListFiles(tenant, agentId, "2.0.0", ".agent/skills", CancellationToken.None)

            Assert.Equal(0, unknown.Count)

            let! foreign = store.ListFiles(this.OtherTenant, agentId, "1.0.0", ".agent/skills", CancellationToken.None)

            Assert.Equal(0, foreign.Count)

            // Control-plane preconditions still throw: the version rule,
            // the null prefix, and the path rule.
            Assert.Throws<ArgumentException>(fun () ->
                store
                    .ListFiles(tenant, agentId, "no spaces", ".agent/skills", CancellationToken.None)
                    .GetAwaiter()
                    .GetResult()
                |> ignore)
            |> ignore

            Assert.Throws<ArgumentNullException>(fun () ->
                store
                    .ListFiles(tenant, agentId, "1.0.0", Unchecked.defaultof<string>, CancellationToken.None)
                    .GetAwaiter()
                    .GetResult()
                |> ignore)
            |> ignore

            Assert.Throws<InvalidPackagePathException>(fun () ->
                store.ListFiles(tenant, agentId, "1.0.0", "../escape", CancellationToken.None).GetAwaiter().GetResult()
                |> ignore)
            |> ignore
        }

    /// One entry stream that yields a single valid entry and then fails
    /// mid-enumeration: the failed-upload fact's portable fault, proving a
    /// broken upload leaves the previous version active on every store.
    static member EntriesFailing(path: string, text: string, failure: exn) =
        let bytes = System.Text.Encoding.UTF8.GetBytes text
        let entry = AgentPackageEntry(path, new MemoryStream(bytes))

        { new IAsyncEnumerable<AgentPackageEntry> with
            member _.GetAsyncEnumerator(_: CancellationToken) =
                let mutable step = 0

                { new IAsyncEnumerator<AgentPackageEntry> with
                    member _.MoveNextAsync() =
                        step <- step + 1

                        if step = 1 then ValueTask<bool>(true) else raise failure

                    member _.Current: AgentPackageEntry = entry

                    member _.DisposeAsync() : ValueTask = ValueTask()
                }
        }

    [<Fact>]
    member this.``Invalid versions reject on every entry point``() =
        task {
            let agentId = this.AgentId

            let entries () =
                AgentPackageStoreConformance.EntriesText("AGENTS.md", "Be helpful.")

            Assert.Throws<ArgumentException>(fun () ->
                store
                    .UploadPackage(tenant, agentId, "no spaces", "bad", entries (), CancellationToken.None)
                    .GetAwaiter()
                    .GetResult()
                |> ignore)
            |> ignore

            Assert.Throws<ArgumentException>(fun () ->
                store
                    .ReplacePackageVersion(tenant, agentId, "no spaces", "bad", entries (), CancellationToken.None)
                    .GetAwaiter()
                    .GetResult()
                |> ignore)
            |> ignore

            Assert.Throws<ArgumentException>(fun () ->
                store
                    .DeletePackageVersion(tenant, agentId, "no spaces", CancellationToken.None)
                    .GetAwaiter()
                    .GetResult()
                |> ignore)
            |> ignore

            Assert.Throws<ArgumentException>(fun () ->
                store
                    .ReadFile(tenant, agentId, "no spaces", "AGENTS.md", CancellationToken.None)
                    .GetAwaiter()
                    .GetResult()
                |> ignore)
            |> ignore

            Assert.Throws<ArgumentException>(fun () ->
                store
                    .ListFiles(tenant, agentId, "no spaces", ".agent/skills", CancellationToken.None)
                    .GetAwaiter()
                    .GetResult()
                |> ignore)
            |> ignore

            let! info = store.GetPackageInfo(tenant, agentId, CancellationToken.None)

            Assert.Null(info)

            let! versions = store.ListVersions(tenant, agentId, CancellationToken.None)

            Assert.Equal(0, versions.Count)
        }

    [<Fact>]
    member this.``A failed upload keeps the previous version active``() =
        task {
            let agentId = this.AgentId

            let! _ =
                store.UploadPackage(
                    tenant,
                    agentId,
                    "1.0.0",
                    "first",
                    AgentPackageStoreConformance.EntriesText("AGENTS.md", "one"),
                    CancellationToken.None
                )

            let failing =
                AgentPackageStoreConformance.EntriesFailing(
                    "AGENTS.md",
                    "partial",
                    InvalidOperationException("The entry stream failed mid-upload.")
                )

            Assert.Throws<InvalidOperationException>(fun () ->
                store
                    .UploadPackage(tenant, agentId, "2.0.0", "broken", failing, CancellationToken.None)
                    .GetAwaiter()
                    .GetResult()
                |> ignore)
            |> ignore

            let! info = store.GetPackageInfo(tenant, agentId, CancellationToken.None)

            match info with
            | null -> failwith "expected the previous version to stay active after a failed upload"
            | active ->
                Assert.Equal("1.0.0", active.ActiveVersion)
                Assert.Equal("one", active.Instructions)
                Assert.Equal("first", active.Source)

            let! versions = store.ListVersions(tenant, agentId, CancellationToken.None)

            Assert.Equal<string list>(
                [ "1.0.0" ],
                versions |> Seq.map (fun packageVersion -> packageVersion.Version) |> Seq.toList
            )

            let! stream = store.ReadFile(tenant, agentId, "1.0.0", "AGENTS.md", CancellationToken.None)

            match stream with
            | null -> failwith "expected the previous version's bytes after a failed upload"
            | readable ->
                use reader = new StreamReader(readable)
                let! text = reader.ReadToEndAsync()
                Assert.Equal("one", text)
        }

    [<Fact>]
    member this.``Deleting the active version never auto-promotes and the next upload re-activates``() =
        task {
            let agentId = this.AgentId

            let! _ =
                store.UploadPackage(
                    tenant,
                    agentId,
                    "1.0.0",
                    "first",
                    AgentPackageStoreConformance.EntriesText("AGENTS.md", "one"),
                    CancellationToken.None
                )

            let! _ =
                store.UploadPackage(
                    tenant,
                    agentId,
                    "2.0.0",
                    "second",
                    AgentPackageStoreConformance.EntriesText("AGENTS.md", "two"),
                    CancellationToken.None
                )

            let! deleted = store.DeletePackageVersion(tenant, agentId, "2.0.0", CancellationToken.None)

            Assert.True(deleted)

            // No auto-promote: the surviving version stays stored and
            // readable, but nothing is active until the next upload.
            let! cleared = store.GetPackageInfo(tenant, agentId, CancellationToken.None)

            Assert.Null(cleared)

            let! versions = store.ListVersions(tenant, agentId, CancellationToken.None)

            Assert.Equal<string list>(
                [ "1.0.0" ],
                versions |> Seq.map (fun packageVersion -> packageVersion.Version) |> Seq.toList
            )

            let! stream = store.ReadFile(tenant, agentId, "1.0.0", "AGENTS.md", CancellationToken.None)

            match stream with
            | null -> failwith "expected the surviving version to stay readable"
            | readable ->
                use reader = new StreamReader(readable)
                let! text = reader.ReadToEndAsync()
                Assert.Equal("one", text)

            let! _ =
                store.UploadPackage(
                    tenant,
                    agentId,
                    "3.0.0",
                    "third",
                    AgentPackageStoreConformance.EntriesText("AGENTS.md", "three"),
                    CancellationToken.None
                )

            let! reactivated = store.GetPackageInfo(tenant, agentId, CancellationToken.None)

            match reactivated with
            | null -> failwith "expected the next upload to re-activate the package"
            | info ->
                Assert.Equal("3.0.0", info.ActiveVersion)
                Assert.Equal("three", info.Instructions)
        }
