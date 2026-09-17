// SPDX-License-Identifier: Apache-2.0
namespace Legate.Tests

open System
open System.Collections.Generic
open System.IO
open System.Text
open System.Threading
open System.Threading.Tasks
open Legate
open Legate.Storage.FileSystem
open Legate.Testing
open Xunit

/// The file-system agent package store derives the shared conformance suite
/// over a temp-directory root.
type FileSystemAgentPackageTests private (store: IAgentPackageStore, root: string) =
    inherit AgentPackageStoreConformance(store)

    new() =
        let root = FileSystemTestRoot.fresh ()

        new FileSystemAgentPackageTests(FileSystemAgentPackageStore(FileSystemTestRoot.optionsFor root ignore), root)

    interface IDisposable with
        member _.Dispose() = FileSystemTestRoot.deleteRoot root

/// Local-only edge tests the shared suite cannot pin: atomic staging,
/// layout on disk, active-pointer clearing, and input validation.
module FileSystemAgentPackageEdgeTests =

    let private textEntries (entries: (string * string) list) : IAsyncEnumerable<AgentPackageEntry> =
        let prepared =
            entries
            |> List.map (fun (path, text) -> AgentPackageEntry(path, new MemoryStream(Encoding.UTF8.GetBytes text)))
            |> List.toArray

        let mutable index = -1

        { new IAsyncEnumerable<AgentPackageEntry> with
            member _.GetAsyncEnumerator(_: CancellationToken) =
                index <- -1

                { new IAsyncEnumerator<AgentPackageEntry> with
                    member _.MoveNextAsync() =
                        index <- index + 1
                        ValueTask<bool>(index < prepared.Length)

                    member _.Current: AgentPackageEntry = prepared[index]

                    member _.DisposeAsync() : ValueTask = ValueTask()
                }
        }

    /// Runs work over a file-system package store on a fresh temp root,
    /// deleting the root afterwards.
    let private useStore (work: IAgentPackageStore -> string -> Task) : Task =
        task {
            let root = FileSystemTestRoot.fresh ()

            let store =
                FileSystemAgentPackageStore(FileSystemTestRoot.optionsFor root ignore) :> IAgentPackageStore

            try
                do! work store root
            finally
                FileSystemTestRoot.deleteRoot root
        }

    [<Fact>]
    let ``ReadFile returns a stream from its start`` () =
        useStore (fun store _ ->
            task {
                let tenant = TenantId.Create "local"
                let agentId = AgentId.New()

                let! _ =
                    store.UploadPackage(
                        tenant,
                        agentId,
                        "1.0.0",
                        "local",
                        textEntries [ ("AGENTS.md", "readable") ],
                        CancellationToken.None
                    )

                let! stream = store.ReadFile(tenant, agentId, "1.0.0", "AGENTS.md", CancellationToken.None)

                match stream with
                | null -> failwith "expected the file stream"
                | readable ->
                    use reader = new StreamReader(readable)
                    let! text = reader.ReadToEndAsync()
                    Assert.Equal("readable", text)

                let! absent = store.ReadFile(tenant, agentId, "1.0.0", "MISSING.md", CancellationToken.None)
                Assert.Null(absent)

                let! absentVersion = store.ReadFile(tenant, agentId, "9.9.9", "AGENTS.md", CancellationToken.None)
                Assert.Null(absentVersion)
            })

    [<Fact>]
    let ``ListVersions orders lexicographically`` () =
        useStore (fun store _ ->
            task {
                let tenant = TenantId.Create "local"
                let agentId = AgentId.New()

                for version in [ "0.9.0"; "1.0.0"; "10.0.0"; "2.0.0" ] do
                    let! _ =
                        store.UploadPackage(
                            tenant,
                            agentId,
                            version,
                            "local",
                            textEntries [ ("AGENTS.md", version) ],
                            CancellationToken.None
                        )

                    ()

                let! versions = store.ListVersions(tenant, agentId, CancellationToken.None)

                let names =
                    versions |> Seq.map (fun packageVersion -> packageVersion.Version) |> Seq.toList

                Assert.Equal<string list>([ "0.9.0"; "1.0.0"; "10.0.0"; "2.0.0" ], names)
            })

    [<Fact>]
    let ``Duplicate uploads land nothing and keep no partial version`` () =
        useStore (fun store root ->
            task {
                let tenant = TenantId.Create "local"
                let agentId = AgentId.New()

                let duplicate =
                    textEntries
                        [
                            ("AGENTS.md", "dup")
                            ("AGENTS.md", "dup again")
                        ]

                Assert.Throws<ArgumentException>(fun () ->
                    store
                        .UploadPackage(tenant, agentId, "1.0.0", "dupes", duplicate, CancellationToken.None)
                        .GetAwaiter()
                        .GetResult()
                    |> ignore)
                |> ignore

                let! info = store.GetPackageInfo(tenant, agentId, CancellationToken.None)
                Assert.Null(info)

                let! versions = store.ListVersions(tenant, agentId, CancellationToken.None)
                Assert.Equal(0, versions.Count)

                // No version directory and no staging residue either.
                Assert.Empty(
                    Directory.EnumerateFileSystemEntries(
                        Path.Combine(root, FileSystemPaths.packagesDirectoryName),
                        "*",
                        SearchOption.AllDirectories
                    )
                )

                Assert.Empty(
                    Directory.EnumerateFiles(
                        Path.Combine(root, FileSystemPaths.stagingDirectoryName),
                        "*",
                        SearchOption.AllDirectories
                    )
                )
            })

    [<Fact>]
    let ``Re-upload preserves CreatedAt and replaces content`` () =
        useStore (fun store _ ->
            task {
                let tenant = TenantId.Create "local"
                let agentId = AgentId.New()

                let! uploaded =
                    store.UploadPackage(
                        tenant,
                        agentId,
                        "1.0.0",
                        "first",
                        textEntries [ ("AGENTS.md", "one") ],
                        CancellationToken.None
                    )

                let! reuploaded =
                    store.UploadPackage(
                        tenant,
                        agentId,
                        "1.0.0",
                        "second",
                        textEntries [ ("AGENTS.md", "one replaced") ],
                        CancellationToken.None
                    )

                Assert.Equal(uploaded.CreatedAt, reuploaded.CreatedAt)

                let! info = store.GetPackageInfo(tenant, agentId, CancellationToken.None)

                match info with
                | null -> failwith "expected package info after re-upload"
                | info ->
                    Assert.Equal("one replaced", info.Instructions)
                    Assert.Equal("1.0.0", info.ActiveVersion)
                    Assert.Equal("second", info.Source)
            })

    [<Fact>]
    let ``Replace on an unknown version throws and stores nothing`` () =
        useStore (fun store _ ->
            task {
                let tenant = TenantId.Create "local"
                let agentId = AgentId.New()

                Assert.Throws<ArgumentException>(fun () ->
                    store
                        .ReplacePackageVersion(
                            tenant,
                            agentId,
                            "9.9.9",
                            "refresh",
                            textEntries [ ("AGENTS.md", "ghost") ],
                            CancellationToken.None
                        )
                        .GetAwaiter()
                        .GetResult()
                    |> ignore)
                |> ignore

                let! info = store.GetPackageInfo(tenant, agentId, CancellationToken.None)
                Assert.Null(info)
            })

    [<Fact>]
    let ``Deleting a non-active version keeps the active one`` () =
        useStore (fun store _ ->
            task {
                let tenant = TenantId.Create "local"
                let agentId = AgentId.New()

                let! _ =
                    store.UploadPackage(
                        tenant,
                        agentId,
                        "1.0.0",
                        "first",
                        textEntries [ ("AGENTS.md", "one") ],
                        CancellationToken.None
                    )

                let! _ =
                    store.UploadPackage(
                        tenant,
                        agentId,
                        "2.0.0",
                        "second",
                        textEntries [ ("AGENTS.md", "two") ],
                        CancellationToken.None
                    )

                let! deleted = store.DeletePackageVersion(tenant, agentId, "1.0.0", CancellationToken.None)
                Assert.True(deleted)

                let! info = store.GetPackageInfo(tenant, agentId, CancellationToken.None)

                match info with
                | null -> failwith "expected package info after deleting the non-active version"
                | info ->
                    Assert.Equal("2.0.0", info.ActiveVersion)
                    Assert.Equal("two", info.Instructions)
            })

    [<Fact>]
    let ``Versions and paths validate before anything lands`` () =
        useStore (fun store _ ->
            task {
                let tenant = TenantId.Create "local"
                let agentId = AgentId.New()

                Assert.Throws<ArgumentException>(fun () ->
                    store
                        .UploadPackage(
                            tenant,
                            agentId,
                            "no spaces",
                            "bad",
                            textEntries [ ("AGENTS.md", "bad") ],
                            CancellationToken.None
                        )
                        .GetAwaiter()
                        .GetResult()
                    |> ignore)
                |> ignore

                Assert.Throws<InvalidPackagePathException>(fun () ->
                    store
                        .ReadFile(tenant, agentId, "1.0.0", "../escape", CancellationToken.None)
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
            })

    [<Fact>]
    let ``Upload pins the on-disk layout for future reuse`` () =
        useStore (fun store root ->
            task {
                let tenant = TenantId.Create "local"
                let agentId = AgentId.New()

                let! _ =
                    store.UploadPackage(
                        tenant,
                        agentId,
                        "1.0.0",
                        "layout",
                        textEntries
                            [
                                ("AGENTS.md", "Be helpful.")
                                (".agent/skills/deploy/SKILL.md", "deploy")
                                (".agent/agents/helper/AGENT.md", "helper")
                            ],
                        CancellationToken.None
                    )

                // Layout: packages/{tenant}/{agent}/versions/{version}/ plus
                // the stamp, pointer, and source files beside the content.
                let agentDir = Path.Combine(root, "packages", "local", agentId.Value)
                let versionDir = Path.Combine(agentDir, "versions", "1.0.0")

                Assert.True(File.Exists(Path.Combine(versionDir, "AGENTS.md")))
                Assert.True(File.Exists(Path.Combine(versionDir, ".agent", "skills", "deploy", "SKILL.md")))
                Assert.True(File.Exists(Path.Combine(agentDir, "versions", "1.0.0.json")))
                Assert.Equal("1.0.0", File.ReadAllText(Path.Combine(agentDir, "active.txt")).Trim())
                Assert.Equal("layout", File.ReadAllText(Path.Combine(agentDir, "source.txt")))

                let! info = store.GetPackageInfo(tenant, agentId, CancellationToken.None)

                match info with
                | null -> failwith "expected package info after layout upload"
                | info ->
                    Assert.Equal("Be helpful.", info.Instructions)
                    Assert.Equal<string list>([ "deploy" ], info.Skills |> Seq.toList)
                    Assert.Equal<string list>([ "helper" ], info.SubAgents |> Seq.toList)
            })
