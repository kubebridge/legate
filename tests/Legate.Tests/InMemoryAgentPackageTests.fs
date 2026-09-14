// SPDX-License-Identifier: Apache-2.0
namespace Legate.Tests

open System
open System.Collections.Generic
open System.IO
open System.Threading
open System.Threading.Tasks
open Legate
open Legate.Storage.InMemory
open Legate.Testing
open Xunit

/// The InMemory agent package store derives the shared conformance suite.
type InMemoryAgentPackageTests() =
    inherit AgentPackageStoreConformance(InMemoryStoreFactory.packageStore (InMemoryDatabase()))

/// The InMemory-specific package store rules the shared suite cannot pin
/// because they involve the database options or cross-implementation
/// details.
module InMemoryAgentPackageStoreLocalTests =

    let private textEntries (entries: (string * string) list) : IAsyncEnumerable<AgentPackageEntry> =
        let prepared =
            entries
            |> List.map (fun (path, text) ->
                AgentPackageEntry(path, new MemoryStream(System.Text.Encoding.UTF8.GetBytes text)))
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

    [<Fact>]
    let ``ReadFile returns a stream from its start`` () =
        task {
            let store = InMemoryStoreFactory.packageStore (InMemoryDatabase())
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
        }

    [<Fact>]
    let ``ListVersions orders lexicographically`` () =
        task {
            let store = InMemoryStoreFactory.packageStore (InMemoryDatabase())
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
        }
