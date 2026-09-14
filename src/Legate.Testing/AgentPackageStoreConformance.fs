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
