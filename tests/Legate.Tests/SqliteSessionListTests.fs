// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.SqliteSessionListTests

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Legate.Storage.Sqlite
open Legate.Testing
open Xunit

// SQLite ListSessions filters (issue 124): the agent and created-time
// filters narrow the ordered page and paging walks the filtered set. Each
// test owns a temp-file database over a controllable TestClock, so the
// created stamps separate deterministically.

let private tenant = TenantId.Create "sqlite-list"
let private noState = Unchecked.defaultof<Nullable<SessionState>>
let private noAgent = Unchecked.defaultof<Nullable<AgentId>>
let private noInstant = Unchecked.defaultof<Nullable<DateTimeOffset>>

let private sessionWith (agent: AgentId) =
    {
        Id = SessionId.New()
        Tenant = tenant
        AgentId = agent
        Title = ""
        State = SessionState.Idle
        CurrentTurnId = Unchecked.defaultof<Nullable<TurnId>>
        CreatedAt = DateTimeOffset.UtcNow
        UpdatedAt = DateTimeOffset.UtcNow
        ClosedAt = Unchecked.defaultof<Nullable<DateTimeOffset>>
        WorkspaceBinding = null
        Options = SessionOptions()
        PermissionGrants = ResizeArray<string>() :> IReadOnlyList<string>
    }

let private useStore (work: ISessionStore -> TestClock -> Task) : unit =
    let clock = TestClock()
    let database, path = SqliteTestFixture.openTestDatabase clock

    try
        try
            work (SqliteStoreFactory.sessionStore database) clock |> (fun t -> t.Wait())
        finally
            (database :> IDisposable).Dispose()
    finally
        SqliteTestFixture.deleteDatabaseFiles path

[<Fact>]
let ``Agent and created filters narrow the ordered page`` () =
    useStore (fun store clock ->
        task {
            let agentA = AgentId.New()
            let agentB = AgentId.New()

            let! first = store.CreateSession(tenant, sessionWith agentA, CancellationToken.None)
            clock.Advance(TimeSpan.FromHours 1.0)
            let middleAt = clock.GetUtcNow()
            let! _ = store.CreateSession(tenant, sessionWith agentB, CancellationToken.None)
            clock.Advance(TimeSpan.FromHours 1.0)
            let! last = store.CreateSession(tenant, sessionWith agentA, CancellationToken.None)

            let! byAgent =
                store.ListSessions(
                    tenant,
                    noState,
                    Nullable agentA,
                    noInstant,
                    noInstant,
                    10,
                    null,
                    CancellationToken.None
                )

            byAgent.Items.Count |> should equal 2
            byAgent.Items |> Seq.map (fun s -> s.Id) |> should contain first.Id
            byAgent.Items |> Seq.map (fun s -> s.Id) |> should contain last.Id

            let! fromMiddle =
                store.ListSessions(
                    tenant,
                    noState,
                    noAgent,
                    Nullable middleAt,
                    noInstant,
                    10,
                    null,
                    CancellationToken.None
                )

            fromMiddle.Items.Count |> should equal 2

            let! toMiddle =
                store.ListSessions(
                    tenant,
                    noState,
                    noAgent,
                    noInstant,
                    Nullable middleAt,
                    10,
                    null,
                    CancellationToken.None
                )

            toMiddle.Items.Count |> should equal 2

            let! combined =
                store.ListSessions(
                    tenant,
                    noState,
                    Nullable agentA,
                    Nullable middleAt,
                    noInstant,
                    10,
                    null,
                    CancellationToken.None
                )

            combined.Items.Count |> should equal 1
            combined.Items[0].Id |> should equal last.Id
        })

[<Fact>]
let ``Paging walks filtered rows exactly once`` () =
    useStore (fun store _ ->
        task {
            let target = AgentId.New()

            for _ in 1..5 do
                let! _ = store.CreateSession(tenant, sessionWith target, CancellationToken.None)
                ()

            for _ in 1..3 do
                let! _ = store.CreateSession(tenant, sessionWith (AgentId.New()), CancellationToken.None)
                ()

            let seen = ResizeArray<SessionId>()
            let mutable continuation: string | null = null
            let mutable more = true

            while more do
                let! page =
                    store.ListSessions(
                        tenant,
                        noState,
                        Nullable target,
                        noInstant,
                        noInstant,
                        2,
                        continuation,
                        CancellationToken.None
                    )

                for item in page.Items do
                    seen.Add(item.Id)

                continuation <- page.Continuation
                more <- not (isNull (box page.Continuation))

            seen.Count |> should equal 5
            seen |> Seq.distinct |> Seq.length |> should equal 5
        })
