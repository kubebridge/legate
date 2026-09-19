// SPDX-License-Identifier: Apache-2.0
namespace Legate.Storage.Postgres.Tests

open System
open System.Threading
open Legate
open Legate.Storage.Postgres.Tests.PostgresTestDatabase
open Xunit

// Explicit tenant-isolation proof across two tenants: sessions, the event
// journal, and agents resolve nothing across the boundary. The conformance
// suites pin the same rules; these tests name the guarantee in one place
// with fresh tenants, so parallel suites never share rows.
module PostgresTenantIsolationTests =

    [<Fact>]
    let ``Sessions resolve null and count zero across tenants`` () =
        task {
            let _, sessions, _, _ = createStores ()
            let tenant = freshTenant "iso-sessions"
            let other = freshTenant "iso-sessions-other"

            let! created = sessions.CreateSession(tenant, sampleSession tenant, CancellationToken.None)

            let! wrongTenant = sessions.GetSession(other, created.Id, CancellationToken.None)
            Assert.Null(wrongTenant)

            let! listed =
                sessions.ListSessions(
                    other,
                    Nullable(),
                    Nullable(),
                    Nullable(),
                    Nullable(),
                    10,
                    null,
                    CancellationToken.None
                )

            Assert.Empty(listed.Items)

            // Filtered listing stays tenant-scoped: the agent and
            // created-time filters narrow within the tenant, never across
            // it.
            let! ownAgent =
                sessions.ListSessions(
                    tenant,
                    Nullable(),
                    Nullable created.AgentId,
                    Nullable(),
                    Nullable(),
                    10,
                    null,
                    CancellationToken.None
                )

            Assert.Single(ownAgent.Items) |> ignore

            let! otherAgent =
                sessions.ListSessions(
                    other,
                    Nullable(),
                    Nullable created.AgentId,
                    Nullable(),
                    Nullable(),
                    10,
                    null,
                    CancellationToken.None
                )

            Assert.Empty(otherAgent.Items)

            let! tooNew =
                sessions.ListSessions(
                    tenant,
                    Nullable(),
                    Nullable(),
                    Nullable(DateTimeOffset.UtcNow.AddHours 1.0),
                    Nullable(),
                    10,
                    null,
                    CancellationToken.None
                )

            Assert.Empty(tooNew.Items)

            let! wide =
                sessions.ListSessions(
                    tenant,
                    Nullable(),
                    Nullable(),
                    Nullable(DateTimeOffset.UtcNow.AddHours -1.0),
                    Nullable(DateTimeOffset.UtcNow.AddHours 1.0),
                    10,
                    null,
                    CancellationToken.None
                )

            Assert.Single(wide.Items) |> ignore

            let! otherCount = sessions.CountSessionsByTenant(other, CancellationToken.None)
            Assert.Equal(0, otherCount)

            Assert.Throws<SessionNotFoundException>(fun () ->
                sessions
                    .UpdateSessionState(other, created.Id, SessionState.Running, CancellationToken.None)
                    .GetAwaiter()
                    .GetResult()
                |> ignore)
            |> ignore
        }

    [<Fact>]
    let ``Inbox and journal writes in one tenant are invisible in the other`` () =
        task {
            let _, sessions, events, _ = createStores ()
            let tenant = freshTenant "iso-events"
            let other = freshTenant "iso-events-other"

            let! sessionId, claim = claimedSession sessions tenant "iso-owner"

            let! appended =
                events.Append(tenant, sessionId, claim.Token, [ delta sessionId claim.TurnId ], CancellationToken.None)

            Assert.True(appended :? EventAppended)

            let! replayed = events.Replay(other, sessionId, 0L, 10, CancellationToken.None)
            Assert.True(replayed :? EventReplayUnknownSession)

            Assert.Throws<SessionNotFoundException>(fun () ->
                sessions.ReadPendingInbox(other, sessionId, CancellationToken.None).GetAwaiter().GetResult()
                |> ignore)
            |> ignore
        }

    [<Fact>]
    let ``Agents and custom tools resolve null and list empty across tenants`` () =
        task {
            let _, _, _, agents = createStores ()
            let tenant = freshTenant "iso-agents"
            let other = freshTenant "iso-agents-other"

            let agentStore = agents :> IAgentStore
            let toolStore = agents :> IAgentCustomToolStore

            let! stored = agentStore.UpdateIfUnchanged(tenant, sampleAgent tenant, 0UL, CancellationToken.None)
            let agentId = (stored :?> AgentUpdated).Agent.Id

            let! wrongTenant = agentStore.GetAgent(other, agentId, CancellationToken.None)
            Assert.Null(wrongTenant)

            let! listed = agentStore.ListAgents(other, CancellationToken.None)
            Assert.Empty(listed)

            let! tools = toolStore.ListCustomTools(other, agentId, CancellationToken.None)
            Assert.Empty(tools)
        }
