// SPDX-License-Identifier: Apache-2.0
namespace Legate.Testing

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open Legate
open Xunit

/// The shared conformance suite for
/// <see cref="T:Legate.IAgentStore" /> and
/// <see cref="T:Legate.IAgentCustomToolStore" /> implementations: the
/// checked upsert (insert, update, conflict), the schedule query, the
/// custom-tool plain upsert with versioning and CreatedAt preservation,
/// name validation, the missing-agent precondition, and tenancy.
[<AbstractClass>]
type AgentStoreConformance(agentStore: IAgentStore, toolStore: IAgentCustomToolStore, tenant: TenantId) =

    do
        if isNull (box agentStore) then
            raise (ArgumentNullException(nameof agentStore))

        if isNull (box toolStore) then
            raise (ArgumentNullException(nameof toolStore))

    /// The agent store under test.
    member this.AgentStore = agentStore

    /// The custom-tool store under test.
    member this.ToolStore = toolStore

    /// The primary tenant every row belongs to.
    member this.Tenant = tenant

    /// The second tenant proving isolation.
    member this.OtherTenant = TenantId.Create "other"

    /// Constructs the suite over the stores, in the primary tenant.
    new(agentStore, toolStore) = AgentStoreConformance(agentStore, toolStore, TenantId.Create "conformance")

    /// A minimal agent the suite upserts under the tenant.
    member this.SampleAgent() =
        {
            Id = AgentId.New()
            Tenant = tenant
            Name = "conformance-agent"
            Description = null
            Model = ModelReference.Parse("test/model-a")
            SystemPrompt = "You are under test."
            EnvironmentVariables = null
            PermissionDefaults = null
            ToolSelection = null
            PackageReference = null
            Enabled = true
            Schedule = null
            RowVersion = 0UL
            CreatedAt = DateTimeOffset.MinValue
            UpdatedAt = DateTimeOffset.MinValue
        }

    /// A minimal custom tool the suite upserts under an agent.
    member this.SampleTool(agentId: AgentId) =
        {
            Tenant = tenant
            AgentId = agentId
            Name = "test_tool"
            Description = null
            Endpoint = Uri("https://example.test/hook")
            InputSchema = null
            Headers = null
            SigningSecret = [| 1uy; 2uy; 3uy |]
            Enabled = true
            RowVersion = 0UL
            CreatedAt = DateTimeOffset.MinValue
            UpdatedAt = DateTimeOffset.MinValue
        }

    [<Fact>]
    member this.``Expected version 0 inserts and version checks update``() =
        task {
            let agent = this.SampleAgent()

            let! inserted = agentStore.UpdateIfUnchanged(tenant, agent, 0UL, CancellationToken.None)

            let stored = (inserted :?> AgentUpdated).Agent
            Assert.Equal(1UL, stored.RowVersion)

            let renamed = { stored with Name = "renamed-agent" }

            let! updated = agentStore.UpdateIfUnchanged(tenant, renamed, stored.RowVersion, CancellationToken.None)

            Assert.True(updated :? AgentUpdated)

            let! conflicted = agentStore.UpdateIfUnchanged(tenant, renamed, 99UL, CancellationToken.None)

            Assert.True(conflicted :? AgentUpdateConflict)
            Assert.NotNull((conflicted :?> AgentUpdateConflict).Agent)
        }

    [<Fact>]
    member this.``A nonzero expected version on a missing agent throws``() =
        task {
            let missing = this.SampleAgent()

            Assert.Throws<AgentNotFoundException>(fun () ->
                agentStore.UpdateIfUnchanged(tenant, missing, 3UL, CancellationToken.None).GetAwaiter().GetResult()
                |> ignore)
            |> ignore
        }

    [<Fact>]
    member this.``The schedule query lists only enabled schedules``() =
        task {
            let scheduled =
                { this.SampleAgent() with
                    Schedule =
                        {
                            AgentSchedule.Cron = "* * * * *"
                            TimeZone = "UTC"
                            Message = "tick"
                            Enabled = true
                        }
                }

            let disabled =
                { this.SampleAgent() with
                    Schedule =
                        {
                            AgentSchedule.Cron = "0 0 * * *"
                            TimeZone = "UTC"
                            Message = "tock"
                            Enabled = false
                        }
                }

            let! storedScheduled = agentStore.UpdateIfUnchanged(tenant, scheduled, 0UL, CancellationToken.None)
            let! storedDisabled = agentStore.UpdateIfUnchanged(tenant, disabled, 0UL, CancellationToken.None)

            Assert.True(storedScheduled :? AgentUpdated)
            Assert.True(storedDisabled :? AgentUpdated)

            let! listed = agentStore.ListAgentsWithEnabledSchedules(tenant, CancellationToken.None)

            Assert.Equal(1, listed.Count)
        }

    [<Fact>]
    member this.``Custom tools version on upsert and preserve CreatedAt``() =
        task {
            let agent = this.SampleAgent()

            let! stored = agentStore.UpdateIfUnchanged(tenant, agent, 0UL, CancellationToken.None)

            let agentId = (stored :?> AgentUpdated).Agent.Id

            let! first = toolStore.UpsertCustomTool(tenant, agentId, this.SampleTool(agentId), CancellationToken.None)

            Assert.Equal(1UL, first.RowVersion)

            let edited = { first with Description = "edited" }

            let! second = toolStore.UpsertCustomTool(tenant, agentId, edited, CancellationToken.None)

            Assert.Equal(2UL, second.RowVersion)
            Assert.Equal(first.CreatedAt, second.CreatedAt)
        }

    [<Fact>]
    member this.``A custom tool on a missing agent throws``() =
        task {
            let missingAgent = AgentId.New()

            Assert.Throws<AgentNotFoundException>(fun () ->
                toolStore
                    .UpsertCustomTool(tenant, missingAgent, this.SampleTool(missingAgent), CancellationToken.None)
                    .GetAwaiter()
                    .GetResult()
                |> ignore)
            |> ignore
        }

    [<Fact>]
    member this.``A wrong-tenant agent resolves null and tools list empty``() =
        task {
            let agent = this.SampleAgent()

            let! stored = agentStore.UpdateIfUnchanged(tenant, agent, 0UL, CancellationToken.None)

            let agentId = (stored :?> AgentUpdated).Agent.Id

            let! wrongTenant = agentStore.GetAgent(this.OtherTenant, agentId, CancellationToken.None)

            Assert.Null(wrongTenant)

            let! wrongTenantTools = toolStore.ListCustomTools(this.OtherTenant, agentId, CancellationToken.None)

            Assert.Equal(0, wrongTenantTools.Count)
        }
