// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Generic
open System.Text.Json.Serialization
open System.Threading
open System.Threading.Tasks

// Agent store contracts. IAgentStore is the durable store contract for
// agent definitions and IAgentCustomToolStore for the custom HTTP tools
// enabled per agent; the Postgres, SQLite, in-memory, and file-based
// implementations implement them. Every method takes a TenantId; the
// isolation is enforced in the stores, not only in the host. The agent
// upsert is checked: UpdateIfUnchanged branches on a result object because
// a row-version conflict is an expected branch of a concurrently edited
// definition, never a failure. The custom-tool upsert is a plain upsert,
// the What contrasts it with the agents' checked path. Both contracts are
// safe to implement read-only: a read-only implementation returns data and
// throws ReadOnlyAgentStoreException on every write. Schedules ride on the
// agent definition; their cron and time-zone semantics stay with the
// dispatcher epic, and custom-tool name sanitisation and collisions stay
// with the invocation epic.

// ───────────────────────────────────────────────────────────────────────────
// Agent upsert outcomes

/// What the store decided when
/// <see cref="M:Legate.IAgentStore.UpdateIfUnchanged*" /> landed: the
/// caller must branch on the outcome. A result object, never an exception:
/// a row-version conflict is an expected branch of a concurrently edited
/// definition, mirroring the lease-state precedent. Serialises
/// polymorphically: every concrete outcome carries a stable <c>$type</c>
/// discriminator on the wire, mirroring <see cref="T:Legate.TurnSettlement" />.
[<AbstractClass>]
[<JsonPolymorphic(TypeDiscriminatorPropertyName = "$type")>]
[<JsonDerivedType(typeof<AgentUpdated>, "agentUpdated")>]
[<JsonDerivedType(typeof<AgentUpdateConflict>, "agentUpdateConflict")>]
type AgentUpdateOutcome() = class end

/// The upsert was applied: expected 0 inserted a new agent, and a matching
/// row version updated the existing one with the version incremented and
/// <see cref="T:Legate.Agent" />.UpdatedAt stamped.
/// <param name="agent">The stored agent after the upsert.</param>
and [<Sealed>] AgentUpdated(agent: Agent) =
    inherit AgentUpdateOutcome()

    /// The stored agent after the upsert.
    member _.Agent = agent

/// The row version did not match what the caller read: another writer
/// changed the agent in between. Nothing was applied. The current row
/// travels back so the caller can re-read and retry against it; it is null
/// when the row vanished between the read and the write.
/// <param name="agent">The current stored agent, or null when the row no longer exists.</param>
and [<Sealed>] AgentUpdateConflict(agent: Agent | null) =
    inherit AgentUpdateOutcome()

    /// The current stored agent to retry against, or null when the row
    /// vanished.
    member _.Agent: Agent | null = agent

// ───────────────────────────────────────────────────────────────────────────
// The store contracts

/// The durable store contract for agent definitions: get by id, list by
/// tenant, the checked upsert, delete, and the schedule query the
/// dispatcher polls. Every method takes the tenant the data belongs to and
/// must not see or touch another tenant's rows (isolation is enforced
/// here, not only in the host).
///
/// <para>Lists are unpaged: agents are few and hosts load all of them per
/// activation, so paging would add continuations nothing consumes.</para>
///
/// <para>The upsert is checked: <see cref="M:Legate.IAgentStore.UpdateIfUnchanged*" />
/// applies when the stored row version matches the expected one and
/// returns the conflict outcome with the current row otherwise. Expected 0
/// inserts when the agent is absent; a missing row with a nonzero expected
/// version is a control-plane precondition and throws
/// <see cref="T:Legate.AgentNotFoundException" />.</para>
///
/// <para>The contract is safe to implement read-only: a read-only
/// implementation serves every read and throws
/// <see cref="T:Legate.ReadOnlyAgentStoreException" /> from every write
/// (<see cref="M:Legate.IAgentStore.UpdateIfUnchanged*" /> and
/// <see cref="M:Legate.IAgentStore.DeleteAgent*" />).</para>
type IAgentStore =

    /// Reads one agent. Returns null when the id does not exist in the
    /// tenant, mirroring <see cref="M:Legate.ISessionStore.GetSession*" />.
    /// <param name="tenant">The tenant the agent belongs to.</param>
    /// <param name="agentId">The agent to read.</param>
    /// <param name="cancellationToken">Token that abandons the read.</param>
    /// <returns>The agent, or null when absent.</returns>
    abstract GetAgent: tenant: TenantId * agentId: AgentId * cancellationToken: CancellationToken -> Task<Agent | null>

    /// Lists the tenant's agents, including disabled ones, unpaged: agents
    /// are few and hosts load all of them per activation.
    /// <param name="tenant">The tenant whose agents to list.</param>
    /// <param name="cancellationToken">Token that abandons the list.</param>
    /// <returns>The tenant's agents; empty when it has none.</returns>
    abstract ListAgents: tenant: TenantId * cancellationToken: CancellationToken -> Task<IReadOnlyList<Agent>>

    /// Upserts the agent under optimistic concurrency. When the agent is
    /// absent, expected version 0 inserts it; a nonzero expected version on
    /// a missing row throws
    /// <see cref="T:Legate.AgentNotFoundException" />. When the stored row
    /// version matches the expected one, the store applies the agent with
    /// the version incremented and UpdatedAt stamped and returns the
    /// updated outcome. When the versions do not match, nothing is applied
    /// and the conflict outcome carries the current row to retry against.
    /// A conflict is an expected branch of a concurrently edited
    /// definition, never an exception.
    /// <param name="tenant">The tenant the agent belongs to.</param>
    /// <param name="agent">The agent to upsert. Must not be null.</param>
    /// <param name="expectedRowVersion">The row version the caller read: 0 to insert, otherwise the version the update is checked against.</param>
    /// <param name="cancellationToken">Token that abandons the upsert.</param>
    /// <returns>The applied outcome with the stored agent, or the conflict outcome with the current row.</returns>
    /// <exception cref="T:System.ArgumentNullException">The agent is null.</exception>
    /// <exception cref="T:Legate.AgentNotFoundException">The agent does not exist in this tenant and the expected version is not 0.</exception>
    /// <exception cref="T:Legate.ReadOnlyAgentStoreException">The store is read-only.</exception>
    abstract UpdateIfUnchanged:
        tenant: TenantId * agent: Agent * expectedRowVersion: uint64 * cancellationToken: CancellationToken ->
            Task<AgentUpdateOutcome>

    /// Deletes the agent. Idempotent: deleting an absent agent is a no-op.
    /// <param name="tenant">The tenant the agent belongs to.</param>
    /// <param name="agentId">The agent to delete.</param>
    /// <param name="cancellationToken">Token that abandons the delete.</param>
    /// <returns>true when the agent existed and was deleted; false when it was already absent.</returns>
    /// <exception cref="T:Legate.ReadOnlyAgentStoreException">The store is read-only.</exception>
    abstract DeleteAgent: tenant: TenantId * agentId: AgentId * cancellationToken: CancellationToken -> Task<bool>

    /// Lists the tenant's agents whose
    /// <see cref="T:Legate.Agent" />.Schedule is non-null and enabled: the
    /// query the dispatcher polls for scheduled prompts. The schedule
    /// travels with the agent because saving it is the store's one write
    /// path; cron and time-zone semantics stay with the dispatcher epic.
    /// <param name="tenant">The tenant whose scheduled agents to list.</param>
    /// <param name="cancellationToken">Token that abandons the query.</param>
    /// <returns>The agents with an enabled schedule; empty when it has none.</returns>
    abstract ListAgentsWithEnabledSchedules:
        tenant: TenantId * cancellationToken: CancellationToken -> Task<IReadOnlyList<Agent>>

/// The durable store contract for the custom HTTP tools enabled per agent.
/// Tools are keyed by <see cref="T:Legate.AgentCustomTool" />.Name within
/// one agent; the upsert validates the name against
/// <see cref="P:Legate.ToolNameRules.Pattern" /> and leaves name
/// sanitisation and cross-source collisions to the invocation epic. Every
/// method takes the tenant the data belongs to and must not see or touch
/// another tenant's rows (isolation is enforced here, not only in the
/// host).
///
/// <para>The upsert is plain, in contrast with
/// <see cref="M:Legate.IAgentStore.UpdateIfUnchanged*" />: last write wins,
/// the store stamps <see cref="T:Legate.AgentCustomTool" />.RowVersion
/// (previous plus one, or one on insert) and preserves CreatedAt, and a
/// missing agent is a host bug that throws
/// <see cref="T:Legate.AgentNotFoundException" />.</para>
///
/// <para>The contract is safe to implement read-only: a read-only
/// implementation serves every read and throws
/// <see cref="T:Legate.ReadOnlyAgentStoreException" /> from every write
/// (<see cref="M:Legate.IAgentCustomToolStore.UpsertCustomTool*" /> and
/// <see cref="M:Legate.IAgentCustomToolStore.DeleteCustomTool*" />).</para>
type IAgentCustomToolStore =

    /// Lists the agent's enabled custom tools, the shape the runtime loads
    /// per invocation. Disabled tools stay stored and unlisted.
    /// <param name="tenant">The tenant the agent belongs to.</param>
    /// <param name="agentId">The agent whose tools to list.</param>
    /// <param name="cancellationToken">Token that abandons the list.</param>
    /// <returns>The enabled tools; empty when the agent has none.</returns>
    abstract ListCustomTools:
        tenant: TenantId * agentId: AgentId * cancellationToken: CancellationToken ->
            Task<IReadOnlyList<AgentCustomTool>>

    /// Reads one custom tool by name, in any state (enabled or disabled).
    /// <param name="tenant">The tenant the agent belongs to.</param>
    /// <param name="agentId">The agent the tool belongs to.</param>
    /// <param name="name">The model-facing tool name to read.</param>
    /// <param name="cancellationToken">Token that abandons the read.</param>
    /// <returns>The tool, or null when the name does not exist for the agent in the tenant.</returns>
    abstract GetCustomTool:
        tenant: TenantId * agentId: AgentId * name: string * cancellationToken: CancellationToken ->
            Task<AgentCustomTool | null>

    /// Upserts the custom tool under the agent. Plain upsert: last write
    /// wins. The store stamps RowVersion (previous plus one, or one on
    /// insert) and preserves CreatedAt, and validates the tool name
    /// against <see cref="P:Legate.ToolNameRules.Pattern" />. A missing
    /// agent is a host bug and throws
    /// <see cref="T:Legate.AgentNotFoundException" />. The tool record
    /// carries the signing secret and must never be logged by
    /// implementations.
    /// <param name="tenant">The tenant the agent belongs to.</param>
    /// <param name="agentId">The agent the tool belongs to.</param>
    /// <param name="customTool">The tool to upsert. Must not be null.</param>
    /// <param name="cancellationToken">Token that abandons the upsert.</param>
    /// <returns>The stored tool, version and timestamps stamped by the store.</returns>
    /// <exception cref="T:System.ArgumentNullException">The tool is null.</exception>
    /// <exception cref="T:System.ArgumentException">The tool name fails <see cref="P:Legate.ToolNameRules.Pattern" />.</exception>
    /// <exception cref="T:Legate.AgentNotFoundException">The agent does not exist in this tenant.</exception>
    /// <exception cref="T:Legate.ReadOnlyAgentStoreException">The store is read-only.</exception>
    abstract UpsertCustomTool:
        tenant: TenantId * agentId: AgentId * customTool: AgentCustomTool * cancellationToken: CancellationToken ->
            Task<AgentCustomTool>

    /// Deletes one custom tool by name. Idempotent: deleting an absent
    /// name is a no-op.
    /// <param name="tenant">The tenant the agent belongs to.</param>
    /// <param name="agentId">The agent the tool belongs to.</param>
    /// <param name="name">The model-facing tool name to delete.</param>
    /// <param name="cancellationToken">Token that abandons the delete.</param>
    /// <returns>true when the tool existed and was deleted; false when it was already absent.</returns>
    /// <exception cref="T:Legate.ReadOnlyAgentStoreException">The store is read-only.</exception>
    abstract DeleteCustomTool:
        tenant: TenantId * agentId: AgentId * name: string * cancellationToken: CancellationToken -> Task<bool>
