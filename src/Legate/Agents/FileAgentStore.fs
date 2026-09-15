// SPDX-License-Identifier: Apache-2.0
namespace Legate.Agents

open System
open System.Collections.Generic
open System.IO
open System.Threading
open System.Threading.Tasks
open Legate
open Microsoft.Extensions.DependencyInjection

// File-backed read-only IAgentStore for CLI hosts. Reads merge three
// layers with later definitions winning: the backing store registered
// through AgentsBuilder.UseStore, then `.agent/agents/*.md` files in
// sorted file order, then code-defined AgentsBuilder.Add entries in call
// order, all keyed by agent Name. Directory and code-defined layers apply
// to TenantId.Default only, so other tenants see the backing store alone
// (isolation holds even for single-tenant CLI hosts). Every read re-reads
// the directories from disk: no mtime cache, no watcher, so a disk change
// is visible on the next open. Every write throws
// ReadOnlyAgentStoreException. File agents keep stable ids for the store's
// lifetime through a name-to-id map: the map is identity, never a content
// cache, and contents are always re-read.

// ──────────────────────────────────────────────────────────────────────────
// The store

/// The file-backed read-only agent store. Internal: hosts reach it through
/// <c>AgentsBuilder.AddFromDirectory</c> and <c>AgentsBuilder.Add</c>, never
/// by construction.
type internal FileAgentStore
    (backing: IAgentStore | null, directories: IReadOnlyList<string>, codeDefined: IReadOnlyList<Agent>) =

    do
        ArgumentNullException.ThrowIfNull(directories)
        ArgumentNullException.ThrowIfNull(codeDefined)

    // Stable ids per agent name for the store's lifetime: identity only,
    // never file contents, which are re-read on every store read.
    let ids = Dictionary<string, AgentId>(StringComparer.Ordinal)

    /// Reads one directory's `*.md` files in sorted ordinal file order,
    /// with each file's last-write stamp. A missing directory reads as
    /// empty so hosts without `.agent` still run.
    /// <param name="directory">The directory to read.</param>
    /// <returns>The parsed definitions with their file stamps.</returns>
    let readDirectory (directory: string) : (AgentFileParser.AgentFileDefinition * DateTimeOffset) list =
        if Directory.Exists directory then
            Directory.GetFiles(directory, "*.md", SearchOption.TopDirectoryOnly)
            |> Array.sortWith (fun left right -> String.Compare(left, right, StringComparison.Ordinal))
            |> Array.map (fun file ->
                let definition = AgentFileParser.parseFile file
                let stamp = DateTimeOffset(File.GetLastWriteTimeUtc(file), TimeSpan.Zero)
                definition, stamp)
            |> Array.toList
        else
            []

    /// Builds the stored agent for one parsed definition, reusing the
    /// name's stable id across reloads. The definition's allowlist maps
    /// through SubAgents.toToolSelection: absent means the runtime default,
    /// present names travel verbatim (unknown names diagnose, never fail).
    /// <param name="tenant">The tenant the agent is served under.</param>
    /// <param name="definition">The parsed file definition.</param>
    /// <param name="stamp">The file's last-write stamp.</param>
    /// <returns>The stored agent.</returns>
    let toAgent (tenant: TenantId) (definition: AgentFileParser.AgentFileDefinition) (stamp: DateTimeOffset) : Agent =
        let id =
            match ids.TryGetValue definition.Name with
            | true, existing -> existing
            | false, _ ->
                let fresh = AgentId.New()
                ids[definition.Name] <- fresh
                fresh

        {
            Id = id
            Tenant = tenant
            Name = definition.Name
            Description = definition.Description
            Model = definition.Model
            SystemPrompt = definition.SystemPrompt
            EnvironmentVariables = null
            PermissionDefaults = null
            ToolSelection = SubAgents.toToolSelection definition.Tools
            PackageReference = null
            Enabled = definition.Enabled
            Schedule = null
            RowVersion = 0UL
            CreatedAt = stamp
            UpdatedAt = stamp
        }

    /// Loads the merged agent view for one tenant: backing rows, then
    /// directory files, then code-defined entries, later names winning.
    /// <param name="tenant">The tenant whose agents to load.</param>
    /// <param name="cancellationToken">Token that abandons the load.</param>
    /// <returns>The merged agents sorted by name.</returns>
    let loadMerged (tenant: TenantId) (cancellationToken: CancellationToken) : Task<IReadOnlyList<Agent>> =
        task {
            let merged = Dictionary<string, Agent>(StringComparer.Ordinal)

            let! stored =
                match backing with
                | null -> Task.FromResult(ResizeArray<Agent>() :> IReadOnlyList<Agent>)
                | store -> store.ListAgents(tenant, cancellationToken)

            for agent in stored do
                merged[agent.Name] <- agent

            if tenant.Equals TenantId.Default then
                for directory in directories do
                    for definition, stamp in readDirectory directory do
                        merged[definition.Name] <- toAgent tenant definition stamp

                for coded in codeDefined do
                    merged[coded.Name] <- { coded with Tenant = tenant }

            return merged.Values |> Seq.sortBy (fun agent -> agent.Name) |> Seq.toList :> IReadOnlyList<Agent>
        }

    /// Lists the load-time diagnostics for the merged directory definitions:
    /// one AgentInvalidEvent per missing description and per unknown-tools
    /// allowlist, over the later-definitions-win winners only. The agents
    /// themselves always stay listed: diagnostics explain, never exclude, so
    /// an invalid definition is never a silent drop and never fails the
    /// load. Missing names and bad YAML stay fatal in the parser and surface
    /// as throws from the reads, never as events here. Backing-store and
    /// code-defined entries are host-constructed, never parsed, so they
    /// carry no diagnostics.
    /// <param name="sessionId">The session the diagnostics run for.</param>
    /// <param name="turnId">The turn the diagnostics run inside.</param>
    /// <param name="timestamp">When the diagnostics ran: the stamp every event carries.</param>
    /// <returns>The diagnostic events for the merged definitions; empty when all parse clean.</returns>
    member _.ListDiagnostics
        (sessionId: SessionId, turnId: TurnId, timestamp: DateTimeOffset)
        : IReadOnlyList<SessionEvent> =
        let merged =
            Dictionary<string, AgentFileParser.AgentFileDefinition>(StringComparer.Ordinal)

        for directory in directories do
            for definition, _stamp in readDirectory directory do
                merged[definition.Name] <- definition

        merged.Values
        |> Seq.sortBy (fun definition -> definition.Name)
        |> Seq.collect (fun definition -> SubAgents.diagnoseDefinition definition sessionId turnId timestamp)
        |> Seq.toList
        :> IReadOnlyList<SessionEvent>

    interface IAgentStore with

        member _.GetAgent(tenant, agentId, cancellationToken) =
            task {
                let! agents = loadMerged tenant cancellationToken

                return agents |> Seq.tryFind (fun agent -> agent.Id.Equals agentId) |> Option.toObj
            }

        member _.ListAgents(tenant, cancellationToken) = loadMerged tenant cancellationToken

        member _.UpdateIfUnchanged(_, _, _, _) =
            Task.FromException<AgentUpdateOutcome>(
                ReadOnlyAgentStoreException(
                    "UpdateIfUnchanged",
                    "The file-based agent store is read-only: agent definitions come from the agent directory and code-defined entries, never from store writes."
                )
            )

        member _.DeleteAgent(_, _, _) =
            Task.FromException<bool>(
                ReadOnlyAgentStoreException(
                    "DeleteAgent",
                    "The file-based agent store is read-only: agent definitions come from the agent directory and code-defined entries, never from store writes."
                )
            )

        member _.ListAgentsWithEnabledSchedules(tenant, cancellationToken) =
            task {
                let! agents = loadMerged tenant cancellationToken

                return
                    agents
                    |> Seq.filter (fun agent ->
                        match agent.Schedule with
                        | null -> false
                        | schedule -> schedule.Enabled)
                    |> Seq.sortBy (fun agent -> agent.Name)
                    |> Seq.toList
                    :> IReadOnlyList<Agent>
            }

// ──────────────────────────────────────────────────────────────────────────
// Builder composition

/// Wires the builder's file layers into the container: when the host
/// registered at least one directory or code-defined agent, the current
/// IAgentStore registration (if any) becomes the backing layer of a
/// FileAgentStore. Internal: runs once from AddLegate after the host
/// configuration, so UseStore wins as backing whatever order the host
/// called it in.
module internal FileAgentStoreRegistration =

    /// Composes the file layers when the host registered any.
    /// <param name="agents">The configured agents sub-builder.</param>
    let compose (agents: AgentsBuilder) : unit =
        ArgumentNullException.ThrowIfNull(agents)

        if agents.HasFileLayers then
            let services = agents.Services

            let prior =
                services
                |> Seq.filter (fun descriptor -> descriptor.ServiceType = typeof<IAgentStore>)
                |> Seq.toArray

            for descriptor in prior do
                services.Remove(descriptor) |> ignore

            let last: obj | null =
                if prior.Length = 0 then
                    null
                else
                    box prior[prior.Length - 1]

            services.AddSingleton<IAgentStore>(
                Func<IServiceProvider, IAgentStore>(fun provider ->
                    let backing: IAgentStore | null =
                        match last with
                        | null -> null
                        | :? ServiceDescriptor as descriptor ->
                            match box descriptor.ImplementationInstance :?> (IAgentStore | null) with
                            | null ->
                                match
                                    box descriptor.ImplementationFactory :?> (Func<IServiceProvider, obj> | null)
                                with
                                | null ->
                                    match box descriptor.ImplementationType with
                                    | :? Type as implementationType ->
                                        box (ActivatorUtilities.CreateInstance(provider, implementationType))
                                        :?> (IAgentStore | null)
                                    | _ -> null
                                | factory -> box (factory.Invoke(provider)) :?> (IAgentStore | null)
                            | instance -> instance
                        | _ -> null

                    FileAgentStore(backing, agents.FileDirectories, agents.FileAgents) :> IAgentStore)
            )
            |> ignore
