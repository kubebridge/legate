// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Generic
open System.IO
open System.Text
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.AI

// Internal pure prompt composer (issue 66): assembles one turn system
// prompt from the package instructions (both AGENTS.md casings, lowercase
// wins), the agent's system prompt, the #67 skill directory block, and the
// host instruction files, in that fixed order with fixed separators and
// empty parts omitted. Consumes #67's directory shape as a caller-supplied
// string and #69's agent row as a caller-supplied prompt: this module never
// parses YAML, never executes skills, and never loads agent definitions.
// Package instructions are probed from the store per active version and
// snapshotted in the per-session cache; host files are read fresh every
// turn under the shared discovery byte bound. The read path takes no
// package lease: writers coordinate through IAgentPackageLeaseService but
// the store never checks leases, so composition stays a plain read.
module internal PromptComposition =

    /// Separator placed between consecutive non-empty prompt parts: two
    /// line feeds, a thematic break, two line feeds. One separator only,
    /// never doubled: empty parts are omitted before joining, so the order
    /// and the separators a test pins are exactly what the model sees.
    [<Literal>]
    let PartSeparator = "\n\n---\n\n"

    /// Resolves the package instructions from the two probed casings:
    /// the lowercase <c>agents.md</c> content wins when present, otherwise
    /// the canonical <c>AGENTS.md</c> content. Both paths stay
    /// ordinal/case-sensitive in <see cref="T:Legate.AgentPackagePaths" />:
    /// the composer reads them as distinct paths and prefers the lowercase
    /// text here. Null, empty, and whitespace-only inputs count as absent,
    /// so an empty file never contributes a part.
    /// <param name="canonical">The canonical AGENTS.md text, or null when absent.</param>
    /// <param name="lowercase">The lowercase agents.md text, or null when absent.</param>
    /// <returns>The lowercase text when present, else the canonical text, else null.</returns>
    let resolveInstructions (canonical: string | null) (lowercase: string | null) : string | null =
        if not (String.IsNullOrWhiteSpace lowercase) then
            lowercase
        elif not (String.IsNullOrWhiteSpace canonical) then
            canonical
        else
            null

    /// Assembles the turn system prompt from its four parts in fixed
    /// order: package instructions, agent system prompt, skills block,
    /// then each host file's text in the listed order, host files appended
    /// last. Null, empty, and whitespace-only parts are omitted, including
    /// individual blank host entries; a null host list reads as no host
    /// files. The surviving parts join with
    /// <see cref="M:Legate.PromptComposition.PartSeparator" />: no leading
    /// or trailing separator, and empty when every part is empty.
    /// <param name="packageInstructions">The resolved package instructions, or null when the package has none.</param>
    /// <param name="agentSystemPrompt">The agent's system prompt, or null when the agent carries none.</param>
    /// <param name="skillsBlock">The #67 available-skills block, or null when discovery ran with no skills.</param>
    /// <param name="hostFiles">The host file texts in SessionOptions order, or null when the session adds none.</param>
    /// <returns>The composed system prompt, or empty when every part is empty.</returns>
    let compose
        (packageInstructions: string | null)
        (agentSystemPrompt: string | null)
        (skillsBlock: string | null)
        (hostFiles: IReadOnlyList<string> | null)
        : string =
        let parts = ResizeArray<string>()

        let addPart (part: string | null) =
            match part with
            | null -> ()
            | text when String.IsNullOrWhiteSpace text -> ()
            | text -> parts.Add text

        addPart packageInstructions
        addPart agentSystemPrompt
        addPart skillsBlock

        match hostFiles with
        | null -> ()
        | files ->
            for text in files do
                addPart text

        String.Join(PartSeparator, parts)

    /// Drains one readable stream up to the shared discovery bound and
    /// disposes it, decoding as UTF-8. Maps every failure to absent: a
    /// null stream, a read error, or content past
    /// <see cref="F:Legate.SkillDiscovery.MaxSkillFileBytes" /> reads as
    /// null, so one oversized or unreadable casing never fails the turn
    /// and the surviving casing still wins.
    /// <param name="source">The stream to drain; always disposed unless null.</param>
    /// <param name="cancellationToken">Token that abandons the read.</param>
    /// <returns>The decoded text, or null when the stream is missing, unreadable, or over the bound.</returns>
    let private drainCapped (source: Stream | null) (cancellationToken: CancellationToken) : Task<string | null> =
        task {
            match source with
            | null -> return null
            | stream ->
                try
                    use _stream = stream
                    use memory = new MemoryStream()
                    let buffer = Array.zeroCreate<byte> 8192
                    let mutable total = 0
                    let mutable finished = false
                    let mutable tooLarge = false

                    while not finished do
                        let! read = stream.ReadAsync(buffer, 0, buffer.Length, cancellationToken)
                        cancellationToken.ThrowIfCancellationRequested()

                        if read = 0 then
                            finished <- true
                        elif total + read > SkillDiscovery.MaxSkillFileBytes then
                            tooLarge <- true
                            finished <- true
                        else
                            total <- total + read
                            memory.Write(buffer, 0, read)

                    if tooLarge then
                        return null
                    else
                        let text: string | null = Encoding.UTF8.GetString(memory.ToArray())
                        return text
                with
                | :? OperationCanceledException as canceled -> return! Task.FromException<string | null>(canceled)
                | _ -> return null
        }

    /// Probes one version's package instructions from the store: reads the
    /// canonical <c>AGENTS.md</c> and the lowercase <c>agents.md</c> as
    /// distinct ordinal paths and resolves with
    /// <see cref="M:Legate.PromptComposition.resolveInstructions" />, so
    /// lowercase wins when both exist. Either read missing, unreadable, or
    /// over the shared bound counts as absent for that casing. Takes no
    /// package lease: the store never checks leases.
    /// <param name="store">The package store to read from. Must not be null.</param>
    /// <param name="tenant">The tenant the agent belongs to.</param>
    /// <param name="agentId">The agent whose package to read from.</param>
    /// <param name="version">The active version to read from. Must not be null.</param>
    /// <param name="cancellationToken">Token that abandons the probe.</param>
    /// <returns>The resolved instructions, or null when the version carries neither casing.</returns>
    /// <exception cref="T:System.ArgumentNullException">The store or version is null.</exception>
    let readPackageInstructionsAsync
        (store: IAgentPackageStore)
        (tenant: TenantId)
        (agentId: AgentId)
        (version: string)
        (cancellationToken: CancellationToken)
        : Task<string | null> =
        ArgumentNullException.ThrowIfNull(store)

        if isNull (box version) then
            raise (ArgumentNullException(nameof version))

        task {
            let! canonical = store.ReadFile(tenant, agentId, version, "AGENTS.md", cancellationToken)
            let! canonicalText = drainCapped canonical cancellationToken
            let! lowercase = store.ReadFile(tenant, agentId, version, "agents.md", cancellationToken)
            let! lowercaseText = drainCapped lowercase cancellationToken
            return resolveInstructions canonicalText lowercaseText
        }

    /// Reads one host instruction file up to the shared discovery bound:
    /// missing, unreadable, blank, and over-bound files all read as null,
    /// so one bad path never fails the turn and never contributes a part.
    /// <param name="path">The host file path. Blank reads as null.</param>
    /// <param name="cancellationToken">Token that abandons the read.</param>
    /// <returns>The file text, or null when the file is skipped.</returns>
    let private readHostFile (path: string | null) (cancellationToken: CancellationToken) : Task<string | null> =
        task {
            match path with
            | null -> return null
            | blank when String.IsNullOrWhiteSpace blank -> return null
            | file ->
                try
                    use stream =
                        new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.Read, 8192, true)

                    return! drainCapped stream cancellationToken
                with
                | :? OperationCanceledException as canceled -> return! Task.FromException<string | null>(canceled)
                | _ -> return null
        }

    /// Reads the session's host instruction files in the listed order for
    /// one turn: a null list reads as no files, and each missing,
    /// unreadable, blank, or over-bound file is skipped, so the returned
    /// texts are exactly the surviving parts in SessionOptions order.
    /// <param name="paths">The host file paths, or null when the session adds none.</param>
    /// <param name="cancellationToken">Token that abandons the reads.</param>
    /// <returns>The surviving file texts, in the listed order.</returns>
    let readHostFilesAsync
        (paths: IReadOnlyList<string> | null)
        (cancellationToken: CancellationToken)
        : Task<IReadOnlyList<string>> =
        task {
            let texts = ResizeArray<string>()

            match paths with
            | null -> ()
            | listed ->
                for path in listed do
                    let! text = readHostFile path cancellationToken

                    match text with
                    | null -> ()
                    | value when String.IsNullOrWhiteSpace value -> ()
                    | value -> texts.Add value

            return texts :> IReadOnlyList<string>
        }

    /// Per-session cache of probed package instructions, keyed on
    /// tenant, agent, and the ordinal active-version string. One entry:
    /// a session converses with one agent, so the latest probe is the
    /// only one worth keeping. A null active version (no version ever
    /// uploaded, or the active version deleted) always misses and is
    /// never stored, so an active-version delete invalidates by
    /// construction. Thread-safe through a lock; turns run sequentially
    /// but resumes may interleave.
    [<Sealed>]
    type SessionPromptCache() =

        let gate = obj ()
        let mutable entry: (string * string * string * (string | null)) option = None

        /// Tries the cached instructions for one tenant, agent, and active
        /// version, compared ordinally with no trimming or normalisation:
        /// versions are storage keys, mirroring
        /// <see cref="T:Legate.PackageVersions" />. A null version always
        /// misses.
        /// <param name="tenant">The tenant the agent belongs to.</param>
        /// <param name="agentId">The agent whose instructions were cached.</param>
        /// <param name="activeVersion">The active version to hit on, or null for a certain miss.</param>
        /// <returns>Some instructions (possibly null) on a hit; None on any miss.</returns>
        member _.TryGet(tenant: TenantId, agentId: AgentId, activeVersion: string | null) : (string | null) option =
            lock gate (fun () ->
                match activeVersion, entry with
                | null, _ -> None
                | _, None -> None
                | version, Some(tenantText, agentText, versionText, instructions) ->
                    if
                        String.Equals(tenantText, tenant.Value, StringComparison.Ordinal)
                        && String.Equals(agentText, agentId.Value, StringComparison.Ordinal)
                        && String.Equals(versionText, version, StringComparison.Ordinal)
                    then
                        Some instructions
                    else
                        None)

        /// Stores probed instructions under one non-null version,
        /// replacing whatever the session cached before.
        /// <param name="tenant">The tenant the agent belongs to.</param>
        /// <param name="agentId">The agent whose instructions were probed.</param>
        /// <param name="activeVersion">The version the probe ran against. Must not be null.</param>
        /// <param name="instructions">The probed instructions, possibly null when the version carries neither casing.</param>
        /// <exception cref="T:System.ArgumentNullException">The version is null.</exception>
        member _.Store(tenant: TenantId, agentId: AgentId, activeVersion: string, instructions: string | null) : unit =
            if isNull (box activeVersion) then
                raise (ArgumentNullException(nameof activeVersion))

            lock gate (fun () -> entry <- Some(tenant.Value, agentId.Value, activeVersion, instructions))

    /// What one turn-prompt composition needs. The agent prompt and the
    /// skills block arrive caller-supplied: the agent row comes from #69's
    /// agent store and the block from #67's discovery pass, so this
    /// request carries their outputs, never their mechanisms.
    type TurnPromptRequest =
        {
            /// The package store instructions probe against. Must not be null.
            PackageStore: IAgentPackageStore
            /// The tenant the agent belongs to.
            Tenant: TenantId
            /// The agent the turn runs for.
            AgentId: AgentId
            /// The agent's system prompt (loaded via the agent store), or
            /// null when the agent carries none.
            AgentSystemPrompt: string | null
            /// The #67 available-skills block for the active version, or
            /// null when discovery ran with no skills.
            SkillsBlock: string | null
            /// The session's host instruction file paths
            /// (<see cref="P:Legate.SessionOptions.HostInstructionFiles" />),
            /// or null when the session adds none.
            HostFilePaths: IReadOnlyList<string> | null
            /// The session's instruction cache, or null to probe the store
            /// on every turn.
            Cache: SessionPromptCache | null
        }

    /// Resolves one turn's package instructions through the session
    /// cache: a version hit returns the snapshot, a miss probes the store
    /// and stores the probe, and a null version (no package, or the
    /// active version deleted) returns null without touching the cache.
    /// <param name="request">The stores, identities, and cache the turn composes with.</param>
    /// <param name="version">The active version, or null when there is none.</param>
    /// <param name="cancellationToken">Token that abandons the resolution.</param>
    /// <returns>The resolved instructions, or null when the version carries neither casing.</returns>
    let private resolveTurnInstructions
        (request: TurnPromptRequest)
        (version: string | null)
        (cancellationToken: CancellationToken)
        : Task<string | null> =
        task {
            match version with
            | null -> return null
            | active ->
                match request.Cache with
                | null ->
                    return!
                        readPackageInstructionsAsync
                            request.PackageStore
                            request.Tenant
                            request.AgentId
                            active
                            cancellationToken
                | cache ->
                    match cache.TryGet(request.Tenant, request.AgentId, active) with
                    | Some cached -> return cached
                    | None ->
                        let! probed =
                            readPackageInstructionsAsync
                                request.PackageStore
                                request.Tenant
                                request.AgentId
                                active
                                cancellationToken

                        cache.Store(request.Tenant, request.AgentId, active, probed)
                        return probed
        }

    /// Composes one turn's system prompt at the session/turn package-load
    /// step: reads the active version through
    /// <see cref="M:Legate.IAgentPackageStore.GetPackageInfo*" />,
    /// resolves instructions from the cache on a version hit or probes
    /// the store on a miss (storing the probe), reads the host files
    /// fresh, and assembles with
    /// <see cref="M:Legate.PromptComposition.compose" />. No version (no
    /// package ever uploaded, or the active version deleted) means no
    /// instructions and never touches the cache. Takes no package lease.
    /// <param name="request">The stores, identities, caller-supplied parts, and cache the turn composes with.</param>
    /// <param name="cancellationToken">Token that abandons the composition.</param>
    /// <returns>The composed system prompt, possibly empty when every part is empty.</returns>
    /// <exception cref="T:System.ArgumentNullException">The package store is null.</exception>
    let composeTurnSystemPromptAsync
        (request: TurnPromptRequest)
        (cancellationToken: CancellationToken)
        : Task<string> =
        ArgumentNullException.ThrowIfNull(request)
        ArgumentNullException.ThrowIfNull(request.PackageStore)

        task {
            let! info = request.PackageStore.GetPackageInfo(request.Tenant, request.AgentId, cancellationToken)

            let version: string | null =
                match info with
                | null -> null
                | package -> package.ActiveVersion

            let! instructions = resolveTurnInstructions request version cancellationToken
            let! hostTexts = readHostFilesAsync request.HostFilePaths cancellationToken

            return compose instructions request.AgentSystemPrompt request.SkillsBlock hostTexts
        }

    /// Resolves the turn's composed system prompt for one inbox entry:
    /// the hook the turn runners consult at the package-load step. A
    /// null resolution means the turn runs with no system message.
    type GetTurnSystemPrompt = InboxEntry -> CancellationToken -> Task<string | null>

    /// Prepends the composed system prompt to a fresh turn history as the
    /// leading <see cref="F:Microsoft.Extensions.AI.ChatRole.System" />
    /// message: the one mutation the turn wiring applies before the user
    /// message. A null, empty, or whitespace-only prompt is a no-op, so a
    /// turn with nothing composed keeps today's user-only history shape.
    /// <param name="history">The fresh turn history, mutated in place. Must not be null.</param>
    /// <param name="systemPrompt">The composed system prompt, or null for no system message.</param>
    /// <exception cref="T:System.ArgumentNullException">The history is null.</exception>
    let prependSystemPrompt (history: IList<ChatMessage>) (systemPrompt: string | null) : unit =
        ArgumentNullException.ThrowIfNull(history)

        if not (String.IsNullOrWhiteSpace systemPrompt) then
            history.Insert(0, ChatMessage(ChatRole.System, systemPrompt))
