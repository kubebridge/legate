// SPDX-License-Identifier: Apache-2.0
module Dot.ReplEngine

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open Legate

// REPL engine over the session client facade for the dot host: one
// continuous logical attachment observation per selected session plus
// receipt-bound authoritative observation, permission/question console
// replies, and the /new, /sessions, /resume, /model, /steer, /follow,
// /abort, /compact, /tree, /fork, /clone, /session, /export,
// /<template>, /help, and /quit commands over the SQLite session store.
// Modeled on
// samples/LegateCli/CliEngine.fs: each event renders as a stable single
// line, and permission/question suspensions are answered inline through
// ReplyAsync. Divergences from CliEngine, all required by the SQLite host
// or this issue: the engine provisions one enabled scripted agent per
// opened session (the SQLite authority check rejects turns for missing
// agents, while CliEngine's InMemory catalog authorizes); opened sessions
// carry a title but no timeout, so the operation-wait bound reports a
// deadline without killing the turn; there is no /agent stub; /model
// switches the session through SetAgentAsync against a model-carrying
// agent row, so the journal transcript and workspace binding survive;
// /steer interrupts (Interrupt), /follow folds in (Inject) and plain input
// queues (Queue); /tree pages ReadEventsAsync and /fork branches the
// prefix through ForkAsync (plus a free /clone at the tail); unknown
// commands reprint the command usage; and an operation wait that outruns
// its bound reports DEADLINE naming the accepted operation while the turn
// keeps running and the attachment keeps observing. Selecting, opening,
// or attaching a session establishes the attachment (including to
// already-running or suspended work with no prompt); individual waits,
// deadline expiry, and terminal results never end it; transport recovery
// replaces the subscription with no overlapping consumers and no loss of
// session identity. Replies bind to the request's original session plus
// continued validity and turn association; stale, resolved, switched, or
// replayed requests are rejected visibly with no cross-session send.
// Transport-agnostic: the host wires the chat client, tools, and policy.
// Reads and writes through the given reader/writer so scripted transports
// drive it without a console.

/// The command usage reprinted on startup and for unknown commands.
let private commandsUsage =
    "Commands: /new [title], /sessions, /resume <id-or-index>, /model [provider[/model]], /agents, /steer <text>, /follow <text>, /abort, /compact, /tree, /fork <sequence>, /clone, /session, /export <file>, /<template>, /help, /quit."

/// How client waiting, queued time, turn execution, and suspension
/// relate: the completion-wait budget starts on confirmed input
/// acceptance and includes time queued behind earlier work; queued-time
/// expiry cancels nothing and charges nothing against the runtime turn
/// execution budget; suspension waits for host input through ReplyAsync
/// and never resolves a completion wait; execution-budget exhaustion is
/// reported from the authoritative runtime outcome, never from a client
/// deadline. Stopping a wait, detaching, or cancelling observation never
/// aborts; only /abort (explicit execution control) affects execution.
let waitSemanticsHelp =
    "Waiting: PromptAsync acceptance returns an AcceptedOperation receipt (Queue/Inject/Interrupt/Reply distinguishable); "
    + "WaitForOperationAsync bounds only client completion waiting from acceptance (queued time included; queued-time expiry cancels nothing and charges nothing against the runtime turn execution budget); "
    + "GetOperationResultAsync reports Pending/Terminal/Unknown/Unavailable explicitly (unknown stays unknown, never running/success/idle); "
    + "suspension waits for host input through ReplyAsync and never resolves a completion wait; execution-budget exhaustion is reported from the authoritative runtime outcome, never from a client deadline; "
    + "a wait that outruns its bound prints DEADLINE naming the operation and stating only that client waiting stopped (no abort, no result consumption, no disconnect); "
    + "stopping a wait, detaching, or cancelling observation never aborts execution; only explicit execution control affects execution; "
    + "steer (/steer Interrupt), follow (/follow Inject), abort (/abort), and replies stay usable after deadlines and while input queues."

/// When an old persistence, wait, or event contract is no longer
/// supported: fail clearly and require an explicit host/user clean start
/// (new database path), never fabricate resumed state, historical
/// results, or conversation, and never rewrite landed migrations.
let cleanStartHelp =
    "Clean start: unsupported old persistence/wait/event contracts fail with UNSUPPORTED-CONTRACT and require an explicit clean start (a fresh database path); "
    + "Dot never invents history, terminal outcomes, or request validity to keep an old integration appearing functional."

// ──────────────────────────────────────────────────────────────────────────
// Provider and model selection (issue 307)

/// One provider dot can converse through: its id, its default model text,
/// and the env var enabling it (null for the scripted transport).
type ProviderOption =
    {
        /// The provider id, matching the model reference segment.
        Id: string
        /// The default model text: bare or full.
        DefaultModel: string
        /// The env var carrying the key, or null for scripted.
        EnvVar: string | null
    }

/// The provider ids in default priority order when several keys are set
/// and --provider is absent.
let defaultProviderOrder =
    [
        "anthropic"
        "openai"
        "google"
        "ollamacloud"
    ]

/// The env var carrying the provider key, or null for unknown ids.
let private providerEnvVar (id: string) : string | null =
    match id.ToLowerInvariant() with
    | "anthropic" -> "ANTHROPIC_API_KEY"
    | "openai" -> "OPENAI_API_KEY"
    | "google" -> "GOOGLE_API_KEY"
    | "ollamacloud" -> "OLLAMA_API_KEY"
    | _ -> null

/// Snapshots the registered providers for selection and /model listing.
/// <param name="providers">The registered providers. Must not be null.</param>
/// <returns>One option per registered provider.</returns>
let describeProviders (providers: ILlmProvider seq) : ProviderOption list =
    ArgumentNullException.ThrowIfNull(providers)

    providers
    |> Seq.filter (fun candidate -> not (isNull (box candidate)))
    |> Seq.map (fun candidate ->
        {
            Id = candidate.Id
            DefaultModel = candidate.DefaultModel
            EnvVar = providerEnvVar candidate.Id
        })
    |> List.ofSeq

/// Names the registered provider ids for error messages.
/// <param name="options">The registered provider options.</param>
/// <returns>The comma-joined ids.</returns>
let private knownIds (options: ProviderOption list) : string =
    options |> List.map (fun option -> option.Id) |> String.concat ", "

/// Finds a registered provider by id, case-insensitively.
/// <param name="options">The registered provider options.</param>
/// <param name="id">The id to find.</param>
/// <returns>The matching option, or None.</returns>
let private findOption (options: ProviderOption list) (id: string) : ProviderOption option =
    options
    |> List.tryFind (fun option -> String.Equals(option.Id, id, StringComparison.OrdinalIgnoreCase))

/// Raises the unknown-provider error naming the known ids, plus the env
/// var when the asked id is a live provider missing its key.
let private raiseUnknown (asked: string) (options: ProviderOption list) : 'T =
    let trimmed = asked.Trim()

    match providerEnvVar trimmed with
    | null -> raise (InvalidOperationException($"Unknown provider '{trimmed}' (known: {knownIds options})."))
    | env ->
        raise (
            InvalidOperationException(
                $"Unknown provider '{trimmed}' (known: {knownIds options}): set {env} to enable it."
            )
        )

/// Picks the provider when --provider is absent: the single registered
/// one, else the default order, else the first registered.
let private pickDefault (options: ProviderOption list) : ProviderOption =
    match options with
    | [] ->
        raise (
            InvalidOperationException(
                "No provider is registered (known: anthropic, openai, google, ollamacloud). Set ANTHROPIC_API_KEY, OPENAI_API_KEY, GOOGLE_API_KEY, or OLLAMA_API_KEY."
            )
        )
    | [ single ] -> single
    | _ ->
        defaultProviderOrder
        |> List.tryPick (findOption options)
        |> Option.defaultValue options.Head

/// Renders the provider default as a full reference: the configured text
/// when it already carries a slash, else id plus the bare default.
/// <param name="option">The provider option.</param>
/// <returns>The full reference text.</returns>
let private defaultReferenceText (option: ProviderOption) : string =
    if String.IsNullOrWhiteSpace option.DefaultModel then
        raise (
            InvalidOperationException($"Provider '{option.Id}' has no default model: pass --model <provider/model>.")
        )
    elif option.DefaultModel.Contains("/") then
        option.DefaultModel.Trim()
    else
        $"{option.Id}/{option.DefaultModel.Trim()}"

/// Resolves the model reference from the --provider/--model flags (the
/// /model argument maps onto them): --provider picks a registered id,
/// --model overrides the model, and a lone --model naming a registered
/// provider selects it. Unknown providers and unparsable references fail
/// naming the known ids; a reference naming an unregistered live provider
/// names its env var.
/// <param name="options">The registered provider options. Must not be empty.</param>
/// <param name="providerFlag">The --provider value, or null for automatic.</param>
/// <param name="modelFlag">The --model value, or null for the provider default.</param>
/// <returns>The resolved model reference.</returns>
let selectReference
    (options: ProviderOption list)
    (providerFlag: string | null)
    (modelFlag: string | null)
    : ModelReference =
    if options.IsEmpty then
        raise (
            InvalidOperationException(
                "No provider is registered (known: anthropic, openai, google, ollamacloud). Set ANTHROPIC_API_KEY, OPENAI_API_KEY, GOOGLE_API_KEY, or OLLAMA_API_KEY."
            )
        )

    let providerText =
        match providerFlag with
        | null -> null
        | text when String.IsNullOrWhiteSpace text -> null
        | text -> text.Trim()

    let modelText =
        match modelFlag with
        | null -> null
        | text when String.IsNullOrWhiteSpace text -> null
        | text -> text.Trim()

    let explicit =
        match providerText with
        | null -> None
        | asked -> findOption options asked

    match explicit with
    | Some _ -> ()
    | None ->
        match providerText with
        | null -> ()
        | asked -> raiseUnknown asked options

    let mutable parsed = Unchecked.defaultof<ModelReference>

    let modelProvider =
        match modelText with
        | null -> null
        | text when ModelReference.TryParse(text, &parsed) -> parsed.Provider
        | _ -> null

    let selected =
        match explicit with
        | Some option -> option
        | None ->
            let fromModel =
                match modelProvider with
                | null -> None
                | id -> findOption options id

            match fromModel with
            | Some option -> option
            | None -> pickDefault options

    match modelText with
    | null -> ModelReference.Parse(defaultReferenceText selected)
    | text ->
        if not (ModelReference.TryParse(text, &parsed)) then
            raise (
                ArgumentException(
                    $"The model '{text}' is not a <provider/model> reference (known providers: {knownIds options}).",
                    "model"
                )
            )

        if String.Equals(parsed.Provider, selected.Id, StringComparison.OrdinalIgnoreCase) then
            parsed
        else
            match findOption options parsed.Provider with
            | Some other ->
                raise (
                    InvalidOperationException(
                        $"The model '{text}' names provider '{parsed.Provider}', not the selected '{selected.Id}': pass --provider {other.Id} to use it."
                    )
                )
            | None -> raiseUnknown parsed.Provider options

/// One REPL session: its id, title, and event cursor.
type private ReplSession =
    {
        Id: SessionId
        Title: string
        mutable Cursor: int64
    }

/// One queued turn: the session it runs in, the accepted-operation
/// receipt captured on confirmed PromptAsync acceptance (Queue, Inject,
/// or Interrupt distinguishable by receipt kind), the receipt-bound
/// WaitForOperationAsync waiter started at acceptance (bound includes
/// queued time), and the waiter CTS the drain disposes. The streamed
/// prefix accumulates this turn's TextDelta fragments while the
/// attachment observes (claimed by the first TurnStarted the attachment
/// sees, so queued turns never share a prefix); the settled prefix
/// snapshots that text when the turn's terminal event lands, so
/// settlement rendering stays scoped to this turn even when the next
/// turn already streams. Acceptance failures never create a turn: they
/// report not-accepted and cancel nothing.
type private PendingTurn =
    {
        Session: ReplSession
        Receipt: AcceptedOperation
        AcceptedAt: DateTimeOffset
        WaitTask: Task<OperationResult>
        WaitCts: CancellationTokenSource
        mutable TurnKey: string
        mutable StreamedPrefix: string
        mutable SettledPrefix: string
    }

/// One session-bound pending host-input request: the original session
/// identity plus the request and turn association needed to validate a
/// reply at submit. Switching sessions, resolving or aborting the turn,
/// disconnecting, or invalidating the request rejects stale input
/// visibly with no work and no disclosure; replayed already-resolved
/// requests never become actionable again.
type private PendingRequest =
    {
        SessionId: SessionId
        RequestId: string
        TurnId: TurnId
        IsPermission: bool
    }

/// Ensures the model-carrying agent exists in the agent store: the
/// runtime's authority check rejects turns for missing agents. One enabled
/// row per provider/model, shared across sessions: the first session
/// inserts it (expected 0) and later sessions reuse it, so /model switches
/// and restarts converge instead of multiplying rows. The transcript lives
/// on the session row and the workspace binds from it, so both survive a
/// SetAgentAsync rebind. The ensure also uploads dot's sample review skill
/// package idempotently and records its version on the agent row's
/// PackageReference (opaque host bookkeeping the runtime never reads).
/// Reused by the -p one-shot print path, which opens the same agent row
/// without entering the REPL.
/// <param name="agents">The agent store.</param>
/// <param name="packages">The package store the sample skill uploads to.</param>
/// <param name="reference">The model the agent carries.</param>
/// <param name="cancellationToken">Abandons the upsert.</param>
/// <returns>The agent id conversing under the model.</returns>
let ensureModelAgentAsync
    (agents: IAgentStore)
    (packages: IAgentPackageStore)
    (reference: ModelReference)
    (cancellationToken: CancellationToken)
    : Task<AgentId> =
    task {
        let isMatch (agent: Agent) : bool =
            not (isNull (box agent))
            && agent.Enabled
            && agent.Name.StartsWith("dot ", StringComparison.Ordinal)
            && agent.Model.Equals(reference)

        let findMatch (listed: IReadOnlyList<Agent>) : Agent option =
            if isNull (box listed) then
                None
            else
                listed |> Seq.tryFind isMatch

        let findById (listed: IReadOnlyList<Agent>) (id: AgentId) : Agent option =
            if isNull (box listed) then
                None
            else
                listed
                |> Seq.tryFind (fun agent -> not (isNull (box agent)) && agent.Id.Equals(id))

        // Records the sample package version on the agent row when it
        // differs: advisory host bookkeeping, so a conflict or an
        // unexpected outcome keeps the ensured id instead of failing.
        let stampPackageReferenceAsync (agentId: AgentId) : Task =
            task {
                let! listed = agents.ListAgents(TenantId.Default, cancellationToken)

                match findById listed agentId with
                | None -> ()
                | Some found when
                    String.Equals(found.PackageReference, DotSkills.sampleVersion, StringComparison.Ordinal)
                    ->
                    ()
                | Some found ->
                    let stamped: Agent =
                        { found with
                            PackageReference = DotSkills.sampleVersion
                        }

                    let! _ = agents.UpdateIfUnchanged(TenantId.Default, stamped, found.RowVersion, cancellationToken)
                    ()
            }

        let! agentId =
            task {
                let! listed = agents.ListAgents(TenantId.Default, cancellationToken)

                match findMatch listed with
                | Some agent -> return agent.Id
                | None ->
                    let now = DateTimeOffset.UtcNow

                    let agent: Agent =
                        {
                            Id = AgentId.New()
                            Tenant = TenantId.Default
                            Name = $"dot {reference.Value}"
                            Description = null
                            Model = reference
                            SystemPrompt = ""
                            EnvironmentVariables = null
                            PermissionDefaults = null
                            ToolSelection = null
                            PackageReference = DotSkills.sampleVersion
                            Enabled = true
                            Schedule = null
                            RowVersion = 0UL
                            CreatedAt = now
                            UpdatedAt = now
                        }

                    let! outcome = agents.UpdateIfUnchanged(TenantId.Default, agent, 0UL, cancellationToken)

                    match outcome with
                    | :? AgentUpdated as updated -> return updated.Agent.Id
                    | :? AgentUpdateConflict as conflict ->
                        // Fresh ids never collide: the conflict branch is defensive
                        // (a concurrent insert of the same model), so re-list and
                        // reuse the winner's row.
                        let! relisted = agents.ListAgents(TenantId.Default, cancellationToken)

                        match findMatch relisted with
                        | Some found -> return found.Id
                        | None ->
                            match conflict.Agent with
                            | null ->
                                return
                                    raise (
                                        InvalidOperationException(
                                            $"The agent for model '{reference.Value}' could not be provisioned."
                                        )
                                    )
                            | current -> return current.Id
                    | _ ->
                        return
                            raise (
                                InvalidOperationException(
                                    $"The agent for model '{reference.Value}' could not be provisioned."
                                )
                            )
            }

        do! DotSkills.uploadSamplePackageAsync packages TenantId.Default agentId cancellationToken
        do! stampPackageReferenceAsync agentId
        do! DotSkills.ensureSubAgentsAsync agents reference cancellationToken
        return agentId
    }

/// Renders one journaled event with the given line prefix. Module-level
/// (moved verbatim from the former Engine closure) so the TUI shell paints
/// the same plain transcript lines the REPL streams.
/// <param name="prefix">The line prefix: EVENT for the live stream, TREE for /tree.</param>
/// <param name="evt">The event to render.</param>
/// <returns>The rendered line.</returns>
let renderWith (prefix: string) (evt: SessionEvent) : string =
    let sequence =
        if evt.Sequence.HasValue then
            evt.Sequence.Value.ToString()
        else
            "-"

    let detail =
        match evt with
        | :? PermissionRequestedEvent as asked when not (isNull (box asked)) ->
            $" tool={asked.ToolName} id={asked.RequestId}"
        | :? PermissionResolvedEvent as resolved when not (isNull (box resolved)) ->
            $" id={resolved.RequestId} decision={resolved.Decision}"
        | :? QuestionAskedEvent as asked when not (isNull (box asked)) ->
            $" id={asked.QuestionId} question={asked.Question}"
        | :? QuestionAnsweredEvent as answered when not (isNull (box answered)) -> $" id={answered.QuestionId}"
        | :? TurnFailedEvent as failed when not (isNull (box failed)) -> $" reason={failed.Reason}"
        | :? TurnAbortedEvent as aborted when not (isNull (box aborted)) -> $" reason={aborted.Reason}"
        | :? CompactedEvent as compacted when not (isNull (box compacted)) ->
            $" before={compacted.BeforeEstimate} after={compacted.AfterEstimate}"
        | :? CompactionFailedEvent as failed when not (isNull (box failed)) -> $" reason={failed.Reason}"
        | :? UserMessageEvent -> " user-message"
        | _ -> ""

    $"{prefix} seq={sequence} {evt.GetType().Name}{detail}"

/// Renders one journaled event as a stable single line.
/// <param name="evt">The event to render.</param>
/// <returns>The rendered line.</returns>
let renderEvent (evt: SessionEvent) : string = renderWith "EVENT" evt

/// Renders one journaled event for /tree output.
/// <param name="evt">The event to render.</param>
/// <returns>The rendered line.</returns>
let renderTree (evt: SessionEvent) : string = renderWith "TREE" evt

/// Maps one authoritative operation observation to visible text without
/// an engine instance: accepted-queued, committed success/failure/abort,
/// or explicit unknown/unavailable. Null reads unknown. Local task
/// lifetime is never presented as runtime state.
/// <param name="observed">The operation observation. Null reads unknown.</param>
/// <returns>The visible state text.</returns>
let describeOperationState (observed: OperationResult) : string =
    if isNull (box observed) then
        "unknown (insufficient evidence)"
    else
        match observed.Status with
        | OperationStatus.Pending ->
            $"accepted-queued op={observed.Position} kind={observed.Kind} (no committed result yet)"
        | OperationStatus.Terminal ->
            match box observed.Result with
            | null -> $"committed op={observed.Position} kind={observed.Kind} (terminal, result unavailable)"
            | boxed ->
                let result = unbox<TurnResult> boxed

                match result.Status with
                | TurnStatus.Completed -> $"committed-success op={observed.Position} kind={observed.Kind}"
                | TurnStatus.Aborted -> $"committed-abort op={observed.Position} kind={observed.Kind}"
                | TurnStatus.Failed -> $"committed-failure op={observed.Position} kind={observed.Kind}"
                | other -> $"committed op={observed.Position} kind={observed.Kind} status={other}"
        | OperationStatus.Unknown -> $"unknown op={observed.Position} (stale, missing, or mismatched evidence)"
        | OperationStatus.Unavailable -> $"unavailable op={observed.Position} (storage failed; never terminal)"
        | _ -> $"unknown op={observed.Position} (unrecognized status)"

/// The REPL engine: drives one current session through prompt, stream,
/// reply, and settle over the given reader/writer.
type Engine
    (
        client: SessionClient,
        agents: IAgentStore,
        packages: IAgentPackageStore,
        reader: System.IO.TextReader,
        writer: System.IO.TextWriter,
        waitBound: TimeSpan,
        initialModel: ModelReference,
        providerOptions: ProviderOption list
    ) =

    do
        ArgumentNullException.ThrowIfNull(client)
        ArgumentNullException.ThrowIfNull(agents)
        ArgumentNullException.ThrowIfNull(packages)
        ArgumentNullException.ThrowIfNull(reader)
        ArgumentNullException.ThrowIfNull(writer)

        if waitBound <= TimeSpan.Zero then
            raise (ArgumentOutOfRangeException(nameof waitBound, "The settle wait bound must be positive."))

        if isNull (box providerOptions) then
            raise (ArgumentNullException(nameof providerOptions))

        if providerOptions.IsEmpty then
            raise (ArgumentException("At least one provider option is required.", nameof providerOptions))

    let sessions = ResizeArray<ReplSession>()
    let mutable current = -1
    let mutable currentModel = initialModel
    let mutable tuiOwnsApprovals = false
    let mutable onEvent: (SessionEvent -> unit) option = None
    let lineGate = obj ()
    let pendingGate = obj ()
    let pendingQueue = Queue<PendingTurn>()
    let mutable drainTask: Task option = None
    // Continuous logical attachment (issue 365): one active consumer per
    // selected session. The attachment starts on select/open/attach
    // (including to already-running or suspended work with no prompt) and
    // survives individual waits, deadline expiry, and terminal results.
    // Transport recovery replaces the subscription without overlapping
    // active consumers or losing session identity; the session's own last
    // actually observed durable cursor is the only resume position, never
    // another session's cursor. Detach/switch releases the obsolete
    // observer promptly without aborting detached work.
    let attachmentGate = obj ()
    let mutable attachmentCts: CancellationTokenSource option = None
    let mutable attachmentTask: Task option = None
    let mutable attachmentSession: SessionId option = None
    let mutable activeConsumers = 0
    // The turn the drain loop currently waits on (dequeued, not queued):
    // the attachment routes prefix tracking to it as well as to queued
    // turns so settlement rendering stays per-turn scoped.
    let mutable drainCurrent: PendingTurn option = None
    // Authoritative operation observations by inbox position: Pending means
    // accepted with no committed winner yet; Terminal carries the winning
    // TurnResult; Unknown/Unavailable are shown explicitly as unknown,
    // never as running, success, or idle. An idle session may retain its
    // failed last operation.
    let operationGate = obj ()
    let operationStates = Dictionary<int64, OperationResult>()
    let mutable lastOperation: OperationResult option = None
    let mutable authoritativeRunning = false
    // Session-bound pending host-input requests by request id: the reply
    // path validates original session identity plus continued validity
    // and turn association at submit.
    let requestGate = obj ()
    let pendingRequests = Dictionary<string, PendingRequest>()
    // In-flight acceptances (prompt sent, receipt not yet observed): the
    // receipt-bound waiter is created on confirmed acceptance, but the
    // foreground loop and /quit drain must treat acceptance-in-flight as
    // live work so steering lands mid-turn and quits wait for the
    // acceptance plus its operation. Stopping observation never aborts.
    let inflightGate = obj ()
    let mutable inflightAcceptances = 0
    // Consumer dedup (issue 385): durable (session, sequence) identities
    // already printed on the stream. Duplicate transport delivery repeats
    // the identical pair, so the second sighting prints nothing; distinct
    // same-text events carry distinct sequences and stay distinct.
    // Session-scoped keys keep session switches and resumes from
    // cross-deduplicating unrelated content.
    let renderGate = obj ()
    let rendered = HashSet<string>()

    /// True when the event was already printed: the second sighting of a
    /// duplicate delivery. Marks first sightings. Null and in-flight
    /// (sequence-free) events never deduplicate.
    /// <param name="evt">The event just observed.</param>
    /// <returns>True when the event must be skipped.</returns>
    let isDuplicate (evt: SessionEvent) : bool =
        lock renderGate (fun () ->
            match DotDedup.durableKeyOf evt with
            | None -> false
            | Some identity ->
                if rendered.Contains identity then
                    true
                else
                    rendered.Add identity |> ignore
                    false)

    let approvalGate = obj ()
    let mutable approvalCount = 0
    let usageGate = obj ()
    let usageTotals = Dictionary<SessionId, int64 * int64>()

    let line (text: string) : unit =
        lock lineGate (fun () ->
            writer.WriteLine(text)
            writer.Flush())

    /// True while the drain loop owns a live turn.
    let isDrainRunning () : bool =
        lock pendingGate (fun () ->
            match drainTask with
            | Some running when not running.IsCompleted -> true
            | Some _
            | None -> false)

    /// Marks one inline approval wait as pending.
    let markApproval () : unit =
        lock approvalGate (fun () -> approvalCount <- approvalCount + 1)

    /// Clears one inline approval wait.
    let clearApproval () : unit =
        lock approvalGate (fun () -> approvalCount <- max 0 (approvalCount - 1))

    /// True while a permission or question answer owns the reader.
    let isApprovalPending () : bool =
        lock approvalGate (fun () -> approvalCount > 0)

    /// True while the drain loop owns a live client wait or an acceptance
    /// is in flight (prompt sent, receipt not yet observed). Local wait or
    /// acceptance ownership is never presented as runtime execution state;
    /// use IsTurnRunning (authoritative) for status display. The
    /// foreground loop treats acceptance-in-flight as live so steering
    /// lands mid-turn and /quit drains the acceptance plus its operation.
    let isDrainRunning () : bool =
        let drainLive =
            lock pendingGate (fun () ->
                match drainTask with
                | Some running when not running.IsCompleted -> true
                | Some _
                | None -> false)

        if drainLive then
            true
        else
            lock pendingGate (fun () -> pendingQueue.Count > 0)
            || lock inflightGate (fun () -> inflightAcceptances > 0)

    /// Marks one acceptance as in flight (prompt sent, receipt pending).
    let markInflight () : unit =
        lock inflightGate (fun () -> inflightAcceptances <- inflightAcceptances + 1)

    /// Clears one in-flight acceptance when its receipt (or failure)
    /// lands: the receipt-bound waiter takes over from here.
    let clearInflight () : unit =
        lock inflightGate (fun () -> inflightAcceptances <- max 0 (inflightAcceptances - 1))

    /// Records one authoritative operation observation: Pending marks the
    /// operation accepted with no committed winner (execution may be
    /// queued, running, or suspended for input); Terminal carries the
    /// committed winning TurnResult; Unknown/Unavailable are retained
    /// explicitly and never rendered as running, success, or idle.
    let recordOperation (observed: OperationResult) : unit =
        if not (isNull (box observed)) then
            lock operationGate (fun () ->
                operationStates[observed.Position] <- observed
                lastOperation <- Some observed

                match observed.Status with
                | OperationStatus.Pending -> authoritativeRunning <- true
                | OperationStatus.Terminal -> authoritativeRunning <- false
                | OperationStatus.Unknown
                | OperationStatus.Unavailable -> ()
                | _ -> ())

    /// Marks authoritative execution from the attachment's durable event
    /// observation: started/permission/question means work is live,
    /// terminal settlement means it committed. Local task lifetime never
    /// drives this flag.
    let markAuthoritativeEvent (evt: SessionEvent) : unit =
        if not (isNull (box evt)) then
            lock operationGate (fun () ->
                match evt with
                | :? TurnStartedEvent -> authoritativeRunning <- true
                | :? PermissionRequestedEvent -> authoritativeRunning <- true
                | :? QuestionAskedEvent -> authoritativeRunning <- true
                | :? TurnCompletedEvent
                | :? TurnAbortedEvent
                | :? TurnFailedEvent -> authoritativeRunning <- false
                | _ -> ())

    /// Tracks one session-bound host-input request as pending for its
    /// original session, request id, and turn. Replayed or duplicate asks
    /// keep the first registration; resolution removes it so replays never
    /// become actionable again.
    let trackRequest (sessionId: SessionId) (requestId: string) (turnId: TurnId) (isPermission: bool) : unit =
        if not (String.IsNullOrWhiteSpace requestId) then
            lock requestGate (fun () ->
                if not (pendingRequests.ContainsKey requestId) then
                    pendingRequests[requestId] <-
                        {
                            SessionId = sessionId
                            RequestId = requestId
                            TurnId = turnId
                            IsPermission = isPermission
                        })

    /// Clears one host-input request when it resolves, aborts, or is
    /// otherwise invalidated: later replays are rejected visibly.
    let resolveRequest (requestId: string) : unit =
        if not (String.IsNullOrWhiteSpace requestId) then
            lock requestGate (fun () -> pendingRequests.Remove requestId |> ignore)

    /// Validates a reply at submit: the request must still be pending for
    /// its original session with continued validity, and the engine must
    /// still be attached to that session. Switching sessions, resolving
    /// or aborting the turn, disconnecting, or invalidating the request
    /// rejects stale input visibly with no work and no disclosure, and
    /// never sends it to the newly selected session.
    let validateReply (requestId: string) (currentId: SessionId) : PendingRequest =
        let found =
            lock requestGate (fun () ->
                match pendingRequests.TryGetValue requestId with
                | true, pending -> Some pending
                | false, _ -> None)

        match found with
        | None ->
            line
                $"REPLY-REJECTED id={requestId} reason=stale-or-resolved (no pending request; switching, resolve/abort, disconnect, or replay never reuses it)"

            raise (
                InvalidOperationException(
                    $"The request '{requestId}' is not pending: it resolved, aborted, switched sessions, or was never asked."
                )
            )
        | Some pending ->
            let attached = lock attachmentGate (fun () -> attachmentSession)

            let attachedOk =
                match attached with
                | Some attachedId -> attachedId.Equals(pending.SessionId)
                | None -> false

            if not (pending.SessionId.Equals(currentId)) || not attachedOk then
                line
                    $"REPLY-REJECTED id={requestId} reason=session-mismatch (request belongs to {pending.SessionId}, selected is {currentId}; stale input is never sent cross-session)"

                raise (
                    InvalidOperationException(
                        $"The request '{requestId}' belongs to session {pending.SessionId}, not the selected {currentId}."
                    )
                )
            else
                pending

    /// Adds one settled turn's token usage to the session's running total:
    /// the journal carries no UsageEvents on this path, so the REPL
    /// accumulates what the authoritative terminal observation reports
    /// per operation instead. Only this process's settles accumulate;
    /// the journal sums stay durable.
    /// <param name="sessionId">The session the turn settled in.</param>
    /// <param name="usage">The settled turn's usage.</param>
    let recordUsage (sessionId: SessionId) (usage: UsageSummary) : unit =
        if not (isNull (box usage)) then
            lock usageGate (fun () ->
                let input, output =
                    match usageTotals.TryGetValue sessionId with
                    | true, (previousInput, previousOutput) -> previousInput, previousOutput
                    | false, _ -> 0L, 0L

                usageTotals[sessionId] <- (input + usage.InputTokens, output + usage.OutputTokens))

    /// Records usage from an authoritative terminal operation observation
    /// (the committed winning TurnResult), when it carries usage.
    /// <param name="observed">The terminal operation observation.</param>
    let recordOperationUsage (observed: OperationResult) : unit =
        if not (isNull (box observed)) && observed.Status = OperationStatus.Terminal then
            match box observed.Result with
            | null -> ()
            | boxed ->
                let result = unbox<TurnResult> boxed

                if not (isNull (box result.Usage)) then
                    recordUsage observed.SessionId result.Usage

    /// Reads the session's accumulated token usage in this process.
    /// <param name="sessionId">The session to read.</param>
    /// <returns>Accumulated input and output tokens, zeros when no turn settled here.</returns>
    let readUsage (sessionId: SessionId) : int64 * int64 =
        lock usageGate (fun () ->
            match usageTotals.TryGetValue sessionId with
            | true, totals -> totals
            | false, _ -> 0L, 0L)

    let currentSession () : ReplSession =
        if current < 0 || current >= sessions.Count then
            raise (InvalidOperationException("The REPL has no current session."))

        sessions[current]

    /// Finds a session by 1-based open index or session id text.
    /// <param name="text">The index or id the user typed.</param>
    /// <returns>The matching session index, or -1.</returns>
    let findSession (text: string) : int =
        match Int32.TryParse(text.Trim()) with
        | true, number when number >= 1 && number <= sessions.Count -> number - 1
        | _ ->
            let mutable parsed = Unchecked.defaultof<SessionId>

            if SessionId.TryParse(text, &parsed) then
                sessions
                |> Seq.tryFindIndex (fun session -> session.Id.Equals(parsed))
                |> Option.defaultValue -1
            else
                -1

    /// Answers one permission request from the console. Binds to the
    /// request's original session identity: the reply goes to the session
    /// that asked, and only while the request is still pending for its
    /// turn. Switching sessions or resolving/aborting the turn rejects
    /// the answer visibly with no send.
    /// <param name="asked">The pending permission request.</param>
    /// <param name="cancellationToken">Abandons the reply.</param>
    let answerPermission (asked: PermissionRequestedEvent) (cancellationToken: CancellationToken) : Task =
        task {
            markApproval ()

            try
                // Plain-input ownership: an obsolete approval reader never
                // consumes unrelated command input as permission. The
                // console prompt owns the reader only while this request
                // is still pending for the attached session.
                let stillPending =
                    lock requestGate (fun () -> pendingRequests.ContainsKey asked.RequestId)

                if not stillPending then
                    line
                        $"REPLY-REJECTED id={asked.RequestId} reason=stale-or-resolved (permission no longer pending; unrelated input is never granted)"
                else
                    line
                        $"PERMISSION tool={asked.ToolName} id={asked.RequestId} [a]llow once, allow for [s]ession, [d]eny:"

                    let! rawChoice = reader.ReadLineAsync()

                    let choiceText =
                        match rawChoice with
                        | null -> ""
                        | text -> text.Trim().ToLowerInvariant()

                    let decision =
                        match choiceText with
                        | "s"
                        | "session" -> PermissionDecisionKind.AllowForSession
                        | "d"
                        | "deny" -> PermissionDecisionKind.Deny
                        | _ -> PermissionDecisionKind.AllowOnce

                    try
                        let! _ =
                            SessionClientOperations.ReplyAsync(
                                client,
                                asked.SessionId,
                                PermissionDecision(asked.RequestId, decision),
                                cancellationToken
                            )

                        resolveRequest asked.RequestId
                        ()
                    with
                    | :? ReplyMismatchException as mismatch ->
                        resolveRequest asked.RequestId
                        line $"REPLY-REJECTED id={asked.RequestId} reason=invalid ({mismatch.Message})"
                    | error -> line $"REPLY-REJECTED id={asked.RequestId} reason=invalid ({error.Message})"
            finally
                clearApproval ()
        }

    /// Answers one agent question from the console. Binds to the
    /// request's original session identity with the same stale/replay
    /// rejection as permission replies.
    /// <param name="asked">The pending question.</param>
    /// <param name="cancellationToken">Abandons the reply.</param>
    let answerQuestion (asked: QuestionAskedEvent) (cancellationToken: CancellationToken) : Task =
        task {
            markApproval ()

            try
                let stillPending =
                    lock requestGate (fun () -> pendingRequests.ContainsKey asked.QuestionId)

                if not stillPending then
                    line $"REPLY-REJECTED id={asked.QuestionId} reason=stale-or-resolved (question no longer pending)"
                else
                    line $"QUESTION id={asked.QuestionId}: {asked.Question}"
                    line "ANSWER:"

                    let! rawAnswer = reader.ReadLineAsync()

                    let answer =
                        match rawAnswer with
                        | null -> ""
                        | text -> text

                    try
                        let! _ =
                            SessionClientOperations.ReplyAsync(
                                client,
                                asked.SessionId,
                                QuestionAnswer(asked.QuestionId, answer),
                                cancellationToken
                            )

                        resolveRequest asked.QuestionId
                        ()
                    with
                    | :? ReplyMismatchException as mismatch ->
                        resolveRequest asked.QuestionId
                        line $"REPLY-REJECTED id={asked.QuestionId} reason=invalid ({mismatch.Message})"
                    | error -> line $"REPLY-REJECTED id={asked.QuestionId} reason=invalid ({error.Message})"
            finally
                clearApproval ()
        }

    /// True while a permission or question answer owns the reader.
    /// <returns>True while an approval answer is pending.</returns>
    member _.IsApprovalPending: bool = isApprovalPending ()

    /// True when the fullscreen TUI owns approvals: the engine's stream
    /// renders events without blocking on the console reader, and the TUI
    /// answers through ReplyPermissionAsync/ReplyQuestionAsync over the
    /// same ReplyAsync path. False keeps the plain REPL console reader.
    /// <returns>True when the TUI owns approvals.</returns>
    member _.TuiOwnsApprovals: bool = tuiOwnsApprovals

    /// Takes over the #332 console-reader deferral: the TUI sets single
    /// ownership so permission/question prompts move inline and the
    /// engine never blocks on ReadLineAsync beside the key pump.
    /// <param name="value">True when the TUI owns approvals.</param>
    member _.SetTuiOwnsApprovals(value: bool) : unit = tuiOwnsApprovals <- value

    /// The TUI renderer hook: invoked with every Subscribe event the
    /// stream observes, in addition to the plain EVENT line. None keeps
    /// the plain REPL path byte-identical; the fullscreen shell sets it
    /// to fold the same stream into viewport blocks.
    /// <returns>The current hook, or None.</returns>
    member _.OnEvent: (SessionEvent -> unit) option = onEvent

    /// Sets the TUI renderer hook (see OnEvent).
    /// <param name="hook">The hook, or None to clear.</param>
    member _.SetOnEvent(hook: (SessionEvent -> unit) option) : unit = onEvent <- hook

    /// Answers one permission request without console I/O: the TUI inline
    /// approval path over the same ReplyAsync PermissionDecision the REPL
    /// console reader uses. Binds to the request's original session plus
    /// continued validity and turn association: switching sessions,
    /// resolving/aborting the turn, disconnecting, or replaying an
    /// already-resolved request rejects stale input visibly with no work,
    /// no disclosure, and never sends it to the newly selected session.
    /// <param name="requestId">The permission request id.</param>
    /// <param name="decision">What the host decided.</param>
    /// <param name="cancellationToken">Abandons the reply.</param>
    member _.ReplyPermissionAsync
        (requestId: string, decision: PermissionDecisionKind, cancellationToken: CancellationToken)
        : Task =
        task {
            let session = currentSession ()

            let safeRequest =
                match box requestId with
                | null -> ""
                | _ -> requestId

            let pending = validateReply safeRequest session.Id

            try
                let! _ =
                    SessionClientOperations.ReplyAsync(
                        client,
                        pending.SessionId,
                        PermissionDecision(safeRequest, decision),
                        cancellationToken
                    )

                resolveRequest safeRequest
                ()
            with
            | :? ReplyMismatchException as mismatch ->
                resolveRequest safeRequest
                line $"REPLY-REJECTED id={safeRequest} reason=invalid ({mismatch.Message})"
                raise (InvalidOperationException(mismatch.Message, mismatch :> exn))
            | error ->
                line $"REPLY-REJECTED id={safeRequest} reason=invalid ({error.Message})"
                raise error
        }

    /// Answers one agent question without console I/O: the TUI inline
    /// answer-field path over the same ReplyAsync QuestionAnswer the REPL
    /// console reader uses (null becomes empty, every other buffer resumes
    /// verbatim). Session- and turn-bound exactly like permission replies.
    /// <param name="questionId">The question id.</param>
    /// <param name="answer">The submitted answer buffer.</param>
    /// <param name="cancellationToken">Abandons the reply.</param>
    member _.ReplyQuestionAsync(questionId: string, answer: string, cancellationToken: CancellationToken) : Task =
        task {
            let session = currentSession ()

            let safeQuestion =
                match box questionId with
                | null -> ""
                | _ -> questionId

            let safeAnswer =
                match box answer with
                | null -> ""
                | _ -> answer

            let pending = validateReply safeQuestion session.Id

            try
                let! _ =
                    SessionClientOperations.ReplyAsync(
                        client,
                        pending.SessionId,
                        QuestionAnswer(safeQuestion, safeAnswer),
                        cancellationToken
                    )

                resolveRequest safeQuestion
                ()
            with
            | :? ReplyMismatchException as mismatch ->
                resolveRequest safeQuestion
                line $"REPLY-REJECTED id={safeQuestion} reason=invalid ({mismatch.Message})"
                raise (InvalidOperationException(mismatch.Message, mismatch :> exn))
            | error ->
                line $"REPLY-REJECTED id={safeQuestion} reason=invalid ({error.Message})"
                raise error
        }

    /// The current session id for fullscreen status display.
    /// <returns>The current session id.</returns>
    member _.CurrentSessionId: SessionId = (currentSession ()).Id

    /// The currently selected model, including interactive model switches.
    member _.CurrentModel: ModelReference = currentModel

    /// Authoritative execution state for status display: true while the
    /// last authoritative observation (receipt Pending or durable
    /// TurnStarted/permission/question) says work is live, false after a
    /// committed terminal settlement. Local drain-task lifetime never
    /// drives this flag: an idle session can retain a failed last
    /// operation, and unavailable/stale evidence reads as unknown, never
    /// as running, success, or idle.
    /// <returns>True while authoritative evidence says work is live.</returns>
    member _.IsTurnRunning: bool = lock operationGate (fun () -> authoritativeRunning)

    /// True while the drain loop owns a live client wait (local wait
    /// ownership, never execution evidence). Public for tests proving
    /// that stopping a wait never aborts execution.
    /// <returns>True while a client wait is owned.</returns>
    member _.IsWaitOwned: bool = isDrainRunning ()

    /// The session the continuous attachment currently observes, or None
    /// when detached. Public for tests proving one active consumer and
    /// session-identity retention across recovery.
    /// <returns>The attached session id, or None.</returns>
    member _.AttachmentSessionId: SessionId option =
        lock attachmentGate (fun () -> attachmentSession)

    /// How many live attachment consumers exist (0 or 1). Repeated
    /// waits, turns, and switches keep one active consumer with bounded
    /// cleanup rather than accumulating obsolete readers/subscriptions.
    /// <returns>0 or 1.</returns>
    member _.ActiveConsumerCount: int = lock attachmentGate (fun () -> activeConsumers)

    /// The last authoritative operation observation text for diagnostics:
    /// explicit Pending/Terminal/Unknown/Unavailable with position, kind,
    /// turn association, and the mapped visible state (accepted-queued,
    /// committed-success/failure/abort, unknown, unavailable), or unknown
    /// when no operation was observed. An idle session retains its failed
    /// last operation visibly instead of reading idle or running.
    /// <returns>The diagnostic text.</returns>
    member _.LastOperationText: string =
        lock operationGate (fun () ->
            match lastOperation with
            | None -> "unknown (no accepted operation observed)"
            | Some observed ->
                let turn =
                    if observed.TurnId.HasValue then
                        observed.TurnId.Value.ToString()
                    else
                        "none"

                let visible = describeOperationState observed

                $"op={observed.Position} kind={observed.Kind} status={observed.Status} turn={turn} ({visible})")

    /// Maps one authoritative operation observation to the visible
    /// session plus accepted-operation state: accepted-queued, executing,
    /// awaiting permission/question input, committed
    /// success/failure/abort, or explicit unknown/unavailable. Local task
    /// running or ending is never presented as runtime state.
    /// <param name="observed">The operation observation. Null reads unknown.</param>
    /// <returns>The visible state text.</returns>
    member _.DescribeOperationState(observed: OperationResult) : string = describeOperationState observed

    /// Folds one fresh event into one queued turn's streamed prefix: the
    /// first TurnStarted claims the turn, deltas accumulate only for the
    /// claimed turn, and the terminal event snapshots the prefix so
    /// settlement stays scoped when the next turn already streams.
    /// <param name="pending">The queued turn to track.</param>
    /// <param name="evt">The event just observed.</param>
    /// <param name="hasHook">True when the fullscreen hook renders deltas progressively.</param>
    member private _.TrackPrefixFor(pending: PendingTurn, evt: SessionEvent, hasHook: bool) : unit =
        match evt with
        | :? TurnStartedEvent ->
            let turnKey = DotDedup.turnKeyOf evt

            if pending.TurnKey = "" then
                pending.TurnKey <- turnKey

            if turnKey = pending.TurnKey then
                pending.StreamedPrefix <- ""
                pending.SettledPrefix <- ""
        | :? TextDeltaEvent as delta when not (isNull (box delta)) ->
            let raw: string | null = delta.Text

            // Visible scope (issue 412): the plain EVENT line never
            // carries delta text, so only the hooked fullscreen fold
            // (which renders delta text progressively) contributes to
            // the streamed prefix. Fallback deltas on an unhooked
            // stream leave the prefix empty, so the settlement carrying
            // the sole visible copy renders fully instead of being
            // mistaken for already-rendered output.
            let fragment = DotDedup.visibleFragment hasHook raw

            if fragment <> "" then
                let turnKey = DotDedup.turnKeyOf evt

                if pending.TurnKey = "" then
                    pending.TurnKey <- turnKey

                if turnKey = pending.TurnKey then
                    // Uncapped: the prefix must stay an exact prefix
                    // of the settlement for suffix matching; one
                    // turn's deltas are bounded by the model.
                    pending.StreamedPrefix <- pending.StreamedPrefix + fragment
        | :? TurnCompletedEvent
        | :? TurnAbortedEvent
        | :? TurnFailedEvent ->
            let turnKey = DotDedup.turnKeyOf evt

            if pending.TurnKey = "" then
                pending.TurnKey <- turnKey

            if turnKey = pending.TurnKey then
                pending.SettledPrefix <- pending.StreamedPrefix
        | _ -> ()

    /// Observes one attachment event for the attached session: advances
    /// the session's own durable cursor, tracks streamed prefixes for
    /// queued and draining turns, renders the stable line, folds the TUI
    /// hook, tracks session-bound permission/question validity, marks
    /// authoritative execution, and answers console prompts inline.
    /// Late events from an old attachment never reach here: Detach cancels
    /// the obsolete subscription before a new session attaches.
    /// <param name="session">The attached session.</param>
    /// <param name="evt">The event just observed.</param>
    /// <param name="cancellationToken">Stops inline replies.</param>
    member private this.ObserveEventAsync
        (session: ReplSession, evt: SessionEvent, cancellationToken: CancellationToken)
        : Task =
        task {
            if not (isNull (box evt)) && not (isDuplicate evt) then
                if evt.SessionId.Equals(session.Id) then
                    if evt.Sequence.HasValue && evt.Sequence.Value > session.Cursor then
                        session.Cursor <- evt.Sequence.Value

                    let hasHook = onEvent.IsSome

                    // Prefix tracking across queued and draining turns:
                    // every pending for this session sees the event so
                    // settlement rendering stays scoped per turn.
                    lock pendingGate (fun () ->
                        for queued in pendingQueue do
                            if queued.Session.Id.Equals(session.Id) then
                                this.TrackPrefixFor(queued, evt, hasHook)

                        match drainCurrent with
                        | Some active when active.Session.Id.Equals(session.Id) ->
                            this.TrackPrefixFor(active, evt, hasHook)
                        | Some _
                        | None -> ())

                    markAuthoritativeEvent evt

                    match evt with
                    | :? PermissionRequestedEvent as asked when not (isNull (box asked)) ->
                        trackRequest session.Id asked.RequestId asked.TurnId true
                    | :? QuestionAskedEvent as asked when not (isNull (box asked)) ->
                        trackRequest session.Id asked.QuestionId asked.TurnId false
                    | :? PermissionResolvedEvent as resolved when not (isNull (box resolved)) ->
                        resolveRequest resolved.RequestId
                    | :? QuestionAnsweredEvent as answered when not (isNull (box answered)) ->
                        resolveRequest answered.QuestionId
                    | :? TurnCompletedEvent
                    | :? TurnAbortedEvent
                    | :? TurnFailedEvent -> ()
                    | _ -> ()

                    line (renderEvent evt)

                    match onEvent with
                    | Some hook ->
                        try
                            hook evt
                        with _ ->
                            ()
                    | None -> ()

                    match evt with
                    | :? PermissionRequestedEvent as asked when not (isNull (box asked)) ->
                        if not tuiOwnsApprovals then
                            do! answerPermission asked cancellationToken
                    | :? QuestionAskedEvent as asked when not (isNull (box asked)) ->
                        if not tuiOwnsApprovals then
                            do! answerQuestion asked cancellationToken
                    | _ -> ()
                else
                    // Late old-session event after a switch: never updates
                    // the new session's transcript, state, or controls.
                    line $"IGNORED-STALE-EVENT session={evt.SessionId} selected={session.Id} type={evt.GetType().Name}"
            else
                ()
        }

    /// Runs the continuous logical attachment observation for one session:
    /// Subscribe from the session's own last actually observed durable
    /// cursor, then yield live publishes gap-free. Individual waits,
    /// deadline expiry, and terminal results never end this loop; only
    /// detach, switch, or reconnect replaces it. Transport recovery
    /// re-subscribes from the same cursor with no overlapping consumers
    /// and no loss of session identity. Expired ranges, unsupported
    /// versions, denied access, and observation failures surface explicit
    /// diagnostics and keep the attachment identity for reattach.
    /// <param name="session">The attached session.</param>
    /// <param name="cancellationToken">Detaches the attachment.</param>
    member private this.AttachmentLoopAsync(session: ReplSession, cancellationToken: CancellationToken) : Task =
        task {
            let mutable alive = true

            while alive && not cancellationToken.IsCancellationRequested do
                let stream =
                    try
                        Some(SessionClientOperations.Subscribe(client, session.Id, session.Cursor, cancellationToken))
                    with error ->
                        line
                            $"OBSERVE-FAILED session={session.Id} reason={error.Message} (attachment kept; reattach recovers retained events)"

                        line cleanStartHelp
                        None

                match stream with
                | None ->
                    try
                        do! Task.Delay(TimeSpan.FromSeconds 1.0, cancellationToken)
                    with :? OperationCanceledException ->
                        alive <- false
                | Some events ->
                    let enumerator = events.GetAsyncEnumerator(cancellationToken)

                    try
                        let mutable go = true

                        while go && not cancellationToken.IsCancellationRequested do
                            try
                                let! has = enumerator.MoveNextAsync().AsTask()

                                if not has then
                                    go <- false
                                else
                                    do! this.ObserveEventAsync(session, enumerator.Current, cancellationToken)
                            with
                            | :? OperationCanceledException -> go <- false
                            | :? SessionNotFoundException as missing ->
                                line
                                    $"OBSERVE-FAILED session={session.Id} reason={missing.Message} (unknown session; reattach or clean start)"

                                go <- false
                                alive <- false
                            | :? InvalidOperationException as invalid ->
                                // Unsupported old persistence/wait/event
                                // contracts fail clearly with a documented
                                // clean-start requirement; never fabricate
                                // resumed state, results, or conversation.
                                line $"UNSUPPORTED-CONTRACT session={session.Id} reason={invalid.Message}"
                                line cleanStartHelp
                                go <- false
                                alive <- false
                            | error ->
                                line
                                    $"OBSERVE-FAILED session={session.Id} cursor={session.Cursor} reason={error.Message} (recovering from retained cursor; never borrows another session)"

                                go <- false
                    finally
                        try
                            enumerator.DisposeAsync().AsTask() |> ignore
                        with _ ->
                            ()

                    // Transport ended without detach (reconnect path):
                    // replace the subscription from the same cursor after
                    // a brief pause, keeping session identity and never
                    // overlapping consumers (this loop is the only one).
                    if alive && not cancellationToken.IsCancellationRequested then
                        try
                            do! Task.Delay(TimeSpan.FromMilliseconds 250.0, cancellationToken)
                        with :? OperationCanceledException ->
                            alive <- false
        }

    /// Ensures the continuous attachment observes the given session: a
    /// matching live attachment is kept; any obsolete observer is
    /// released promptly (bounded join, never abort) before the new
    /// subscription starts, so repeated waits, turns, and switches keep
    /// exactly one active consumer with bounded cleanup.
    /// <param name="session">The session to attach.</param>
    /// <param name="cancellationToken">Abandons the attach.</param>
    member private this.EnsureAttachment(session: ReplSession, cancellationToken: CancellationToken) : unit =
        lock attachmentGate (fun () ->
            let live =
                match attachmentTask, attachmentSession with
                | Some running, Some attachedId when attachedId.Equals(session.Id) && not running.IsCompleted -> true
                | Some _, _
                | None, _ -> false

            if not live then
                // Release the obsolete observer promptly without aborting
                // detached work: cancellation ends observation only.
                match attachmentCts with
                | Some obsolete ->
                    try
                        obsolete.Cancel()
                    with _ ->
                        ()
                | None -> ()

                match attachmentTask with
                | Some obsoleteTask ->
                    try
                        obsoleteTask.Wait(TimeSpan.FromSeconds 5.0) |> ignore
                    with _ ->
                        ()
                | None -> ()

                match attachmentCts with
                | Some obsolete ->
                    try
                        obsolete.Dispose()
                    with _ ->
                        ()
                | None -> ()

                let linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken)
                attachmentCts <- Some linked
                attachmentSession <- Some session.Id
                activeConsumers <- 1
                attachmentTask <- Some(this.AttachmentLoopAsync(session, linked.Token))
            else
                ())

    /// Detaches the continuous attachment without aborting detached work:
    /// stopping observation never affects execution. The session's own
    /// cursor is retained, so reattach recovers retained events, results,
    /// and actual state.
    member private _.DetachAttachment() : unit =
        lock attachmentGate (fun () ->
            match attachmentCts with
            | Some obsolete ->
                try
                    obsolete.Cancel()
                with _ ->
                    ()
            | None -> ()

            match attachmentTask with
            | Some running ->
                try
                    running.Wait(TimeSpan.FromSeconds 5.0) |> ignore
                with _ ->
                    ()
            | None -> ()

            match attachmentCts with
            | Some obsolete ->
                try
                    obsolete.Dispose()
                with _ ->
                    ()
            | None -> ()

            attachmentCts <- None
            attachmentTask <- None
            attachmentSession <- None
            activeConsumers <- 0)

    /// Opens a session and makes it current without entering the prompt
    /// loop: the fullscreen TUI route hook (issue 332). The prompt,
    /// attachment, reply, and settle behavior stays identical: the TUI feeds
    /// HandleLineAsync, which routes exactly as the plain REPL.
    /// <param name="title">The session title.</param>
    /// <param name="cancellationToken">Abandons the open.</param>
    member this.OpenSessionAsync(title: string, cancellationToken: CancellationToken) : Task =
        this.OpenAsync(title, cancellationToken)

    /// Reports the authoritative observation for one receipt without
    /// consuming it: Pending, Terminal, Unknown, or Unavailable. Unknown
    /// and Unavailable never carry a terminal result and are shown
    /// explicitly, never as running, success, or idle.
    /// <param name="receipt">The accepted-operation receipt.</param>
    /// <param name="cancellationToken">Abandons the lookup.</param>
    /// <returns>The diagnostic state text.</returns>
    member this.QueryOperationStateAsync
        (receipt: AcceptedOperation, cancellationToken: CancellationToken)
        : Task<string> =
        task {
            try
                let! observed = SessionClientOperations.GetOperationResultAsync(client, receipt, cancellationToken)

                recordOperation observed
                return this.DescribeOperationState observed
            with
            | :? SessionNotFoundException as missing -> return $"unknown (session missing: {missing.Message})"
            | error -> return $"unavailable ({error.Message}; never terminal)"
        }

    /// Runs one queued turn: awaits its receipt-bound operation wait and
    /// prints the authoritative settle. Settlement rendering is
    /// prefix-aware per turn (issue 385): streamed-then-success prints only
    /// the genuinely unrendered suffix, partial-then-failure/abort keeps the
    /// streamed partial once with the truthful terminal outcome and no
    /// invented success text, and settlement-only/nonstreaming output prints
    /// fully. A wait that outruns its bound reports DEADLINE naming the
    /// accepted operation and stating only that client waiting stopped (no
    /// abort, no result consumption, no disconnect, no continued-execution
    /// assertion); the attachment keeps observing so committed events and
    /// the terminal outcome remain visible across subsequent turns and
    /// reconnect. Committed completion or failure is never mislabeled as
    /// ongoing work. Stopping the wait never aborts execution.
    /// <param name="pending">The queued turn.</param>
    /// <param name="_cancellationToken">Abandons the drain (observation is receipt-bound; kept for signature).</param>
    member private this.RunPendingAsync(pending: PendingTurn, _cancellationToken: CancellationToken) : Task =
        task {
            lock pendingGate (fun () -> drainCurrent <- Some pending)

            try
                try
                    let! observed = pending.WaitTask
                    recordOperation observed
                    recordOperationUsage observed

                    match observed.Status with
                    | OperationStatus.Terminal ->
                        match box observed.Result with
                        | null -> line $"RESULT unknown (terminal op={observed.Position} with no result)"
                        | boxed ->
                            let result = unbox<TurnResult> boxed
                            line $"RESULT {result.Status}"

                            if
                                result.Status = TurnStatus.Completed
                                && not (String.IsNullOrEmpty result.AssistantText)
                            then
                                let prefix =
                                    if not (String.IsNullOrEmpty pending.SettledPrefix) then
                                        pending.SettledPrefix
                                    else
                                        pending.StreamedPrefix

                                let suffix = DotDedup.settlementSuffix prefix result.AssistantText

                                if suffix <> "" then
                                    line suffix

                        line "END-RESULT"
                        line $"STATE {this.DescribeOperationState observed}"
                    | OperationStatus.Pending ->
                        line
                            $"STATE {this.DescribeOperationState observed} (wait resolved pending; continuing observation)"
                    | OperationStatus.Unknown
                    | OperationStatus.Unavailable -> line $"STATE {this.DescribeOperationState observed}"
                    | _ -> line $"STATE {this.DescribeOperationState observed}"
                with
                | :? DeadlineExceededException ->
                    // The Dot bound lapsed: client completion waiting
                    // stopped for this operation. Execution continues,
                    // the result is not consumed, the attachment stays
                    // connected, and no continued execution is asserted
                    // without evidence. Steer, follow, abort, and replies
                    // remain usable afterwards.
                    line
                        $"DEADLINE op={pending.Receipt.Position} kind={pending.Receipt.Kind} client waiting stopped after {waitBound} (work continues; attachment still observing; STATE follows)"

                    try
                        let! observed =
                            SessionClientOperations.GetOperationResultAsync(
                                client,
                                pending.Receipt,
                                CancellationToken.None
                            )

                        recordOperation observed
                        line $"STATE {this.DescribeOperationState observed}"
                    with error ->
                        line $"STATE unavailable op={pending.Receipt.Position} ({error.Message}; never terminal)"
                | :? OperationCanceledException -> ()
            finally
                lock pendingGate (fun () -> drainCurrent <- None)

                try
                    pending.WaitCts.Dispose()
                with _ ->
                    ()
        }

    /// Drains the pending queue in order on one task: the single in-flight
    /// turn task the foreground loop stays responsive beside. Each entry
    /// keeps its own waiter-before-prompt ordering, so a pre-empted turn's
    /// waiter still completes and its RESULT Aborted line prints instead of
    /// going silent. Cursor-advanced Subscribe keeps the stream gap-free
    /// with no duplicates across turns.
    /// <param name="cancellationToken">Abandons the drain.</param>
    member private this.DrainLoopAsync(cancellationToken: CancellationToken) : Task =
        task {
            let mutable go = true

            while go do
                let next: PendingTurn option =
                    lock pendingGate (fun () ->
                        if pendingQueue.Count > 0 then
                            Some(pendingQueue.Dequeue())
                        else
                            None)

                match next with
                | None ->
                    lock pendingGate (fun () -> drainTask <- None)
                    go <- false
                | Some pending -> do! this.RunPendingAsync(pending, cancellationToken)
        }

    /// Starts the drain loop when none runs.
    /// <param name="cancellationToken">Abandons the drain.</param>
    member private this.EnsureDrain(cancellationToken: CancellationToken) : unit =
        lock pendingGate (fun () ->
            let running =
                match drainTask with
                | Some running when not running.IsCompleted -> true
                | Some _
                | None -> false

            if not running then
                drainTask <- Some(this.DrainLoopAsync(cancellationToken)))

    /// Waits for in-flight acceptances plus the drain to empty: /quit and
    /// end-of-input exit only after every accepted turn settles visibly.
    /// Acceptance-in-flight (prompt sent, receipt pending) is awaited
    /// first, then queued waits, then the active drain; new acceptances
    /// landing mid-drain are picked up in the same pass. Stopping the wait
    /// never aborts execution.
    /// <param name="cancellationToken">Abandons the wait.</param>
    member private _.DrainAsync(cancellationToken: CancellationToken) : Task =
        task {
            let deadline = DateTimeOffset.UtcNow.AddMinutes(5.0)
            let mutable go = true

            while go
                  && DateTimeOffset.UtcNow < deadline
                  && not cancellationToken.IsCancellationRequested do
                let inflight = lock inflightGate (fun () -> inflightAcceptances)
                let queued = lock pendingGate (fun () -> pendingQueue.Count)
                let running: Task option = lock pendingGate (fun () -> drainTask)

                let live =
                    match running with
                    | Some active when not active.IsCompleted -> Some active
                    | Some _
                    | None -> None

                if inflight = 0 && queued = 0 then
                    match live with
                    | None -> go <- false
                    | Some active ->
                        try
                            do! active.WaitAsync(cancellationToken)
                        with _ ->
                            ()
                else
                    match live with
                    | Some active ->
                        try
                            do! (Task.WhenAny(active, Task.Delay(50, cancellationToken)) :> Task)
                        with _ ->
                            ()
                    | None ->
                        try
                            do! Task.Delay(50, cancellationToken)
                        with :? OperationCanceledException ->
                            go <- false
        }

    /// Prompts with a receipt-bound waiter: PromptAsync acceptance
    /// returns the AcceptedOperation receipt first (confirmed acceptance),
    /// then WaitForOperationAsync bounds only client completion waiting
    /// from acceptance (queued time included). Acceptance or request
    /// failures report not-accepted and never as accepted execution;
    /// queued-time expiry cancels nothing and charges nothing against the
    /// runtime execution budget. The accepted control (ACCEPTED line) stays
    /// distinguishable from its eventual effect (RESULT/STATE lines).
    /// <param name="session">The session to prompt.</param>
    /// <param name="text">The user text.</param>
    /// <param name="delivery">How the message is delivered to a running turn.</param>
    /// <param name="cancellationToken">Abandons the turn.</param>
    member private this.EnqueueTurnAsync
        (session: ReplSession, text: string, delivery: DeliveryMode, cancellationToken: CancellationToken)
        : Task =
        task {
            // Attachment first: selecting/prompting establishes the
            // continuous observation (including to already-running work)
            // before the new input lands.
            this.EnsureAttachment(session, cancellationToken)

            let message: UserMessage option =
                try
                    Some(UserMessage.Text text)
                with error ->
                    line $"ERROR not-accepted ({error.Message})"
                    None

            match message with
            | None -> ()
            | Some userMessage ->
                // Fire-and-forget acceptance: PromptAsync answers once the
                // actor accepts, which may ride behind a running turn, so
                // awaiting here would block the foreground loop until the
                // turn settles and mid-turn steering could never land in
                // time. The receipt-bound waiter is created on confirmed
                // acceptance (bound starts at acceptance, queued time
                // included), so no pre-registration can steal results.
                // The acceptance is marked in flight synchronously so the
                // foreground loop and /quit drain treat it as live work.
                markInflight ()

                try
                    let promptTask =
                        SessionClientOperations.PromptAsync(
                            client,
                            session.Id,
                            userMessage,
                            delivery,
                            cancellationToken
                        )

                    promptTask.ContinueWith(fun (completed: Task<AcceptedOperation>) ->
                        try
                            if completed.IsCompletedSuccessfully then
                                let accepted = completed.Result
                                let acceptedAt = DateTimeOffset.UtcNow
                                lock operationGate (fun () -> authoritativeRunning <- true)

                                line
                                    $"ACCEPTED op={accepted.Position} kind={accepted.Kind} session={accepted.SessionId}"

                                let waitCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken)

                                let waitTask =
                                    SessionClientOperations.WaitForOperationAsync(
                                        client,
                                        accepted,
                                        waitBound,
                                        waitCts.Token
                                    )

                                lock pendingGate (fun () ->
                                    pendingQueue.Enqueue(
                                        {
                                            Session = session
                                            Receipt = accepted
                                            AcceptedAt = acceptedAt
                                            WaitTask = waitTask
                                            WaitCts = waitCts
                                            TurnKey = ""
                                            StreamedPrefix = ""
                                            SettledPrefix = ""
                                        }
                                    ))

                                this.EnsureDrain(cancellationToken)
                            else
                                let error =
                                    match completed.Exception with
                                    | null -> Exception("The prompt failed.")
                                    | aggregate when aggregate.InnerExceptions.Count > 0 ->
                                        aggregate.InnerExceptions[0]
                                    | aggregate -> aggregate :> exn

                                line $"ERROR not-accepted ({error.Message})"
                        finally
                            clearInflight ())
                    |> ignore
                with error ->
                    clearInflight ()
                    line $"ERROR not-accepted ({error.Message})"
        }

    /// Folds follow-up text into the running turn with its own receipt:
    /// Inject appends and folds at the next iteration boundary without
    /// starting an independent turn. The ACCEPTED line names the Inject
    /// receipt; the eventual effect still settles the folded turn once.
    /// Replies never claim independent turns; only explicit
    /// execution-control actions (/abort, /steer Interrupt, /follow
    /// Inject) affect execution.
    /// <param name="session">The session to prompt.</param>
    /// <param name="text">The user text.</param>
    /// <param name="cancellationToken">Abandons the prompt.</param>
    member private this.InjectAsync(session: ReplSession, text: string, cancellationToken: CancellationToken) : Task =
        task {
            this.EnsureAttachment(session, cancellationToken)

            let message: UserMessage option =
                try
                    Some(UserMessage.Text text)
                with error ->
                    line $"ERROR not-accepted ({error.Message})"
                    None

            match message with
            | None -> ()
            | Some userMessage ->
                // Fire-and-forget like the queue path: Inject acceptance
                // may ride behind a running turn, so awaiting would block
                // steering. Inject never claims an independent turn. Marked
                // in flight so /quit waits for the acceptance.
                markInflight ()

                try
                    let promptTask =
                        SessionClientOperations.PromptAsync(
                            client,
                            session.Id,
                            userMessage,
                            DeliveryMode.Inject,
                            cancellationToken
                        )

                    promptTask.ContinueWith(fun (completed: Task<AcceptedOperation>) ->
                        try
                            if completed.IsCompletedSuccessfully then
                                let accepted = completed.Result

                                // Inject never claims an independent turn: the
                                // receipt is accepted control, the folded turn's
                                // settlement is the eventual effect observed on
                                // the attachment.
                                line
                                    $"ACCEPTED op={accepted.Position} kind={accepted.Kind} session={accepted.SessionId} (inject folds; no independent turn)"

                                lock operationGate (fun () -> authoritativeRunning <- true)
                            else
                                let error =
                                    match completed.Exception with
                                    | null -> Exception("The prompt failed.")
                                    | aggregate when aggregate.InnerExceptions.Count > 0 ->
                                        aggregate.InnerExceptions[0]
                                    | aggregate -> aggregate :> exn

                                line $"ERROR not-accepted ({error.Message})"
                        finally
                            clearInflight ())
                    |> ignore
                with error ->
                    clearInflight ()
                    line $"ERROR not-accepted ({error.Message})"
        }

    /// Lists the current session journal positions to branch from: paged
    /// ReadEventsAsync with sequence plus event-type detail.
    /// <param name="session">The session whose journal to list.</param>
    /// <param name="cancellationToken">Abandons the read.</param>
    member private _.TreeAsync(session: ReplSession, cancellationToken: CancellationToken) : Task =
        task {
            try
                let collected = ResizeArray<SessionEvent>()
                let mutable cursor = 0L
                let mutable paging = true

                while paging do
                    let! page =
                        SessionClientOperations.ReadEventsAsync(client, session.Id, cursor, 100, cancellationToken)

                    if isNull (box page) || page.Count = 0 then
                        paging <- false
                    else
                        for evt in page do
                            if not (isNull (box evt)) then
                                collected.Add(evt)

                                if evt.Sequence.HasValue && evt.Sequence.Value > cursor then
                                    cursor <- evt.Sequence.Value

                        if page.Count < 100 then
                            paging <- false

                line $"TREE {collected.Count} events"

                for evt in collected do
                    line (renderTree evt)
            with error ->
                line $"ERROR {error.Message}"
        }

    /// Reports one session's counts and token usage: messages (accepted
    /// prompts plus folded follow-ups, counted from user evidence) and
    /// completed turns over a single journal pass, plus the token sums (the
    /// journal's UsageEvents, if any, with this process's settled-turn
    /// accumulation on top: the runtime journals no usage on this path).
    /// Never prices: the runtime carries no cost by construction. Per-call
    /// tool markers are execution evidence, not prompts, so they never
    /// count as messages.
    /// <param name="session">The session to report on.</param>
    /// <param name="cancellationToken">Abandons the read.</param>
    member private _.SessionAsync(session: ReplSession, cancellationToken: CancellationToken) : Task =
        task {
            try
                let! events = DotExport.readAllEventsAsync client session.Id cancellationToken
                let messages, turns, journalInput, journalOutput = DotExport.summarize events
                let liveInput, liveOutput = readUsage session.Id

                line
                    $"SESSION {session.Id} messages={messages} turns={turns} input-tokens={journalInput + liveInput} output-tokens={journalOutput + liveOutput}"
            with error ->
                line $"ERROR {error.Message}"
        }

    /// Exports one session's journal to a file under the working directory:
    /// JSONL (one $type-polymorphic event per line), or escaped static HTML
    /// for an .html target. Escape outside the working directory is refused;
    /// overwrite mirrors write_file.
    /// <param name="session">The session whose journal to export.</param>
    /// <param name="argument">The file target the user typed.</param>
    /// <param name="cancellationToken">Abandons the export.</param>
    member private _.ExportAsync(session: ReplSession, argument: string, cancellationToken: CancellationToken) : Task =
        task {
            let target = argument.Trim()

            if target = "" then
                line "ERROR /export needs a file: /export <file>"
            else
                match DotExport.resolveExportPath Environment.CurrentDirectory target with
                | Error reason -> line $"ERROR {reason}"
                | Ok path ->
                    try
                        let! events = DotExport.readAllEventsAsync client session.Id cancellationToken

                        if target.EndsWith(".html", StringComparison.OrdinalIgnoreCase) then
                            do! DotExport.writeHtmlAsync path events cancellationToken
                        else
                            do! DotExport.writeJsonlAsync path events cancellationToken

                        line $"EXPORTED {events.Count} events to {path}"
                    with error ->
                        line $"ERROR {error.Message}"
        }

    /// Branches the current session prefix through ForkAsync: the new
    /// session is registered and made current, the source row and journal
    /// are untouched, beyond-tail cursors clamp to the full journal, and a
    /// cursor below the first sequence forks an empty transcript.
    /// <param name="session">The source session.</param>
    /// <param name="argument">The sequence text the user typed.</param>
    /// <param name="cancellationToken">Abandons the fork.</param>
    member private this.ForkAsync(session: ReplSession, argument: string, cancellationToken: CancellationToken) : Task =
        task {
            let text = argument.Trim()

            if text = "" then
                line "ERROR /fork needs a sequence: /fork <sequence>"
            else
                match Int64.TryParse(text) with
                | false, _ -> line $"ERROR /fork needs a sequence: '{text}' is not a number."
                | true, sequence ->
                    try
                        let! forked = SessionClientOperations.ForkAsync(client, session.Id, sequence, cancellationToken)

                        sessions.Add(
                            {
                                Id = forked.Id
                                Title = forked.Title
                                Cursor = 0L
                            }
                        )

                        current <- sessions.Count - 1
                        line $"FORKED {forked.Id} from {session.Id} up-to {sequence}"
                        line $"RESUMED {forked.Id}"
                        // The fork is a new attachment target with its own
                        // cursor from zero; the source cursor is untouched.
                        this.EnsureAttachment(sessions[current], cancellationToken)
                        line $"ATTACHED {forked.Id} cursor=0 (continuous observation)"
                    with error ->
                        line $"ERROR {error.Message}"
        }

    /// Duplicates the active branch into a new session: ForkAsync at the
    /// tail, free because the facade clamps beyond-tail cursors.
    /// <param name="session">The source session.</param>
    /// <param name="cancellationToken">Abandons the fork.</param>
    member private this.CloneAsync(session: ReplSession, cancellationToken: CancellationToken) : Task =
        task {
            try
                let! forked = SessionClientOperations.ForkAsync(client, session.Id, Int64.MaxValue, cancellationToken)

                sessions.Add(
                    {
                        Id = forked.Id
                        Title = forked.Title
                        Cursor = 0L
                    }
                )

                current <- sessions.Count - 1
                line $"FORKED {forked.Id} from {session.Id} up-to tail"
                line $"RESUMED {forked.Id}"
                this.EnsureAttachment(sessions[current], cancellationToken)
                line $"ATTACHED {forked.Id} cursor=0 (continuous observation)"
            with error ->
                line $"ERROR {error.Message}"
        }

    /// Opens a session and makes it current. Permissions stay null so the
    /// session uses the container policy (allow-all by default, ask-all
    /// under --ask): a concrete policy on SessionOptions does not survive
    /// the SQLite JSON round-trip. No timeout is set, so the settle-wait
    /// bound reports a deadline without killing the turn. The session opens
    /// with the current model-carrying agent row, and carries the resolved
    /// context files (SYSTEM.md first, then the AGENTS.md chain) the
    /// runtime re-reads every turn.
    /// <param name="title">The session title.</param>
    /// <param name="cancellationToken">Abandons the open.</param>
    member private this.OpenAsync(title: string, cancellationToken: CancellationToken) : Task =
        task {
            let! agentId = ensureModelAgentAsync agents packages currentModel cancellationToken

            let options = SessionOptions()
            options.Title <- title
            options.HostInstructionFiles <- DotContext.resolveHostInstructionFiles Environment.CurrentDirectory

            let! created = SessionClientOperations.OpenSessionAsync(client, agentId, options, cancellationToken)

            sessions.Add(
                {
                    Id = created.Id
                    Title = created.Title
                    Cursor = 0L
                }
            )

            current <- sessions.Count - 1
            line $"SESSION {created.Id} {created.Title}"
            // Attachment starts on open, including before any prompt.
            this.EnsureAttachment(sessions[current], cancellationToken)

            line
                $"ATTACHED {created.Id} cursor=0 (continuous observation; waits, deadlines, and terminal results never detach)"
        }

    /// Attaches an existing stored session by id or open index. The probe
    /// reads the SQLite journal, so sessions opened by an earlier dot
    /// process attach here and the follow-up turn continues their
    /// context. Attaching to already-running or suspended work starts
    /// continuous observation with no prompt; the target session's own
    /// last actually observed durable cursor is the only resume position.
    /// Detaching or switching releases the obsolete observer promptly
    /// without aborting detached work; reattach recovers retained events,
    /// results, and actual state.
    /// <param name="text">The id or 1-based index.</param>
    /// <param name="cancellationToken">Abandons the probe.</param>
    /// <returns>True when the attach landed.</returns>
    member private this.AttachAsync(text: string, cancellationToken: CancellationToken) : Task<bool> =
        task {
            let found = findSession text

            if found >= 0 then
                current <- found
                do! this.SyncModelAsync(sessions[found].Id, cancellationToken)
                // Switch uses the target session's own cursor; never
                // borrows another session's cursor or skips queued events.
                this.EnsureAttachment(sessions[found], cancellationToken)
                line $"RESUMED {sessions[found].Id}"
                line $"ATTACHED {sessions[found].Id} cursor={sessions[found].Cursor} (continuous observation)"
                return true
            else
                let mutable parsed = Unchecked.defaultof<SessionId>

                if not (SessionId.TryParse(text, &parsed)) then
                    line $"RESUME-FAILED '{text}' is not a session id or open index."
                    return false
                else
                    try
                        // Probe the journal: unknown sessions throw
                        // SessionNotFoundException here.
                        let! _ = SessionClientOperations.ReadEventsAsync(client, parsed, 0L, 1, cancellationToken)

                        match sessions |> Seq.tryFindIndex (fun session -> session.Id.Equals(parsed)) with
                        | Some known -> current <- known
                        | None ->
                            sessions.Add({ Id = parsed; Title = ""; Cursor = 0L })
                            current <- sessions.Count - 1

                        do! this.SyncModelAsync(parsed, cancellationToken)
                        this.EnsureAttachment(sessions[current], cancellationToken)
                        line $"RESUMED {parsed}"
                        line $"ATTACHED {parsed} cursor={sessions[current].Cursor} (continuous observation)"
                        return true
                    with
                    | :? SessionNotFoundException ->
                        line $"RESUME-FAILED no session {parsed} in this process."
                        return false
                    | :? InvalidOperationException as invalid ->
                        line $"UNSUPPORTED-CONTRACT session={parsed} reason={invalid.Message}"
                        line cleanStartHelp
                        return false
        }

    /// Resolves the model the bare /model marker displays: the current
    /// session's stored agent row when it resolves, else the
    /// startup-selected fallback. Never throws: a listing or agent lookup
    /// failure keeps today's marker instead of crashing it (issue 323).
    /// The read goes through the public list operation because the
    /// client's store accessor is internal to the runtime; the agent
    /// lookup reuses the engine's agent store handle.
    /// <param name="sessionId">The session whose stored agent to read.</param>
    /// <param name="cancellationToken">Abandons the lookup.</param>
    /// <returns>The stored agent's model, or the startup-selected model.</returns>
    member private _.StoredModelAsync
        (sessionId: SessionId, cancellationToken: CancellationToken)
        : Task<ModelReference> =
        task {
            try
                let mutable found: Session = Unchecked.defaultof<Session>
                let options = SessionListOptions()
                options.PageSize <- SessionClientListingOperations.MaxPageSize

                let mutable continuation: string | null = null
                let mutable paging = true

                while paging do
                    options.Continuation <- continuation

                    let! page = SessionClientListingOperations.ListSessionsAsync(client, options, cancellationToken)

                    if isNull (box page) || isNull (box page.Items) then
                        paging <- false
                    else
                        for listed in page.Items do
                            if not (isNull (box listed)) && listed.Id.Equals(sessionId) then
                                found <- listed

                        if isNull (box found) && not (isNull (box page.Continuation)) then
                            continuation <- page.Continuation
                        else
                            paging <- false

                if isNull (box found) then
                    return currentModel
                else
                    let! agent = agents.GetAgent(found.Tenant, found.AgentId, cancellationToken)

                    match agent with
                    | null -> return currentModel
                    | resolved -> return resolved.Model
            with _ ->
                return currentModel
        }

    /// Resolves the bare /model display model: the current session's
    /// stored agent when a session is current and its row resolves, else
    /// the startup-selected fallback. Never throws.
    /// <param name="cancellationToken">Abandons the lookup.</param>
    /// <returns>The model the marker names.</returns>
    member private this.DisplayModelAsync(cancellationToken: CancellationToken) : Task<ModelReference> =
        task {
            try
                let session = currentSession ()
                return! this.StoredModelAsync(session.Id, cancellationToken)
            with _ ->
                return currentModel
        }

    /// Best-effort syncs the startup-selected model from the attached
    /// session's stored agent, so the explicit /model reference equality
    /// check stays correct after /resume. Never throws: a lookup failure
    /// leaves the startup value and the display-time read still reports
    /// truth.
    /// <param name="sessionId">The session just attached.</param>
    /// <param name="cancellationToken">Abandons the lookup.</param>
    member private this.SyncModelAsync(sessionId: SessionId, cancellationToken: CancellationToken) : Task =
        task {
            try
                let! stored = this.StoredModelAsync(sessionId, cancellationToken)
                currentModel <- stored
            with _ ->
                ()
        }

    /// Handles one input line. Returns false when the REPL should exit.
    /// End of input drains the in-flight turn first, so a pre-empted turn
    /// settles visibly instead of going silent.
    /// <param name="inputLine">The line read, or null at end of input.</param>
    /// <param name="cancellationToken">Abandons the turn.</param>
    /// <returns>False when the REPL should exit.</returns>
    member private this.HandleAsync(inputLine: string | null, cancellationToken: CancellationToken) : Task<bool> =
        task {
            match inputLine with
            | null ->
                do! this.DrainAsync(cancellationToken)
                return false
            | line -> return! this.HandleLineAsync(line, cancellationToken)
        }

    /// Handles one trimmed input line. Returns false when the REPL should exit.
    /// Public for the fullscreen TUI route hook (issue 332): DotTui feeds
    /// submitted and steered lines here verbatim, so every slash command
    /// routes exactly as in the plain REPL with no forked parser.
    /// <param name="input">The trimmed input line.</param>
    /// <param name="cancellationToken">Abandons the turn.</param>
    /// <returns>False when the REPL should exit.</returns>
    member this.HandleLineAsync(input: string, cancellationToken: CancellationToken) : Task<bool> =
        task {
            let text = input.Trim()

            if text = "" then
                return true
            elif text = "/quit" || text = "/exit" then
                do! this.DrainAsync(cancellationToken)
                // Quit releases observation promptly without aborting:
                // stopping observation never affects execution.
                this.DetachAttachment()
                return false
            elif text = "/help" then
                line commandsUsage
                line waitSemanticsHelp
                line cleanStartHelp
                return true
            elif text = "/compact" then
                try
                    let session = currentSession ()

                    let! outcome = SessionClientOperations.CompactAsync(client, session.Id, cancellationToken)

                    match outcome with
                    | :? SessionCompacted as compacted ->
                        line $"COMPACT completed {compacted.BeforeEstimate}->{compacted.AfterEstimate}"
                    | :? SessionCompactDeferred -> line "COMPACT deferred"
                    | :? SessionCompactFenced -> line "COMPACT fenced"
                    | _ -> line "COMPACT not-needed"
                with error ->
                    line $"ERROR {error.Message}"

                return true
            elif text = "/abort" then
                try
                    let session = currentSession ()

                    let! current = SessionClientOperations.ReadAbortTargetAsync(client, session.Id, cancellationToken)

                    match current with
                    | null -> line "ABORT NoCurrentTurn"
                    | target ->
                        // Only explicit execution-control actions affect
                        // execution: stopping a wait, detaching, or
                        // cancelling observation never aborts. /abort is
                        // explicit control with a durable intent receipt;
                        // terminal settlement is separate and observed on
                        // the attachment.
                        let! receipt =
                            SessionClientOperations.AbortAsync(
                                client,
                                session.Id,
                                target.TurnId,
                                StopCause.ExplicitAbort,
                                "dot /abort",
                                cancellationToken
                            )

                        line
                            $"ABORT {receipt.Outcome}: {receipt.TurnId} (terminal settlement is separate; attachment keeps observing)"
                with error ->
                    line $"ERROR {error.Message}"

                return true
            elif text.StartsWith("/new", StringComparison.Ordinal) then
                let title = text.Substring("/new".Length).Trim()

                try
                    do! this.OpenAsync((if title = "" then "dot" else title), cancellationToken)
                with error ->
                    line $"ERROR {error.Message}"

                return true
            elif text = "/sessions" then
                try
                    // The store-backed listing, not just the in-process
                    // REPL list: the facade page carries every session in
                    // the store with its title and state. The marker names
                    // the REPL's current session.
                    let! page =
                        SessionClientListingOperations.ListSessionsAsync(
                            client,
                            SessionListOptions(),
                            cancellationToken
                        )

                    let items =
                        if isNull (box page) || isNull (box page.Items) then
                            ResizeArray<Session>() :> IReadOnlyList<Session>
                        else
                            page.Items

                    line $"SESSIONS {items.Count}"

                    let currentId =
                        if current >= 0 && current < sessions.Count then
                            sessions[current].Id
                        else
                            Unchecked.defaultof<SessionId>

                    for session in items do
                        if not (isNull (box session)) then
                            let marker = if session.Id.Equals(currentId) then "*" else " "
                            line $"{marker} {session.Id} {session.Title} [{session.State}]"
                with error ->
                    line $"ERROR {error.Message}"

                return true
            elif text.StartsWith("/resume", StringComparison.Ordinal) then
                let target = text.Substring("/resume".Length).Trim()

                try
                    let! _ = this.AttachAsync(target, cancellationToken)
                    ()
                with error ->
                    line $"ERROR {error.Message}"

                return true
            elif text = "/model" then
                try
                    let! display = this.DisplayModelAsync(cancellationToken)
                    line $"MODEL {display.Value}"

                    for option in providerOptions do
                        let marker =
                            if String.Equals(option.Id, display.Provider, StringComparison.OrdinalIgnoreCase) then
                                "*"
                            else
                                " "

                        line $"{marker} {option.Id} default {defaultReferenceText option}"
                with error ->
                    line $"ERROR {error.Message}"

                return true
            elif text = "/agents" then
                let! available = agents.ListAgents(TenantId.Default, cancellationToken)

                for agent in available do
                    if agent.Enabled && not (String.IsNullOrWhiteSpace agent.Description) then
                        line $"AGENT {agent.Name}: {agent.Description}"

                return true
            elif
                text.StartsWith("/model", StringComparison.Ordinal)
                && (text.Length = "/model".Length || Char.IsWhiteSpace(text["/model".Length]))
            then
                let arg = text.Substring("/model".Length).Trim()

                try
                    if arg = "" then
                        let! display = this.DisplayModelAsync(cancellationToken)
                        line $"MODEL {display.Value}"
                    else
                        let flags: (string | null) * (string | null) =
                            if arg.Contains("/") then null, arg else arg, null

                        let reference = selectReference providerOptions (fst flags) (snd flags)

                        if reference.Equals(currentModel) then
                            line $"MODEL already {reference.Value}"
                        else
                            let session = currentSession ()
                            let! agentId = ensureModelAgentAsync agents packages reference cancellationToken

                            let! rebound =
                                SessionClientOperations.SetAgentAsync(client, session.Id, agentId, cancellationToken)

                            currentModel <- reference

                            if rebound.AgentId.Equals(agentId) then
                                line $"MODEL-SWITCHED {reference.Value} applied at once"
                            else
                                line $"MODEL-SWITCHED {reference.Value} pending: applies when the turn settles"
                with error ->
                    line $"ERROR {error.Message}"

                return true
            elif
                text.StartsWith("/steer", StringComparison.Ordinal)
                && (text.Length = "/steer".Length || Char.IsWhiteSpace(text["/steer".Length]))
            then
                let arg = text.Substring("/steer".Length).Trim()

                if arg = "" then
                    line "ERROR /steer needs text: /steer <text>"
                else
                    do! this.EnqueueTurnAsync(currentSession (), arg, DeliveryMode.Interrupt, cancellationToken)

                return true
            elif
                text.StartsWith("/follow", StringComparison.Ordinal)
                && (text.Length = "/follow".Length || Char.IsWhiteSpace(text["/follow".Length]))
            then
                let arg = text.Substring("/follow".Length).Trim()

                if arg = "" then
                    line "ERROR /follow needs text: /follow <text>"
                elif isDrainRunning () then
                    // A live client wait means work may still be live under
                    // the authoritative observation: fold without claiming
                    // an independent turn. Idle folds through the receipt
                    // path with the same Inject kind.
                    do! this.InjectAsync(currentSession (), arg, cancellationToken)
                else
                    do! this.EnqueueTurnAsync(currentSession (), arg, DeliveryMode.Inject, cancellationToken)

                return true
            elif text = "/tree" then
                do! this.TreeAsync(currentSession (), cancellationToken)
                return true
            elif
                text.StartsWith("/fork", StringComparison.Ordinal)
                && (text.Length = "/fork".Length || Char.IsWhiteSpace(text["/fork".Length]))
            then
                let arg = text.Substring("/fork".Length).Trim()
                do! this.ForkAsync(currentSession (), arg, cancellationToken)
                return true
            elif text = "/clone" then
                do! this.CloneAsync(currentSession (), cancellationToken)
                return true
            elif text = "/session" then
                do! this.SessionAsync(currentSession (), cancellationToken)
                return true
            elif
                text.StartsWith("/export", StringComparison.Ordinal)
                && (text.Length = "/export".Length || Char.IsWhiteSpace(text["/export".Length]))
            then
                let arg = text.Substring("/export".Length).Trim()
                do! this.ExportAsync(currentSession (), arg, cancellationToken)
                return true
            elif text.StartsWith("/", StringComparison.Ordinal) then
                // Prompt templates (issue 309): /<name> expands
                // <cwd>/.agent/templates/<name>.md verbatim as the next
                // prompt (idle starts a turn, running queues behind it, like
                // plain input). Known commands win above, so a template only
                // serves names no command owns. Unknown names error listing
                // the available names, keeping the UNKNOWN-COMMAND shape the
                // earlier smokes pin.
                let name =
                    let rest = text.Substring(1)
                    let space = rest.IndexOfAny([| ' '; '\t' |])

                    if space < 0 then
                        rest.Trim()
                    else
                        rest.Substring(0, space).Trim()

                let trailing =
                    let rest = text.Substring(1)

                    if name = "" then
                        rest
                    elif rest.Length > name.Length then
                        rest.Substring(name.Length).Trim()
                    else
                        ""

                if name = "" || trailing <> "" || not (DotTemplates.isValidName name) then
                    line $"UNKNOWN-COMMAND {text}"
                    line commandsUsage

                    let available = DotTemplates.listTemplates Environment.CurrentDirectory

                    match available with
                    | [] -> line "TEMPLATES none"
                    | names ->
                        let joined = String.Join(", ", names)
                        line $"TEMPLATES {joined}"

                    return true
                else
                    match DotTemplates.tryReadTemplate Environment.CurrentDirectory name with
                    | Some content when not (String.IsNullOrWhiteSpace content) ->
                        line $"TEMPLATE {name}"
                        do! this.EnqueueTurnAsync(currentSession (), content, DeliveryMode.Queue, cancellationToken)
                        return true
                    | _ ->
                        line $"UNKNOWN-COMMAND {text}"
                        line commandsUsage

                        let available = DotTemplates.listTemplates Environment.CurrentDirectory

                        match available with
                        | [] -> line "TEMPLATES none"
                        | names ->
                            let joined = String.Join(", ", names)
                            line $"TEMPLATES {joined}"

                        return true
            else
                do! this.EnqueueTurnAsync(currentSession (), text, DeliveryMode.Queue, cancellationToken)
                return true
        }

    /// Runs the REPL until /quit or end of input. The loop stays
    /// foreground while one turn runs in flight: a suspension grace keeps
    /// piped approval answers on the stream reader (the stream owns the
    /// reader while a permission or question pends), and steering lines
    /// are read while the turn still runs.
    /// <param name="resume">The session id to attach at startup, or null to open.</param>
    /// <param name="cancellationToken">Abandons the REPL.</param>
    /// <returns>The process exit code.</returns>
    member this.RunAsync(resume: string | null, cancellationToken: CancellationToken) : Task<int> =
        task {
            line "Dot REPL (SQLite session store: --resume works across processes.)"

            match resume with
            | null -> do! this.OpenAsync("dot", cancellationToken)
            | raw when String.IsNullOrWhiteSpace raw -> do! this.OpenAsync("dot", cancellationToken)
            | raw ->
                let! attached = this.AttachAsync(raw.Trim(), cancellationToken)

                if not attached then
                    do! this.OpenAsync("dot", cancellationToken)

            line commandsUsage

            let mutable go = true

            while go do
                let approvalWait = isDrainRunning () && isApprovalPending ()

                if approvalWait then
                    try
                        do! Task.Delay(50, cancellationToken)
                    with :? OperationCanceledException ->
                        ()

                if cancellationToken.IsCancellationRequested then
                    go <- false
                elif isDrainRunning () && isApprovalPending () then
                    ()
                elif isDrainRunning () then
                    let mutable waited = 0
                    let mutable waiting = true

                    while waiting do
                        if
                            waited >= 300
                            || not (isDrainRunning ())
                            || isApprovalPending ()
                            || cancellationToken.IsCancellationRequested
                        then
                            waiting <- false
                        else
                            try
                                do! Task.Delay(20, cancellationToken)
                            with :? OperationCanceledException ->
                                ()

                            waited <- waited + 20

                    if isApprovalPending () || cancellationToken.IsCancellationRequested then
                        ()
                    else
                        lock lineGate (fun () ->
                            writer.Write("> ")
                            writer.Flush())

                        let! inputLine = reader.ReadLineAsync()
                        let! keepGoing = this.HandleAsync(inputLine, cancellationToken)
                        go <- keepGoing
                else
                    lock lineGate (fun () ->
                        writer.Write("> ")
                        writer.Flush())

                    let! inputLine = reader.ReadLineAsync()
                    let! keepGoing = this.HandleAsync(inputLine, cancellationToken)
                    go <- keepGoing

            return 0
        }
