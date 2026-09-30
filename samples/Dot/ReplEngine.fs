// SPDX-License-Identifier: Apache-2.0
module Dot.ReplEngine

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open Legate

// REPL engine over the session client facade for the dot host: foreground
// prompt loop with one turn in flight, Subscribe streaming to the writer,
// permission/question console replies, and the /new, /sessions, /resume,
// /model, /steer, /follow, /abort, /compact, /tree, /fork, /clone, and
// /quit commands over the SQLite session store. Modeled on
// samples/LegateCli/CliEngine.fs: the settle waiter is queued before the
// prompt lands (a settle with no waiter only records), each event renders
// as a stable single line, and permission/question suspensions are
// answered inline through ReplyAsync. Divergences from CliEngine, all
// required by the SQLite host or this issue: the engine provisions one
// enabled scripted agent per opened session (the SQLite authority check
// rejects turns for missing agents, while CliEngine's InMemory catalog
// authorizes); opened sessions carry a title but no timeout, so the
// settle-wait bound reports a deadline without killing the turn; there is
// no /agent stub; /model switches the session through SetAgentAsync
// against a model-carrying agent row, so the journal transcript and
// workspace binding survive; /steer interrupts (Interrupt), /follow folds
// in (Inject, waiter-free while a turn runs so the folded turn settles
// once), and plain input queues (Queue); /tree pages ReadEventsAsync and
// /fork branches the prefix through ForkAsync (plus a free /clone at the
// tail); unknown commands reprint the command usage; and a settle wait
// that outruns its bound reports DEADLINE while the turn keeps running.
// Transport-agnostic: the host wires the chat client, tools, and policy.
// Reads and writes through the given reader/writer so scripted transports
// drive it without a console.

/// The command usage reprinted on startup and for unknown commands.
let private commandsUsage =
    "Commands: /new [title], /sessions, /resume <id-or-index>, /model [provider[/model]], /steer <text>, /follow <text>, /abort, /compact, /tree, /fork <sequence>, /clone, /quit."

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
let defaultProviderOrder = [ "anthropic"; "openai"; "google" ]

/// The env var carrying the provider key, or null for unknown ids.
let private providerEnvVar (id: string) : string | null =
    match id.ToLowerInvariant() with
    | "anthropic" -> "ANTHROPIC_API_KEY"
    | "openai" -> "OPENAI_API_KEY"
    | "google" -> "GOOGLE_API_KEY"
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
                "No provider is registered (known: anthropic, openai, google). Set ANTHROPIC_API_KEY, OPENAI_API_KEY, or GOOGLE_API_KEY."
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
                "No provider is registered (known: anthropic, openai, google). Set ANTHROPIC_API_KEY, OPENAI_API_KEY, or GOOGLE_API_KEY."
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

/// One queued turn: the session it runs in, the settle waiter queued
/// before its prompt landed, and the waiter CTS the drain disposes.
type private PendingTurn =
    {
        Session: ReplSession
        WaitTask: Task<TurnResult>
        WaitCts: CancellationTokenSource
    }

/// Ensures the model-carrying agent exists in the agent store: the
/// runtime's authority check rejects turns for missing agents. One enabled
/// row per provider/model, shared across sessions: the first session
/// inserts it (expected 0) and later sessions reuse it, so /model switches
/// and restarts converge instead of multiplying rows. The transcript lives
/// on the session row and the workspace binds from it, so both survive a
/// SetAgentAsync rebind.
/// <param name="agents">The agent store.</param>
/// <param name="reference">The model the agent carries.</param>
/// <param name="cancellationToken">Abandons the upsert.</param>
/// <returns>The agent id conversing under the model.</returns>
let private ensureModelAgentAsync
    (agents: IAgentStore)
    (reference: ModelReference)
    (cancellationToken: CancellationToken)
    : Task<AgentId> =
    task {
        let isMatch (agent: Agent) : bool =
            not (isNull (box agent)) && agent.Enabled && agent.Model.Equals(reference)

        let findMatch (listed: IReadOnlyList<Agent>) : Agent option =
            if isNull (box listed) then
                None
            else
                listed |> Seq.tryFind isMatch

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
                    PackageReference = null
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
                        InvalidOperationException($"The agent for model '{reference.Value}' could not be provisioned.")
                    )
    }

/// The REPL engine: drives one current session through prompt, stream,
/// reply, and settle over the given reader/writer.
type Engine
    (
        client: SessionClient,
        agents: IAgentStore,
        reader: System.IO.TextReader,
        writer: System.IO.TextWriter,
        waitBound: TimeSpan,
        initialModel: ModelReference,
        providerOptions: ProviderOption list
    ) =

    do
        ArgumentNullException.ThrowIfNull(client)
        ArgumentNullException.ThrowIfNull(agents)
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
    let lineGate = obj ()
    let pendingGate = obj ()
    let pendingQueue = Queue<PendingTurn>()
    let mutable drainTask: Task option = None
    let approvalGate = obj ()
    let mutable approvalCount = 0

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

    /// Renders one journaled event with the given line prefix.
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

    /// Answers one permission request from the console.
    /// <param name="asked">The pending permission request.</param>
    /// <param name="cancellationToken">Abandons the reply.</param>
    let answerPermission (asked: PermissionRequestedEvent) (cancellationToken: CancellationToken) : Task =
        task {
            markApproval ()

            try
                line $"PERMISSION tool={asked.ToolName} id={asked.RequestId} [a]llow once, allow for [s]ession, [d]eny:"

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

                let! _ =
                    SessionClientOperations.ReplyAsync(
                        client,
                        asked.SessionId,
                        PermissionDecision(asked.RequestId, decision),
                        cancellationToken
                    )

                ()
            finally
                clearApproval ()
        }

    /// Answers one agent question from the console.
    /// <param name="asked">The pending question.</param>
    /// <param name="cancellationToken">Abandons the reply.</param>
    let answerQuestion (asked: QuestionAskedEvent) (cancellationToken: CancellationToken) : Task =
        task {
            markApproval ()

            try
                line $"QUESTION id={asked.QuestionId}: {asked.Question}"
                line "ANSWER:"

                let! rawAnswer = reader.ReadLineAsync()

                let answer =
                    match rawAnswer with
                    | null -> ""
                    | text -> text

                let! _ =
                    SessionClientOperations.ReplyAsync(
                        client,
                        asked.SessionId,
                        QuestionAnswer(asked.QuestionId, answer),
                        cancellationToken
                    )

                ()
            finally
                clearApproval ()
        }

    /// Streams one turn's events until the subscriber is cancelled,
    /// answering permission requests and questions inline.
    /// <param name="session">The session streaming.</param>
    /// <param name="cancellationToken">Stops the stream.</param>
    member private _.StreamAsync(session: ReplSession, cancellationToken: CancellationToken) : Task =
        task {
            let stream =
                SessionClientOperations.Subscribe(client, session.Id, session.Cursor, cancellationToken)

            let enumerator = stream.GetAsyncEnumerator(cancellationToken)

            try
                let mutable go = true

                while go do
                    try
                        let! has = enumerator.MoveNextAsync().AsTask()

                        if not has then
                            go <- false
                        else
                            let evt = enumerator.Current

                            if not (isNull (box evt)) then
                                if evt.Sequence.HasValue && evt.Sequence.Value > session.Cursor then
                                    session.Cursor <- evt.Sequence.Value

                                line (renderEvent evt)

                                match evt with
                                | :? PermissionRequestedEvent as asked when not (isNull (box asked)) ->
                                    do! answerPermission asked cancellationToken
                                | :? QuestionAskedEvent as asked when not (isNull (box asked)) ->
                                    do! answerQuestion asked cancellationToken
                                | _ -> ()
                    with :? OperationCanceledException ->
                        go <- false
            finally
                try
                    enumerator.DisposeAsync().AsTask() |> ignore
                with _ ->
                    ()
        }

    /// Runs one queued turn: streams its events from the cursor, awaits
    /// its waiter, and prints the settle. A settle wait that outruns its
    /// bound reports DEADLINE while the turn keeps running. The stream is
    /// cancelled on settle and joined best-effort, so a stuck stream never
    /// blocks the drain.
    /// <param name="pending">The queued turn.</param>
    /// <param name="cancellationToken">Abandons the drain.</param>
    member private this.RunPendingAsync(pending: PendingTurn, cancellationToken: CancellationToken) : Task =
        task {
            use streamCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken)
            let stream = this.StreamAsync(pending.Session, streamCts.Token)

            try
                try
                    let! result = pending.WaitTask
                    line $"RESULT {result.Status}"

                    if not (String.IsNullOrEmpty result.AssistantText) then
                        line result.AssistantText

                    line "END-RESULT"
                with
                | :? DeadlineExceededException as exceeded -> line $"DEADLINE {exceeded.Message}"
                | :? OperationCanceledException -> ()
            finally
                try
                    pending.WaitCts.Dispose()
                with _ ->
                    ()

                try
                    streamCts.Cancel()
                with _ ->
                    ()

                try
                    stream.Wait(TimeSpan.FromSeconds 5.0) |> ignore
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

    /// Waits for the in-flight drain to empty: /quit and end-of-input exit
    /// only after every queued turn settles visibly.
    /// <param name="cancellationToken">Abandons the wait.</param>
    member private _.DrainAsync(_cancellationToken: CancellationToken) : Task =
        task {
            let running: Task option = lock pendingGate (fun () -> drainTask)

            match running with
            | None -> ()
            | Some active ->
                try
                    do! active
                with _ ->
                    ()
        }

    /// Prompts with its own waiter: queues the settle waiter before the
    /// prompt lands (a settle with no waiter only records), appends the
    /// turn to the drain, and returns to the foreground loop at once.
    /// <param name="session">The session to prompt.</param>
    /// <param name="text">The user text.</param>
    /// <param name="delivery">How the message is delivered to a running turn.</param>
    /// <param name="cancellationToken">Abandons the turn.</param>
    member private this.EnqueueTurnAsync
        (session: ReplSession, text: string, delivery: DeliveryMode, cancellationToken: CancellationToken)
        : Task =
        task {
            // Queue the waiter and the drain entry synchronously, then send
            // the prompt without awaiting it: PromptAsync only answers once
            // the actor accepts, which rides behind a running turn, so an
            // await here would block the foreground loop until the turn
            // settles and mid-turn steering could never land in time. The
            // waiter is already queued before the prompt is sent, so the
            // pre-empted turn still settles visibly.
            let waitCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken)

            let waitTask =
                SessionClientOperations.WaitForSettleAsync(client, session.Id, waitBound, waitCts.Token)

            lock pendingGate (fun () ->
                pendingQueue.Enqueue(
                    {
                        Session = session
                        WaitTask = waitTask
                        WaitCts = waitCts
                    }
                ))

            this.EnsureDrain(cancellationToken)

            let message: UserMessage option =
                try
                    Some(UserMessage.Text text)
                with error ->
                    line $"ERROR {error.Message}"
                    None

            match message with
            | None ->
                try
                    waitCts.Cancel()
                with _ ->
                    ()
            | Some userMessage ->
                try
                    let promptTask =
                        SessionClientOperations.PromptAsync(
                            client,
                            session.Id,
                            userMessage,
                            delivery,
                            cancellationToken
                        )

                    promptTask.ContinueWith(fun (completed: Task<InboxEntry>) ->
                        if not completed.IsCompletedSuccessfully then
                            let error =
                                match completed.Exception with
                                | null -> Exception("The prompt failed.")
                                | aggregate when aggregate.InnerExceptions.Count > 0 -> aggregate.InnerExceptions[0]
                                | aggregate -> aggregate :> exn

                            try
                                waitCts.Cancel()
                            with _ ->
                                ()

                            line $"ERROR {error.Message}")
                    |> ignore
                with error ->
                    try
                        waitCts.Cancel()
                    with _ ->
                        ()

                    line $"ERROR {error.Message}"
        }

    /// Folds follow-up text into the running turn without its own waiter:
    /// Inject appends and folds at the next iteration boundary without
    /// starting a new turn, so the in-flight waiter settles the folded
    /// turn once as Completed.
    /// <param name="session">The session to prompt.</param>
    /// <param name="text">The user text.</param>
    /// <param name="cancellationToken">Abandons the prompt.</param>
    member private _.InjectAsync(session: ReplSession, text: string, cancellationToken: CancellationToken) : Task =
        task {
            let message: UserMessage option =
                try
                    Some(UserMessage.Text text)
                with error ->
                    line $"ERROR {error.Message}"
                    None

            match message with
            | None -> ()
            | Some userMessage ->
                try
                    let promptTask =
                        SessionClientOperations.PromptAsync(
                            client,
                            session.Id,
                            userMessage,
                            DeliveryMode.Inject,
                            cancellationToken
                        )

                    promptTask.ContinueWith(fun (completed: Task<InboxEntry>) ->
                        if not completed.IsCompletedSuccessfully then
                            let error =
                                match completed.Exception with
                                | null -> Exception("The prompt failed.")
                                | aggregate when aggregate.InnerExceptions.Count > 0 -> aggregate.InnerExceptions[0]
                                | aggregate -> aggregate :> exn

                            line $"ERROR {error.Message}")
                    |> ignore
                with error ->
                    line $"ERROR {error.Message}"
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

    /// Branches the current session prefix through ForkAsync: the new
    /// session is registered and made current, the source row and journal
    /// are untouched, beyond-tail cursors clamp to the full journal, and a
    /// cursor below the first sequence forks an empty transcript.
    /// <param name="session">The source session.</param>
    /// <param name="argument">The sequence text the user typed.</param>
    /// <param name="cancellationToken">Abandons the fork.</param>
    member private _.ForkAsync(session: ReplSession, argument: string, cancellationToken: CancellationToken) : Task =
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
                    with error ->
                        line $"ERROR {error.Message}"
        }

    /// Duplicates the active branch into a new session: ForkAsync at the
    /// tail, free because the facade clamps beyond-tail cursors.
    /// <param name="session">The source session.</param>
    /// <param name="cancellationToken">Abandons the fork.</param>
    member private _.CloneAsync(session: ReplSession, cancellationToken: CancellationToken) : Task =
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
            with error ->
                line $"ERROR {error.Message}"
        }

    /// Opens a session and makes it current. Permissions stay null so the
    /// session uses the container policy (allow-all by default, ask-all
    /// under --ask): a concrete policy on SessionOptions does not survive
    /// the SQLite JSON round-trip. No timeout is set, so the settle-wait
    /// bound reports a deadline without killing the turn. The session opens
    /// with the current model-carrying agent row.
    /// <param name="title">The session title.</param>
    /// <param name="cancellationToken">Abandons the open.</param>
    member private _.OpenAsync(title: string, cancellationToken: CancellationToken) : Task =
        task {
            let! agentId = ensureModelAgentAsync agents currentModel cancellationToken

            let options = SessionOptions()
            options.Title <- title

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
        }

    /// Attaches an existing stored session by id or open index. The probe
    /// reads the SQLite journal, so sessions opened by an earlier dot
    /// process attach here and the follow-up turn continues their
    /// context.
    /// <param name="text">The id or 1-based index.</param>
    /// <param name="cancellationToken">Abandons the probe.</param>
    /// <returns>True when the attach landed.</returns>
    member private _.AttachAsync(text: string, cancellationToken: CancellationToken) : Task<bool> =
        task {
            let found = findSession text

            if found >= 0 then
                current <- found
                line $"RESUMED {sessions[found].Id}"
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

                        line $"RESUMED {parsed}"
                        return true
                    with :? SessionNotFoundException ->
                        line $"RESUME-FAILED no session {parsed} in this process."
                        return false
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
    /// <param name="input">The trimmed input line.</param>
    /// <param name="cancellationToken">Abandons the turn.</param>
    /// <returns>False when the REPL should exit.</returns>
    member private this.HandleLineAsync(input: string, cancellationToken: CancellationToken) : Task<bool> =
        task {
            let text = input.Trim()

            if text = "" then
                return true
            elif text = "/quit" || text = "/exit" then
                do! this.DrainAsync(cancellationToken)
                return false
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

                    do!
                        SessionClientOperations.AbortAsync(
                            client,
                            session.Id,
                            StopCause.ExplicitAbort,
                            "dot /abort",
                            cancellationToken
                        )

                    line "ABORTED"
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
                    line $"MODEL {currentModel.Value}"

                    for option in providerOptions do
                        let marker =
                            if String.Equals(option.Id, currentModel.Provider, StringComparison.OrdinalIgnoreCase) then
                                "*"
                            else
                                " "

                        line $"{marker} {option.Id} default {defaultReferenceText option}"
                with error ->
                    line $"ERROR {error.Message}"

                return true
            elif
                text.StartsWith("/model", StringComparison.Ordinal)
                && (text.Length = "/model".Length || Char.IsWhiteSpace(text["/model".Length]))
            then
                let arg = text.Substring("/model".Length).Trim()

                try
                    if arg = "" then
                        line $"MODEL {currentModel.Value}"
                    else
                        let flags: (string | null) * (string | null) =
                            if arg.Contains("/") then null, arg else arg, null

                        let reference = selectReference providerOptions (fst flags) (snd flags)

                        if reference.Equals(currentModel) then
                            line $"MODEL already {reference.Value}"
                        else
                            let session = currentSession ()
                            let! agentId = ensureModelAgentAsync agents reference cancellationToken

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
            elif text.StartsWith("/", StringComparison.Ordinal) then
                line $"UNKNOWN-COMMAND {text}"
                line commandsUsage
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
