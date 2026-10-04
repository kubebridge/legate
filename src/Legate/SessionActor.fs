// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open Akka.Actor
open Akka.FSharp
open Microsoft.Extensions.AI
open Microsoft.Extensions.Logging

// Session actor: the Akka.FSharp child owning one session's
// Idle -> Running -> (Idle | WaitingForInput | Closed) state machine with a
// durable ISessionStore inbox, driving TurnLoop.runAsync for Queue delivery
// only. The LocalActorSystem router spawns one of these per session id.
//
// Store-first persistence: Prompt appends the inbox entry and persists the
// Running state before the turn starts; settle consumes the completed entry
// before draining the next one or returning to Idle. Consumption at settle
// (not at start) makes a mid-turn crash redeliver: the entry is still
// pending, so restart rebuilds Idle with the work intact (at-least-once).
// An observed in-process turn fault consumes its entry instead, so a poison
// message cannot hot-loop; retry policy belongs to later issues.
//
// Threading: the Akka.FSharp actor computation can only receive messages,
// never await tasks, so the fast store metadata ops are awaited
// synchronously on the actor thread. That preserves the actor's
// single-threaded sequencing guarantee (a Close can never slip between a
// settle's consume and its Idle write), which piped follow-up tasks would
// lose. Only the unbounded turn execution leaves the thread: it runs
// fire-and-forget and pipes its outcome back as a message, per the actor
// rules. A future clustered or high-throughput redesign can move the
// metadata ops to stashed follow-ups with epoch fencing.
//
// Out of scope here, owned elsewhere: claim tokens and fencing (#33),
// Inject/Interrupt delivery (#34), and suspend triggers with reply matching
// (#36: WaitingForInput is modelled and preserved, never entered). Abort is
// a first-class turn-level verb here (#35): AbortSession carries a typed
// stop cause, exactly one of settlement and stop wins per turn, and Close
// stays lifecycle-only (it wires turn cancellation without recording a
// stop cause).

// ──────────────────────────────────────────────────────────────────────────
// Protocol

/// How the session actor answers a prompt. The actor never throws
/// InvalidSessionStateException: a Closed-state prompt is rejected with the
/// current state and the client boundary maps it to the exception.
type internal SessionPromptReply =

    /// The prompt was accepted: the entry was appended to the durable inbox
    /// (and a turn started when the session was Idle).
    | PromptAccepted of entry: InboxEntry

    /// The prompt arrived while the session was Closed; nothing was
    /// appended. Carries the state the session was in.
    | PromptRejected of state: SessionState

/// How the session actor answers an on-demand compaction. The actor never
/// throws InvalidSessionStateException: a Closed-state compact is rejected
/// with the current state and the client boundary maps it to the exception.
type internal SessionCompactReply =

    /// The Idle session compacted now without starting a turn: the
    /// before/after estimates from the CompactionOutcome.
    | CompactCompleted of beforeEstimate: int64 * afterEstimate: int64

    /// Nothing compacted and no summariser call ran: the session was Idle
    /// but under threshold (or had nothing replaceable), WaitingForInput (a
    /// suspended turn owns the history), in an out-of-range state,
    /// unconfigured, cancelled, or the summariser failed and the session
    /// continues uncompacted (the CompactionFailedEvent carries the reason).
    | CompactNotNeeded

    /// The session was Running: the one-shot force flag is armed and the
    /// running turn's force-aware boundary hook compacts at the next
    /// iteration boundary, bypassing the threshold once.
    | CompactDeferred

    /// The actor lost its claim before the journal write landed, so it
    /// journaled nothing: the takeover winner owns the session.
    | CompactFenced

    /// The compact arrived while the session was Closed; nothing ran.
    /// Carries the state the session was in.
    | CompactRejected of state: SessionState

/// The session actor protocol. QueuePrompt, InjectPrompt,
/// InterruptPrompt, CloseSession, and GetSnapshot
/// are answered to the sender; the SessionTurnSettled and SessionTurnFaulted completions
/// are one-way (Tell) from the turn task back to the actor.
type internal SessionActorMessage =

    /// Queue a user message: appends to the durable inbox, starts a turn
    /// when Idle, waits when Running or WaitingForInput, rejects when
    /// Closed. Carries Queue delivery only; Inject and Interrupt delivery
    /// arrive through InjectPrompt and InterruptPrompt. Answered with
    /// <see cref="T:Legate.SessionPromptReply" />.
    | QueuePrompt of payload: InboxPayload * cancellationToken: CancellationToken

    /// Inject a user message into the running turn: appends to the durable
    /// inbox with Inject delivery, starts a turn when Idle, folds at the
    /// next iteration boundary when Running through the runner's drain
    /// hooks, waits when WaitingForInput, rejects when Closed. Never
    /// aborts the running turn. Answered with
    /// <see cref="T:Legate.SessionPromptReply" />.
    | InjectPrompt of payload: InboxPayload * cancellationToken: CancellationToken

    /// Interrupt the running turn with a user message: appends to the
    /// durable inbox with Interrupt delivery, starts a turn when Idle,
    /// aborts the running turn under ExplicitAbort and drains the
    /// Interrupt entry first when Running, waits when WaitingForInput,
    /// rejects when Closed. Answered with
    /// <see cref="T:Legate.SessionPromptReply" />.
    | InterruptPrompt of payload: InboxPayload * cancellationToken: CancellationToken

    /// Close the session: aborts the running turn first through
    /// cancellation only, then closes idempotently in the store. Valid in
    /// every state. Answered with the stored session.
    | CloseSession of cancellationToken: CancellationToken

    /// Abort the running turn under a typed stop cause: records the pending
    /// stop, cancels the turn, and settles Aborted under the winning cause
    /// when the turn reports back. Exactly one of settlement and stop wins:
    /// a completion that landed first stands and the abort is a no-op, a
    /// stop that landed first maps even a successful completion to Aborted,
    /// and a second abort keeps the first cause. Idle is a no-op returning
    /// the current state, as is WaitingForInput (suspended turns belong to
    /// issue 36: nothing runs to abort). Only abort-family causes
    /// (ExplicitAbort, HostShutdown) act; anything else is a no-op. Answered
    /// with <see cref="T:Legate.SessionSnapshot" />.
    | AbortSession of cause: StopCause * reason: string * cancellationToken: CancellationToken
    /// Exact-target durable-intent wake hint. It never authorizes stop without provider evidence.
    | ObserveHostAbort of tenant: TenantId * sessionId: SessionId * targetTurnId: TurnId

    /// Compact the session on demand: Idle replays the journal into a
    /// history and compacts now without starting a turn, Running arms the
    /// one-shot force flag the turn's force-aware boundary hook honors at
    /// the next iteration (threshold bypassed, single-pass guard kept),
    /// WaitingForInput is a no-op (suspended turns belong to issue 36:
    /// nothing runs to compact), and Closed rejects. Answered with
    /// <see cref="T:Legate.SessionCompactReply" />.
    | CompactSession of cancellationToken: CancellationToken

    /// Reads the actor's current state plus the store's pending inbox count.
    /// Answered with <see cref="T:Legate.SessionSnapshot" />.
    | GetSnapshot

    /// The running turn settled with a TurnResult. Consumes the completed
    /// entry, then drains the next pending Queue entry or returns to Idle.
    /// Stale completions (arrived after Close) are ignored.
    | SessionTurnSettled of entry: InboxEntry * result: TurnResult

    /// The running turn faulted (cancellation, or a runner failure).
    /// Consumes the faulted entry so a poison message cannot hot-loop, then
    /// drains the next pending Queue entry or returns to Idle. Faults
    /// arriving after Close are ignored.
    | SessionTurnFaulted of entry: InboxEntry * error: Exception

/// The actor's observable state: its in-memory lifecycle state, the store's
/// pending inbox count, and the running turn's entry position when a turn is
/// in flight.
type internal SessionSnapshot =
    {
        /// The session the snapshot was taken for.
        SessionId: SessionId
        /// The actor's current lifecycle state.
        State: SessionState
        /// How many inbox entries the store reports pending.
        PendingCount: int
        /// The position of the entry the running turn executes, or None
        /// when no turn is in flight.
        RunningPosition: int64 option
        /// The pending suspend request id while WaitingForInput, or null
        /// when no turn is suspended. The journal is the normative resume
        /// source; this is the observable projection of it.
        PendingRequestId: string | null
    }

/// A turn in flight: the inbox entry it executes plus the source Close
/// cancels to abort it. The source is cancelled but never disposed on the
/// abort path: disposal races in-flight token observations, and the source
/// is reclaimed by the GC instead.
type private RunningTurn =
    {
        /// The inbox entry the turn executes.
        Entry: InboxEntry
        /// The turn the attempt runs as (issue 289): the CurrentTurnId
        /// snapshot at start, or a fresh mint when the row carries none.
        /// The settle choke points journal the terminal event under it.
        TurnId: TurnId
        /// The source Close cancels to abort it.
        Cts: CancellationTokenSource
    }

/// What an on-demand compact needs outside a turn (issue 46): the same
/// summariser wiring a per-turn CompactionWiring carries, plus the journal
/// to replay and the one-shot force cell the turn's force-aware boundary
/// hooks share. The Idle path replays the journal into a history through
/// Transcripts.readTranscript and Compaction.messagesFromCells, then runs
/// the merged runner; the Running path only arms Force.
type internal CompactDeps =
    {
        /// LLM settings carrying the Compaction model override and the
        /// CompactionKeepMessages tail. Never null.
        Llm: LlmOptions
        /// The tokens held back beyond the reserved output when deriving
        /// the threshold.
        ReservedBufferTokens: int
        /// The session's model: the threshold lookup and default
        /// summariser model.
        SessionModel: ModelReference
        /// Resolves the session model to its catalog entry, or null
        /// when the host keeps no catalog (the threshold falls back to
        /// the catalog defaults).
        Catalog: ILlmModelCatalog | null
        /// The chat client the summariser call runs against. Never null.
        Client: IChatClient
        /// Receives the summariser usage checkpoint, or null.
        Observer: IUsageObserver | null
        /// Authorises the summariser model call, or null.
        Policy: IModelPolicy | null
        /// The journal the Idle path replays and appends to. Never null.
        EventStore: ISessionEventStore
        /// The claim token fencing the journal appends. Never null.
        JournalToken: string
        /// The one-shot force cell the turn's force-aware boundary hooks
        /// share: arming fires compaction at the next boundary exactly
        /// once. Never null.
        Force: Compaction.CompactForce
    }

/// What a session actor is built from: the durable store, the tenant and
/// session it owns, and the turn runner it drives per Queue entry. The
/// default runner (<see cref="M:Legate.SessionActor.createTurnRunner" />)
/// calls TurnLoop.runAsync; tests inject scripted runners.
type internal SessionActorProps =
    {
        /// The durable store the inbox and lifecycle state persist through.
        Store: ISessionStore
        /// The tenant the session belongs to.
        Tenant: TenantId
        /// The session the actor owns.
        SessionId: SessionId
        /// Runs one turn for an inbox entry. Never null.
        RunTurn: InboxEntry -> CancellationToken -> Task<TurnResult>
        /// Observes each settled turn result (the carried result, or the
        /// abort-mapped Aborted result when a stop won) on the actor thread,
        /// or None for no observation. Guarded: a throwing observer never
        /// kills the actor.
        OnTurnSettled: (TurnResult -> unit) option
        /// Observes each Inject user message the running turn folds, as a
        /// UserMessageEvent carrying the running (injecting) turn's id, or
        /// None for no observation. The runner calls it once per folded
        /// entry, after the message is appended to history and before the
        /// consume hook. Guarded: a throwing observer never kills the turn.
        OnInjectJournaled: (UserMessageEvent -> unit) option
        /// Carries the on-demand compaction wiring (summariser client,
        /// model, catalog, journal, and the one-shot force cell the turn's
        /// force-aware boundary hooks share), or None when the host did not
        /// configure compaction: CompactSession then answers without
        /// compacting and never starts a turn.
        Compact: CompactDeps option
        /// The logger the actor reports prompt/reply/settle/suspend points
        /// to, or null for no logging (the CustomToolSource precedent: a
        /// null logger resolves to the NullLogger). Internal-only wiring.
        Logger: Microsoft.Extensions.Logging.ILogger | null
    }

// ──────────────────────────────────────────────────────────────────────────
// Behaviour

/// The fenced journal appends for facade-driven turns (issue 321):
/// testable wrappers around the fenced writer mirroring
/// journalTurnStartedAsync. Extracted so wrapper-level fencing tests drive
/// the same mapping the live actor uses: a stale-token rejection raises
/// TurnLeaseLostException so the takeover loser stops with zero effects; a
/// failed write returns silently and the turn proceeds (best-effort
/// observability).
module internal SessionJournal =

    /// Journals one cumulative usage checkpoint for a facade-driven turn
    /// (issue 321): a UsageEvent carrying the turn's accumulated totals
    /// under the given journal token through the fenced writer.
    /// <param name="eventStore">The journal the checkpoint appends to.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session whose journal appends.</param>
    /// <param name="token">The journal token snapshot fencing the write.</param>
    /// <param name="turnId">The turn the usage was checkpointed for.</param>
    /// <param name="inputTokens">The input tokens the turn had consumed.</param>
    /// <param name="outputTokens">The output tokens the turn had produced.</param>
    /// <param name="cancellationToken">Abandons the append.</param>
    let journalUsageAsync
        (eventStore: ISessionEventStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (token: string)
        (turnId: TurnId)
        (inputTokens: int64)
        (outputTokens: int64)
        (cancellationToken: CancellationToken)
        : Task<unit> =
        task {
            let checkpoint =
                UsageEvent(
                    sessionId,
                    turnId,
                    Unchecked.defaultof<Nullable<int64>>,
                    DateTimeOffset.UtcNow,
                    inputTokens,
                    outputTokens
                )
                :> SessionEvent

            let batch =
                ResizeArray<SessionEvent>([| checkpoint |]) :> IReadOnlyList<SessionEvent>

            match! JournalWriter.appendWithTokenAsync eventStore tenant sessionId token batch cancellationToken with
            | JournalWriter.JournalAppended _ -> ()
            | JournalWriter.JournalRejected _ -> return raise (TurnLoop.TurnLeaseLostException())
            | JournalWriter.JournalFailed _ -> ()
        }

    /// Journals one skill load for a facade-driven turn (issue 321): the
    /// SkillLoadedEvent the SkillTool's onLoaded carried, re-keyed under
    /// the running turn when the tool was bound with a stale id, through
    /// the fenced writer.
    /// <param name="eventStore">The journal the event appends to.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session whose journal appends.</param>
    /// <param name="token">The journal token snapshot fencing the write.</param>
    /// <param name="turnId">The running turn the load ran inside.</param>
    /// <param name="loaded">The loaded event the tool reported.</param>
    /// <param name="cancellationToken">Abandons the append.</param>
    let journalSkillLoadedAsync
        (eventStore: ISessionEventStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (token: string)
        (turnId: TurnId)
        (loaded: SkillLoadedEvent)
        (cancellationToken: CancellationToken)
        : Task<unit> =
        task {
            let companions =
                if isNull (box loaded) || isNull (box loaded.Companions) then
                    ResizeArray<string>() :> IReadOnlyList<string>
                else
                    loaded.Companions

            let skillName =
                if isNull (box loaded) || isNull (box loaded.SkillName) then
                    ""
                else
                    loaded.SkillName

            let event =
                SkillLoadedEvent(
                    sessionId,
                    turnId,
                    Unchecked.defaultof<Nullable<int64>>,
                    DateTimeOffset.UtcNow,
                    skillName,
                    companions
                )
                :> SessionEvent

            let batch = ResizeArray<SessionEvent>([| event |]) :> IReadOnlyList<SessionEvent>

            match! JournalWriter.appendWithTokenAsync eventStore tenant sessionId token batch cancellationToken with
            | JournalWriter.JournalAppended _ -> ()
            | JournalWriter.JournalRejected _ -> return raise (TurnLoop.TurnLeaseLostException())
            | JournalWriter.JournalFailed _ -> ()
        }

/// The session actor behaviour and its client boundary. Internal so no Akka
/// type ever crosses the public API.
module internal SessionActor =

    /// How long a client-boundary Ask waits for the actor's reply before
    /// the call fails. The actor answers from memory plus fast store reads,
    /// so this only fires when the system is wedged.
    let askTimeout = TimeSpan.FromSeconds 10.0

    /// Reason carried by TurnAborted when an Interrupt prompt pre-empts the
    /// running turn. Never contains secrets or tool arguments.
    [<Literal>]
    let InterruptReason = "The turn was interrupted by a new message."

    /// Awaits a fast store metadata task synchronously on the actor thread.
    /// The actor computation cannot bind tasks (only mailbox receives), and
    /// blocking here preserves the single-threaded sequencing the
    /// store-first transitions rely on. The unbounded turn execution never
    /// blocks: it runs fire-and-forget and pipes its outcome back.
    /// <param name="task">The store task to await.</param>
    /// <returns>The task's result.</returns>
    let private awaitTask<'T> (task: Task<'T>) : 'T = task.GetAwaiter().GetResult()

    /// Selects the drainable entries from a pending read in position order,
    /// in two tiers: Interrupt user messages first in position order, then
    /// Queue-plus-Inject user messages in position order. Reply payloads
    /// never drain to a new turn: they resume a suspended turn instead, so
    /// they stay pending for their owning issue, as does anything that is
    /// not a user message. Null entries and a null read result are treated
    /// as empty.
    /// <param name="entries">The pending read result.</param>
    /// <returns>The drainable entries, Interrupt tier first.</returns>
    let private selectDrainableEntries (entries: IReadOnlyList<InboxEntry>) : InboxEntry list =
        if isNull (box entries) then
            []
        else
            let userMessages =
                entries
                |> Seq.filter (fun entry -> not (isNull (box entry)) && (entry.Payload :? UserMessagePayload))
                |> Seq.sortBy (fun entry -> entry.Position)
                |> List.ofSeq

            let interrupts =
                userMessages
                |> List.filter (fun entry -> entry.Delivery = DeliveryMode.Interrupt)

            let queued =
                userMessages
                |> List.filter (fun entry ->
                    entry.Delivery = DeliveryMode.Queue || entry.Delivery = DeliveryMode.Inject)

            interrupts @ queued

    /// Rebuilds the actor's starting state from the store: the stored
    /// lifecycle state. A stored Running state means the previous owner
    /// died mid-turn (this issue has no lease or fencing to decide
    /// otherwise, those belong to #33), so it is released back to Idle in
    /// the store and the still-pending entry redelivers on the next drain.
    /// A missing session row starts as an empty Idle shell: the client
    /// boundary rejects every mutation for unknown sessions, so the shell
    /// can never persist phantom work. An out-of-range stored state is
    /// preserved verbatim and treated as append-only by the loop.
    /// <param name="props">The session actor dependencies.</param>
    /// <returns>The recovered lifecycle state.</returns>
    let private recover (props: SessionActorProps) : SessionState =
        let found =
            awaitTask (props.Store.GetSession(props.Tenant, props.SessionId, CancellationToken.None))

        match found with
        | null -> SessionState.Idle
        | session ->
            match session.State with
            | SessionState.Running ->
                awaitTask (
                    props.Store.UpdateSessionState(
                        props.Tenant,
                        props.SessionId,
                        SessionState.Idle,
                        CancellationToken.None
                    )
                )
                |> ignore

                SessionState.Idle
            | SessionState.Idle -> SessionState.Idle
            | SessionState.WaitingForInput -> SessionState.WaitingForInput
            | SessionState.Closed -> SessionState.Closed
            | unknown -> unknown

    /// Reads the store's pending inbox count for a snapshot. A null read
    /// result counts as empty, as does a session row that does not exist
    /// yet (a resolved-but-never-opened session reports an empty Idle
    /// shell; the client boundary rejects its mutations).
    /// <param name="props">The session actor dependencies.</param>
    /// <returns>How many inbox entries are pending.</returns>
    let private pendingCount (props: SessionActorProps) : int =
        try
            let pending =
                awaitTask (props.Store.ReadPendingInbox(props.Tenant, props.SessionId, CancellationToken.None))

            if isNull (box pending) then 0 else pending.Count
        with :? SessionNotFoundException ->
            0

    /// Store-backed Inject fold wiring for one session: what the
    /// Inject-aware turn runner closes over to fold Inject entries at
    /// iteration boundaries (issue 34 over issue 41's drain hooks). The
    /// drain reads the session's pending inbox, the journal sink observes
    /// one UserMessageEvent per folded entry under the running turn's id,
    /// and the consume marks each folded entry so it never refolds.
    type internal InjectFoldWiring =
        {
            /// The durable store the inbox persists through.
            Store: ISessionStore
            /// The tenant the session belongs to.
            Tenant: TenantId
            /// The session the wiring drains and consumes for.
            SessionId: SessionId
            /// Observes each folded Inject entry as a UserMessageEvent, or
            /// None for no observation. Guarded by the runner: a throwing
            /// observer never kills the turn.
            JournalEvent: (UserMessageEvent -> unit) option
        }

    /// Builds a Queue turn runner over TurnLoop.runAsync: one ChatRole.User
    /// history message from the entry's parts and no Inject fold, running
    /// under the given lease hook and the given deadline seam. With an
    /// Inject wiring the runner instead calls
    /// TurnLoop.runAsyncWithInjects: the turn mints one TurnId at
    /// invocation (the invocation runs synchronously inside the actor's
    /// startTurn, so the id is the running turn's), folds pending Inject
    /// entries at each iteration boundary, journals each folded entry once
    /// as a UserMessageEvent under that id, and marks it consumed before
    /// the next provider call. The drain and consume block the turn thread
    /// on the store (the hooks are synchronous): the consume must land
    /// before the turn reports back, or the settle drain would redeliver
    /// the folded entry as a new turn and journal it twice. The
    /// would-complete pending signal is discarded: the settle drain
    /// re-reads the store and starts the Inject new turn implicitly.
    /// <param name="client">The chat client the turn runs against.</param>
    /// <param name="tools">The tools the turn may call.</param>
    /// <param name="options">The turn loop tuning and per-turn budget.</param>
    /// <param name="delay">The delay seam the hard deadline fires off. Must not be null.</param>
    /// <param name="isLeaseValid">The lease hook the loop checks. Must not be null.</param>
    /// <param name="inject">The store-backed Inject fold wiring, or None for a Queue-only turn with no fold.</param>
    /// <param name="getSystemPrompt">The composed system prompt hook (issue 66), or None to run with no system message.</param>
    /// <returns>A runner executing one inbox entry per turn.</returns>
    let private runnerFor
        (client: IChatClient)
        (tools: IReadOnlyDictionary<string, AITool>)
        (options: TurnLoop.TurnLoopOptions)
        (delay: ILlmDelay)
        (isLeaseValid: unit -> bool)
        (inject: InjectFoldWiring option)
        (getSystemPrompt: PromptComposition.GetTurnSystemPrompt option)
        : (InboxEntry -> CancellationToken -> Task<TurnResult>) =
        ArgumentNullException.ThrowIfNull(client)
        ArgumentNullException.ThrowIfNull(tools)
        ArgumentNullException.ThrowIfNull(delay)
        ArgumentNullException.ThrowIfNull(isLeaseValid)

        fun entry cancellationToken ->
            task {
                let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

                // Package-load step (issue 66): resolve the composed
                // system prompt and lead the history with it. A
                // null/empty resolution keeps today's user-only shape.
                match getSystemPrompt with
                | Some resolve ->
                    let! systemPrompt = resolve entry cancellationToken
                    PromptComposition.prependSystemPrompt history systemPrompt
                | None -> ()

                match entry.Payload with
                | :? UserMessagePayload as userMessage when
                    not (isNull (box userMessage))
                    && not (isNull (box userMessage.Message))
                    && not (isNull (box userMessage.Message.Parts))
                    ->
                    let parts = ResizeArray<AIContent>()

                    for part in userMessage.Message.Parts do
                        if not (isNull (box part)) then
                            parts.Add(part)

                    history.Add(ChatMessage(ChatRole.User, parts :> IList<AIContent>))
                | _ -> history.Add(ChatMessage(ChatRole.User, ""))

                match inject with
                | None -> return! TurnLoop.runAsync client history tools options delay cancellationToken isLeaseValid
                | Some wiring ->
                    // Per-turn mint: this closure runs synchronously inside
                    // the actor's startTurn, once per turn, so the id is
                    // the running (injecting) turn's for every entry this
                    // turn folds.
                    let turnId = TurnId.New()

                    let drainInjected () : IReadOnlyList<InboxEntry> =
                        wiring.Store
                            .ReadPendingInbox(wiring.Tenant, wiring.SessionId, CancellationToken.None)
                            .GetAwaiter()
                            .GetResult()

                    let journalInjected (injected: InboxEntry) : unit =
                        if not (isNull (box injected)) then
                            match injected.Payload with
                            | :? UserMessagePayload as payload when
                                not (isNull (box payload)) && not (isNull (box payload.Message))
                                ->
                                let event =
                                    UserMessageEvent(
                                        injected.SessionId,
                                        turnId,
                                        Nullable<int64>(),
                                        DateTimeOffset.UtcNow,
                                        payload.Message
                                    )

                                // Route the fold through the journal writer:
                                // the observer sees the redacted shape, so a
                                // downstream journal carries no secrets. The
                                // writer never drops: kind and ids survive,
                                // only secret shapes are replaced.
                                let redacted = JournalWriter.sanitizeEvent event :?> UserMessageEvent

                                match wiring.JournalEvent with
                                | Some observe ->
                                    try
                                        observe redacted
                                    with _ ->
                                        ()
                                | None -> ()
                            | _ -> ()

                    let consumeInjected (injected: InboxEntry) : unit =
                        let positions = [| injected.Position |] :> IReadOnlyList<int64>

                        wiring.Store
                            .MarkInboxConsumed(wiring.Tenant, wiring.SessionId, positions, CancellationToken.None)
                            .GetAwaiter()
                            .GetResult()
                        |> ignore

                    let! completion =
                        TurnLoop.runAsyncWithInjects
                            client
                            history
                            tools
                            options
                            delay
                            cancellationToken
                            isLeaseValid
                            drainInjected
                            journalInjected
                            consumeInjected

                    return completion.Result
            }

    /// Builds the default turn runner over TurnLoop.runAsync for Queue
    /// delivery: one ChatRole.User history message from the entry's parts,
    /// no Inject fold, and an always-live lease hook with no claim fence.
    /// <param name="client">The chat client the turn runs against.</param>
    /// <param name="tools">The tools the turn may call.</param>
    /// <param name="options">The turn loop tuning and per-turn budget.</param>
    /// <param name="delay">The delay seam the hard deadline fires off. Must not be null.</param>
    /// <returns>A runner executing one Queue inbox entry per turn.</returns>
    let createTurnRunner
        (client: IChatClient)
        (tools: IReadOnlyDictionary<string, AITool>)
        (options: TurnLoop.TurnLoopOptions)
        (delay: ILlmDelay)
        : (InboxEntry -> CancellationToken -> Task<TurnResult>) =
        runnerFor client tools options delay (fun () -> true) None None

    /// Builds the composed turn runner over TurnLoop.runAsync for Queue
    /// delivery (issue 66): the same history shape as
    /// <see cref="M:Legate.SessionActor.createTurnRunner" />, but the
    /// turn's composed system prompt leads the history as a
    /// ChatRole.System message. A null or empty resolution runs the
    /// user-only shape, so an uncomposed turn behaves exactly like the
    /// default runner.
    /// <param name="client">The chat client the turn runs against.</param>
    /// <param name="tools">The tools the turn may call.</param>
    /// <param name="options">The turn loop tuning and per-turn budget.</param>
    /// <param name="delay">The delay seam the hard deadline fires off. Must not be null.</param>
    /// <param name="getSystemPrompt">Resolves the turn's composed system prompt. Must not be null.</param>
    /// <returns>A runner executing one Queue inbox entry per turn with the composed system message first.</returns>
    let createComposedTurnRunner
        (client: IChatClient)
        (tools: IReadOnlyDictionary<string, AITool>)
        (options: TurnLoop.TurnLoopOptions)
        (delay: ILlmDelay)
        (getSystemPrompt: PromptComposition.GetTurnSystemPrompt)
        : (InboxEntry -> CancellationToken -> Task<TurnResult>) =
        if isNull (box getSystemPrompt) then
            raise (ArgumentNullException(nameof getSystemPrompt))

        runnerFor client tools options delay (fun () -> true) None (Some getSystemPrompt)

    /// Builds the Inject-aware turn runner over
    /// TurnLoop.runAsyncWithInjects (issue 34): the same history shape as
    /// <see cref="M:Legate.SessionActor.createTurnRunner" />, folding
    /// pending Inject entries at each iteration boundary through the
    /// store-backed wiring and journaling each folded entry once as a
    /// UserMessageEvent under the running turn's id. The would-complete
    /// pending signal is discarded: the settle drain re-reads the store
    /// and starts the Inject new turn implicitly.
    /// <param name="client">The chat client the turn runs against.</param>
    /// <param name="tools">The tools the turn may call.</param>
    /// <param name="options">The turn loop tuning and per-turn budget.</param>
    /// <param name="delay">The delay seam the hard deadline fires off. Must not be null.</param>
    /// <param name="wiring">The store-backed Inject fold wiring for the session.</param>
    /// <returns>A runner executing one inbox entry per turn with the Inject fold.</returns>
    let createInjectFoldRunner
        (client: IChatClient)
        (tools: IReadOnlyDictionary<string, AITool>)
        (options: TurnLoop.TurnLoopOptions)
        (delay: ILlmDelay)
        (wiring: InjectFoldWiring)
        : (InboxEntry -> CancellationToken -> Task<TurnResult>) =
        if isNull (box wiring) then
            raise (ArgumentNullException(nameof wiring))

        runnerFor client tools options delay (fun () -> true) (Some wiring) None

    /// Builds the claimed turn runner over TurnLoop.runAsync for Queue
    /// delivery (issue 33): the same history shape as
    /// <see cref="M:Legate.SessionActor.createTurnRunner" />, running under
    /// the claim's heartbeat-backed lease hook with the options-carried
    /// per-tool fence, so a fenced loser stops before its next provider
    /// call and never invokes a tool.
    /// <param name="client">The chat client the turn runs against.</param>
    /// <param name="tools">The tools the turn may call.</param>
    /// <param name="options">The turn loop tuning and per-turn budget.</param>
    /// <param name="delay">The delay seam the hard deadline fires off. Must not be null.</param>
    /// <param name="isLeaseValid">The heartbeat-backed lease hook (ClaimHeartbeat.ClaimLeaseView.IsValid). Must not be null.</param>
    /// <param name="verifyClaim">The last-moment per-tool fence (ClaimFence.checkBeforeCallAsync), or None for no fence.</param>
    /// <returns>A runner executing one Queue inbox entry per turn under the claim.</returns>
    let createClaimedTurnRunner
        (client: IChatClient)
        (tools: IReadOnlyDictionary<string, AITool>)
        (options: TurnLoop.TurnLoopOptions)
        (delay: ILlmDelay)
        (isLeaseValid: unit -> bool)
        (verifyClaim: (unit -> Task<bool>) option)
        : (InboxEntry -> CancellationToken -> Task<TurnResult>) =
        runnerFor
            client
            tools
            { options with
                VerifyClaim = verifyClaim
            }
            delay
            isLeaseValid
            None
            None

    // ────────────────── Compaction wiring (issue 45) ──────────────────

    /// Store-backed compaction wiring for one turn: what the TurnLoop
    /// iteration-boundary hook closes over to compact at each boundary
    /// (issue 45 over issue 44's threshold). The hook resolves the catalog
    /// entry for the session model once here, journals through
    /// JournalWriter.appendWithTokenAsync under the wiring's journal token,
    /// and checks the lease hook at the last moment before every journal
    /// write, so the takeover loser journals nothing. Sub-agent turns share
    /// the TurnLoop entry and its options-carried hook, so they inherit the
    /// same mechanism with no separate code.
    type internal CompactionWiring =
        {
            /// LLM settings carrying the Compaction model override and the
            /// CompactionKeepMessages tail. Never null.
            Llm: LlmOptions
            /// The tokens held back beyond the reserved output when deriving
            /// the threshold.
            ReservedBufferTokens: int
            /// The session's model: the threshold lookup and default
            /// summariser model.
            SessionModel: ModelReference
            /// Resolves the session model to its catalog entry, or null
            /// when the host keeps no catalog (the threshold falls back to
            /// the catalog defaults).
            Catalog: ILlmModelCatalog | null
            /// The chat client the turn runs against. Never null.
            Client: IChatClient
            /// Receives the summariser usage checkpoint, or null.
            Observer: IUsageObserver | null
            /// Authorises the summariser model call, or null.
            Policy: IModelPolicy | null
            /// The tenant the session belongs to.
            Tenant: TenantId
            /// The session the turn runs in.
            SessionId: SessionId
            /// The turn compacting.
            TurnId: TurnId
            /// The 1-based attempt the turn runs under.
            Attempt: int
            /// The journal the compaction events append to. Never null.
            EventStore: ISessionEventStore
            /// The claim token fencing the journal appends. Never null.
            JournalToken: string
            /// The last-moment claim fence the journal writes check. Never
            /// null.
            IsLeaseValid: unit -> bool
        }

    /// Resolves the session model's catalog entry: the wiring's catalog, or
    /// null when the host keeps no catalog (the threshold falls back to the
    /// catalog defaults).
    /// <param name="catalog">The model catalog, or null for the fallback threshold.</param>
    /// <param name="sessionModel">The session's model.</param>
    /// <returns>The catalog entry, or null when the model is unknown.</returns>
    let private catalogEntryOf
        (catalog: ILlmModelCatalog | null)
        (sessionModel: ModelReference)
        : ModelCatalogEntry | null =
        match box catalog with
        | null -> Unchecked.defaultof<ModelCatalogEntry>
        | :? ILlmModelCatalog as live -> live.GetEntry(sessionModel)
        | _ -> Unchecked.defaultof<ModelCatalogEntry>

    /// Journals one event under a claim token: the sink every compaction
    /// path shares, fenced store-side so a takeover loser journals nothing.
    /// <param name="eventStore">The journal the event appends to. Must not be null.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session compacting.</param>
    /// <param name="token">The claim token fencing the append. Must not be null.</param>
    /// <returns>The single-event journal sink.</returns>
    let private journalSink
        (eventStore: ISessionEventStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (token: string)
        : SessionEvent -> Task<JournalWriter.JournalWriteResult> =
        fun event ->
            let events = ResizeArray<SessionEvent>([| event |]) :> IReadOnlyList<SessionEvent>

            JournalWriter.appendWithTokenAsync eventStore tenant sessionId token events CancellationToken.None

    /// Validates the store-backed compaction wiring shared by the hook
    /// builders: every reference the boundary dereferences must be set.
    /// <param name="wiring">The wiring to validate. Must not be null.</param>
    let private requireWiring (wiring: CompactionWiring) : unit =
        if isNull (box wiring) then
            raise (ArgumentNullException(nameof wiring))

        ArgumentNullException.ThrowIfNull(wiring.Llm)
        ArgumentNullException.ThrowIfNull(wiring.Client)
        ArgumentNullException.ThrowIfNull(wiring.EventStore)
        ArgumentNullException.ThrowIfNull(wiring.JournalToken)
        ArgumentNullException.ThrowIfNull(wiring.IsLeaseValid)

    /// Builds the per-turn hook dependencies from the store-backed wiring.
    /// <param name="wiring">The store-backed compaction wiring for the turn. Must be validated.</param>
    /// <param name="turnId">The turn compacting.</param>
    /// <param name="attempt">The 1-based attempt the turn runs under.</param>
    /// <param name="journalAsync">The fenced single-event journal sink. Must not be null.</param>
    /// <param name="isLeaseValid">The last-moment claim fence. Must not be null.</param>
    /// <returns>The boundary dependencies.</returns>
    let private hookDepsOf
        (wiring: CompactionWiring)
        (turnId: TurnId)
        (attempt: int)
        (journalAsync: SessionEvent -> Task<JournalWriter.JournalWriteResult>)
        (isLeaseValid: unit -> bool)
        : Compaction.CompactionHookDeps =
        {
            Client = wiring.Client
            SessionModel = wiring.SessionModel
            CompactionModel = wiring.Llm.Compaction
            KeepMessages = wiring.Llm.CompactionKeepMessages
            CatalogEntry = catalogEntryOf wiring.Catalog wiring.SessionModel
            ReservedBufferTokens = wiring.ReservedBufferTokens
            Observer = wiring.Observer
            ModelPolicy = wiring.Policy
            Tenant = wiring.Tenant
            SessionId = wiring.SessionId
            TurnId = turnId
            Attempt = attempt
            JournalAsync = journalAsync
            IsLeaseValid = isLeaseValid
        }

    /// Builds the per-turn compaction hook from the store-backed wiring:
    /// one summarise-and-rewrite pass per iteration boundary, journaled
    /// under the wiring's token and fenced by its lease hook.
    /// <param name="wiring">The store-backed compaction wiring for the turn.</param>
    /// <returns>The boundary hook for TurnLoopOptions.</returns>
    let buildCompactionHook (wiring: CompactionWiring) : TurnLoop.CompactionHook =
        requireWiring wiring

        let journalAsync =
            journalSink wiring.EventStore wiring.Tenant wiring.SessionId wiring.JournalToken

        Compaction.createHook (hookDepsOf wiring wiring.TurnId wiring.Attempt journalAsync wiring.IsLeaseValid)

    /// Builds the force-aware per-turn compaction hook from the store-backed
    /// wiring (issue 46): each boundary takes the shared one-shot force
    /// cell, so an armed Compact fires one threshold-bypassed pass at the
    /// next boundary exactly once while unforced boundaries behave like
    /// <see cref="M:Legate.SessionActor.buildCompactionHook" />. The host
    /// shares the cell instance with the CompactDeps it passes in props.
    /// <param name="force">The one-shot cell the session actor arms. Must not be null.</param>
    /// <param name="wiring">The store-backed compaction wiring for the turn.</param>
    /// <returns>The force-aware boundary hook for TurnLoopOptions.</returns>
    let buildForcedCompactionHook
        (force: Compaction.CompactForce)
        (wiring: CompactionWiring)
        : TurnLoop.CompactionHook =
        ArgumentNullException.ThrowIfNull(force)
        requireWiring wiring

        let journalAsync =
            journalSink wiring.EventStore wiring.Tenant wiring.SessionId wiring.JournalToken

        Compaction.createForceHook
            force
            (hookDepsOf wiring wiring.TurnId wiring.Attempt journalAsync wiring.IsLeaseValid)

    /// Validates the on-demand compaction dependencies: every reference the
    /// Idle path dereferences must be set.
    /// <param name="compact">The dependencies to validate. Must not be null.</param>
    let private requireCompactDeps (compact: CompactDeps) : unit =
        if isNull (box compact) then
            raise (ArgumentNullException(nameof compact))

        ArgumentNullException.ThrowIfNull(compact.Llm)
        ArgumentNullException.ThrowIfNull(compact.Client)
        ArgumentNullException.ThrowIfNull(compact.EventStore)
        ArgumentNullException.ThrowIfNull(compact.JournalToken)
        ArgumentNullException.ThrowIfNull(compact.Force)

    /// Compacts an Idle session now without starting a turn: replays the
    /// journal into a history through the shared estimate mapping, then
    /// runs the merged runner. Under threshold (or with nothing
    /// replaceable) no summariser call runs and a summariser failure
    /// journals CompactionFailedEvent while the session continues
    /// uncompacted. The mailbox serializes Idle work, so the lease hook is
    /// actor-owned constant-true; the store-side token still fences a
    /// takeover loser into TurnLeaseLostException before anything journals.
    /// Runs synchronously on the actor thread like the other fast
    /// store-first paths; the summariser call bounds the block.
    /// <param name="props">The session actor dependencies.</param>
    /// <param name="compact">The on-demand compaction wiring. Must be validated.</param>
    /// <param name="cancellationToken">Abandons the replay and the summariser call.</param>
    /// <returns>How the on-demand compact answered.</returns>
    let private compactIdleNow
        (props: SessionActorProps)
        (compact: CompactDeps)
        (cancellationToken: CancellationToken)
        : SessionCompactReply =
        try
            let options = ReadTranscriptOptions()

            let cells =
                awaitTask (
                    Transcripts.readTranscript
                        compact.EventStore
                        props.Tenant
                        props.SessionId
                        options
                        100
                        cancellationToken
                )

            let history = Compaction.messagesFromCells cells

            // The host fence stamp for the idle compact (issue 373): read
            // before the summariser call. A concurrent idle writer moves
            // the stamp and the host append below fences as CompactFenced.
            // A missing row falls back to the primed token sink, so the
            // compact stays fenced either way; the prime itself is untouched.
            let expectedStamp =
                try
                    match awaitTask (props.Store.GetSession(props.Tenant, props.SessionId, CancellationToken.None)) with
                    | null -> None
                    | session -> Some session.UpdatedAt
                with _ ->
                    None

            let journalAsync =
                match expectedStamp with
                | Some stamp ->
                    fun (event: SessionEvent) ->
                        let events = ResizeArray<SessionEvent>([| event |]) :> IReadOnlyList<SessionEvent>

                        JournalWriter.appendHostAsync
                            compact.EventStore
                            props.Tenant
                            props.SessionId
                            stamp
                            events
                            CancellationToken.None
                | None -> journalSink compact.EventStore props.Tenant props.SessionId compact.JournalToken

            let request: Compaction.CompactionRequest =
                {
                    Client = compact.Client
                    History = history
                    SessionModel = compact.SessionModel
                    CompactionModel = compact.Llm.Compaction
                    KeepMessages = compact.Llm.CompactionKeepMessages
                    CatalogEntry = catalogEntryOf compact.Catalog compact.SessionModel
                    ReservedBufferTokens = compact.ReservedBufferTokens
                    Observer = compact.Observer
                    ModelPolicy = compact.Policy
                    Tenant = props.Tenant
                    SessionId = props.SessionId
                    // The host-operation sentinel (issue 373): the idle
                    // compact is host authority, not execution, so the
                    // journaled CompactedEvent carries the default TurnId.
                    TurnId = Unchecked.defaultof<TurnId>
                    Attempt = 1
                    InputTokens = 0L
                    OutputTokens = 0L
                    JournalAsync = journalAsync
                    IsLeaseValid = (fun () -> true)
                    CancellationToken = cancellationToken
                }

            match awaitTask (Compaction.tryCompactAsync request) with
            | Compaction.NotNeeded -> CompactNotNeeded
            | Compaction.Compacted(beforeEstimate, afterEstimate, _, _) ->
                CompactCompleted(beforeEstimate, afterEstimate)
            | Compaction.FailedContinue _ -> CompactNotNeeded
        with
        | :? TurnLoop.TurnLeaseLostException -> CompactFenced
        | :? OperationCanceledException -> CompactNotNeeded

    // ────────────────── AutoClose (issue 82) ──────────────────

    /// Reads whether the session closes itself after its first completed
    /// turn: the AutoClose snapshot the session was opened with. A missing
    /// row, missing options, or a store read failure reads as false, so the
    /// close never fires spuriously.
    /// <param name="props">The session actor dependencies.</param>
    /// <returns>True when the session closes after its first Completed turn.</returns>
    let private autoCloseEnabled (props: SessionActorProps) : bool =
        try
            match awaitTask (props.Store.GetSession(props.Tenant, props.SessionId, CancellationToken.None)) with
            | null -> false
            | session when isNull (box session.Options) -> false
            | session -> session.Options.AutoClose
        with :? SessionNotFoundException ->
            false

    /// Consumes the settled entry and closes the session store-first for an
    /// AutoClose turn: the entry leaves the pending set before the Closed
    /// write lands, so a restart never redelivers a turn the close already
    /// answered. CloseSession is idempotent, and the actor's single-threaded
    /// sequencing keeps a second prompt from slipping between the consume
    /// and the close.
    /// <param name="props">The session actor dependencies.</param>
    /// <param name="entry">The entry the AutoClose turn executed.</param>
    let private consumeAndCloseSession (props: SessionActorProps) (entry: InboxEntry) : unit =
        let positions = [| entry.Position |] :> IReadOnlyList<int64>

        awaitTask (props.Store.MarkInboxConsumed(props.Tenant, props.SessionId, positions, CancellationToken.None))
        |> ignore

        awaitTask (props.Store.CloseSession(props.Tenant, props.SessionId, CancellationToken.None))
        |> ignore

    // ────────────────── Completion outbox (issue 84) ──────────────────

    /// Mints the stable idempotency key one settlement shares between its
    /// outbox row and its inline Notify: random 32-hex per settlement, so
    /// an inline delivery overlapping a re-drive deduplicates on the
    /// receiver's Idempotency-Key.
    /// <returns>A fresh stable key for one settlement.</returns>
    let private mintCompletionKey () : string = Guid.NewGuid().ToString("N")

    /// Enqueues the settlement's immutable route-snapshot completion row and
    /// returns the stored completion: the durable redriver is the sole
    /// delivery path, so this step performs no inline notification. Only
    /// sessions carrying a completion destination id enqueue: sinkless
    /// sessions store nothing. Best-effort and guarded: a store failure
    /// stores nothing, and the actor-thread sequencing is the fence.
    /// <param name="props">The session actor dependencies.</param>
    /// <param name="result">The settled turn result to deliver.</param>
    /// <returns>The stored completion, or None when sinkless or best-effort failed.</returns>
    let private dispatchCompletion (props: SessionActorProps) (result: TurnResult) : SessionCompletion option =
        try
            if isNull (box result) then
                None
            else
                match awaitTask (props.Store.GetSession(props.Tenant, props.SessionId, CancellationToken.None)) with
                | null -> None
                | session when isNull (box session.Options) -> None
                | session ->
                    match session.Options.CompletionDestinationId with
                    | null -> None
                    | destinationId ->
                        let completion =
                            {
                                SessionId = props.SessionId
                                TurnResult = result
                                Metadata = session.Options.Metadata
                                IdempotencyKey = mintCompletionKey ()
                            }

                        let stored =
                            try
                                awaitTask (
                                    props.Store.EnqueueCompletionOutbox(
                                        props.Tenant,
                                        destinationId,
                                        completion,
                                        CancellationToken.None
                                    )
                                )
                                |> Some
                            with _ ->
                                None

                        match stored with
                        | None -> None
                        | Some row -> Some row.Completion
        with _ ->
            None

    /// The session actor: recovers from the store, then owns the state
    /// machine. The mailbox parameter is injected by the spawn functions;
    /// one message is processed fully before the next is received, so the
    /// store-first sequences never interleave.
    /// <param name="props">The session actor dependencies.</param>
    /// <param name="mailbox">The actor mailbox, injected by spawn.</param>
    /// <returns>The Akka.FSharp actor computation to spawn.</returns>
    let behavior (props: SessionActorProps) (mailbox: Actor<SessionActorMessage>) =
        if isNull (box props.Store) then
            raise (ArgumentNullException(nameof props))

        if isNull (box props.RunTurn) then
            raise (ArgumentNullException(nameof props))

        match props.Compact with
        | Some compact -> requireCompactDeps compact
        | None -> ()

        let initialState = recover props
        let self = mailbox.Self

        let log = LoggingScopes.resolveLogger props.Logger

        /// Logs one actor point under the six canonical scopes, scoped to
        /// the synchronous handler block only (never across awaits). Text
        /// travels redacted, so a prompt carrying a secret shape never
        /// lands verbatim in the log.
        /// <param name="turnId">The turn, or null for prompt receipt.</param>
        /// <param name="message">The fixed message template.</param>
        let logScoped (turnId: string | null) (message: string) : unit =
            let scope =
                LoggingScopes.createScope (props.Tenant.ToString()) (props.SessionId.ToString()) turnId null 0 null

            use _scope = LoggingScopes.beginScope log scope
            log.LogInformation("{Message}", LoggingScopes.redactForLog message)

        /// Starts a turn for an inbox entry: guards the runner call itself
        /// (a synchronously throwing or null-returning runner faults the
        /// turn, never the actor), then pipes the outcome back as a
        /// one-way message without blocking the actor thread.
        /// <param name="entry">The inbox entry the turn executes.</param>
        /// <returns>The in-flight turn handle.</returns>
        let startTurn (entry: InboxEntry) : RunningTurn =
            let cts = new CancellationTokenSource()

            // Snapshot the live turn (issue 289): the claimed turn id the
            // settle choke points journal under. A missing row or an empty
            // snapshot mints fresh, preserving the pre-plumbing shape.
            let turnId =
                try
                    match awaitTask (props.Store.GetSession(props.Tenant, props.SessionId, CancellationToken.None)) with
                    | null -> TurnId.New()
                    | session when session.CurrentTurnId.HasValue -> session.CurrentTurnId.Value
                    | _ -> TurnId.New()
                with _ ->
                    TurnId.New()

            let runTask =
                try
                    let started = props.RunTurn entry cts.Token

                    if isNull (box started) then
                        Task.FromException<TurnResult>(
                            InvalidOperationException("The session turn runner returned null.")
                        )
                    else
                        started
                with ex ->
                    Task.FromException<TurnResult>(ex)

            runTask.ContinueWith(fun (completed: Task<TurnResult>) ->
                if completed.IsCanceled then
                    self.Tell(SessionTurnFaulted(entry, OperationCanceledException("The session turn was aborted.")))
                elif completed.IsFaulted then
                    let error =
                        match completed.Exception with
                        | null ->
                            InvalidOperationException("The session turn faulted without an exception.") :> Exception
                        | aggregate -> aggregate.GetBaseException()

                    self.Tell(SessionTurnFaulted(entry, error))
                else
                    self.Tell(SessionTurnSettled(entry, completed.Result)))
            |> ignore

            {
                Entry = entry
                TurnId = turnId
                Cts = cts
            }

        /// Settles a finished turn attempt: consumes the attempt's entry
        /// store-first, then drains the next pending entry into a new turn
        /// or returns the session to Idle. The drain tiers Interrupt user
        /// messages first in position order, then Queue-plus-Inject user
        /// messages in position order; Reply payloads never start a turn.
        /// A final-iteration Inject the loop left pending (its
        /// would-complete signal is discarded across the runner boundary)
        /// starts its new turn here, implicitly.
        /// <param name="entry">The entry the finished attempt executed.</param>
        /// <param name="cancellationToken">Abandons the settle reads.</param>
        /// <returns>The next loop state and in-flight turn.</returns>
        let settle (entry: InboxEntry) (cancellationToken: CancellationToken) : SessionState * RunningTurn option =
            let positions = [| entry.Position |] :> IReadOnlyList<int64>

            awaitTask (props.Store.MarkInboxConsumed(props.Tenant, props.SessionId, positions, cancellationToken))
            |> ignore

            Telemetry.addQueueDepth -1

            let pending =
                awaitTask (props.Store.ReadPendingInbox(props.Tenant, props.SessionId, cancellationToken))

            match selectDrainableEntries pending with
            | next :: _ ->
                let running = startTurn next
                (SessionState.Running, Some running)
            | [] ->
                awaitTask (
                    props.Store.UpdateSessionState(props.Tenant, props.SessionId, SessionState.Idle, cancellationToken)
                )
                |> ignore

                (SessionState.Idle, None)

        /// Builds the observable snapshot for a state: the lifecycle state,
        /// the store's pending inbox count, and the running entry position.
        /// <param name="state">The actor's current lifecycle state.</param>
        /// <param name="running">The turn in flight, or None.</param>
        /// <returns>The actor's current snapshot.</returns>
        let takeSnapshot (state: SessionState) (running: RunningTurn option) : SessionSnapshot =
            {
                SessionId = props.SessionId
                State = state
                PendingCount = pendingCount props
                RunningPosition = running |> Option.map (fun inFlight -> inFlight.Entry.Position)
                PendingRequestId = null
            }

        /// Observes a settled turn result through the props hook. Guarded: a
        /// throwing observer never kills the actor.
        /// <param name="result">The settled (or abort-mapped) turn result.</param>
        let notifySettled (result: TurnResult) : unit =
            match props.OnTurnSettled with
            | Some observe ->
                try
                    observe result
                with _ ->
                    ()
            | None -> ()

        /// Maps a reported result to the Aborted result a won stop settles:
        /// the stop cause wins over whatever the turn reported, even a
        /// success, so settlement and stop stay mutually exclusive. Falls
        /// back to the carried result when the cause maps to no settlement
        /// (only abort-family causes reach the pending stop, so this never
        /// fires).
        /// <param name="cause">The stop cause that won.</param>
        /// <param name="reason">Why the turn stopped.</param>
        /// <param name="result">The result the turn reported.</param>
        /// <returns>The result the actor settles.</returns>
        let mapAborted (cause: StopCause) (reason: string) (result: TurnResult) : TurnResult =
            match StopArbitration.settlementFor cause reason with
            | Some(status, outcome) ->
                { result with
                    Status = status
                    Outcome = outcome
                }
            | None -> result

        /// Builds the Aborted result a won stop settles when the turn left
        /// no result behind (a faulted attempt): zero iterations and usage,
        /// the abort-family outcome carrying who and why. Falls back to a
        /// Failed result when the cause maps to no settlement (only
        /// abort-family causes reach the pending stop, so this never fires).
        /// <param name="cause">The stop cause that won.</param>
        /// <param name="reason">Why the turn stopped.</param>
        /// <returns>The result the actor settles.</returns>
        let abortedResult (cause: StopCause) (reason: string) : TurnResult =
            match StopArbitration.settlementFor cause reason with
            | Some(status, outcome) ->
                {
                    AssistantText = ""
                    Status = status
                    Iterations = 0
                    Usage = { InputTokens = 0L; OutputTokens = 0L }
                    Outcome = outcome
                }
            | None ->
                {
                    AssistantText = ""
                    Status = TurnStatus.Failed
                    Iterations = 0
                    Usage = { InputTokens = 0L; OutputTokens = 0L }
                    Outcome = TurnFailed(reason) :> TurnOutcome
                }

        /// Appends a prompt entry without starting a turn: the entry waits
        /// for the settle drain (Running), for the Reply resume
        /// (WaitingForInput), or stays durable with nothing new starting
        /// (an out-of-range stored state).
        /// <param name="payload">What the entry carries: a user message.</param>
        /// <param name="delivery">How the message was delivered.</param>
        /// <param name="cancellationToken">Abandons the append.</param>
        /// <returns>The appended inbox entry.</returns>
        let appendWaiting
            (payload: InboxPayload)
            (delivery: DeliveryMode)
            (cancellationToken: CancellationToken)
            : InboxEntry =
            let appended =
                awaitTask (
                    props.Store.AppendInboxMessage(props.Tenant, props.SessionId, payload, delivery, cancellationToken)
                )

            Telemetry.addQueueDepth 1
            appended

        /// Appends a prompt entry while Idle and starts its turn: persists
        /// Running store-first, then drains tier-first (Interrupt first,
        /// then Queue-plus-Inject in position order), so an older entry
        /// orphaned by a restart wins over the just-appended one. The
        /// appended entry is the fallback when nothing else is drainable.
        /// <param name="payload">What the entry carries: a user message.</param>
        /// <param name="delivery">How the message was delivered.</param>
        /// <param name="cancellationToken">Abandons the append.</param>
        /// <returns>The appended entry and the in-flight turn.</returns>
        let startIdleTurn
            (payload: InboxPayload)
            (delivery: DeliveryMode)
            (cancellationToken: CancellationToken)
            : InboxEntry * RunningTurn =
            let appended = appendWaiting payload delivery cancellationToken

            awaitTask (
                props.Store.UpdateSessionState(props.Tenant, props.SessionId, SessionState.Running, cancellationToken)
            )
            |> ignore

            let pending =
                awaitTask (props.Store.ReadPendingInbox(props.Tenant, props.SessionId, cancellationToken))

            let first =
                selectDrainableEntries pending |> List.tryHead |> Option.defaultValue appended

            let next = startTurn first
            (appended, next)

        let rec loop
            (state: SessionState)
            (running: RunningTurn option)
            (arbitration: StopArbitration.ArbitrationState)
            (pendingStop: (StopCause * string) option)
            =
            actor {
                let! message = mailbox.Receive()

                match message with
                | QueuePrompt(payload, cancellationToken) ->
                    match state with
                    | SessionState.Closed ->
                        logScoped null "The session rejected a prompt: the session is closed."
                        mailbox.Sender() <! PromptRejected SessionState.Closed
                        return! loop state running arbitration pendingStop
                    | SessionState.Idle ->
                        let appended, next = startIdleTurn payload DeliveryMode.Queue cancellationToken
                        mailbox.Sender() <! PromptAccepted appended
                        logScoped null "The session accepted a prompt and started a turn."
                        return! loop SessionState.Running (Some next) StopArbitration.Undecided None
                    | SessionState.Running
                    | SessionState.WaitingForInput ->
                        let appended = appendWaiting payload DeliveryMode.Queue cancellationToken
                        mailbox.Sender() <! PromptAccepted appended
                        logScoped null "The session accepted a prompt while busy."
                        return! loop state running arbitration pendingStop
                    | _ ->
                        // Out-of-range stored state: stay durable but start
                        // nothing new.
                        let appended = appendWaiting payload DeliveryMode.Queue cancellationToken
                        mailbox.Sender() <! PromptAccepted appended
                        logScoped null "The session accepted a prompt while out of range."
                        return! loop state running arbitration pendingStop
                | InjectPrompt(payload, cancellationToken) ->
                    match state with
                    | SessionState.Closed ->
                        logScoped null "The session rejected an injected prompt: the session is closed."
                        mailbox.Sender() <! PromptRejected SessionState.Closed
                        return! loop state running arbitration pendingStop
                    | SessionState.Idle ->
                        let appended, next = startIdleTurn payload DeliveryMode.Inject cancellationToken
                        mailbox.Sender() <! PromptAccepted appended
                        logScoped null "The session accepted an injected prompt and started a turn."
                        return! loop SessionState.Running (Some next) StopArbitration.Undecided None
                    | SessionState.Running
                    | SessionState.WaitingForInput ->
                        // Append-and-wait: the running turn folds the entry
                        // at its next iteration boundary, and a suspended
                        // turn leaves it for the settle drain. Never aborts.
                        let appended = appendWaiting payload DeliveryMode.Inject cancellationToken
                        mailbox.Sender() <! PromptAccepted appended
                        logScoped null "The session accepted an injected prompt while busy."
                        return! loop state running arbitration pendingStop
                    | _ ->
                        let appended = appendWaiting payload DeliveryMode.Inject cancellationToken
                        mailbox.Sender() <! PromptAccepted appended
                        logScoped null "The session accepted an injected prompt while out of range."
                        return! loop state running arbitration pendingStop
                | InterruptPrompt(payload, cancellationToken) ->
                    match state with
                    | SessionState.Closed ->
                        logScoped null "The session rejected an interrupt prompt: the session is closed."
                        mailbox.Sender() <! PromptRejected SessionState.Closed
                        return! loop state running arbitration pendingStop
                    | SessionState.Idle ->
                        let appended, next = startIdleTurn payload DeliveryMode.Interrupt cancellationToken
                        mailbox.Sender() <! PromptAccepted appended
                        logScoped null "The session accepted an interrupt prompt and started a turn."
                        return! loop SessionState.Running (Some next) StopArbitration.Undecided None
                    | SessionState.Running ->
                        // Pre-empt through the abort verb: the entry joins
                        // the inbox first so the settle drain finds it, then
                        // the running turn aborts under ExplicitAbort through
                        // the same arbitration cell AbortSession uses. The
                        // settle consumes the aborted entry and drains the
                        // Interrupt entry first with the rest of the inbox
                        // intact. A stop that already won keeps the first
                        // cause; the new entry still drains after the
                        // settle.
                        let appended = appendWaiting payload DeliveryMode.Interrupt cancellationToken

                        match running with
                        | Some inFlight ->
                            let nextArbitration, won =
                                StopArbitration.applyStop arbitration StopCause.ExplicitAbort

                            let nextStop =
                                if won then
                                    Some(StopCause.ExplicitAbort, InterruptReason)
                                else
                                    pendingStop

                            if won then
                                inFlight.Cts.Cancel()

                            mailbox.Sender() <! PromptAccepted appended
                            logScoped null "The session accepted an interrupt prompt and pre-empted the running turn."
                            return! loop state running nextArbitration nextStop
                        | None ->
                            mailbox.Sender() <! PromptAccepted appended
                            logScoped null "The session accepted an interrupt prompt with no turn in flight."
                            return! loop state running arbitration pendingStop
                    | SessionState.WaitingForInput ->
                        // Append-and-wait: suspended turns belong to issue
                        // 36, so nothing runs to abort and Reply still
                        // resumes the suspended turn.
                        let appended = appendWaiting payload DeliveryMode.Interrupt cancellationToken
                        mailbox.Sender() <! PromptAccepted appended
                        logScoped null "The session accepted an interrupt prompt while suspended."
                        return! loop state running arbitration pendingStop
                    | _ ->
                        let appended = appendWaiting payload DeliveryMode.Interrupt cancellationToken
                        mailbox.Sender() <! PromptAccepted appended
                        return! loop state running arbitration pendingStop
                | CloseSession cancellationToken ->
                    match running with
                    | Some inFlight -> inFlight.Cts.Cancel()
                    | None -> ()

                    let closed =
                        awaitTask (props.Store.CloseSession(props.Tenant, props.SessionId, cancellationToken))

                    mailbox.Sender() <! closed
                    return! loop SessionState.Closed None StopArbitration.Undecided None
                | ObserveHostAbort(tenant, sessionId, targetTurnId) ->
                    match state, running, props.Store with
                    | SessionState.Running, Some inFlight, (:? ISessionAbortControlStore as control) when
                        tenant = props.Tenant
                        && sessionId = props.SessionId
                        && inFlight.TurnId = targetTurnId
                        ->
                        match awaitTask (control.ReadAbortTarget(tenant, sessionId, CancellationToken.None)) with
                        | null -> return! loop state running arbitration pendingStop
                        | target when target.TurnId = targetTurnId ->
                            match target.Stop with
                            | null -> return! loop state running arbitration pendingStop
                            | stop ->
                                let next, won = StopArbitration.applyStop arbitration stop.Cause.Value

                                if won then
                                    inFlight.Cts.Cancel()

                                let selected =
                                    if won then
                                        Some(stop.Cause.Value, stop.Reason |> Option.ofObj |> Option.defaultValue "")
                                    else
                                        pendingStop

                                return! loop state running next selected
                        | _ -> return! loop state running arbitration pendingStop
                    | _ -> return! loop state running arbitration pendingStop
                | AbortSession(cause, reason, _) ->
                    match state, running with
                    | SessionState.Running, Some inFlight when
                        cause = StopCause.ExplicitAbort || cause = StopCause.HostShutdown
                        ->
                        let nextArbitration, won = StopArbitration.applyStop arbitration cause

                        let nextStop = if won then Some(cause, reason) else pendingStop

                        if won then
                            inFlight.Cts.Cancel()

                        mailbox.Sender() <! takeSnapshot state running
                        return! loop state running nextArbitration nextStop
                    | _ ->
                        // Idle, WaitingForInput (suspended turns belong to
                        // issue 36: nothing runs to abort), Closed, unknown
                        // states, and non-abort-family causes: a no-op
                        // returning the current state.
                        mailbox.Sender() <! takeSnapshot state running
                        return! loop state running arbitration pendingStop
                | CompactSession cancellationToken ->
                    match state with
                    | SessionState.Closed ->
                        mailbox.Sender() <! CompactRejected SessionState.Closed
                        return! loop state running arbitration pendingStop
                    | SessionState.Idle ->
                        match props.Compact with
                        | None ->
                            mailbox.Sender() <! CompactNotNeeded
                            return! loop state running arbitration pendingStop
                        | Some compact ->
                            let reply = compactIdleNow props compact cancellationToken
                            mailbox.Sender() <! reply
                            return! loop state running arbitration pendingStop
                    | SessionState.Running ->
                        match props.Compact with
                        | Some compact when not (isNull (box compact.Force)) ->
                            compact.Force.Request()
                            mailbox.Sender() <! CompactDeferred
                            return! loop state running arbitration pendingStop
                        | _ ->
                            // Unconfigured: no boundary hook shares the
                            // one-shot cell, so nothing can fire later.
                            mailbox.Sender() <! CompactNotNeeded
                            return! loop state running arbitration pendingStop
                    | SessionState.WaitingForInput ->
                        // Suspended turns belong to issue 36: their history
                        // is parked, so an on-demand compact no-ops.
                        mailbox.Sender() <! CompactNotNeeded
                        return! loop state running arbitration pendingStop
                    | _ ->
                        // Out-of-range stored state: stay durable but
                        // compact nothing.
                        mailbox.Sender() <! CompactNotNeeded
                        return! loop state running arbitration pendingStop
                | GetSnapshot ->
                    mailbox.Sender() <! takeSnapshot state running
                    return! loop state running arbitration pendingStop
                | SessionTurnSettled(entry, result) ->
                    match state, running with
                    | SessionState.Running, Some inFlight when inFlight.Entry.Position = entry.Position ->
                        match arbitration with
                        | StopArbitration.Undecided ->
                            // Settlement wins: the carried result stands.
                            inFlight.Cts.Dispose()
                            notifySettled result
                            dispatchCompletion props result |> ignore
                            logScoped null "The session settled a turn."

                            if result.Status = TurnStatus.Completed && autoCloseEnabled props then
                                // AutoClose (issue 82): the first Completed
                                // turn closes the session store-first instead
                                // of draining. Aborted and Failed results
                                // never take this path, so failed runs stay
                                // open for inspection.
                                consumeAndCloseSession props entry
                                return! loop SessionState.Closed None StopArbitration.Undecided None
                            else
                                let nextState, nextRunning = settle entry CancellationToken.None
                                return! loop nextState nextRunning StopArbitration.Undecided None
                        | StopArbitration.Decided(StopArbitration.StopWins cause) ->
                            // The stop landed first, so it wins even over a
                            // success: map to Aborted under the winning
                            // cause, then run the settle bookkeeping once.
                            inFlight.Cts.Dispose()

                            let reason = pendingStop |> Option.map snd |> Option.defaultValue ""

                            let settled = mapAborted cause reason result
                            notifySettled settled
                            dispatchCompletion props settled |> ignore
                            logScoped null "The session settled a turn under a stop cause."
                            let nextState, nextRunning = settle entry CancellationToken.None
                            return! loop nextState nextRunning StopArbitration.Undecided None
                        | StopArbitration.Decided StopArbitration.SettlementWins ->
                            // Stale: the turn already settled, so this
                            // completion produces zero effects.
                            return! loop state running arbitration pendingStop
                    | _ -> return! loop state running arbitration pendingStop
                | SessionTurnFaulted(entry, _) ->
                    match state, running with
                    | SessionState.Running, Some inFlight when inFlight.Entry.Position = entry.Position ->
                        match arbitration with
                        | StopArbitration.Undecided ->
                            inFlight.Cts.Dispose()
                            logScoped null "The session turn faulted and its entry was consumed."
                            let nextState, nextRunning = settle entry CancellationToken.None
                            return! loop nextState nextRunning StopArbitration.Undecided None
                        | StopArbitration.Decided(StopArbitration.StopWins cause) ->
                            // The stop arrived first, so it wins even over
                            // a real fault: settle Aborted under the cause.
                            inFlight.Cts.Dispose()

                            let reason = pendingStop |> Option.map snd |> Option.defaultValue ""

                            notifySettled (abortedResult cause reason)
                            logScoped null "The session turn faulted under a stop cause."
                            let nextState, nextRunning = settle entry CancellationToken.None
                            return! loop nextState nextRunning StopArbitration.Undecided None
                        | StopArbitration.Decided StopArbitration.SettlementWins ->
                            // Stale: the turn already settled, so this fault
                            // produces zero effects.
                            return! loop state running arbitration pendingStop
                    | _ -> return! loop state running arbitration pendingStop
            }

        loop initialState None StopArbitration.Undecided None

    /// Builds the child-spawn factory the session router uses: parses the
    /// router's string id into a SessionId and spawns the session actor,
    /// falling back to the legacy identity-only child for ids that do not
    /// parse (the router historically accepted any non-blank string).
    /// <param name="store">The durable store session actors persist through.</param>
    /// <param name="tenant">The tenant router-spawned sessions belong to.</param>
    /// <param name="runTurn">The turn runner session actors drive per Queue entry.</param>
    /// <returns>A factory mapping a session id string to a child spawn.</returns>
    let spawnFactory
        (store: ISessionStore)
        (tenant: TenantId)
        (runTurn: InboxEntry -> CancellationToken -> Task<TurnResult>)
        : (string -> IActorContext -> string -> IActorRef) =
        ArgumentNullException.ThrowIfNull(store)

        if isNull (box runTurn) then
            raise (ArgumentNullException(nameof runTurn))

        fun sessionId context name ->
            let mutable parsed = Unchecked.defaultof<SessionId>

            if SessionId.TryParse(sessionId, &parsed) then
                let captured = parsed

                let props =
                    {
                        Store = store
                        Tenant = tenant
                        SessionId = captured
                        RunTurn = runTurn
                        OnTurnSettled = Some(fun result -> PromptWaitHubs.ObserveSettledScoped tenant captured result)
                        OnInjectJournaled = None
                        Compact = None
                        Logger = null
                    }

                spawn context name (behavior props)
            else
                spawn context name (actorOf (fun (_: obj) -> ()))

    /// Asks the actor with the shared timeout, honouring the caller's
    /// cancellation. The timeout bounds the wait while the token honours
    /// the caller.
    /// <param name="session">The session actor.</param>
    /// <param name="message">The message to ask with.</param>
    /// <param name="cancellationToken">Cancels the ask.</param>
    /// <returns>The actor's reply.</returns>
    let private askAsync<'Reply>
        (session: IActorRef)
        (message: SessionActorMessage)
        (cancellationToken: CancellationToken)
        : Task<'Reply> =
        task {
            use timeoutCts = new CancellationTokenSource(askTimeout)

            use linkedCts =
                CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token)

            let! reply = session.Ask<'Reply>(message, linkedCts.Token)
            return reply
        }

    /// Reads the session row or throws the boundary precondition failure.
    /// <param name="store">The durable store.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to require.</param>
    /// <param name="cancellationToken">Abandons the read.</param>
    /// <returns>The stored session.</returns>
    let private requireSessionAsync
        (store: ISessionStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (cancellationToken: CancellationToken)
        : Task<Session> =
        task {
            let! found = store.GetSession(tenant, sessionId, cancellationToken)

            match found with
            | null -> return raise (SessionNotFoundException(sessionId, "The session does not exist."))
            | session -> return session
        }

    /// Prompts a session with a delivery mode: the client boundary. Validates
    /// the session is present and not Closed before touching the actor, so
    /// invalid transitions throw here, never inside the actor. A rejection
    /// that still races through (Closed between the check and the actor)
    /// maps to the same exception. Queue appends and acts on the message
    /// once the running turn (if any) finishes; Inject appends and folds
    /// into the running turn at its next iteration boundary without
    /// interrupting it; Interrupt appends and pre-empts the running turn,
    /// settling it as Aborted under ExplicitAbort before starting the new
    /// turn. While WaitingForInput every mode appends and waits (Reply
    /// still resumes the suspended turn), and while Idle every mode starts
    /// a turn normally through the tiered drain.
    /// <param name="store">The durable store.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to prompt.</param>
    /// <param name="session">The session actor.</param>
    /// <param name="message">The user message. Must not be null.</param>
    /// <param name="delivery">How the message is delivered to a running turn.</param>
    /// <param name="cancellationToken">Cancels the prompt.</param>
    /// <returns>The appended inbox entry.</returns>
    let promptAsync
        (store: ISessionStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (session: IActorRef)
        (message: UserMessage)
        (delivery: DeliveryMode)
        (cancellationToken: CancellationToken)
        : Task<InboxEntry> =
        ArgumentNullException.ThrowIfNull(store)
        ArgumentNullException.ThrowIfNull(session)

        if isNull (box message) then
            raise (ArgumentNullException(nameof message))

        task {
            let! current = requireSessionAsync store tenant sessionId cancellationToken

            if current.State = SessionState.Closed then
                raise (
                    InvalidSessionStateException(
                        sessionId,
                        current.State.ToString(),
                        "The session is closed and accepts no further prompts."
                    )
                )

            let payload = UserMessagePayload(message) :> InboxPayload

            let prompt =
                match delivery with
                | DeliveryMode.Queue -> QueuePrompt(payload, cancellationToken)
                | DeliveryMode.Inject -> InjectPrompt(payload, cancellationToken)
                | DeliveryMode.Interrupt -> InterruptPrompt(payload, cancellationToken)
                | unknown ->
                    raise (
                        ArgumentOutOfRangeException(
                            nameof delivery,
                            sprintf "Unknown delivery mode: %O. Expected Queue, Inject, or Interrupt." unknown
                        )
                    )

            let! reply = askAsync<SessionPromptReply> session prompt cancellationToken

            match reply with
            | PromptAccepted entry -> return entry
            | PromptRejected rejectedState ->
                return
                    raise (
                        InvalidSessionStateException(
                            sessionId,
                            rejectedState.ToString(),
                            "The session closed before the prompt was accepted."
                        )
                    )
        }

    /// Queues a user message on a session: the client boundary. Validates
    /// the session is present and not Closed before touching the actor, so
    /// invalid transitions throw here, never inside the actor. A rejection
    /// that still races through (Closed between the check and the actor)
    /// maps to the same exception.
    /// <param name="store">The durable store.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to prompt.</param>
    /// <param name="session">The session actor.</param>
    /// <param name="message">The user message. Must not be null.</param>
    /// <param name="cancellationToken">Cancels the prompt.</param>
    /// <returns>The appended inbox entry.</returns>
    let promptQueueAsync
        (store: ISessionStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (session: IActorRef)
        (message: UserMessage)
        (cancellationToken: CancellationToken)
        : Task<InboxEntry> =
        promptAsync store tenant sessionId session message DeliveryMode.Queue cancellationToken

    /// Closes a session: the client boundary. Valid in every state and
    /// idempotent; on a Running session the actor aborts the turn first
    /// through cancellation only. Unknown sessions throw
    /// SessionNotFoundException before touching the actor.
    /// <param name="store">The durable store.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to close.</param>
    /// <param name="session">The session actor.</param>
    /// <param name="cancellationToken">Cancels the close.</param>
    /// <returns>The stored session after the close.</returns>
    let closeAsync
        (store: ISessionStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (session: IActorRef)
        (cancellationToken: CancellationToken)
        : Task<Session> =
        ArgumentNullException.ThrowIfNull(store)
        ArgumentNullException.ThrowIfNull(session)

        task {
            let! _ = requireSessionAsync store tenant sessionId cancellationToken
            let! closed = askAsync<Session> session (CloseSession cancellationToken) cancellationToken
            return closed
        }

    /// Reads the session actor's snapshot: the client boundary read.
    /// <param name="session">The session actor.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The actor's current snapshot.</returns>
    let getSnapshotAsync (session: IActorRef) (cancellationToken: CancellationToken) : Task<SessionSnapshot> =
        ArgumentNullException.ThrowIfNull(session)
        askAsync<SessionSnapshot> session GetSnapshot cancellationToken

    /// Aborts the turn running in a session: the client boundary turn-level
    /// verb. Idle is a no-op returning the current snapshot, as is
    /// WaitingForInput (suspended turns belong to issue 36: nothing runs to
    /// abort). Running records the pending stop under the typed cause,
    /// cancels the turn, and settles Aborted under the winning cause when
    /// the turn reports back; settlement and stop stay mutually exclusive
    /// and a second abort keeps the first cause. Close stays lifecycle-only
    /// and never records a stop cause. Unknown sessions throw
    /// SessionNotFoundException and Closed sessions throw
    /// InvalidSessionStateException before touching the actor; a Close
    /// racing the abort maps to the same exception.
    /// <param name="store">The durable store.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to abort the turn in.</param>
    /// <param name="session">The session actor.</param>
    /// <param name="cause">Which abort-family stop cause wins: ExplicitAbort or HostShutdown.</param>
    /// <param name="reason">Why the turn stops. Must not be null. Never contains secrets or tool arguments.</param>
    /// <param name="cancellationToken">Cancels the abort.</param>
    /// <returns>The actor's snapshot after the abort was accepted.</returns>
    let abortAsync
        (store: ISessionStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (session: IActorRef)
        (cause: StopCause)
        (reason: string)
        (cancellationToken: CancellationToken)
        : Task<SessionSnapshot> =
        ArgumentNullException.ThrowIfNull(store)
        ArgumentNullException.ThrowIfNull(session)

        if isNull (box reason) then
            raise (ArgumentNullException(nameof reason))

        if cause <> StopCause.ExplicitAbort && cause <> StopCause.HostShutdown then
            raise (
                ArgumentOutOfRangeException(
                    nameof cause,
                    "Only ExplicitAbort and HostShutdown abort a turn: the deadline arrives through the turn loop and lease loss through the claim fence."
                )
            )

        task {
            let! current = requireSessionAsync store tenant sessionId cancellationToken

            if current.State = SessionState.Closed then
                raise (
                    InvalidSessionStateException(
                        sessionId,
                        current.State.ToString(),
                        "The session is closed and accepts no abort."
                    )
                )

            let! snapshot =
                askAsync<SessionSnapshot> session (AbortSession(cause, reason, cancellationToken)) cancellationToken

            if snapshot.State = SessionState.Closed then
                return
                    raise (
                        InvalidSessionStateException(
                            sessionId,
                            snapshot.State.ToString(),
                            "The session closed before the abort was accepted."
                        )
                    )
            else
                return snapshot
        }

    /// Compacts a session on demand: the client boundary. Idle replays the
    /// journal and compacts now without starting a turn, answering the
    /// before/after estimates; Running arms the one-shot force flag the
    /// turn's force-aware hook honors at the next boundary;
    /// WaitingForInput no-ops (suspended turns belong to issue 36).
    /// Unknown sessions throw SessionNotFoundException and Closed sessions
    /// throw InvalidSessionStateException before touching the actor; a
    /// Close racing the compact maps to the same exception. The future
    /// public ILegateClient wrapper stays a follow-up: this boundary is
    /// internal.
    /// <param name="store">The durable store.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to compact.</param>
    /// <param name="session">The session actor.</param>
    /// <param name="cancellationToken">Cancels the compact.</param>
    /// <returns>The actor's compact reply.</returns>
    let compactAsync
        (store: ISessionStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (session: IActorRef)
        (cancellationToken: CancellationToken)
        : Task<SessionCompactReply> =
        ArgumentNullException.ThrowIfNull(store)
        ArgumentNullException.ThrowIfNull(session)

        task {
            let! current = requireSessionAsync store tenant sessionId cancellationToken

            if current.State = SessionState.Closed then
                raise (
                    InvalidSessionStateException(
                        sessionId,
                        current.State.ToString(),
                        "The session is closed and accepts no compact."
                    )
                )

            let! reply = askAsync<SessionCompactReply> session (CompactSession cancellationToken) cancellationToken

            match reply with
            | CompactRejected rejectedState ->
                return
                    raise (
                        InvalidSessionStateException(
                            sessionId,
                            rejectedState.ToString(),
                            "The session closed before the compact was accepted."
                        )
                    )
            | _ -> return reply
        }

    // ────────────────── Suspend and resume (issue 36) ──────────────────

    /// How a suspendable turn runs: the first run carries no cursor and no
    /// reply, a resume carries both. Attempt is 1-based and incremented on
    /// every resume, so a resumed run continues the same turn id. The
    /// session memory of AllowForSession decisions travels with the turn.
    /// The crash seed carries the in-memory rehydrated history (with the
    /// resumption note) on crash-resume activation and is None elsewhere:
    /// a seeded run leads its runner history input with the seed instead
    /// of the entry-derived message. The in-call marker hook (issue 284)
    /// rides fourth to last: Some journals the fenced TurnStartedEvent at the first
    /// provider-call entry, None journals nothing (resumes already marked
    /// before they suspended). The usage-checkpoint hook (issue 321) rides
    /// third to last and the skill-load hook second to last: Some journals
    /// the fenced UsageEvent at iteration boundaries plus settle and the
    /// fenced SkillLoadedEvent per successful load, None journals nothing.
    /// Resumes carry usage and skill hooks (post-resume work checkpoints)
    /// while OnTurnStarted stays None there. The turn id rides last (issue
    /// 289): the actor-supplied loop-run id the marker, the completion, and
    /// the settle choke points all key on. Tests inject scripted runners; the
    /// TurnLoop-backed runner wires
    /// TurnLoop.runSuspendableAsync plus the resume continuations.
    type SuspendableRunner =
        InboxEntry
            -> int
            -> HashSet<string>
            -> TurnLoop.TurnLoopSuspension option
            -> Reply option
            -> IList<ChatMessage> option
            -> CancellationToken
            -> TurnLoop.TurnStartedHook option
            -> TurnLoop.UsageCheckpointHook option
            -> TurnLoop.SkillLoadedHook option
            -> TurnId
            -> Task<TurnLoop.TurnLoopCompletion>

    /// What a suspendable session actor is built from: the base actor
    /// dependencies plus the journal, the AskTimeout seam, the journal
    /// token, and the suspendable runner. The journal token fences journal
    /// appends; tests prime it with ISessionStore.ClaimNextTurn so the
    /// in-memory journal lands, and a takeover re-claims so the loser
    /// appends nothing. The heartbeat is not cancelled on suspend: renewal
    /// continues while suspended under the same claim, bounded by lease
    /// expiry and AskTimeout. The behavior copies the token into a mutable
    /// cell a SetAgent swap replaces, so every journal write below reads
    /// the cell, never this initial value, after the first swap.
    type SuspendDeps =
        {
            /// The journal suspend and resolve events append to.
            EventStore: ISessionEventStore
            /// The seam the AskTimeout deadline fires off. Never the clock.
            Delay: ILlmDelay
            /// How long a suspension waits for its Reply before settling Failed.
            AskTimeout: TimeSpan
            /// The claim token fencing journal appends.
            JournalToken: string
            /// Genuine captured journal-prime authority. None only for internal unclaimed test shells.
            PrimeClaim: TurnClaim option
            /// Provider-fenced original entry for unstopped current-format recovery; null on fresh activation.
            Recovery: ControlTargetRecovery | null
            /// Runs one suspendable attempt. Never null.
            RunSuspendable: SuspendableRunner
            /// Re-primes the journal after the actor settles its primed
            /// claim: appends a bootstrap entry and claims it, returning the
            /// live claim, or None when no turn is claimable (a live claim
            /// is held) or the prime failed. The SetAgent swap calls it
            /// after settling the old prime and again to restore the live
            /// prime; a quiescent boundary that finds a recorded rebind
            /// retries through it until it succeeds. None when the host
            /// never re-primes (direct test constructions): a recorded
            /// rebind then stays pending.
            ReprimeJournal: (unit -> TurnClaim option) option
            /// Rebuilds the on-demand compaction wiring for a fresh journal
            /// token after a SetAgent swap, or None when the host drives
            /// Compact directly. The factory supplies the spawn-time
            /// compactFor; without one the swap keeps the wiring and only
            /// refreshes its token, so later Compacts stay live either way.
            RefreshCompact: (string -> CompactDeps option) option
            /// The agent catalog the per-turn authority gate reads, or null
            /// when the host runs without one: the gate is skipped then (the
            /// SetAgent facade validation precedent). Never drives tool
            /// resolution.
            AgentStore: IAgentStore | null
            /// Reads the completion era (issue 289): true once Open or Fork
            /// marked the session, false for pre-era sessions. The
            /// entity-start predicate consults it exactly like the journal.
            /// Never null.
            EraMarked: CompletionEra.CompletionEraReader
        }

    /// A rebuilt pending request from the journal: the crash path carries
    /// no in-memory cursor (history, tool call), so the matching Reply
    /// retries the turn from its inbox entry with attempt plus 1 instead of
    /// resuming from the cursor. The journal is the normative resume source.
    type RebuiltPending =
        {
            /// The pending request or question id the Reply must carry.
            RequestId: string
            /// The tool whose call raised the request.
            ToolName: string
            /// Which reply resumes the turn.
            Kind: TurnLoop.SuspensionKind
            /// The question text, or empty for permission suspensions.
            QuestionText: string
        }

    /// A suspended turn: the inbox entry it parked on, either the live
    /// cursor (in-memory resume) or the rebuilt pending (crash retry), the
    /// session AllowForSession memory, the attempt the parked run used, and
    /// the source the AskTimeout delay pipes through. The source is
    /// cancelled on Reply and never disposed on the timeout path.
    type private SuspendedTurn =
        {
            /// The inbox entry the parked turn executes.
            Entry: InboxEntry
            /// The turn the parked run executes (issue 289): captured at
            /// suspend from the completion, or the live-turn snapshot for a
            /// crash rebuild. The AskTimeout journal carries it.
            TurnId: TurnId
            /// The live cursor, or None after a crash rebuild.
            Cursor: TurnLoop.TurnLoopSuspension option
            /// The rebuilt pending, or None for a live suspension.
            Rebuilt: RebuiltPending option
            /// Tool names the host already allowed for the session.
            Allowed: HashSet<string>
            /// The 1-based attempt the parked run used.
            Attempt: int
            /// The source the AskTimeout delay cancels through.
            TimeoutCts: CancellationTokenSource
        }

    /// How the suspendable actor answers a Reply. The actor never throws
    /// ReplyMismatchException: an unknown or already-resolved request id is
    /// rejected with the exception and the client boundary throws it.
    type internal SessionReplyReply =

        /// The reply matched the pending request: carries the appended Reply
        /// inbox entry. The turn resumes under attempt plus 1.
        | ReplyAccepted of entry: InboxEntry

        /// The reply answered nothing pending: unknown or already-resolved.
        /// Carries the typed error the boundary throws.
        | ReplyRejected of error: ReplyMismatchException

    /// How the suspendable actor answers a SetAgent. The actor never throws
    /// InvalidSessionStateException: a Closed-state rebind is rejected with
    /// the current state and the client boundary maps it to the exception.
    /// Agent existence and enablement are enforced at the facade boundary
    /// (which holds the agent store); the actor applies the rebound id
    /// verbatim, at once when quiescent and at the next quiescent boundary
    /// otherwise.
    type internal SessionSetAgentReply =

        /// The rebind applied at once: the session converses with the new
        /// agent from the next turn. Carries the stored session after the
        /// rebind.
        | SetAgentApplied of session: Session

        /// The rebind was recorded and applies at the next quiescent
        /// boundary: a turn is running or entries are queued. Carries the
        /// stored session as it stands, still conversing with the previous
        /// agent.
        | SetAgentPending of session: Session

        /// The rebind arrived while the session was Closed; nothing was
        /// recorded. Carries the state the session was in.
        | SetAgentRejected of state: SessionState

    /// The suspendable session actor protocol extension. QueuePrompt,
    /// CloseSession, AbortSession, and GetSnapshot keep their base meaning;
    /// the completions below carry suspendable outcomes back to the actor.
    type internal SuspendableActorMessage =

        /// Queue a user message on the suspendable actor: appends to the
        /// durable inbox, starts a suspendable turn when Idle, waits when
        /// Running or WaitingForInput. Answered with SessionPromptReply.
        | SuspendableQueuePrompt of payload: InboxPayload * cancellationToken: CancellationToken

        /// Inject a user message into the running suspendable turn: appends
        /// to the durable inbox with Inject delivery, starts a suspendable
        /// turn when Idle, folds at the next iteration boundary when Running
        /// through the runner's drain hooks, waits when WaitingForInput,
        /// rejects when Closed. Never aborts the running turn. Answered with
        /// <see cref="T:Legate.SessionPromptReply" />.
        | SuspendableInjectPrompt of payload: InboxPayload * cancellationToken: CancellationToken

        /// Interrupt the running suspendable turn with a user message:
        /// appends to the durable inbox with Interrupt delivery, starts a
        /// suspendable turn when Idle, records the ExplicitAbort pending stop
        /// and drains the Interrupt entry first when Running, waits when
        /// WaitingForInput, rejects when Closed. The detached suspendable
        /// turn runs un-cancellable, so a recorded stop wins at its next
        /// report. Answered with <see cref="T:Legate.SessionPromptReply" />.
        | SuspendableInterruptPrompt of payload: InboxPayload * cancellationToken: CancellationToken

        /// The suspendable turn finished: settled (Suspension None) or
        /// suspended (Suspension Some). One-way from the turn task.
        | SuspendableFinished of
            entry: InboxEntry *
            completion: TurnLoop.TurnLoopCompletion *
            attempt: int *
            allowed: HashSet<string>

        /// The suspendable turn faulted. One-way from the turn task. The
        /// turn id rides last (issue 289): Some when the fault arrived
        /// from a running attempt, None when the runner never started
        /// (a miswired or null runner faults before any mint).
        | SuspendableFaulted of entry: InboxEntry * error: Exception * attempt: int * turnId: TurnId option

        /// A Reply inbox entry arrived for the suspended turn. Answered with
        /// SessionReplyReply; a mismatch rejects with ReplyMismatchException.
        | ReplyEntry of entry: InboxEntry

        /// Internal direct-actor reply ingress used by the unit-level actor
        /// seam. Routed production replies are admitted and appended by the
        /// scoped route gate before it sends ReplyEntry to this actor.
        | SessionReplyPayload of reply: Reply

        /// Reads the suspendable actor's snapshot. Answered with SessionSnapshot.
        | SuspendableGetSnapshot

        /// The AskTimeout deadline fired for the pending request id.
        /// One-way from the delay seam.
        | SuspendTimedOut of requestId: string

        /// Closes the suspendable session: the store row closes (evicting
        /// the grant memory) and the actor drops to Closed. Answered with
        /// the stored session.
        | SuspendableCloseSession of cancellationToken: CancellationToken

        /// Abort the turn running in a suspendable session under a typed
        /// stop cause: Idle and WaitingForInput (a suspended turn owns
        /// nothing running to abort) no-op returning the current snapshot;
        /// Running records the pending stop with first-cause-wins and answers
        /// the snapshot. The detached suspendable turn runs un-cancellable,
        /// so the recorded stop wins at its next report, mapping even a
        /// success to Aborted. Only abort-family causes
        /// (ExplicitAbort, HostShutdown) act; anything else is a no-op.
        /// Answered with <see cref="T:Legate.SessionSnapshot" />.
        | SuspendableAbortSession of cause: StopCause * reason: string * cancellationToken: CancellationToken
        /// Exact-target wake hint; receiving actors read persisted intent and validate tenant/session.
        | SuspendableObserveHostAbort of tenant: TenantId * sessionId: SessionId * targetTurnId: TurnId

        /// Compact the suspendable session on demand: Idle replays the
        /// journal and compacts now without starting a turn, Running arms
        /// the one-shot force flag the turn's force-aware boundary hook
        /// honors at the next iteration, WaitingForInput no-ops (a suspended
        /// turn owns the history), and Closed rejects. Answered with
        /// <see cref="T:Legate.SessionCompactReply" />.
        | SuspendableCompactSession of cancellationToken: CancellationToken

        /// Wakes the suspendable session for the polling dispatcher: when
        /// the actor is Idle and the durable inbox holds a drainable entry,
        /// starts a suspendable turn on the oldest one through the existing
        /// start path, otherwise a no-op. Never appends: dispatch routes
        /// through the prompt wire would duplicate pending work, so the
        /// wake only starts what is already stored. Idempotent by mailbox
        /// serialization plus the in-handler Idle re-check: a wake racing a
        /// prompt or a second wake collapses to a no-op or ordered
        /// queueing, never a duplicate or out-of-turn start. One-way from
        /// the dispatcher: no reply.
        | SuspendableCheckInbox

        /// Rebind the agent the session converses with: applies at once
        /// through the 6-step quiescent protocol when no turn is live and
        /// the pending inbox is empty, records the rebind as pending
        /// otherwise (Running, WaitingForInput, or queued entries), and
        /// rejects when Closed. A pending rebind applies at the next
        /// quiescent boundary: the settle drain, the faulted path, or the
        /// Idle handler before draining. Answered with
        /// <see cref="T:Legate.SessionSetAgentReply" />.
        | SuspendableSetAgent of agentId: AgentId * cancellationToken: CancellationToken

    /// Reason carried by TurnFailed when AskTimeout fires while suspended.
    /// Never contains secrets or tool arguments.
    [<Literal>]
    let AskTimeoutReason = "The suspended turn timed out waiting for a host reply."

    /// Extracts the request id a Reply answers: the permission request id
    /// or the question id. Returns None when the reply carries no id.
    /// <param name="reply">The reply.</param>
    /// <returns>The id the reply answers, or None.</returns>
    let private replyRequestId (reply: Reply) : string option =
        match reply with
        | :? PermissionDecision as decision when not (isNull (box decision)) ->
            if isNull (box decision.RequestId) then
                None
            else
                Some decision.RequestId
        | :? QuestionAnswer as answer when not (isNull (box answer)) ->
            if isNull (box answer.QuestionId) then
                None
            else
                Some answer.QuestionId
        | _ -> None

    /// Rebuilds the pending request from the journal: replays from the
    /// start and returns the latest PermissionRequested or QuestionAsked
    /// with no matching resolve after it. A resolve matches when its
    /// request id equals the ask id. Returns None when nothing is pending.
    /// <param name="eventStore">The journal to replay.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to rebuild.</param>
    /// <returns>The rebuilt pending, or None.</returns>
    let rebuildPendingFromJournal
        (eventStore: ISessionEventStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        : RebuiltPending option =
        ArgumentNullException.ThrowIfNull(eventStore)

        let rec replay cursor (pending: RebuiltPending option) =
            let outcome =
                awaitTask (eventStore.Replay(tenant, sessionId, cursor, 100, CancellationToken.None))

            match outcome with
            | :? EventReplayPage as page when not (isNull (box page)) ->
                let mutable current = pending
                let mutable nextCursor = cursor

                if not (isNull (box page.Events)) then
                    for event in page.Events do
                        if not (isNull (box event)) then
                            match event with
                            | :? PermissionRequestedEvent as asked when not (isNull (box asked)) ->
                                current <-
                                    Some
                                        {
                                            RequestId = asked.RequestId
                                            ToolName = asked.ToolName
                                            Kind = TurnLoop.PermissionSuspension
                                            QuestionText = ""
                                        }
                            | :? QuestionAskedEvent as asked when not (isNull (box asked)) ->
                                current <-
                                    Some
                                        {
                                            RequestId = asked.QuestionId
                                            ToolName = TurnLoop.AskUserToolName
                                            Kind = TurnLoop.QuestionSuspension
                                            QuestionText = asked.Question
                                        }
                            | :? PermissionResolvedEvent as resolved when not (isNull (box resolved)) ->
                                match current with
                                | Some awaiting when
                                    String.Equals(awaiting.RequestId, resolved.RequestId, StringComparison.Ordinal)
                                    ->
                                    current <- None
                                | _ -> ()
                            | :? QuestionAnsweredEvent as answered when not (isNull (box answered)) ->
                                match current with
                                | Some awaiting when
                                    String.Equals(awaiting.RequestId, answered.QuestionId, StringComparison.Ordinal)
                                    ->
                                    current <- None
                                | _ -> ()
                            | _ -> ()

                    if page.NextCursor.HasValue then
                        nextCursor <- page.NextCursor.Value

                if page.NextCursor.HasValue then
                    replay nextCursor current
                else
                    current
            | _ -> pending

        replay 0L None

    /// In-memory resumption note appended to a crash-rehydrated history.
    /// Never journaled, so no wire-contract change and no duplication
    /// across repeated restarts. Never contains secrets or tool arguments.
    [<Literal>]
    let CrashResumptionNote =
        "The previous attempt was interrupted by a restart. Tool calls from the interrupted attempt were not replayed."

    /// Client-safe reason carried by TurnFailed when OnCrashResume fails the
    /// interrupted turn instead of resuming it. Never contains secrets or
    /// tool arguments.
    [<Literal>]
    let CrashFailReason = "The turn was interrupted by a restart."

    /// Rebuilds a crash-rehydrated LLM history from transcript cells: drops
    /// the interrupted attempt's ToolCall and ToolResult cells (completed
    /// exchanges survive as text, never replayed as calls), keeps every
    /// other cell in order through the shared estimate mapping, and appends
    /// the in-memory resumption note. Pure: reads the cells, returns fresh
    /// messages, performs no I/O.
    /// <param name="cells">The transcript cells, in order. Must not be null and must not contain null.</param>
    /// <param name="interruptedTurn">The interrupted turn whose tool cells drop.</param>
    /// <returns>The rehydrated history with the resumption note.</returns>
    let rehydrateHistoryFromCells (cells: IReadOnlyList<SessionCell>) (interruptedTurn: TurnId) : IList<ChatMessage> =
        if isNull (box cells) then
            raise (ArgumentNullException(nameof cells))

        let kept = ResizeArray<SessionCell>(cells.Count)

        for cell in cells do
            if isNull (box cell) then
                raise (ArgumentNullException(nameof cells))

            let isInterruptedTool =
                cell.TurnId = interruptedTurn
                && (cell.Kind = SessionCellKind.ToolCall || cell.Kind = SessionCellKind.ToolResult)

            if not isInterruptedTool then
                kept.Add(cell)

        let history = Compaction.messagesFromCells (kept :> IReadOnlyList<SessionCell>)
        history.Add(ChatMessage(ChatRole.System, CrashResumptionNote))
        history

    /// Rehydrates a crash-interrupted turn's history from the journal: pages
    /// Replay from cursor 0 through the shared transcript read (which folds
    /// every turn through SessionCellDeriver.Fold), drops the interrupted
    /// turn's tool cells, and appends the in-memory resumption note. The
    /// journal is append-only: old-attempt events stay, nothing journals.
    /// Unknown session, expired journal, and end of stream read as the note
    /// alone, so recovery never fails spuriously.
    /// <param name="eventStore">The journal to replay.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to rehydrate.</param>
    /// <param name="interruptedTurn">The interrupted turn whose tool cells drop.</param>
    /// <returns>The rehydrated history with the resumption note.</returns>
    let rehydrateCrashHistory
        (eventStore: ISessionEventStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (interruptedTurn: TurnId)
        : IList<ChatMessage> =
        ArgumentNullException.ThrowIfNull(eventStore)

        let options = ReadTranscriptOptions()

        let cells =
            awaitTask (Transcripts.readTranscript eventStore tenant sessionId options 100 CancellationToken.None)

        rehydrateHistoryFromCells cells interruptedTurn

    /// Reads the interrupted turn id as the journal's most recent turn: the
    /// last event's turn in sequence order. Returns None when the journal
    /// carries no events, so the caller falls back to a fresh turn id.
    /// <param name="eventStore">The journal to replay.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to inspect.</param>
    /// <returns>The most recent turn id, or None on an empty journal.</returns>
    let lastJournalTurnId (eventStore: ISessionEventStore) (tenant: TenantId) (sessionId: SessionId) : TurnId option =
        ArgumentNullException.ThrowIfNull(eventStore)

        let rec replay cursor (last: TurnId option) =
            let outcome =
                awaitTask (eventStore.Replay(tenant, sessionId, cursor, 100, CancellationToken.None))

            match outcome with
            | :? EventReplayPage as page when not (isNull (box page)) ->
                let mutable current = last
                let mutable nextCursor = cursor

                if not (isNull (box page.Events)) then
                    for event in page.Events do
                        if not (isNull (box event)) then
                            current <- Some event.TurnId

                    if page.NextCursor.HasValue then
                        nextCursor <- page.NextCursor.Value

                if page.NextCursor.HasValue then
                    replay nextCursor current
                else
                    current
            | _ -> last

        replay 0L None

    /// Reads whether the journal tail holds an unterminated turn (issue
    /// 287): the last TurnStartedEvent with no terminal after it
    /// (TurnCompleted, TurnFailed, TurnAborted, or SessionClosed). A
    /// marker-only journal (a single TurnStartedEvent, the mid-LLM-call kill
    /// shape) reads as true; an empty journal reads as false, so a Running
    /// row with no work to recover still idles instead of failing
    /// spuriously. Completion journals nothing, so this check runs only for
    /// Running rows with an empty inbox: Idle rows (including healthy
    /// completions, which also end marker-only) never consult it, and a
    /// settled orphan flips to Idle with its TurnFailedEvent terminal, so a
    /// later restart reads false and stays quiet. Read-only: never appends.
    /// <param name="eventStore">The journal to replay.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to inspect.</param>
    /// <returns>True when the tail shows an unterminated turn.</returns>
    let hasUnterminatedTurnTail (eventStore: ISessionEventStore) (tenant: TenantId) (sessionId: SessionId) : bool =
        ArgumentNullException.ThrowIfNull(eventStore)

        let rec replay cursor (started: bool) =
            let outcome =
                try
                    awaitTask (eventStore.Replay(tenant, sessionId, cursor, 100, CancellationToken.None))
                with _ ->
                    Unchecked.defaultof<EventReplayOutcome>

            match outcome with
            | :? EventReplayPage as page when not (isNull (box page)) ->
                let mutable current = started
                let mutable nextCursor = cursor

                if not (isNull (box page.Events)) then
                    for event in page.Events do
                        if not (isNull (box event)) then
                            match event with
                            | :? TurnStartedEvent -> current <- true
                            | :? TurnCompletedEvent -> current <- false
                            | :? TurnFailedEvent -> current <- false
                            | :? TurnAbortedEvent -> current <- false
                            | :? SessionClosedEvent -> current <- false
                            | _ -> ()

                    if page.NextCursor.HasValue then
                        nextCursor <- page.NextCursor.Value

                if page.NextCursor.HasValue then
                    replay nextCursor current
                else
                    current
            | _ -> started

        replay 0L false

    /// The suspendable session actor: like behavior but driving the
    /// suspendable runner, entering WaitingForInput store-first on suspend,
    /// matching Reply ids with the typed error, resuming from the cursor
    /// with attempt plus 1, settling Failed with TurnFailed on AskTimeout,
    /// and rebuilding the pending request from the journal on restart. The
    /// claim heartbeat is never cancelled on suspend: renewal continues
    /// while suspended under the same claim (proven by test; no new
    /// background work, bounded by lease expiry and AskTimeout). Reply
    /// never starts a turn: it only resumes the suspended one. Inject folds
    /// at the next iteration boundary through the runner's drain hooks and
    /// Interrupt pre-empts through the ExplicitAbort pending stop; the
    /// detached suspendable turn runs un-cancellable, so a recorded stop
    /// wins at its next report. Abort on WaitingForInput stays a no-op per
    /// #35, as does Compact there.
    /// <param name="props">The base session actor dependencies.</param>
    /// <param name="suspend">The suspend dependencies.</param>
    /// <param name="mailbox">The actor mailbox, injected by spawn.</param>
    /// <returns>The Akka.FSharp actor computation to spawn.</returns>
    let behaviorWithSuspendRouted
        (validateRoute: unit -> unit)
        (props: SessionActorProps)
        (suspend: SuspendDeps)
        (mailbox: Actor<SuspendableActorMessage>)
        =
        validateRoute ()

        if isNull (box props.Store) then
            raise (ArgumentNullException(nameof props))

        if isNull (box props.RunTurn) then
            raise (ArgumentNullException(nameof props))

        if isNull (box suspend.EventStore) then
            raise (ArgumentNullException(nameof suspend))

        if isNull (box suspend.Delay) then
            raise (ArgumentNullException(nameof suspend))

        if isNull (box suspend.JournalToken) then
            raise (ArgumentNullException(nameof suspend))

        if isNull (box suspend.RunSuspendable) then
            raise (ArgumentNullException(nameof suspend))

        if isNull (box suspend.EraMarked) then
            raise (ArgumentNullException(nameof suspend))

        if suspend.AskTimeout <= TimeSpan.Zero then
            raise (ArgumentOutOfRangeException(nameof suspend, "SuspendDeps.AskTimeout must be positive."))

        match props.Compact with
        | Some compact -> requireCompactDeps compact
        | None -> ()

        let suspendSelf = mailbox.Self

        /// Reads the session's persisted AllowForSession memory: the grant
        /// tool names the store row carries. A missing row or a null grant
        /// list reads as empty, so a stored row from before grants existed
        /// resumes with no memory and the persisted set always wins over the
        /// actor's in-memory copy on rebuild.
        /// <returns>The granted tool names.</returns>
        let readGrantsNow () : HashSet<string> =
            try
                match awaitTask (props.Store.GetSession(props.Tenant, props.SessionId, CancellationToken.None)) with
                | null -> HashSet<string>()
                | session when isNull (box session.PermissionGrants) -> HashSet<string>()
                | session -> HashSet<string>(session.PermissionGrants :> seq<string>)
            with :? SessionNotFoundException ->
                HashSet<string>()

        let crashKnobOf (session: Session) : OnCrashResume =
            if isNull (box session) || isNull (box session.Options) then
                OnCrashResume.FailAttempt
            else
                session.Options.OnCrashResume

        // The journal token fencing this actor's journal appends: the
        // spawn-primed token at first, replaced by the SetAgent swap with
        // each re-prime. Every journal write below reads this cell, never
        // SuspendDeps.JournalToken, so post-swap writes stay live. A cell
        // rather than a loop parameter, like pendingStop below.
        let mutable journalToken = suspend.JournalToken
        let mutable controlPrime = suspend.PrimeClaim
        let controlReports = Dictionary<int64, TurnId * TurnClaim * string>()
        let completedControlReports = HashSet<string>()

        let controlStore =
            match props.Store with
            | :? ISessionAbortControlStore as control -> Some control
            | _ when suspend.PrimeClaim.IsNone -> None
            | _ -> raise (InvalidOperationException("ISessionAbortControlStore is required."))

        let controlAdmission turn position claim () =
            match controlStore with
            | None -> true
            | Some control ->
                let checkedTarget =
                    awaitTask (
                        control.CheckControlTarget(
                            props.Tenant,
                            props.SessionId,
                            turn,
                            position,
                            claim,
                            CancellationToken.None
                        )
                    )

                checkedTarget.Outcome = ControlOperationOutcome.Applied

        let bindControl (entry: InboxEntry) =
            match controlStore, controlPrime with
            | Some control, Some claim ->
                // Real-turn identity (issue 374): the durable TurnId stamped
                // at accept owns execution; the in-memory report reuses it so
                // restart and recovery agree with the store. Legacy entries
                // without a stamped identity mint once here.
                let turn =
                    match controlReports.TryGetValue entry.Position with
                    | true, (turn, _, _) -> turn
                    | _ ->
                        if isNull (box entry.TurnId.Value) then
                            TurnId.New()
                        else
                            entry.TurnId

                let bound =
                    awaitTask (
                        control.BindControlTarget(
                            props.Tenant,
                            props.SessionId,
                            turn,
                            entry.Position,
                            claim,
                            CancellationToken.None
                        )
                    )

                if bound.Outcome <> ControlOperationOutcome.Applied then
                    raise (
                        InvalidSessionStateException(
                            props.SessionId,
                            "controlPending",
                            "Current target refused execution admission."
                        )
                    )

                controlReports[entry.Position] <- turn, claim, Guid.NewGuid().ToString("N")
                Some turn
            | _ -> None

        let decideControl (entry: InboxEntry) (candidate: TurnResult) =
            match controlStore, controlReports.TryGetValue entry.Position with
            | Some control, (true, (turn, claim, id)) ->
                if completedControlReports.Contains id then
                    raise (
                        InvalidSessionStateException(
                            props.SessionId,
                            "duplicateControlReport",
                            "A duplicate report cannot publish or drain."
                        )
                    )

                let cause, reason =
                    match candidate.Outcome with
                    | :? TurnAborted as stop -> Nullable stop.Cause, (stop.Reason: string | null)
                    | _ when candidate.Status = TurnStatus.Aborted ->
                        Nullable StopCause.ExplicitAbort, (InterruptReason: string | null)
                    | _ -> Nullable(), null

                let decided =
                    awaitTask (
                        control.TryDecideControlTarget(
                            props.Tenant,
                            props.SessionId,
                            turn,
                            entry.Position,
                            claim,
                            id,
                            candidate.Status,
                            cause,
                            reason,
                            CancellationToken.None
                        )
                    )

                match decided.Outcome, decided.Decision with
                | ControlOperationOutcome.Applied, evidence ->
                    match evidence with
                    | null -> raise (InvalidOperationException("Applied control decision has no evidence."))
                    | evidence when evidence.Status = TurnStatus.Aborted ->
                        { candidate with
                            Status = TurnStatus.Aborted
                            Outcome =
                                TurnAborted(
                                    evidence.Cause.Value,
                                    evidence.Reason |> Option.ofObj |> Option.defaultValue ""
                                )
                                :> TurnOutcome
                        }
                    | evidence ->
                        { candidate with
                            Status = evidence.Status
                        }
                | _ ->
                    raise (
                        InvalidSessionStateException(
                            props.SessionId,
                            "controlPending",
                            "Control decision refused this report; no downstream effects are authorized."
                        )
                    )
            | _ -> candidate

        let retireControl (entry: InboxEntry) =
            match controlStore, controlReports.TryGetValue entry.Position with
            | Some control, (true, (turn, claim, id)) ->
                let retired =
                    awaitTask (
                        control.RetireControlTarget(
                            props.Tenant,
                            props.SessionId,
                            turn,
                            entry.Position,
                            claim,
                            id,
                            CancellationToken.None
                        )
                    )

                if retired.Outcome <> ControlOperationOutcome.Applied then
                    raise (
                        InvalidSessionStateException(
                            props.SessionId,
                            "controlPending",
                            "Control retirement refused further settlement or queue drain."
                        )
                    )

                completedControlReports.Add id |> ignore
            | _ -> ()

        let durableStop () =
            match controlStore, controlPrime with
            | Some control, Some _ ->
                match awaitTask (control.ReadAbortTarget(props.Tenant, props.SessionId, CancellationToken.None)) with
                | null -> None
                | target ->
                    match target.Stop with
                    | null -> None
                    | stop -> Some(stop.Cause.Value, stop.Reason |> Option.ofObj |> Option.defaultValue "")
            | _ -> None

        // The on-demand compaction wiring the Idle compact path runs: the
        // spawn wiring at first, refreshed by the SetAgent swap with each
        // re-prime, so later Compacts journal under the live token.
        let mutable currentCompact = props.Compact

        // The turn id the running attempt executes as (issue 289): set by
        // startSuspendable and resumeSuspendable, cleared when the attempt
        // reports. The SuspendableFinished/Faulted handlers resolve the
        // settling id from the carried id, then this cell, then the
        // CurrentTurnId snapshot, else journal nothing.
        let mutable runningTurnId: TurnId option = None

        /// Snapshots the session row's live turn, or None when the row is
        /// missing, the turn settled to null, or the read fails.
        /// <returns>The live turn id, or None.</returns>
        let currentTurnSnapshot () : TurnId option =
            try
                match awaitTask (props.Store.GetSession(props.Tenant, props.SessionId, CancellationToken.None)) with
                | null -> None
                | session when session.CurrentTurnId.HasValue -> Some session.CurrentTurnId.Value
                | _ -> None
            with _ ->
                None

        /// Resolves the settling turn id (issue 289): the
        /// completion-carried id, then the running attempt's cell, then
        /// the CurrentTurnId snapshot, else None (journal nothing).
        /// <param name="carried">The completion-carried turn id.</param>
        /// <returns>The settling turn id, or None.</returns>
        let resolveSettlingTurnId (carried: TurnId) : TurnId option =
            if not (carried.Equals(Unchecked.defaultof<TurnId>)) then
                Some carried
            else
                match runningTurnId with
                | Some live -> Some live
                | None -> currentTurnSnapshot ()

        /// Re-primes the journal through the spawn wiring: a fresh bootstrap
        /// plus ClaimNextTurn, or None when the host never re-primes, the
        /// prime fails, or a live claim is held (takeover, or a restart
        /// inside the old prime's lease). Total: a throwing prime reads as
        /// None and the recorded rebind retries at the next boundary.
        /// <returns>The live claim, or None.</returns>
        let reprimeNow () : TurnClaim option =
            match suspend.ReprimeJournal with
            | None -> None
            | Some reprime ->
                try
                    reprime ()
                with _ ->
                    None

        /// Settles a claim Completed with a null outcome, best-effort: the
        /// first settle wins and a retry of the same outcome observes it, so
        /// only the settled/already-settled outcomes read as settled. Total:
        /// a rejected or faulted settle reads as false.
        /// <param name="claim">The claim fencing the settlement. Must not be null.</param>
        /// <returns>True when the turn settled.</returns>
        let settleTurnQuiet (claim: TurnClaim) : bool =
            try
                match
                    awaitTask (
                        props.Store.SettleTurn(props.Tenant, claim, TurnStatus.Completed, null, CancellationToken.None)
                    )
                with
                | :? TurnSettled -> true
                | :? TurnAlreadySettled -> true
                | _ -> false
            with _ ->
                false

        /// Settles the primed journal claim the spawn (or a re-prime)
        /// holds: synthesizes the claim from the row's CurrentTurnId stamp
        /// plus the token cell, so the settle needs no plumbed claim object
        /// and survives restarts. Live or lapsed-but-uncontested it clears
        /// CurrentTurnIds; a stale claim rejects with no effects. Total: the
        /// outcome is advisory (the re-prime below gates the apply), so
        /// every failure is swallowed.
        let settlePrimedNow () : unit =
            try
                match awaitTask (props.Store.GetSession(props.Tenant, props.SessionId, CancellationToken.None)) with
                | null -> ()
                | session ->
                    if session.CurrentTurnId.HasValue then
                        let claim =
                            {
                                TurnId = session.CurrentTurnId.Value
                                Token = journalToken
                                Owner = ""
                                ExpiresAt = DateTimeOffset.MinValue
                                Attempt = 1
                            }

                        settleTurnQuiet claim |> ignore
            with _ ->
                ()

        /// Settles the prompt turn's prime claim at quiescence (issue 313):
        /// the Completed-to-Idle path releases the facade (spawn or
        /// re-prime) prime exactly once through the fenced settle,
        /// synthesizing the claim from the row's CurrentTurnId stamp plus
        /// the live journal token cell (the settlePrimedNow precedent), so
        /// the settle needs no plumbed claim object and survives restarts.
        /// A live prime settles (settled or already-settled); a stale
        /// claim (a takeover winner holds the turn) verifies fenced-out
        /// with zero effects. Total: the outcome is advisory (quiescence
        /// needs no branch), so every failure is swallowed.
        let settleCompletedPrimeNow () : unit =
            try
                match currentTurnSnapshot () with
                | None -> ()
                | Some turnId ->
                    let claim =
                        {
                            TurnId = turnId
                            Token = journalToken
                            Owner = ""
                            ExpiresAt = DateTimeOffset.MinValue
                            Attempt = 1
                        }

                    awaitTask (
                        ClaimFence.settleTurnAsync
                            props.Store
                            props.Tenant
                            claim
                            TurnStatus.Completed
                            null
                            CancellationToken.None
                    )
                    |> ignore
            with _ ->
                ()

        let failInterruptedTurn (liveId: TurnId option) (entryOpt: InboxEntry option) : unit =
            let candidate =
                {
                    AssistantText = ""
                    Status = TurnStatus.Failed
                    Iterations = 0
                    Usage = { InputTokens = 0L; OutputTokens = 0L }
                    Outcome = TurnFailed(CrashFailReason) :> TurnOutcome
                }

            let fenced =
                entryOpt
                |> Option.exists (fun entry -> controlReports.ContainsKey entry.Position)

            let result =
                match entryOpt with
                | Some entry when fenced -> decideControl entry candidate
                | _ -> candidate

            if fenced then
                match entryOpt with
                | Some entry ->
                    awaitTask (
                        props.Store.MarkInboxConsumed(
                            props.Tenant,
                            props.SessionId,
                            [| entry.Position |],
                            CancellationToken.None
                        )
                    )
                    |> ignore
                | None -> ()

            match props.OnTurnSettled with
            | Some observe ->
                try
                    observe result
                with _ ->
                    ()
            | None -> ()

            dispatchCompletion props result |> ignore

            match entryOpt with
            | Some _ when fenced -> ()
            | Some entry ->
                let positions = [| entry.Position |] :> IReadOnlyList<int64>

                try
                    awaitTask (
                        props.Store.MarkInboxConsumed(props.Tenant, props.SessionId, positions, CancellationToken.None)
                    )
                    |> ignore
                with _ ->
                    ()
            | None -> ()

            if fenced then
                match liveId with
                | Some turnId ->
                    let event: SessionEvent =
                        match result.Outcome with
                        | :? TurnAborted as stop ->
                            TurnAbortedEvent(
                                props.SessionId,
                                turnId,
                                Nullable(),
                                DateTimeOffset.UtcNow,
                                stop.Cause,
                                stop.Reason
                            )
                        | _ ->
                            TurnFailedEvent(props.SessionId, turnId, Nullable(), DateTimeOffset.UtcNow, CrashFailReason)

                    awaitTask (
                        JournalWriter.appendWithTokenAsync
                            suspend.EventStore
                            props.Tenant
                            props.SessionId
                            journalToken
                            [| event |]
                            CancellationToken.None
                    )
                    |> ignore
                | None -> ()

                match entryOpt with
                | Some entry -> retireControl entry
                | None -> ()

                awaitTask (
                    props.Store.UpdateSessionState(
                        props.Tenant,
                        props.SessionId,
                        SessionState.Idle,
                        CancellationToken.None
                    )
                )
                |> ignore
            else
                try
                    awaitTask (
                        props.Store.UpdateSessionState(
                            props.Tenant,
                            props.SessionId,
                            SessionState.Idle,
                            CancellationToken.None
                        )
                    )
                    |> ignore
                with _ ->
                    ()

            // The crash-path terminal (issue 289): the CurrentTurnId
            // snapshot the crash path never ran a loop for. Fenced under
            // the live journal token; a settled-NULL turn (None) journals
            // nothing. Stays on the prime (issue 373): the write terminates
            // an execution turn, so only the claim fence proves the writer
            // is no takeover loser; the host path cannot tell a stale
            // generation from the live one. Re-anchoring belongs to #400.
            match liveId with
            | Some _ when fenced -> ()
            | Some turnId ->
                try
                    let failedEvent =
                        TurnFailedEvent(
                            props.SessionId,
                            turnId,
                            Nullable<int64>(),
                            DateTimeOffset.UtcNow,
                            CrashFailReason
                        )
                        :> SessionEvent

                    let events =
                        ResizeArray<SessionEvent>([| failedEvent |]) :> IReadOnlyList<SessionEvent>

                    awaitTask (
                        JournalWriter.appendWithTokenAsync
                            suspend.EventStore
                            props.Tenant
                            props.SessionId
                            journalToken
                            events
                            CancellationToken.None
                    )
                    |> ignore
                with _ ->
                    ()
            | None -> ()

        /// Reads the journal state for one turn id (issue 289): whether the
        /// journal holds the TurnStartedEvent marker for the id, whether
        /// any terminal row (TurnCompleted, TurnFailed, TurnAborted,
        /// SessionClosed) carries it, and the latest marker id with no
        /// terminal row (the journal-derived orphan). Read-only: never
        /// appends. A failed replay reads as no marker, so the probe stays
        /// quiet.
        ///
        /// The fallback exists because a winning prime replaces the row's
        /// turn id: after the dispatcher's poke-claim (or the spawn prime)
        /// wins post-expiry, CurrentTurnId names the prime, never the
        /// orphan, so marker-for-live-id alone would stay quiet forever.
        /// The orphan id survives only in the journal.
        /// <param name="liveId">The live turn to inspect.</param>
        /// <returns>The marker flag and terminal flag for the live id, plus the latest unterminated marker id.</returns>
        let journalOrphanState (liveId: TurnId) : bool * bool * TurnId option =
            let rec replay
                cursor
                (marker: bool)
                (terminal: bool)
                (markers: ResizeArray<TurnId>)
                (terminals: HashSet<TurnId>)
                =
                let outcome =
                    try
                        awaitTask (
                            suspend.EventStore.Replay(
                                props.Tenant,
                                props.SessionId,
                                cursor,
                                100,
                                CancellationToken.None
                            )
                        )
                    with _ ->
                        Unchecked.defaultof<EventReplayOutcome>

                match outcome with
                | :? EventReplayPage as page when not (isNull (box page)) ->
                    let mutable foundMarker = marker
                    let mutable foundTerminal = terminal
                    let mutable nextCursor = cursor

                    if not (isNull (box page.Events)) then
                        for event in page.Events do
                            if not (isNull (box event)) then
                                if event.TurnId.Equals(liveId) then
                                    match event with
                                    | :? TurnStartedEvent -> foundMarker <- true
                                    | :? TurnCompletedEvent
                                    | :? TurnFailedEvent
                                    | :? TurnAbortedEvent
                                    | :? SessionClosedEvent -> foundTerminal <- true
                                    | _ -> ()

                                match event with
                                | :? TurnStartedEvent -> markers.Add(event.TurnId)
                                | :? TurnCompletedEvent
                                | :? TurnFailedEvent
                                | :? TurnAbortedEvent
                                | :? SessionClosedEvent -> terminals.Add(event.TurnId) |> ignore
                                | _ -> ()

                        if page.NextCursor.HasValue then
                            nextCursor <- page.NextCursor.Value

                    if page.NextCursor.HasValue then
                        replay nextCursor foundMarker foundTerminal markers terminals
                    else
                        let fallback =
                            markers
                            |> Seq.filter (fun candidate -> not (terminals.Contains(candidate)))
                            |> Seq.tryLast

                        foundMarker, foundTerminal, fallback
                | _ ->
                    let fallback =
                        markers
                        |> Seq.filter (fun candidate -> not (terminals.Contains(candidate)))
                        |> Seq.tryLast

                    marker, terminal, fallback

            replay 0L false false (ResizeArray<TurnId>()) (HashSet<TurnId>())

        /// Reads whether the live turn is an orphan (issue 289), in order:
        /// the settling id (the live id when its marker stands unterminated,
        /// else the journal-derived orphan the prime replaced), an
        /// era-marked session, and a won re-prime (a live lease held
        /// elsewhere reads quiet). The won claim's token is adopted into
        /// the journal cell so fenced writes land; the caller settles the
        /// prime quietly after failing the orphan (a never-ran prime
        /// journals nothing).
        /// <param name="liveId">The live turn to probe.</param>
        /// <returns>The settling turn id with the won prime claim, or None when the session stays quiet.</returns>
        let probeOrphanTurn (liveId: TurnId) : (TurnId * TurnClaim) option =
            let marker, terminal, fallback = journalOrphanState liveId

            let settling = if marker && not terminal then Some liveId else fallback

            match settling with
            | None -> None
            | Some settlingId ->
                let marked =
                    try
                        awaitTask (suspend.EraMarked props.Tenant props.SessionId CancellationToken.None)
                    with _ ->
                        false

                if not marked then
                    None
                else
                    match reprimeNow () with
                    | None -> None
                    | Some prime ->
                        journalToken <- prime.Token
                        controlPrime <- Some prime
                        Some(settlingId, prime)

        let originalRecoveryEntry () =
            match suspend.Recovery with
            | null ->
                try
                    awaitTask (props.Store.ReadPendingInbox(props.Tenant, props.SessionId, CancellationToken.None))
                    |> selectDrainableEntries
                    |> List.tryHead
                with _ ->
                    None
            | recovery -> recovery.Entry |> Option.ofObj

        let initialRecovered: SessionState * RebuiltPending option * InboxEntry option =
            match controlStore, controlPrime with
            | Some control, Some _ ->
                match awaitTask (control.ReadAbortTarget(props.Tenant, props.SessionId, CancellationToken.None)) with
                | null -> ()
                | target ->
                    match target, suspend.Recovery with
                    | _, null ->
                        raise (
                            InvalidSessionStateException(
                                props.SessionId,
                                "controlPending",
                                "Recovery requires a provider-fenced original association."
                            )
                        )
                    | target, recovery when target.State = ControlTargetState.Active && isNull (box target.Stop) ->
                        match recovery.Claim, recovery.Entry with
                        | null, _
                        | _, null ->
                            raise (
                                InvalidSessionStateException(
                                    props.SessionId,
                                    "controlPending",
                                    "Recovery has no original association or authority."
                                )
                            )
                        | claim, _ ->
                            if not (controlAdmission target.TurnId target.InboxPosition claim ()) then
                                raise (
                                    InvalidSessionStateException(
                                        props.SessionId,
                                        "controlPending",
                                        "Recovery admission lost its exact authority or observed stop."
                                    )
                                )

                            controlReports[target.InboxPosition] <- target.TurnId, claim, Guid.NewGuid().ToString("N")
                    | _ ->
                        raise (
                            InvalidSessionStateException(
                                props.SessionId,
                                "controlPending",
                                "Persisted attribution blocks fresh recovery; no synthetic prime or settlement is authorized."
                            )
                        )
            | _ -> ()

            let found =
                awaitTask (props.Store.GetSession(props.Tenant, props.SessionId, CancellationToken.None))

            match found with
            | null -> SessionState.Idle, None, None
            | session ->
                // The crash-path terminal id (issue 289): the live turn
                // the crash interrupted, or None for a settled-NULL row
                // (which journals nothing).
                let liveId =
                    match suspend.Recovery with
                    | null when session.CurrentTurnId.HasValue -> Some session.CurrentTurnId.Value
                    | null -> None
                    | recovery -> recovery.Target |> Option.ofObj |> Option.map _.TurnId

                match session.State with
                | SessionState.Running ->
                    let drainable = originalRecoveryEntry ()

                    match crashKnobOf session with
                    | OnCrashResume.FailAttempt ->
                        match drainable with
                        | Some _ ->
                            failInterruptedTurn liveId drainable
                            SessionState.Idle, None, None
                        | None ->
                            // Marker-only orphan (issue 287): the claim
                            // consumed the inbox before the mid-LLM-call
                            // kill, so no drainable remains but the journal
                            // tail shows an unterminated turn. Fail fenced
                            // under the fresh primed token. An empty tail
                            // (no TurnStarted) idles instead of failing
                            // spuriously.
                            let orphaned =
                                try
                                    hasUnterminatedTurnTail suspend.EventStore props.Tenant props.SessionId
                                with _ ->
                                    false

                            if orphaned then
                                failInterruptedTurn liveId None
                                SessionState.Idle, None, None
                            else
                                awaitTask (
                                    props.Store.UpdateSessionState(
                                        props.Tenant,
                                        props.SessionId,
                                        SessionState.Idle,
                                        CancellationToken.None
                                    )
                                )
                                |> ignore

                                SessionState.Idle, None, None
                    | _ ->
                        match drainable with
                        | Some entry -> SessionState.Running, None, Some entry
                        | None ->
                            // Resume knob with an empty inbox and an
                            // unterminated tail cannot restart: the consumed
                            // claim took the only message and the
                            // marker-only journal carries no UserMessage to
                            // retry from, so a seedless restart has no entry
                            // to run. Fail fenced to settle the orphan
                            // (the smoke default Fail terminal); restart
                            // stays available whenever a drainable entry
                            // exists. An empty tail idles as before.
                            let orphaned =
                                try
                                    hasUnterminatedTurnTail suspend.EventStore props.Tenant props.SessionId
                                with _ ->
                                    false

                            if orphaned then
                                failInterruptedTurn liveId None
                                SessionState.Idle, None, None
                            else
                                awaitTask (
                                    props.Store.UpdateSessionState(
                                        props.Tenant,
                                        props.SessionId,
                                        SessionState.Idle,
                                        CancellationToken.None
                                    )
                                )
                                |> ignore

                                SessionState.Idle, None, None
                | SessionState.WaitingForInput ->
                    let rebuilt =
                        rebuildPendingFromJournal suspend.EventStore props.Tenant props.SessionId

                    SessionState.WaitingForInput, rebuilt, None
                | SessionState.Idle ->
                    // Live-turn orphan probe (issue 289): an Idle row still
                    // carrying a live turn whose journal holds an
                    // unterminated marker — for the live id itself, or for
                    // the orphan id a won prime replaced (the marker is the
                    // only surviving record of it) — on an era-marked
                    // session with no live lease elsewhere, enters the
                    // Running-branch shape so entity start runs the
                    // journal-tail recovery. Settled-NULL, terminated,
                    // empty-tail, live-lease, and pre-era sessions all stay
                    // quiet with the turn preserved; the losing branch
                    // journals nothing (loser-zero-effects).
                    if session.CurrentTurnId.HasValue then
                        let liveId = session.CurrentTurnId.Value

                        match probeOrphanTurn liveId with
                        | None -> SessionState.Idle, None, None
                        | Some(settlingId, prime) ->
                            let drainable =
                                try
                                    let pending =
                                        awaitTask (
                                            props.Store.ReadPendingInbox(
                                                props.Tenant,
                                                props.SessionId,
                                                CancellationToken.None
                                            )
                                        )

                                    selectDrainableEntries pending |> List.tryHead
                                with _ ->
                                    None

                            match drainable with
                            | Some entry when crashKnobOf session <> OnCrashResume.FailAttempt ->
                                SessionState.Running, None, Some entry
                            | _ ->
                                failInterruptedTurn (Some settlingId) drainable
                                settleTurnQuiet prime |> ignore
                                SessionState.Idle, None, None
                    else
                        SessionState.Idle, None, None
                | SessionState.Closed -> SessionState.Closed, None, None
                | unknown -> unknown, None, None

        let initialState, initialRebuilt, initialResumeEntry = initialRecovered

        let initialSuspended: SuspendedTurn option =
            match initialState, initialRebuilt with
            | SessionState.WaitingForInput, Some rebuilt ->
                // Crash rebuild: no cursor and no running task; the matching
                // Reply retries from the oldest drainable entry. The
                // timeout is not restarted here: the AskTimeout bound restarts
                // when the retried turn suspends again, so a restarted host
                // never inherits a fired deadline.
                let queueEntry = originalRecoveryEntry ()

                match queueEntry with
                | Some entry ->
                    Some
                        {
                            Entry = entry
                            TurnId =
                                match suspend.Recovery with
                                | null -> currentTurnSnapshot () |> Option.defaultValue Unchecked.defaultof<TurnId>
                                | recovery ->
                                    recovery.Target
                                    |> Option.ofObj
                                    |> Option.map _.TurnId
                                    |> Option.defaultValue Unchecked.defaultof<TurnId>
                            Cursor = None
                            Rebuilt = Some rebuilt
                            Allowed = readGrantsNow ()
                            Attempt = controlPrime |> Option.map _.Attempt |> Option.defaultValue 1
                            TimeoutCts = new CancellationTokenSource()
                        }
                | None -> None
            | _ -> None

        let pendingCountNow () : int =
            try
                let pending =
                    awaitTask (props.Store.ReadPendingInbox(props.Tenant, props.SessionId, CancellationToken.None))

                if isNull (box pending) then 0 else pending.Count
            with :? SessionNotFoundException ->
                0

        let takeSuspendSnapshot (state: SessionState) (suspended: SuspendedTurn option) : SessionSnapshot =
            let pendingId: string | null =
                match suspended with
                | Some parked ->
                    match parked.Cursor with
                    | Some cursor -> cursor.RequestId
                    | None ->
                        match parked.Rebuilt with
                        | Some rebuilt -> rebuilt.RequestId
                        | None -> null
                | None -> null

            {
                SessionId = props.SessionId
                State = state
                PendingCount = pendingCountNow ()
                RunningPosition = None
                PendingRequestId = pendingId
            }

        /// Maps a reported result to the Aborted result a won stop settles:
        /// the stop cause wins over whatever the detached turn reported,
        /// even a success, so settlement and stop stay mutually exclusive.
        /// Falls back to the carried result when the cause maps to no
        /// settlement (only abort-family causes reach the pending stop, so
        /// this never fires).
        /// <param name="cause">The stop cause that won.</param>
        /// <param name="reason">Why the turn stopped.</param>
        /// <param name="result">The result the turn reported.</param>
        /// <returns>The result the actor settles.</returns>
        let mapSuspendAborted (cause: StopCause) (reason: string) (result: TurnResult) : TurnResult =
            match StopArbitration.settlementFor cause reason with
            | Some(status, outcome) ->
                { result with
                    Status = status
                    Outcome = outcome
                }
            | None -> result

        /// Builds the Aborted result a won stop settles when the turn left
        /// no result behind (a faulted attempt): zero iterations and usage,
        /// the abort-family outcome carrying who and why. Falls back to a
        /// Failed result when the cause maps to no settlement (only
        /// abort-family causes reach the pending stop, so this never fires).
        /// <param name="cause">The stop cause that won.</param>
        /// <param name="reason">Why the turn stopped.</param>
        /// <returns>The result the actor settles.</returns>
        let abortedSuspendResult (cause: StopCause) (reason: string) : TurnResult =
            match StopArbitration.settlementFor cause reason with
            | Some(status, outcome) ->
                {
                    AssistantText = ""
                    Status = status
                    Iterations = 0
                    Usage = { InputTokens = 0L; OutputTokens = 0L }
                    Outcome = outcome
                }
            | None ->
                {
                    AssistantText = ""
                    Status = TurnStatus.Failed
                    Iterations = 0
                    Usage = { InputTokens = 0L; OutputTokens = 0L }
                    Outcome = TurnFailed(reason) :> TurnOutcome
                }

        let notifySettled (result: TurnResult) : unit =
            match props.OnTurnSettled with
            | Some observe ->
                try
                    observe result
                with _ ->
                    ()
            | None -> ()

        /// Settles a turn whose suspend/resolve journal write never landed
        /// as Failed with the typed reason: parking or resuming would strand
        /// the turn on a missing journal event. Consumes the entry and
        /// returns the session to Idle with the Failed result observed,
        /// mirroring the AskTimeout settle.
        /// <param name="entry">The turn's inbox entry to consume.</param>
        /// <param name="reason">Why the turn failed. Never contains secrets or tool arguments.</param>
        let settleJournalFailure (entry: InboxEntry) (reason: string) : unit =
            let result =
                {
                    AssistantText = ""
                    Status = TurnStatus.Failed
                    Iterations = 0
                    Usage = { InputTokens = 0L; OutputTokens = 0L }
                    Outcome = TurnFailed(reason) :> TurnOutcome
                }

            let result = decideControl entry result

            let positions = [| entry.Position |] :> IReadOnlyList<int64>

            awaitTask (props.Store.MarkInboxConsumed(props.Tenant, props.SessionId, positions, CancellationToken.None))
            |> ignore

            notifySettled result
            dispatchCompletion props result |> ignore
            retireControl entry

            awaitTask (
                props.Store.UpdateSessionState(props.Tenant, props.SessionId, SessionState.Idle, CancellationToken.None)
            )
            |> ignore

        /// Journals the in-call marker for one turn entering its first
        /// provider call (issue 284): a TurnStartedEvent under the given
        /// journal token through the fenced writer. The caller snapshots
        /// the live token at turn start and passes the snapshot, so a
        /// takeover between snapshot and append still fences out: the store
        /// rejects the stale token. Appended proceeds to the provider call;
        /// a stale-token rejection raises TurnLeaseLostException so the
        /// takeover loser stops before the provider call with zero effects;
        /// a failed write (a persistent store fault past the bounded
        /// retries) returns silently and the turn proceeds unmarked:
        /// best-effort observability, since completion journals nothing and
        /// stays valid without the marker.
        /// <param name="eventStore">The journal the marker appends to.</param>
        /// <param name="tenant">The tenant the session belongs to.</param>
        /// <param name="sessionId">The session whose journal appends.</param>
        /// <param name="token">The journal token snapshot fencing the write.</param>
        /// <param name="turnId">The turn entering its first provider call.</param>
        /// <param name="cancellationToken">Abandons the append.</param>
        let journalTurnStartedAsync
            (eventStore: ISessionEventStore)
            (tenant: TenantId)
            (sessionId: SessionId)
            (token: string)
            (turnId: TurnId)
            (cancellationToken: CancellationToken)
            : Task<unit> =
            task {
                let marker =
                    TurnStartedEvent(sessionId, turnId, Unchecked.defaultof<Nullable<int64>>, DateTimeOffset.UtcNow)
                    :> SessionEvent

                let batch = ResizeArray<SessionEvent>([| marker |]) :> IReadOnlyList<SessionEvent>

                match! JournalWriter.appendWithTokenAsync eventStore tenant sessionId token batch cancellationToken with
                | JournalWriter.JournalAppended _ -> ()
                | JournalWriter.JournalRejected _ -> return raise (TurnLoop.TurnLeaseLostException())
                | JournalWriter.JournalFailed _ -> ()
            }

        let journalSuspend (suspension: TurnLoop.TurnLoopSuspension) : JournalWriter.JournalWriteResult =
            let turnId =
                match controlStore, controlPrime with
                | Some control, Some _ ->
                    match
                        awaitTask (control.ReadAbortTarget(props.Tenant, props.SessionId, CancellationToken.None))
                    with
                    | null ->
                        raise (
                            InvalidSessionStateException(
                                props.SessionId,
                                "missingControlTarget",
                                "Suspension requires current attribution."
                            )
                        )
                    | target -> target.TurnId
                | _ -> TurnId.New()

            let stamp = DateTimeOffset.UtcNow

            let event =
                match suspension.Kind with
                | TurnLoop.PermissionSuspension ->
                    PermissionRequestedEvent(
                        props.SessionId,
                        turnId,
                        Nullable<int64>(),
                        stamp,
                        suspension.RequestId,
                        suspension.ToolName
                    )
                    :> SessionEvent
                | TurnLoop.QuestionSuspension ->
                    QuestionAskedEvent(
                        props.SessionId,
                        turnId,
                        Nullable<int64>(),
                        stamp,
                        suspension.RequestId,
                        suspension.QuestionText
                    )
                    :> SessionEvent

            let events = ResizeArray<SessionEvent>([| event |]) :> IReadOnlyList<SessionEvent>

            // Through the journal writer: sanitized, bounded, fenced on the
            // journal token with bounded retries. The caller branches the
            // result: Appended parks the turn, Rejected/Failed settle it
            // Failed with the typed reason instead.
            awaitTask (
                JournalWriter.appendWithTokenAsync
                    suspend.EventStore
                    props.Tenant
                    props.SessionId
                    journalToken
                    events
                    CancellationToken.None
            )

        let journalResolve (reply: Reply) : JournalWriter.JournalWriteResult =
            let turnId =
                match controlStore, controlPrime with
                | Some control, Some _ ->
                    match
                        awaitTask (control.ReadAbortTarget(props.Tenant, props.SessionId, CancellationToken.None))
                    with
                    | null ->
                        raise (
                            InvalidSessionStateException(
                                props.SessionId,
                                "missingControlTarget",
                                "Reply resolution requires current attribution."
                            )
                        )
                    | target -> target.TurnId
                | _ -> TurnId.New()

            let stamp = DateTimeOffset.UtcNow

            let eventOpt: SessionEvent option =
                match reply with
                | :? PermissionDecision as decision when not (isNull (box decision)) ->
                    PermissionResolvedEvent(
                        props.SessionId,
                        turnId,
                        Nullable<int64>(),
                        stamp,
                        decision.RequestId,
                        decision.Decision
                    )
                    :> SessionEvent
                    |> Some
                | :? QuestionAnswer as answer when not (isNull (box answer)) ->
                    QuestionAnsweredEvent(
                        props.SessionId,
                        turnId,
                        Nullable<int64>(),
                        stamp,
                        answer.QuestionId,
                        answer.Answer
                    )
                    :> SessionEvent
                    |> Some
                | _ -> None

            match eventOpt with
            | None ->
                // No journal shape for this reply kind (Reply carries only
                // the two known subtypes): nothing to append, so the resume
                // proceeds on an empty applied result.
                JournalWriter.JournalAppended(ResizeArray<SessionEvent>() :> IReadOnlyList<SessionEvent>)
            | Some event ->
                let events = ResizeArray<SessionEvent>([| event |]) :> IReadOnlyList<SessionEvent>

                // Through the journal writer, like the suspend event: the
                // caller branches the result instead of resuming on a write
                // that never landed.
                awaitTask (
                    JournalWriter.appendWithTokenAsync
                        suspend.EventStore
                        props.Tenant
                        props.SessionId
                        journalToken
                        events
                        CancellationToken.None
                )

        /// Reads the failure reason to journal for a Failed result: the
        /// typed outcome reason when present, else the assistant text,
        /// else a fixed fallback. Never synthesizes secrets: both sources
        /// are contract-bound to never carry them.
        /// <param name="result">The Failed result.</param>
        /// <returns>The reason the terminal event carries.</returns>
        let failedReasonOf (result: TurnResult) : string =
            match result.Outcome with
            | :? TurnFailed as failed when not (isNull (box failed)) && not (String.IsNullOrEmpty failed.Reason) ->
                failed.Reason
            | :? TurnAgentRejected as rejected when
                not (isNull (box rejected)) && not (String.IsNullOrEmpty rejected.Reason)
                ->
                rejected.Reason
            | _ when not (String.IsNullOrEmpty result.AssistantText) -> result.AssistantText
            | _ -> "The turn failed."

        /// Journals the terminal completion event for one settled turn
        /// (issue 289): Completed maps to TurnCompletedEvent, Aborted to
        /// TurnAbortedEvent under the winning cause and reason, Failed to
        /// TurnFailedEvent under the outcome reason. Exactly once per
        /// settling turn id, fenced under the live journal token via
        /// appendWithTokenAsync; best-effort (verdict-first): a rejected
        /// or failed write carries no further turn to fail, and the loser
        /// branch journals nothing (loser-zero-effects).
        /// <param name="turnId">The settling turn's id.</param>
        /// <param name="result">The settled (possibly abort-mapped) result.</param>
        let journalSettledCompletion (turnId: TurnId) (result: TurnResult) : unit =
            try
                let stamp = DateTimeOffset.UtcNow

                let eventOpt: SessionEvent option =
                    match result.Status with
                    | TurnStatus.Completed ->
                        TurnCompletedEvent(props.SessionId, turnId, Nullable<int64>(), stamp) :> SessionEvent
                        |> Some
                    | TurnStatus.Aborted ->
                        match result.Outcome with
                        | :? TurnAborted as aborted when not (isNull (box aborted)) ->
                            TurnAbortedEvent(
                                props.SessionId,
                                turnId,
                                Nullable<int64>(),
                                stamp,
                                aborted.Cause,
                                aborted.Reason
                            )
                            :> SessionEvent
                            |> Some
                        | _ ->
                            TurnAbortedEvent(
                                props.SessionId,
                                turnId,
                                Nullable<int64>(),
                                stamp,
                                StopCause.ExplicitAbort,
                                InterruptReason
                            )
                            :> SessionEvent
                            |> Some
                    | TurnStatus.Failed ->
                        TurnFailedEvent(props.SessionId, turnId, Nullable<int64>(), stamp, failedReasonOf result)
                        :> SessionEvent
                        |> Some
                    | _ -> None

                match eventOpt with
                | None -> ()
                | Some event ->
                    let events = ResizeArray<SessionEvent>([| event |]) :> IReadOnlyList<SessionEvent>

                    awaitTask (
                        JournalWriter.appendWithTokenAsync
                            suspend.EventStore
                            props.Tenant
                            props.SessionId
                            journalToken
                            events
                            CancellationToken.None
                    )
                    |> ignore
            with _ ->
                ()

        let journalTimeout (turnId: TurnId) : unit =
            // The parked turn id (issue 289): the default id (a cursor
            // that never carried one) journals nothing, while the settle
            // effects below run unchanged.
            if turnId.Equals(Unchecked.defaultof<TurnId>) then
                ()
            else
                let stamp = DateTimeOffset.UtcNow

                let event =
                    TurnFailedEvent(props.SessionId, turnId, Nullable<int64>(), stamp, AskTimeoutReason) :> SessionEvent

                let events = ResizeArray<SessionEvent>([| event |]) :> IReadOnlyList<SessionEvent>

                // Through the journal writer, best-effort: the turn already
                // settles Failed, so a rejected or failed write carries no
                // further turn to fail.
                awaitTask (
                    JournalWriter.appendWithTokenAsync
                        suspend.EventStore
                        props.Tenant
                        props.SessionId
                        journalToken
                        events
                        CancellationToken.None
                )
                |> ignore

        /// Builds the Failed result an authority refusal settles: zero
        /// iterations and usage, the typed rejection carrying which branch
        /// refused and why.
        /// <param name="failure">Which authority branch refused the turn.</param>
        /// <param name="reason">Why the turn refused to run. Never contains secrets or tool arguments.</param>
        /// <returns>The result the actor settles.</returns>
        let authorityRefusalResult (failure: AgentAuthorityFailure) (reason: string) : TurnResult =
            {
                AssistantText = ""
                Status = TurnStatus.Failed
                Iterations = 0
                Usage = { InputTokens = 0L; OutputTokens = 0L }
                Outcome = TurnAgentRejected(failure, reason) :> TurnOutcome
            }

        /// Journals an authority refusal as nothing (issue 289): the runner
        /// never ran, so no turn id ever existed for the entry and
        /// borrowing the prime id would misattribute. The entry is
        /// consumed, the session returns Idle, and the absent marker keeps
        /// the orphan trigger quiet. Kept as a named step so the refusal
        /// path reads explicitly.
        /// <param name="reason">Why the turn refused to run. Never contains secrets or tool arguments.</param>
        let journalAuthorityFailure (_reason: string) : unit = ()

        /// Checks the per-turn execution authority for a fresh turn start:
        /// re-reads the session's agent from the store. Missing, disabled,
        /// or tenant-mismatched agents refuse without ever invoking the
        /// runner.
        /// A null agent catalog, a missing session row, or a store failure
        /// authorizes (the no-catalog and empty-shell precedents): the turn
        /// runs and the failure surfaces where it always has.
        /// <returns>The refusal branch and reason, or None when authorized.</returns>
        let checkAgentAuthority () : (AgentAuthorityFailure * string) option =
            match suspend.AgentStore with
            | null -> None
            | agentStore ->
                try
                    match awaitTask (props.Store.GetSession(props.Tenant, props.SessionId, CancellationToken.None)) with
                    | null -> None
                    | session ->
                        let agentId = session.AgentId

                        match awaitTask (agentStore.GetAgent(props.Tenant, agentId, CancellationToken.None)) with
                        | null ->
                            Some(AgentAuthorityFailure.NotFound, sprintf "No agent %O exists in this tenant." agentId)
                        | agent when not agent.Enabled ->
                            Some(AgentAuthorityFailure.Disabled, sprintf "Agent %O is disabled." agentId)
                        | agent when not (agent.Tenant.Equals(props.Tenant)) ->
                            Some(
                                AgentAuthorityFailure.TenantMismatch,
                                sprintf "Agent %O belongs to another tenant." agentId
                            )
                        | _ -> None
                with _ ->
                    None

        /// Settles an authority refusal as Failed with the typed outcome:
        /// journals the TurnFailedEvent best-effort, observes and dispatches
        /// the result, consumes the entry, and returns the session to Idle.
        /// The caller drains next or stays Idle (the settleEntryNow drain
        /// precedent, minus AutoClose: Failed turns never close).
        /// <param name="entry">The turn's inbox entry to consume.</param>
        /// <param name="failure">Which authority branch refused the turn.</param>
        /// <param name="reason">Why the turn refused to run. Never contains secrets or tool arguments.</param>
        let settleAuthorityRefusal (entry: InboxEntry) (failure: AgentAuthorityFailure) (reason: string) : unit =
            journalAuthorityFailure reason

            let result = authorityRefusalResult failure reason
            notifySettled result
            dispatchCompletion props result |> ignore

            let positions = [| entry.Position |] :> IReadOnlyList<int64>

            try
                awaitTask (
                    props.Store.MarkInboxConsumed(props.Tenant, props.SessionId, positions, CancellationToken.None)
                )
                |> ignore
            with _ ->
                ()

            try
                awaitTask (
                    props.Store.UpdateSessionState(
                        props.Tenant,
                        props.SessionId,
                        SessionState.Idle,
                        CancellationToken.None
                    )
                )
                |> ignore
            with _ ->
                ()

        let startSuspendable
            (entry: InboxEntry)
            (attempt: int)
            (allowed: HashSet<string>)
            (seed: IList<ChatMessage> option)
            : unit =
            // Snapshot the live journal token for the turn's in-call
            // marker (issue 284): the marker presents this snapshot, so a
            // takeover between snapshot and append still fences out (the
            // store rejects the stale token and the loser stops before the
            // provider call with zero effects).
            let markerToken = journalToken

            // The actor-supplied loop-run id (issue 289): the live-turn
            // snapshot the marker, the completion, and the settle choke
            // points all key on. A missing snapshot mints fresh,
            // preserving the pre-plumbing marker shape.
            let runTurnId =
                match bindControl entry |> Option.orElseWith currentTurnSnapshot with
                | Some live -> live
                | None -> TurnId.New()

            awaitTask (
                props.Store.UpdateSessionState(
                    props.Tenant,
                    props.SessionId,
                    SessionState.Running,
                    CancellationToken.None
                )
            )
            |> ignore

            runningTurnId <- Some runTurnId

            let onTurnStarted: TurnLoop.TurnStartedHook option =
                Some(fun turnId cancellationToken ->
                    journalTurnStartedAsync
                        suspend.EventStore
                        props.Tenant
                        props.SessionId
                        markerToken
                        turnId
                        cancellationToken)

            // Usage and skill hooks (issue 321): fenced under the same token
            // snapshot, so a takeover loser journals nothing for either kind.
            // Fresh runs carry all three; resumes carry usage and skill but
            // no marker (see resumeSuspendable).
            let onUsageCheckpoint: TurnLoop.UsageCheckpointHook option =
                Some(fun inputTokens outputTokens cancellationToken ->
                    SessionJournal.journalUsageAsync
                        suspend.EventStore
                        props.Tenant
                        props.SessionId
                        markerToken
                        runTurnId
                        inputTokens
                        outputTokens
                        cancellationToken)

            let onSkillLoaded: TurnLoop.SkillLoadedHook option =
                Some(fun loaded cancellationToken ->
                    SessionJournal.journalSkillLoadedAsync
                        suspend.EventStore
                        props.Tenant
                        props.SessionId
                        markerToken
                        runTurnId
                        loaded
                        cancellationToken)

            // None when the runner never started (a synchronously throwing
            // or null-returning runner faults before any mint): the fault
            // handler then falls back to the turn cell and the snapshot.
            let mutable faultTurnId: TurnId option = Some runTurnId

            let runTask =
                try
                    use _controlScope =
                        match controlReports.TryGetValue entry.Position with
                        | true, (turn, claim, _) -> ControlAdmission.enter (controlAdmission turn entry.Position claim)
                        | _ -> ControlAdmission.enter (fun () -> true)

                    if not (ControlAdmission.check ()) then
                        raise (TurnLoop.TurnLeaseLostException())

                    let started =
                        suspend.RunSuspendable
                            entry
                            attempt
                            allowed
                            None
                            None
                            seed
                            CancellationToken.None
                            onTurnStarted
                            onUsageCheckpoint
                            onSkillLoaded
                            runTurnId

                    if isNull (box started) then
                        faultTurnId <- None

                        Task.FromException<TurnLoop.TurnLoopCompletion>(
                            InvalidOperationException("The suspendable turn runner returned null.")
                        )
                    else
                        started
                with ex ->
                    faultTurnId <- None
                    Task.FromException<TurnLoop.TurnLoopCompletion>(ex)

            runTask.ContinueWith(fun (completed: Task<TurnLoop.TurnLoopCompletion>) ->
                if completed.IsCanceled then
                    suspendSelf.Tell(
                        SuspendableFaulted(
                            entry,
                            OperationCanceledException("The suspendable turn was aborted."),
                            attempt,
                            faultTurnId
                        )
                    )
                elif completed.IsFaulted then
                    let error =
                        match completed.Exception with
                        | null ->
                            InvalidOperationException("The suspendable turn faulted without an exception.")
                            :> Exception
                        | aggregate -> aggregate.GetBaseException()

                    suspendSelf.Tell(SuspendableFaulted(entry, error, attempt, faultTurnId))
                else
                    suspendSelf.Tell(SuspendableFinished(entry, completed.Result, attempt, allowed)))
            |> ignore

        let resumeSuspendable (parked: SuspendedTurn) (reply: Reply) (nextAttempt: int) : unit =
            match controlReports.TryGetValue parked.Entry.Position with
            | true, (turn, claim, _) when not (controlAdmission turn parked.Entry.Position claim ()) ->
                raise (
                    InvalidSessionStateException(props.SessionId, "controlPending", "Durable control forbids resume.")
                )
            | _ -> ()

            use _controlScope =
                match controlReports.TryGetValue parked.Entry.Position with
                | true, (turn, claim, _) -> ControlAdmission.enter (controlAdmission turn parked.Entry.Position claim)
                | _ -> ControlAdmission.enter (fun () -> true)

            let cursor =
                match parked.Cursor with
                | Some live -> Some live
                | None -> None

            // Resumes continue the parked turn id (issue 289): the origin
            // id the suspend carried, so the settled completion keys on
            // the same id the marker journaled.
            runningTurnId <- Some parked.TurnId

            // Resumes already marked before they suspended (no marker), but
            // post-resume iteration boundaries and settles still checkpoint
            // usage and post-resume skill loads still journal (issue 321),
            // fenced under the live token so a takeover loser journals
            // nothing.
            let resumeToken = journalToken

            let onUsageResumed: TurnLoop.UsageCheckpointHook option =
                Some(fun inputTokens outputTokens cancellationToken ->
                    SessionJournal.journalUsageAsync
                        suspend.EventStore
                        props.Tenant
                        props.SessionId
                        resumeToken
                        parked.TurnId
                        inputTokens
                        outputTokens
                        cancellationToken)

            let onSkillResumed: TurnLoop.SkillLoadedHook option =
                Some(fun loaded cancellationToken ->
                    SessionJournal.journalSkillLoadedAsync
                        suspend.EventStore
                        props.Tenant
                        props.SessionId
                        resumeToken
                        parked.TurnId
                        loaded
                        cancellationToken)

            let mutable faultTurnId: TurnId option = Some parked.TurnId

            let runTask =
                try
                    let started =
                        suspend.RunSuspendable
                            parked.Entry
                            nextAttempt
                            parked.Allowed
                            cursor
                            (Some reply)
                            None
                            CancellationToken.None
                            // A resumed turn already marked before it
                            // suspended: no marker on resume.
                            None
                            onUsageResumed
                            onSkillResumed
                            parked.TurnId

                    if isNull (box started) then
                        faultTurnId <- None

                        Task.FromException<TurnLoop.TurnLoopCompletion>(
                            InvalidOperationException("The suspendable turn runner returned null.")
                        )
                    else
                        started
                with ex ->
                    faultTurnId <- None
                    Task.FromException<TurnLoop.TurnLoopCompletion>(ex)

            runTask.ContinueWith(fun (completed: Task<TurnLoop.TurnLoopCompletion>) ->
                if completed.IsCanceled then
                    suspendSelf.Tell(
                        SuspendableFaulted(
                            parked.Entry,
                            OperationCanceledException("The resumed turn was aborted."),
                            nextAttempt,
                            faultTurnId
                        )
                    )
                elif completed.IsFaulted then
                    let error =
                        match completed.Exception with
                        | null ->
                            InvalidOperationException("The resumed turn faulted without an exception.") :> Exception
                        | aggregate -> aggregate.GetBaseException()

                    suspendSelf.Tell(SuspendableFaulted(parked.Entry, error, nextAttempt, faultTurnId))
                else
                    suspendSelf.Tell(SuspendableFinished(parked.Entry, completed.Result, nextAttempt, parked.Allowed)))
            |> ignore

        let armTimeout (requestId: string) (timeoutCts: CancellationTokenSource) : unit =
            let delayTask =
                try
                    suspend.Delay.Delay(suspend.AskTimeout, timeoutCts.Token)
                with ex ->
                    Task.FromException(ex)

            delayTask.ContinueWith(fun (elapsed: Task) ->
                if elapsed.Status = TaskStatus.RanToCompletion then
                    suspendSelf.Tell(SuspendTimedOut requestId))
            |> ignore

        let timeoutResult () : TurnResult =
            {
                AssistantText = ""
                Status = TurnStatus.Failed
                Iterations = 0
                Usage = { InputTokens = 0L; OutputTokens = 0L }
                Outcome = TurnFailed(AskTimeoutReason) :> TurnOutcome
            }

        // The pending stop an Abort (or an Interrupt pre-empt) recorded
        // while Running: first cause wins, cleared on every settle. A cell
        // rather than a loop parameter: the actor processes one message
        // fully before the next, so the actor-thread read-modify-write never
        // races, mirroring the base loop's first-cause-wins arbitration.
        let mutable pendingStop: (StopCause * string) option = None

        // The agent rebind a SetAgent recorded while the session was not
        // quiescent (a turn running, or entries queued): applied at the
        // next quiescent boundary, cleared when it lands or the session
        // closes. A crash before apply loses it (in-memory only): the host
        // sees the unchanged AgentId on GetSession and retries.
        let mutable pendingAgent: AgentId option = None

        /// Reads whether the session's pending inbox is empty. A missing
        /// row or a store failure reads as non-empty, so a recorded rebind
        /// stays pending rather than applying half-informed.
        /// <returns>True when no inbox entry is pending.</returns>
        let inboxEmptyNow () : bool =
            try
                let pending =
                    awaitTask (props.Store.ReadPendingInbox(props.Tenant, props.SessionId, CancellationToken.None))

                isNull (box pending) || pending.Count = 0
            with _ ->
                false

        /// Adopts a fresh journal claim into the mutable cells: the token
        /// every journal write below reads, plus the on-demand compaction
        /// wiring rebuilt for it (or the kept wiring with only its token
        /// refreshed when the host drives Compact directly), so later
        /// Compacts stay live. Called the moment a fresh claim is held, so
        /// an abort below still leaves the cells fencing live writes.
        /// <param name="fresh">The live journal claim to adopt.</param>
        let swapJournal (fresh: TurnClaim) : unit =
            journalToken <- fresh.Token
            controlPrime <- Some fresh

            match suspend.RefreshCompact with
            | Some refresh ->
                try
                    currentCompact <- refresh fresh.Token
                with _ ->
                    currentCompact <-
                        currentCompact
                        |> Option.map (fun wiring ->
                            { wiring with
                                JournalToken = fresh.Token
                            })
            | None ->
                currentCompact <-
                    currentCompact
                    |> Option.map (fun wiring ->
                        { wiring with
                            JournalToken = fresh.Token
                        })

        /// Reads the agent the session converses with now, for the switch
        /// audit: the row before the rebind lands. Total: a missing row or
        /// a store failure reads as the fallback.
        /// <param name="fallback">The agent to report when the row cannot be read.</param>
        /// <returns>The session's current agent, or the fallback.</returns>
        let previousAgentNow (fallback: AgentId) : AgentId =
            try
                match awaitTask (props.Store.GetSession(props.Tenant, props.SessionId, CancellationToken.None)) with
                | null -> fallback
                | session -> session.AgentId
            with _ ->
                fallback

        /// Applies a recorded agent rebind through the 6-step quiescent
        /// protocol: (1) settle the primed claim Completed with a null
        /// outcome, (2) re-prime to a fresh claim, (3) journal the switch
        /// under the fresh token, (4) settle the fresh claim, (5) rebind the
        /// row, (6) re-prime to restore the steady-state live prime. Every
        /// write and settlement stays claim-checked last-moment by the
        /// stores; the mailbox serialization is what makes the re-primes
        /// safe (no real entry can be stolen and no rival claim can
        /// interleave), so the caller guarantees quiescence: no live turn
        /// task and an empty pending inbox. The apply is inbox-neutral (each
        /// bootstrap is consumed by its claim), so the caller's following
        /// inbox read stays exact. An abort after the fresh claim is held
        /// still adopts it and releases it best-effort, so the next boundary
        /// retries cleanly; a rebind that landed but lost its re-prime stays
        /// pending and the retry journals one duplicate switch event (turn
        /// ids are unique, so nothing collides) before the idempotent
        /// rebind converges.
        /// <param name="target">The agent the session converses with from now on.</param>
        /// <returns>True when the rebind landed.</returns>
        let applyPendingAgent (target: AgentId) : bool =
            settlePrimedNow ()

            match reprimeNow () with
            | None -> false
            | Some fresh ->
                swapJournal fresh

                let previous = previousAgentNow target

                // The host-operation sentinel (issue 373): the rebind is
                // idle/host authority, not execution, so it carries the
                // default TurnId. The prime stays for execution; only this
                // journal step rides the host path.
                let switched =
                    AgentSwitchedEvent(
                        props.SessionId,
                        Unchecked.defaultof<TurnId>,
                        Unchecked.defaultof<Nullable<int64>>,
                        DateTimeOffset.UtcNow,
                        previous,
                        target
                    )
                    :> SessionEvent

                let batch = ResizeArray<SessionEvent>([| switched |]) :> IReadOnlyList<SessionEvent>

                // The host fence reads the version stamp after the re-prime
                // (settle and claim both stamp the row), so the exact
                // equality compares against the current row.
                let expectedStamp =
                    try
                        match
                            awaitTask (props.Store.GetSession(props.Tenant, props.SessionId, CancellationToken.None))
                        with
                        | null -> None
                        | session -> Some session.UpdatedAt
                    with _ ->
                        None

                let hostOutcome =
                    match expectedStamp with
                    | None -> None
                    | Some stamp ->
                        try
                            Some(
                                awaitTask (
                                    JournalWriter.appendHostAsync
                                        suspend.EventStore
                                        props.Tenant
                                        props.SessionId
                                        stamp
                                        batch
                                        CancellationToken.None
                                )
                            )
                        with _ ->
                            None

                match hostOutcome with
                | Some(JournalWriter.JournalAppended _) ->
                    if not (settleTurnQuiet fresh) then
                        false
                    else
                        try
                            awaitTask (
                                props.Store.SetSessionAgent(
                                    props.Tenant,
                                    props.SessionId,
                                    target,
                                    CancellationToken.None
                                )
                            )
                            |> ignore

                            match reprimeNow () with
                            | Some restored ->
                                swapJournal restored
                                true
                            | None -> false
                        with _ ->
                            try
                                match reprimeNow () with
                                | Some restored -> swapJournal restored
                                | None -> ()
                            with _ ->
                                ()

                            false
                | _ ->
                    settleTurnQuiet fresh |> ignore
                    false

        /// Applies the recorded agent rebind when the inbox is empty. The
        /// caller guarantees no live turn task: the Idle/WaitingForInput
        /// handler arms (no task runs in those states), the settle drain
        /// (the reporting task is done and no new turn started), and the
        /// faulted path (the faulted task is done). Total: a throwing apply
        /// keeps the rebind pending for the next boundary.
        let tryApplyPendingWhenIdle () : unit =
            match pendingAgent with
            | None -> ()
            | Some target ->
                if inboxEmptyNow () then
                    try
                        if applyPendingAgent target then
                            pendingAgent <- None
                    with _ ->
                        ()

        /// Restores the live journal prime when quiescence settled it
        /// (issue 313): a Completed turn settles its prime at Idle, so the
        /// next fresh turn re-primes through the spawn wiring and adopts
        /// the fresh token before appending, keeping the in-call marker
        /// and the suspend/resolve writes live. Skips when a live turn is
        /// snapshotted (drain chains keep their prime) or the inbox is
        /// non-empty (a prime claim would consume the head entry), and
        /// when the re-prime fails (a takeover-held claim or a prime
        /// fault): the turn then starts on the snapshot as before, fenced
        /// exactly like today.
        let ensurePrimedNow () : unit =
            match currentTurnSnapshot () with
            | Some _ -> ()
            | None ->
                if inboxEmptyNow () then
                    match reprimeNow () with
                    | None -> ()
                    | Some fresh -> swapJournal fresh

        /// Drains the next fresh turn after an authority refusal: applies a
        /// recorded rebind when quiescent, then gates the oldest drainable
        /// entry. Authorized entries start (the session returns to Running);
        /// refused entries settle Failed and the drain recurses; an empty
        /// inbox returns the session to Idle. Resume paths never enter here:
        /// only fresh-turn starts gate.
        /// <returns>The next loop state.</returns>
        let rec drainAfterRefusal () : SessionState =
            validateRoute ()
            tryApplyPendingWhenIdle ()

            let pending =
                awaitTask (props.Store.ReadPendingInbox(props.Tenant, props.SessionId, CancellationToken.None))

            match selectDrainableEntries pending with
            | following :: _ ->
                match checkAgentAuthority () with
                | None ->
                    awaitTask (
                        props.Store.UpdateSessionState(
                            props.Tenant,
                            props.SessionId,
                            SessionState.Running,
                            CancellationToken.None
                        )
                    )
                    |> ignore

                    startSuspendable following 1 (readGrantsNow ()) None
                    SessionState.Running
                | Some(failure, reason) ->
                    settleAuthorityRefusal following failure reason
                    drainAfterRefusal ()
            | [] ->
                awaitTask (
                    props.Store.UpdateSessionState(
                        props.Tenant,
                        props.SessionId,
                        SessionState.Idle,
                        CancellationToken.None
                    )
                )
                |> ignore

                SessionState.Idle

        /// Resolves the atomic terminal settlement capability the store
        /// exposes (issue 363): the same provider the startup validation
        /// requires. None only for direct test constructions over a bare
        /// ISessionStore that never registered the capability; production
        /// activation always carries it.
        /// <returns>The settlement capability, or None when absent.</returns>
        let settlementStore: ISessionSettlementStore option =
            match props.Store with
            | :? ISessionSettlementStore as capable -> Some capable
            | _ -> None

        /// Resolves the captured claim authority one suspendable entry
        /// executes under (issue 363): the per-entry bound claim when the
        /// control target bound it, else the primed claim. Local
        /// correlation only; the store validates it as durable authority.
        /// <param name="entry">The entry the attempt executed.</param>
        /// <returns>The captured claim, or None for unclaimed shells.</returns>
        let settlementClaimFor (entry: InboxEntry) : TurnClaim option =
            match controlReports.TryGetValue entry.Position with
            | true, (_, claim, _) when not (isNull (box claim)) -> Some claim
            | _ ->
                match controlPrime with
                | Some claim when not (isNull (box claim)) -> Some claim
                | _ -> None

        /// Admits one suspendable entry under its captured claim (issue
        /// 363): records the execution admission the terminal settlement
        /// requires, so a settle without a prior start-time admit still
        /// commits. Idempotent for the same claim; best-effort, since the
        /// settlement itself enforces authority and a stale admission
        /// settles Rejected with zero effects.
        /// <param name="entry">The entry to admit.</param>
        let admitSettlementExecution (entry: InboxEntry) : unit =
            match settlementStore, settlementClaimFor entry with
            | Some capable, Some claim ->
                try
                    awaitTask (
                        capable.AdmitExecution(
                            props.Tenant,
                            props.SessionId,
                            entry.Position,
                            claim,
                            CancellationToken.None
                        )
                    )
                    |> ignore
                with _ ->
                    ()
            | _ -> ()

        /// Settles one suspendable attempt through the atomic capability
        /// (issue 363): validates the captured claim authority and commits
        /// terminal consumption, lifecycle and prime disposition, completion
        /// deduplication and outbox, and settlement bookkeeping under the
        /// same takeover-serializing boundary. The terminal event rides
        /// null here; the committed path journals best-effort afterwards as
        /// before, so no second terminal event is ever emitted. Returns
        /// None when no capability or claim is available, or when the call
        /// itself faults: the caller then falls back to the legacy
        /// unclaimed-shell path. A Rejected outcome is Some, never None: a
        /// stale loser observes it and performs zero effects.
        /// <param name="entry">The entry the attempt executed.</param>
        /// <param name="result">The decided terminal result.</param>
        /// <param name="executionId">The settling turn id, or None when no loop id ever existed.</param>
        /// <returns>The committed disposition, or None when unsettleable here.</returns>
        let trySettleSuspendable
            (entry: InboxEntry)
            (result: TurnResult)
            (executionId: TurnId option)
            : SessionSettlementOutcome option =
            match settlementStore, settlementClaimFor entry with
            | Some capable, Some claim ->
                try
                    admitSettlementExecution entry

                    let execution =
                        match executionId with
                        | Some id -> Nullable id
                        | None -> Nullable()

                    let request =
                        SessionSettlementRequest(
                            props.SessionId,
                            entry.Position,
                            claim,
                            execution,
                            result,
                            mintCompletionKey (),
                            null
                        )

                    let outcome =
                        awaitTask (capable.SettleExecution(props.Tenant, request, CancellationToken.None))

                    Some outcome
                with _ ->
                    None
            | _ -> None

        /// Drains the committed settlement's authoritative following entry
        /// (issue 363): the provider-selected runnable candidate, never an
        /// actor-computed inbox snapshot. Authorized entries start; refused
        /// ones settle through the existing refusal path and the drain
        /// recurses.
        /// <param name="following">The authoritative following entry. Never null.</param>
        /// <returns>The next loop state.</returns>
        let drainSettledFollowing (following: InboxEntry) : SessionState =
            tryApplyPendingWhenIdle ()

            match checkAgentAuthority () with
            | None ->
                startSuspendable following 1 (readGrantsNow ()) None
                SessionState.Running
            | Some(failure, reason) ->
                settleAuthorityRefusal following failure reason
                drainAfterRefusal ()

        let mutable replyInFlight = false

        let rec loop (state: SessionState) (suspended: SuspendedTurn option) (resolved: HashSet<string>) =
            actor {
                let! message = mailbox.Receive()

                let refusal =
                    match message with
                    | SuspendableQueuePrompt _
                    | SuspendableInjectPrompt _
                    | SuspendableInterruptPrompt _
                    | SessionReplyPayload _
                    | ReplyEntry _
                    | SuspendableCheckInbox ->
                        try
                            validateRoute ()
                            None
                        with :? CompletionRoutingException as error ->
                            Some
                                {
                                    Tenant = props.Tenant
                                    SessionId = props.SessionId
                                    DestinationId = error.DestinationId
                                    Reason = error.Reason
                                }
                    | _ -> None

                match message with
                | _ when refusal.IsSome ->
                    mailbox.Sender() <! refusal.Value
                    return! loop state suspended resolved
                | SessionReplyPayload reply ->
                    let sender = mailbox.Sender()

                    let reject requestId message =
                        sender
                        <! ReplyRejected(ReplyMismatchException(props.SessionId, requestId, message))

                    let expectedRequestId (parked: SuspendedTurn) : string option =
                        match parked.Cursor with
                        | Some cursor -> Some cursor.RequestId
                        | None ->
                            match parked.Rebuilt with
                            | Some rebuilt -> Some rebuilt.RequestId
                            | None -> None

                    match state, suspended, replyRequestId reply with
                    | _, _, _ when replyInFlight ->
                        reject (replyRequestId reply |> Option.defaultValue "") "A reply is already being consumed."
                    | SessionState.WaitingForInput, Some _, Some requestId when durableStop().IsSome ->
                        reject requestId "Accepted stop forbids reply consumption or resume."
                    | SessionState.WaitingForInput, Some parked, Some requestId ->
                        match expectedRequestId parked with
                        | Some expected when String.Equals(requestId, expected, StringComparison.Ordinal) ->
                            // The actor mailbox is the ordinary scoped reply
                            // gate: validate and append in one serialized
                            // turn, so concurrent answers cannot both pass a
                            // snapshot-before-append check.
                            let appended =
                                awaitTask (
                                    props.Store.AppendInboxMessage(
                                        props.Tenant,
                                        props.SessionId,
                                        ReplyPayload(reply),
                                        DeliveryMode.Queue,
                                        CancellationToken.None
                                    )
                                )

                            replyInFlight <- true
                            mailbox.Self.Tell(ReplyEntry appended, sender)
                        | _ -> reject requestId "The reply answered no pending request."
                    | SessionState.WaitingForInput, Some _, None ->
                        reject "" "The reply carried no answer for the pending request."
                    | _ ->
                        let requestId = replyRequestId reply |> Option.defaultValue ""
                        reject requestId "The session has no pending request for the reply."

                    return! loop state suspended resolved
                | SuspendableQueuePrompt(payload, cancellationToken) ->
                    match state with
                    | SessionState.Closed ->
                        mailbox.Sender() <! PromptRejected SessionState.Closed
                        return! loop state suspended resolved
                    | SessionState.Idle ->
                        // A recorded rebind applies before draining when
                        // the inbox is still empty (a quiescent boundary
                        // without its own hook settled here): the protocol
                        // leaves the inbox as found, so the drain below
                        // sees only real entries.
                        tryApplyPendingWhenIdle ()

                        // A prime settled at quiescence is restored before
                        // appending (issue 313): the fresh turn re-primes
                        // while the inbox is still empty, so its marker and
                        // suspend/resolve writes fence live.
                        ensurePrimedNow ()

                        let appended =
                            awaitTask (
                                props.Store.AppendInboxMessage(
                                    props.Tenant,
                                    props.SessionId,
                                    payload,
                                    DeliveryMode.Queue,
                                    cancellationToken
                                )
                            )

                        let pending =
                            awaitTask (props.Store.ReadPendingInbox(props.Tenant, props.SessionId, cancellationToken))

                        let first =
                            selectDrainableEntries pending |> List.tryHead |> Option.defaultValue appended

                        // The per-turn authority gate runs at this fresh-turn
                        // boundary only: authorized entries run, refused ones
                        // settle Failed without ever invoking the runner and
                        // the drain moves on.
                        match checkAgentAuthority () with
                        | None ->
                            let started =
                                try
                                    startSuspendable first 1 (readGrantsNow ()) None
                                    true
                                with error ->
                                    mailbox.Sender() <! Status.Failure(error)
                                    false

                            if started then
                                mailbox.Sender() <! PromptAccepted appended
                                return! loop SessionState.Running None resolved
                            else
                                return! loop state suspended resolved
                        | Some(failure, reason) ->
                            settleAuthorityRefusal first failure reason
                            mailbox.Sender() <! PromptAccepted appended
                            let next = drainAfterRefusal ()
                            return! loop next None resolved
                    | SessionState.Running
                    | SessionState.WaitingForInput ->
                        let appended =
                            awaitTask (
                                props.Store.AppendInboxMessage(
                                    props.Tenant,
                                    props.SessionId,
                                    payload,
                                    DeliveryMode.Queue,
                                    cancellationToken
                                )
                            )

                        mailbox.Sender() <! PromptAccepted appended
                        return! loop state suspended resolved
                    | _ ->
                        let appended =
                            awaitTask (
                                props.Store.AppendInboxMessage(
                                    props.Tenant,
                                    props.SessionId,
                                    payload,
                                    DeliveryMode.Queue,
                                    cancellationToken
                                )
                            )

                        mailbox.Sender() <! PromptAccepted appended
                        return! loop state suspended resolved
                | SuspendableInjectPrompt(payload, cancellationToken) ->
                    match state with
                    | SessionState.Closed ->
                        mailbox.Sender() <! PromptRejected SessionState.Closed
                        return! loop state suspended resolved
                    | SessionState.Idle ->
                        // A recorded rebind applies before draining when
                        // the inbox is still empty (a quiescent boundary
                        // without its own hook settled here): the protocol
                        // leaves the inbox as found, so the drain below
                        // sees only real entries.
                        tryApplyPendingWhenIdle ()

                        // A prime settled at quiescence is restored before
                        // appending (issue 313): the fresh turn re-primes
                        // while the inbox is still empty, so its marker and
                        // suspend/resolve writes fence live.
                        ensurePrimedNow ()

                        let appended =
                            awaitTask (
                                props.Store.AppendInboxMessage(
                                    props.Tenant,
                                    props.SessionId,
                                    payload,
                                    DeliveryMode.Inject,
                                    cancellationToken
                                )
                            )

                        let pending =
                            awaitTask (props.Store.ReadPendingInbox(props.Tenant, props.SessionId, cancellationToken))

                        let first =
                            selectDrainableEntries pending |> List.tryHead |> Option.defaultValue appended

                        // The per-turn authority gate runs at this fresh-turn
                        // boundary only: authorized entries run, refused ones
                        // settle Failed without ever invoking the runner and
                        // the drain moves on.
                        match checkAgentAuthority () with
                        | None ->
                            let started =
                                try
                                    startSuspendable first 1 (readGrantsNow ()) None
                                    true
                                with error ->
                                    mailbox.Sender() <! Status.Failure(error)
                                    false

                            if started then
                                mailbox.Sender() <! PromptAccepted appended
                                return! loop SessionState.Running None resolved
                            else
                                return! loop state suspended resolved
                        | Some(failure, reason) ->
                            settleAuthorityRefusal first failure reason
                            mailbox.Sender() <! PromptAccepted appended
                            let next = drainAfterRefusal ()
                            return! loop next None resolved
                    | SessionState.Running
                    | SessionState.WaitingForInput ->
                        // Append-and-wait: the running turn folds the entry
                        // at its next iteration boundary through the runner's
                        // drain hooks, and a suspended turn leaves it for the
                        // settle drain. Never aborts.
                        let appended =
                            awaitTask (
                                props.Store.AppendInboxMessage(
                                    props.Tenant,
                                    props.SessionId,
                                    payload,
                                    DeliveryMode.Inject,
                                    cancellationToken
                                )
                            )

                        mailbox.Sender() <! PromptAccepted appended
                        return! loop state suspended resolved
                    | _ ->
                        let appended =
                            awaitTask (
                                props.Store.AppendInboxMessage(
                                    props.Tenant,
                                    props.SessionId,
                                    payload,
                                    DeliveryMode.Inject,
                                    cancellationToken
                                )
                            )

                        mailbox.Sender() <! PromptAccepted appended
                        return! loop state suspended resolved
                | SuspendableInterruptPrompt(payload, cancellationToken) ->
                    match state with
                    | SessionState.Closed ->
                        mailbox.Sender() <! PromptRejected SessionState.Closed
                        return! loop state suspended resolved
                    | SessionState.Idle ->
                        // A recorded rebind applies before draining when
                        // the inbox is still empty (a quiescent boundary
                        // without its own hook settled here): the protocol
                        // leaves the inbox as found, so the drain below
                        // sees only real entries.
                        tryApplyPendingWhenIdle ()

                        // A prime settled at quiescence is restored before
                        // appending (issue 313): the fresh turn re-primes
                        // while the inbox is still empty, so its marker and
                        // suspend/resolve writes fence live.
                        ensurePrimedNow ()

                        let appended =
                            awaitTask (
                                props.Store.AppendInboxMessage(
                                    props.Tenant,
                                    props.SessionId,
                                    payload,
                                    DeliveryMode.Interrupt,
                                    cancellationToken
                                )
                            )

                        let pending =
                            awaitTask (props.Store.ReadPendingInbox(props.Tenant, props.SessionId, cancellationToken))

                        let first =
                            selectDrainableEntries pending |> List.tryHead |> Option.defaultValue appended

                        // The per-turn authority gate runs at this fresh-turn
                        // boundary only: authorized entries run, refused ones
                        // settle Failed without ever invoking the runner and
                        // the drain moves on.
                        match checkAgentAuthority () with
                        | None ->
                            let started =
                                try
                                    startSuspendable first 1 (readGrantsNow ()) None
                                    true
                                with error ->
                                    mailbox.Sender() <! Status.Failure(error)
                                    false

                            if started then
                                mailbox.Sender() <! PromptAccepted appended
                                return! loop SessionState.Running None resolved
                            else
                                return! loop state suspended resolved
                        | Some(failure, reason) ->
                            settleAuthorityRefusal first failure reason
                            mailbox.Sender() <! PromptAccepted appended
                            let next = drainAfterRefusal ()
                            return! loop next None resolved
                    | SessionState.Running ->
                        // Pre-empt through the abort verb: the entry joins
                        // the inbox first so the settle drain finds it
                        // first, then the pending stop records
                        // ExplicitAbort. The detached suspendable turn runs
                        // un-cancellable, so the stop wins at its next
                        // report and the settle drains the Interrupt entry
                        // first. A stop that already won keeps the first
                        // cause; the new entry still drains after the settle.
                        let appended =
                            awaitTask (
                                props.Store.AppendInboxMessage(
                                    props.Tenant,
                                    props.SessionId,
                                    payload,
                                    DeliveryMode.Interrupt,
                                    cancellationToken
                                )
                            )

                        if pendingStop.IsNone then
                            pendingStop <- Some(StopCause.ExplicitAbort, InterruptReason)

                        mailbox.Sender() <! PromptAccepted appended
                        return! loop state suspended resolved
                    | SessionState.WaitingForInput ->
                        // Append-and-wait: nothing runs to pre-empt and
                        // Reply still resumes the suspended turn.
                        let appended =
                            awaitTask (
                                props.Store.AppendInboxMessage(
                                    props.Tenant,
                                    props.SessionId,
                                    payload,
                                    DeliveryMode.Interrupt,
                                    cancellationToken
                                )
                            )

                        mailbox.Sender() <! PromptAccepted appended
                        return! loop state suspended resolved
                    | _ ->
                        let appended =
                            awaitTask (
                                props.Store.AppendInboxMessage(
                                    props.Tenant,
                                    props.SessionId,
                                    payload,
                                    DeliveryMode.Interrupt,
                                    cancellationToken
                                )
                            )

                        mailbox.Sender() <! PromptAccepted appended
                        return! loop state suspended resolved
                | SuspendableFinished(entry, completion, attempt, allowed) ->
                    match state, suspended with
                    | SessionState.Running, None when
                        controlReports.ContainsKey entry.Position
                        && (let _, _, id = controlReports[entry.Position] in completedControlReports.Contains id)
                        ->
                        return! loop state suspended resolved
                    | SessionState.Running, None ->
                        // A recorded stop wins over whatever the detached
                        // turn reported, even a success or a suspension: map
                        // to Aborted and clear the cell. Settlement already
                        // won when the cell is empty.
                        let stop = durableStop () |> Option.orElse pendingStop

                        let carried =
                            match stop with
                            | Some(cause, reason) -> mapSuspendAborted cause reason completion.Result
                            | None -> completion.Result

                        // Settles one reported attempt: consumes the entry,
                        // observes the (possibly abort-mapped) result, then
                        // AutoCloses on the first Completed turn or drains
                        // the next drainable entry (Interrupt tier first,
                        // then Queue-plus-Inject in position order) into a
                        // new turn, else returns to Idle.
                        // <param name="entry">The entry the attempt executed.</param>
                        // <param name="result">The result the actor settles.</param>
                        // <param name="turnId">The settling turn's id, or None when no loop id ever existed.</param>
                        // <returns>The next loop state.</returns>
                        let settleEntryNow
                            (entry: InboxEntry)
                            (result: TurnResult)
                            (turnId: TurnId option)
                            : SessionState =
                            let result = decideControl entry result

                            match trySettleSuspendable entry result turnId with
                            | Some outcome when outcome.Status = SessionSettlementStatus.Applied ->
                                // Committed winner (issue 363): the atomic
                                // boundary already consumed the entry, chose
                                // the lifecycle disposition and the queued
                                // candidate, enqueued the completion under
                                // the stable key, and released the prime at
                                // quiescence. Publish only this winner:
                                // observe once, journal the terminal event
                                // best-effort, then drain the authoritative
                                // following entry or rest at quiescence. No
                                // unfenced execution cleanup runs here.
                                pendingStop <- None
                                runningTurnId <- None
                                notifySettled result

                                // Terminal completion event (issue 289):
                                // verdict-first (the committed settle above
                                // decided the terminal kind), journaled
                                // best-effort under the live token. A prime
                                // that never ran never reaches here, and a
                                // fault before any mint (None) journals
                                // nothing.
                                match turnId with
                                | Some tid -> journalSettledCompletion tid result
                                | None -> ()

                                retireControl entry

                                if outcome.State = SessionState.Closed then
                                    pendingAgent <- None
                                    SessionState.Closed
                                elif outcome.State = SessionState.Running then
                                    match outcome.Following with
                                    | null ->
                                        tryApplyPendingWhenIdle ()

                                        if result.Status = TurnStatus.Completed then
                                            settleCompletedPrimeNow ()

                                        SessionState.Idle
                                    | following -> drainSettledFollowing following
                                else
                                    // The settled entry is consumed: an empty
                                    // inbox is quiescent, so a recorded
                                    // rebind applies before resting, and the
                                    // Completed-turn prime settle below keeps
                                    // the facade prime releasable. The
                                    // lifecycle write already landed in the
                                    // atomic boundary.
                                    tryApplyPendingWhenIdle ()

                                    if result.Status = TurnStatus.Completed then
                                        settleCompletedPrimeNow ()

                                    SessionState.Idle
                            | Some outcome when outcome.Status = SessionSettlementStatus.AlreadyApplied ->
                                // Identical retry already committed: suppress
                                // every duplicate effect (no second
                                // observation, journal, retirement, or
                                // drain) and honor the recorded disposition
                                // as the loop state only.
                                pendingStop <- None
                                runningTurnId <- None

                                if outcome.State = SessionState.Closed then
                                    pendingAgent <- None
                                    SessionState.Closed
                                elif outcome.State = SessionState.Running then
                                    SessionState.Running
                                else
                                    SessionState.Idle
                            | Some _ ->
                                // Rejected: a takeover winner owns the turn
                                // now. Zero effects from this loser: no
                                // observation, no journal, no completion, no
                                // control retirement, no lifecycle write.
                                runningTurnId <- None
                                state
                            | None ->
                                // No capability or claim (unclaimed test
                                // shells) or a faulted settlement call: the
                                // legacy store-first path below.
                                pendingStop <- None
                                runningTurnId <- None
                                let positions = [| entry.Position |] :> IReadOnlyList<int64>

                                awaitTask (
                                    props.Store.MarkInboxConsumed(
                                        props.Tenant,
                                        props.SessionId,
                                        positions,
                                        CancellationToken.None
                                    )
                                )
                                |> ignore

                                notifySettled result
                                dispatchCompletion props result |> ignore

                                // Terminal completion event (issue 289):
                                // verdict-first (the store-first settle above
                                // decided the terminal kind), journaled
                                // best-effort under the live token. A prime
                                // that never ran never reaches here, and a
                                // fault before any mint (None) journals
                                // nothing.
                                match turnId with
                                | Some tid -> journalSettledCompletion tid result
                                | None -> ()

                                retireControl entry

                                if result.Status = TurnStatus.Completed && autoCloseEnabled props then
                                    // AutoClose (issue 82): the first Completed
                                    // turn closes the session store-first instead
                                    // of draining; the entry is already consumed
                                    // above. Aborted and Failed results never take
                                    // this path, so failed runs stay open for
                                    // inspection. A recorded rebind dies with
                                    // the session: Closed rejects it.
                                    awaitTask (
                                        props.Store.CloseSession(props.Tenant, props.SessionId, CancellationToken.None)
                                    )
                                    |> ignore

                                    pendingAgent <- None

                                    SessionState.Closed
                                else
                                    // The settled entry is consumed: an empty
                                    // inbox is quiescent (the reporting task is
                                    // done and no new turn started), so a
                                    // recorded rebind applies before draining
                                    // next, and stays pending while entries
                                    // remain.
                                    tryApplyPendingWhenIdle ()

                                    let pending =
                                        awaitTask (
                                            props.Store.ReadPendingInbox(
                                                props.Tenant,
                                                props.SessionId,
                                                CancellationToken.None
                                            )
                                        )

                                    match selectDrainableEntries pending with
                                    | following :: _ ->
                                        // The per-turn authority gate runs at this
                                        // settle-drain boundary only: authorized
                                        // entries run, refused ones settle Failed
                                        // without ever invoking the runner and
                                        // the drain moves on.
                                        match checkAgentAuthority () with
                                        | None ->
                                            startSuspendable following 1 (readGrantsNow ()) None
                                            SessionState.Running
                                        | Some(failure, reason) ->
                                            settleAuthorityRefusal following failure reason
                                            drainAfterRefusal ()
                                    | [] ->
                                        // Completed-turn prime settle (issue
                                        // 313): release the facade prime exactly
                                        // once at quiescence through the fenced
                                        // settle, so a later prompt (or a
                                        // respawn prime after a restart) claims
                                        // anew instead of observing
                                        // TurnLeaseMissing. Aborted and Failed
                                        // results keep their prime, like the
                                        // fault path; a stale (taken-over) token
                                        // settles nothing. The terminal event
                                        // above already journaled under the live
                                        // token, so the settle lands after it.
                                        if result.Status = TurnStatus.Completed then
                                            settleCompletedPrimeNow ()

                                        awaitTask (
                                            props.Store.UpdateSessionState(
                                                props.Tenant,
                                                props.SessionId,
                                                SessionState.Idle,
                                                CancellationToken.None
                                            )
                                        )
                                        |> ignore

                                        SessionState.Idle

                        match completion.Suspension, stop with
                        | Some cursor, None ->
                            awaitTask (
                                props.Store.UpdateSessionState(
                                    props.Tenant,
                                    props.SessionId,
                                    SessionState.WaitingForInput,
                                    CancellationToken.None
                                )
                            )
                            |> ignore

                            match journalSuspend cursor with
                            | JournalWriter.JournalAppended _ ->
                                let timeoutCts = new CancellationTokenSource()

                                let carriedAllowed = if isNull (box allowed) then HashSet<string>() else allowed

                                // No running attempt remains once parked:
                                // the parked turn id carries the settle
                                // identity from here on.
                                runningTurnId <- None

                                let parked =
                                    {
                                        Entry = entry
                                        TurnId =
                                            match resolveSettlingTurnId completion.TurnId with
                                            | Some live -> live
                                            | None -> Unchecked.defaultof<TurnId>
                                        Cursor = Some cursor
                                        Rebuilt = None
                                        Allowed = carriedAllowed
                                        Attempt = attempt
                                        TimeoutCts = timeoutCts
                                    }

                                armTimeout cursor.RequestId timeoutCts
                                return! loop SessionState.WaitingForInput (Some parked) resolved
                            | JournalWriter.JournalRejected rejection ->
                                // The suspend event never landed: parking
                                // would strand the turn on a missing journal
                                // entry, so the turn fails with the typed
                                // reason instead.
                                settleJournalFailure entry (sprintf "The journal append was rejected: %s." rejection)
                                return! loop SessionState.Idle None resolved
                            | JournalWriter.JournalFailed failure ->
                                settleJournalFailure entry failure
                                return! loop SessionState.Idle None resolved
                        | _ ->
                            // Settled, or suspended after a stop won: the
                            // stop settles Aborted with no suspend event
                            // journaled and nothing parked for a Reply. The
                            // settling id resolves completion-carried, then
                            // turn-cell, then snapshot, else the terminal
                            // journals nothing.
                            let settling = resolveSettlingTurnId completion.TurnId
                            let next = settleEntryNow entry carried settling
                            return! loop next None resolved
                    | SessionState.WaitingForInput, Some parked when parked.Cursor.IsNone && parked.Rebuilt.IsSome ->
                        // Crash-retry path should never produce a running
                        // finish while still parked; ignore stale completions.
                        return! loop state suspended resolved
                    | _ -> return! loop state suspended resolved
                | SuspendableFaulted(entry, error, _, faultTurnId) ->
                    match state, suspended with
                    | SessionState.Running, None when
                        controlReports.ContainsKey entry.Position
                        && (let _, _, id = controlReports[entry.Position] in completedControlReports.Contains id)
                        ->
                        return! loop state suspended resolved
                    | SessionState.Running, None ->
                        // A recorded stop wins even over a real fault:
                        // settle Aborted under the cause instead of failing
                        // silently. The cell clears on every fault settle.
                        let stop = durableStop () |> Option.orElse pendingStop

                        // The fault's settling id (issue 289): the
                        // message-carried id, then the turn cell, then the
                        // snapshot; None (fault before any mint) journals
                        // nothing while the settle effects run unchanged.
                        let settling =
                            match faultTurnId with
                            | Some _ as resolved -> resolved
                            | None ->
                                match runningTurnId with
                                | Some _ as resolved -> resolved
                                | None -> currentTurnSnapshot ()

                        let candidate =
                            match stop with
                            | Some(cause, reason) -> abortedSuspendResult cause reason
                            | None ->
                                {
                                    AssistantText = ""
                                    Status = TurnStatus.Failed
                                    Iterations = 0
                                    Usage = { InputTokens = 0L; OutputTokens = 0L }
                                    Outcome = TurnFailed(ProviderFailureReason.formatFault error) :> TurnOutcome
                                }

                        let selected = decideControl entry candidate

                        match trySettleSuspendable entry selected settling with
                        | Some outcome when outcome.Status = SessionSettlementStatus.Applied ->
                            // Committed winner (issue 363): the atomic
                            // boundary already consumed the entry, chose the
                            // lifecycle disposition and the queued candidate,
                            // enqueued the completion under the stable key,
                            // and released the prime at quiescence. Publish
                            // only this winner: observe once, journal the
                            // terminal event best-effort, then drain the
                            // authoritative following entry or rest at
                            // quiescence. No unfenced execution cleanup runs
                            // here.
                            pendingStop <- None
                            runningTurnId <- None
                            notifySettled selected

                            match settling with
                            | Some tid -> journalSettledCompletion tid selected
                            | None -> ()

                            retireControl entry

                            if outcome.State = SessionState.Closed then
                                pendingAgent <- None
                                return! loop SessionState.Closed None resolved
                            elif outcome.State = SessionState.Running then
                                match outcome.Following with
                                | null ->
                                    tryApplyPendingWhenIdle ()

                                    return! loop SessionState.Idle None resolved
                                | following ->
                                    let next = drainSettledFollowing following
                                    return! loop next None resolved
                            else
                                // The faulted entry is consumed and the
                                // lifecycle write already landed in the
                                // atomic boundary: an empty inbox is
                                // quiescent, so a recorded rebind applies
                                // here; entries remaining were drained above.
                                tryApplyPendingWhenIdle ()

                                return! loop SessionState.Idle None resolved
                        | Some outcome when outcome.Status = SessionSettlementStatus.AlreadyApplied ->
                            // Identical retry already committed: suppress
                            // every duplicate effect and honor the recorded
                            // disposition as the loop state only.
                            pendingStop <- None
                            runningTurnId <- None

                            if outcome.State = SessionState.Closed then
                                pendingAgent <- None
                                return! loop SessionState.Closed None resolved
                            elif outcome.State = SessionState.Running then
                                return! loop SessionState.Running None resolved
                            else
                                return! loop SessionState.Idle None resolved
                        | Some _ ->
                            // Rejected: a takeover winner owns the turn now.
                            // Zero effects from this loser.
                            runningTurnId <- None
                            return! loop state None resolved
                        | None ->
                            // No capability or claim (unclaimed test shells)
                            // or a faulted settlement call: the legacy
                            // store-first path below.
                            let positions = [| entry.Position |] :> IReadOnlyList<int64>

                            awaitTask (
                                props.Store.MarkInboxConsumed(
                                    props.Tenant,
                                    props.SessionId,
                                    positions,
                                    CancellationToken.None
                                )
                            )
                            |> ignore

                            pendingStop <- None
                            runningTurnId <- None
                            notifySettled selected
                            dispatchCompletion props selected |> ignore

                            match settling with
                            | Some tid -> journalSettledCompletion tid selected
                            | None -> ()

                            retireControl entry

                            awaitTask (
                                props.Store.UpdateSessionState(
                                    props.Tenant,
                                    props.SessionId,
                                    SessionState.Idle,
                                    CancellationToken.None
                                )
                            )
                            |> ignore

                            // The faulted entry is consumed and no turn runs:
                            // an empty inbox is quiescent, so a recorded rebind
                            // applies here; entries remaining keep it pending
                            // for the Idle handler.
                            tryApplyPendingWhenIdle ()

                            return! loop SessionState.Idle None resolved
                    | _ -> return! loop state suspended resolved
                | ReplyEntry replyEntry ->
                    replyInFlight <- false

                    match state, suspended with
                    | SessionState.WaitingForInput, Some _ when durableStop().IsSome ->
                        mailbox.Sender()
                        <! Status.Failure(
                            InvalidSessionStateException(
                                props.SessionId,
                                "controlPending",
                                "Accepted stop forbids reply consumption or resume."
                            )
                        )

                        return! loop state suspended resolved
                    | SessionState.WaitingForInput, Some parked ->
                        let replyOpt: Reply option =
                            match replyEntry.Payload with
                            | :? ReplyPayload as payload when not (isNull (box payload)) ->
                                if isNull (box payload.Reply) then
                                    None
                                else
                                    Some payload.Reply
                            | _ -> None

                        match replyOpt with
                        | None ->
                            let error =
                                ReplyMismatchException(
                                    props.SessionId,
                                    "",
                                    "The reply carried no answer for the pending request."
                                )

                            mailbox.Sender() <! ReplyRejected error
                            return! loop state suspended resolved
                        | Some reply ->
                            let requestOpt = replyRequestId reply

                            let expectedOpt: string option =
                                match parked.Cursor with
                                | Some cursor -> Some cursor.RequestId
                                | None ->
                                    match parked.Rebuilt with
                                    | Some rebuilt -> Some rebuilt.RequestId
                                    | None -> None

                            match requestOpt, expectedOpt with
                            | Some requestId, Some expected when
                                String.Equals(requestId, expected, StringComparison.Ordinal)
                                ->
                                if resolved.Contains(requestId) then
                                    let error =
                                        ReplyMismatchException(
                                            props.SessionId,
                                            requestId,
                                            "The reply answers an already-resolved request."
                                        )

                                    mailbox.Sender() <! ReplyRejected error
                                    return! loop state suspended resolved
                                else
                                    try
                                        parked.TimeoutCts.Cancel()
                                    with _ ->
                                        ()

                                    let positions = [| replyEntry.Position |] :> IReadOnlyList<int64>

                                    awaitTask (
                                        props.Store.MarkInboxConsumed(
                                            props.Tenant,
                                            props.SessionId,
                                            positions,
                                            CancellationToken.None
                                        )
                                    )
                                    |> ignore

                                    let writeResult = journalResolve reply

                                    match writeResult with
                                    | JournalWriter.JournalAppended _ ->
                                        resolved.Add(requestId) |> ignore

                                        awaitTask (
                                            props.Store.UpdateSessionState(
                                                props.Tenant,
                                                props.SessionId,
                                                SessionState.Running,
                                                CancellationToken.None
                                            )
                                        )
                                        |> ignore

                                        // AllowForSession memory: remember the tool
                                        // in memory before resuming so the continued
                                        // run skips Evaluate for it, and persist the
                                        // grant on the session row so it survives a
                                        // restart; the close evicts it.
                                        match reply with
                                        | :? PermissionDecision as decision when
                                            not (isNull (box decision))
                                            && decision.Decision = PermissionDecisionKind.AllowForSession
                                            ->
                                            let toolName =
                                                match parked.Cursor with
                                                | Some cursor -> cursor.ToolName
                                                | None ->
                                                    match parked.Rebuilt with
                                                    | Some rebuilt -> rebuilt.ToolName
                                                    | None -> ""

                                            if not (String.IsNullOrEmpty toolName) then
                                                parked.Allowed.Add(toolName) |> ignore

                                                awaitTask (
                                                    props.Store.GrantSessionTool(
                                                        props.Tenant,
                                                        props.SessionId,
                                                        toolName,
                                                        CancellationToken.None
                                                    )
                                                )
                                                |> ignore
                                        | _ -> ()

                                        mailbox.Sender() <! ReplyAccepted replyEntry

                                        let nextAttempt = parked.Attempt + 1

                                        match parked.Cursor with
                                        | Some _ ->
                                            resumeSuspendable parked reply nextAttempt
                                            return! loop SessionState.Running None resolved
                                        | None ->
                                            // Crash-retry: no cursor, so retry the
                                            // parked entry from scratch under the new
                                            // attempt. The retried run suspends again
                                            // or settles; either path re-enters this
                                            // loop.
                                            startSuspendable parked.Entry nextAttempt parked.Allowed None
                                            return! loop SessionState.Running None resolved
                                    | JournalWriter.JournalRejected rejection ->
                                        // The resolve event never landed: resuming
                                        // would strand the turn on a missing
                                        // journal entry, so the turn fails with
                                        // the typed reason instead. The reply
                                        // matched and is consumed, so it still
                                        // acks Accepted, and the request id is
                                        // recorded so a redelivery replays
                                        // Accepted instead of ReplyMismatch.
                                        resolved.Add(requestId) |> ignore

                                        settleJournalFailure
                                            parked.Entry
                                            (sprintf "The journal append was rejected: %s." rejection)

                                        mailbox.Sender() <! ReplyAccepted replyEntry
                                        return! loop SessionState.Idle None resolved
                                    | JournalWriter.JournalFailed failure ->
                                        resolved.Add(requestId) |> ignore
                                        settleJournalFailure parked.Entry failure
                                        mailbox.Sender() <! ReplyAccepted replyEntry
                                        return! loop SessionState.Idle None resolved
                            | Some requestId, _ ->
                                let error =
                                    ReplyMismatchException(
                                        props.SessionId,
                                        requestId,
                                        "The reply answered no pending request."
                                    )

                                mailbox.Sender() <! ReplyRejected error
                                return! loop state suspended resolved
                            | None, _ ->
                                let error =
                                    ReplyMismatchException(
                                        props.SessionId,
                                        "",
                                        "The reply carried no answer for the pending request."
                                    )

                                mailbox.Sender() <! ReplyRejected error
                                return! loop state suspended resolved
                    | _ ->
                        let requestId: string =
                            match replyEntry.Payload with
                            | :? ReplyPayload as payload when not (isNull (box payload)) ->
                                match replyRequestId payload.Reply with
                                | Some id -> id
                                | None -> ""
                            | _ -> ""

                        let error =
                            ReplyMismatchException(
                                props.SessionId,
                                requestId,
                                "The session has no pending request for the reply."
                            )

                        mailbox.Sender() <! ReplyRejected error
                        return! loop state suspended resolved
                | SuspendTimedOut requestId ->
                    match state, suspended with
                    | SessionState.WaitingForInput, Some parked ->
                        let expectedOpt: string option =
                            match parked.Cursor with
                            | Some cursor -> Some cursor.RequestId
                            | None ->
                                match parked.Rebuilt with
                                | Some rebuilt -> Some rebuilt.RequestId
                                | None -> None

                        match expectedOpt with
                        | Some expected when String.Equals(requestId, expected, StringComparison.Ordinal) ->
                            try
                                parked.TimeoutCts.Cancel()
                            with _ ->
                                ()

                            let result = decideControl parked.Entry (timeoutResult ())

                            // The timeout settled the turn Failed: a
                            // recorded stop loses to the settlement.
                            pendingStop <- None

                            let positions = [| parked.Entry.Position |] :> IReadOnlyList<int64>

                            awaitTask (
                                props.Store.MarkInboxConsumed(
                                    props.Tenant,
                                    props.SessionId,
                                    positions,
                                    CancellationToken.None
                                )
                            )
                            |> ignore

                            journalTimeout parked.TurnId
                            notifySettled result
                            dispatchCompletion props result |> ignore
                            retireControl parked.Entry

                            awaitTask (
                                props.Store.UpdateSessionState(
                                    props.Tenant,
                                    props.SessionId,
                                    SessionState.Idle,
                                    CancellationToken.None
                                )
                            )
                            |> ignore

                            return! loop SessionState.Idle None resolved
                        | _ -> return! loop state suspended resolved
                    | _ -> return! loop state suspended resolved
                | SuspendableObserveHostAbort(tenant, sessionId, turn) ->
                    if
                        tenant = props.Tenant
                        && sessionId = props.SessionId
                        && runningTurnId = Some turn
                    then
                        match durableStop () with
                        | Some stop -> pendingStop <- Some stop
                        | None -> ()

                    return! loop state suspended resolved
                | SuspendableAbortSession(cause, reason, _) ->
                    match state, suspended with
                    | SessionState.Running, None when cause = StopCause.ExplicitAbort || cause = StopCause.HostShutdown ->
                        // First cause wins: the detached suspendable turn
                        // runs un-cancellable, so the recorded stop wins at
                        // its next report.
                        if pendingStop.IsNone then
                            pendingStop <- Some(cause, reason)

                        mailbox.Sender() <! takeSuspendSnapshot state suspended
                        return! loop state suspended resolved
                    | _ ->
                        // Idle, WaitingForInput (a suspended turn owns
                        // nothing running to abort), Closed, unknown states,
                        // and non-abort-family causes: a no-op returning the
                        // current state.
                        mailbox.Sender() <! takeSuspendSnapshot state suspended
                        return! loop state suspended resolved
                | SuspendableCompactSession cancellationToken ->
                    match state with
                    | SessionState.Closed ->
                        mailbox.Sender() <! CompactRejected SessionState.Closed
                        return! loop state suspended resolved
                    | SessionState.Idle ->
                        match currentCompact with
                        | None ->
                            mailbox.Sender() <! CompactNotNeeded
                            return! loop state suspended resolved
                        | Some compact ->
                            let reply = compactIdleNow props compact cancellationToken
                            mailbox.Sender() <! reply
                            return! loop state suspended resolved
                    | SessionState.Running ->
                        match currentCompact with
                        | Some compact when not (isNull (box compact.Force)) ->
                            compact.Force.Request()
                            mailbox.Sender() <! CompactDeferred
                            return! loop state suspended resolved
                        | _ ->
                            // Unconfigured: no boundary hook shares the
                            // one-shot cell, so nothing can fire later.
                            mailbox.Sender() <! CompactNotNeeded
                            return! loop state suspended resolved
                    | SessionState.WaitingForInput ->
                        // A suspended turn owns the history, so an on-demand
                        // compact no-ops.
                        mailbox.Sender() <! CompactNotNeeded
                        return! loop state suspended resolved
                    | _ ->
                        // Out-of-range stored state: stay durable but
                        // compact nothing.
                        mailbox.Sender() <! CompactNotNeeded
                        return! loop state suspended resolved
                | SuspendableSetAgent(agentId, cancellationToken) ->
                    match state with
                    | SessionState.Closed ->
                        mailbox.Sender() <! SetAgentRejected SessionState.Closed
                        return! loop state suspended resolved
                    | _ ->
                        pendingAgent <- Some agentId

                        if state = SessionState.Idle || state = SessionState.WaitingForInput then
                            // No turn task runs in these states: an empty
                            // inbox applies the rebind at once, queued
                            // entries keep it pending. (A parked suspension
                            // keeps its entry pending, so a Waiting session
                            // applies at the post-resume settle boundary,
                            // never mid-suspension: claiming there would
                            // steal the parked entry and leak the protocol
                            // bootstrap as a turn.)
                            tryApplyPendingWhenIdle ()

                        let current =
                            awaitTask (requireSessionAsync props.Store props.Tenant props.SessionId cancellationToken)

                        match pendingAgent with
                        | None -> mailbox.Sender() <! SetAgentApplied current
                        | Some _ -> mailbox.Sender() <! SetAgentPending current

                        return! loop state suspended resolved
                | SuspendableCloseSession cancellationToken ->
                    match suspended with
                    | Some parked ->
                        try
                            parked.TimeoutCts.Cancel()
                        with _ ->
                            ()
                    | None -> ()

                    // A recorded stop dies with the session: Closed settles
                    // nothing further. A recorded rebind dies with it too:
                    // Closed rejects it.
                    pendingStop <- None
                    pendingAgent <- None

                    let closed =
                        awaitTask (props.Store.CloseSession(props.Tenant, props.SessionId, cancellationToken))

                    mailbox.Sender() <! closed
                    return! loop SessionState.Closed None resolved
                | SuspendableGetSnapshot ->
                    mailbox.Sender() <! takeSuspendSnapshot state suspended
                    return! loop state suspended resolved
                | SuspendableCheckInbox ->
                    match state with
                    | SessionState.Idle ->
                        // The dispatch wake: the mailbox serializes this
                        // against prompts, and the in-memory Idle re-check
                        // above is the last word, so a wake racing a prompt
                        // or a second wake collapses to a no-op or ordered
                        // queueing. Never appends: only the oldest drainable
                        // entry already stored starts, through the same
                        // start path the Idle prompt arms use.
                        let pending =
                            try
                                awaitTask (
                                    props.Store.ReadPendingInbox(props.Tenant, props.SessionId, CancellationToken.None)
                                )
                            with :? SessionNotFoundException ->
                                ResizeArray<InboxEntry>() :> IReadOnlyList<InboxEntry>

                        match selectDrainableEntries pending with
                        | first :: _ ->
                            // The per-turn authority gate runs at this Idle
                            // wake boundary too: authorized entries run,
                            // refused ones settle Failed without ever invoking
                            // the runner and the drain moves on.
                            match checkAgentAuthority () with
                            | None ->
                                awaitTask (
                                    props.Store.UpdateSessionState(
                                        props.Tenant,
                                        props.SessionId,
                                        SessionState.Running,
                                        CancellationToken.None
                                    )
                                )
                                |> ignore

                                startSuspendable first 1 (readGrantsNow ()) None
                                return! loop SessionState.Running None resolved
                            | Some(failure, reason) ->
                                settleAuthorityRefusal first failure reason
                                let next = drainAfterRefusal ()
                                return! loop next None resolved
                        | [] -> return! loop state suspended resolved
                    | _ -> return! loop state suspended resolved
            }

        match initialState, initialResumeEntry with
        | SessionState.Running, Some entry ->
            // Crash resume: the interrupted turn restarts as a new attempt
            // under the fresh spawn-primed journal token (old-attempt events
            // stay since the journal is append-only). The rehydrated history
            // seeds the resumed run's runner input in-memory (never
            // journaled, so replay cursors stay untouched); a rehydration
            // failure falls back to a seedless retry from the inbox entry
            // per the existing crash-retry precedent.
            let crashSeed: IList<ChatMessage> option =
                try
                    match lastJournalTurnId suspend.EventStore props.Tenant props.SessionId with
                    | Some interrupted ->
                        Some(rehydrateCrashHistory suspend.EventStore props.Tenant props.SessionId interrupted)
                    | None -> None
                with _ ->
                    None

            startSuspendable
                entry
                (controlPrime |> Option.map _.Attempt |> Option.defaultValue 2)
                (readGrantsNow ())
                crashSeed

            loop SessionState.Running None (HashSet<string>())
        | _ -> loop initialState initialSuspended (HashSet<string>())

    let behaviorWithSuspend props suspend mailbox =
        behaviorWithSuspendRouted (fun () -> ()) props suspend mailbox

    /// Asks a suspendable actor with the shared timeout, honouring the
    /// caller's cancellation. Mirrors askAsync for the suspendable protocol.
    /// <param name="session">The suspendable session actor.</param>
    /// <param name="message">The message to ask with.</param>
    /// <param name="cancellationToken">Cancels the ask.</param>
    /// <returns>The actor's reply.</returns>
    let private askSuspendableAsync<'Reply>
        (session: IActorRef)
        (message: SuspendableActorMessage)
        (cancellationToken: CancellationToken)
        : Task<'Reply> =
        task {
            use timeoutCts = new CancellationTokenSource(askTimeout)

            use linkedCts =
                CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token)

            let! reply = session.Ask<obj>(message, linkedCts.Token)

            match reply with
            | :? CompletionRoutingRefused as refusal -> return raise refusal.Exception
            | _ -> return unbox<'Reply> reply
        }

    /// Prompts a suspendable session actor: appends the Queue inbox entry
    /// and starts a suspendable turn when Idle. Validates the session is
    /// present and not Closed before touching the actor.
    /// <param name="store">The durable store.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to prompt.</param>
    /// <param name="session">The suspendable session actor.</param>
    /// <param name="message">The user message. Must not be null.</param>
    /// <param name="cancellationToken">Cancels the prompt.</param>
    /// <returns>The appended inbox entry.</returns>
    let promptSuspendableAsync
        (store: ISessionStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (session: IActorRef)
        (message: UserMessage)
        (cancellationToken: CancellationToken)
        : Task<InboxEntry> =
        ArgumentNullException.ThrowIfNull(store)
        ArgumentNullException.ThrowIfNull(session)

        if isNull (box message) then
            raise (ArgumentNullException(nameof message))

        task {
            let! current = requireSessionAsync store tenant sessionId cancellationToken

            if current.State = SessionState.Closed then
                raise (
                    InvalidSessionStateException(
                        sessionId,
                        current.State.ToString(),
                        "The session is closed and accepts no further prompts."
                    )
                )

            // The suspendable actor owns its queue wire: the suspendable
            // behavior starts its suspendable runner for it.
            let payload = UserMessagePayload(message) :> InboxPayload

            let! reply =
                askSuspendableAsync<SessionPromptReply>
                    session
                    (SuspendableQueuePrompt(payload, cancellationToken))
                    cancellationToken

            match reply with
            | PromptAccepted entry -> return entry
            | PromptRejected rejectedState ->
                return
                    raise (
                        InvalidSessionStateException(
                            sessionId,
                            rejectedState.ToString(),
                            "The session closed before the prompt was accepted."
                        )
                    )
        }

    /// Prompts a suspendable session actor through one delivery wire: the
    /// shared validation and Closed rejection behind the Inject and
    /// Interrupt wires. Validates the session is present and not Closed
    /// before touching the actor, so invalid transitions throw here, never
    /// inside the actor.
    /// <param name="store">The durable store.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to prompt.</param>
    /// <param name="session">The suspendable session actor.</param>
    /// <param name="message">The user message. Must not be null.</param>
    /// <param name="ask">Builds the suspendable prompt message for the delivery.</param>
    /// <param name="cancellationToken">Cancels the prompt.</param>
    /// <returns>The appended inbox entry.</returns>
    let private promptSuspendableCore
        (store: ISessionStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (session: IActorRef)
        (message: UserMessage)
        (ask: InboxPayload -> CancellationToken -> SuspendableActorMessage)
        (cancellationToken: CancellationToken)
        : Task<InboxEntry> =
        ArgumentNullException.ThrowIfNull(store)
        ArgumentNullException.ThrowIfNull(session)

        if isNull (box message) then
            raise (ArgumentNullException(nameof message))

        if isNull (box ask) then
            raise (ArgumentNullException(nameof ask))

        task {
            let! current = requireSessionAsync store tenant sessionId cancellationToken

            if current.State = SessionState.Closed then
                raise (
                    InvalidSessionStateException(
                        sessionId,
                        current.State.ToString(),
                        "The session is closed and accepts no further prompts."
                    )
                )

            let payload = UserMessagePayload(message) :> InboxPayload

            let! reply =
                askSuspendableAsync<SessionPromptReply> session (ask payload cancellationToken) cancellationToken

            match reply with
            | PromptAccepted entry -> return entry
            | PromptRejected rejectedState ->
                return
                    raise (
                        InvalidSessionStateException(
                            sessionId,
                            rejectedState.ToString(),
                            "The session closed before the prompt was accepted."
                        )
                    )
        }

    /// Injects a user message into a suspendable session actor: appends with
    /// Inject delivery and folds into the running turn at its next iteration
    /// boundary without interrupting it. Validates the session is present
    /// and not Closed before touching the actor.
    /// <param name="store">The durable store.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to prompt.</param>
    /// <param name="session">The suspendable session actor.</param>
    /// <param name="message">The user message. Must not be null.</param>
    /// <param name="cancellationToken">Cancels the prompt.</param>
    /// <returns>The appended inbox entry.</returns>
    let injectSuspendableAsync
        (store: ISessionStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (session: IActorRef)
        (message: UserMessage)
        (cancellationToken: CancellationToken)
        : Task<InboxEntry> =
        promptSuspendableCore
            store
            tenant
            sessionId
            session
            message
            (fun payload token -> SuspendableInjectPrompt(payload, token))
            cancellationToken

    /// Interrupts a suspendable session actor with a user message: appends
    /// with Interrupt delivery, records the ExplicitAbort pending stop when a
    /// turn runs, and drains the Interrupt entry first after the settle.
    /// Validates the session is present and not Closed before touching the
    /// actor.
    /// <param name="store">The durable store.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to prompt.</param>
    /// <param name="session">The suspendable session actor.</param>
    /// <param name="message">The user message. Must not be null.</param>
    /// <param name="cancellationToken">Cancels the prompt.</param>
    /// <returns>The appended inbox entry.</returns>
    let interruptSuspendableAsync
        (store: ISessionStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (session: IActorRef)
        (message: UserMessage)
        (cancellationToken: CancellationToken)
        : Task<InboxEntry> =
        promptSuspendableCore
            store
            tenant
            sessionId
            session
            message
            (fun payload token -> SuspendableInterruptPrompt(payload, token))
            cancellationToken

    /// Replies to a suspended turn: appends the Reply inbox entry, matches
    /// it against the pending request id, and resumes from the cursor with
    /// attempt plus 1. An unknown or already-resolved request id throws the
    /// typed ReplyMismatchException; a matching one consumes the Reply entry
    /// and resumes. Reply never starts a turn.
    /// <param name="store">The durable store.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to reply to.</param>
    /// <param name="session">The suspendable session actor.</param>
    /// <param name="reply">The host reply. Must not be null.</param>
    /// <param name="cancellationToken">Cancels the reply.</param>
    /// <returns>The consumed Reply inbox entry.</returns>
    let replyAsync
        (store: ISessionStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (session: IActorRef)
        (reply: Reply)
        (cancellationToken: CancellationToken)
        : Task<InboxEntry> =
        ArgumentNullException.ThrowIfNull(store)
        ArgumentNullException.ThrowIfNull(session)

        if isNull (box reply) then
            raise (ArgumentNullException(nameof reply))

        task {
            let! current = requireSessionAsync store tenant sessionId cancellationToken

            if current.State = SessionState.Closed then
                raise (
                    InvalidSessionStateException(
                        sessionId,
                        current.State.ToString(),
                        "The session is closed and accepts no reply."
                    )
                )

            let! answer = session.Ask<obj>(SessionReplyPayload reply, TimeSpan.FromSeconds 30.0, cancellationToken)

            match answer with
            | :? CompletionRoutingRefused as refusal -> return raise refusal.Exception
            | :? SessionReplyReply as replyReply ->
                match replyReply with
                | ReplyAccepted entry -> return entry
                | ReplyRejected error -> return raise error
            | _ -> return raise (InvalidOperationException("The session actor returned an unexpected reply."))
        }

    /// Closes a suspendable session: the client boundary. Valid in every
    /// state and idempotent; the store close evicts the session's grant
    /// memory. Unknown sessions throw SessionNotFoundException before
    /// touching the actor. A turn running while the session closes keeps
    /// its detached task, but its journal writes fence on the claim and its
    /// finish is ignored once the actor is Closed.
    /// <param name="store">The durable store.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to close.</param>
    /// <param name="session">The suspendable session actor.</param>
    /// <param name="cancellationToken">Cancels the close.</param>
    /// <returns>The stored session after the close.</returns>
    let closeSuspendableAsync
        (store: ISessionStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (session: IActorRef)
        (cancellationToken: CancellationToken)
        : Task<Session> =
        ArgumentNullException.ThrowIfNull(store)
        ArgumentNullException.ThrowIfNull(session)

        task {
            let! _ = requireSessionAsync store tenant sessionId cancellationToken

            let! closed =
                askSuspendableAsync<Session> session (SuspendableCloseSession cancellationToken) cancellationToken

            return closed
        }

    /// Reads a suspendable actor's snapshot: its lifecycle state, the
    /// store's pending inbox count, and the pending suspend request id.
    /// <param name="session">The suspendable session actor.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The actor's current snapshot.</returns>
    let getSuspendSnapshotAsync (session: IActorRef) (cancellationToken: CancellationToken) : Task<SessionSnapshot> =
        ArgumentNullException.ThrowIfNull(session)
        askSuspendableAsync<SessionSnapshot> session SuspendableGetSnapshot cancellationToken

    /// Wakes a suspendable session actor for the polling dispatcher: tells
    /// the idempotent check-inbox message, which starts a suspendable turn
    /// on the oldest drainable entry when the actor is Idle and no-ops
    /// otherwise. Validates the session is present and not Closed before
    /// touching the actor, so invalid transitions skip here, never inside
    /// the actor. One-way: the Tell carries no reply, so the dispatcher
    /// never blocks on the actor. Never appends and never claims a turn:
    /// claim ownership and fencing stay with the actor's start path.
    /// <param name="store">The durable store.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to wake.</param>
    /// <param name="session">The suspendable session actor.</param>
    /// <param name="cancellationToken">Cancels the wake.</param>
    /// <returns>A task that completes once the wake was told.</returns>
    let checkInboxAsync
        (store: ISessionStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (session: IActorRef)
        (cancellationToken: CancellationToken)
        : Task =
        ArgumentNullException.ThrowIfNull(store)
        ArgumentNullException.ThrowIfNull(session)

        task {
            let! current = requireSessionAsync store tenant sessionId cancellationToken

            if current.State <> SessionState.Closed then
                session.Tell(SuspendableCheckInbox)
        }

    /// Aborts the turn running in a suspendable session: the client boundary
    /// turn-level verb. Idle is a no-op returning the current snapshot, as
    /// is WaitingForInput (a suspended turn owns nothing running to abort).
    /// Running records the pending stop under the typed cause: the detached
    /// suspendable turn runs un-cancellable, so the stop wins at its next
    /// report and a second abort keeps the first cause. Unknown sessions
    /// throw SessionNotFoundException and Closed sessions throw
    /// InvalidSessionStateException before touching the actor; a Close
    /// racing the abort maps to the same exception.
    /// <param name="store">The durable store.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to abort the turn in.</param>
    /// <param name="session">The suspendable session actor.</param>
    /// <param name="cause">Which abort-family stop cause wins: ExplicitAbort or HostShutdown.</param>
    /// <param name="reason">Why the turn stops. Must not be null. Never contains secrets or tool arguments.</param>
    /// <param name="cancellationToken">Cancels the abort.</param>
    /// <returns>The actor's snapshot after the abort was accepted.</returns>
    let abortSuspendableAsync
        (store: ISessionStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (session: IActorRef)
        (cause: StopCause)
        (reason: string)
        (cancellationToken: CancellationToken)
        : Task<SessionSnapshot> =
        ArgumentNullException.ThrowIfNull(store)
        ArgumentNullException.ThrowIfNull(session)

        if isNull (box reason) then
            raise (ArgumentNullException(nameof reason))

        if cause <> StopCause.ExplicitAbort && cause <> StopCause.HostShutdown then
            raise (
                ArgumentOutOfRangeException(
                    nameof cause,
                    "Only ExplicitAbort and HostShutdown abort a turn: the deadline arrives through the turn loop and lease loss through the claim fence."
                )
            )

        task {
            let! current = requireSessionAsync store tenant sessionId cancellationToken

            if current.State = SessionState.Closed then
                raise (
                    InvalidSessionStateException(
                        sessionId,
                        current.State.ToString(),
                        "The session is closed and accepts no abort."
                    )
                )

            let! snapshot =
                askSuspendableAsync<SessionSnapshot>
                    session
                    (SuspendableAbortSession(cause, reason, cancellationToken))
                    cancellationToken

            if snapshot.State = SessionState.Closed then
                return
                    raise (
                        InvalidSessionStateException(
                            sessionId,
                            snapshot.State.ToString(),
                            "The session closed before the abort was accepted."
                        )
                    )
            else
                return snapshot
        }

    /// Compacts a suspendable session on demand: the client boundary. Idle
    /// replays the journal and compacts now without starting a turn,
    /// answering the before/after estimates; Running arms the one-shot force
    /// flag the turn's force-aware hook honors at the next boundary;
    /// WaitingForInput no-ops (a suspended turn owns the history). Unknown
    /// sessions throw SessionNotFoundException and Closed sessions throw
    /// InvalidSessionStateException before touching the actor; a Close
    /// racing the compact maps to the same exception.
    /// <param name="store">The durable store.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to compact.</param>
    /// <param name="session">The suspendable session actor.</param>
    /// <param name="cancellationToken">Cancels the compact.</param>
    /// <returns>The actor's compact reply.</returns>
    let compactSuspendableAsync
        (store: ISessionStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (session: IActorRef)
        (cancellationToken: CancellationToken)
        : Task<SessionCompactReply> =
        ArgumentNullException.ThrowIfNull(store)
        ArgumentNullException.ThrowIfNull(session)

        task {
            let! current = requireSessionAsync store tenant sessionId cancellationToken

            if current.State = SessionState.Closed then
                raise (
                    InvalidSessionStateException(
                        sessionId,
                        current.State.ToString(),
                        "The session is closed and accepts no compact."
                    )
                )

            let! reply =
                askSuspendableAsync<SessionCompactReply>
                    session
                    (SuspendableCompactSession cancellationToken)
                    cancellationToken

            match reply with
            | CompactRejected rejectedState ->
                return
                    raise (
                        InvalidSessionStateException(
                            sessionId,
                            rejectedState.ToString(),
                            "The session closed before the compact was accepted."
                        )
                    )
            | _ -> return reply
        }

    /// Rebinds the agent a suspendable session converses with: the client
    /// boundary. Applies at once when the session is quiescent and records
    /// the rebind as pending otherwise; the recorded rebind applies at the
    /// next quiescent boundary. Unknown sessions throw
    /// SessionNotFoundException and Closed sessions throw
    /// InvalidSessionStateException before touching the actor; a Close
    /// racing the rebind maps to the same exception. Agent existence and
    /// enablement are enforced by the facade (which holds the agent store),
    /// never here: the actor applies the rebound id verbatim.
    /// <param name="store">The durable store.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to rebind.</param>
    /// <param name="session">The suspendable session actor.</param>
    /// <param name="agentId">The agent the session converses with from now on.</param>
    /// <param name="cancellationToken">Cancels the rebind.</param>
    /// <returns>The stored session: rebound when the rebind applied at once, unchanged while pending.</returns>
    let setAgentSuspendableAsync
        (store: ISessionStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (session: IActorRef)
        (agentId: AgentId)
        (cancellationToken: CancellationToken)
        : Task<Session> =
        ArgumentNullException.ThrowIfNull(store)
        ArgumentNullException.ThrowIfNull(session)

        task {
            let! current = requireSessionAsync store tenant sessionId cancellationToken

            if current.State = SessionState.Closed then
                raise (
                    InvalidSessionStateException(
                        sessionId,
                        current.State.ToString(),
                        "The session is closed and accepts no agent change."
                    )
                )

            let! reply =
                askSuspendableAsync<SessionSetAgentReply>
                    session
                    (SuspendableSetAgent(agentId, cancellationToken))
                    cancellationToken

            match reply with
            | SetAgentApplied rebound -> return rebound
            | SetAgentPending unchanged -> return unchanged
            | SetAgentRejected rejectedState ->
                return
                    raise (
                        InvalidSessionStateException(
                            sessionId,
                            rejectedState.ToString(),
                            "The session closed before the agent change was accepted."
                        )
                    )
        }

    /// Builds the production child-spawn factory the session router uses:
    /// like <see cref="M:Legate.SessionActor.spawnFactory" /> but spawning
    /// the suspendable behavior (<see cref="M:Legate.SessionActor.behaviorWithSuspend" />),
    /// so live sessions suspend on Ask instead of running the base loop.
    /// The suspendable runner carries the DI-resolved
    /// <see cref="T:Legate.IPermissionPolicy" />: the caller builds it (see
    /// SessionPermissions.createRunner) with the policy the container
    /// resolved, and this factory threads it into every child it spawns.
    /// The journal token is primed per session at spawn: when the session
    /// row exists the factory appends a bootstrap inbox entry and claims it,
    /// so the suspend and resolve journal writes fence on a live claim
    /// (mirroring the harness prime); the bootstrap entry is consumed by the
    /// claim, so real prompts still drain first. When the row is missing the
    /// child starts as an empty Idle shell over a fallback token and the
    /// client boundary rejects its mutations, exactly like the base shell.
    /// Claim renewal while suspended belongs to the dispatcher and heartbeat
    /// cycle: the primed lease covers the configured AskTimeout window, and
    /// a lapsed lease settles the turn Failed with the typed reason instead
    /// of journaling half a suspension. The on-demand compaction wiring
    /// comes from compactFor per spawned child, sharing the primed token.
    /// <param name="store">The durable store session actors persist through.</param>
    /// <param name="tenant">The tenant router-spawned sessions belong to.</param>
    /// <param name="eventStore">The journal suspend and resolve events append to.</param>
    /// <param name="delay">The seam the AskTimeout deadline fires off.</param>
    /// <param name="askTimeout">How long a suspension waits for its Reply before settling Failed. Must be positive.</param>
    /// <param name="claimOwner">The claim owner identity the journal prime claims under. Must not be null.</param>
    /// <param name="leaseDuration">How long the primed journal claim lasts. Must be positive.</param>
    /// <param name="runSuspendable">Runs one suspendable attempt, bound to the DI-resolved policy. Never null.</param>
    /// <param name="compactFor">Builds the on-demand compaction wiring for one session from its primed journal token, or None when the host compacts nothing. Must not be null; return None to answer CompactNotNeeded.</param>
    /// <param name="agentStore">The agent catalog the per-turn authority gate reads, or null when the host runs without one: the gate is skipped then.</param>
    /// <param name="eraMarked">Reads the completion era the entity-start probe consults (issue 289). Never null.</param>
    /// <returns>A factory mapping a session id string to a suspendable child spawn.</returns>
    let spawnSuspendFactoryRouted
        (routes: CompletionDestinations option)
        (store: ISessionStore)
        (tenant: TenantId)
        (eventStore: ISessionEventStore)
        (delay: ILlmDelay)
        (askTimeout: TimeSpan)
        (claimOwner: string)
        (leaseDuration: TimeSpan)
        (runSuspendable: SuspendableRunner)
        (compactFor: SessionId -> string -> CompactDeps option)
        (agentStore: IAgentStore | null)
        (eraMarked: CompletionEra.CompletionEraReader)
        : (string -> IActorContext -> string -> IActorRef) =
        ArgumentNullException.ThrowIfNull(store)
        ArgumentNullException.ThrowIfNull(eventStore)
        ArgumentNullException.ThrowIfNull(delay)

        if String.IsNullOrWhiteSpace claimOwner then
            raise (ArgumentException("The claim owner must be a non-empty string.", nameof claimOwner))

        if askTimeout <= TimeSpan.Zero then
            raise (ArgumentOutOfRangeException(nameof askTimeout, "AskTimeout must be positive."))

        if leaseDuration <= TimeSpan.Zero then
            raise (ArgumentOutOfRangeException(nameof leaseDuration, "The lease duration must be positive."))

        if isNull (box runSuspendable) then
            raise (ArgumentNullException(nameof runSuspendable))

        if isNull (box eraMarked) then
            raise (ArgumentNullException(nameof eraMarked))

        if isNull (box compactFor) then
            raise (ArgumentNullException(nameof compactFor))

        /// The base turn runner never runs on a suspendable child: the
        /// suspend behavior drives RunSuspendable only. It stays non-null
        /// because the props contract requires it.
        let unusedRunTurn (_: InboxEntry) (_: CancellationToken) : Task<TurnResult> =
            Task.FromException<TurnResult>(
                InvalidOperationException("A suspendable session actor never runs its base turn runner.")
            )

        /// Primes the journal token for one session: appends a bootstrap
        /// entry and claims it, returning the live claim token. A missing
        /// session row (or any prime failure) falls back to a fresh token:
        /// the child starts as an Idle shell whose mutations the boundary
        /// rejects, so the token never fences a real write.
        /// Primes the journal claim for one session: appends a bootstrap
        /// entry and claims it, returning the live claim. A missing session
        /// row (or any prime failure) primes nothing: the child starts as an
        /// Idle shell over a fallback token and the client boundary rejects
        /// its mutations, so the token never fences a real write. The same
        /// prime backs the SetAgent re-prime the behavior runs after
        /// settling its primed claim: every call appends a fresh bootstrap
        /// and claims it, and the claim consumes the bootstrap, so real
        /// prompts still drain first.
        let primeClaim (sessionId: SessionId) : TurnClaim option =
            try
                match store.GetSession(tenant, sessionId, CancellationToken.None).GetAwaiter().GetResult() with
                | null -> None
                | session when isNull (box session.Options) ->
                    raise (
                        CompletionRoutingException(
                            Nullable tenant,
                            Nullable sessionId,
                            null,
                            CompletionRoutingReason.UnsupportedFormat
                        )
                    )
                | session ->
                    session.Options.ValidatePersistence()

                    match routes, session.Options.CompletionDestinationId with
                    | Some registry, _ -> registry.Validate session
                    | None, null -> ()
                    | None, id ->
                        raise (
                            CompletionRoutingException(
                                Nullable tenant,
                                Nullable sessionId,
                                id,
                                CompletionRoutingReason.Unknown
                            )
                        )

                    let bootstrap =
                        UserMessagePayload(UserMessage.Text "legate journal prime") :> InboxPayload

                    store
                        .AppendInboxMessage(tenant, sessionId, bootstrap, DeliveryMode.Queue, CancellationToken.None)
                        .GetAwaiter()
                        .GetResult()
                    |> ignore

                    match
                        store
                            .ClaimNextTurn(tenant, sessionId, claimOwner, leaseDuration, CancellationToken.None)
                            .GetAwaiter()
                            .GetResult()
                    with
                    | :? TurnLeaseRenewed as renewed when not (isNull (box renewed)) -> Some renewed.Claim
                    | :? TurnLeaseHeld as held when not (isNull (box held)) -> Some held.Claim
                    | :? TurnLeaseExpiring as expiring when not (isNull (box expiring)) -> Some expiring.Claim
                    | _ -> None
            with _ ->
                None

        let recoverTarget captured : ControlTargetRecovery | null =
            match store with
            | :? ISessionAbortControlStore as control ->
                match control.ReadAbortTarget(tenant, captured, CancellationToken.None).GetAwaiter().GetResult() with
                | null -> null
                | target when target.State = ControlTargetState.Active && isNull (box target.Stop) ->
                    let result =
                        control
                            .TryRecoverControlTarget(
                                tenant,
                                captured,
                                target.TurnId,
                                claimOwner,
                                leaseDuration,
                                CancellationToken.None
                            )
                            .GetAwaiter()
                            .GetResult()

                    if result.Outcome <> ControlOperationOutcome.Applied then
                        let category =
                            if result.Outcome = ControlOperationOutcome.Stopped then
                                "controlPending"
                            else
                                "executionAuthorityUnavailable"

                        raise (
                            InvalidSessionStateException(
                                captured,
                                category,
                                "Recovery cannot acquire genuine authority for this exact unstopped target."
                            )
                        )

                    result
                | _ ->
                    raise (
                        InvalidSessionStateException(
                            captured,
                            "controlPending",
                            "Accepted stop or pending control decision forbids activation."
                        )
                    )
            | _ -> raise (InvalidOperationException("ISessionAbortControlStore is required before session activation."))

        fun sessionId context name ->
            let mutable parsed = Unchecked.defaultof<SessionId>

            if SessionId.TryParse(sessionId, &parsed) then
                let captured = parsed

                let validateRoute () =
                    match store.GetSession(tenant, captured, CancellationToken.None).GetAwaiter().GetResult() with
                    | null -> raise (SessionNotFoundException(captured, "The session does not exist."))
                    | session when isNull (box session.Options) ->
                        raise (
                            CompletionRoutingException(
                                Nullable tenant,
                                Nullable captured,
                                null,
                                CompletionRoutingReason.UnsupportedFormat
                            )
                        )
                    | session ->
                        session.Options.ValidatePersistence()

                        match routes, session.Options.CompletionDestinationId with
                        | Some registry, _ -> registry.Validate session
                        | None, null -> ()
                        | None, id ->
                            raise (
                                CompletionRoutingException(
                                    Nullable tenant,
                                    Nullable captured,
                                    id,
                                    CompletionRoutingReason.Unknown
                                )
                            )

                let refusal =
                    try
                        validateRoute ()
                        None
                    with :? CompletionRoutingException as error ->
                        Some
                            {
                                Tenant = tenant
                                SessionId = captured
                                DestinationId = error.DestinationId
                                Reason = error.Reason
                            }

                let blocked (error: CompletionRoutingRefused) (mailbox: Actor<SuspendableActorMessage>) =
                    let rec loop () =
                        actor {
                            let! message = mailbox.Receive()

                            match message with
                            | SuspendableGetSnapshot ->
                                let session =
                                    awaitTask (requireSessionAsync store tenant captured CancellationToken.None)

                                let pending =
                                    awaitTask (store.ReadPendingInbox(tenant, captured, CancellationToken.None))

                                mailbox.Sender()
                                <! {
                                       SessionId = captured
                                       State = session.State
                                       PendingCount = pending.Count
                                       RunningPosition = None
                                       PendingRequestId = null
                                   }
                            | _ -> mailbox.Sender() <! error

                            return! loop ()
                        }

                    loop ()

                let recovery = if refusal.IsNone then recoverTarget captured else null

                let primed =
                    match recovery with
                    | null when refusal.IsNone -> primeClaim captured
                    | null -> None
                    | recovery -> recovery.Claim |> Option.ofObj

                if primed.IsNone && refusal.IsNone then
                    raise (
                        InvalidSessionStateException(
                            captured,
                            "executionAuthorityUnavailable",
                            "Activation acquired no genuine prime authority; no runner may start."
                        )
                    )

                let token =
                    match primed with
                    | Some claim -> claim.Token
                    | None -> Guid.NewGuid().ToString("N")

                let props: SessionActorProps =
                    {
                        Store = store
                        Tenant = tenant
                        SessionId = captured
                        RunTurn = unusedRunTurn
                        OnTurnSettled = Some(fun result -> PromptWaitHubs.ObserveSettledScoped tenant captured result)
                        OnInjectJournaled = None
                        Compact = compactFor captured token
                        Logger = null
                    }

                let suspend: SuspendDeps =
                    {
                        EventStore = eventStore
                        Delay = delay
                        AskTimeout = askTimeout
                        JournalToken = token
                        PrimeClaim = primed
                        Recovery = recovery
                        RunSuspendable = runSuspendable
                        ReprimeJournal = Some(fun () -> primeClaim captured)
                        RefreshCompact = Some(compactFor captured)
                        AgentStore = agentStore
                        EraMarked = eraMarked
                    }

                match refusal with
                | Some error -> spawn context name (blocked error)
                | None -> spawn context name (behaviorWithSuspendRouted validateRoute props suspend)
            else
                spawn context name (actorOf (fun (_: obj) -> ()))

    let spawnSuspendFactory
        store
        tenant
        eventStore
        delay
        askTimeout
        claimOwner
        leaseDuration
        runSuspendable
        compactFor
        agentStore
        eraMarked
        =
        spawnSuspendFactoryRouted
            None
            store
            tenant
            eventStore
            delay
            askTimeout
            claimOwner
            leaseDuration
            runSuspendable
            compactFor
            agentStore
            eraMarked
