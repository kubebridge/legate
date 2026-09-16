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
        /// The source Close cancels to abort the turn.
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

            let journalAsync =
                journalSink compact.EventStore props.Tenant props.SessionId compact.JournalToken

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
                    TurnId = TurnId.New()
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

    /// Enqueues the settlement's completion row and notifies the session's
    /// sink inline with the same stored key, in the actor's settlement
    /// step. Only sessions carrying a CompletionSink enqueue: sinkless
    /// sessions notify nothing and store nothing. Best-effort and guarded:
    /// a store failure skips the Notify (no key was shared, so the
    /// re-drive has nothing to duplicate), a throwing sink never kills the
    /// actor, and the actor-thread sequencing is the fence: this actor
    /// holds no TurnClaim, so ClaimFence.notifyIfLiveAsync has nothing to
    /// verify, and the re-drive deduplicates on the shared key.
    /// <param name="props">The session actor dependencies.</param>
    /// <param name="result">The settled turn result to deliver.</param>
    /// <returns>The delivered completion, or None when sinkless or best-effort failed.</returns>
    let private dispatchCompletion (props: SessionActorProps) (result: TurnResult) : SessionCompletion option =
        try
            if isNull (box result) then
                None
            else
                match awaitTask (props.Store.GetSession(props.Tenant, props.SessionId, CancellationToken.None)) with
                | null -> None
                | session when isNull (box session.Options) -> None
                | session ->
                    match session.Options.CompletionSink with
                    | null -> None
                    | sink ->
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
                                        completion,
                                        CancellationToken.None
                                    )
                                )
                                |> Some
                            with _ ->
                                None

                        match stored with
                        | None -> None
                        | Some row ->
                            try
                                sink.Notify(row.Completion)
                            with _ ->
                                ()

                            Some row.Completion
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

            { Entry = entry; Cts = cts }

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
                        OnTurnSettled = Some(fun result -> PromptWaitHubs.ObserveSettled captured result)
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
    /// Tests inject scripted runners; the TurnLoop-backed runner wires
    /// TurnLoop.runSuspendableAsync plus the resume continuations.
    type SuspendableRunner =
        InboxEntry
            -> int
            -> HashSet<string>
            -> TurnLoop.TurnLoopSuspension option
            -> Reply option
            -> CancellationToken
            -> Task<TurnLoop.TurnLoopCompletion>

    /// What a suspendable session actor is built from: the base actor
    /// dependencies plus the journal, the AskTimeout seam, the journal
    /// token, and the suspendable runner. The journal token fences journal
    /// appends; tests prime it with ISessionStore.ClaimNextTurn so the
    /// in-memory journal lands, and a takeover re-claims so the loser
    /// appends nothing. The heartbeat is not cancelled on suspend: renewal
    /// continues while suspended under the same claim, bounded by lease
    /// expiry and AskTimeout.
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
            /// Runs one suspendable attempt. Never null.
            RunSuspendable: SuspendableRunner
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

        /// The suspendable turn faulted. One-way from the turn task.
        | SuspendableFaulted of entry: InboxEntry * error: Exception * attempt: int

        /// A Reply inbox entry arrived for the suspended turn. Answered with
        /// SessionReplyReply; a mismatch rejects with ReplyMismatchException.
        | ReplyEntry of entry: InboxEntry

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

        /// Compact the suspendable session on demand: Idle replays the
        /// journal and compacts now without starting a turn, Running arms
        /// the one-shot force flag the turn's force-aware boundary hook
        /// honors at the next iteration, WaitingForInput no-ops (a suspended
        /// turn owns the history), and Closed rejects. Answered with
        /// <see cref="T:Legate.SessionCompactReply" />.
        | SuspendableCompactSession of cancellationToken: CancellationToken

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
    let behaviorWithSuspend
        (props: SessionActorProps)
        (suspend: SuspendDeps)
        (mailbox: Actor<SuspendableActorMessage>)
        =
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

        let initialRecovered: SessionState * RebuiltPending option =
            let found =
                awaitTask (props.Store.GetSession(props.Tenant, props.SessionId, CancellationToken.None))

            match found with
            | null -> SessionState.Idle, None
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

                    SessionState.Idle, None
                | SessionState.WaitingForInput ->
                    let rebuilt =
                        rebuildPendingFromJournal suspend.EventStore props.Tenant props.SessionId

                    SessionState.WaitingForInput, rebuilt
                | SessionState.Idle -> SessionState.Idle, None
                | SessionState.Closed -> SessionState.Closed, None
                | unknown -> unknown, None

        let initialState, initialRebuilt = initialRecovered

        let initialSuspended: SuspendedTurn option =
            match initialState, initialRebuilt with
            | SessionState.WaitingForInput, Some rebuilt ->
                // Crash rebuild: no cursor and no running task; the matching
                // Reply retries from the oldest drainable entry. The
                // timeout is not restarted here: the AskTimeout bound restarts
                // when the retried turn suspends again, so a restarted host
                // never inherits a fired deadline.
                let queueEntry =
                    try
                        let pending =
                            awaitTask (
                                props.Store.ReadPendingInbox(props.Tenant, props.SessionId, CancellationToken.None)
                            )

                        selectDrainableEntries pending |> List.tryHead
                    with _ ->
                        None

                match queueEntry with
                | Some entry ->
                    Some
                        {
                            Entry = entry
                            Cursor = None
                            Rebuilt = Some rebuilt
                            Allowed = readGrantsNow ()
                            Attempt = 1
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

            notifySettled result
            dispatchCompletion props result |> ignore

            let positions = [| entry.Position |] :> IReadOnlyList<int64>

            awaitTask (props.Store.MarkInboxConsumed(props.Tenant, props.SessionId, positions, CancellationToken.None))
            |> ignore

            awaitTask (
                props.Store.UpdateSessionState(props.Tenant, props.SessionId, SessionState.Idle, CancellationToken.None)
            )
            |> ignore

        let journalSuspend (suspension: TurnLoop.TurnLoopSuspension) : JournalWriter.JournalWriteResult =
            let turnId = TurnId.New()
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
                    suspend.JournalToken
                    events
                    CancellationToken.None
            )

        let journalResolve (reply: Reply) : JournalWriter.JournalWriteResult =
            let turnId = TurnId.New()
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
                        suspend.JournalToken
                        events
                        CancellationToken.None
                )

        let journalTimeout () : unit =
            let turnId = TurnId.New()
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
                    suspend.JournalToken
                    events
                    CancellationToken.None
            )
            |> ignore

        let startSuspendable (entry: InboxEntry) (attempt: int) (allowed: HashSet<string>) : unit =
            let runTask =
                try
                    let started =
                        suspend.RunSuspendable entry attempt allowed None None CancellationToken.None

                    if isNull (box started) then
                        Task.FromException<TurnLoop.TurnLoopCompletion>(
                            InvalidOperationException("The suspendable turn runner returned null.")
                        )
                    else
                        started
                with ex ->
                    Task.FromException<TurnLoop.TurnLoopCompletion>(ex)

            runTask.ContinueWith(fun (completed: Task<TurnLoop.TurnLoopCompletion>) ->
                if completed.IsCanceled then
                    suspendSelf.Tell(
                        SuspendableFaulted(
                            entry,
                            OperationCanceledException("The suspendable turn was aborted."),
                            attempt
                        )
                    )
                elif completed.IsFaulted then
                    let error =
                        match completed.Exception with
                        | null ->
                            InvalidOperationException("The suspendable turn faulted without an exception.")
                            :> Exception
                        | aggregate -> aggregate.GetBaseException()

                    suspendSelf.Tell(SuspendableFaulted(entry, error, attempt))
                else
                    suspendSelf.Tell(SuspendableFinished(entry, completed.Result, attempt, allowed)))
            |> ignore

        let resumeSuspendable (parked: SuspendedTurn) (reply: Reply) (nextAttempt: int) : unit =
            let cursor =
                match parked.Cursor with
                | Some live -> Some live
                | None -> None

            let runTask =
                try
                    let started =
                        suspend.RunSuspendable
                            parked.Entry
                            nextAttempt
                            parked.Allowed
                            cursor
                            (Some reply)
                            CancellationToken.None

                    if isNull (box started) then
                        Task.FromException<TurnLoop.TurnLoopCompletion>(
                            InvalidOperationException("The suspendable turn runner returned null.")
                        )
                    else
                        started
                with ex ->
                    Task.FromException<TurnLoop.TurnLoopCompletion>(ex)

            runTask.ContinueWith(fun (completed: Task<TurnLoop.TurnLoopCompletion>) ->
                if completed.IsCanceled then
                    suspendSelf.Tell(
                        SuspendableFaulted(
                            parked.Entry,
                            OperationCanceledException("The resumed turn was aborted."),
                            nextAttempt
                        )
                    )
                elif completed.IsFaulted then
                    let error =
                        match completed.Exception with
                        | null ->
                            InvalidOperationException("The resumed turn faulted without an exception.") :> Exception
                        | aggregate -> aggregate.GetBaseException()

                    suspendSelf.Tell(SuspendableFaulted(parked.Entry, error, nextAttempt))
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

        let rec loop (state: SessionState) (suspended: SuspendedTurn option) (resolved: HashSet<string>) =
            actor {
                let! message = mailbox.Receive()

                match message with
                | SuspendableQueuePrompt(payload, cancellationToken) ->
                    match state with
                    | SessionState.Closed ->
                        mailbox.Sender() <! PromptRejected SessionState.Closed
                        return! loop state suspended resolved
                    | SessionState.Idle ->
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

                        awaitTask (
                            props.Store.UpdateSessionState(
                                props.Tenant,
                                props.SessionId,
                                SessionState.Running,
                                cancellationToken
                            )
                        )
                        |> ignore

                        let pending =
                            awaitTask (props.Store.ReadPendingInbox(props.Tenant, props.SessionId, cancellationToken))

                        let first =
                            selectDrainableEntries pending |> List.tryHead |> Option.defaultValue appended

                        startSuspendable first 1 (readGrantsNow ())
                        mailbox.Sender() <! PromptAccepted appended
                        return! loop SessionState.Running None resolved
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

                        awaitTask (
                            props.Store.UpdateSessionState(
                                props.Tenant,
                                props.SessionId,
                                SessionState.Running,
                                cancellationToken
                            )
                        )
                        |> ignore

                        let pending =
                            awaitTask (props.Store.ReadPendingInbox(props.Tenant, props.SessionId, cancellationToken))

                        let first =
                            selectDrainableEntries pending |> List.tryHead |> Option.defaultValue appended

                        startSuspendable first 1 (readGrantsNow ())
                        mailbox.Sender() <! PromptAccepted appended
                        return! loop SessionState.Running None resolved
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

                        awaitTask (
                            props.Store.UpdateSessionState(
                                props.Tenant,
                                props.SessionId,
                                SessionState.Running,
                                cancellationToken
                            )
                        )
                        |> ignore

                        let pending =
                            awaitTask (props.Store.ReadPendingInbox(props.Tenant, props.SessionId, cancellationToken))

                        let first =
                            selectDrainableEntries pending |> List.tryHead |> Option.defaultValue appended

                        startSuspendable first 1 (readGrantsNow ())
                        mailbox.Sender() <! PromptAccepted appended
                        return! loop SessionState.Running None resolved
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
                    | SessionState.Running, None ->
                        // A recorded stop wins over whatever the detached
                        // turn reported, even a success or a suspension: map
                        // to Aborted and clear the cell. Settlement already
                        // won when the cell is empty.
                        let stop = pendingStop
                        pendingStop <- None

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
                        // <returns>The next loop state.</returns>
                        let settleEntryNow (entry: InboxEntry) (result: TurnResult) : SessionState =
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

                            if result.Status = TurnStatus.Completed && autoCloseEnabled props then
                                // AutoClose (issue 82): the first Completed
                                // turn closes the session store-first instead
                                // of draining; the entry is already consumed
                                // above. Aborted and Failed results never take
                                // this path, so failed runs stay open for
                                // inspection.
                                awaitTask (
                                    props.Store.CloseSession(props.Tenant, props.SessionId, CancellationToken.None)
                                )
                                |> ignore

                                SessionState.Closed
                            else
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
                                    startSuspendable following 1 (readGrantsNow ())
                                    SessionState.Running
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

                                let parked =
                                    {
                                        Entry = entry
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
                            // journaled and nothing parked for a Reply.
                            let next = settleEntryNow entry carried
                            return! loop next None resolved
                    | SessionState.WaitingForInput, Some parked when parked.Cursor.IsNone && parked.Rebuilt.IsSome ->
                        // Crash-retry path should never produce a running
                        // finish while still parked; ignore stale completions.
                        return! loop state suspended resolved
                    | _ -> return! loop state suspended resolved
                | SuspendableFaulted(entry, _, _) ->
                    match state, suspended with
                    | SessionState.Running, None ->
                        // A recorded stop wins even over a real fault:
                        // settle Aborted under the cause instead of failing
                        // silently. The cell clears on every fault settle.
                        let stop = pendingStop
                        pendingStop <- None

                        match stop with
                        | Some(cause, reason) ->
                            let settled = abortedSuspendResult cause reason
                            notifySettled settled
                            dispatchCompletion props settled |> ignore
                        | None -> ()

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
                | ReplyEntry replyEntry ->
                    match state, suspended with
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
                                            startSuspendable parked.Entry nextAttempt parked.Allowed
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

                            journalTimeout ()

                            let result = timeoutResult ()
                            notifySettled result
                            dispatchCompletion props result |> ignore

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
                        match props.Compact with
                        | None ->
                            mailbox.Sender() <! CompactNotNeeded
                            return! loop state suspended resolved
                        | Some compact ->
                            let reply = compactIdleNow props compact cancellationToken
                            mailbox.Sender() <! reply
                            return! loop state suspended resolved
                    | SessionState.Running ->
                        match props.Compact with
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
                | SuspendableCloseSession cancellationToken ->
                    match suspended with
                    | Some parked ->
                        try
                            parked.TimeoutCts.Cancel()
                        with _ ->
                            ()
                    | None -> ()

                    // A recorded stop dies with the session: Closed settles
                    // nothing further.
                    pendingStop <- None

                    let closed =
                        awaitTask (props.Store.CloseSession(props.Tenant, props.SessionId, cancellationToken))

                    mailbox.Sender() <! closed
                    return! loop SessionState.Closed None resolved
                | SuspendableGetSnapshot ->
                    mailbox.Sender() <! takeSuspendSnapshot state suspended
                    return! loop state suspended resolved
            }

        loop initialState initialSuspended (HashSet<string>())

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

            let! reply = session.Ask<'Reply>(message, linkedCts.Token)
            return reply
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

            let payload = ReplyPayload(reply) :> InboxPayload

            let! appended = store.AppendInboxMessage(tenant, sessionId, payload, DeliveryMode.Queue, cancellationToken)

            let! answer = askSuspendableAsync<SessionReplyReply> session (ReplyEntry appended) cancellationToken

            match answer with
            | ReplyAccepted entry -> return entry
            | ReplyRejected error -> return raise error
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
    /// <returns>A factory mapping a session id string to a suspendable child spawn.</returns>
    let spawnSuspendFactory
        (store: ISessionStore)
        (tenant: TenantId)
        (eventStore: ISessionEventStore)
        (delay: ILlmDelay)
        (askTimeout: TimeSpan)
        (claimOwner: string)
        (leaseDuration: TimeSpan)
        (runSuspendable: SuspendableRunner)
        (compactFor: SessionId -> string -> CompactDeps option)
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
        let primeToken (sessionId: SessionId) : string =
            try
                match store.GetSession(tenant, sessionId, CancellationToken.None).GetAwaiter().GetResult() with
                | null -> Guid.NewGuid().ToString("N")
                | _ ->
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
                    | :? TurnLeaseRenewed as renewed when not (isNull (box renewed)) -> renewed.Claim.Token
                    | :? TurnLeaseHeld as held when not (isNull (box held)) -> held.Claim.Token
                    | :? TurnLeaseExpiring as expiring when not (isNull (box expiring)) -> expiring.Claim.Token
                    | _ -> Guid.NewGuid().ToString("N")
            with _ ->
                Guid.NewGuid().ToString("N")

        fun sessionId context name ->
            let mutable parsed = Unchecked.defaultof<SessionId>

            if SessionId.TryParse(sessionId, &parsed) then
                let captured = parsed
                let token = primeToken captured

                let props: SessionActorProps =
                    {
                        Store = store
                        Tenant = tenant
                        SessionId = captured
                        RunTurn = unusedRunTurn
                        OnTurnSettled = Some(fun result -> PromptWaitHubs.ObserveSettled captured result)
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
                        RunSuspendable = runSuspendable
                    }

                spawn context name (behaviorWithSuspend props suspend)
            else
                spawn context name (actorOf (fun (_: obj) -> ()))
