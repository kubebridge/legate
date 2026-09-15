// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open Akka.Actor
open Akka.FSharp
open Microsoft.Extensions.AI

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

/// How the session actor answers a Queue prompt. The actor never throws
/// InvalidSessionStateException: a Closed-state prompt is rejected with the
/// current state and the client boundary maps it to the exception.
type internal SessionPromptReply =

    /// The prompt was accepted: the entry was appended to the durable inbox
    /// (and a turn started when the session was Idle).
    | PromptAccepted of entry: InboxEntry

    /// The prompt arrived while the session was Closed; nothing was
    /// appended. Carries the state the session was in.
    | PromptRejected of state: SessionState

/// The session actor protocol. QueuePrompt, CloseSession, and GetSnapshot
/// are answered to the sender; the SessionTurnSettled and SessionTurnFaulted completions
/// are one-way (Tell) from the turn task back to the actor.
type internal SessionActorMessage =

    /// Queue a user message: appends to the durable inbox, starts a turn
    /// when Idle, waits when Running or WaitingForInput, rejects when
    /// Closed. Only Queue delivery reaches the actor; the client boundary
    /// enforces that. Answered with
    /// <see cref="T:Legate.SessionPromptReply" />.
    | QueuePrompt of payload: InboxPayload * cancellationToken: CancellationToken

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

    /// Awaits a fast store metadata task synchronously on the actor thread.
    /// The actor computation cannot bind tasks (only mailbox receives), and
    /// blocking here preserves the single-threaded sequencing the
    /// store-first transitions rely on. The unbounded turn execution never
    /// blocks: it runs fire-and-forget and pipes its outcome back.
    /// <param name="task">The store task to await.</param>
    /// <returns>The task's result.</returns>
    let private awaitTask<'T> (task: Task<'T>) : 'T = task.GetAwaiter().GetResult()

    /// Selects the drainable entries from a pending read in position order:
    /// Queue delivery carrying a user message. Anything else (Inject,
    /// Interrupt, Reply payloads) stays pending for its owning issue. Null
    /// entries and a null read result are treated as empty.
    /// <param name="entries">The pending read result.</param>
    /// <returns>The drainable entries in position order.</returns>
    let private selectQueueUserMessages (entries: IReadOnlyList<InboxEntry>) : InboxEntry list =
        if isNull (box entries) then
            []
        else
            entries
            |> Seq.filter (fun entry ->
                not (isNull (box entry))
                && entry.Delivery = DeliveryMode.Queue
                && (entry.Payload :? UserMessagePayload))
            |> Seq.sortBy (fun entry -> entry.Position)
            |> List.ofSeq

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

    /// Builds a Queue turn runner over TurnLoop.runAsync: one ChatRole.User
    /// history message from the entry's parts and no Inject fold, running
    /// under the given lease hook and the given deadline seam.
    /// <param name="client">The chat client the turn runs against.</param>
    /// <param name="tools">The tools the turn may call.</param>
    /// <param name="options">The turn loop tuning and per-turn budget.</param>
    /// <param name="delay">The delay seam the hard deadline fires off. Must not be null.</param>
    /// <param name="isLeaseValid">The lease hook the loop checks. Must not be null.</param>
    /// <returns>A runner executing one Queue inbox entry per turn.</returns>
    let private runnerFor
        (client: IChatClient)
        (tools: IReadOnlyDictionary<string, AITool>)
        (options: TurnLoop.TurnLoopOptions)
        (delay: ILlmDelay)
        (isLeaseValid: unit -> bool)
        : (InboxEntry -> CancellationToken -> Task<TurnResult>) =
        ArgumentNullException.ThrowIfNull(client)
        ArgumentNullException.ThrowIfNull(tools)
        ArgumentNullException.ThrowIfNull(delay)
        ArgumentNullException.ThrowIfNull(isLeaseValid)

        fun entry cancellationToken ->
            task {
                let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

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

                return! TurnLoop.runAsync client history tools options delay cancellationToken isLeaseValid
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
        runnerFor client tools options delay (fun () -> true)

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

        let initialState = recover props
        let self = mailbox.Self

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
        /// store-first, then drains the next pending Queue entry into a
        /// new turn or returns the session to Idle.
        /// <param name="entry">The entry the finished attempt executed.</param>
        /// <param name="cancellationToken">Abandons the settle reads.</param>
        /// <returns>The next loop state and in-flight turn.</returns>
        let settle (entry: InboxEntry) (cancellationToken: CancellationToken) : SessionState * RunningTurn option =
            let positions = [| entry.Position |] :> IReadOnlyList<int64>

            awaitTask (props.Store.MarkInboxConsumed(props.Tenant, props.SessionId, positions, cancellationToken))
            |> ignore

            let pending =
                awaitTask (props.Store.ReadPendingInbox(props.Tenant, props.SessionId, cancellationToken))

            match selectQueueUserMessages pending with
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
                        mailbox.Sender() <! PromptRejected SessionState.Closed
                        return! loop state running arbitration pendingStop
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

                        // Drain oldest-first: the just-appended entry joins
                        // whatever the inbox already held (for example work
                        // that survived a restart), and the earliest entry
                        // starts the turn.
                        let pending =
                            awaitTask (props.Store.ReadPendingInbox(props.Tenant, props.SessionId, cancellationToken))

                        let first =
                            selectQueueUserMessages pending |> List.tryHead |> Option.defaultValue appended

                        let next = startTurn first
                        mailbox.Sender() <! PromptAccepted appended
                        return! loop SessionState.Running (Some next) StopArbitration.Undecided None
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
                        return! loop state running arbitration pendingStop
                    | _ ->
                        // Out-of-range stored state: stay durable but start
                        // nothing new.
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
                            let nextState, nextRunning = settle entry CancellationToken.None
                            return! loop nextState nextRunning StopArbitration.Undecided None
                        | StopArbitration.Decided(StopArbitration.StopWins cause) ->
                            // The stop landed first, so it wins even over a
                            // success: map to Aborted under the winning
                            // cause, then run the settle bookkeeping once.
                            inFlight.Cts.Dispose()

                            let reason = pendingStop |> Option.map snd |> Option.defaultValue ""

                            notifySettled (mapAborted cause reason result)
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
                            let nextState, nextRunning = settle entry CancellationToken.None
                            return! loop nextState nextRunning StopArbitration.Undecided None
                        | StopArbitration.Decided(StopArbitration.StopWins cause) ->
                            // The stop arrived first, so it wins even over
                            // a real fault: settle Aborted under the cause.
                            inFlight.Cts.Dispose()

                            let reason = pendingStop |> Option.map snd |> Option.defaultValue ""

                            notifySettled (abortedResult cause reason)
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
                let props =
                    {
                        Store = store
                        Tenant = tenant
                        SessionId = parsed
                        RunTurn = runTurn
                        OnTurnSettled = None
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

            let! reply =
                askAsync<SessionPromptReply> session (QueuePrompt(payload, cancellationToken)) cancellationToken

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
    /// never starts a turn: it only resumes the suspended one, and
    /// Inject/Interrupt routing stays with #34. Abort on WaitingForInput
    /// stays a no-op per #35.
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

        let suspendSelf = mailbox.Self

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
                // Reply retries from the oldest pending Queue entry. The
                // timeout is not restarted here: the AskTimeout bound restarts
                // when the retried turn suspends again, so a restarted host
                // never inherits a fired deadline.
                let queueEntry =
                    try
                        let pending =
                            awaitTask (
                                props.Store.ReadPendingInbox(props.Tenant, props.SessionId, CancellationToken.None)
                            )

                        pending
                        |> Seq.filter (fun entry ->
                            not (isNull (box entry))
                            && entry.Delivery = DeliveryMode.Queue
                            && (entry.Payload :? UserMessagePayload))
                        |> Seq.sortBy (fun entry -> entry.Position)
                        |> Seq.tryHead
                    with _ ->
                        None

                match queueEntry with
                | Some entry ->
                    Some
                        {
                            Entry = entry
                            Cursor = None
                            Rebuilt = Some rebuilt
                            Allowed = HashSet<string>()
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

        let notifySettled (result: TurnResult) : unit =
            match props.OnTurnSettled with
            | Some observe ->
                try
                    observe result
                with _ ->
                    ()
            | None -> ()

        let journalSuspend (suspension: TurnLoop.TurnLoopSuspension) : unit =
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

            try
                awaitTask (
                    suspend.EventStore.Append(
                        props.Tenant,
                        props.SessionId,
                        suspend.JournalToken,
                        events,
                        CancellationToken.None
                    )
                )
                |> ignore
            with _ ->
                ()

        let journalResolve (reply: Reply) : unit =
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
            | None -> ()
            | Some event ->
                let events = ResizeArray<SessionEvent>([| event |]) :> IReadOnlyList<SessionEvent>

                try
                    awaitTask (
                        suspend.EventStore.Append(
                            props.Tenant,
                            props.SessionId,
                            suspend.JournalToken,
                            events,
                            CancellationToken.None
                        )
                    )
                    |> ignore
                with _ ->
                    ()

        let journalTimeout () : unit =
            let turnId = TurnId.New()
            let stamp = DateTimeOffset.UtcNow

            let event =
                TurnFailedEvent(props.SessionId, turnId, Nullable<int64>(), stamp, AskTimeoutReason) :> SessionEvent

            let events = ResizeArray<SessionEvent>([| event |]) :> IReadOnlyList<SessionEvent>

            try
                awaitTask (
                    suspend.EventStore.Append(
                        props.Tenant,
                        props.SessionId,
                        suspend.JournalToken,
                        events,
                        CancellationToken.None
                    )
                )
                |> ignore
            with _ ->
                ()

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
                            pending
                            |> Seq.filter (fun candidate ->
                                not (isNull (box candidate))
                                && candidate.Delivery = DeliveryMode.Queue
                                && (candidate.Payload :? UserMessagePayload))
                            |> Seq.sortBy (fun candidate -> candidate.Position)
                            |> Seq.tryHead
                            |> Option.defaultValue appended

                        startSuspendable first 1 (HashSet<string>())
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
                | SuspendableFinished(entry, completion, attempt, allowed) ->
                    match state, suspended with
                    | SessionState.Running, None ->
                        match completion.Suspension with
                        | None ->
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

                            notifySettled completion.Result

                            let pending =
                                awaitTask (
                                    props.Store.ReadPendingInbox(props.Tenant, props.SessionId, CancellationToken.None)
                                )

                            let next =
                                pending
                                |> Seq.filter (fun candidate ->
                                    not (isNull (box candidate))
                                    && candidate.Delivery = DeliveryMode.Queue
                                    && (candidate.Payload :? UserMessagePayload))
                                |> Seq.sortBy (fun candidate -> candidate.Position)
                                |> Seq.tryHead

                            match next with
                            | Some following ->
                                startSuspendable following 1 (HashSet<string>())
                                return! loop SessionState.Running None resolved
                            | None ->
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
                        | Some cursor ->
                            awaitTask (
                                props.Store.UpdateSessionState(
                                    props.Tenant,
                                    props.SessionId,
                                    SessionState.WaitingForInput,
                                    CancellationToken.None
                                )
                            )
                            |> ignore

                            journalSuspend cursor

                            let timeoutCts = new CancellationTokenSource()

                            let carried = if isNull (box allowed) then HashSet<string>() else allowed

                            let parked =
                                {
                                    Entry = entry
                                    Cursor = Some cursor
                                    Rebuilt = None
                                    Allowed = carried
                                    Attempt = attempt
                                    TimeoutCts = timeoutCts
                                }

                            armTimeout cursor.RequestId timeoutCts
                            return! loop SessionState.WaitingForInput (Some parked) resolved
                    | SessionState.WaitingForInput, Some parked when parked.Cursor.IsNone && parked.Rebuilt.IsSome ->
                        // Crash-retry path should never produce a running
                        // finish while still parked; ignore stale completions.
                        return! loop state suspended resolved
                    | _ -> return! loop state suspended resolved
                | SuspendableFaulted(entry, _, _) ->
                    match state, suspended with
                    | SessionState.Running, None ->
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

                                    journalResolve reply
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
                                    // before resuming so the continued run skips
                                    // Evaluate for it.
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

    /// Reads a suspendable actor's snapshot: its lifecycle state, the
    /// store's pending inbox count, and the pending suspend request id.
    /// <param name="session">The suspendable session actor.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The actor's current snapshot.</returns>
    let getSuspendSnapshotAsync (session: IActorRef) (cancellationToken: CancellationToken) : Task<SessionSnapshot> =
        ArgumentNullException.ThrowIfNull(session)
        askSuspendableAsync<SessionSnapshot> session SuspendableGetSnapshot cancellationToken
