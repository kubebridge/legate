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
// Inject/Interrupt delivery (#34), abort stop-cause arbitration (#35: Close
// wires turn cancellation only), and suspend triggers with reply matching
// (#36: WaitingForInput is modelled and preserved, never entered).

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

    /// Builds the default turn runner over TurnLoop.runAsync for Queue
    /// delivery: one ChatRole.User history message from the entry's parts,
    /// no Inject fold, and an always-live lease hook (claim tokens and
    /// fencing belong to #33).
    /// <param name="client">The chat client the turn runs against.</param>
    /// <param name="tools">The tools the turn may call.</param>
    /// <param name="options">The turn loop tuning and per-turn budget.</param>
    /// <returns>A runner executing one Queue inbox entry per turn.</returns>
    let createTurnRunner
        (client: IChatClient)
        (tools: IReadOnlyDictionary<string, AITool>)
        (options: TurnLoop.TurnLoopOptions)
        : (InboxEntry -> CancellationToken -> Task<TurnResult>) =
        ArgumentNullException.ThrowIfNull(client)
        ArgumentNullException.ThrowIfNull(tools)

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

                return! TurnLoop.runAsync client history tools options cancellationToken (fun () -> true)
            }

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

        let rec loop (state: SessionState) (running: RunningTurn option) =
            actor {
                let! message = mailbox.Receive()

                match message with
                | QueuePrompt(payload, cancellationToken) ->
                    match state with
                    | SessionState.Closed ->
                        mailbox.Sender() <! PromptRejected SessionState.Closed
                        return! loop state running
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
                        return! loop SessionState.Running (Some next)
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
                        return! loop state running
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
                        return! loop state running
                | CloseSession cancellationToken ->
                    match running with
                    | Some inFlight -> inFlight.Cts.Cancel()
                    | None -> ()

                    let closed =
                        awaitTask (props.Store.CloseSession(props.Tenant, props.SessionId, cancellationToken))

                    mailbox.Sender() <! closed
                    return! loop SessionState.Closed None
                | GetSnapshot ->
                    let snapshot =
                        {
                            SessionId = props.SessionId
                            State = state
                            PendingCount = pendingCount props
                            RunningPosition = running |> Option.map (fun inFlight -> inFlight.Entry.Position)
                        }

                    mailbox.Sender() <! snapshot
                    return! loop state running
                | SessionTurnSettled(entry, _) ->
                    match state, running with
                    | SessionState.Running, Some inFlight when inFlight.Entry.Position = entry.Position ->
                        inFlight.Cts.Dispose()
                        let nextState, nextRunning = settle entry CancellationToken.None
                        return! loop nextState nextRunning
                    | _ -> return! loop state running
                | SessionTurnFaulted(entry, _) ->
                    match state, running with
                    | SessionState.Running, Some inFlight when inFlight.Entry.Position = entry.Position ->
                        inFlight.Cts.Dispose()
                        let nextState, nextRunning = settle entry CancellationToken.None
                        return! loop nextState nextRunning
                    | _ -> return! loop state running
            }

        loop initialState None

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
