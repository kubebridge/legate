// SPDX-License-Identifier: Apache-2.0
namespace Legate.Testing

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open Akka.Actor
open Akka.FSharp
open Legate
open Legate.Storage.InMemory
open Microsoft.Extensions.AI

// Session harness: opens a session on in-memory stores, prompts it through
// the session actor, collects the journaled events, and answers permission
// requests programmatically. The harness drives the real suspendable actor
// (behaviorWithSuspend) with a TurnLoop-backed runner over
// runSuspendableAsync plus the resume continuations: no new runtime code,
// only test wiring. Deadlines never fire on their own: both delay seams
// default to a recording wait on the harness clock, so a test advances time
// explicitly when it means to. Every wait is event-driven (settle probe,
// journal observer) with a ten-second bound, never a sleep.

/// How a session harness opens its session: the tenant and agent ids, the
/// session title, the turn budget, the suspension wait, the permission
/// policy, the ask-user headless policy, and the two delay seams.
/// Construct and set properties; every knob carries a default, so
/// `SessionHarnessOptions()` alone opens a plain prompted session.
type SessionHarnessOptions() =

    /// The tenant the session belongs to. Defaults to "harness".
    member val Tenant: TenantId = TenantId.Create "harness" with get, set

    /// The agent the session converses with. Defaults to a fresh id per options instance.
    member val AgentId: AgentId = AgentId.New() with get, set

    /// The session title. Must not be null or whitespace. Defaults to "harness".
    member val Title: string = "harness" with get, set

    /// Maximum model iterations per turn. Must be at least 1. Defaults to
    /// the configured Turns default.
    member val MaxIterations: int = TurnsOptions().DefaultMaxIterations with get, set

    /// Maximum wall-clock time per turn. Must be positive. Defaults to the
    /// configured Turns default.
    member val TurnTimeout: TimeSpan = TurnsOptions().DefaultTimeout with get, set

    /// How long a suspension waits for its Reply before settling Failed.
    /// Must be positive. Defaults to five minutes.
    member val AskTimeout: TimeSpan = TimeSpan.FromMinutes 5.0 with get, set

    /// The permission policy the turn evaluates per tool call, or null for
    /// no gate (every call executes). Defaults to null.
    member val Policy: IPermissionPolicy | null = null with get, set

    /// The ask_user headless policy the turn answers questions with, or
    /// null to suspend for a host answer (interactive). Defaults to null.
    member val AskUser: AskUserOptions | null = null with get, set

    /// The delay seam the turn's hard deadline fires off, or null for the
    /// harness default: a recording wait on the harness clock that fires
    /// only when the test advances it. Defaults to null.
    member val LoopDelay: ILlmDelay | null = null with get, set

    /// The delay seam the AskTimeout deadline fires off, or null for the
    /// harness default: a recording wait on the harness clock that fires
    /// only when the test advances it. Defaults to null.
    member val AskDelay: ILlmDelay | null = null with get, set

/// What a prompted turn settled with: the ordered journaled events plus
/// the turn result.
[<Sealed>]
type CollectedTurn(events: IReadOnlyList<SessionEvent>, result: TurnResult) =

    do
        ArgumentNullException.ThrowIfNull(events)

        if isNull (box result) then
            raise (ArgumentNullException(nameof result))

    /// The journaled events in sequence order.
    member _.Events: IReadOnlyList<SessionEvent> = events

    /// The settled turn result.
    member _.Result: TurnResult = result

/// A recording wait on the harness clock: records every requested delay on
/// the shared RecordingDelay, then waits on the FakeClock instead.
/// RecordingDelay's own wait completes immediately by design (it only
/// records), so only the clock wait can fire the deadline: deadlines stay
/// pending until the test advances the clock.
type internal ClockDelay(clock: FakeClock, recording: RecordingDelay) =

    do
        ArgumentNullException.ThrowIfNull(clock)
        ArgumentNullException.ThrowIfNull(recording)

    interface ILlmDelay with
        member _.Delay(span: TimeSpan, cancellationToken: CancellationToken) =
            (recording :> ILlmDelay).Delay(span, CancellationToken.None) |> ignore
            Task.Delay(span, clock, cancellationToken)

/// An event-store decorator reporting every journaled event to an
/// observer. The callback is guarded: a throwing observer never breaks the
/// append.
type internal ObservingEventStore(inner: ISessionEventStore, onAppended: SessionEvent -> unit) =

    do
        ArgumentNullException.ThrowIfNull(inner)

        if isNull (box onAppended) then
            raise (ArgumentNullException(nameof onAppended))

    interface ISessionEventStore with
        member _.Append(tenant, sessionId, claimToken, events, cancellationToken) =
            task {
                let! outcome = inner.Append(tenant, sessionId, claimToken, events, cancellationToken)

                match outcome with
                | :? EventAppended as appended when not (isNull (box appended)) ->
                    if not (isNull (box appended.Events)) then
                        for event in appended.Events do
                            if not (isNull (box event)) then
                                try
                                    onAppended event
                                with _ ->
                                    ()
                | _ -> ()

                return outcome
            }

        member _.Replay(tenant, sessionId, fromSequence, limit, cancellationToken) =
            inner.Replay(tenant, sessionId, fromSequence, limit, cancellationToken)

        member _.TryClaimCleanup(tenant, sessionId, owner, leaseDuration, cancellationToken) =
            inner.TryClaimCleanup(tenant, sessionId, owner, leaseDuration, cancellationToken)

        member _.CompleteCleanup(tenant, sessionId, claimToken, cancellationToken) =
            inner.CompleteCleanup(tenant, sessionId, claimToken, cancellationToken)

        member _.DeferCleanup(tenant, sessionId, claimToken, cancellationToken) =
            inner.DeferCleanup(tenant, sessionId, claimToken, cancellationToken)

/// The per-harness wait machinery: settled-result recording with queued
/// per-turn waiters, plus suspension signalling from the journal observer.
/// Waiters queue per prompt or reply, so sequential turns never steal each
/// other's signal; a settle with no waiter still records.
type internal HarnessSignals() =

    let settledGate = obj ()
    let settled = ResizeArray<TurnResult>()
    let settleWaiters = Queue<TaskCompletionSource<TurnResult>>()

    let suspendGate = obj ()
    let mutable suspendWaiter: TaskCompletionSource<string> option = None
    let mutable stickySuspend: string option = None

    /// Records a settled result and wakes its waiter when one waits.
    member _.ObserveSettled(result: TurnResult) =
        if not (isNull (box result)) then
            lock settledGate (fun () ->
                settled.Add(result)

                if settleWaiters.Count > 0 then
                    settleWaiters.Dequeue().TrySetResult(result) |> ignore)

    /// Queues a waiter for the next settle.
    member _.EnqueueSettle() : TaskCompletionSource<TurnResult> =
        lock settledGate (fun () ->
            let waiter = TaskCompletionSource<TurnResult>()
            settleWaiters.Enqueue(waiter)
            waiter)

    /// Every settled result, in settle order.
    member _.Settled: IReadOnlyList<TurnResult> =
        lock settledGate (fun () -> ResizeArray<TurnResult>(settled) :> IReadOnlyList<TurnResult>)

    /// Wakes the suspension waiter, or parks the request id for a waiter
    /// that attaches after the journal write.
    member _.SignalSuspend(requestId: string | null) =
        match requestId with
        | null -> ()
        | value ->
            lock suspendGate (fun () ->
                match suspendWaiter with
                | Some waiter ->
                    suspendWaiter <- None
                    waiter.TrySetResult(value) |> ignore
                | None -> stickySuspend <- Some value)

    /// Observes one journaled event, signalling on suspend events.
    member this.ObserveJournal(event: SessionEvent) =
        if not (isNull (box event)) then
            match event with
            | :? PermissionRequestedEvent as asked when not (isNull (box asked)) -> this.SignalSuspend asked.RequestId
            | :? QuestionAskedEvent as asked when not (isNull (box asked)) -> this.SignalSuspend asked.QuestionId
            | _ -> ()

    /// Takes a parked suspension id, if the journal wrote one before the
    /// waiter attached.
    member _.TakeSticky() : string option =
        lock suspendGate (fun () ->
            let sticky = stickySuspend
            stickySuspend <- None
            sticky)

    /// Parks a waiter for the next suspension signal.
    member _.WaitSuspend() : TaskCompletionSource<string> =
        lock suspendGate (fun () ->
            let waiter = TaskCompletionSource<string>()
            suspendWaiter <- Some waiter
            waiter)

/// A scripted session under test: an in-memory store pair, a local actor
/// system, and one suspendable session actor driven by the scripted client
/// and the static tool source through runSuspendableAsync plus the resume
/// continuations. Open through CreateAsync; dispose when the test ends.
/// Turn settlement and suspension signals are event-driven with a
/// ten-second bound, never sleeps: a bound that lapses raises
/// TimeoutException naming the awaited signal.
[<Sealed>]
type SessionHarness
    private
    (
        system: ActorSystem,
        store: ISessionStore,
        journal: ISessionEventStore,
        tenant: TenantId,
        sessionId: SessionId,
        client: ScriptedChatClient,
        tools: StaticToolSource,
        clock: FakeClock,
        delays: RecordingDelay,
        actor: IActorRef,
        signals: HarnessSignals
    ) =

    /// How long a wait for a settle or suspension signal lasts before the
    /// harness fails the wait instead of hanging the test host.
    static let waitBound = TimeSpan.FromSeconds 10.0

    /// Builds the user history for a fresh run from the entry's parts,
    /// mirroring the actor's Queue runner shape.
    static let historyOf (entry: InboxEntry) : IList<ChatMessage> =
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

        history

    /// Validates harness options synchronously: fail fast before the task.
    static let validateOptions (options: SessionHarnessOptions) : unit =
        if String.IsNullOrWhiteSpace options.Title then
            raise (ArgumentException("The harness session needs a non-empty title.", nameof options))

        if options.MaxIterations < 1 then
            raise (ArgumentOutOfRangeException(nameof options, "MaxIterations must be at least 1."))

        if options.TurnTimeout <= TimeSpan.Zero then
            raise (ArgumentOutOfRangeException(nameof options, "TurnTimeout must be positive."))

        if options.AskTimeout <= TimeSpan.Zero then
            raise (ArgumentOutOfRangeException(nameof options, "AskTimeout must be positive."))

        match Option.ofObj (options.AskUser) with
        | None -> ()
        | Some ask ->
            match Option.ofObj (ask.Validate()) with
            | None -> ()
            | Some violation ->
                raise (ArgumentException($"The harness ask-user policy is invalid: {violation}", nameof options))

    /// Extracts the journal claim token from a granted claim.
    static let claimTokenOf (claim: TurnLeaseState) : string =
        match claim with
        | :? TurnLeaseHeld as held when not (isNull (box held)) -> held.Claim.Token
        | :? TurnLeaseRenewed as renewed when not (isNull (box renewed)) -> renewed.Claim.Token
        | :? TurnLeaseExpiring as expiring when not (isNull (box expiring)) -> expiring.Claim.Token
        | _ -> raise (InvalidOperationException("The harness could not claim the session turn."))

    /// Opens a scripted session: in-memory stores on the harness clock, a
    /// local actor system, the session row, the journal claim, and the
    /// suspendable actor wired to the scripted client and static tools.
    /// <param name="client">The scripted chat client the turns run against. Must not be null.</param>
    /// <param name="tools">The static tool source the turns resolve. Must not be null.</param>
    /// <param name="options">The harness options, or null for defaults.</param>
    /// <param name="cancellationToken">Abandons the open.</param>
    /// <returns>The open harness.</returns>
    static member CreateAsync
        (
            client: ScriptedChatClient,
            tools: StaticToolSource,
            options: SessionHarnessOptions | null,
            cancellationToken: CancellationToken
        ) : Task<SessionHarness> =
        if isNull (box client) then
            raise (ArgumentNullException(nameof client))

        if isNull (box tools) then
            raise (ArgumentNullException(nameof tools))

        let resolved: SessionHarnessOptions =
            match options with
            | null -> SessionHarnessOptions()
            | present -> present

        validateOptions resolved

        task {
            let signals = HarnessSignals()
            let clock = FakeClock()
            let recording = RecordingDelay()

            let loopDelay: ILlmDelay =
                match resolved.LoopDelay with
                | null -> ClockDelay(clock, recording) :> ILlmDelay
                | seam -> seam

            let askDelay: ILlmDelay =
                match resolved.AskDelay with
                | null -> ClockDelay(clock, recording) :> ILlmDelay
                | seam -> seam

            let loopOptions =
                { TurnLoop.TurnLoopOptions.Default with
                    MaxIterations = resolved.MaxIterations
                    Timeout = resolved.TurnTimeout
                    AskUser = Option.ofObj resolved.AskUser
                }

            // The loop reads a null policy as no gate; defaultof carries
            // that null under the non-null reference type.
            let policy: IPermissionPolicy =
                match resolved.Policy with
                | null -> Unchecked.defaultof<IPermissionPolicy>
                | present -> present

            let database = InMemoryDatabase(clock :> TimeProvider)
            let store = InMemorySessionStore(database) :> ISessionStore

            let journal =
                ObservingEventStore(InMemorySessionEventStore(database) :> ISessionEventStore, signals.ObserveJournal)
                :> ISessionEventStore

            let system = LocalActorSystem.createSystem ()

            try
                let session =
                    {
                        Id = SessionId.New()
                        Tenant = resolved.Tenant
                        AgentId = resolved.AgentId
                        Title = resolved.Title
                        State = SessionState.Idle
                        CurrentTurnId = Unchecked.defaultof<Nullable<TurnId>>
                        CreatedAt = clock.Instant
                        UpdatedAt = clock.Instant
                        ClosedAt = Unchecked.defaultof<Nullable<DateTimeOffset>>
                        WorkspaceBinding = null
                        Options = SessionOptions()
                        PermissionGrants = ResizeArray<string>() :> IReadOnlyList<string>
                    }

                let! created = store.CreateSession(resolved.Tenant, session, cancellationToken)

                // The journal fence needs a live claim, and claiming
                // consumes the head inbox entry: prime it with a bootstrap
                // message the actor never runs, so real prompts stay
                // pending for the turn. The frozen harness clock holds the
                // lease until the test advances past it.
                let bootstrap =
                    UserMessagePayload(UserMessage.Text "harness bootstrap") :> InboxPayload

                let! _ =
                    store.AppendInboxMessage(
                        resolved.Tenant,
                        created.Id,
                        bootstrap,
                        DeliveryMode.Queue,
                        cancellationToken
                    )

                let! claimed =
                    store.ClaimNextTurn(
                        resolved.Tenant,
                        created.Id,
                        "harness",
                        TimeSpan.FromHours 1.0,
                        cancellationToken
                    )

                let token = claimTokenOf claimed
                let toolsDict = tools.AsDictionary()

                let noDrain () : IReadOnlyList<InboxEntry> =
                    ResizeArray<InboxEntry>() :> IReadOnlyList<InboxEntry>

                let runner: SessionActor.SuspendableRunner =
                    fun entry _attempt allowed cursor reply runnerToken ->
                        task {
                            match cursor, reply with
                            | None, None ->
                                let history = historyOf entry

                                return!
                                    TurnLoop.runSuspendableAsync
                                        (client :> IChatClient)
                                        history
                                        toolsDict
                                        loopOptions
                                        loopDelay
                                        runnerToken
                                        (fun () -> true)
                                        noDrain
                                        ignore
                                        ignore
                                        policy
                                        created.Id
                                        (TurnId.New())
                                        None
                                        allowed
                            | Some live, Some(:? PermissionDecision as decision) ->
                                return!
                                    TurnLoop.resumePermissionAsync
                                        live
                                        decision.Decision
                                        (client :> IChatClient)
                                        live.HistorySnapshot
                                        toolsDict
                                        loopOptions
                                        loopDelay
                                        runnerToken
                                        (fun () -> true)
                                        policy
                                        allowed
                            | Some live, Some(:? QuestionAnswer as answer) ->
                                return!
                                    TurnLoop.resumeQuestionAsync
                                        live
                                        answer.Answer
                                        (client :> IChatClient)
                                        live.HistorySnapshot
                                        toolsDict
                                        loopOptions
                                        loopDelay
                                        runnerToken
                                        (fun () -> true)
                                        policy
                                        allowed
                            | None, Some _ ->
                                // Crash-rebuild shape: no live cursor, so
                                // retry the turn from its inbox entry.
                                let history = historyOf entry

                                return!
                                    TurnLoop.runSuspendableAsync
                                        (client :> IChatClient)
                                        history
                                        toolsDict
                                        loopOptions
                                        loopDelay
                                        runnerToken
                                        (fun () -> true)
                                        noDrain
                                        ignore
                                        ignore
                                        policy
                                        created.Id
                                        (TurnId.New())
                                        None
                                        allowed
                            | _ ->
                                return
                                    raise (
                                        InvalidOperationException(
                                            "The suspendable runner received a cursor without a matching reply."
                                        )
                                    )
                        }

                let unusedResult =
                    {
                        AssistantText = "unused"
                        Status = TurnStatus.Completed
                        Iterations = 0
                        Usage = { InputTokens = 0L; OutputTokens = 0L }
                        Outcome = null
                    }

                let baseProps: SessionActorProps =
                    {
                        Store = store
                        Tenant = resolved.Tenant
                        SessionId = created.Id
                        RunTurn = (fun _ _ -> Task.FromResult(unusedResult))
                        // The harness signals own the test wait; the shared
                        // PromptWaitHubs fan-out lets PromptAndWait-style
                        // clients (issue 85) wait on the same settle through
                        // their FIFO waiter queue, mirroring the production
                        // spawn sites. Hubs are keyed by the globally unique
                        // session id, so harnesses never share one.
                        OnTurnSettled =
                            Some(fun result ->
                                signals.ObserveSettled result
                                PromptWaitHubs.ObserveSettled created.Id result)
                        OnInjectJournaled = None
                        Logger = null
                        Compact = None
                    }

                let deps: SessionActor.SuspendDeps =
                    {
                        EventStore = journal
                        Delay = askDelay
                        AskTimeout = resolved.AskTimeout
                        JournalToken = token
                        RunSuspendable = runner
                    }

                let actor =
                    spawn system $"harness-{Guid.NewGuid():N}" (SessionActor.behaviorWithSuspend baseProps deps)

                return
                    new SessionHarness(
                        system,
                        store,
                        journal,
                        resolved.Tenant,
                        created.Id,
                        client,
                        tools,
                        clock,
                        recording,
                        actor,
                        signals
                    )
            with ex ->
                try
                    system.Terminate() |> ignore
                with _ ->
                    ()

                return! Task.FromException<SessionHarness>(ex)
        }

    /// Opens a scripted session with default options.
    /// <param name="client">The scripted chat client the turns run against. Must not be null.</param>
    /// <param name="tools">The static tool source the turns resolve. Must not be null.</param>
    /// <returns>The open harness.</returns>
    static member CreateAsync(client: ScriptedChatClient, tools: StaticToolSource) : Task<SessionHarness> =
        SessionHarness.CreateAsync(client, tools, null, CancellationToken.None)

    /// Opens a scripted session with the given options.
    /// <param name="client">The scripted chat client the turns run against. Must not be null.</param>
    /// <param name="tools">The static tool source the turns resolve. Must not be null.</param>
    /// <param name="options">The harness options, or null for defaults.</param>
    /// <returns>The open harness.</returns>
    static member CreateAsync
        (client: ScriptedChatClient, tools: StaticToolSource, options: SessionHarnessOptions | null)
        : Task<SessionHarness> =
        SessionHarness.CreateAsync(client, tools, options, CancellationToken.None)

    /// The session the harness owns.
    member _.SessionId: SessionId = sessionId

    /// The tenant the session belongs to.
    member _.Tenant: TenantId = tenant

    /// The manually advanced clock the stores and the default delay seams
    /// read. The test advances it when it means a deadline to fire.
    member _.Clock: FakeClock = clock

    /// The shared recording delay: every default-seam delay request lands
    /// here, in request order. Tests assert arming without sleeping.
    member _.Delays: RecordingDelay = delays

    /// The scripted chat client the turns run against.
    member _.Client: ScriptedChatClient = client

    /// The static tool source the turns resolve.
    member _.Tools: StaticToolSource = tools

    /// Every settled turn result, in settle order.
    member _.SettledResults: IReadOnlyList<TurnResult> = signals.Settled

    /// The session actor PromptAndWait-style clients resolve. Internal so
    /// no Akka type crosses the public API.
    member internal _.Actor: IActorRef = actor

    /// The durable store the session persists through.
    member internal _.Store: ISessionStore = store

    /// The journal suspend and resolve events append to.
    member internal _.Journal: ISessionEventStore = journal

    /// Prompts the session with one text message.
    /// <param name="text">The user text. Must be a non-empty string.</param>
    /// <param name="cancellationToken">Cancels the prompt.</param>
    /// <returns>The appended inbox entry.</returns>
    member _.PromptAsync(text: string, cancellationToken: CancellationToken) : Task<InboxEntry> =
        SessionActor.promptSuspendableAsync store tenant sessionId actor (UserMessage.Text text) cancellationToken

    /// Prompts the session and waits for the turn to settle. A suspended
    /// turn never settles: wait for the suspension and reply instead.
    /// <param name="text">The user text. Must be a non-empty string.</param>
    /// <param name="cancellationToken">Cancels the wait, not the turn.</param>
    /// <returns>The settled turn result.</returns>
    member this.PromptAndSettleAsync(text: string, cancellationToken: CancellationToken) : Task<TurnResult> =
        task {
            let waiter = signals.EnqueueSettle()
            let! _ = this.PromptAsync(text, cancellationToken)

            let mutable outcome: TurnResult option = None

            try
                let! settled = waiter.Task.WaitAsync(waitBound, cancellationToken)
                outcome <- Some settled
            with :? TimeoutException ->
                ()

            match outcome with
            | Some result ->
                // Settle barrier: the actor observes the settle before it
                // writes Idle, so round-trip the mailbox first. The snapshot
                // only answers after the finish handling (Idle write
                // included) completed, making a following store read exact
                // without sleeping.
                let! _ = SessionActor.getSuspendSnapshotAsync actor cancellationToken
                return result
            | None ->
                return
                    raise (
                        TimeoutException(
                            "The harness timed out waiting for the turn to settle after the prompt; a suspended turn never settles."
                        )
                    )
        }

    /// Prompts the session, waits for the settle, then collects the ordered
    /// journaled events with the turn result.
    /// <param name="text">The user text. Must be a non-empty string.</param>
    /// <param name="cancellationToken">Cancels the wait, not the turn.</param>
    /// <returns>The ordered events with the settled turn result.</returns>
    member this.PromptAndCollectAsync(text: string, cancellationToken: CancellationToken) : Task<CollectedTurn> =
        task {
            let! result = this.PromptAndSettleAsync(text, cancellationToken)
            let! events = this.CollectEventsAsync(cancellationToken)
            return CollectedTurn(events, result)
        }

    /// Waits for the running turn to suspend and returns the pending
    /// request id: the id a PermissionDecision (or a QuestionAnswer with
    /// the question id) carries back.
    /// <param name="cancellationToken">Cancels the wait, not the turn.</param>
    /// <returns>The pending suspend request id.</returns>
    member _.WaitForSuspensionAsync(cancellationToken: CancellationToken) : Task<string> =
        task {
            let! snapshot = SessionActor.getSuspendSnapshotAsync actor cancellationToken

            match snapshot.PendingRequestId with
            | null ->
                match signals.TakeSticky() with
                | Some requestId -> return requestId
                | None ->
                    let waiter = signals.WaitSuspend()
                    let mutable outcome: string option = None

                    try
                        let! signaled = waiter.Task.WaitAsync(waitBound, cancellationToken)
                        outcome <- Some signaled
                    with :? TimeoutException ->
                        ()

                    match outcome with
                    | None -> return raise (TimeoutException("The harness timed out waiting for the turn to suspend."))
                    | Some _ ->
                        let! parked = SessionActor.getSuspendSnapshotAsync actor cancellationToken

                        match parked.PendingRequestId with
                        | null ->
                            return
                                raise (
                                    InvalidOperationException(
                                        "The suspension resolved before the harness observed its request id."
                                    )
                                )
                        | requestId -> return requestId
            | requestId -> return requestId
        }

    /// Replies to the suspended turn: a PermissionDecision answering the
    /// pending permission request, or a QuestionAnswer answering the
    /// pending question. Never starts a turn.
    /// <param name="reply">The host reply. Must not be null.</param>
    /// <param name="cancellationToken">Cancels the reply.</param>
    /// <returns>The consumed Reply inbox entry.</returns>
    member _.ReplyAsync(reply: Reply, cancellationToken: CancellationToken) : Task<InboxEntry> =
        SessionActor.replyAsync store tenant sessionId actor reply cancellationToken

    /// Closes the harness session: the lifecycle boundary. Valid in every
    /// state and idempotent; a turn running while the session closes keeps
    /// its detached task under the claim fence.
    /// <param name="cancellationToken">Cancels the close.</param>
    /// <returns>The closed session.</returns>
    member _.CloseAsync(cancellationToken: CancellationToken) : Task<Session> =
        SessionActor.closeSuspendableAsync store tenant sessionId actor cancellationToken

    /// Replies to the suspended turn and waits for the resumed turn to
    /// settle.
    /// <param name="reply">The host reply. Must not be null.</param>
    /// <param name="cancellationToken">Cancels the wait, not the turn.</param>
    /// <returns>The settled turn result.</returns>
    member this.ReplyAndSettleAsync(reply: Reply, cancellationToken: CancellationToken) : Task<TurnResult> =
        task {
            let waiter = signals.EnqueueSettle()
            let! _ = this.ReplyAsync(reply, cancellationToken)

            let mutable outcome: TurnResult option = None

            try
                let! settled = waiter.Task.WaitAsync(waitBound, cancellationToken)
                outcome <- Some settled
            with :? TimeoutException ->
                ()

            match outcome with
            | Some result ->
                // Settle barrier, as in PromptAndSettleAsync: the snapshot
                // round-trip orders the following store read after the
                // actor's Idle write.
                let! _ = SessionActor.getSuspendSnapshotAsync actor cancellationToken
                return result
            | None -> return raise (TimeoutException("The harness timed out waiting for the resumed turn to settle."))
        }

    /// Collects the session journal in sequence order, paging from the
    /// first event.
    /// <param name="cancellationToken">Abandons the replay.</param>
    /// <returns>The journaled events in sequence order.</returns>
    member _.CollectEventsAsync(cancellationToken: CancellationToken) : Task<IReadOnlyList<SessionEvent>> =
        task {
            let collected = ResizeArray<SessionEvent>()
            let mutable cursor = 0L
            let mutable go = true
            let mutable failure: string option = None

            while go do
                let! outcome = journal.Replay(tenant, sessionId, cursor, 100, cancellationToken)

                match outcome with
                | :? EventReplayPage as page when not (isNull (box page)) ->
                    if not (isNull (box page.Events)) then
                        for event in page.Events do
                            if not (isNull (box event)) then
                                collected.Add(event)

                    if page.NextCursor.HasValue then
                        cursor <- page.NextCursor.Value
                    else
                        go <- false
                | :? EventReplayEndOfStream -> go <- false
                | _ ->
                    failure <-
                        Some "The harness journal replay left the readable journal: unknown session or expired journal."

                    go <- false

            match failure with
            | Some message -> return raise (InvalidOperationException(message))
            | None -> return collected :> IReadOnlyList<SessionEvent>
        }

    /// Reads the stored session row.
    /// <param name="cancellationToken">Abandons the read.</param>
    /// <returns>The stored session.</returns>
    member _.GetSessionAsync(cancellationToken: CancellationToken) : Task<Session> =
        task {
            let! found = store.GetSession(tenant, sessionId, cancellationToken)

            match found with
            | null -> return raise (InvalidOperationException("The harness session row is missing."))
            | session -> return session
        }

    interface IDisposable with
        member _.Dispose() =
            system.Terminate() |> ignore
            system.WhenTerminated.Wait(TimeSpan.FromSeconds 10.0) |> ignore
