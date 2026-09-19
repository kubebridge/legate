// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Runtime.CompilerServices
open System.Threading
open System.Threading.Tasks
open Akka.Actor

// Session client: the public receiver PromptAndWaitAsync extends. The
// client holds the suspendable prompt path (store, tenant, actor
// resolution), the event bus the suspension signal subscribes to, the
// agent store SetAgent validates against, and the wait-bound seam.
// Construction stays internal: the container facade owns it once it lands,
// while tests construct it directly. The actor resolver never crosses the
// public API (Akka types stay internal).
/// <summary>The session client PromptAndWaitAsync extends.</summary>
[<Sealed>]
type SessionClient
    internal
    (
        store: ISessionStore,
        tenant: TenantId,
        resolve: SessionId -> CancellationToken -> Task<IActorRef>,
        eventBus: SessionEventBus,
        defaultBound: TimeSpan,
        waitDelay: ILlmDelay,
        agents: IAgentStore option
    ) =

    do ArgumentNullException.ThrowIfNull(store)
    do ArgumentNullException.ThrowIfNull(eventBus)
    do ArgumentNullException.ThrowIfNull(waitDelay)

    do
        if isNull (box resolve) then
            raise (ArgumentNullException(nameof resolve))

    do
        if defaultBound <= TimeSpan.Zero then
            raise (ArgumentOutOfRangeException(nameof defaultBound, "The default wait bound must be positive."))

    let semaphores = ConcurrentDictionary<SessionId, SemaphoreSlim>()

    /// The durable store prompts, aborts, and session reads go through.
    member internal _.Store: ISessionStore = store

    /// The tenant sessions belong to.
    member internal _.Tenant: TenantId = tenant

    /// The agent store SetAgent validates the rebound agent against, or
    /// None when the host runs without a managed agent catalog: validation
    /// is skipped then and any agent id rebinds.
    member internal _.Agents: IAgentStore option = agents

    /// The journal event hub suspensions subscribe to.
    member internal _.EventBus: SessionEventBus = eventBus

    /// The wait bound when the session carries no explicit timeout.
    member internal _.DefaultBound: TimeSpan = defaultBound

    /// The seam the wait bound fires off.
    member internal _.WaitDelay: ILlmDelay = waitDelay

    /// Resolves the session actor.
    member internal _.Resolve(sessionId: SessionId, cancellationToken: CancellationToken) : Task<IActorRef> =
        resolve sessionId cancellationToken

    /// The per-session gate serialising enqueue-plus-prompt so FIFO waiter
    /// order matches Queue append order under concurrent waits.
    member internal _.SemaphoreFor(sessionId: SessionId) : SemaphoreSlim =
        semaphores.GetOrAdd(sessionId, fun _ -> new SemaphoreSlim(1, 1))

// ────────────────── PromptAndWait ──────────────────

/// <summary>Extension methods on <see cref="T:Legate.SessionClient" />.</summary>
[<Sealed; AbstractClass; Extension>]
type SessionClientExtensions =

    /// Prompts the session with one user message over Queue delivery and
    /// waits until the turn settles, returning the settled
    /// <see cref="T:Legate.TurnResult" /> carrying the structured outcome.
    /// <param name="client">The session client. Must not be null.</param>
    /// <param name="sessionId">The session to prompt.</param>
    /// <param name="message">The user message. Must not be null.</param>
    /// <param name="cancellationToken">Cancels the wait, never the turn: abandon the wait and throw OperationCanceledException with the turn left running to settle normally, unless the settle already won the race (settlement wins, mirroring StopArbitration). A cancellation before the prompt lands prevents the prompt.</param>
    /// <returns>The settled turn result.</returns>
    /// <exception cref="T:Legate.SessionNotFoundException">The session id does not exist.</exception>
    /// <exception cref="T:Legate.InvalidSessionStateException">The session is closed.</exception>
    /// <exception cref="T:Legate.PermissionApprovalRequiredException">The turn suspended on a permission request instead of settling.</exception>
    /// <exception cref="T:Legate.DeadlineExceededException">The wait bound lapsed with the turn left running.</exception>
    [<Extension>]
    static member PromptAndWaitAsync
        (client: SessionClient, sessionId: SessionId, message: UserMessage, cancellationToken: CancellationToken)
        : Task<TurnResult> =
        ArgumentNullException.ThrowIfNull(client)

        if isNull (box message) then
            raise (ArgumentNullException(nameof message))

        task {
            let store = client.Store
            let tenant = client.Tenant
            let bus = client.EventBus

            // Pre-prompt cursor: the suspension Subscribe replays from
            // here, so a suspension journaled before the Subscribe
            // attaches still surfaces (the sticky fallback is the replay).
            let! cursor = SessionClientExtensions.CursorAsync(bus, tenant, sessionId, cancellationToken)

            let semaphore = client.SemaphoreFor(sessionId)
            do! semaphore.WaitAsync(cancellationToken)

            let mutable waiterOpt: TaskCompletionSource<TurnResult> option = None
            let mutable promptError: exn option = None

            try
                let! resolved = client.Resolve(sessionId, cancellationToken)

                let hub = PromptWaitHubs.GetOrAdd(sessionId)
                waiterOpt <- Some(hub.EnqueueSettle())

                let! _ = SessionActor.promptSuspendableAsync store tenant sessionId resolved message cancellationToken

                ()
            with ex ->
                match waiterOpt with
                | Some waiter -> (PromptWaitHubs.GetOrAdd(sessionId)).Cancel(waiter)
                | None -> ()

                // Wait-abandonment (issue 85 decision): a cancellation racing
                // the prompt never aborts the turn. AbortSession would fault
                // the suspendable actor, whose mailbox has no such arm (its
                // runner is invoked with CancellationToken.None), and Abort
                // on WaitingForInput is a no-op per #35 anyway. The waiter is
                // already cancelled above; rethrow and leave any appended
                // turn running to settle normally. The Abort client verb
                // remains the explicit abort path.
                promptError <- Some ex

            semaphore.Release() |> ignore

            match promptError, waiterOpt with
            | Some error, _ -> return raise error
            | None, Some waiter ->
                // The prompt landed: housekeeping reads never observe the
                // caller token, so a cancellation surfaces in the race below
                // (abandon the wait, then throw) rather than as a raw store
                // throw.
                let! session = store.GetSession(tenant, sessionId, CancellationToken.None)

                match session with
                | null ->
                    (PromptWaitHubs.GetOrAdd(sessionId)).Cancel(waiter)
                    return raise (SessionNotFoundException(sessionId, "The session does not exist."))
                | live ->
                    let bound = SessionClientExtensions.BoundOf(live, client.DefaultBound)

                    return!
                        SessionClientExtensions.RaceAsync(client, sessionId, waiter, cursor, bound, cancellationToken)
            | None, _ -> return raise (InvalidOperationException("The prompt completed without a settle waiter."))
        }

    /// Reads the pre-prompt journal cursor: the greatest stamped sequence,
    /// or 0 when the journal holds nothing yet.
    static member private CursorAsync
        (bus: SessionEventBus, tenant: TenantId, sessionId: SessionId, cancellationToken: CancellationToken)
        : Task<int64> =
        task {
            let mutable cursor = 0L
            let mutable go = true

            while go do
                let! page = bus.ReadEventsAsync(tenant, sessionId, cursor, 100, cancellationToken)

                if isNull (box page) || page.Count = 0 then
                    go <- false
                else
                    let mutable advanced = cursor

                    for evt in page do
                        if not (isNull (box evt)) && evt.Sequence.HasValue && evt.Sequence.Value > advanced then
                            advanced <- evt.Sequence.Value

                    if advanced = cursor then
                        go <- false
                    else
                        cursor <- advanced

            return cursor
        }

    /// Resolves the wait bound: the session timeout when set, else the
    /// configured default.
    static member private BoundOf(session: Session, defaultBound: TimeSpan) : TimeSpan =
        match box session.Options with
        | null -> defaultBound
        | _ ->
            if
                session.Options.Timeout.HasValue
                && session.Options.Timeout.Value > TimeSpan.Zero
            then
                session.Options.Timeout.Value
            else
                defaultBound

    /// Races the settle waiter against the suspension Subscribe, the caller
    /// token, and the wait bound. Settlement wins ties.
    static member private RaceAsync
        (
            client: SessionClient,
            sessionId: SessionId,
            waiter: TaskCompletionSource<TurnResult>,
            cursor: int64,
            bound: TimeSpan,
            cancellationToken: CancellationToken
        ) : Task<TurnResult> =
        task {
            let tenant = client.Tenant
            let bus = client.EventBus
            let hub = PromptWaitHubs.GetOrAdd(sessionId)

            use subscribeCts = new CancellationTokenSource()
            use boundCts = new CancellationTokenSource()
            let cancelTcs = TaskCompletionSource<bool>()

            use _registration =
                cancellationToken.Register(fun () -> cancelTcs.TrySetResult(true) |> ignore)

            if cancellationToken.IsCancellationRequested then
                cancelTcs.TrySetResult(true) |> ignore

            let settleTask = waiter.Task
            let boundTask = client.WaitDelay.Delay(bound, boundCts.Token)
            let cancelTask = cancelTcs.Task

            let suspendTask =
                SessionClientExtensions.SuspendAsync(bus, tenant, sessionId, cursor, subscribeCts)

            let mutable suspendActive = true
            let mutable outcome: TurnResult option = None
            let mutable failure: exn option = None

            while outcome.IsNone && failure.IsNone do
                let candidates = ResizeArray<Task>()
                candidates.Add(settleTask)
                candidates.Add(boundTask)
                candidates.Add(cancelTask)

                if suspendActive then
                    candidates.Add(suspendTask)

                let! winner = Task.WhenAny(candidates)

                // Settlement wins every tie, mirroring StopArbitration.
                if settleTask.IsCompletedSuccessfully then
                    try
                        subscribeCts.Cancel()
                    with _ ->
                        ()

                    try
                        boundCts.Cancel()
                    with _ ->
                        ()

                    outcome <- Some settleTask.Result
                elif suspendActive && Object.ReferenceEquals(winner, suspendTask) then
                    if suspendTask.IsCompletedSuccessfully then
                        match suspendTask.Result with
                        | None ->
                            // The stream ended with no suspension: drop the
                            // suspension side and keep racing the rest.
                            suspendActive <- false
                        | Some asked ->
                            hub.Cancel(waiter)

                            try
                                subscribeCts.Cancel()
                            with _ ->
                                ()

                            try
                                boundCts.Cancel()
                            with _ ->
                                ()

                            failure <-
                                Some(
                                    PermissionApprovalRequiredException(
                                        asked.SessionId,
                                        asked.TurnId,
                                        asked.RequestId,
                                        asked.ToolName,
                                        sprintf
                                            "The turn in session %O needs approval for tool '%s' (request %s)."
                                            asked.SessionId
                                            asked.ToolName
                                            asked.RequestId
                                    )
                                    :> exn
                                )
                    elif suspendTask.IsFaulted then
                        let inner =
                            match suspendTask.Exception with
                            | null -> Exception("The suspension wait failed.")
                            | aggregate when aggregate.InnerExceptions.Count > 0 -> aggregate.InnerExceptions[0]
                            | aggregate -> aggregate :> exn

                        match inner with
                        | :? OperationCanceledException ->
                            // The Subscribe tore down with our own cancel:
                            // drop the suspension side and keep racing.
                            suspendActive <- false
                        | _ ->
                            hub.Cancel(waiter)

                            try
                                subscribeCts.Cancel()
                            with _ ->
                                ()

                            try
                                boundCts.Cancel()
                            with _ ->
                                ()

                            failure <- Some inner
                    elif suspendTask.IsCanceled then
                        suspendActive <- false
                    else
                        ()
                elif Object.ReferenceEquals(winner, boundTask) then
                    if boundTask.IsCompletedSuccessfully then
                        hub.Cancel(waiter)

                        try
                            subscribeCts.Cancel()
                        with _ ->
                            ()

                        failure <-
                            Some(
                                DeadlineExceededException(
                                    "PromptAndWait",
                                    "The PromptAndWait wait exceeded its bound while the turn kept running."
                                )
                                :> exn
                            )
                    elif boundTask.IsFaulted then
                        // The seam faulted: surface it rather than hanging.
                        hub.Cancel(waiter)

                        try
                            subscribeCts.Cancel()
                        with _ ->
                            ()

                        let inner =
                            match boundTask.Exception with
                            | null -> Exception("The wait-bound delay failed.")
                            | aggregate when aggregate.InnerExceptions.Count > 0 -> aggregate.InnerExceptions[0]
                            | aggregate -> aggregate :> exn

                        failure <- Some inner
                    else if
                        // Bound wait cancelled alongside our own teardown;
                        // keep racing unless everything else resolved.
                        settleTask.IsCompleted || (not suspendActive && cancelTask.IsCompleted)
                    then
                        ()
                elif Object.ReferenceEquals(winner, cancelTask) then
                    if settleTask.IsCompletedSuccessfully then
                        try
                            subscribeCts.Cancel()
                        with _ ->
                            ()

                        try
                            boundCts.Cancel()
                        with _ ->
                            ()

                        outcome <- Some settleTask.Result
                    else
                        // Wait-abandonment (issue 85 decision): never abort
                        // the turn from here; AbortSession would fault the
                        // suspendable actor and Abort on WaitingForInput is
                        // a no-op per #35 anyway. Cancel the waiter so it
                        // never steals a later settle, tear down the
                        // subscription and the bound, then throw with the
                        // turn left running to settle normally.
                        hub.Cancel(waiter)

                        try
                            subscribeCts.Cancel()
                        with _ ->
                            ()

                        try
                            boundCts.Cancel()
                        with _ ->
                            ()

                        cancellationToken.ThrowIfCancellationRequested()
                        failure <- Some(OperationCanceledException(cancellationToken))
                else if
                    // A stale side completed (a dropped suspension or a torn
                    // down bound): keep racing the live sides.
                    settleTask.IsCompleted
                    || suspendActive && suspendTask.IsCompleted
                    || boundTask.IsCompleted
                    || cancelTask.IsCompleted
                then
                    ()

            match outcome, failure with
            | Some result, _ -> return result
            | None, Some error -> return raise error
            | None, None -> return raise (InvalidOperationException("The PromptAndWait race resolved with no outcome."))
        }

    /// Waits for the first permission suspension journaled after the
    /// cursor, ignoring questions and terminal events per the Decisions:
    /// questions end in the wait bound. Returns None when the stream ends
    /// with no suspension.
    static member private SuspendAsync
        (
            bus: SessionEventBus,
            tenant: TenantId,
            sessionId: SessionId,
            cursor: int64,
            subscribeCts: CancellationTokenSource
        ) : Task<PermissionRequestedEvent option> =
        task {
            let enumerable = bus.Subscribe(tenant, sessionId, cursor, subscribeCts.Token)
            let enumerator = enumerable.GetAsyncEnumerator(subscribeCts.Token)

            try
                let mutable found: PermissionRequestedEvent option = None
                let mutable go = true

                while go do
                    let! has = enumerator.MoveNextAsync().AsTask()

                    if not has then
                        go <- false
                    else
                        match enumerator.Current with
                        | :? PermissionRequestedEvent as asked when not (isNull (box asked)) ->
                            found <- Some asked
                            go <- false
                        | _ -> ()

                return found
            finally
                try
                    enumerator.DisposeAsync().AsTask() |> ignore
                with _ ->
                    ()
        }
