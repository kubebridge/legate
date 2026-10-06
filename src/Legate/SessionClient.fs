// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Runtime.CompilerServices
open System.Text
open System.Threading
open System.Threading.Tasks
open Akka.Actor
open Microsoft.Extensions.AI
open Microsoft.Extensions.Logging

// Session client: the public receiver PromptAndWaitAsync extends. The
// client holds the suspendable prompt path (store, tenant, actor
// resolution), the event bus the suspension signal subscribes to, the
// agent store SetAgent validates against, and the wait-bound seam.
// Construction stays internal: the container facade owns it once it lands,
// while tests construct it directly. The actor resolver never crosses the
// public API (Akka types stay internal).
/// What the shared auto-title helper needs: the enablement flag, the
/// title-model fallback chain, the chat client the title call goes
/// through, and the logger titling reports to. The container wiring sets
/// it on the client; a missing value (or a missing client) degrades to
/// no-op titling.
type internal AutoTitleDeps =
    {
        /// Whether untitled sessions title from their first prompt.
        Enabled: bool
        /// The configured title model in provider/model form, or null to
        /// fall back.
        TitleModel: string | null
        /// The configured compaction model in provider/model form, or null
        /// to fall back.
        CompactionModel: string | null
        /// The facade default model in provider/model form, or null to fall
        /// back.
        FacadeDefaultModel: string | null
        /// The configured Llm:DefaultModel in provider/model form, or null
        /// to fall back to the agent-file default.
        LlmDefaultModel: string | null
        /// The agent-file default model: the last fallback when no
        /// configured model names one.
        FallbackModel: ModelReference
        /// The chat client the title call goes through, or null when the
        /// host registered none (identity-only mode: titling no-ops).
        ChatClient: IChatClient | null
        /// The logger titling reports to, or null for silent titling.
        Logger: ILogger | null
    }

// ────────────────── Subscribe routing ──────────────────

/// Where a Subscribe streams from: the process-local bus or the owning
/// session entity through the shard region. Internal so no Akka type ever
/// crosses the public API.
type internal ISubscribeRouter =

    /// Streams the session's events from the cursor: replay-then-live,
    /// at-least-once with gap detection and redelivery.
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to subscribe to.</param>
    /// <param name="fromSequence">The exclusive cursor.</param>
    /// <param name="cancellationToken">Abandons the replay and the live wait.</param>
    /// <returns>The replay-then-live event stream.</returns>
    abstract Subscribe:
        tenant: TenantId * sessionId: SessionId * fromSequence: int64 * cancellationToken: CancellationToken ->
            IAsyncEnumerable<SessionEvent>

/// The Local-mode router: the process-local bus replay-then-live path,
/// unchanged.
type internal LocalSubscribeRouter(eventBus: SessionEventBus) =

    do ArgumentNullException.ThrowIfNull(eventBus)

    interface ISubscribeRouter with
        member _.Subscribe(tenant, sessionId, fromSequence, cancellationToken) =
            eventBus.Subscribe(tenant, sessionId, fromSequence, cancellationToken)

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
    let mutable autoTitle: AutoTitleDeps option = None
    let mutable subscribeRouter: ISubscribeRouter option = None
    let mutable completionEra: CompletionEra.CompletionEraMarker option = None
    let mutable destinations: CompletionDestinations option = None
    let mutable settlementStore: ISessionSettlementStore option = None

    /// The atomic settlement capability durable receipt lookup reads
    /// through, or None when the host never registered one: direct test
    /// constructions and providers without the capability carry None here
    /// and the lookup fails fast instead of serving an unfenced fallback.
    /// Set once by the container wiring from the registered
    /// ISessionSettlementStore; tests set it directly.
    member internal _.SettlementStore
        with get (): ISessionSettlementStore option = settlementStore
        and set (value: ISessionSettlementStore option) = settlementStore <- value

    /// Requires the durable settlement capability behind receipt-bound
    /// observation: the container-registered ISessionSettlementStore the
    /// client carries, else the session store itself when it implements the
    /// capability (the in-memory composition). Stores with neither fail
    /// clearly here instead of serving an unsafe process-local or unfenced
    /// fallback.
    member internal _.RequireSettlementStore() : ISessionSettlementStore =
        match settlementStore with
        | Some capable -> capable
        | None ->
            match box store with
            | :? ISessionSettlementStore as capable -> capable
            | _ ->
                raise (
                    InvalidOperationException(
                        "The configured ISessionStore must implement ISessionSettlementStore to accept work requiring durable receipts."
                    )
                )

    member internal _.CompletionDestinations
        with get () = destinations
        and set value = destinations <- value

    member internal _.ValidateCompletionRoute(session: Session) =
        if isNull (box session) || isNull (box session.Options) then
            raise (
                CompletionRoutingException(
                    Nullable tenant,
                    Nullable(
                        if isNull (box session) then
                            Unchecked.defaultof<SessionId>
                        else
                            session.Id
                    ),
                    null,
                    CompletionRoutingReason.UnsupportedFormat
                )
            )

        session.Options.ValidatePersistence()

        match destinations, session.Options.CompletionDestinationId with
        | _, null -> ()
        | Some routes, _ -> routes.Validate session
        | None, id ->
            raise (
                CompletionRoutingException(Nullable tenant, Nullable session.Id, id, CompletionRoutingReason.Unknown)
            )

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

    /// The auto-title knobs the shared facade helper reads, or None when
    /// the host never configured them: without them both prompt entries
    /// skip titling. Set once by the container wiring; tests set it
    /// directly.
    member internal _.AutoTitle
        with get (): AutoTitleDeps option = autoTitle
        and set (value: AutoTitleDeps option) = autoTitle <- value

    /// The per-session gate serialising concurrent prompts so Queue append
    /// order stays deterministic.
    member internal _.SemaphoreFor(sessionId: SessionId) : SemaphoreSlim =
        semaphores.GetOrAdd(sessionId, fun _ -> new SemaphoreSlim(1, 1))

    /// The router Subscribe streams through: Local mode delegates to the
    /// process-local event bus; the cluster modes route through the owning
    /// session entity with store-replay resume. None means the local bus
    /// path (existing constructions and tests keep Local behavior
    /// unchanged); the session client facade sets a cluster router when
    /// the host runs StaticSeeds or Kubernetes. Set once by the container
    /// wiring; tests set it directly.
    member internal _.SubscribeRouter
        with get (): ISubscribeRouter option = subscribeRouter
        and set (value: ISubscribeRouter option) = subscribeRouter <- value

    /// The completion-era marker Open and Fork call after a successful
    /// CreateSession (issue 289), or None when the host runs without an
    /// era gate: unmarked sessions read pre-era (quiet). Set once by the
    /// container wiring; tests set it directly.
    member internal _.CompletionEra
        with get (): CompletionEra.CompletionEraMarker option = completionEra
        and set (value: CompletionEra.CompletionEraMarker option) = completionEra <- value

// ────────────────── Shared auto-title helper ──────────────────

/// One shared auto-title helper for both prompt entries
/// (<c>SessionClientOperations.PromptAsync</c> and
/// <c>SessionClientExtensions.PromptAndWaitAsync</c>): titles an untitled
/// session once from its first prompt when the host opted in through
/// <c>SessionsOptions.AutoTitle</c>. Generation runs fire-and-forget on a
/// non-caller token, failures are swallowed and logged without prompt
/// content, and the normalised title lands through
/// <c>ISessionStore.SetSessionTitle</c>, never blocking or failing the
/// turn. Internal: tests drive <c>titleAsync</c> directly.
module internal SessionAutoTitle =

    /// The instruction leading the title call: the first prompt follows as
    /// the user message, and the model replies with the title text only.
    let TitleInstruction =
        "Generate a short title for the chat session that starts with the user message below. Reply with the title text only: one line, at most 60 characters, no quotation marks."

    /// The longest title written to the store: model replies past it are
    /// truncated.
    let MaxTitleLength = 80

    /// The longest first-prompt excerpt sent to the title call: longer
    /// prompts truncate, so one huge first message never becomes a huge
    /// title call.
    let MaxPromptChars = 2000

    /// Sessions with a title call in flight or completed: at most one
    /// generation fires per session per process. Entries clear when the
    /// call fails or yields nothing, so a later prompt retries; a titled
    /// session never refires because the stored title is non-empty.
    let private fired = ConcurrentDictionary<TenantId * SessionId, byte>()

    /// Concatenates a message's text parts with newlines, mirroring the
    /// transcript folding; a message with no text parts reads as empty.
    /// <param name="message">The message to read, or null.</param>
    /// <returns>The joined text, or empty when there is none.</returns>
    let promptTextOf (message: UserMessage) : string =
        if isNull (box message) then
            ""
        else
            let builder = Text.StringBuilder()

            if not (isNull (box message.Parts)) then
                let mutable parts = 0

                for part in message.Parts do
                    match part with
                    | :? TextContent as text when not (isNull (box text)) && not (isNull (box text.Text)) ->
                        if parts > 0 then
                            builder.Append '\n' |> ignore

                        builder.Append(text.Text) |> ignore
                        parts <- parts + 1
                    | _ -> ()

            builder.ToString()

    /// Normalises a title response to one trimmed line within
    /// <c>MaxTitleLength</c>: blank responses read as null and skip the
    /// write.
    /// <param name="text">The model response text, or null.</param>
    /// <returns>The title to store, or null when there is nothing to write.</returns>
    let normalizeTitle (text: string | null) : string | null =
        match text with
        | null -> null
        | value ->
            let first =
                value.Split([| '\r'; '\n' |], StringSplitOptions.RemoveEmptyEntries)
                |> Array.tryHead

            match first with
            | None -> null
            | Some line ->
                let trimmed = line.Trim().Trim('"').Trim()

                let bounded =
                    if trimmed.Length > MaxTitleLength then
                        trimmed.Substring(0, MaxTitleLength).TrimEnd()
                    else
                        trimmed

                if String.IsNullOrWhiteSpace bounded then null else bounded

    /// Resolves which model the title call is attributed to: the
    /// configured title model, then the compaction model, then the facade
    /// default, then the configured default, then the agent-file default.
    /// <param name="deps">The auto-title knobs. Must not be null.</param>
    /// <returns>The model the title call is attributed to.</returns>
    let resolveTitleModel (deps: AutoTitleDeps) : ModelReference =
        if isNull (box deps) then
            raise (ArgumentNullException(nameof deps))

        let first =
            [
                deps.TitleModel
                deps.CompactionModel
                deps.FacadeDefaultModel
                deps.LlmDefaultModel
            ]
            |> List.tryPick (fun raw ->
                match raw with
                | null -> None
                | text when String.IsNullOrWhiteSpace text -> None
                | text -> Some(text.Trim()))

        match first with
        | Some model -> ModelReference.Parse(model)
        | None -> deps.FallbackModel

    /// Logs one auto-title line without prompt content: ids, the model,
    /// and lengths only.
    /// <param name="logger">The logger, or null for silent titling.</param>
    /// <param name="level">True for warning, false for debug.</param>
    /// <param name="sessionId">The session being titled.</param>
    /// <param name="detail">The detail line, already free of prompt content.</param>
    let private log (logger: ILogger | null) (warning: bool) (sessionId: SessionId) (detail: string) : unit =
        match logger with
        | null -> ()
        | live ->
            if warning then
                live.LogWarning("Auto-title for session {SessionId}: {Detail}", sessionId, detail)
            else
                live.LogDebug("Auto-title for session {SessionId}: {Detail}", sessionId, detail)

    /// Titles one untitled session: re-reads the row, generates through
    /// the configured chat client, and writes the normalised title. Every
    /// failure (a missing row, a set title, no text, a provider error, an
    /// empty response, a lost write) returns without throwing and without
    /// logging prompt content; the caller never awaits this on the
    /// prompt path.
    /// <param name="client">The session client. Must not be null.</param>
    /// <param name="sessionId">The session to title.</param>
    /// <param name="message">The first prompt, or null.</param>
    /// <param name="cancellationToken">Abandons the title call (callers pass a non-caller token).</param>
    let titleAsync
        (client: SessionClient)
        (sessionId: SessionId)
        (message: UserMessage)
        (cancellationToken: CancellationToken)
        : Task<unit> =
        task {
            try
                if isNull (box client) then
                    ()
                else
                    match client.AutoTitle with
                    | None -> ()
                    | Some deps when not deps.Enabled -> ()
                    | Some deps ->
                        match deps.ChatClient with
                        | null -> ()
                        | chat ->
                            let promptText = promptTextOf message

                            if String.IsNullOrWhiteSpace promptText then
                                ()
                            elif not (fired.TryAdd((client.Tenant, sessionId), 0uy)) then
                                ()
                            else
                                let mutable keep = false

                                try
                                    try
                                        let! session =
                                            client.Store.GetSession(client.Tenant, sessionId, cancellationToken)

                                        match session with
                                        | null -> ()
                                        | titled when not (String.IsNullOrWhiteSpace titled.Title) -> ()
                                        | _ ->
                                            let model = resolveTitleModel deps

                                            log deps.Logger false sessionId (sprintf "generating with model %O." model)

                                            let excerpt =
                                                if promptText.Length > MaxPromptChars then
                                                    promptText.Substring(0, MaxPromptChars)
                                                else
                                                    promptText

                                            let history =
                                                ResizeArray<ChatMessage>(
                                                    [|
                                                        ChatMessage(ChatRole.System, TitleInstruction)
                                                        ChatMessage(ChatRole.User, excerpt)
                                                    |]
                                                )
                                                :> IList<ChatMessage>

                                            let! response =
                                                chat.GetResponseAsync(history, ChatOptions(), cancellationToken)

                                            let raw =
                                                if isNull (box response) || isNull (box response.Text) then
                                                    null
                                                else
                                                    response.Text

                                            match normalizeTitle raw with
                                            | null ->
                                                log deps.Logger false sessionId "the model returned no usable title."
                                            | title ->
                                                let! _ =
                                                    client.Store.SetSessionTitle(
                                                        client.Tenant,
                                                        sessionId,
                                                        title,
                                                        cancellationToken
                                                    )

                                                keep <- true

                                                log
                                                    deps.Logger
                                                    false
                                                    sessionId
                                                    (sprintf "titled (%d characters)." title.Length)
                                    with ex ->
                                        // Client-safe like the compaction
                                        // mapping: the message only, never
                                        // secrets, arguments, or prompt
                                        // content (the prompt travels only
                                        // to the model, never to the logs).
                                        let reason =
                                            if isNull (box ex) || String.IsNullOrEmpty ex.Message then
                                                ex.GetType().Name
                                            else
                                                ex.Message

                                        log deps.Logger true sessionId (sprintf "failed: %s" reason)
                                finally
                                    if not keep then
                                        fired.TryRemove((client.Tenant, sessionId)) |> ignore
            with _ ->
                ()
        }

    /// Fires <c>titleAsync</c> without awaiting it: the prompt path never
    /// blocks on titling. Fast sync pre-checks (enabled, a chat client, a
    /// text prompt) skip the task entirely when titling cannot run; the
    /// task itself swallows everything else.
    /// <param name="client">The session client, or null.</param>
    /// <param name="sessionId">The session to title.</param>
    /// <param name="message">The first prompt, or null.</param>
    let fire (client: SessionClient) (sessionId: SessionId) (message: UserMessage) : unit =
        try
            if isNull (box client) then
                ()
            else
                match client.AutoTitle with
                | None -> ()
                | Some deps when not deps.Enabled -> ()
                | Some deps ->
                    match deps.ChatClient with
                    | null -> ()
                    | _ ->
                        if String.IsNullOrWhiteSpace(promptTextOf message) then
                            ()
                        else
                            titleAsync client sessionId message CancellationToken.None |> ignore
        with _ ->
            ()

// ────────────────── PromptAndWait ──────────────────

/// <summary>Extension methods on <see cref="T:Legate.SessionClient" />.</summary>
[<Sealed; AbstractClass; Extension>]
type SessionClientExtensions =

    /// Prompts with Queue delivery and waits using CancellationToken.None.
    /// Uses the session timeout or client default bound and observes the accepted operation's durable receipt.
    [<Extension>]
    static member PromptAndWaitAsync(client: SessionClient, sessionId: SessionId, message: UserMessage) =
        SessionClientExtensions.PromptAndWaitAsync(client, sessionId, message, CancellationToken.None)

    /// Prompts with non-null plain text, Queue delivery and CancellationToken.None.
    /// Uses the session timeout or client default bound; suspended permission workflows require explicit handling.
    [<Extension>]
    static member PromptAndWaitAsync(client: SessionClient, sessionId: SessionId, text: string) =
        SessionClientExtensions.PromptAndWaitAsync(client, sessionId, text, CancellationToken.None)

    /// Prompts with non-null plain text and Queue delivery, observing the accepted operation's durable receipt.
    /// Cancellation abandons the operation but never aborts an accepted turn; settlement wins ties.
    /// Uses the session timeout or client default bound. Cast ambiguous null/default literals or use named arguments.
    [<Extension>]
    static member PromptAndWaitAsync
        (client: SessionClient, sessionId: SessionId, text: string, cancellationToken: CancellationToken)
        =
        ArgumentNullException.ThrowIfNull(text)
        SessionClientExtensions.PromptAndWaitAsync(client, sessionId, UserMessage.Text(text), cancellationToken)

    /// Prompts the session with one user message over Queue delivery and
    /// waits until the accepted operation settles, returning the committed
    /// winning <see cref="T:Legate.TurnResult" />. The wait follows this
    /// prompt's own receipt (its immutable inbox position), never the next
    /// session FIFO result, so concurrent callers cannot steal one another's
    /// results and a late observer of the same operation reads the same
    /// committed outcome.
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

            let mutable entryOpt: InboxEntry option = None
            let mutable promptError: exn option = None

            try
                let! current = store.GetSession(tenant, sessionId, cancellationToken)

                match current with
                | null -> raise (SessionNotFoundException(sessionId, "The session does not exist."))
                | live -> client.ValidateCompletionRoute live

                // Fail fast on unsupported providers before accepting work:
                // without the settlement capability no durable observation
                // exists, and no process-local fallback is safe.
                client.RequireSettlementStore() |> ignore

                let! resolved = client.Resolve(sessionId, cancellationToken)

                let! entry =
                    SessionActor.promptSuspendableAsync store tenant sessionId resolved message cancellationToken

                // Titling never blocks or fails the turn: the shared
                // helper no-ops unless the host opted in and the stored
                // title is still empty.
                SessionAutoTitle.fire client sessionId message

                entryOpt <- Some entry
            with ex ->
                // Wait-abandonment (issue 85 decision): a cancellation racing
                // the prompt never aborts the turn. AbortSession would fault
                // the suspendable actor, whose mailbox has no such arm (its
                // runner is invoked with CancellationToken.None), and Abort
                // on WaitingForInput is a no-op per #35 anyway. Rethrow and
                // leave any appended turn running to settle normally. The
                // Abort client verb remains the explicit abort path.
                promptError <- Some ex

            semaphore.Release() |> ignore

            match promptError, entryOpt with
            | Some error, _ -> return raise error
            | None, Some entry ->
                // The prompt landed: housekeeping reads never observe the
                // caller token, so a cancellation surfaces in the race below
                // (abandon the wait, then throw) rather than as a raw store
                // throw.
                let! session = store.GetSession(tenant, sessionId, CancellationToken.None)

                match session with
                | null -> return raise (SessionNotFoundException(sessionId, "The session does not exist."))
                | live ->
                    // Receipt-bound observation (issue 383): the wait follows
                    // this prompt's own accepted inbox position, never the
                    // next FIFO settle, so concurrent waiters cannot steal
                    // one another's results.
                    let receipt =
                        AcceptedOperation(
                            sessionId,
                            entry.Position,
                            entry.TurnId,
                            OperationKind.Queue,
                            entry.AppendedAt
                        )

                    let bound = SessionClientExtensions.BoundOf(live, client.DefaultBound)

                    return!
                        SessionClientExtensions.RaceAsync(client, sessionId, receipt, cursor, bound, cancellationToken)
            | None, _ -> return raise (InvalidOperationException("The prompt completed without an accepted entry."))
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

    /// Maps an accepted inbox entry to its public durable receipt: the
    /// immutable position plus the turn identity stamped at accept.
    /// Reply payloads map to OperationKind.Reply and never promise an
    /// independent turn; user messages map from their delivery mode.
    /// <param name="entry">The accepted inbox entry. Must not be null.</param>
    static member internal ToReceipt(entry: InboxEntry | null) : AcceptedOperation =
        if isNull (box entry) then
            raise (ArgumentNullException(nameof entry))

        let present = unbox<InboxEntry> (box entry)

        let kind =
            match box present.Payload with
            | :? ReplyPayload -> OperationKind.Reply
            | _ ->
                match present.Delivery with
                | DeliveryMode.Inject -> OperationKind.Inject
                | DeliveryMode.Interrupt -> OperationKind.Interrupt
                | _ -> OperationKind.Queue

        AcceptedOperation(present.SessionId, present.Position, present.TurnId, kind, present.AppendedAt)

    /// Observes one accepted operation through its receipt: the
    /// authoritative durable status, the associated real turn when known,
    /// and the committed winning terminal result when available. Pending,
    /// Unknown, Unavailable, and Terminal are distinguishable: an unknown
    /// session throws SessionNotFoundException (other-tenant sessions read
    /// the same way, so possession grants neither access nor turn
    /// ownership); an entry missing or mismatched in this tenant reads
    /// Unknown; a storage failure reads Unavailable and never terminal;
    /// the winning execution_settlements row reads Terminal; otherwise
    /// the operation reads Pending with its turn association when known.
    /// Stale or losing reports never replace the committed winner.
    /// Sinkless sessions observe the same way. Retention is bounded.
    /// <param name="client">The session client. Must not be null.</param>
    /// <param name="receipt">The accepted-operation receipt. Must not be null.</param>
    /// <param name="cancellationToken">Abandons the lookup.</param>
    static member internal ReadOperationAsync
        (client: SessionClient, receipt: AcceptedOperation, cancellationToken: CancellationToken)
        : Task<OperationResult> =
        ArgumentNullException.ThrowIfNull(client)

        if isNull (box receipt) then
            raise (ArgumentNullException(nameof receipt))

        task {
            let tenant = client.Tenant
            let sessionId = receipt.SessionId
            let position = receipt.Position

            let! current = SessionClientExtensions.RequireAsync(client, sessionId, cancellationToken)
            client.ValidateCompletionRoute current
            let settlement = client.RequireSettlementStore()

            let receiptKind = receipt.Kind
            let receiptTurn = receipt.OperationId

            let toUnknown () =
                OperationResult(
                    sessionId,
                    position,
                    receiptKind,
                    OperationStatus.Unknown,
                    Nullable(),
                    Unchecked.defaultof<TurnResult>
                )

            let toUnavailable () =
                OperationResult(
                    sessionId,
                    position,
                    receiptKind,
                    OperationStatus.Unavailable,
                    Nullable(),
                    Unchecked.defaultof<TurnResult>
                )

            let mutable entryFailed = false
            let mutable committedFailed = false

            let! entry =
                task {
                    try
                        return! settlement.TryReadEntry(tenant, sessionId, position, cancellationToken)
                    with _ ->
                        entryFailed <- true
                        return null
                }

            let! committed =
                task {
                    try
                        return! settlement.TryReadCommitted(tenant, sessionId, position, cancellationToken)
                    with _ ->
                        committedFailed <- true
                        return null
                }

            let entryMissing = isNull (box entry)

            let committedPresent = not (isNull (box committed))

            let committedResult: TurnResult | null =
                if committedPresent then
                    (unbox<SessionSettlementOutcome> (box committed)).Result
                else
                    null

            let hasWinner = committedPresent && not (isNull (box committedResult))

            let winnerResult: TurnResult =
                if hasWinner then
                    unbox<TurnResult> (box committedResult)
                else
                    Unchecked.defaultof<TurnResult>

            if entryFailed || committedFailed then
                // Storage failure is never terminal and never fabricated:
                // a winner already in hand still reads Terminal (it is the
                // committed truth), otherwise the observation is Unavailable.
                if hasWinner then
                    let knownTurn =
                        if receiptTurn <> Unchecked.defaultof<TurnId> then
                            Nullable receiptTurn
                        else
                            Nullable()

                    return
                        OperationResult(
                            sessionId,
                            position,
                            receiptKind,
                            OperationStatus.Terminal,
                            knownTurn,
                            winnerResult
                        )
                else
                    return toUnavailable ()
            elif entryMissing then
                if hasWinner then
                    let knownTurn =
                        if receiptTurn <> Unchecked.defaultof<TurnId> then
                            Nullable receiptTurn
                        else
                            Nullable()

                    return
                        OperationResult(
                            sessionId,
                            position,
                            receiptKind,
                            OperationStatus.Terminal,
                            knownTurn,
                            winnerResult
                        )
                else
                    return toUnknown ()
            else
                let present = unbox<InboxEntry> (box entry)

                if present.SessionId <> sessionId then
                    return toUnknown ()
                else
                    let entryKind = SessionClientExtensions.ToReceipt(present).Kind

                    if entryKind <> receiptKind then
                        return toUnknown ()
                    else if
                        present.TurnId <> Unchecked.defaultof<TurnId>
                        && receiptTurn <> Unchecked.defaultof<TurnId>
                        && present.TurnId <> receiptTurn
                    then
                        return toUnknown ()
                    else
                        let knownTurn =
                            if receiptTurn <> Unchecked.defaultof<TurnId> then
                                Nullable receiptTurn
                            elif present.TurnId <> Unchecked.defaultof<TurnId> then
                                Nullable present.TurnId
                            else
                                Nullable()

                        if hasWinner then
                            return
                                OperationResult(
                                    sessionId,
                                    position,
                                    receiptKind,
                                    OperationStatus.Terminal,
                                    knownTurn,
                                    winnerResult
                                )
                        else
                            // Accepted but with no committed winner yet:
                            // a clean null read is Pending (the turn may
                            // still run, fold, or resume), never
                            // Unavailable. True storage failure already
                            // returned above via the failure flags.
                            return
                                OperationResult(
                                    sessionId,
                                    position,
                                    receiptKind,
                                    OperationStatus.Pending,
                                    knownTurn,
                                    Unchecked.defaultof<TurnResult>
                                )
        }

    /// Requires the session row or throws the boundary precondition
    /// failure. Existence only: the per-operation boundaries own the
    /// state checks, so a Close racing the read maps there.
    /// <param name="client">The session client. Must not be null.</param>
    /// <param name="sessionId">The session to require.</param>
    /// <param name="cancellationToken">Abandons the read.</param>
    static member private RequireAsync
        (client: SessionClient, sessionId: SessionId, cancellationToken: CancellationToken)
        : Task<Session> =
        task {
            let! found = client.Store.GetSession(client.Tenant, sessionId, cancellationToken)

            match found with
            | null ->
                let ex = SessionNotFoundException(sessionId, "The session does not exist.")
                return raise ex
            | session -> return session
        }

    /// The poll cadence between durable re-reads while no live hint fires:
    /// remote, restarted, and reconnected observers converge on this beat.
    /// Live local observers wake on the hint instead of waiting out the
    /// poll.
    static member internal WaitPollInterval = TimeSpan.FromMilliseconds 50.0

    /// Reads the committed winning observation once when terminal: Some
    /// only when the durable row carries a winner. Never throws for storage
    /// faults (the observation reads Unavailable, never terminal); session
    /// and capability preconditions belong to the fail-fast pre-read, so
    /// mid-wait faults keep polling instead of manufacturing a verdict.
    /// <param name="client">The session client. Must not be null.</param>
    /// <param name="receipt">The accepted-operation receipt. Must not be null.</param>
    static member internal TryTerminalAsync
        (client: SessionClient, receipt: AcceptedOperation)
        : Task<OperationResult option> =
        task {
            try
                let! observed = SessionClientExtensions.ReadOperationAsync(client, receipt, CancellationToken.None)

                match observed.Status with
                | OperationStatus.Terminal when not (isNull (box observed.Result)) -> return Some observed
                | _ -> return None
            with _ ->
                return None
        }

    /// Unwraps the committed winning turn result from a terminal
    /// observation. TryTerminalAsync only surfaces observations carrying a
    /// winner; a missing result keeps polling instead of manufacturing a
    /// verdict.
    /// <param name="observed">The terminal observation. Must not be null.</param>
    static member internal WinnerResult(observed: OperationResult) : TurnResult option =
        if isNull (box observed) then
            None
        else
            match box observed.Result with
            | null -> None
            | _ -> Some(unbox<TurnResult> (box observed.Result))

    /// Races the receipt-bound durable observation against the suspension
    /// Subscribe, the caller token, and the wait bound. Settlement wins
    /// ties: every wake re-reads the committed row before honoring any
    /// other side.
    static member private RaceAsync
        (
            client: SessionClient,
            sessionId: SessionId,
            receipt: AcceptedOperation,
            cursor: int64,
            bound: TimeSpan,
            cancellationToken: CancellationToken
        ) : Task<TurnResult> =
        task {
            let tenant = client.Tenant
            let bus = client.EventBus
            let position = receipt.Position

            // Fail fast before waiting: unknown sessions (including
            // other-tenant sessions), route violations, and unsupported
            // providers surface now instead of lapsing the bound.
            let! _ = SessionClientExtensions.ReadOperationAsync(client, receipt, CancellationToken.None)

            use subscribeCts = new CancellationTokenSource()
            use boundCts = new CancellationTokenSource()
            let cancelTcs = TaskCompletionSource<bool>()

            use _registration =
                cancellationToken.Register(fun () -> cancelTcs.TrySetResult(true) |> ignore)

            if cancellationToken.IsCancellationRequested then
                cancelTcs.TrySetResult(true) |> ignore

            let boundTask = client.WaitDelay.Delay(bound, boundCts.Token)
            let cancelTask = cancelTcs.Task

            let suspendTask =
                SessionClientExtensions.SuspendAsync(bus, tenant, sessionId, cursor, subscribeCts)

            let mutable suspendActive = true
            let mutable outcome: TurnResult option = None
            let mutable failure: exn option = None

            while outcome.IsNone && failure.IsNone do
                // Settlement fast-path: a commit before or during the
                // subscribe still converges here.
                let! fast = SessionClientExtensions.TryTerminalAsync(client, receipt)

                match fast with
                | Some settled ->
                    match SessionClientExtensions.WinnerResult(settled) with
                    | Some result -> outcome <- Some result
                    | None -> ()
                | None ->
                    let hint, unsubscribe = PromptWaitHubs.SubscribePosition tenant sessionId position

                    try
                        // The commit raced the subscribe: re-read before
                        // parking on the hint, since the actor always commits
                        // before notifying.
                        let! raced = SessionClientExtensions.TryTerminalAsync(client, receipt)

                        match raced with
                        | Some settled ->
                            match SessionClientExtensions.WinnerResult(settled) with
                            | Some result -> outcome <- Some result
                            | None -> ()
                        | None ->
                            let pollTask =
                                client.WaitDelay.Delay(SessionClientExtensions.WaitPollInterval, CancellationToken.None)

                            let candidates = ResizeArray<Task>()
                            candidates.Add(hint)
                            candidates.Add(boundTask)
                            candidates.Add(cancelTask)

                            if suspendActive then
                                candidates.Add(suspendTask)

                            candidates.Add(pollTask)

                            let! _winner = Task.WhenAny(candidates)

                            // Settlement wins every tie, mirroring
                            // StopArbitration: re-read before honoring any
                            // other side.
                            let! committed = SessionClientExtensions.TryTerminalAsync(client, receipt)

                            match committed with
                            | Some settled ->
                                match SessionClientExtensions.WinnerResult(settled) with
                                | Some result -> outcome <- Some result
                                | None -> ()
                            | None ->
                                if suspendActive && suspendTask.IsCompleted then
                                    if suspendTask.IsCompletedSuccessfully then
                                        match suspendTask.Result with
                                        | None ->
                                            // The stream ended with no suspension: drop the
                                            // suspension side and keep racing the rest.
                                            suspendActive <- false
                                        | Some asked ->
                                            try
                                                subscribeCts.Cancel()
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
                                            | aggregate when aggregate.InnerExceptions.Count > 0 ->
                                                aggregate.InnerExceptions[0]
                                            | aggregate -> aggregate :> exn

                                        match inner with
                                        | :? OperationCanceledException ->
                                            // The Subscribe tore down with our own cancel:
                                            // drop the suspension side and keep racing.
                                            suspendActive <- false
                                        | _ ->
                                            try
                                                subscribeCts.Cancel()
                                            with _ ->
                                                ()

                                            failure <- Some inner
                                    elif suspendTask.IsCanceled then
                                        suspendActive <- false
                                    else
                                        ()
                                elif cancelTask.IsCompleted then
                                    // Wait-abandonment (issue 85 decision): never abort
                                    // the turn from here; AbortSession would fault the
                                    // suspendable actor and Abort on WaitingForInput is
                                    // a no-op per #35 anyway. Abandon only this
                                    // observation and throw with the turn left running
                                    // to settle normally; a later wait for the same
                                    // receipt remains valid.
                                    cancellationToken.ThrowIfCancellationRequested()
                                    failure <- Some(OperationCanceledException(cancellationToken))
                                elif boundTask.IsCompletedSuccessfully then
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
                                    let inner =
                                        match boundTask.Exception with
                                        | null -> Exception("The wait-bound delay failed.")
                                        | aggregate when aggregate.InnerExceptions.Count > 0 ->
                                            aggregate.InnerExceptions[0]
                                        | aggregate -> aggregate :> exn

                                    failure <- Some inner
                                else
                                    // A hint or poll fired with nothing committed
                                    // yet, or a stale side completed: loop and
                                    // re-read the durable row.
                                    ()
                    finally
                        unsubscribe ()

            try
                subscribeCts.Cancel()
            with _ ->
                ()

            try
                boundCts.Cancel()
            with _ ->
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
