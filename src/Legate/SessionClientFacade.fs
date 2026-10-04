// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Generic
open System.Runtime.CompilerServices
open System.Text.Json.Serialization
open System.Threading
open System.Threading.Tasks
open Akka.Actor
open Microsoft.Extensions.AI
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.DependencyInjection.Extensions
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Logging
open Microsoft.Extensions.Options
open Legate.Agents

// Host-driving facade over the suspendable session actor: the public
// OpenSession, Prompt (+Inject/+Interrupt delivery), Reply, Abort, Compact,
// Fork, SetAgent, ListSessions, WaitForSettle, Subscribe, ReadTranscript,
// and ReadEvents operations per the normative Docs/ARCHITECTURE.md Client
// API table, plus the DI registration
// wiring LocalActorSystem.SessionChildFactory to behaviorWithSuspend with
// the production suspendable runner. WaitForSettle is additive sugar the
// table's PromptAndWait implies but does not name: the settle-wait half an
// interactive host needs beside Subscribe plus Reply (PromptAndWait throws
// on suspension instead of waiting through it). SessionClient keeps its
// internal constructor: this facade is its public factory/DI path, never a
// second client type.
//
// Opt-in rule: registering a single IChatClient opts the host into
// suspendable children (the runner needs a chat client); without one the
// identity-only children stay and SessionClient still serves Open and the
// reads. Tool sources resolve per session from the container's
// IEnumerable<IToolSource>; per-session budgets resolve from the session
// row through TurnLoop.resolveBudget. Resolve the client before starting
// the host: the factory sets SessionChildFactory on the hosted actor
// system service, which must happen before StartAsync spawns the router.

// ──────────────────────────────────────────────────────────────────────────
// Options

/// Options for the DI-registered session client facade. A plain class with
/// mutable properties and defaults, so C# object initialisers work and
/// absent configuration keeps the defaults. Hosts customize by registering
/// their own instance (host registrations win over this default).
[<Sealed>]
type SessionClientOptions() =

    /// The tenant facade-driven sessions belong to. Defaults to
    /// <see cref="F:Legate.TenantId.Default" />: single-tenant hosts keep
    /// it, multi-tenant hosts scope a facade per tenant.
    member val Tenant: TenantId = TenantId.Default with get, set

    /// The wait bound PromptAndWait falls back to when the session carries
    /// no explicit timeout. Must be positive. Defaults to five minutes.
    member val DefaultWaitBound: TimeSpan = TimeSpan.FromMinutes 5.0 with get, set

    /// The claim owner identity the journal prime claims under. Must be a
    /// non-empty string. Defaults to "legate-session-facade".
    member val ClaimOwner: string = "legate-session-facade" with get, set

    /// How long the primed journal claim lasts. Must be positive. Defaults
    /// to one hour, covering the AskTimeout window for interactive hosts.
    /// Prime-only (issue 375): the spawn-time journal prime claims under
    /// this duration, while production renewals grant the Sessions lease
    /// duration (60 s by default) through ClaimHeartbeat.fromSessions.
    member val LeaseDuration: TimeSpan = TimeSpan.FromHours 1.0 with get, set

    /// The default model reference (provider/model) the on-demand
    /// compaction uses as the session model, or null to fall back to the
    /// configured Llm:DefaultModel and then the agent-file default.
    /// Parses through <see cref="M:Legate.ModelReference.Parse(System.String)" />.
    member val DefaultModel: string | null = null with get, set

    /// Validates the options, returning the first violation or null when
    /// valid.
    /// <returns>The first violation, or null when the options are valid.</returns>
    member this.Validate() : string | null =
        if this.DefaultWaitBound <= TimeSpan.Zero then
            "SessionClientOptions.DefaultWaitBound must be positive."
        elif String.IsNullOrWhiteSpace this.ClaimOwner then
            "SessionClientOptions.ClaimOwner must be a non-empty string."
        elif this.LeaseDuration <= TimeSpan.Zero then
            "SessionClientOptions.LeaseDuration must be positive."
        else
            null

// ──────────────────────────────────────────────────────────────────────────
// Compact outcome

/// What an on-demand compact decided: the host branches on the outcome.
/// Serialises polymorphically: every concrete outcome carries a stable
/// <c>$type</c> discriminator on the wire, mirroring
/// <see cref="T:Legate.Reply" /> and <see cref="T:Legate.TurnOutcome" />.
[<AbstractClass>]
[<JsonPolymorphic(TypeDiscriminatorPropertyName = "$type")>]
[<JsonDerivedType(typeof<SessionCompacted>, "compacted")>]
[<JsonDerivedType(typeof<SessionCompactNotNeeded>, "compactNotNeeded")>]
[<JsonDerivedType(typeof<SessionCompactDeferred>, "compactDeferred")>]
[<JsonDerivedType(typeof<SessionCompactFenced>, "compactFenced")>]
type SessionCompactOutcome() = class end

/// The Idle session compacted now without starting a turn.
/// <param name="beforeEstimate">The estimated tokens before compaction.</param>
/// <param name="afterEstimate">The estimated tokens after compaction.</param>
and [<Sealed>] SessionCompacted(beforeEstimate: int64, afterEstimate: int64) =
    inherit SessionCompactOutcome()

    /// The estimated tokens before compaction.
    member _.BeforeEstimate = beforeEstimate

    /// The estimated tokens after compaction.
    member _.AfterEstimate = afterEstimate

/// Nothing compacted and no summariser call ran: the session was Idle but
/// under threshold, WaitingForInput, unconfigured, or the summariser
/// failed and the session continues uncompacted.
and [<Sealed>] SessionCompactNotNeeded() =
    inherit SessionCompactOutcome()

/// The session was Running: the one-shot force flag is armed and the
/// running turn's force-aware boundary hook compacts at the next iteration
/// boundary, bypassing the threshold once.
and [<Sealed>] SessionCompactDeferred() =
    inherit SessionCompactOutcome()

/// The actor lost its claim before the journal write landed, so it
/// journaled nothing: the takeover winner owns the session.
and [<Sealed>] SessionCompactFenced() =
    inherit SessionCompactOutcome()

// ──────────────────────────────────────────────────────────────────────────
// Operations

/// Host-driving operations on <see cref="T:Legate.SessionClient" /> per the
/// normative <c>Docs/ARCHITECTURE.md</c> Client API table: OpenSession,
/// Prompt with
/// every <see cref="T:Legate.DeliveryMode" />, Reply, Abort, Compact, Fork,
/// SetAgent, ListSessions, WaitForSettle, Subscribe as <see cref="T:System.Collections.Generic.IAsyncEnumerable`1" />,
/// ReadTranscript, and ReadEvents. WaitForSettle is additive sugar beyond
/// the table: the settle-wait half an interactive host drives beside
/// Subscribe plus Reply. Control-plane precondition failures
/// throw the <c>Exceptions.fs</c> family; anything after a turn is accepted
/// is a turn outcome returned through <see cref="T:Legate.TurnResult" />.
/// Extension methods, so C# hosts call them on the resolved client with no
/// wrapper type.
[<Sealed; AbstractClass; Extension>]
type SessionClientOperations =

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
            | null -> return raise (SessionNotFoundException(sessionId, "The session does not exist."))
            | session -> return session
        }

    /// Reads the source journal prefix a fork copies: the events with a
    /// stamped sequence through upToSequence (inclusive), in sequence
    /// order. Unknown session, expired journal, and end of stream resolve
    /// to the settled tail: the prefix holds what the replay saw (none on
    /// a fresh unknown or expired journal).
    /// <param name="client">The session client. Must not be null.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session whose journal to read.</param>
    /// <param name="upToSequence">The inclusive sequence the prefix runs through.</param>
    /// <param name="cancellationToken">Abandons the replay.</param>
    static member private ReadPrefixAsync
        (
            client: SessionClient,
            tenant: TenantId,
            sessionId: SessionId,
            upToSequence: int64,
            cancellationToken: CancellationToken
        ) : Task<IReadOnlyList<SessionEvent>> =
        task {
            let collected = ResizeArray<SessionEvent>()
            let mutable cursor = 0L
            let mutable paging = true

            while paging do
                cancellationToken.ThrowIfCancellationRequested()

                let! outcome = client.EventBus.EventStore.Replay(tenant, sessionId, cursor, 100, cancellationToken)

                match outcome with
                | :? EventReplayPage as page when not (isNull (box page)) ->
                    let mutable highest = cursor

                    if not (isNull (box page.Events)) then
                        for event in page.Events do
                            if not (isNull (box event)) then
                                if event.Sequence.HasValue then
                                    if event.Sequence.Value > highest then
                                        highest <- event.Sequence.Value

                                    if event.Sequence.Value <= upToSequence then
                                        collected.Add(event)

                    // Sequences ascend, so a page reaching past the cursor
                    // completes the prefix; otherwise follow it.
                    if page.NextCursor.HasValue && highest <= upToSequence then
                        cursor <- page.NextCursor.Value
                    else
                        paging <- false
                | _ -> paging <- false

            return collected :> IReadOnlyList<SessionEvent>
        }

    /// Requires a registered agent for a rebind: an unknown id throws
    /// <see cref="T:Legate.AgentNotFoundException" /> and a disabled agent
    /// throws <see cref="T:Legate.AgentDisabledException" />.
    /// <param name="agents">The agent store to read. Must not be null.</param>
    /// <param name="tenant">The tenant the agent belongs to.</param>
    /// <param name="agentId">The agent to require.</param>
    /// <param name="cancellationToken">Abandons the read.</param>
    static member private RequireAgentAsync
        (agents: IAgentStore, tenant: TenantId, agentId: AgentId, cancellationToken: CancellationToken)
        : Task =
        task {
            let! registered = agents.GetAgent(tenant, agentId, cancellationToken)

            match registered with
            | null ->
                return raise (AgentNotFoundException(agentId, sprintf "No agent %O exists in this tenant." agentId))
            | agent ->
                if not agent.Enabled then
                    return raise (AgentDisabledException(agentId, sprintf "Agent %O is disabled." agentId))
                else
                    return ()
        }

    /// Snapshots the source options for a fork: every field verbatim, with
    /// the source id merged under ForkedFrom metadata. Null source options
    /// (a row from before options existed) snapshot to defaults plus the
    /// ForkedFrom entry, so the fork always carries its parent.
    /// <param name="source">The session being forked. Must not be null.</param>
    /// <param name="sourceId">The source session id the ForkedFrom entry carries.</param>
    static member private ForkOptions (source: Session) (sourceId: SessionId) : SessionOptions =
        let options = SessionOptions()
        let merged = Dictionary<string, string>(StringComparer.Ordinal)

        match box source with
        | null -> ()
        | _ ->
            match box source.Options with
            | null -> ()
            | _ ->
                // Nullable option fields copy when set and keep the fresh
                // null defaults otherwise: skipping a null is exactly
                // assigning it on a fresh instance. Option.ofObj carries
                // the set value as a non-null binding either way.
                let from: SessionOptions = source.Options

                match Option.ofObj from.Title with
                | Some title -> options.Title <- title
                | None -> ()

                options.AutoClose <- from.AutoClose
                options.Outcome <- from.Outcome

                match Option.ofObj from.AskUser with
                | Some askUser -> options.AskUser <- askUser
                | None -> ()

                match Option.ofObj from.CompletionDestinationId with
                | Some destinationId -> options.CompletionDestinationId <- destinationId
                | None -> ()

                options.MaxIterations <- from.MaxIterations
                options.Timeout <- from.Timeout

                match Option.ofObj from.HostInstructionFiles with
                | Some hostInstructionFiles -> options.HostInstructionFiles <- hostInstructionFiles
                | None -> ()

                options.OnCrashResume <- from.OnCrashResume

                match Option.ofObj from.Metadata with
                | None -> ()
                | Some metadata ->
                    for pair in metadata do
                        if not (merged.ContainsKey pair.Key) then
                            let value = pair.Value

                            match box value with
                            | null -> merged[pair.Key] <- Unchecked.defaultof<string>
                            | _ -> merged[pair.Key] <- value

        merged["ForkedFrom"] <- sourceId.ToString()
        options.Metadata <- merged :> IReadOnlyDictionary<string, string>
        options

    /// Re-keys one journaled event for a fork: the new session id with an
    /// empty (in-flight) sequence, keeping the turn, timestamp, and payload,
    /// so the store stamps the fork's own sequences on append and the fork's
    /// transcript folds exactly like the source prefix.
    /// <param name="sessionId">The forked session the copy belongs to.</param>
    /// <param name="event">The source event to re-key. Must not be null.</param>
    static member private RekeyForFork (sessionId: SessionId) (event: SessionEvent) : SessionEvent =
        if isNull (box event) then
            raise (ArgumentNullException(nameof event))

        let noSequence = Unchecked.defaultof<Nullable<int64>>
        let turnId = event.TurnId
        let timestamp = event.Timestamp

        match event with
        | :? TurnStartedEvent -> TurnStartedEvent(sessionId, turnId, noSequence, timestamp) :> SessionEvent
        | :? TextDeltaEvent as source ->
            TextDeltaEvent(sessionId, turnId, noSequence, timestamp, source.Text) :> SessionEvent
        | :? ReasoningDeltaEvent as source ->
            ReasoningDeltaEvent(sessionId, turnId, noSequence, timestamp, source.Text) :> SessionEvent
        | :? ToolCallStartedEvent as source ->
            ToolCallStartedEvent(sessionId, turnId, noSequence, timestamp, source.ToolCallId, source.ToolName)
            :> SessionEvent
        | :? ToolCallOutputEvent as source ->
            ToolCallOutputEvent(sessionId, turnId, noSequence, timestamp, source.ToolCallId, source.Output)
            :> SessionEvent
        | :? ToolCallCompletedEvent as source ->
            ToolCallCompletedEvent(sessionId, turnId, noSequence, timestamp, source.ToolCallId, source.Error)
            :> SessionEvent
        | :? PermissionRequestedEvent as source ->
            PermissionRequestedEvent(sessionId, turnId, noSequence, timestamp, source.RequestId, source.ToolName)
            :> SessionEvent
        | :? PermissionResolvedEvent as source ->
            PermissionResolvedEvent(sessionId, turnId, noSequence, timestamp, source.RequestId, source.Decision)
            :> SessionEvent
        | :? QuestionAskedEvent as source ->
            QuestionAskedEvent(sessionId, turnId, noSequence, timestamp, source.QuestionId, source.Question)
            :> SessionEvent
        | :? QuestionAnsweredEvent as source ->
            QuestionAnsweredEvent(sessionId, turnId, noSequence, timestamp, source.QuestionId, source.Answer)
            :> SessionEvent
        | :? UsageEvent as source ->
            UsageEvent(sessionId, turnId, noSequence, timestamp, source.InputTokens, source.OutputTokens)
            :> SessionEvent
        | :? CompactedEvent as source ->
            CompactedEvent(sessionId, turnId, noSequence, timestamp, source.BeforeEstimate, source.AfterEstimate)
            :> SessionEvent
        | :? CompactionFailedEvent as source ->
            CompactionFailedEvent(sessionId, turnId, noSequence, timestamp, source.Reason) :> SessionEvent
        | :? TurnCompletedEvent -> TurnCompletedEvent(sessionId, turnId, noSequence, timestamp) :> SessionEvent
        | :? TurnAbortedEvent as source ->
            TurnAbortedEvent(sessionId, turnId, noSequence, timestamp, source.Cause, source.Reason) :> SessionEvent
        | :? TurnFailedEvent as source ->
            TurnFailedEvent(sessionId, turnId, noSequence, timestamp, source.Reason) :> SessionEvent
        | :? SessionClosedEvent -> SessionClosedEvent(sessionId, turnId, noSequence, timestamp) :> SessionEvent
        | :? UserMessageEvent as source ->
            UserMessageEvent(sessionId, turnId, noSequence, timestamp, source.Message) :> SessionEvent
        | :? ContextPrunedEvent as source ->
            ContextPrunedEvent(
                sessionId,
                turnId,
                noSequence,
                timestamp,
                source.PrunedCount,
                source.BeforeEstimate,
                source.AfterEstimate
            )
            :> SessionEvent
        | :? SkillInvalidEvent as source ->
            SkillInvalidEvent(sessionId, turnId, noSequence, timestamp, source.SkillName, source.Reason) :> SessionEvent
        | :? SkillLoadedEvent as source ->
            SkillLoadedEvent(sessionId, turnId, noSequence, timestamp, source.SkillName, source.Companions)
            :> SessionEvent
        | :? AgentInvalidEvent as source ->
            AgentInvalidEvent(sessionId, turnId, noSequence, timestamp, source.AgentName, source.Reason) :> SessionEvent
        | :? AgentSwitchedEvent as source ->
            AgentSwitchedEvent(sessionId, turnId, noSequence, timestamp, source.PreviousAgentId, source.NewAgentId)
            :> SessionEvent
        | _ -> raise (ArgumentException("The journal carries an unknown event kind.", nameof event))

    /// Opens a session: creates the Idle row and eagerly resolves its actor
    /// while the row exists, so the journal prime claims a live token.
    /// The resolve is best-effort: when the local actor system has not
    /// started yet the row still opens and the actor resolves lazily on
    /// the first prompt.
    /// <param name="client">The session client. Must not be null.</param>
    /// <param name="agentId">The agent the session converses with.</param>
    /// <param name="options">The options the session opens with, or null for interactive defaults.</param>
    /// <param name="cancellationToken">Cancels the open.</param>
    /// <returns>The stored session.</returns>
    /// <exception cref="T:Legate.InvalidSessionStateException">A session with the same id already exists in this tenant.</exception>
    [<Extension>]
    static member OpenSessionAsync
        (client: SessionClient, agentId: AgentId, options: SessionOptions | null, cancellationToken: CancellationToken)
        : Task<Session> =
        ArgumentNullException.ThrowIfNull(client)

        task {
            let tenant = client.Tenant

            let effective =
                match options with
                | null -> SessionOptions()
                | present -> SessionOptionsPersistence.Snapshot present

            let title =
                match effective.Title with
                | null -> ""
                | text when String.IsNullOrWhiteSpace text -> ""
                | text -> text

            let now = DateTimeOffset.UtcNow

            let session =
                {
                    Id = SessionId.New()
                    Tenant = tenant
                    AgentId = agentId
                    Title = title
                    State = SessionState.Idle
                    CurrentTurnId = Unchecked.defaultof<Nullable<TurnId>>
                    CreatedAt = now
                    UpdatedAt = now
                    ClosedAt = Unchecked.defaultof<Nullable<DateTimeOffset>>
                    WorkspaceBinding = null
                    Options = effective
                    PermissionGrants = ResizeArray<string>() :> IReadOnlyList<string>
                }

            client.ValidateCompletionRoute session
            let! created = client.Store.CreateSession(tenant, session, cancellationToken)

            // Completion era (issue 289): mark after a successful create;
            // a marking failure degrades to pre-era (quiet) and never
            // fails the open.
            match client.CompletionEra with
            | Some mark ->
                try
                    do! mark tenant created.Id cancellationToken
                with _ ->
                    ()
            | None -> ()

            try
                let! _ = client.Resolve(created.Id, cancellationToken)
                ()
            with :? InvalidOperationException ->
                ()

            return created
        }

    /// Prompts a session with a delivery mode: Queue appends and acts on
    /// the message once the running turn (if any) finishes; Inject appends
    /// and folds into the running turn at its next iteration boundary
    /// without interrupting it; Interrupt appends and pre-empts the running
    /// turn, settling it as Aborted under ExplicitAbort before starting the
    /// new turn. While WaitingForInput every mode appends and waits (Reply
    /// still resumes the suspended turn), and while Idle every mode starts
    /// a turn normally.
    /// <param name="client">The session client. Must not be null.</param>
    /// <param name="sessionId">The session to prompt.</param>
    /// <param name="message">The user message. Must not be null.</param>
    /// <param name="delivery">How the message is delivered to a running turn.</param>
    /// <param name="cancellationToken">Cancels the prompt.</param>
    /// <returns>The appended inbox entry.</returns>
    /// <exception cref="T:Legate.SessionNotFoundException">The session id does not exist.</exception>
    /// <exception cref="T:Legate.InvalidSessionStateException">The session is closed.</exception>
    [<Extension>]
    static member PromptAsync
        (
            client: SessionClient,
            sessionId: SessionId,
            message: UserMessage,
            delivery: DeliveryMode,
            cancellationToken: CancellationToken
        ) : Task<InboxEntry> =
        ArgumentNullException.ThrowIfNull(client)

        if isNull (box message) then
            raise (ArgumentNullException(nameof message))

        task {
            let! current = SessionClientOperations.RequireAsync(client, sessionId, cancellationToken)
            client.ValidateCompletionRoute current

            // Titling never blocks or fails the prompt: the shared helper
            // no-ops unless the host opted in and the stored title is
            // still empty.
            SessionAutoTitle.fire client sessionId message

            let! actor = client.Resolve(sessionId, cancellationToken)

            match delivery with
            | DeliveryMode.Queue ->
                return!
                    SessionActor.promptSuspendableAsync
                        client.Store
                        client.Tenant
                        sessionId
                        actor
                        message
                        cancellationToken
            | DeliveryMode.Inject ->
                return!
                    SessionActor.injectSuspendableAsync
                        client.Store
                        client.Tenant
                        sessionId
                        actor
                        message
                        cancellationToken
            | DeliveryMode.Interrupt ->
                return!
                    SessionActor.interruptSuspendableAsync
                        client.Store
                        client.Tenant
                        sessionId
                        actor
                        message
                        cancellationToken
            | unknown ->
                return
                    raise (
                        ArgumentOutOfRangeException(
                            nameof delivery,
                            sprintf "Unknown delivery mode: %O. Expected Queue, Inject, or Interrupt." unknown
                        )
                    )
        }

    /// Replies to a suspended turn: matches the Reply against the pending
    /// request id and resumes from the cursor with attempt plus 1. An
    /// unknown or already-resolved request id throws the typed
    /// ReplyMismatchException. Reply never starts a turn.
    /// <param name="client">The session client. Must not be null.</param>
    /// <param name="sessionId">The session to reply to.</param>
    /// <param name="reply">The host reply. Must not be null.</param>
    /// <param name="cancellationToken">Cancels the reply.</param>
    /// <returns>The consumed Reply inbox entry.</returns>
    /// <exception cref="T:Legate.SessionNotFoundException">The session id does not exist.</exception>
    /// <exception cref="T:Legate.InvalidSessionStateException">The session is closed.</exception>
    /// <exception cref="T:Legate.ReplyMismatchException">The reply answered nothing pending.</exception>
    [<Extension>]
    static member ReplyAsync
        (client: SessionClient, sessionId: SessionId, reply: Reply, cancellationToken: CancellationToken)
        : Task<InboxEntry> =
        ArgumentNullException.ThrowIfNull(client)

        if isNull (box reply) then
            raise (ArgumentNullException(nameof reply))

        task {
            let! current = SessionClientOperations.RequireAsync(client, sessionId, cancellationToken)
            client.ValidateCompletionRoute current
            let! actor = client.Resolve(sessionId, cancellationToken)
            return! SessionActor.replyAsync client.Store client.Tenant sessionId actor reply cancellationToken
        }

    /// Reads the exact current control target without activating an actor or loading its options.
    [<Extension>]
    static member ReadAbortTargetAsync
        (client: SessionClient, sessionId: SessionId, cancellationToken: CancellationToken)
        : Task<AbortTarget | null> =
        ArgumentNullException.ThrowIfNull(client)

        match client.Store with
        | :? ISessionAbortControlStore as control ->
            control.ReadAbortTarget(client.Tenant, sessionId, cancellationToken)
        | _ ->
            raise (InvalidOperationException("The configured ISessionStore must implement ISessionAbortControlStore."))

    /// Durably requests stop of the exact existing current target, independently of actor activation
    /// or destination availability. Acceptance is not terminal completion. If cancellation or transport
    /// loss makes commit uncertain, retry the same target, never reread and retarget the request.
    /// <param name="client">The session client. Must not be null.</param>
    /// <param name="sessionId">The session to abort the turn in.</param>
    /// <param name="expectedTurnId">The exact target read before requesting stop. Must be nonempty.</param>
    /// <param name="cause">Which abort-family stop cause wins: ExplicitAbort or HostShutdown.</param>
    /// <param name="reason">Why the turn stops. Must not be null. Never contains secrets or tool arguments.</param>
    /// <param name="cancellationToken">Abandons acceptance; cancellation after commit does not undo intent.</param>
    /// <returns>A durable intent receipt, never a terminal result.</returns>
    /// <exception cref="T:Legate.SessionNotFoundException">The session id does not exist.</exception>
    /// <exception cref="T:Legate.InvalidSessionStateException">The session is closed.</exception>
    [<Extension>]
    static member AbortAsync
        (
            client: SessionClient,
            sessionId: SessionId,
            expectedTurnId: TurnId,
            cause: StopCause,
            reason: string,
            cancellationToken: CancellationToken
        ) : Task<HostAbortReceipt> =
        ArgumentNullException.ThrowIfNull(client)

        if isNull (box reason) then
            raise (ArgumentNullException(nameof reason))

        match client.Store with
        | :? ISessionAbortControlStore as control ->
            control.RequestHostAbort(client.Tenant, sessionId, expectedTurnId, cause, reason, cancellationToken)
        | _ ->
            raise (InvalidOperationException("The configured ISessionStore must implement ISessionAbortControlStore."))

    /// Compacts a session on demand: Idle replays the journal and compacts
    /// now without starting a turn; Running arms the one-shot force flag
    /// the turn's force-aware boundary hook honors at the next iteration;
    /// WaitingForInput no-ops (a suspended turn owns the history).
    /// <param name="client">The session client. Must not be null.</param>
    /// <param name="sessionId">The session to compact.</param>
    /// <param name="cancellationToken">Cancels the compact.</param>
    /// <returns>What the compact decided.</returns>
    /// <exception cref="T:Legate.SessionNotFoundException">The session id does not exist.</exception>
    /// <exception cref="T:Legate.InvalidSessionStateException">The session is closed.</exception>
    [<Extension>]
    static member CompactAsync
        (client: SessionClient, sessionId: SessionId, cancellationToken: CancellationToken)
        : Task<SessionCompactOutcome> =
        ArgumentNullException.ThrowIfNull(client)

        task {
            let! current = SessionClientOperations.RequireAsync(client, sessionId, cancellationToken)
            client.ValidateCompletionRoute current
            let! actor = client.Resolve(sessionId, cancellationToken)

            let! reply =
                SessionActor.compactSuspendableAsync client.Store client.Tenant sessionId actor cancellationToken

            match reply with
            | SessionCompactReply.CompactCompleted(beforeEstimate, afterEstimate) ->
                return SessionCompacted(beforeEstimate, afterEstimate) :> SessionCompactOutcome
            | SessionCompactReply.CompactNotNeeded -> return SessionCompactNotNeeded() :> SessionCompactOutcome
            | SessionCompactReply.CompactDeferred -> return SessionCompactDeferred() :> SessionCompactOutcome
            | SessionCompactReply.CompactFenced -> return SessionCompactFenced() :> SessionCompactOutcome
            | SessionCompactReply.CompactRejected rejectedState ->
                // Unreachable: the boundary maps a racing Close to
                // InvalidSessionStateException before the reply surfaces.
                // Kept explicit (never a wildcard) so the match stays
                // exhaustive if the protocol grows.
                return
                    raise (
                        InvalidSessionStateException(
                            sessionId,
                            rejectedState.ToString(),
                            "The session closed before the compact was accepted."
                        )
                    )
        }

    /// Rebinds the agent a session converses with: applies at once when the
    /// session is quiescent (no live turn and an empty pending inbox) and
    /// records the rebind as pending otherwise; the recorded rebind applies
    /// at the next quiescent boundary. The actor journals one
    /// agentSwitched event per applied rebind, and the next turn resolves
    /// its tools and budget under the new agent. Agent validation needs the
    /// registered agent store: an unknown id throws
    /// <see cref="T:Legate.AgentNotFoundException" /> and a disabled agent
    /// throws <see cref="T:Legate.AgentDisabledException" />; hosts running
    /// without a managed agent catalog skip validation and rebind any id.
    /// <param name="client">The session client. Must not be null.</param>
    /// <param name="sessionId">The session to rebind.</param>
    /// <param name="agentId">The agent the session converses with from now on.</param>
    /// <param name="cancellationToken">Cancels the rebind.</param>
    /// <returns>The stored session: rebound when the rebind applied at once, unchanged while pending.</returns>
    /// <exception cref="T:Legate.SessionNotFoundException">The session id does not exist.</exception>
    /// <exception cref="T:Legate.InvalidSessionStateException">The session is closed.</exception>
    /// <exception cref="T:Legate.AgentNotFoundException">No registered agent carries the id.</exception>
    /// <exception cref="T:Legate.AgentDisabledException">The registered agent is disabled.</exception>
    [<Extension>]
    static member SetAgentAsync
        (client: SessionClient, sessionId: SessionId, agentId: AgentId, cancellationToken: CancellationToken)
        : Task<Session> =
        ArgumentNullException.ThrowIfNull(client)

        task {
            let! source = SessionClientOperations.RequireAsync(client, sessionId, cancellationToken)
            client.ValidateCompletionRoute source

            if source.State = SessionState.Closed then
                raise (
                    InvalidSessionStateException(
                        sessionId,
                        source.State.ToString(),
                        "The session is closed and accepts no agent change."
                    )
                )

            match client.Agents with
            | Some agents ->
                do! SessionClientOperations.RequireAgentAsync(agents, client.Tenant, agentId, cancellationToken)
            | None -> ()

            let! actor = client.Resolve(sessionId, cancellationToken)

            return!
                SessionActor.setAgentSuspendableAsync
                    client.Store
                    client.Tenant
                    sessionId
                    actor
                    agentId
                    cancellationToken
        }

    /// Forks a session: creates a new Idle session whose transcript is a
    /// prefix of the source's journal. The new row carries the source agent,
    /// an options snapshot with the source id under ForkedFrom metadata, and
    /// the source title; it carries no claim or lease state and no inbox
    /// entries, and its journal holds the source events with a sequence
    /// through upToSequence (inclusive), re-keyed to the new session.
    /// Beyond-tail cursors clamp to the full journal; a cursor below the
    /// first sequence forks an empty transcript. Forking never validates the
    /// agent: the copy carries the source row's agent verbatim. The source
    /// may be open or closed; its state is untouched, and a fork of a fork
    /// references its immediate parent.
    /// <param name="client">The session client. Must not be null.</param>
    /// <param name="sessionId">The session to fork.</param>
    /// <param name="upToSequence">The inclusive sequence the prefix runs through; beyond-tail clamps to the full journal.</param>
    /// <param name="cancellationToken">Cancels the fork.</param>
    /// <returns>The stored forked session.</returns>
    /// <exception cref="T:Legate.SessionNotFoundException">The session id does not exist.</exception>
    [<Extension>]
    static member ForkAsync
        (client: SessionClient, sessionId: SessionId, upToSequence: int64, cancellationToken: CancellationToken)
        : Task<Session> =
        ArgumentNullException.ThrowIfNull(client)

        task {
            let tenant = client.Tenant
            let! source = SessionClientOperations.RequireAsync(client, sessionId, cancellationToken)
            client.ValidateCompletionRoute source

            let! prefix =
                SessionClientOperations.ReadPrefixAsync(client, tenant, sessionId, upToSequence, cancellationToken)

            let now = DateTimeOffset.UtcNow

            let forked =
                {
                    Id = SessionId.New()
                    Tenant = tenant
                    AgentId = source.AgentId
                    Title = source.Title
                    State = SessionState.Idle
                    CurrentTurnId = Unchecked.defaultof<Nullable<TurnId>>
                    CreatedAt = now
                    UpdatedAt = now
                    ClosedAt = Unchecked.defaultof<Nullable<DateTimeOffset>>
                    WorkspaceBinding = null
                    Options = SessionClientOperations.ForkOptions source sessionId
                    PermissionGrants = ResizeArray<string>() :> IReadOnlyList<string>
                }

            client.ValidateCompletionRoute forked
            let! created = client.Store.CreateSession(tenant, forked, cancellationToken)

            // Completion era (issue 289): mark after a successful create;
            // a marking failure degrades to pre-era (quiet) and never
            // fails the fork.
            match client.CompletionEra with
            | Some mark ->
                try
                    do! mark tenant created.Id cancellationToken
                with _ ->
                    ()
            | None -> ()

            if prefix.Count > 0 then
                // Host-append copy (issue 373): the fork-prefix lands as a
                // host-authorized batch preserving the source TurnIds
                // through RekeyForFork, fenced on the fork's UpdatedAt
                // version stamp. No inbox append, no ClaimNextTurn, no
                // SettleTurn for the copy: the fork carries no turn claim
                // and no lease state, so it opens Idle with an empty inbox
                // and real prompts drain first. The actor prime on Resolve
                // below stays exactly as before for future execution.
                let rekeyed =
                    prefix
                    |> Seq.map (SessionClientOperations.RekeyForFork created.Id)
                    |> List.ofSeq

                let mutable expectedStamp = created.UpdatedAt

                for batch in rekeyed |> List.chunkBySize 100 do
                    let events = ResizeArray<SessionEvent>(batch) :> IReadOnlyList<SessionEvent>

                    match!
                        JournalWriter.appendHostAsync
                            client.EventBus.EventStore
                            tenant
                            created.Id
                            expectedStamp
                            events
                            cancellationToken
                    with
                    | JournalWriter.JournalAppended _ ->
                        let! refreshed = SessionClientOperations.RequireAsync(client, created.Id, cancellationToken)
                        expectedStamp <- refreshed.UpdatedAt
                    | JournalWriter.JournalRejected reason ->
                        raise (
                            InvalidOperationException(
                                sprintf "The fork of session %O lost its host fence: %s." sessionId reason
                            )
                        )
                    | JournalWriter.JournalFailed reason ->
                        raise (
                            InvalidOperationException(
                                sprintf "The fork of session %O failed to copy its prefix: %s." sessionId reason
                            )
                        )

            try
                let! _ = client.Resolve(created.Id, cancellationToken)
                ()
            with :? InvalidOperationException ->
                ()

            return created
        }

    /// Waits for the session's running turn to settle without prompting:
    /// the interactive companion to Subscribe plus Reply. Suspensions do
    /// not resolve the wait (the host answers them through Reply after
    /// observing them on the event stream); settlement, the wait bound, and
    /// the caller's cancellation do. Queue the wait before prompting:
    /// a settle with no waiter only records, so a turn settling first
    /// would leave a later wait hanging until its bound.
    /// <param name="client">The session client. Must not be null.</param>
    /// <param name="sessionId">The session whose turn to wait on.</param>
    /// <param name="bound">How long to wait for the settle; must be positive. The turn keeps running past it.</param>
    /// <param name="cancellationToken">Abandons the wait, never the turn: the waiter is cancelled and the turn settles normally.</param>
    /// <returns>The settled turn result.</returns>
    /// <exception cref="T:Legate.DeadlineExceededException">The wait bound lapsed with the turn left running.</exception>
    [<Extension>]
    static member WaitForSettleAsync
        (client: SessionClient, sessionId: SessionId, bound: TimeSpan, cancellationToken: CancellationToken)
        : Task<TurnResult> =
        ArgumentNullException.ThrowIfNull(client)

        if bound <= TimeSpan.Zero then
            raise (ArgumentOutOfRangeException(nameof bound, "The settle wait bound must be positive."))

        task {
            let hub = PromptWaitHubs.GetOrAddScoped client.Tenant sessionId
            let waiter = hub.EnqueueSettle()

            use boundCts = new CancellationTokenSource()
            let cancelTcs = TaskCompletionSource<bool>()

            use _registration =
                cancellationToken.Register(fun () -> cancelTcs.TrySetResult(true) |> ignore)

            if cancellationToken.IsCancellationRequested then
                cancelTcs.TrySetResult(true) |> ignore

            let settleTask = waiter.Task
            let boundTask = client.WaitDelay.Delay(bound, boundCts.Token)
            let cancelTask = cancelTcs.Task

            let! winner = Task.WhenAny(settleTask, boundTask, cancelTask)

            // Settlement wins every tie, mirroring StopArbitration.
            if settleTask.IsCompletedSuccessfully then
                try
                    boundCts.Cancel()
                with _ ->
                    ()

                return settleTask.Result
            else
                hub.Cancel(waiter)

                try
                    boundCts.Cancel()
                with _ ->
                    ()

                if Object.ReferenceEquals(winner, cancelTask) || cancelTask.IsCompleted then
                    cancellationToken.ThrowIfCancellationRequested()
                    return raise (OperationCanceledException(cancellationToken))
                elif boundTask.IsFaulted then
                    let inner =
                        match boundTask.Exception with
                        | null -> Exception("The settle-wait delay failed.")
                        | aggregate when aggregate.InnerExceptions.Count > 0 -> aggregate.InnerExceptions[0]
                        | aggregate -> aggregate :> exn

                    return raise inner
                else
                    return
                        raise (
                            DeadlineExceededException(
                                "WaitForSettle",
                                "The settle wait exceeded its bound while the turn kept running."
                            )
                        )
        }

    /// Subscribes to the session's events from the cursor: replays the
    /// journal up to the live position, then yields live publishes
    /// gap-free with no duplicates across the handoff. In Local mode this
    /// is the process-local bus path; in the cluster modes the facade-wired
    /// router streams from the owning session entity through the shard
    /// region with store-replay resume (at-least-once: duplicates
    /// acceptable, gaps are not). Unknown session throws
    /// SessionNotFoundException on the first move.
    /// <param name="client">The session client. Must not be null.</param>
    /// <param name="sessionId">The session to subscribe to.</param>
    /// <param name="fromSequence">The exclusive cursor: replay events with a sequence strictly greater than it; 0 replays from the journal's first event.</param>
    /// <param name="cancellationToken">Token that abandons the replay and the live wait.</param>
    /// <returns>The replay-then-live event stream.</returns>
    [<Extension>]
    static member Subscribe
        (client: SessionClient, sessionId: SessionId, fromSequence: int64, cancellationToken: CancellationToken)
        : IAsyncEnumerable<SessionEvent> =
        ArgumentNullException.ThrowIfNull(client)

        match client.SubscribeRouter with
        | Some router -> router.Subscribe(client.Tenant, sessionId, fromSequence, cancellationToken)
        | None -> client.EventBus.Subscribe(client.Tenant, sessionId, fromSequence, cancellationToken)

    /// Reads the session's coarse transcript cells derived from its
    /// journaled events.
    /// <param name="client">The session client. Must not be null.</param>
    /// <param name="sessionId">The session whose transcript to read.</param>
    /// <param name="cancellationToken">Abandons the read.</param>
    /// <returns>The transcript cells.</returns>
    /// <exception cref="T:Legate.SessionNotFoundException">The session id does not exist.</exception>
    [<Extension>]
    static member ReadTranscriptAsync
        (client: SessionClient, sessionId: SessionId, cancellationToken: CancellationToken)
        : Task<IReadOnlyList<SessionCell>> =
        ArgumentNullException.ThrowIfNull(client)

        Transcripts.readTranscript
            client.EventBus.EventStore
            client.Tenant
            sessionId
            (ReadTranscriptOptions())
            100
            cancellationToken

    /// Reads one bounded page of the session's journal: the events with a
    /// sequence strictly greater than the cursor, in sequence order.
    /// <param name="client">The session client. Must not be null.</param>
    /// <param name="sessionId">The session whose journal to read.</param>
    /// <param name="fromSequence">The exclusive cursor: read events with a sequence strictly greater than it; 0 reads from the journal's first event.</param>
    /// <param name="limit">The maximum number of events to return; must be positive.</param>
    /// <param name="cancellationToken">Abandons the read.</param>
    /// <returns>The journaled events, in sequence order; empty at end of stream.</returns>
    /// <exception cref="T:Legate.SessionNotFoundException">The session id does not exist.</exception>
    [<Extension>]
    static member ReadEventsAsync
        (
            client: SessionClient,
            sessionId: SessionId,
            fromSequence: int64,
            limit: int,
            cancellationToken: CancellationToken
        ) : Task<IReadOnlyList<SessionEvent>> =
        ArgumentNullException.ThrowIfNull(client)
        client.EventBus.ReadEventsAsync(client.Tenant, sessionId, fromSequence, limit, cancellationToken)

// ──────────────────────────────────────────────────────────────────────────
// Listing

/// Filter and paging options for
/// <see cref="M:Legate.SessionClientOperations.ListSessionsAsync*" />. A
/// plain class with mutable properties and defaults, so C# object
/// initialisers work and absent configuration keeps the defaults.
[<Sealed>]
type SessionListOptions() =
    let mutable continuation: string | null = null

    /// The lifecycle state to filter by, or empty for every state.
    /// Default empty.
    member val State: Nullable<SessionState> = Nullable() with get, set

    /// The agent to filter by, or empty for every agent. Default empty.
    member val AgentId: Nullable<AgentId> = Nullable() with get, set

    /// Only sessions created at or after this instant, or empty for no
    /// lower bound. Default empty.
    member val CreatedFrom: Nullable<DateTimeOffset> = Nullable() with get, set

    /// Only sessions created at or before this instant, or empty for no
    /// upper bound. Default empty.
    member val CreatedTo: Nullable<DateTimeOffset> = Nullable() with get, set

    /// The maximum number of sessions on the page. Defaults to 50 and
    /// clamps to 200; must be positive.
    member val PageSize: int = 50 with get, set

    /// The continuation token from the previous page, or null for the
    /// first page. Callers pass it verbatim. Default null. An explicit
    /// property (not an auto-property) so the setter keeps the nullable
    /// annotation F# callers rely on.
    member _.Continuation
        with get (): string | null = continuation
        and set (value: string | null) = continuation <- value

/// Listing operations on <see cref="T:Legate.SessionClient" />.
[<Sealed; AbstractClass; Extension>]
type SessionClientListingOperations =

    /// The largest page <c>ListSessionsAsync</c> asks the store for: the
    /// store pages stay bounded no matter what the host sets.
    static member MaxPageSize = 200

    /// The page size <c>ListSessionsAsync</c> asks for when the host leaves
    /// it unset.
    static member DefaultPageSize = 50

    /// Lists the client's tenant sessions newest-first with the
    /// <see cref="T:Legate.SessionListOptions" /> filters, delegating to
    /// the store: filtering stays store-side so paging walks the filtered
    /// set. The continuation is opaque and store-owned; callers pass it
    /// verbatim into the next call.
    /// <param name="client">The session client. Must not be null.</param>
    /// <param name="options">The filters and paging, or null for the defaults.</param>
    /// <param name="cancellationToken">Abandons the list.</param>
    /// <returns>One bounded page of sessions.</returns>
    /// <exception cref="T:System.ArgumentOutOfRangeException">The page size is not positive.</exception>
    [<Extension>]
    static member ListSessionsAsync
        (client: SessionClient, options: SessionListOptions | null, cancellationToken: CancellationToken)
        : Task<SessionPage> =
        ArgumentNullException.ThrowIfNull(client)

        let effective =
            match options with
            | null -> SessionListOptions()
            | present -> present

        if effective.PageSize <= 0 then
            raise (ArgumentOutOfRangeException(nameof options, "The session list page size must be positive."))

        let take = min effective.PageSize SessionClientListingOperations.MaxPageSize

        client.Store.ListSessions(
            client.Tenant,
            effective.State,
            effective.AgentId,
            effective.CreatedFrom,
            effective.CreatedTo,
            take,
            effective.Continuation,
            cancellationToken
        )

// ──────────────────────────────────────────────────────────────────────────
// Cross-node subscriptions

/// The cluster-mode Subscribe router (issue 133): streams a session's
/// events from the owning entity through the shard region with
/// store-replay resume. Local mode keeps the process-local bus path
/// (see SessionClientOperations.Subscribe); the facade wires this router
/// only in StaticSeeds and Kubernetes modes. Delivery is at-least-once:
/// the stream resumes from its last sequence cursor on entity move or
/// node restart, sequence numbers let the consumer detect gaps, and
/// redelivery covers them. Unknown session and expired journal surface
/// as their typed exceptions; subscriber, cache, and payload bounds bind
/// from the host options while the global Cluster:MaxWirePayloadBytes
/// caps every manifest on top.
module internal ClusterSubscriptions =

    /// How long one entity batch Ask waits before the router rebinds and
    /// falls back to a direct store page. Warm entities answer in
    /// milliseconds; the bound only fires when the owner is gone.
    let askTimeout = TimeSpan.FromSeconds 15.0

    /// How long the live tail waits between end-of-stream polls. Short
    /// enough that appended events surface promptly, long enough to avoid
    /// hot-polling the owner; the caller's cancellation abandons the wait.
    let pollDelay = TimeSpan.FromMilliseconds 50.0

    /// The maximum time spent acknowledging an unsubscribe at the owning
    /// entity. A refusal is a typed scope failure; transport loss is the
    /// only case that degrades to best-effort detach.
    let detachTimeout = TimeSpan.FromSeconds 5.0

    /// One cluster-mode subscription: entity batches with a direct store
    /// fallback, resumed from the last delivered sequence.
    type private ClusterSubscribeEnumerator
        (
            resolver: ISessionResolver,
            eventStore: ISessionEventStore,
            options: SessionSubscriptionOptions,
            tenant: TenantId,
            sessionId: SessionId,
            fromSequence: int64,
            subscribeToken: CancellationToken,
            enumeratorToken: CancellationToken
        ) =

        do ArgumentNullException.ThrowIfNull(resolver)
        do ArgumentNullException.ThrowIfNull(eventStore)
        do ArgumentNullException.ThrowIfNull(options)

        let token = Guid.NewGuid().ToString("N")
        let queue = Queue<SessionEvent>()
        let mutable resumeCursor = fromSequence
        let mutable lastDelivered = fromSequence
        let mutable current: SessionEvent = Unchecked.defaultof<SessionEvent>
        let mutable finished = false
        let mutable detached = false
        let detachGate = obj ()
        let mutable detachTask: Task option = None
        let mutable proxyOpt: IActorRef option = None

        let detachBestEffort () : Task =
            lock detachGate (fun () ->
                match detachTask with
                | Some pending -> pending
                | None ->
                    detached <- true

                    let pending =
                        task {
                            try
                                match proxyOpt with
                                | Some proxy ->
                                    let unsubscribe: CrossNodeSubscriptions.CrossNodeUnsubscribe =
                                        {
                                            Tenant = tenant
                                            SessionId = sessionId
                                            SubscriberToken = token
                                        }

                                    use detachCts = new CancellationTokenSource(detachTimeout)
                                    let! reply = proxy.Ask<obj>(unsubscribe :> obj, detachCts.Token)

                                    match reply with
                                    | :? SessionRouteAccepted -> ()
                                    | :? Exception as error -> raise error
                                    | _ ->
                                        raise (
                                            InvalidOperationException("The owning entity did not acknowledge detach.")
                                        )
                                | None -> ()
                            with
                            | :? SessionScopeRejectedException as refusal -> return raise refusal
                            | _ ->
                                // Disposal is best effort when the owner has
                                // already moved or stopped; the single-flight
                                // task still prevents repeated detach effects.
                                ()
                        }

                    detachTask <- Some pending
                    pending)

        let throwForReply (reply: obj) : unit =
            match reply with
            | :? CrossNodeSubscriptions.CrossNodeEventBatch -> ()
            | :? OperationCanceledException as canceled -> raise canceled
            | :? Exception as error -> raise error
            | _ ->
                raise (InvalidOperationException("The owning entity answered the subscription with an unknown reply."))

        let enqueueBatch (batch: CrossNodeSubscriptions.CrossNodeEventBatch) : unit =
            if batch.Tenant <> tenant || batch.SessionId <> sessionId then
                raise (SessionScopeRejectedException(SessionScopeRejectionReason.ResponseMismatch))

            if
                String.IsNullOrWhiteSpace batch.SubscriberToken
                || batch.SubscriberToken <> token
            then
                raise (SessionScopeRejectedException(SessionScopeRejectionReason.ResponseMismatch))

            if isNull (box batch.Events) then
                raise (SessionScopeRejectedException(SessionScopeRejectionReason.ResponseMismatch))

            for evt in batch.Events do
                if isNull (box evt) || evt.SessionId <> sessionId then
                    raise (SessionScopeRejectedException(SessionScopeRejectionReason.ResponseMismatch))
                elif evt.Sequence.HasValue && evt.Sequence.Value <= lastDelivered then
                    ()
                else
                    queue.Enqueue(evt)

                    if evt.Sequence.HasValue && evt.Sequence.Value > resumeCursor then
                        resumeCursor <- evt.Sequence.Value

            if batch.NextCursor > resumeCursor then
                resumeCursor <- batch.NextCursor

        let applyReplayPage (page: EventReplayPage) : unit =
            let events =
                if isNull (box page.Events) then
                    Array.Empty<SessionEvent>()
                else
                    page.Events |> Seq.filter (fun evt -> not (isNull (box evt))) |> Array.ofSeq

            for evt in events do
                if evt.Sequence.HasValue && evt.Sequence.Value <= lastDelivered then
                    ()
                else
                    let observed = CrossNodeSubscriptions.estimateEventBytes evt

                    if observed > options.MaxEventPayloadBytes then
                        raise (
                            EventLimitExceededException(
                                "perEventBytes",
                                int64 options.MaxEventPayloadBytes,
                                int64 observed,
                                sprintf
                                    "The event at sequence %d in session %O is %d bytes, above the cross-node bound."
                                    (if evt.Sequence.HasValue then
                                         evt.Sequence.Value
                                     else
                                         resumeCursor)
                                    sessionId
                                    observed
                            )
                        )

                    queue.Enqueue(evt)

                    if evt.Sequence.HasValue && evt.Sequence.Value > resumeCursor then
                        resumeCursor <- evt.Sequence.Value

            if page.NextCursor.HasValue && page.NextCursor.Value > resumeCursor then
                resumeCursor <- page.NextCursor.Value

        let throwForReplayOutcome (outcome: obj) : unit =
            match outcome with
            | :? EventReplayPage as page when not (isNull (box page)) -> applyReplayPage page
            | :? EventReplayEndOfStream -> ()
            | :? EventReplayUnknownSession as unknown when not (isNull (box unknown)) ->
                raise (
                    SessionNotFoundException(
                        unknown.SessionId,
                        sprintf "No session %O exists in tenant %O." unknown.SessionId tenant
                    )
                )
            | :? EventReplayJournalExpired as expired when not (isNull (box expired)) ->
                raise (
                    SessionJournalExpiredException(
                        expired.SessionId,
                        sprintf "The journal for session %O is gone." expired.SessionId
                    )
                )
            | _ -> raise (InvalidOperationException("The event store returned an unknown replay outcome."))

        let fallbackPageAsync (linkedCt: CancellationToken) : Task<unit> =
            task {
                let! outcome =
                    eventStore.Replay(tenant, sessionId, resumeCursor, CrossNodeSubscriptions.MaxBatchEvents, linkedCt)

                if isNull (box outcome) then
                    raise (InvalidOperationException("The event store returned null."))
                else
                    throwForReplayOutcome outcome
            }

        member _.Current = current

        member _.MoveNextAsync() : ValueTask<bool> =
            ValueTask<bool>(
                task {
                    use linkedCts =
                        CancellationTokenSource.CreateLinkedTokenSource(subscribeToken, enumeratorToken)

                    if finished then
                        return false
                    else
                        let mutable step: bool option = None

                        while step.IsNone do
                            if queue.Count > 0 then
                                let next = queue.Dequeue()
                                current <- next

                                if next.Sequence.HasValue && next.Sequence.Value > lastDelivered then
                                    lastDelivered <- next.Sequence.Value

                                if next :? SessionClosedEvent then
                                    finished <- true
                                    do! detachBestEffort ()

                                step <- Some true
                            else
                                let request: CrossNodeSubscriptions.CrossNodeSubscribeRequest =
                                    {
                                        Tenant = tenant
                                        SessionId = sessionId
                                        FromSequence = resumeCursor
                                        SubscriberToken = token
                                    }

                                let! batchOpt =
                                    task {
                                        try
                                            let! proxy =
                                                match proxyOpt with
                                                | Some live -> Task.FromResult live
                                                | None ->
                                                    task {
                                                        let! resolved =
                                                            resolver.ResolveSessionAsync(
                                                                SessionAddress(tenant, sessionId),
                                                                linkedCts.Token
                                                            )

                                                        proxyOpt <- Some resolved
                                                        return resolved
                                                    }

                                            use askCts =
                                                CancellationTokenSource.CreateLinkedTokenSource(
                                                    linkedCts.Token,
                                                    (new CancellationTokenSource(askTimeout)).Token
                                                )

                                            let! reply = proxy.Ask<obj>(request :> obj, askCts.Token)

                                            match reply with
                                            | :? CrossNodeSubscriptions.CrossNodeEventBatch as batch ->
                                                enqueueBatch batch
                                                return Some batch
                                            | _ ->
                                                throwForReply reply
                                                return None
                                        with
                                        | :? OperationCanceledException as canceled ->
                                            if linkedCts.Token.IsCancellationRequested then
                                                do! detachBestEffort ()
                                                return raise canceled
                                            else
                                                proxyOpt <- None
                                                do! fallbackPageAsync linkedCts.Token
                                                return None
                                        | :? SessionScopeRejectedException
                                        | :? SessionNotFoundException
                                        | :? SessionJournalExpiredException
                                        | :? SessionSubscriptionLimitExceededException
                                        | :? EventLimitExceededException as fatal ->
                                            do! detachBestEffort ()
                                            return raise fatal
                                        | _ ->
                                            proxyOpt <- None
                                            do! fallbackPageAsync linkedCts.Token
                                            return None
                                    }

                                match batchOpt with
                                | Some batch when batch.Events.Count > 0 || not batch.EndOfStream -> ()
                                | _ ->
                                    try
                                        do! Task.Delay(pollDelay, linkedCts.Token)
                                    with :? OperationCanceledException as canceled ->
                                        do! detachBestEffort ()
                                        raise canceled

                        match step with
                        | Some value -> return value
                        | None -> return false
                }
            )

        member _.DisposeAsync() : ValueTask = ValueTask(detachBestEffort ())

        interface IAsyncEnumerator<SessionEvent> with
            member this.Current = this.Current
            member this.MoveNextAsync() = this.MoveNextAsync()
            member this.DisposeAsync() = this.DisposeAsync()

    /// One cluster-mode subscription enumeration over entity batches with
    /// a direct store fallback.
    type private ClusterSubscribeEnumerable
        (
            resolver: ISessionResolver,
            eventStore: ISessionEventStore,
            options: SessionSubscriptionOptions,
            tenant: TenantId,
            sessionId: SessionId,
            fromSequence: int64,
            subscribeToken: CancellationToken
        ) =

        interface IAsyncEnumerable<SessionEvent> with
            member _.GetAsyncEnumerator(cancellationToken: CancellationToken) : IAsyncEnumerator<SessionEvent> =
                upcast
                    ClusterSubscribeEnumerator(
                        resolver,
                        eventStore,
                        options,
                        tenant,
                        sessionId,
                        fromSequence,
                        subscribeToken,
                        cancellationToken
                    )

    /// The cluster-mode Subscribe router: entity batches through the
    /// shard region with store-replay resume. Internal so no Akka type
    /// ever crosses the public API.
    type ClusterSubscribeRouter
        internal (resolver: ISessionResolver, eventStore: ISessionEventStore, options: SessionSubscriptionOptions) =

        do ArgumentNullException.ThrowIfNull(resolver)
        do ArgumentNullException.ThrowIfNull(eventStore)
        do ArgumentNullException.ThrowIfNull(options)

        do
            let violation = options.Validate()

            if not (isNull (box violation)) then
                raise (ArgumentException(violation, nameof options))

        interface ISubscribeRouter with
            member _.Subscribe(tenant, sessionId, fromSequence, cancellationToken) =
                if fromSequence < 0L then
                    raise (ArgumentOutOfRangeException(nameof fromSequence, "The cursor must not be negative."))

                upcast
                    ClusterSubscribeEnumerable(
                        resolver,
                        eventStore,
                        options,
                        tenant,
                        sessionId,
                        fromSequence,
                        cancellationToken
                    )

// ──────────────────────────────────────────────────────────────────────────
// Wiring

/// Builds the facade client from the container and wires the session
/// router to the suspendable behavior for hosts that opted in with an
/// IChatClient. Internal: hosts resolve SessionClient from DI and call
/// the SessionClientOperations extensions; nothing here crosses the public
/// API.
type internal SessionModelClients(resolveClient: Func<ModelReference, IChatClient> | null) =
    let clients =
        System.Collections.Concurrent.ConcurrentDictionary<string, Lazy<IChatClient>>(StringComparer.Ordinal)

    member _.Resolve(model: ModelReference, fallback: IChatClient) : IChatClient =
        match resolveClient with
        | null -> fallback
        | resolve ->
            clients
                .GetOrAdd(
                    model.Value,
                    fun _ ->
                        lazy
                            (let client = resolve.Invoke(model)

                             if isNull (box client) then
                                 raise (InvalidOperationException("The model-aware chat client factory returned null."))

                             client)
                )
                .Value

    interface IDisposable with
        member _.Dispose() =
            for client in clients.Values do
                if client.IsValueCreated then
                    client.Value.Dispose()

module internal SessionClientWiring =

    /// Catalog reads belong to the attempt's cancellation scope, including
    /// stores that do not complete promptly after receiving cancellation.
    let agentsForEntryAsync (agents: IAgentStore | null) tenant (token: CancellationToken) =
        match agents with
        | null -> Task.FromResult(Array.empty<Agent> :> IReadOnlyList<Agent>)
        | agents -> agents.ListAgents(tenant, token).WaitAsync(token)

    /// Resolves the stored session agent's model for each new attempt. Hosts
    /// without provider registrations keep their explicitly supplied client.
    let modelForEntryAsync (store: ISessionStore) (agents: IAgentStore | null) tenant (entry: InboxEntry) ct =
        task {
            match agents with
            | null -> return None
            | catalog ->
                let! session = store.GetSession(tenant, entry.SessionId, ct)

                match session with
                | null -> return None
                | session ->
                    let! agent = catalog.GetAgent(tenant, session.AgentId, ct)
                    return Option.ofObj agent |> Option.map (fun agent -> agent.Model)
        }

    /// Resolves one entry's tool set and turn budget: the session row
    /// carries the agent (for the tool context) and the options snapshot
    /// (for the budget through TurnLoop.resolveBudget with the session
    /// AskUser winning over the configured default). Tool names validate
    /// on assembly and first registration wins across sources.
    /// <param name="store">The durable store session rows persist through.</param>
    /// <param name="tenant">The tenant facade-driven sessions belong to.</param>
    /// <param name="sources">The registered tool sources, in registration order.</param>
    /// <param name="turns">The configured turn defaults.</param>
    /// <param name="askUserDefault">The configured ask-user default, or null.</param>
    /// <returns>The per-entry tool set and budget resolver the runner calls.</returns>
    let resolveInputs
        (store: ISessionStore)
        (tenant: TenantId)
        (sources: IToolSource list)
        (turns: TurnsOptions)
        (askUserDefault: AskUserOptions | null)
        : (InboxEntry -> IReadOnlyDictionary<string, AITool> * TurnLoop.TurnLoopOptions) =
        ArgumentNullException.ThrowIfNull(store)
        ArgumentNullException.ThrowIfNull(turns)

        fun entry ->
            let stored =
                store.GetSession(tenant, entry.SessionId, CancellationToken.None).GetAwaiter().GetResult()

            let session =
                match stored with
                | null ->
                    raise (
                        InvalidOperationException(
                            sprintf "The session %O has no stored row for its running turn." entry.SessionId
                        )
                    )
                | live -> live

            let sessionOptions =
                match box session.Options with
                | null -> SessionOptions()
                | _ -> session.Options

            let context =
                {
                    Tenant = tenant
                    AgentId = session.AgentId
                    SessionId = entry.SessionId
                }

            let table = Dictionary<string, AITool>(StringComparer.Ordinal)

            for source in sources do
                if not (isNull (box source)) then
                    let tools = source.GetTools(context).GetAwaiter().GetResult()

                    if not (isNull (box tools)) then
                        for tool in tools do
                            if not (isNull (box tool)) then
                                if String.IsNullOrWhiteSpace tool.Name then
                                    raise (
                                        ArgumentException(
                                            "A tool source returned a tool with no name: every tool name must match [a-zA-Z0-9_-]{1,128}.",
                                            "tools"
                                        )
                                    )
                                elif not (ToolNameRules.TryValidate tool.Name) then
                                    raise (
                                        ArgumentException(
                                            sprintf
                                                "A tool source returned a non-conforming tool name '%s': every tool name must match [a-zA-Z0-9_-]{1,128}."
                                                tool.Name,
                                            "tools"
                                        )
                                    )
                                elif not (table.ContainsKey tool.Name) then
                                    table[tool.Name] <- tool

            let budget = TurnLoop.resolveBudget sessionOptions turns

            let ask =
                Option.ofObj sessionOptions.AskUser
                |> Option.orElse (Option.ofObj askUserDefault)

            (table :> IReadOnlyDictionary<string, AITool>, { budget with AskUser = ask })

    /// Builds the per-entry composed system prompt hook from the session's
    /// host instruction files: read fresh every turn and joined through
    /// the shared composer. Package instructions, the agent prompt, and
    /// the skills block stay out: the agent-catalog work owns them. Never
    /// fails a turn: every failure resolves to no system message.
    /// <param name="store">The durable store session rows persist through.</param>
    /// <param name="tenant">The tenant facade-driven sessions belong to.</param>
    /// <returns>The hook the runner consults at the package-load step.</returns>
    let systemPromptFor (store: ISessionStore) (tenant: TenantId) : PromptComposition.GetTurnSystemPrompt =
        ArgumentNullException.ThrowIfNull(store)

        let hook (entry: InboxEntry) (cancellationToken: CancellationToken) : Task<string | null> =
            task {
                try
                    let! session = store.GetSession(tenant, entry.SessionId, cancellationToken)

                    let paths =
                        match session with
                        | null -> null
                        | live ->
                            match box live.Options with
                            | null -> null
                            | _ -> live.Options.HostInstructionFiles

                    let! texts = PromptComposition.readHostFilesAsync paths cancellationToken
                    let composed = PromptComposition.compose null null null texts

                    let outcome: string | null =
                        if String.IsNullOrWhiteSpace composed then
                            null
                        else
                            composed

                    return outcome
                with _ ->
                    return null
            }

        hook

    /// Parses the compaction session model: the facade DefaultModel, then
    /// the configured Llm:DefaultModel, then the agent-file default.
    /// <param name="clientOptions">The facade options.</param>
    /// <param name="legateOptions">The configured runtime options.</param>
    /// <returns>The model the on-demand compaction thresholds against.</returns>
    let sessionModelOf (clientOptions: SessionClientOptions) (legateOptions: LegateOptions) : ModelReference =
        ArgumentNullException.ThrowIfNull(clientOptions)
        ArgumentNullException.ThrowIfNull(legateOptions)

        let fromFacade =
            match clientOptions.DefaultModel with
            | null -> null
            | raw when String.IsNullOrWhiteSpace raw -> null
            | raw -> raw.Trim()

        let fromConfig =
            match box legateOptions.Llm with
            | null -> null
            | _ ->
                match legateOptions.Llm.DefaultModel with
                | null -> null
                | raw when String.IsNullOrWhiteSpace raw -> null
                | raw -> raw.Trim()

        match fromFacade with
        | null ->
            match fromConfig with
            | null -> AgentFileParser.defaultModel
            | model -> ModelReference.Parse(model)
        | model -> ModelReference.Parse(model)

    /// Maps the host Sessions options onto the runtime subscription
    /// bounds the bus, the entity hubs, and the cluster router enforce.
    /// Validation runs at the bus/router constructors; this only carries
    /// the configured values across the options graph.
    /// <param name="sessions">The configured sessions options. Must not be null.</param>
    /// <returns>The runtime subscription options.</returns>
    let subscriptionOptionsOf (sessions: SessionsOptions) : SessionSubscriptionOptions =
        ArgumentNullException.ThrowIfNull(sessions)

        let options = SessionSubscriptionOptions()
        options.MaxSubscribersPerSession <- sessions.MaxSubscribersPerSession
        options.PerSubscriberBufferSize <- sessions.PerSubscriberBufferSize
        options.ReplayCacheSize <- sessions.SubscriptionReplayCacheSize
        options.MaxEventPayloadBytes <- sessions.SubscriptionMaxEventPayloadBytes
        options

    /// Resolves the completion-era reader the entity factory closes over
    /// (issue 289): the registered gate's reader, or the pre-era reader
    /// when no gate (or no reader) is registered. Never null and never
    /// throws for a missing registration.
    /// <param name="provider">The container to resolve from. Must not be null.</param>
    /// <returns>The era reader the probe consults.</returns>
    let eraReaderOf (provider: IServiceProvider) : CompletionEra.CompletionEraReader =
        ArgumentNullException.ThrowIfNull(provider)

        match provider.GetService<CompletionEra.CompletionEraGate>() with
        | null -> CompletionEra.preEraGate.Reader
        | gate when isNull (box gate.Reader) -> CompletionEra.preEraGate.Reader
        | gate -> gate.Reader

    /// Resolves the completion-era marker Open and Fork call (issue 289):
    /// the registered gate's marker, or None when no gate (or no marker)
    /// is registered. A missing gate reads pre-era quiet.
    /// <param name="provider">The container to resolve from. Must not be null.</param>
    /// <returns>The era marker, or None.</returns>
    let eraMarkerOf (provider: IServiceProvider) : CompletionEra.CompletionEraMarker option =
        ArgumentNullException.ThrowIfNull(provider)

        match provider.GetService<CompletionEra.CompletionEraGate>() with
        | null -> None
        | gate when isNull (box gate.Marker) -> None
        | gate -> Some gate.Marker

    /// Builds the client from the container: resolves the stores, bounds,
    /// and hosted actor system, validates the facade options, opts into
    /// suspendable children when an IChatClient is registered, and returns
    /// the client resolving through the session router.
    /// <param name="provider">The container to build from. Must not be null.</param>
    /// <returns>The DI-owned session client.</returns>
    let assembleContext
        (provider: IServiceProvider)
        (resolver: ISessionResolver)
        (nodeMode: ClusterMode)
        (subscriptionLifetime: SessionSubscriptionLifetime)
        (workTracker: ExecutionWorkTracker)
        : SessionExecutionContext =
        ArgumentNullException.ThrowIfNull(provider)
        ArgumentNullException.ThrowIfNull(subscriptionLifetime)
        ArgumentNullException.ThrowIfNull(workTracker)

        let store = provider.GetRequiredService<ISessionStore>()
        let routes = provider.GetRequiredService<CompletionDestinations>()
        let bus = provider.GetRequiredService<SessionEventBus>()

        let legateOptions =
            match provider.GetService<IOptions<LegateOptions>>() with
            | null -> LegateOptions()
            | options when isNull (box options.Value) -> LegateOptions()
            | options ->
                System.Text.Json.JsonSerializer.Deserialize<LegateOptions>(
                    System.Text.Json.JsonSerializer.Serialize(options.Value)
                )
                |> function
                    | null -> invalidOp "LegateOptions snapshot was null."
                    | snapshot -> snapshot

        let clientOptions =
            match provider.GetService<SessionClientOptions>() with
            | null -> SessionClientOptions()
            | options ->
                System.Text.Json.JsonSerializer.Deserialize<SessionClientOptions>(
                    System.Text.Json.JsonSerializer.Serialize(options)
                )
                |> function
                    | null -> invalidOp "SessionClientOptions snapshot was null."
                    | snapshot -> snapshot

        match clientOptions.Validate() with
        | null -> ()
        | violation -> raise (InvalidOperationException($"Invalid SessionClientOptions: %s{violation}"))

        let delay =
            match provider.GetService<ILlmDelay>() with
            | null -> SystemLlmDelay(TimeProvider.System) :> ILlmDelay
            | seam -> seam

        // Production ownership clock (issue 375): the DI TimeProvider with
        // a System fallback, mirroring the ILlmDelay precedent above. The
        // heartbeat reuses the delay seam (virtual-time friendly) and the
        // Sessions snapshot for its renewal tuning.
        let clock =
            match provider.GetService<TimeProvider>() with
            | null -> TimeProvider.System
            | resolved -> resolved

        let sessionsSnapshot =
            match box legateOptions.Sessions with
            | null -> SessionsOptions()
            | _ -> legateOptions.Sessions

        // Renewal tuning derives from Sessions (60 s lease renewed every
        // 15 s by default, under-half bound re-checked at use), never from
        // SessionClientOptions.LeaseDuration, which stays prime-only.
        let heartbeatOptions = ClaimHeartbeat.fromSessions sessionsSnapshot

        // A complete execution binding always owns a chat client.  Startup
        // validation enforces this before actor creation; resolving it here
        // keeps the production factory free of an identity-only fallback.
        let titleClient: IChatClient = provider.GetRequiredService<IChatClient>()

        let mutable spawnContext =
            Unchecked.defaultof<SessionAddress -> IActorContext -> string -> IActorRef>

        match titleClient with
        | client ->
            let sources =
                provider.GetServices<IToolSource>()
                |> Seq.filter (fun source -> not (isNull (box source)))
                |> List.ofSeq

            let policy = SessionPermissions.resolvePolicy provider

            let modelClients = provider.GetRequiredService<SessionModelClients>()
            let agentStore = provider.GetService<IAgentStore>()

            let baseInputs =
                resolveInputs store clientOptions.Tenant sources legateOptions.Turns legateOptions.AskUser

            let inputs (available: IReadOnlyList<Agent>) entry =
                let tools, options = baseInputs entry

                match agentStore with
                | null -> tools, options
                | agents ->
                    let nested =
                        available
                        |> Seq.filter (fun agent -> agent.Enabled && not (String.IsNullOrWhiteSpace agent.Description))
                        |> Seq.toArray
                        :> IReadOnlyList<Agent>

                    if nested.Count = 0 then
                        tools, options
                    else
                        let table = Dictionary<string, AITool>(tools, StringComparer.Ordinal)
                        table[TaskTool.ToolName] <- TaskTool.Create(TaskTool.DescriptionFor nested)

                        let deps: TaskRunner.TaskHookDeps =
                            {
                                Store = agents
                                Tenant = clientOptions.Tenant
                                Config = legateOptions.Sessions.SubAgents
                                Depth = 0
                                ResolveClient = Some(fun model -> modelClients.Resolve(model, client))
                                Journal = None
                            }

                        table :> IReadOnlyDictionary<string, AITool>,
                        { options with
                            TaskNested = Some(TaskRunner.createHook deps)
                        }

            let runner: SessionActor.SuspendableRunner =
                fun entry attempt allowed cursor reply seed token started usage skill turnId ->
                    workTracker.Track(fun () ->
                        task {
                            let! model = modelForEntryAsync store agentStore clientOptions.Tenant entry token
                            let! available = agentsForEntryAsync agentStore clientOptions.Tenant token

                            let selected =
                                model
                                |> Option.map (fun model -> modelClients.Resolve(model, client))
                                |> Option.defaultValue client

                            let run =
                                SessionPermissions.createRunner
                                    selected
                                    store
                                    clientOptions.Tenant
                                    (inputs available)
                                    delay
                                    policy
                                    (Some(systemPromptFor store clientOptions.Tenant))

                            return! run entry attempt allowed cursor reply seed token started usage skill turnId
                        })

            let eventStore = bus.EventStore
            let model = sessionModelOf clientOptions legateOptions

            let catalog = provider.GetService<ILlmModelCatalog>()
            let observer = provider.GetService<IUsageObserver>()
            let modelPolicy = provider.GetService<IModelPolicy>()

            let compactFor (_sessionId: SessionId) (journalToken: string) : CompactDeps option =
                Some(
                    {
                        Llm = legateOptions.Llm
                        ReservedBufferTokens = legateOptions.Pruning.ReservedBufferTokens
                        SessionModel = model
                        Catalog = catalog
                        Client = client
                        Observer = observer
                        Policy = modelPolicy
                        EventStore = eventStore
                        JournalToken = journalToken
                        Force = Compaction.CompactForce()
                    }
                )

            let entityFactory =
                SessionActor.spawnSuspendFactoryRouted
                    (Some routes)
                    store
                    clientOptions.Tenant
                    eventStore
                    delay
                    legateOptions.Permissions.AskTimeout
                    clientOptions.ClaimOwner
                    clientOptions.LeaseDuration
                    runner
                    compactFor
                    agentStore
                    (eraReaderOf provider)
                    clock
                    (Some heartbeatOptions)

            spawnContext <-
                fun address context name ->
                    if address.Tenant <> clientOptions.Tenant then
                        raise (SessionScopeRejectedException(SessionScopeRejectionReason.AddressMismatch))

                    entityFactory address.SessionId.Value context name

        let resolve (sessionId: SessionId) (cancellationToken: CancellationToken) : Task<IActorRef> =
            resolver.ResolveSessionAsync(SessionAddress(clientOptions.Tenant, sessionId), cancellationToken)

        // The agent catalog SetAgent validates against, or None when the
        // host runs without one: validation is skipped then.
        let agents = Option.ofObj (provider.GetService<IAgentStore>())

        // Auto-title rides the resolved options: off unless the host opts
        // in, the title model falling back to the compaction model and
        // then the session/default resolution, through the registered
        // chat client (no new provider wiring).
        let sessions =
            match box legateOptions.Sessions with
            | null -> SessionsOptions()
            | _ -> legateOptions.Sessions

        let llm =
            match box legateOptions.Llm with
            | null -> LlmOptions()
            | _ -> legateOptions.Llm

        let autoTitle =
            {
                Enabled = sessions.AutoTitle
                TitleModel = sessions.AutoTitleModel
                CompactionModel = llm.Compaction
                FacadeDefaultModel = clientOptions.DefaultModel
                LlmDefaultModel = llm.DefaultModel
                FallbackModel = AgentFileParser.defaultModel
                ChatClient = titleClient
                Logger = provider.GetService<ILogger<SessionClient>>()
            }

        // Cross-node subscriptions (issue 133): subscriber, cache, and
        // payload bounds bind from the host Sessions options into the
        // runtime subscription options; the cluster service takes the
        // journal and bounds so its session entities serve remote
        // subscribers, and the client routes Subscribe through the owning
        // entity in the cluster modes. Local mode keeps the bus path:
        // SubscribeRouter stays None and Subscribe delegates to the bus.
        let subscriptionOptions = subscriptionOptionsOf sessions

        // Completion era (issue 289): the marker Open and Fork call after
        // a successful CreateSession, captured here like the event store
        // and delay above. Absent (or marker-less) gate registration
        // leaves the client unmarked: pre-era quiet.
        let marker = eraMarkerOf provider

        let client =
            lazy
                (let built =
                    new SessionClient(
                        store,
                        clientOptions.Tenant,
                        resolve,
                        bus,
                        clientOptions.DefaultWaitBound,
                        delay,
                        agents
                    )

                 built.AutoTitle <- Some autoTitle
                 built.CompletionEra <- marker
                 built.CompletionDestinations <- Some routes

                 built.SubscribeRouter <-
                     Some(
                         { new ISubscribeRouter with
                             member _.Subscribe(tenant, sessionId, fromSequence, token) =
                                 subscriptionLifetime.Wrap(
                                     token,
                                     fun linkedToken -> bus.Subscribe(tenant, sessionId, fromSequence, linkedToken)
                                 )
                         }
                     )

                 match nodeMode with
                 | ClusterMode.StaticSeeds
                 | ClusterMode.Kubernetes ->
                     built.SubscribeRouter <-
                         Some(
                             { new ISubscribeRouter with
                                 member _.Subscribe(tenant, sessionId, fromSequence, token) =
                                     subscriptionLifetime.Wrap(
                                         token,
                                         fun linkedToken ->
                                             (ClusterSubscriptions.ClusterSubscribeRouter(
                                                 resolver,
                                                 bus.EventStore,
                                                 subscriptionOptions
                                             )
                                             :> ISubscribeRouter)
                                                 .Subscribe(tenant, sessionId, fromSequence, linkedToken)
                                     )
                             }
                         )
                 | _ -> ()

                 built :> obj)

        {
            Tenant = clientOptions.Tenant
            Store = store
            EventStore = bus.EventStore
            SubscriptionOptions = subscriptionOptions
            SubscriptionLifetime = subscriptionLifetime
            WorkTracker = workTracker
            Spawn = spawnContext
            Client = client
        }

    let buildClient (provider: IServiceProvider) : SessionClient =
        let contexts = provider.GetRequiredService<ISessionHostContexts>()
        // Facade construction is read-only. Initialization belongs to the
        // hosted startup lifecycle; resolving this service must not assemble
        // providers or start actor infrastructure as a lazy side effect.
        contexts.Get(contexts.DefaultTenant).Client.Value :?> SessionClient

/// Registers the session client facade: the options default, the event
/// bus over the durable journal, and the DI-owned client with its router
/// wiring. Host registrations win (TryAdd): a host-owned SessionClient,
/// bus, or options replaces the facade default.
module internal SessionClientRegistration =

    /// Registers the facade on the container.
    /// <param name="services">The container to add the facade to.</param>
    let register (services: IServiceCollection) : unit =
        ArgumentNullException.ThrowIfNull(services)

        services.TryAddSingleton<SessionModelClients>(
            Func<IServiceProvider, SessionModelClients>(fun provider ->
                new SessionModelClients(provider.GetService<Func<ModelReference, IChatClient>>()))
        )
        |> ignore

        services.TryAddSingleton(SessionClientOptions()) |> ignore

        services.TryAddSingleton<SessionEventBus>(
            Func<IServiceProvider, SessionEventBus>(fun provider ->
                let eventStore = provider.GetRequiredService<ISessionEventStore>()
                let logger = provider.GetService<ILogger<SessionEventBus>>()

                let sessions =
                    match provider.GetService<IOptions<LegateOptions>>() with
                    | null -> SessionsOptions()
                    | options when isNull (box options.Value) -> SessionsOptions()
                    | options ->
                        match box options.Value.Sessions with
                        | null -> SessionsOptions()
                        | _ -> options.Value.Sessions

                new SessionEventBus(eventStore, SessionClientWiring.subscriptionOptionsOf sessions, logger))
        )
        |> ignore

        services.TryAddSingleton<SessionClient>(
            Func<IServiceProvider, SessionClient>(fun provider -> SessionClientWiring.buildClient provider)
        )
        |> ignore
