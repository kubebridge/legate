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

                match Option.ofObj from.Permissions with
                | Some permissions -> options.Permissions <- permissions
                | None -> ()

                match Option.ofObj from.AskUser with
                | Some askUser -> options.AskUser <- askUser
                | None -> ()

                match Option.ofObj from.CompletionSink with
                | Some completionSink -> options.CompletionSink <- completionSink
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
                | present -> present

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

            let! created = client.Store.CreateSession(tenant, session, cancellationToken)

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
            let! _ = SessionClientOperations.RequireAsync(client, sessionId, cancellationToken)

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
            let! _ = SessionClientOperations.RequireAsync(client, sessionId, cancellationToken)
            let! actor = client.Resolve(sessionId, cancellationToken)
            return! SessionActor.replyAsync client.Store client.Tenant sessionId actor reply cancellationToken
        }

    /// Aborts the turn running in a session under a typed stop cause. Idle
    /// is a no-op, as is WaitingForInput (a suspended turn owns nothing
    /// running to abort). Running records the pending stop: the detached
    /// suspendable turn runs un-cancellable, so the stop wins at its next
    /// report and a second abort keeps the first cause.
    /// <param name="client">The session client. Must not be null.</param>
    /// <param name="sessionId">The session to abort the turn in.</param>
    /// <param name="cause">Which abort-family stop cause wins: ExplicitAbort or HostShutdown.</param>
    /// <param name="reason">Why the turn stops. Must not be null. Never contains secrets or tool arguments.</param>
    /// <param name="cancellationToken">Cancels the abort.</param>
    /// <exception cref="T:Legate.SessionNotFoundException">The session id does not exist.</exception>
    /// <exception cref="T:Legate.InvalidSessionStateException">The session is closed.</exception>
    [<Extension>]
    static member AbortAsync
        (
            client: SessionClient,
            sessionId: SessionId,
            cause: StopCause,
            reason: string,
            cancellationToken: CancellationToken
        ) : Task =
        ArgumentNullException.ThrowIfNull(client)

        if isNull (box reason) then
            raise (ArgumentNullException(nameof reason))

        task {
            let! _ = SessionClientOperations.RequireAsync(client, sessionId, cancellationToken)
            let! actor = client.Resolve(sessionId, cancellationToken)

            let! _ =
                SessionActor.abortSuspendableAsync
                    client.Store
                    client.Tenant
                    sessionId
                    actor
                    cause
                    reason
                    cancellationToken

            ()
        }

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
            let! _ = SessionClientOperations.RequireAsync(client, sessionId, cancellationToken)
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

            let! created = client.Store.CreateSession(tenant, forked, cancellationToken)

            if prefix.Count > 0 then
                // Prime, copy, and settle under the facade-held claim before
                // Resolve re-primes: the bootstrap is consumed by the claim,
                // so the forked actor drains real prompts first. The owner
                // and lease mirror the SessionClientOptions prime defaults;
                // the client does not carry the options, and both are
                // attribution only on a claim settled below.
                let bootstrap =
                    UserMessagePayload(UserMessage.Text "legate fork prime") :> InboxPayload

                let! _ =
                    client.Store.AppendInboxMessage(
                        tenant,
                        created.Id,
                        bootstrap,
                        DeliveryMode.Queue,
                        cancellationToken
                    )

                let! lease =
                    client.Store.ClaimNextTurn(
                        tenant,
                        created.Id,
                        "legate-session-facade",
                        TimeSpan.FromHours 1.0,
                        cancellationToken
                    )

                let claim =
                    match lease with
                    | :? TurnLeaseRenewed as renewed when not (isNull (box renewed)) -> renewed.Claim
                    | :? TurnLeaseHeld as held when not (isNull (box held)) -> held.Claim
                    | :? TurnLeaseExpiring as expiring when not (isNull (box expiring)) -> expiring.Claim
                    | _ ->
                        raise (
                            InvalidOperationException(
                                sprintf "The fork of session %O claimed no turn on its fresh row." sessionId
                            )
                        )

                try
                    let rekeyed =
                        prefix
                        |> Seq.map (SessionClientOperations.RekeyForFork created.Id)
                        |> List.ofSeq

                    for batch in rekeyed |> List.chunkBySize 100 do
                        let events = ResizeArray<SessionEvent>(batch) :> IReadOnlyList<SessionEvent>

                        match!
                            JournalWriter.appendWithTokenAsync
                                client.EventBus.EventStore
                                tenant
                                created.Id
                                claim.Token
                                events
                                cancellationToken
                        with
                        | JournalWriter.JournalAppended _ -> ()
                        | JournalWriter.JournalRejected reason ->
                            raise (
                                InvalidOperationException(
                                    sprintf "The fork of session %O lost its journal claim: %s." sessionId reason
                                )
                            )
                        | JournalWriter.JournalFailed reason ->
                            raise (
                                InvalidOperationException(
                                    sprintf "The fork of session %O failed to copy its prefix: %s." sessionId reason
                                )
                            )

                    let! _ = client.Store.SettleTurn(tenant, claim, TurnStatus.Completed, null, cancellationToken)
                    ()
                with ex ->
                    try
                        client.Store.SettleTurn(tenant, claim, TurnStatus.Completed, null, CancellationToken.None)
                        |> ignore
                    with _ ->
                        ()

                    raise ex

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
            let hub = PromptWaitHubs.GetOrAdd(sessionId)
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
    /// gap-free with no duplicates across the handoff. Unknown session
    /// throws SessionNotFoundException on the first move.
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
        client.EventBus.Subscribe(client.Tenant, sessionId, fromSequence, cancellationToken)

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
// Wiring

/// Builds the facade client from the container and wires the session
/// router to the suspendable behavior for hosts that opted in with an
/// IChatClient. Internal: hosts resolve SessionClient from DI and call
/// the SessionClientOperations extensions; nothing here crosses the public
/// API.
module internal SessionClientWiring =

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

    /// Builds the client from the container: resolves the stores, bounds,
    /// and hosted actor system, validates the facade options, opts into
    /// suspendable children when an IChatClient is registered, and returns
    /// the client resolving through the session router.
    /// <param name="provider">The container to build from. Must not be null.</param>
    /// <returns>The DI-owned session client.</returns>
    let buildClient (provider: IServiceProvider) : SessionClient =
        ArgumentNullException.ThrowIfNull(provider)

        let store = provider.GetRequiredService<ISessionStore>()
        let bus = provider.GetRequiredService<SessionEventBus>()

        let legateOptions =
            match provider.GetService<IOptions<LegateOptions>>() with
            | null -> LegateOptions()
            | options when isNull (box options.Value) -> LegateOptions()
            | options -> options.Value

        let clientOptions =
            match provider.GetService<SessionClientOptions>() with
            | null -> SessionClientOptions()
            | options -> options

        match clientOptions.Validate() with
        | null -> ()
        | violation -> raise (InvalidOperationException($"Invalid SessionClientOptions: %s{violation}"))

        let hostedServices = provider.GetServices<IHostedService>() |> List.ofSeq

        let localService =
            match
                hostedServices
                |> List.tryPick (fun service ->
                    match service with
                    | :? LocalActorSystemService as typed -> Some typed
                    | _ -> None)
            with
            | Some service -> service
            | None ->
                raise (
                    InvalidOperationException(
                        "The Legate local actor system is not registered: AddLegate registers it, so a replaced service collection breaks the session client."
                    )
                )

        let clusterService =
            match
                hostedServices
                |> List.tryPick (fun service ->
                    match service with
                    | :? ClusterActorSystemService as typed -> Some typed
                    | _ -> None)
            with
            | Some service -> service
            | None ->
                raise (
                    InvalidOperationException(
                        "The Legate cluster actor system is not registered: AddLegate registers it, so a replaced service collection breaks the session client."
                    )
                )

        // The mode seam: Local resolves through the in-process router,
        // the cluster modes through the shard region proxy. Both spawn
        // the same SessionActor through the factory wired below.
        let resolver: ISessionResolver =
            match legateOptions.Cluster.Mode with
            | ClusterMode.Local -> localService :> ISessionResolver
            | ClusterMode.StaticSeeds
            | ClusterMode.Kubernetes -> clusterService :> ISessionResolver
            | _ ->
                raise (
                    InvalidOperationException(
                        $"Unknown Legate cluster mode '%O{legateOptions.Cluster.Mode}'. Expected one of: Local, StaticSeeds, Kubernetes."
                    )
                )

        let delay =
            match provider.GetService<ILlmDelay>() with
            | null -> SystemLlmDelay(TimeProvider.System) :> ILlmDelay
            | seam -> seam

        // Opt-in: without a chat client the identity-only children stay and
        // the client still serves Open and the reads. The same client
        // serves the auto-title call; without one titling no-ops.
        let titleClient = provider.GetService<IChatClient>()

        match titleClient with
        | null -> ()
        | client ->
            let sources =
                provider.GetServices<IToolSource>()
                |> Seq.filter (fun source -> not (isNull (box source)))
                |> List.ofSeq

            let policy = SessionPermissions.resolvePolicy provider

            let runner =
                SessionPermissions.createRunner
                    client
                    store
                    clientOptions.Tenant
                    (resolveInputs store clientOptions.Tenant sources legateOptions.Turns legateOptions.AskUser)
                    delay
                    policy
                    (Some(systemPromptFor store clientOptions.Tenant))

            let eventStore = bus.EventStore
            let model = sessionModelOf clientOptions legateOptions

            let compactFor (_sessionId: SessionId) (journalToken: string) : CompactDeps option =
                Some(
                    {
                        Llm = legateOptions.Llm
                        ReservedBufferTokens = legateOptions.Pruning.ReservedBufferTokens
                        SessionModel = model
                        Catalog = provider.GetService<ILlmModelCatalog>()
                        Client = client
                        Observer = provider.GetService<IUsageObserver>()
                        Policy = provider.GetService<IModelPolicy>()
                        EventStore = eventStore
                        JournalToken = journalToken
                        Force = Compaction.CompactForce()
                    }
                )

            let entityFactory =
                Some(
                    SessionActor.spawnSuspendFactory
                        store
                        clientOptions.Tenant
                        eventStore
                        delay
                        legateOptions.Permissions.AskTimeout
                        clientOptions.ClaimOwner
                        clientOptions.LeaseDuration
                        runner
                        compactFor
                )

            // The factory lands on the mode-active service only; the idle
            // service keeps identity children either way.
            match resolver with
            | :? LocalActorSystemService as local -> local.SessionChildFactory <- entityFactory
            | :? ClusterActorSystemService as clustered -> clustered.SessionEntityFactory <- entityFactory
            | _ ->
                raise (
                    InvalidOperationException(
                        "The Legate actor system resolver is neither local nor clustered: AddLegate registers both, so a replaced service collection breaks the session client."
                    )
                )

        let resolve (sessionId: SessionId) (cancellationToken: CancellationToken) : Task<IActorRef> =
            resolver.ResolveSessionAsync(sessionId.ToString(), cancellationToken)

        // The agent catalog SetAgent validates against, or None when the
        // host runs without one: validation is skipped then.
        let agents = Option.ofObj (provider.GetService<IAgentStore>())

        let built =
            new SessionClient(store, clientOptions.Tenant, resolve, bus, clientOptions.DefaultWaitBound, delay, agents)

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

        built.AutoTitle <-
            Some(
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
            )

        built

/// Registers the session client facade: the options default, the event
/// bus over the durable journal, and the DI-owned client with its router
/// wiring. Host registrations win (TryAdd): a host-owned SessionClient,
/// bus, or options replaces the facade default.
module internal SessionClientRegistration =

    /// Registers the facade on the container.
    /// <param name="services">The container to add the facade to.</param>
    let register (services: IServiceCollection) : unit =
        ArgumentNullException.ThrowIfNull(services)

        services.TryAddSingleton(SessionClientOptions()) |> ignore

        services.TryAddSingleton<SessionEventBus>(
            Func<IServiceProvider, SessionEventBus>(fun provider ->
                let eventStore = provider.GetRequiredService<ISessionEventStore>()
                let logger = provider.GetService<ILogger<SessionEventBus>>()
                new SessionEventBus(eventStore, SessionSubscriptionOptions(), logger))
        )
        |> ignore

        services.TryAddSingleton<SessionClient>(
            Func<IServiceProvider, SessionClient>(fun provider -> SessionClientWiring.buildClient provider)
        )
        |> ignore
