// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.AI
open Microsoft.Extensions.DependencyInjection

// Production permission pipeline: the TurnLoop-backed suspendable runner
// the live session actor drives, plus the DI policy resolution the spawn
// wiring binds it with. The runner mirrors the harness shape (first run
// through runSuspendableAsync, permission resumes through
// resumePermissionAsync, question resumes through resumeQuestionAsync, and
// a crash-rebuild reply without a cursor retries the turn from its inbox
// entry), but it is runtime code: SessionActor.spawnSuspendFactory threads
// it into every live child, so behaviorWithSuspend stops being
// harness-only. Per-entry inputs (the session tool set and turn budget)
// resolve through the given function, so one runner serves every session
// the facade owns; Inject entries fold at iteration boundaries through the
// store-backed drain with the consume landing before the next provider
// call. The policy arrives resolved from the container (null when
// the host registered none, meaning no gate): TurnLoop consults the
// session AllowForSession memory before Evaluate, the actor persists
// AllowForSession grants on the session row, and closing evicts them.

/// The production permission pipeline: the live suspendable runner and
/// the container policy resolution behind it. Internal so no runner or
/// policy type ever crosses the public API beyond the contracts.
module internal SessionPermissions =

    /// Extracts the executing entry's user message in conversational shape:
    /// the entry's parts in order, or an empty user message when the entry
    /// carries no user payload. Non-text parts pass by reference, so
    /// supported images and files survive exactly as the host sent them.
    /// <param name="entry">The inbox entry the run executes.</param>
    /// <returns>The user message the turn executes.</returns>
    let private userMessageOfEntry (entry: InboxEntry) : ChatMessage =
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

            ChatMessage(ChatRole.User, parts :> IList<AIContent>)
        | _ -> ChatMessage(ChatRole.User, "")

    /// Builds the user history for a fresh run from the entry's parts,
    /// mirroring the actor's Queue runner shape, with the composed system
    /// prompt (issue 66) leading when present. A crash seed (Some) wins:
    /// the rehydrated transcript plus the in-memory resumption note
    /// replaces the entry-derived message, while the composed system
    /// prompt still leads. The seed is copied: the turn owns its history.
    /// Seedless production fresh runs do not use this shape: the runner
    /// assembles the ordinary-turn history from the journal instead (issue
    /// 366, assembleOrdinaryHistoryAsync below).
    /// <param name="entry">The inbox entry the run executes.</param>
    /// <param name="systemPrompt">The composed system prompt, or null for the user-only shape.</param>
    /// <param name="seed">The crash-resume seed history, or None for a seedless run.</param>
    /// <returns>The history carrying the system message (when present) and the entry's user message, or the seeded history.</returns>
    let private historyOf
        (entry: InboxEntry)
        (systemPrompt: string | null)
        (seed: IList<ChatMessage> option)
        : IList<ChatMessage> =
        match seed with
        | Some seeded when not (isNull (box seeded)) ->
            let history = ResizeArray<ChatMessage>(seeded) :> IList<ChatMessage>
            PromptComposition.prependSystemPrompt history systemPrompt
            history
        | _ ->
            let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>
            history.Add(userMessageOfEntry entry)
            PromptComposition.prependSystemPrompt history systemPrompt
            history

    /// Replays one session's journal in sequence order from cursor 0,
    /// concatenating bounded pages. Pages only transport: every page split
    /// replays identically, mirroring the Transcripts.readTranscript
    /// invariant. Unknown session, expired journal, and end of stream all
    /// stop the replay, so a fresh session assembles from an empty prefix.
    /// <param name="eventStore">The journal to replay. Must not be null.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session to replay.</param>
    /// <returns>The journaled events in sequence order.</returns>
    let private replayJournalAsync
        (eventStore: ISessionEventStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        : Task<IReadOnlyList<SessionEvent>> =
        ArgumentNullException.ThrowIfNull(eventStore)

        task {
            let collected = ResizeArray<SessionEvent>()
            let mutable cursor = 0L
            let mutable paging = true

            while paging do
                let! outcome = eventStore.Replay(tenant, sessionId, cursor, 100, CancellationToken.None)

                match outcome with
                | :? EventReplayPage as page when not (isNull (box page)) ->
                    if not (isNull (box page.Events)) then
                        for event in page.Events do
                            if not (isNull (box event)) then
                                collected.Add(event)

                    if page.NextCursor.HasValue then
                        cursor <- page.NextCursor.Value
                    else
                        paging <- false
                | _ -> paging <- false

            return collected :> IReadOnlyList<SessionEvent>
        }

    /// Maps one history rejection to the client-safe turn-fault reason. The
    /// reason names the offending call id (never secrets or tool arguments)
    /// so the host can act on it.
    /// <param name="rejection">The rejection recovery refused the journal with.</param>
    /// <returns>The typed fault reason.</returns>
    let private historyRejectionReason (rejection: ConversationRecovery.RecoveryRejection) : string =
        match rejection with
        | ConversationRecovery.MissingToolArguments callId ->
            sprintf
                "The conversation history is missing tool arguments for call '%s': start a clean session."
                (if isNull (box callId) then "" else callId)
        | ConversationRecovery.InvalidToolArguments callId ->
            sprintf
                "The conversation history carries invalid tool arguments for call '%s': start a clean session."
                (if isNull (box callId) then "" else callId)
        | ConversationRecovery.UnpairedToolCompletion callId ->
            sprintf
                "The conversation history has a tool completion without its call for call '%s': start a clean session."
                (if isNull (box callId) then "" else callId)
        | ConversationRecovery.MissingToolResult callId ->
            sprintf
                "The conversation history is missing the tool result for call '%s': start a clean session."
                (if isNull (box callId) then "" else callId)
        | ConversationRecovery.InvalidJournal reason ->
            sprintf
                "The conversation journal is unusable (%s): start a clean session."
                (if isNull (box reason) then "invalid batch" else reason)

    /// Assembles the ordinary-turn history for a seedless fresh run (issue
    /// 366): replays the journal from cursor 0, folds the prefix through
    /// the read-only #380 recovery builder (never the lossy display cells),
    /// appends the executing entry's initial user message in conversational
    /// order, then leads with the composed system prompt exactly once. The
    /// composed prompt is never journaled, so prepend-once never duplicates.
    /// A rejection (or an unreadable journal) reads as an Error carrying the
    /// turn-fault reason: the runner fails the turn before any provider call
    /// instead of fabricating history.
    /// <param name="eventStore">The journal to replay. Must not be null.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="entry">The inbox entry the run executes.</param>
    /// <param name="systemPrompt">The composed system prompt, or null for the user-only shape.</param>
    /// <returns>The assembled history, or the explicit fault reason.</returns>
    let private assembleOrdinaryHistoryAsync
        (eventStore: ISessionEventStore)
        (tenant: TenantId)
        (entry: InboxEntry)
        (systemPrompt: string | null)
        : Task<Result<IList<ChatMessage>, string>> =
        task {
            try
                let! events = replayJournalAsync eventStore tenant entry.SessionId

                match ConversationRecovery.tryRecover events with
                | Error rejection -> return Error(historyRejectionReason rejection)
                | Ok history ->
                    history.Add(userMessageOfEntry entry)
                    PromptComposition.prependSystemPrompt history systemPrompt
                    return Ok history
            with _ ->
                return Error "The conversation history could not be read from the journal: start a clean session."
        }

    /// Builds the turn fault for an unassemblable ordinary history: a
    /// Failed completion carrying the explicit reason with zero provider
    /// calls and zero new journal writes. Never a
    /// CompletionRoutingException: recovery-policy rejection stays at the
    /// replay/wire layer (issue 372).
    /// <param name="turnId">The running turn the fault settles under.</param>
    /// <param name="reason">The explicit fault reason. Never null.</param>
    /// <returns>The faulted completion.</returns>
    let private failedHistoryCompletion (turnId: TurnId) (reason: string) : TurnLoop.TurnLoopCompletion =
        {
            Result =
                {
                    AssistantText = ""
                    Status = TurnStatus.Failed
                    Iterations = 0
                    Usage = { InputTokens = 0L; OutputTokens = 0L }
                    Outcome = TurnFailed(reason) :> TurnOutcome
                }
            TurnId = turnId
            HasPendingInjects = false
            Suspension = None
        }

    /// Appends one evidence batch under the running claim's last-moment
    /// fence: verifies the live ControlAdmission and LeaseAdmission hooks,
    /// then appends through the fenced writer under the ambient claim token
    /// (JournalWriter redaction and bounds apply; tenant isolation rides the
    /// tenant-scoped store). A fenced-out write raises
    /// TurnLeaseLostException with zero effects, so a takeover loser
    /// performs nothing; a failed write raises with the writer's typed
    /// reason (explicit, never silent loss). Outside a fenced turn
    /// (unclaimed shells) journals nothing, preserving harness shapes.
    /// Callers stamp the running turn id on every event (#374 attribution)
    /// before calling.
    /// <param name="eventStore">The journal the events append to. Must not be null.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session whose journal appends.</param>
    /// <param name="events">The events to append, in order. Must not be null or empty and must carry no nulls.</param>
    let private journalBatchUnderClaimAsync
        (eventStore: ISessionEventStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (events: IReadOnlyList<SessionEvent>)
        : Task<unit> =
        task {
            if isNull (box events) || events.Count = 0 then
                ()
            elif not (ControlAdmission.check ()) || not (LeaseAdmission.check ()) then
                raise (TurnLoop.TurnLeaseLostException())
            else
                match FencedClaimScope.currentClaim () with
                | Some claim when not (isNull (box claim)) && not (String.IsNullOrEmpty claim.Token) ->
                    match!
                        JournalWriter.appendWithTokenAsync
                            eventStore
                            tenant
                            sessionId
                            claim.Token
                            events
                            CancellationToken.None
                    with
                    | JournalWriter.JournalAppended _ -> ()
                    | JournalWriter.JournalRejected _ -> raise (TurnLoop.TurnLeaseLostException())
                    | JournalWriter.JournalFailed reason ->
                        raise (
                            InvalidOperationException(
                                if String.IsNullOrWhiteSpace reason then
                                    "The journal append failed."
                                else
                                    reason
                            )
                        )
                | _ -> ()
        }

    /// Journals one accepted user message as turn evidence (issue 366, Task
    /// 9): the UserMessageEvent carrying the running turn id, through
    /// redaction, bounds, and the claim fence.
    /// <param name="eventStore">The journal the event appends to. Must not be null.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session whose journal appends.</param>
    /// <param name="turnId">The running turn the input executes in.</param>
    /// <param name="message">The accepted user message. Null journals nothing.</param>
    let private journalUserEvidenceAsync
        (eventStore: ISessionEventStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (turnId: TurnId)
        (message: UserMessage)
        : Task<unit> =
        task {
            if not (isNull (box message)) then
                let event =
                    UserMessageEvent(
                        sessionId,
                        turnId,
                        Unchecked.defaultof<Nullable<int64>>,
                        DateTimeOffset.UtcNow,
                        message
                    )
                    :> SessionEvent

                do!
                    journalBatchUnderClaimAsync
                        eventStore
                        tenant
                        sessionId
                        (ResizeArray<SessionEvent>([| event |]) :> IReadOnlyList<SessionEvent>)
        }

    /// Builds the folded-Inject evidence hook (issue 366, Task 9): each
    /// Inject entry the running turn actually folds journals once at the
    /// iteration boundary before its consume lands, so later history carries
    /// it exactly once. Pending, rejected, or never-folded input journals
    /// nothing and is never fabricated. Blocking like the base Inject wiring:
    /// a fenced-out loser raises TurnLeaseLostException with zero effects.
    /// <param name="eventStore">The journal folded input appends to. Must not be null.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session whose journal appends.</param>
    /// <param name="turnId">The running turn the input folds into.</param>
    /// <returns>The Inject journal hook the turn runs with.</returns>
    let private journalInjectedHook
        (eventStore: ISessionEventStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (turnId: TurnId)
        : TurnLoop.JournalInjected =
        fun injected ->
            if not (isNull (box injected)) then
                match injected.Payload with
                | :? UserMessagePayload as payload when not (isNull (box payload)) && not (isNull (box payload.Message)) ->
                    journalUserEvidenceAsync eventStore tenant sessionId turnId payload.Message
                    |> fun write -> write.GetAwaiter().GetResult()
                | _ -> ()

    /// Journals one settled top-level tool observation as its atomic
    /// Started/Output/Completed batch under the running turn. Pure
    /// assembly plus the fenced append: no branch on the observation
    /// itself, so callers screen null before calling.
    /// <param name="eventStore">The journal the markers append to. Must not be null.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session whose journal appends.</param>
    /// <param name="turnId">The running turn the call settled in.</param>
    /// <param name="observation">The settled observation. Must not be null.</param>
    let private journalToolMarkersAsync
        (eventStore: ISessionEventStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (turnId: TurnId)
        (observation: TurnLoop.ToolCallObservation)
        : Task<unit> =
        task {
            let stamp = DateTimeOffset.UtcNow
            let noSequence = Unchecked.defaultof<Nullable<int64>>

            let callId =
                if isNull (box observation.ToolCallId) then
                    ""
                else
                    observation.ToolCallId

            let name =
                if isNull (box observation.ToolName) then
                    ""
                else
                    observation.ToolName

            let text =
                if isNull (box observation.Text) then
                    ""
                else
                    observation.Text

            let error: string | null =
                match observation.Error with
                | Some value when not (isNull (box value)) -> value
                | _ -> null

            let resultText: string | null =
                if isNull (box observation.Text) then
                    null
                else
                    observation.Text

            let batch =
                ResizeArray<SessionEvent>(
                    [|
                        ToolCallStartedEvent(
                            sessionId,
                            turnId,
                            noSequence,
                            stamp,
                            callId,
                            name,
                            observation.ArgumentsJson
                        )
                        :> SessionEvent
                        ToolCallOutputEvent(sessionId, turnId, noSequence, stamp, callId, text) :> SessionEvent
                        ToolCallCompletedEvent(sessionId, turnId, noSequence, stamp, callId, error, resultText)
                        :> SessionEvent
                    |]
                )
                :> IReadOnlyList<SessionEvent>

            do! journalBatchUnderClaimAsync eventStore tenant sessionId batch
        }

    /// Builds the top-level settled-tool evidence sink (issue 366, Task 3,
    /// authorized 2026-10-05): every settled top-level invocation journals
    /// its Started (call id, name, real ArgumentsJson) / Output / Completed
    /// (error, paired result text) markers as one atomic batch under the
    /// running turn, through redaction, bounds, and the claim fence.
    /// Denials settle (their denial text pairs like a result); suspensions
    /// never observe (a suspended call has not settled). Nested sub-agent
    /// runs never inherit this sink: the task tool overrides OnToolCall with
    /// its own nested observer.
    /// <param name="eventStore">The journal the markers append to. Must not be null.</param>
    /// <param name="tenant">The tenant the session belongs to.</param>
    /// <param name="sessionId">The session whose journal appends.</param>
    /// <param name="turnId">The running turn the call settled in.</param>
    /// <returns>The settled-invocation observer the turn runs with.</returns>
    let private toolSinkHook
        (eventStore: ISessionEventStore)
        (tenant: TenantId)
        (sessionId: SessionId)
        (turnId: TurnId)
        : (TurnLoop.ToolCallObservation -> Task<unit>) option =
        Some(fun observation ->
            if isNull (box observation) then
                Task.FromResult(())
            else
                journalToolMarkersAsync eventStore tenant sessionId turnId observation)

    /// Builds the production suspendable runner over
    /// TurnLoop.runSuspendableAsync plus the resume continuations: the
    /// runner SessionActor.spawnSuspendFactory threads into live session
    /// actors. Deny appends a denied tool result and continues the turn,
    /// Ask suspends with the unified carrier, and the per-tool
    /// AllowForSession check runs before Evaluate on every attempt. The
    /// crash seed rides the runner into both fresh-run shapes: Some seeds
    /// the history input with the rehydrated transcript plus note, None
    /// runs from the entry.
    /// <param name="client">The chat client turns run against. Must not be null.</param>
    /// <param name="store">The durable store the Inject drain and consume read. Must not be null.</param>
    /// <param name="tenant">The tenant runner-driven sessions belong to.</param>
    /// <param name="resolveInputs">Resolves one entry's tool set and turn budget. Must not be null and never return null tools.</param>
    /// <param name="loopDelay">The delay seam the turn's hard deadline fires off. Must not be null.</param>
    /// <param name="policy">The permission policy, or null for no gate (every call executes).</param>
    /// <param name="getSystemPrompt">The composed system prompt hook (issue 66), or None to run with no system message.</param>
    /// <param name="eventStore">The journal streaming deltas append to. Must not be null.</param>
    /// <param name="streaming">The per-attempt streaming journaler bounds.</param>
    /// <returns>The suspendable runner executing one attempt per call.</returns>
    let createRunner
        (client: IChatClient)
        (store: ISessionStore)
        (tenant: TenantId)
        (resolveInputs: InboxEntry -> IReadOnlyDictionary<string, AITool> * TurnLoop.TurnLoopOptions)
        (loopDelay: ILlmDelay)
        (policy: IPermissionPolicy | null)
        (getSystemPrompt: PromptComposition.GetTurnSystemPrompt option)
        (eventStore: ISessionEventStore)
        (streaming: SessionStreaming.StreamingBounds)
        : SessionActor.SuspendableRunner =
        ArgumentNullException.ThrowIfNull(client)
        ArgumentNullException.ThrowIfNull(store)
        ArgumentNullException.ThrowIfNull(loopDelay)
        ArgumentNullException.ThrowIfNull(eventStore)

        if isNull (box resolveInputs) then
            raise (ArgumentNullException(nameof resolveInputs))

        // The loop reads a null policy as no gate; defaultof carries that
        // null under the non-null reference type, mirroring the harness.
        let gate: IPermissionPolicy =
            match policy with
            | null -> Unchecked.defaultof<IPermissionPolicy>
            | present -> present

        /// Builds the last-moment per-tool admission fence for the running
        /// attempt (issue 376): ClaimFence.checkBeforeCallAsync over the
        /// AsyncLocal running claim, failing closed on lost, missing, or
        /// unverifiable authority. Some false denies dispatch (TurnLoop
        /// raises TurnLeaseLostException); never None, which would read as
        /// no fence. The renewed isLeaseValid hook (LeaseAdmission over the
        /// #375 heartbeat view) stays the fast cached check; this verify is
        /// the token check immediately before each dispatch.
        /// <returns>The VerifyClaim hook the turn runs with.</returns>
        let verifyForCurrentClaim () : (unit -> Task<bool>) option =
            match FencedClaimScope.currentClaim () with
            | Some claim when not (isNull (box claim)) ->
                Some(fun () ->
                    task {
                        try
                            return! ClaimFence.checkBeforeCallAsync store tenant claim CancellationToken.None
                        with _ ->
                            return false
                    })
            | _ -> Some(fun () -> Task.FromResult(false))

        /// Reads the entry session's pending inbox for the Inject fold: the
        /// loop filters Inject user messages itself, so the drain returns
        /// the raw pending read minus the running entry (an Inject start
        /// never refolds itself, or its input would duplicate in history
        /// and evidence). Runs on the turn thread, blocking like the
        /// base Inject wiring.
        /// <param name="entry">The entry the running turn executes.</param>
        /// <returns>The pending inbox entries.</returns>
        let drainInjected (entry: InboxEntry) () : IReadOnlyList<InboxEntry> =
            try
                let pending =
                    store.ReadPendingInbox(tenant, entry.SessionId, CancellationToken.None).GetAwaiter().GetResult()

                if isNull (box pending) then
                    ResizeArray<InboxEntry>() :> IReadOnlyList<InboxEntry>
                else
                    // The running entry stays pending until settle:
                    // exclude it so an Inject start never refolds
                    // itself and journals twice.
                    pending
                    |> Seq.filter (fun candidate ->
                        isNull (box candidate) |> not && candidate.Position <> entry.Position)
                    |> ResizeArray
                    :> IReadOnlyList<InboxEntry>
            with _ ->
                ResizeArray<InboxEntry>() :> IReadOnlyList<InboxEntry>

        /// Marks one folded Inject entry consumed so it never refolds: the
        /// consume lands atomically under the running claim through the
        /// #377 ConsumeInboxUnderClaim path (never verify-then-write around
        /// the unfenced consume), before the next provider call, or the
        /// settle drain would redeliver the folded entry as a new turn.
        /// Fails closed: a lost claim raises TurnLeaseLostException with
        /// zero effects, and a missing claim raises too (never an unfenced
        /// consume). Runs on the turn thread, blocking like the base Inject
        /// wiring.
        /// <param name="entry">The running turn's entry, carrying the session.</param>
        /// <param name="injected">The folded entry to consume.</param>
        let consumeInjected (entry: InboxEntry) (injected: InboxEntry) : unit =
            if not (isNull (box injected)) then
                if not (ControlAdmission.check ()) then
                    raise (TurnLoop.TurnLeaseLostException())

                if not (LeaseAdmission.check ()) then
                    raise (TurnLoop.TurnLeaseLostException())

                let positions = [| injected.Position |] :> IReadOnlyList<int64>

                try
                    match FencedClaimScope.currentClaim () with
                    | Some claim when not (isNull (box claim)) ->
                        let landed =
                            ClaimFence.consumeInboxAsync
                                store
                                tenant
                                claim
                                entry.SessionId
                                positions
                                CancellationToken.None
                            |> fun task -> task.GetAwaiter().GetResult()

                        if not landed then
                            raise (TurnLoop.TurnLeaseLostException())
                    | _ -> raise (TurnLoop.TurnLeaseLostException())
                with
                | :? TurnLoop.TurnLeaseLostException -> reraise ()
                | _ -> raise (TurnLoop.TurnLeaseLostException())

        /// Builds the progressive delta hooks (issue 379) from the ambient
        /// streaming scope the actor entered per attempt: each non-empty
        /// text/reasoning chunk buffers under the running attempt's
        /// coalescing journaler with the real turn id, flushing bounded
        /// batches through the fenced append as caps trip. The hooks read
        /// the scope per chunk (not the build-time journaler), so nested
        /// and resumed continuations observe the fresh attempt's journaler.
        /// Outside a streaming turn the hooks no-op, preserving harness
        /// and unclaimed-shell shapes.
        /// <returns>The text and reasoning hooks the turn runs with.</returns>
        let streamingHooks () : TurnLoop.TextDeltaHook option * TurnLoop.ReasoningDeltaHook option =
            let onText (text: string) : unit =
                match SessionStreaming.StreamingScope.currentJournaler () with
                | Some journaler when not (isNull (box journaler)) -> journaler.AppendText text
                | _ -> ()

            let onReasoning (text: string) : unit =
                match SessionStreaming.StreamingScope.currentJournaler () with
                | Some journaler when not (isNull (box journaler)) -> journaler.AppendReasoning text
                | _ -> ()

            Some onText, Some onReasoning

        /// Flushes the running attempt's streaming remainder (issue 379):
        /// lands coalesced deltas in bounded appends and awaits the whole
        /// chain after the loop, before the actor journals suspension or
        /// settlement, so committed output stays ordered before the terminal
        /// event. Awaits outstanding mid-stream flushes even with nothing
        /// new. A rejected flush raises TurnLeaseLostException (the takeover
        /// loser stops with zero further effects); a failed flush raises
        /// with the typed reason (explicit, never silent loss); prior landed
        /// batches survive either way.
        let flushStreamingAsync () : Task<unit> =
            match SessionStreaming.StreamingScope.currentJournaler () with
            | Some journaler when not (isNull (box journaler)) -> journaler.FlushAsync()
            | _ -> Task.FromResult(())

        /// Runs one loop attempt under the streaming flush (issue 379): the
        /// remainder lands and the chain settles before the completion
        /// returns, so the actor journals suspension or settlement after
        /// every committed delta. On a loop fault the chain still settles
        /// first (best-effort: flush faults swallow so the original fault
        /// propagates), so partial commits stay ordered before the terminal
        /// event the fault handler journals.
        /// <param name="run">The loop attempt to settle streaming for.</param>
        /// <returns>The loop completion with streaming flushed.</returns>
        let settleStreaming (run: unit -> Task<TurnLoop.TurnLoopCompletion>) : Task<TurnLoop.TurnLoopCompletion> =
            task {
                try
                    let! completion = run ()
                    do! flushStreamingAsync ()
                    return completion
                with ex ->
                    try
                        do! flushStreamingAsync ()
                    with _ ->
                        ()

                    return! Task.FromException<TurnLoop.TurnLoopCompletion>(ex)
            }

        /// Binds the running turn's skill journal hook into the pre-built
        /// tool map (issue 321): hosts build the skill tool once per
        /// session with a log-only onLoaded, so the runner rebinds it per
        /// turn to the fenced journal callback, preserving the host
        /// callback and re-keying under the running turn. Reads the merged
        /// OnSkillLoaded hook, so the TurnLoop option is consumed in the
        /// runner rather than staying write-only. Non-skill maps and
        /// non-SkillFunction entries pass through untouched.
        /// <param name="tools">The pre-built session tool map.</param>
        /// <param name="turnId">The running turn the load runs inside.</param>
        /// <param name="hook">The merged skill-load journal hook, or None.</param>
        /// <returns>The tool map the turn runs with.</returns>
        let bindSkillHook
            (tools: IReadOnlyDictionary<string, AITool>)
            (turnId: TurnId)
            (hook: TurnLoop.SkillLoadedHook option)
            : IReadOnlyDictionary<string, AITool> =
            match hook with
            | None -> tools
            | Some journal ->
                if isNull (box tools) then
                    tools
                elif not (tools.ContainsKey SkillTool.ToolName) then
                    tools
                else
                    let current = tools[SkillTool.ToolName]

                    let callback =
                        Func<SkillLoadedEvent, Task>(fun loaded -> journal loaded CancellationToken.None)

                    let rebound = SkillTool.TryBindJournalHook(current, turnId, callback)

                    if Object.ReferenceEquals(rebound, current) then
                        tools
                    else
                        let table = Dictionary<string, AITool>(tools, StringComparer.Ordinal)
                        table[SkillTool.ToolName] <- rebound
                        table :> IReadOnlyDictionary<string, AITool>

        fun entry _attempt allowed cursor reply seed runnerToken onTurnStarted onUsageCheckpoint onSkillLoaded turnId ->
            let tools, loopOptions = resolveInputs entry

            // Fail fast outside the task computation: a null tool set or
            // budget is a host wiring bug, never a turn outcome.
            let miswired: Task<TurnLoop.TurnLoopCompletion> option =
                if isNull (box tools) then
                    Some(
                        Task.FromException<TurnLoop.TurnLoopCompletion>(
                            InvalidOperationException(
                                "The session tool resolver returned null: it must return the session tools, empty when there are none."
                            )
                        )
                    )
                elif isNull (box loopOptions) then
                    Some(
                        Task.FromException<TurnLoop.TurnLoopCompletion>(
                            InvalidOperationException(
                                "The session options resolver returned null: it must return the entry's turn budget."
                            )
                        )
                    )
                else
                    None

            match miswired with
            | Some failed -> failed
            | None ->
                task {

                    // Per-attempt streaming journaler (issue 379): built
                    // under the running fenced claim with the real turn id
                    // the actor supplied, so deltas journal with actual
                    // session/turn attribution through the token fence. A
                    // missing claim (unclaimed shells) journals nothing.
                    // Entered for the whole attempt so nested and resumed
                    // continuations observe this journaler through the
                    // scope-reading delta hooks.
                    let streamer =
                        match FencedClaimScope.currentClaim () with
                        | Some claim when not (isNull (box claim)) && not (String.IsNullOrEmpty claim.Token) ->
                            Some(
                                SessionStreaming.StreamingJournaler(
                                    eventStore,
                                    tenant,
                                    entry.SessionId,
                                    turnId,
                                    claim.Token,
                                    streaming
                                )
                            )
                        | _ -> None

                    use _streamingScope = SessionStreaming.StreamingScope.enter streamer

                    // Package-load step (issue 66): fresh runs resolve the
                    // composed system prompt and lead with it. Resumes replay
                    // the suspended history verbatim, never recomposing.
                    let resolveSystem () : Task<string | null> =
                        task {
                            match getSystemPrompt with
                            | Some resolve -> return! resolve entry runnerToken
                            | None -> return null
                        }

                    // Folded Inject entries shape the running history and are
                    // consumed, and now journaled once each at the fold
                    // boundary (issue 366, Task 9): the hook below journals
                    // only what the loop actually folds, so pending or
                    // never-folded input stays unjournaled and unfabricated.
                    let drain = drainInjected entry
                    let consume = consumeInjected entry

                    // Fenced evidence sinks for this attempt (issue 366,
                    // Tasks 3 and 9): the settled-tool markers and the
                    // folded-Inject user evidence journal under the running
                    // claim with the running turn id. Unclaimed shells sink
                    // nothing; a fenced-out loser raises with zero effects.
                    let toolSink = toolSinkHook eventStore tenant entry.SessionId turnId
                    let journalInjected = journalInjectedHook eventStore tenant entry.SessionId turnId

                    match cursor, reply with
                    | None, None ->
                        let! systemPrompt = resolveSystem ()

                        let! freshHistory =
                            match seed with
                            | Some _ -> task { return Ok(historyOf entry systemPrompt seed) }
                            | None -> assembleOrdinaryHistoryAsync eventStore tenant entry systemPrompt

                        match freshHistory with
                        | Error reason ->
                            // Fail before any provider call and before any
                            // journal write: the history cannot be rebuilt
                            // truthfully, so the turn settles Failed with
                            // the explicit reason and performs nothing.
                            return failedHistoryCompletion turnId reason
                        | Ok history ->
                            // The journaled prefix (when any) holds the prior
                            // turns; the executing entry's input journals once
                            // here, after assembly (so the prefix never already
                            // carries it) and before the first provider call (so
                            // later turns replay it). Seeded rebuilds skip the
                            // write: the pre-crash attempt already evidenced the
                            // input, and re-journaling would duplicate it.
                            match seed with
                            | Some _ -> ()
                            | None ->
                                match entry.Payload with
                                | :? UserMessagePayload as initial when
                                    not (isNull (box initial)) && not (isNull (box initial.Message))
                                    ->
                                    do!
                                        journalUserEvidenceAsync
                                            eventStore
                                            tenant
                                            entry.SessionId
                                            turnId
                                            initial.Message
                                | _ -> ()

                            // Fresh runs mark at the first provider-call entry
                            // through the behavior-supplied hook and checkpoint
                            // usage plus skill loads through the fenced journal
                            // hooks (issue 321). Progressive text and reasoning
                            // deltas buffer through the per-attempt streaming
                            // journaler (issue 379) and flush after the loop,
                            // before the actor journals suspension or
                            // settlement. Settled top-level tool calls journal
                            // their Started/Output/Completed markers through
                            // the fenced sink (issue 366), and folded Inject
                            // entries journal once each at the fold boundary.
                            // The skill map rebinds per turn
                            // so the pre-built host tool journals under the
                            // running claim. Every dispatch verifies the live
                            // real-turn claim at the last moment (issue 376);
                            // the renewed LeaseAdmission hook is the cached fast
                            // check underneath.
                            let onTextDelta, onReasoningDelta = streamingHooks ()

                            let merged =
                                { loopOptions with
                                    VerifyClaim = verifyForCurrentClaim ()
                                    OnTurnStarted = onTurnStarted
                                    OnUsageCheckpoint = onUsageCheckpoint
                                    OnSkillLoaded = onSkillLoaded
                                    OnTextDelta = onTextDelta
                                    OnReasoningDelta = onReasoningDelta
                                    OnToolCall = toolSink
                                }

                            let boundTools = bindSkillHook tools turnId merged.OnSkillLoaded

                            return!
                                settleStreaming (fun () ->
                                    TurnLoop.runSuspendableAsync
                                        client
                                        history
                                        boundTools
                                        merged
                                        loopDelay
                                        runnerToken
                                        (fun () -> LeaseAdmission.check ())
                                        drain
                                        journalInjected
                                        consume
                                        gate
                                        entry.SessionId
                                        turnId
                                        None
                                        allowed)
                    | Some live, Some reply when live.Nested.IsSome ->
                        // Nested sub-agent suspension (issue 72): the reply
                        // re-enters the nested loop through the suspension's
                        // resume, which continues the parent turn once the
                        // nested run settles. Crash rebuilds lose the resume
                        // and retry from the inbox entry instead.
                        //
                        // The nested resume replays the suspended history
                        // verbatim: Inject entries folded before the suspension
                        // stay folded, and entries arriving mid-resume fold at
                        // the nested loop's own boundaries through the same
                        // drain.
                        let resume = live.Nested.Value

                        if not (ControlAdmission.check () && LeaseAdmission.check ()) then
                            raise (TurnLoop.TurnLeaseLostException())

                        match verifyForCurrentClaim () with
                        | Some verify ->
                            let! admitted = verify ()

                            if not admitted then
                                raise (TurnLoop.TurnLeaseLostException())
                        | None -> raise (TurnLoop.TurnLeaseLostException())

                        // Post-nested streams journal through the fresh
                        // attempt's scope (the delta hooks read it per
                        // chunk), settling with the resumed journaler below.
                        return! settleStreaming (fun () -> resume.ResumeAsync reply runnerToken)
                    | Some live, Some(:? PermissionDecision as decision) ->
                        // Resumes already marked before they suspended: never
                        // mark on resume, but carry the usage, skill,
                        // streaming, and settled-tool hooks so post-resume
                        // work checkpoints, loads, streams, and evidences
                        // tool calls journal (issues 321, 379, 366), with
                        // the skill map rebound per turn.
                        // The pending call re-verifies current authority at
                        // dispatch (issue 376); allowed tools still need
                        // ownership and the permission gate stays intact.
                        let onTextDelta, onReasoningDelta = streamingHooks ()

                        let merged =
                            { loopOptions with
                                VerifyClaim = verifyForCurrentClaim ()
                                OnTurnStarted = None
                                OnUsageCheckpoint = onUsageCheckpoint
                                OnSkillLoaded = onSkillLoaded
                                OnTextDelta = onTextDelta
                                OnReasoningDelta = onReasoningDelta
                                OnToolCall = toolSink
                            }

                        let boundTools = bindSkillHook tools turnId merged.OnSkillLoaded

                        return!
                            settleStreaming (fun () ->
                                TurnLoop.resumePermissionAsync
                                    live
                                    decision.Decision
                                    client
                                    live.HistorySnapshot
                                    boundTools
                                    merged
                                    loopDelay
                                    runnerToken
                                    (fun () -> LeaseAdmission.check ())
                                    gate
                                    allowed)
                    | Some live, Some(:? QuestionAnswer as answer) ->
                        // Resumes already marked before they suspended: never
                        // mark on resume, but carry the usage, skill,
                        // streaming, and settled-tool hooks so post-resume
                        // work checkpoints, loads, streams, and evidences
                        // tool calls journal (issues 321, 379, 366).
                        // Post-resume dispatches re-verify current authority
                        // (issue 376).
                        let onTextDelta, onReasoningDelta = streamingHooks ()

                        let merged =
                            { loopOptions with
                                VerifyClaim = verifyForCurrentClaim ()
                                OnTurnStarted = None
                                OnUsageCheckpoint = onUsageCheckpoint
                                OnSkillLoaded = onSkillLoaded
                                OnTextDelta = onTextDelta
                                OnReasoningDelta = onReasoningDelta
                                OnToolCall = toolSink
                            }

                        let boundTools = bindSkillHook tools turnId merged.OnSkillLoaded

                        return!
                            settleStreaming (fun () ->
                                TurnLoop.resumeQuestionAsync
                                    live
                                    answer.Answer
                                    client
                                    live.HistorySnapshot
                                    boundTools
                                    merged
                                    loopDelay
                                    runnerToken
                                    (fun () -> LeaseAdmission.check ())
                                    gate
                                    allowed)
                    | None, Some _ ->
                        // Crash-rebuild shape: no live cursor, so retry the
                        // turn from its inbox entry with the persisted grants.
                        // History stays verbatim (a seed wins, else the entry
                        // shape): no replay, no executing-entry append, no
                        // user-evidence re-journal, so a retried turn never
                        // duplicates evidence. Settled tools still evidence
                        // through the fenced sink, and folded Injects still
                        // journal once each at the fold boundary.
                        let! rebuildPrompt = resolveSystem ()
                        let history = historyOf entry rebuildPrompt seed

                        // Retries run under the supplied turn id: mark at the
                        // first provider-call entry and carry the usage,
                        // skill, streaming, and settled-tool hooks (issues
                        // 321, 379, 366), with
                        // the skill map rebound per turn. Crash retries
                        // verify the live claim like fresh runs (issue 376),
                        // failing closed when the rebuild holds no authority.
                        let onTextDelta, onReasoningDelta = streamingHooks ()

                        let merged =
                            { loopOptions with
                                VerifyClaim = verifyForCurrentClaim ()
                                OnTurnStarted = onTurnStarted
                                OnUsageCheckpoint = onUsageCheckpoint
                                OnSkillLoaded = onSkillLoaded
                                OnTextDelta = onTextDelta
                                OnReasoningDelta = onReasoningDelta
                                OnToolCall = toolSink
                            }

                        let boundTools = bindSkillHook tools turnId merged.OnSkillLoaded

                        return!
                            settleStreaming (fun () ->
                                TurnLoop.runSuspendableAsync
                                    client
                                    history
                                    boundTools
                                    merged
                                    loopDelay
                                    runnerToken
                                    (fun () -> LeaseAdmission.check ())
                                    drain
                                    journalInjected
                                    consume
                                    gate
                                    entry.SessionId
                                    turnId
                                    None
                                    allowed)
                    | _ ->
                        return
                            raise (
                                InvalidOperationException(
                                    "The suspendable runner received a cursor without a matching reply."
                                )
                            )
                }

    /// Resolves the permission policy the production runner evaluates
    /// against: the container's IPermissionPolicy, or null when the host
    /// registered none (the loop reads null as no gate).
    /// <param name="provider">The container to resolve from. Must not be null.</param>
    /// <returns>The registered policy, or null when none is registered.</returns>
    let resolvePolicy (provider: IServiceProvider) : IPermissionPolicy | null =
        ArgumentNullException.ThrowIfNull(provider)
        provider.GetService<IPermissionPolicy>()
