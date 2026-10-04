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

    /// Builds the user history for a fresh run from the entry's parts,
    /// mirroring the actor's Queue runner shape, with the composed system
    /// prompt (issue 66) leading when present. A crash seed (Some) wins:
    /// the rehydrated transcript plus the in-memory resumption note
    /// replaces the entry-derived message, while the composed system
    /// prompt still leads. The seed is copied: the turn owns its history.
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

            PromptComposition.prependSystemPrompt history systemPrompt
            history

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
    /// <returns>The suspendable runner executing one attempt per call.</returns>
    let createRunner
        (client: IChatClient)
        (store: ISessionStore)
        (tenant: TenantId)
        (resolveInputs: InboxEntry -> IReadOnlyDictionary<string, AITool> * TurnLoop.TurnLoopOptions)
        (loopDelay: ILlmDelay)
        (policy: IPermissionPolicy | null)
        (getSystemPrompt: PromptComposition.GetTurnSystemPrompt option)
        : SessionActor.SuspendableRunner =
        ArgumentNullException.ThrowIfNull(client)
        ArgumentNullException.ThrowIfNull(store)
        ArgumentNullException.ThrowIfNull(loopDelay)

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
        /// the raw pending read. Runs on the turn thread, blocking like the
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
                    pending
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
                    // consumed, but journal nothing: the base spawn sites wire
                    // no Inject observer either, so both paths stay consistent.
                    let drain = drainInjected entry
                    let consume = consumeInjected entry

                    match cursor, reply with
                    | None, None ->
                        let! systemPrompt = resolveSystem ()
                        let history = historyOf entry systemPrompt seed

                        // Fresh runs mark at the first provider-call entry
                        // through the behavior-supplied hook and checkpoint
                        // usage plus skill loads through the fenced journal
                        // hooks (issue 321). The skill map rebinds per turn
                        // so the pre-built host tool journals under the
                        // running claim. Every dispatch verifies the live
                        // real-turn claim at the last moment (issue 376);
                        // the renewed LeaseAdmission hook is the cached fast
                        // check underneath.
                        let merged =
                            { loopOptions with
                                VerifyClaim = verifyForCurrentClaim ()
                                OnTurnStarted = onTurnStarted
                                OnUsageCheckpoint = onUsageCheckpoint
                                OnSkillLoaded = onSkillLoaded
                            }

                        let boundTools = bindSkillHook tools turnId merged.OnSkillLoaded

                        return!
                            TurnLoop.runSuspendableAsync
                                client
                                history
                                boundTools
                                merged
                                loopDelay
                                runnerToken
                                (fun () -> LeaseAdmission.check ())
                                drain
                                ignore
                                consume
                                gate
                                entry.SessionId
                                turnId
                                None
                                allowed
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

                        return! resume.ResumeAsync reply runnerToken
                    | Some live, Some(:? PermissionDecision as decision) ->
                        // Resumes already marked before they suspended: never
                        // mark on resume, but carry the usage and skill hooks
                        // so post-resume work checkpoints and loads journal
                        // (issue 321), with the skill map rebound per turn.
                        // The pending call re-verifies current authority at
                        // dispatch (issue 376); allowed tools still need
                        // ownership and the permission gate stays intact.
                        let merged =
                            { loopOptions with
                                VerifyClaim = verifyForCurrentClaim ()
                                OnTurnStarted = None
                                OnUsageCheckpoint = onUsageCheckpoint
                                OnSkillLoaded = onSkillLoaded
                            }

                        let boundTools = bindSkillHook tools turnId merged.OnSkillLoaded

                        return!
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
                                allowed
                    | Some live, Some(:? QuestionAnswer as answer) ->
                        // Resumes already marked before they suspended: never
                        // mark on resume, but carry the usage and skill hooks
                        // so post-resume work checkpoints and loads journal
                        // (issue 321), with the skill map rebound per turn.
                        // Post-resume dispatches re-verify current authority
                        // (issue 376).
                        let merged =
                            { loopOptions with
                                VerifyClaim = verifyForCurrentClaim ()
                                OnTurnStarted = None
                                OnUsageCheckpoint = onUsageCheckpoint
                                OnSkillLoaded = onSkillLoaded
                            }

                        let boundTools = bindSkillHook tools turnId merged.OnSkillLoaded

                        return!
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
                                allowed
                    | None, Some _ ->
                        // Crash-rebuild shape: no live cursor, so retry the
                        // turn from its inbox entry with the persisted grants.
                        let! rebuildPrompt = resolveSystem ()
                        let history = historyOf entry rebuildPrompt seed

                        // Retries run under the supplied turn id: mark at the
                        // first provider-call entry and carry the usage and
                        // skill hooks (issue 321), with the skill map rebound
                        // per turn. Crash retries verify the live claim like
                        // fresh runs (issue 376), failing closed when the
                        // rebuild holds no authority.
                        let merged =
                            { loopOptions with
                                VerifyClaim = verifyForCurrentClaim ()
                                OnTurnStarted = onTurnStarted
                                OnUsageCheckpoint = onUsageCheckpoint
                                OnSkillLoaded = onSkillLoaded
                            }

                        let boundTools = bindSkillHook tools turnId merged.OnSkillLoaded

                        return!
                            TurnLoop.runSuspendableAsync
                                client
                                history
                                boundTools
                                merged
                                loopDelay
                                runnerToken
                                (fun () -> LeaseAdmission.check ())
                                drain
                                ignore
                                consume
                                gate
                                entry.SessionId
                                turnId
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

    /// Resolves the permission policy the production runner evaluates
    /// against: the container's IPermissionPolicy, or null when the host
    /// registered none (the loop reads null as no gate).
    /// <param name="provider">The container to resolve from. Must not be null.</param>
    /// <returns>The registered policy, or null when none is registered.</returns>
    let resolvePolicy (provider: IServiceProvider) : IPermissionPolicy | null =
        ArgumentNullException.ThrowIfNull(provider)
        provider.GetService<IPermissionPolicy>()
