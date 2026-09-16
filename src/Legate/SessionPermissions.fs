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
    /// prompt (issue 66) leading when present.
    /// <param name="entry">The inbox entry the run executes.</param>
    /// <param name="systemPrompt">The composed system prompt, or null for the user-only shape.</param>
    /// <returns>The history carrying the system message (when present) and the entry's user message.</returns>
    let private historyOf (entry: InboxEntry) (systemPrompt: string | null) : IList<ChatMessage> =
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
    /// AllowForSession check runs before Evaluate on every attempt.
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
        /// consume lands before the next provider call, or the settle drain
        /// would redeliver the folded entry as a new turn. Runs on the turn
        /// thread, blocking like the base Inject wiring.
        /// <param name="entry">The running turn's entry, carrying the session.</param>
        /// <param name="injected">The folded entry to consume.</param>
        let consumeInjected (entry: InboxEntry) (injected: InboxEntry) : unit =
            if not (isNull (box injected)) then
                let positions = [| injected.Position |] :> IReadOnlyList<int64>

                try
                    store
                        .MarkInboxConsumed(tenant, entry.SessionId, positions, CancellationToken.None)
                        .GetAwaiter()
                        .GetResult()
                    |> ignore
                with _ ->
                    ()

        fun entry _attempt allowed cursor reply runnerToken ->
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
                        let history = historyOf entry systemPrompt

                        return!
                            TurnLoop.runSuspendableAsync
                                client
                                history
                                tools
                                loopOptions
                                loopDelay
                                runnerToken
                                (fun () -> true)
                                drain
                                ignore
                                consume
                                gate
                                entry.SessionId
                                (TurnId.New())
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
                        return! resume.ResumeAsync reply runnerToken
                    | Some live, Some(:? PermissionDecision as decision) ->
                        return!
                            TurnLoop.resumePermissionAsync
                                live
                                decision.Decision
                                client
                                live.HistorySnapshot
                                tools
                                loopOptions
                                loopDelay
                                runnerToken
                                (fun () -> true)
                                gate
                                allowed
                    | Some live, Some(:? QuestionAnswer as answer) ->
                        return!
                            TurnLoop.resumeQuestionAsync
                                live
                                answer.Answer
                                client
                                live.HistorySnapshot
                                tools
                                loopOptions
                                loopDelay
                                runnerToken
                                (fun () -> true)
                                gate
                                allowed
                    | None, Some _ ->
                        // Crash-rebuild shape: no live cursor, so retry the
                        // turn from its inbox entry with the persisted grants.
                        let! rebuildPrompt = resolveSystem ()
                        let history = historyOf entry rebuildPrompt

                        return!
                            TurnLoop.runSuspendableAsync
                                client
                                history
                                tools
                                loopOptions
                                loopDelay
                                runnerToken
                                (fun () -> true)
                                drain
                                ignore
                                consume
                                gate
                                entry.SessionId
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

    /// Resolves the permission policy the production runner evaluates
    /// against: the container's IPermissionPolicy, or null when the host
    /// registered none (the loop reads null as no gate).
    /// <param name="provider">The container to resolve from. Must not be null.</param>
    /// <returns>The registered policy, or null when none is registered.</returns>
    let resolvePolicy (provider: IServiceProvider) : IPermissionPolicy | null =
        ArgumentNullException.ThrowIfNull(provider)
        provider.GetService<IPermissionPolicy>()
