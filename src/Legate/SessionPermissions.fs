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
// harness-only. The policy arrives resolved from the container (null when
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
    /// <param name="tools">The tools turns may call. Must not be null.</param>
    /// <param name="options">The turn loop tuning and per-turn budget.</param>
    /// <param name="loopDelay">The delay seam the turn's hard deadline fires off. Must not be null.</param>
    /// <param name="policy">The permission policy, or null for no gate (every call executes).</param>
    /// <param name="getSystemPrompt">The composed system prompt hook (issue 66), or None to run with no system message.</param>
    /// <returns>The suspendable runner executing one attempt per call.</returns>
    let createRunner
        (client: IChatClient)
        (tools: IReadOnlyDictionary<string, AITool>)
        (options: TurnLoop.TurnLoopOptions)
        (loopDelay: ILlmDelay)
        (policy: IPermissionPolicy | null)
        (getSystemPrompt: PromptComposition.GetTurnSystemPrompt option)
        : SessionActor.SuspendableRunner =
        ArgumentNullException.ThrowIfNull(client)
        ArgumentNullException.ThrowIfNull(tools)
        ArgumentNullException.ThrowIfNull(loopDelay)

        // The loop reads a null policy as no gate; defaultof carries that
        // null under the non-null reference type, mirroring the harness.
        let gate: IPermissionPolicy =
            match policy with
            | null -> Unchecked.defaultof<IPermissionPolicy>
            | present -> present

        let noDrain () : IReadOnlyList<InboxEntry> =
            ResizeArray<InboxEntry>() :> IReadOnlyList<InboxEntry>

        fun entry _attempt allowed cursor reply runnerToken ->
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

                match cursor, reply with
                | None, None ->
                    let! systemPrompt = resolveSystem ()
                    let history = historyOf entry systemPrompt

                    return!
                        TurnLoop.runSuspendableAsync
                            client
                            history
                            tools
                            options
                            loopDelay
                            runnerToken
                            (fun () -> true)
                            noDrain
                            ignore
                            ignore
                            gate
                            entry.SessionId
                            (TurnId.New())
                            None
                            allowed
                | Some live, Some(:? PermissionDecision as decision) ->
                    return!
                        TurnLoop.resumePermissionAsync
                            live
                            decision.Decision
                            client
                            live.HistorySnapshot
                            tools
                            options
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
                            options
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
                            options
                            loopDelay
                            runnerToken
                            (fun () -> true)
                            noDrain
                            ignore
                            ignore
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
