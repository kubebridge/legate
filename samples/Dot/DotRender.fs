// SPDX-License-Identifier: Apache-2.0
module Dot.DotRender

open System
open Legate

// Streaming renderer for the dot TUI transcript viewport (issue 333): a
// pure fold over the same SessionClient.Subscribe event stream the plain
// REPL prints, painting richer blocks into the existing DotShell viewport.
// Stdlib-only and TTY-free: every function is pure over its inputs, so
// delta accumulation, tool cards, inline approval/question widgets,
// lifecycle markers, and truncation are proven headlessly without a
// terminal. Markdown stays at readable structure (headings, code fences,
// lists); tables and syntax highlighting are later polish. No new NuGet
// dependencies; samples/Dot only, no src/ changes.

// ──────────────────────────────────────────────────────────────────────────
// Caps: huge tool outputs never flood the viewport or memory.

// The per-tool-call output characters retained while collapsed: the card
// shows this head plus a truncation affordance.
let maxToolOutputChars = 2000

// The per-tool-call output characters retained while expanded: the card
// shows this head after the user presses e.
let maxExpandedToolOutputChars = 20000

// The assistant characters retained: the transcript keeps this head plus a
// truncation marker instead of accumulating unbounded deltas.
let maxAssistantChars = 20000

// The reasoning characters retained: reasoning stays a compact notice.
let maxReasoningChars = 2000

// The lifecycle plus system plus diagnostic lines retained: the viewport
// keeps this tail window instead of growing without bound.
let maxMetaLines = 200

// ──────────────────────────────────────────────────────────────────────────
// Model

/// The live state of one tool call card.
type ToolStatus =
    /// The call is still running.
    | Running
    /// The call succeeded.
    | Succeeded
    /// The call failed with the given reason.
    | Failed of string

/// One tool-call card: the name with its running/completed/failed state
/// plus truncated output with expand/collapse.
type ToolCard =
    {
        /// The tool-call id correlating started/output/completed events.
        Id: string
        /// The tool name from the started event.
        Name: string
        /// The current call state.
        Status: ToolStatus
        /// The retained output head (capped).
        Output: string
        /// The overflow characters dropped beyond the cap.
        Overflow: int
        /// True after the user expands the card.
        Expanded: bool
    }

/// One pending permission request: the inline approval widget answers it
/// from the keyboard without leaving the transcript.
type PermissionPrompt =
    {
        /// The id the host answers a PermissionDecision with.
        RequestId: string
        /// The tool awaiting permission.
        ToolName: string
    }

/// One pending agent question: the inline answer field resumes it.
type QuestionPrompt =
    {
        /// The id the host answers a QuestionAnswer with.
        QuestionId: string
        /// The question the agent asked.
        Question: string
    }

/// The pure renderer state: the fold over Subscribe events plus the engine
/// diagnostic lines the REPL prints outside the journal.
type DisplayBlock =
    | UserText of string
    | AssistantText of string
    | ReasoningText of string
    | ToolReference of string
    | ErrorText of string
    | Notice of string

/// Transcript state shared by the plain-contract tests and fullscreen view.
type RendererState =
    {
        /// The accumulated assistant text head.
        Assistant: string
        /// True when assistant deltas overflowed the cap.
        AssistantTruncated: bool
        /// The accumulated reasoning head.
        Reasoning: string
        /// The tool cards by tool-call id.
        Tools: Map<string, ToolCard>
        /// The tool-card order, oldest first.
        Order: string list
        /// The pending permission widgets, oldest first.
        Permissions: PermissionPrompt list
        /// The pending question widgets, oldest first.
        Questions: QuestionPrompt list
        /// The turn lifecycle markers in place, oldest first.
        Lifecycle: string list
        /// The retained notices (usage, compaction, skills, agents, prune).
        System: string list
        /// The retained engine diagnostic lines (RESULT, ERROR, etc.).
        Diagnostics: string list
        /// Chronological blocks for the human-facing conversation.
        Display: DisplayBlock list
        /// Whether this turn has supplied streaming text.
        StreamedTurn: bool
        /// Whether diagnostic lines are inside a RESULT envelope.
        InResult: bool
        /// The RESULT envelope status token while inside one, else empty.
        ResultStatus: string
        /// The session the streamed prefix belongs to, empty when none.
        ActiveSession: string
        /// The turn the streamed prefix belongs to, empty when none.
        ActiveTurn: string
        /// The assistant delta prefix streamed for the active turn.
        TurnStreamed: string
        /// The durable (session, sequence) identities already folded.
        Rendered: Set<string>
    }

/// The empty renderer: no text, no cards, no prompts, no markers.
let empty: RendererState =
    {
        Assistant = ""
        AssistantTruncated = false
        Reasoning = ""
        Tools = Map.empty
        Order = []
        Permissions = []
        Questions = []
        Lifecycle = []
        System = []
        Diagnostics = []
        Display = []
        StreamedTurn = false
        InResult = false
        ResultStatus = ""
        ActiveSession = ""
        ActiveTurn = ""
        TurnStreamed = ""
        Rendered = Set.empty
    }

// ──────────────────────────────────────────────────────────────────────────
// Small helpers

/// Reads a possibly-null string as non-null.
let private safe (value: string | null) : string =
    match value with
    | null -> ""
    | text -> text

/// Appends a fragment to a capped head: keeps the head, counts overflow.
let private appendCapped (head: string) (fragment: string) (cap: int) : string * bool * int =
    let safeHead = safe head
    let safeFragment = safe fragment
    let combined = safeHead + safeFragment

    if combined.Length <= cap then
        combined, false, 0
    else
        combined.Substring(0, cap), true, combined.Length - cap

/// Keeps the tail window of a line list.
let private takeTail (cap: int) (lines: string list) : string list =
    if lines.Length <= cap then
        lines
    else
        lines |> List.skip (lines.Length - cap)

/// Appends one meta line keeping the tail window.
let private addMeta (lines: string list) (line: string) : string list =
    takeTail maxMetaLines (lines @ [ line ])

// ──────────────────────────────────────────────────────────────────────────
// Consumer deduplication (issue 385)

// True when a RESULT envelope status token names a successful settle:
// the TurnStatus.Completed text the engine prints. Unknown tokens read as
// non-success only when explicitly terminal elsewhere; the envelope fold
// treats only this token as success and only explicit non-completed,
// non-empty tokens as failure, so malformed envelopes fail open.
let private isCompletedStatus (status: string) : bool =
    String.Equals(status, "Completed", StringComparison.Ordinal)

// Scopes the streamed prefix to the event's session and turn: a session
// change (switch, resume attach) or a turn change (TurnStarted, queued
// turns) isolates the prefix and the streamed flag without clearing the
// durable rendered set (sequences are per-session, so unrelated content
// never cross-deduplicates) or the transcript.
let private scopeFor (state: RendererState) (sessionKey: string) (turnKey: string) : RendererState =
    if sessionKey <> state.ActiveSession then
        { state with
            ActiveSession = sessionKey
            ActiveTurn = turnKey
            TurnStreamed = ""
            StreamedTurn = false
        }
    elif turnKey <> "" && turnKey <> state.ActiveTurn then
        { state with
            ActiveTurn = turnKey
            TurnStreamed = ""
            StreamedTurn = false
        }
    else
        state

// ──────────────────────────────────────────────────────────────────────────
// Fold: Subscribe events into viewport blocks.

// Applies one journaled event to the renderer state: TextDelta
// accumulation, ToolCallStarted/Output/Completed correlated by ToolCallId,
// permission/question pending slots, turn lifecycle slots, and system
// notices preserved. Null events and null deltas are ignored; unknown
// event subtypes land as a system notice so parity never silently drops
// them.
// <param name="state">The current renderer state.</param>
// <param name="evt">The Subscribe event just observed.</param>
// <returns>The next renderer state.</returns>
let private applyState (state: RendererState) (evt: SessionEvent) : RendererState =
    if isNull (box evt) then
        state
    else if isNull (box state) then
        empty
    else
        // Consumer dedup (issue 385): scope the streamed prefix to the
        // event's session and turn, then record its durable identity.
        // Duplicate delivery repeats the identical identity, so the
        // apply-level check already returned; distinct same-text events
        // carry distinct sequences and fold normally here.
        let scoped = scopeFor state (DotDedup.sessionKeyOf evt) (DotDedup.turnKeyOf evt)

        let state =
            match DotDedup.durableKeyOf evt with
            | None -> scoped
            | Some identity ->
                { scoped with
                    Rendered = scoped.Rendered.Add identity
                }

        match evt with
        | :? TurnStartedEvent ->
            { state with
                Lifecycle = addMeta state.Lifecycle "turn running..."
            }
        | :? TextDeltaEvent as delta when not (isNull (box delta)) ->
            let fragment = safe delta.Text

            if fragment = "" then
                state
            else
                let combined = state.Assistant + fragment

                let nextPrefix = state.TurnStreamed + fragment

                let nextTurnStreamed =
                    if nextPrefix.Length <= maxAssistantChars then
                        nextPrefix
                    else
                        nextPrefix.Substring(0, maxAssistantChars)

                if combined.Length <= maxAssistantChars then
                    { state with
                        Assistant = combined
                        TurnStreamed = nextTurnStreamed
                    }
                else
                    { state with
                        Assistant = combined.Substring(0, maxAssistantChars)
                        AssistantTruncated = true
                        TurnStreamed = nextTurnStreamed
                    }
        | :? ReasoningDeltaEvent as delta when not (isNull (box delta)) ->
            let fragment = safe delta.Text

            if fragment = "" then
                state
            else
                let combined = state.Reasoning + fragment

                if combined.Length <= maxReasoningChars then
                    { state with Reasoning = combined }
                else
                    { state with
                        Reasoning = combined.Substring(0, maxReasoningChars)
                    }
        | :? ToolCallStartedEvent as started when not (isNull (box started)) ->
            let id = safe started.ToolCallId
            let name = safe started.ToolName
            let key = if id = "" then $"tool-{state.Order.Length}" else id

            if state.Tools.ContainsKey key then
                state
            else
                let card: ToolCard =
                    {
                        Id = key
                        Name = if name = "" then "unknown" else name
                        Status = Running
                        Output = ""
                        Overflow = 0
                        Expanded = false
                    }

                { state with
                    Tools = state.Tools.Add(key, card)
                    Order = state.Order @ [ key ]
                }
        | :? ToolCallOutputEvent as output when not (isNull (box output)) ->
            let id = safe output.ToolCallId
            let fragment = safe output.Output

            if fragment = "" then
                state
            else
                match state.Tools.TryFind id with
                | None ->
                    let cap = maxToolOutputChars
                    let kept, _, overflow = appendCapped "" fragment cap

                    let card: ToolCard =
                        {
                            Id = id
                            Name = "unknown"
                            Status = Running
                            Output = kept
                            Overflow = overflow
                            Expanded = false
                        }

                    { state with
                        Tools = state.Tools.Add(id, card)
                        Order = state.Order @ [ id ]
                    }
                | Some card ->
                    let cap =
                        if card.Expanded then
                            maxExpandedToolOutputChars
                        else
                            maxToolOutputChars

                    let combined = card.Output + fragment

                    let nextOutput, nextOverflow =
                        if combined.Length <= cap then
                            combined, 0
                        else
                            combined.Substring(0, cap), combined.Length - cap

                    { state with
                        Tools =
                            state.Tools.Add(
                                id,
                                { card with
                                    Output = nextOutput
                                    Overflow = nextOverflow
                                }
                            )
                    }
        | :? ToolCallCompletedEvent as completed when not (isNull (box completed)) ->
            let id = safe completed.ToolCallId
            let reason = safe completed.Error

            let status = if reason = "" then Succeeded else Failed reason

            match state.Tools.TryFind id with
            | None ->
                let card: ToolCard =
                    {
                        Id = id
                        Name = "unknown"
                        Status = status
                        Output = ""
                        Overflow = 0
                        Expanded = false
                    }

                { state with
                    Tools = state.Tools.Add(id, card)
                    Order = state.Order @ [ id ]
                }
            | Some card ->
                { state with
                    Tools = state.Tools.Add(id, { card with Status = status })
                }
        | :? PermissionRequestedEvent as asked when not (isNull (box asked)) ->
            let requestId = safe asked.RequestId
            let toolName = safe asked.ToolName

            let prompt: PermissionPrompt =
                {
                    RequestId = requestId
                    ToolName = if toolName = "" then "unknown" else toolName
                }

            if state.Permissions |> List.exists (fun pending -> pending.RequestId = requestId) then
                state
            else
                { state with
                    Permissions = state.Permissions @ [ prompt ]
                    Lifecycle = addMeta state.Lifecycle $"permission pending tool={prompt.ToolName} id={requestId}"
                }
        | :? PermissionResolvedEvent as resolved when not (isNull (box resolved)) ->
            let requestId = safe resolved.RequestId
            let decision = resolved.Decision.ToString()

            { state with
                Permissions = state.Permissions |> List.filter (fun pending -> pending.RequestId <> requestId)
                Lifecycle = addMeta state.Lifecycle $"permission resolved id={requestId} decision={decision}"
            }
        | :? QuestionAskedEvent as asked when not (isNull (box asked)) ->
            let questionId = safe asked.QuestionId
            let question = safe asked.Question

            let prompt: QuestionPrompt =
                {
                    QuestionId = questionId
                    Question = question
                }

            if state.Questions |> List.exists (fun pending -> pending.QuestionId = questionId) then
                state
            else
                { state with
                    Questions = state.Questions @ [ prompt ]
                    Lifecycle = addMeta state.Lifecycle $"question pending id={questionId}"
                }
        | :? QuestionAnsweredEvent as answered when not (isNull (box answered)) ->
            let questionId = safe answered.QuestionId

            { state with
                Questions = state.Questions |> List.filter (fun pending -> pending.QuestionId <> questionId)
                Lifecycle = addMeta state.Lifecycle $"question answered id={questionId}"
            }
        | :? UsageEvent as usage when not (isNull (box usage)) ->
            { state with
                System =
                    addMeta state.System $"usage input-tokens={usage.InputTokens} output-tokens={usage.OutputTokens}"
            }
        | :? CompactedEvent as compacted when not (isNull (box compacted)) ->
            { state with
                Lifecycle =
                    addMeta
                        state.Lifecycle
                        $"compacted before={compacted.BeforeEstimate} after={compacted.AfterEstimate}"
            }
        | :? CompactionFailedEvent as failed when not (isNull (box failed)) ->
            { state with
                Lifecycle = addMeta state.Lifecycle $"compaction failed: {safe failed.Reason}"
            }
        | :? TurnCompletedEvent ->
            { state with
                Lifecycle = addMeta state.Lifecycle "turn completed"
            }
        | :? TurnAbortedEvent as aborted when not (isNull (box aborted)) ->
            { state with
                Lifecycle = addMeta state.Lifecycle $"turn aborted: {safe aborted.Reason}"
            }
        | :? TurnFailedEvent as failed when not (isNull (box failed)) ->
            { state with
                Lifecycle = addMeta state.Lifecycle $"turn failed: {safe failed.Reason}"
            }
        | :? SessionClosedEvent ->
            { state with
                Lifecycle = addMeta state.Lifecycle "session closed"
            }
        | :? UserMessageEvent ->
            { state with
                System = addMeta state.System "user-message folded"
            }
        | :? ContextPrunedEvent as pruned when not (isNull (box pruned)) ->
            { state with
                System =
                    addMeta
                        state.System
                        $"pruned {pruned.PrunedCount} (before={pruned.BeforeEstimate} after={pruned.AfterEstimate})"
            }
        | :? SkillInvalidEvent as invalid when not (isNull (box invalid)) ->
            { state with
                System = addMeta state.System $"skill {safe invalid.SkillName} skipped: {safe invalid.Reason}"
            }
        | :? SkillLoadedEvent as loaded when not (isNull (box loaded)) ->
            let companions =
                if isNull (box loaded.Companions) then
                    0
                else
                    loaded.Companions.Count

            { state with
                System = addMeta state.System $"skill {safe loaded.SkillName} loaded ({companions} companions)"
            }
        | :? AgentInvalidEvent as invalid when not (isNull (box invalid)) ->
            { state with
                System = addMeta state.System $"agent {safe invalid.AgentName} kept: {safe invalid.Reason}"
            }
        | :? AgentSwitchedEvent as switched when not (isNull (box switched)) ->
            let previousRaw = safe switched.PreviousAgentId.Value
            let nextRaw = safe switched.NewAgentId.Value

            let previous =
                if previousRaw = "" then
                    "?"
                else
                    previousRaw.Substring(0, min 8 previousRaw.Length)

            let next =
                if nextRaw = "" then
                    "?"
                else
                    nextRaw.Substring(0, min 8 nextRaw.Length)

            { state with
                System = addMeta state.System $"agent switched {previous} -> {next}"
            }
        | _ ->
            { state with
                System = addMeta state.System $"event {evt.GetType().Name}"
            }

/// Folds an event and maintains chronological display blocks alongside the
/// information-parity model. Tool updates keep their original position.
/// Duplicate transport delivery repeats the identical durable identity and
/// folds nothing: the first delivery already rendered it, while distinct
/// same-text events carry distinct sequences and stay distinct.
let apply (state: RendererState) (evt: SessionEvent) : RendererState =
    let duplicate =
        not (isNull (box evt))
        && not (isNull (box state))
        && (match DotDedup.durableKeyOf evt with
            | Some identity -> state.Rendered.Contains identity
            | None -> false)

    if duplicate then
        state
    else
        let next = applyState state evt

        let append block =
            { next with
                Display = (next.Display @ [ block ]) |> List.rev |> List.truncate maxMetaLines |> List.rev
            }

        match evt with
        | :? TurnStartedEvent -> { next with StreamedTurn = false }
        | :? TextDeltaEvent as delta when not (String.IsNullOrEmpty delta.Text) ->
            let blocks =
                match List.rev next.Display with
                | AssistantText text :: rest when next.StreamedTurn ->
                    let value, _, _ = appendCapped text delta.Text maxAssistantChars
                    List.rev rest @ [ AssistantText value ]
                | _ ->
                    let value, _, _ = appendCapped "" delta.Text maxAssistantChars
                    next.Display @ [ AssistantText value ]

            { next with
                Display = blocks
                StreamedTurn = true
            }
        | :? ReasoningDeltaEvent as delta when not (String.IsNullOrEmpty delta.Text) ->
            let blocks =
                match List.rev next.Display with
                | ReasoningText text :: rest ->
                    let value, _, _ = appendCapped text delta.Text maxAssistantChars
                    List.rev rest @ [ ReasoningText value ]
                | _ -> next.Display @ [ ReasoningText delta.Text ]

            { next with Display = blocks }
        | :? ToolCallStartedEvent as tool -> append (ToolReference tool.ToolCallId)
        | :? TurnFailedEvent as failed -> append (ErrorText($"Error: {failed.Reason}"))
        | :? TurnAbortedEvent as aborted -> append (ErrorText($"Aborted: {aborted.Reason}"))
        | _ -> next

/// Folds a sequence of Subscribe events into the renderer state, oldest
/// first.
/// <param name="state">The starting renderer state.</param>
/// <param name="events">The events to fold. Null entries are ignored.</param>
/// <returns>The folded renderer state.</returns>
let applyAll (state: RendererState) (events: SessionEvent seq) : RendererState =
    if isNull (box events) then
        state
    else
        (state, events) ||> Seq.fold (fun current evt -> apply current evt)

/// Adds one engine diagnostic line (RESULT, ERROR, DEADLINE, SESSION,
/// COMPACT, ABORTED, SESSIONS, TREE, FORKED, RESUMED, MODEL, TEMPLATE,
/// EXPORTED, PERMISSION, QUESTION, and the rest of the REPL line contract)
/// to the viewport: the parity path for output the journal never carries.
/// Null lines are ignored; the tail window caps growth.
/// <param name="state">The current renderer state.</param>
/// <param name="line">The engine line to retain.</param>
/// <returns>The next renderer state.</returns>
let addLine (state: RendererState) (line: string | null) : RendererState =
    match line with
    | null -> state
    | text when String.IsNullOrWhiteSpace text && not state.InResult -> state
    | text ->
        let trimmed = text.Trim()

        if trimmed = "" && not state.InResult then
            state
        else
            let next =
                { state with
                    Diagnostics = addMeta state.Diagnostics trimmed
                }

            if trimmed.StartsWith("RESULT ", StringComparison.Ordinal) then
                let status = trimmed.Substring("RESULT ".Length).Trim()

                { next with
                    InResult = true
                    ResultStatus = status
                }
            elif trimmed = "END-RESULT" then
                { next with
                    InResult = false
                    ResultStatus = ""
                }
            elif trimmed.StartsWith("SESSION ", StringComparison.Ordinal) then
                next
            elif state.InResult then
                if state.ResultStatus <> "" && not (isCompletedStatus state.ResultStatus) then
                    // Terminal failure or abort: the streamed partial stays
                    // visible once and the truthful terminal outcome travels
                    // on the TurnFailed/TurnAborted event plus the RESULT
                    // diagnostic. Settlement carries no success text.
                    next
                elif text = "" then
                    // Blank separator inside a successful envelope: keeps
                    // paragraph structure for settlement-only content.
                    let blocks =
                        match List.rev state.Display with
                        | AssistantText previous :: rest ->
                            List.rev rest
                            @ [
                                AssistantText(previous + "\n" + text)
                            ]
                        | _ -> state.Display @ [ AssistantText text ]

                    { next with Display = blocks }
                else
                    let suffix = DotDedup.settlementSuffix state.TurnStreamed text

                    if suffix = "" then
                        // Already rendered: the settlement repeats the
                        // streamed prefix exactly.
                        next
                    elif
                        state.TurnStreamed <> ""
                        && text.StartsWith(state.TurnStreamed, StringComparison.Ordinal)
                    then
                        // Suffix continuation: extend the streamed block
                        // without repeating the observed prefix.
                        let blocks =
                            match List.rev state.Display with
                            | AssistantText previous :: rest -> List.rev rest @ [ AssistantText(previous + suffix) ]
                            | _ -> state.Display @ [ AssistantText suffix ]

                        { next with Display = blocks }
                    else
                        // Settlement-only, nonstreaming fallback, or an
                        // event/settlement race: render fully so valid
                        // output is never suppressed.
                        let blocks =
                            match List.rev state.Display with
                            | AssistantText previous :: rest ->
                                List.rev rest
                                @ [
                                    AssistantText(previous + "\n" + text)
                                ]
                            | _ -> state.Display @ [ AssistantText text ]

                        { next with Display = blocks }
            else
                { next with
                    Display =
                        (state.Display @ [ Notice trimmed ])
                        |> List.rev
                        |> List.truncate maxMetaLines
                        |> List.rev
                }

/// Retains the submitted text verbatim as a user cell, including indentation.
let addUserMessage (state: RendererState) (text: string) : RendererState =
    { state with
        Display =
            (state.Display @ [ UserText text ])
            |> List.rev
            |> List.truncate maxMetaLines
            |> List.rev
    }

/// Adds engine diagnostic lines in order.
/// <param name="state">The current renderer state.</param>
/// <param name="lines">The engine lines to retain.</param>
/// <returns>The next renderer state.</returns>
let addLines (state: RendererState) (lines: string seq) : RendererState =
    if isNull (box lines) then
        state
    else
        (state, lines) ||> Seq.fold addLine

// ──────────────────────────────────────────────────────────────────────────
// TUI ring fold and long-running progress (issue 334)

// The drain ring in DotTui folds engine lines into the viewport through
// addLine, except lines the journal fold already carries: live EVENT
// lines duplicate the OnEvent hook (the same Subscribe stream), so the
// ring drops them. TREE lines have no journal equivalent (they page
// ReadEventsAsync outside any turn), so they always fold: filtering them
// would hide /tree output fullscreen.
// True when an engine line duplicates the journal fold and the ring must
// drop it.
// <param name="line">The engine line. Null never duplicates.</param>
// <returns>True for live EVENT lines only.</returns>
let isJournalDuplicate (line: string | null) : bool =
    match line with
    | null -> false
    | text -> text.StartsWith("EVENT ", StringComparison.Ordinal)

/// Paints the /compact start marker: the key pump routes fire-and-forget
/// so input stays routable, and this line proves the compaction started
/// before its COMPACT completed/deferred/fenced line lands. Kept in the
/// existing diagnostics tail window.
/// <param name="state">The current renderer state.</param>
/// <returns>The next renderer state.</returns>
let markCompactRunning (state: RendererState) : RendererState = addLine state "compact running..."

/// Paints the /export start marker naming the file target: the key pump
/// routes fire-and-forget so input stays routable, and this line proves
/// the export started before its EXPORTED line lands. Kept in the
/// existing diagnostics tail window.
/// <param name="state">The current renderer state.</param>
/// <param name="target">The file target the user typed. Null or blank paints the bare marker.</param>
/// <returns>The next renderer state.</returns>
let markExportRunning (state: RendererState) (target: string | null) : RendererState =
    match target with
    | null -> addLine state "export running..."
    | text when String.IsNullOrWhiteSpace text -> addLine state "export running..."
    | text -> addLine state $"export running... {text.Trim()}"

// ──────────────────────────────────────────────────────────────────────────
// Markdown-lite plus truncation model.

// Renders markdown-lite readable structure: headings drop their leading
// hashes, fenced code blocks keep their fence plus language, bulleted and
// numbered lists keep one marker per item, and everything else passes
// through verbatim (the plain fallback where the library cannot do more).
// Tables and syntax highlighting are explicitly later polish.
// <param name="text">The accumulated assistant text.</param>
// <returns>The readable viewport lines.</returns>
let renderMarkdownLite (text: string | null) : string list =
    match text with
    | null -> []
    | content when String.IsNullOrEmpty content -> []
    | content ->
        content.Replace("\r\n", "\n").Replace('\r', '\n').Split('\n')
        |> List.ofArray
        |> List.map (fun raw ->
            let line = if isNull (box raw) then "" else raw

            let trimmed = line.TrimStart()

            if trimmed.StartsWith("```", StringComparison.Ordinal) then
                trimmed
            elif trimmed.StartsWith("#", StringComparison.Ordinal) then
                trimmed.TrimStart('#').Trim()
            elif
                trimmed.StartsWith("- ", StringComparison.Ordinal)
                || trimmed.StartsWith("* ", StringComparison.Ordinal)
                || trimmed.StartsWith("+ ", StringComparison.Ordinal)
            then
                "• " + trimmed.Substring(2).TrimStart()
            else
                line)

// ──────────────────────────────────────────────────────────────────────────
// Viewport blocks.

// The status text of one tool card: running, done, or failed.
let private statusText (status: ToolStatus) : string =
    match status with
    | Running -> "running"
    | Succeeded -> "done"
    | Failed _ -> "failed"

/// Renders one tool card as viewport lines: the name with its
/// running/completed/failed state, the truncated output with its expansion
/// affordance, and the failure reason when the call failed.
/// <param name="card">The card to render.</param>
/// <returns>The card lines.</returns>
let renderToolCard (card: ToolCard) : string list =
    let status = statusText card.Status
    let header = $"[tool {card.Name} {status}] id={card.Id}"

    let outputLines =
        if String.IsNullOrEmpty card.Output then
            []
        else
            card.Output.Replace("\r\n", "\n").Replace('\r', '\n').Split('\n')
            |> List.ofArray
            |> List.truncate 20

    let errorLines =
        match card.Status with
        | Failed reason when not (String.IsNullOrWhiteSpace reason) -> [ $"[error: {reason.Trim()}]" ]
        | Failed _ -> [ "[error]" ]
        | Running
        | Succeeded -> []

    let expansion =
        if card.Overflow > 0 && not card.Expanded then
            [
                $"[truncated {card.Overflow} chars, Ctrl+T to expand]"
            ]
        elif card.Expanded && card.Overflow > 0 then
            [
                $"[expanded, still truncated {card.Overflow} chars]"
            ]
        elif card.Expanded then
            [ "[expanded]" ]
        else
            []

    [ header ] @ outputLines @ errorLines @ expansion

/// Renders the whole renderer state as viewport lines: the streaming
/// assistant markdown-lite block, one card per tool call, the inline
/// permission/question widgets, the turn lifecycle markers, the retained
/// notices, and the engine diagnostics. Pure over the state so the
/// DotShell frame budget and the colorless path paint it without a TTY.
/// <param name="state">The renderer state to paint.</param>
/// <returns>The viewport lines, oldest first.</returns>
let toViewportLines (state: RendererState) : string list =
    if isNull (box state) then
        []
    else
        let assistantLines =
            if String.IsNullOrEmpty state.Assistant then
                []
            else
                let body = renderMarkdownLite state.Assistant

                let truncated =
                    if state.AssistantTruncated then
                        [
                            "[truncated, transcript cap reached]"
                        ]
                    else
                        []

                [ "assistant:" ] @ body @ truncated

        let reasoningLines =
            if String.IsNullOrWhiteSpace state.Reasoning then
                []
            else
                [
                    $"reasoning: {state.Reasoning.Trim()}"
                ]

        let cardLines =
            state.Order
            |> List.choose (fun id -> state.Tools.TryFind id)
            |> List.collect renderToolCard

        let permissionLines =
            state.Permissions
            |> List.map (fun pending ->
                $"[permission] tool={pending.ToolName} id={pending.RequestId} [a]llow once / [s]ession / [d]eny")

        let questionLines =
            state.Questions
            |> List.collect (fun pending ->
                [
                    $"[question] id={pending.QuestionId}: {pending.Question}"
                    "[answer]: (type then Enter)"
                ])

        assistantLines
        @ reasoningLines
        @ cardLines
        @ permissionLines
        @ questionLines
        @ state.Lifecycle
        @ state.System
        @ state.Diagnostics

// ──────────────────────────────────────────────────────────────────────────
// Inline prompts: keyboard and answer-field contracts.

/// Human-facing transcript without the plain REPL's wire-format envelopes.
/// Command results and errors remain visible; journal duplicates stay hidden.
let toDisplayLines (state: RendererState) : string list =
    let blocks =
        state.Display
        |> List.collect (function
            | UserText text -> [ "> " + text ]
            | AssistantText text -> [ ""; "Dot" ] @ renderMarkdownLite text @ [ "" ]
            | ReasoningText text -> renderMarkdownLite text
            | ToolReference id -> state.Tools.TryFind id |> Option.map renderToolCard |> Option.defaultValue []
            | ErrorText text -> renderMarkdownLite text
            | Notice text -> renderMarkdownLite text)

    let prompts =
        state.Permissions
        |> List.map (fun pending -> $"[permission] {pending.ToolName} · [a]llow once / [s]ession / [d]eny")

    let questions =
        state.Questions |> List.map (fun pending -> $"[question] {pending.Question}")

    blocks @ prompts @ questions

/// Preserves message semantics through layout instead of flattening every
/// response into indistinguishable console lines.
let toSessionCells (state: RendererState) : DotShell.SessionCell list =
    let cell style text : DotShell.SessionCell = { Style = style; Text = text }

    let blocks =
        state.Display
        |> List.choose (function
            | UserText text -> Some(cell DotShell.User text)
            | AssistantText text -> Some(cell DotShell.Assistant text)
            | ReasoningText text -> Some(cell DotShell.Reasoning text)
            | ToolReference id ->
                state.Tools.TryFind id
                |> Option.map (fun tool -> cell DotShell.Tool (renderToolCard tool |> String.concat "\n"))
            | ErrorText text -> Some(cell DotShell.Error text)
            | Notice text -> Some(cell DotShell.Plain text))

    let permissions =
        state.Permissions
        |> List.map (fun pending ->
            cell DotShell.Tool $"[permission] {pending.ToolName} · [a]llow once / [s]ession / [d]eny")

    let questions =
        state.Questions
        |> List.map (fun pending -> cell DotShell.Assistant $"[question] {pending.Question}")

    blocks @ permissions @ questions

// Maps one typed permission choice to the REPL-identical decision: s or
// session allows for the session, d or deny denies, and everything else
// (including a, allow, allow-once, and empty) allows once, exactly like the
// console reader path.
// <param name="text">The typed choice.</param>
// <returns>The decision to resume with.</returns>
let decisionForText (text: string | null) : PermissionDecisionKind =
    match text with
    | null -> PermissionDecisionKind.AllowOnce
    | content ->
        match content.Trim().ToLowerInvariant() with
        | "s"
        | "session" -> PermissionDecisionKind.AllowForSession
        | "d"
        | "deny" -> PermissionDecisionKind.Deny
        | _ -> PermissionDecisionKind.AllowOnce

/// Maps one permission key to its decision: a allows once, s allows for
/// the session, d denies (case-insensitive). Any other key is None: the
/// keystroke types instead of answering.
/// <param name="key">The console key just read.</param>
/// <returns>The decision, or None when the key does not answer.</returns>
let decisionForKey (key: ConsoleKeyInfo) : PermissionDecisionKind option =
    if key.KeyChar = '\u0000' then
        None
    else
        match Char.ToLowerInvariant key.KeyChar with
        | 'a' -> Some PermissionDecisionKind.AllowOnce
        | 's' -> Some PermissionDecisionKind.AllowForSession
        | 'd' -> Some PermissionDecisionKind.Deny
        | _ -> None

/// Maps one submitted question answer to the REPL-identical resume value:
/// null becomes empty, every other buffer (including blank) resumes
/// verbatim, exactly like the console ANSWER path.
/// <param name="text">The submitted answer buffer.</param>
/// <returns>The answer to resume with.</returns>
let answerForSubmit (text: string | null) : string =
    match text with
    | null -> ""
    | content -> content

/// True while a permission widget waits for a keyboard decision.
/// <param name="state">The renderer state.</param>
/// <returns>True when at least one permission pends.</returns>
let hasPendingPermission (state: RendererState) : bool =
    not (isNull (box state)) && not state.Permissions.IsEmpty

/// True while a question widget waits for an answer-field submit.
/// <param name="state">The renderer state.</param>
/// <returns>True when at least one question pends.</returns>
let hasPendingQuestion (state: RendererState) : bool =
    not (isNull (box state)) && not state.Questions.IsEmpty

/// The first pending permission, oldest first.
/// <param name="state">The renderer state.</param>
/// <returns>The oldest pending permission, or None.</returns>
let firstPermission (state: RendererState) : PermissionPrompt option =
    if isNull (box state) then
        None
    else
        state.Permissions |> List.tryHead

/// The first pending question, oldest first.
/// <param name="state">The renderer state.</param>
/// <returns>The oldest pending question, or None.</returns>
let firstQuestion (state: RendererState) : QuestionPrompt option =
    if isNull (box state) then
        None
    else
        state.Questions |> List.tryHead

/// Toggles one tool card between collapsed and expanded: expansion lifts
/// the per-call cap so the viewport shows more of a truncated output.
/// <param name="state">The current renderer state.</param>
/// <param name="toolCallId">The card to toggle.</param>
/// <returns>The next renderer state.</returns>
let toggleExpanded (state: RendererState) (toolCallId: string | null) : RendererState =
    if isNull (box state) then
        empty
    else
        let id = safe toolCallId

        match state.Tools.TryFind id with
        | None -> state
        | Some card ->
            { state with
                Tools =
                    state.Tools.Add(
                        id,
                        { card with
                            Expanded = not card.Expanded
                        }
                    )
            }

// ──────────────────────────────────────────────────────────────────────────
// Parity: nothing the REPL prints is lost in the TUI.

// Names the key tokens one Subscribe event must contribute to the viewport:
// the same tool names, request ids, reasons, estimates, and markers the
// REPL renderEvent line carries. Empty for null events.
// <param name="evt">The event to cover.</param>
// <returns>The tokens the viewport must contain.</returns>
let parityTokens (evt: SessionEvent) : string list =
    if isNull (box evt) then
        []
    else
        match evt with
        | :? TurnStartedEvent -> [ "turn running" ]
        | :? TextDeltaEvent as delta when not (isNull (box delta)) ->
            let text = safe delta.Text

            if text = "" then [] else [ text ]
        | :? ReasoningDeltaEvent as delta when not (isNull (box delta)) ->
            let text = safe delta.Text

            if text = "" then [] else [ "reasoning:" ]
        | :? ToolCallStartedEvent as started when not (isNull (box started)) ->
            [
                safe started.ToolName
                safe started.ToolCallId
            ]
        | :? ToolCallOutputEvent as output when not (isNull (box output)) ->
            let fragment = safe output.Output

            if fragment = "" then
                [ safe output.ToolCallId ]
            else
                [
                    fragment.Substring(0, min fragment.Length 20)
                    safe output.ToolCallId
                ]
        | :? ToolCallCompletedEvent as completed when not (isNull (box completed)) ->
            let reason = safe completed.Error

            if reason = "" then
                [ safe completed.ToolCallId; "done" ]
            else
                [ safe completed.ToolCallId; reason ]
        | :? PermissionRequestedEvent as asked when not (isNull (box asked)) ->
            [
                safe asked.ToolName
                safe asked.RequestId
                "permission"
            ]
        | :? PermissionResolvedEvent as resolved when not (isNull (box resolved)) ->
            [
                safe resolved.RequestId
                resolved.Decision.ToString()
            ]
        | :? QuestionAskedEvent as asked when not (isNull (box asked)) ->
            [
                safe asked.QuestionId
                safe asked.Question
                "question"
            ]
        | :? QuestionAnsweredEvent as answered when not (isNull (box answered)) ->
            [
                safe answered.QuestionId
                "question answered"
            ]
        | :? UsageEvent as usage when not (isNull (box usage)) ->
            [
                usage.InputTokens.ToString()
                usage.OutputTokens.ToString()
                "usage"
            ]
        | :? CompactedEvent as compacted when not (isNull (box compacted)) ->
            [
                compacted.BeforeEstimate.ToString()
                compacted.AfterEstimate.ToString()
                "compacted"
            ]
        | :? CompactionFailedEvent as failed when not (isNull (box failed)) -> [ safe failed.Reason ]
        | :? TurnCompletedEvent -> [ "turn completed" ]
        | :? TurnAbortedEvent as aborted when not (isNull (box aborted)) -> [ safe aborted.Reason ]
        | :? TurnFailedEvent as failed when not (isNull (box failed)) -> [ safe failed.Reason ]
        | :? SessionClosedEvent -> [ "session closed" ]
        | :? UserMessageEvent -> [ "user-message" ]
        | :? ContextPrunedEvent as pruned when not (isNull (box pruned)) ->
            [
                pruned.PrunedCount.ToString()
                "pruned"
            ]
        | :? SkillInvalidEvent as invalid when not (isNull (box invalid)) ->
            [
                safe invalid.SkillName
                safe invalid.Reason
            ]
        | :? SkillLoadedEvent as loaded when not (isNull (box loaded)) -> [ safe loaded.SkillName ]
        | :? AgentInvalidEvent as invalid when not (isNull (box invalid)) ->
            [
                safe invalid.AgentName
                safe invalid.Reason
            ]
        | :? AgentSwitchedEvent -> [ "agent switched" ]
        | _ -> [ evt.GetType().Name ]

/// True when the renderer covers one Subscribe event: every known subtype
/// folds into a viewport token, so nothing the REPL prints disappears in
/// the TUI (information parity, presentation upgrade).
/// <param name="evt">The event to check.</param>
/// <returns>True when the viewport retains the event.</returns>
let coversEvent (evt: SessionEvent) : bool =
    if isNull (box evt) then
        false
    else
        let lines = toViewportLines (apply empty evt)
        let joined = String.Join("\n", lines)

        parityTokens evt
        |> List.filter (fun token -> not (String.IsNullOrWhiteSpace token))
        |> List.forall (fun token -> joined.Contains(token, StringComparison.Ordinal))
