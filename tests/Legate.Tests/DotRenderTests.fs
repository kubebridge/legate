// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.DotRenderTests

open System
open Legate
open Dot.DotRender
open FsUnit.Xunit
open Xunit

// Streaming renderer proofs (issue 333): the pure DotRender fold over the
// same Subscribe stream the REPL prints (delta accumulation, tool cards,
// inline approval/question widgets, lifecycle markers, truncation with
// expansion) plus the information-parity table over every SessionEvent
// subtype and engine line() prefix. TTY-free: every case runs against pure
// state inputs, so cards, prompts, lifecycle, and truncation are proven
// headlessly without a terminal. The linked DotRender.fs compiles here
// without referencing the Dot exe.

let private stamp = DateTimeOffset.UtcNow

let private at (sequence: int) : Nullable<int64> = Nullable<int64>(int64 sequence)

let private sid () : SessionId = SessionId.New()
let private tid () : TurnId = TurnId.New()

let private started (s: SessionId) (t: TurnId) : SessionEvent =
    TurnStartedEvent(s, t, at 1, stamp) :> SessionEvent

let private delta (s: SessionId) (t: TurnId) (text: string) : SessionEvent =
    TextDeltaEvent(s, t, at 2, stamp, text) :> SessionEvent

let private reasoning (s: SessionId) (t: TurnId) (text: string) : SessionEvent =
    ReasoningDeltaEvent(s, t, at 3, stamp, text) :> SessionEvent

let private callStarted (s: SessionId) (t: TurnId) (id: string) (name: string) : SessionEvent =
    ToolCallStartedEvent(s, t, at 4, stamp, id, name) :> SessionEvent

let private callOutput (s: SessionId) (t: TurnId) (id: string) (text: string) : SessionEvent =
    ToolCallOutputEvent(s, t, at 5, stamp, id, text) :> SessionEvent

let private callCompleted (s: SessionId) (t: TurnId) (id: string) (error: string | null) : SessionEvent =
    ToolCallCompletedEvent(s, t, at 6, stamp, id, error) :> SessionEvent

let private allEvents (s: SessionId) (t: TurnId) : SessionEvent list =
    [
        started s t
        delta s t "hello"
        reasoning s t "thinking"
        callStarted s t "call-1" "read_file"
        callOutput s t "call-1" "file-bytes"
        callCompleted s t "call-1" null
        PermissionRequestedEvent(s, t, at 7, stamp, "req-1", "exec") :> SessionEvent
        PermissionResolvedEvent(s, t, at 8, stamp, "req-1", PermissionDecisionKind.AllowOnce) :> SessionEvent
        QuestionAskedEvent(s, t, at 9, stamp, "q-1", "continue?") :> SessionEvent
        QuestionAnsweredEvent(s, t, at 10, stamp, "q-1", "yes") :> SessionEvent
        UsageEvent(s, t, at 11, stamp, 10L, 20L) :> SessionEvent
        CompactedEvent(s, t, at 12, stamp, 100L, 40L) :> SessionEvent
        CompactionFailedEvent(s, t, at 13, stamp, "busy") :> SessionEvent
        TurnCompletedEvent(s, t, at 14, stamp) :> SessionEvent
        TurnAbortedEvent(s, t, at 15, stamp, StopCause.ExplicitAbort, "aborted") :> SessionEvent
        TurnFailedEvent(s, t, at 16, stamp, "boom") :> SessionEvent
        SessionClosedEvent(s, t, at 17, stamp) :> SessionEvent
        UserMessageEvent(s, t, at 18, stamp, UserMessage.Text("fold me")) :> SessionEvent
        ContextPrunedEvent(s, t, at 19, stamp, 2, 100L, 40L) :> SessionEvent
        SkillInvalidEvent(s, t, at 20, stamp, "review", "no SKILL.md") :> SessionEvent
        SkillLoadedEvent(
            s,
            t,
            at 21,
            stamp,
            "review",
            ResizeArray<string>([| "helper.md" |]) :> System.Collections.Generic.IReadOnlyList<string>
        )
        :> SessionEvent
        AgentInvalidEvent(s, t, at 22, stamp, "helper", "missing description") :> SessionEvent
        AgentSwitchedEvent(s, t, at 23, stamp, AgentId.New(), AgentId.New()) :> SessionEvent
    ]

// ──────────────────────────────────────────────────────────────────────────
// Fold: Subscribe events into viewport blocks

[<Fact>]
let ``Display keeps streaming text and tool cards in conversation order`` () =
    let s, t = sid (), tid ()

    let state =
        addLine empty "> inspect this"
        |> fun state ->
            applyAll
                state
                [
                    started s t
                    delta s t "Before"
                    callStarted s t "read-1" "read_file"
                    delta s t "After"
                    callCompleted s t "read-1" null
                ]

    let lines = toDisplayLines state
    let before = lines |> List.findIndex ((=) "Before")
    let tool = lines |> List.findIndex (fun line -> line.Contains("read_file"))
    let after = lines |> List.findIndex ((=) "After")
    Assert.True(before < tool && tool < after)
    lines.Head |> should equal "> inspect this"

[<Fact>]
let ``Streamed result is not duplicated by the REPL result envelope`` () =
    let s, t = sid (), tid ()
    let state = applyAll empty [ started s t; delta s t "hello" ]

    let state =
        addLines
            state
            [
                "RESULT Completed"
                "hello"
                "END-RESULT"
            ]

    toDisplayLines state
    |> List.filter ((=) "hello")
    |> List.length
    |> should equal 1

    let fallback =
        addLines
            empty
            [
                "RESULT Completed"
                "hello"
                "END-RESULT"
            ]

    toSessionCells fallback
    |> should
        equal
        [
            {
                Dot.DotShell.Style = Dot.DotShell.Assistant
                Dot.DotShell.Text = "hello"
            }
        ]

[<Fact>]
let ``User reasoning and assistant retain distinct session cell styles`` () =
    let s, t = sid (), tid ()

    let state =
        addUserMessage empty "hi\n  keep my indentation"
        |> fun state ->
            applyAll
                state
                [
                    started s t
                    reasoning s t "Considering "
                    reasoning s t "the request.\n\nReady."
                    delta s t "Hello!"
                ]

    let cells = toSessionCells state

    cells
    |> List.map (fun cell -> cell.Style)
    |> should
        equal
        [
            Dot.DotShell.User
            Dot.DotShell.Reasoning
            Dot.DotShell.Assistant
        ]

    cells[0].Text |> should equal "hi\n  keep my indentation"
    cells[1].Text |> should equal "Considering the request.\n\nReady."
    cells[2].Text |> should equal "Hello!"

[<Fact>]
let ``Non-streamed results preserve paragraphs and code indentation`` () =
    let state =
        addLines
            empty
            [
                "RESULT Completed"
                "First paragraph."
                ""
                "    code"
                "END-RESULT"
            ]

    let cells = toSessionCells state
    cells.Length |> should equal 1
    cells.Head.Style |> should equal Dot.DotShell.Assistant
    cells.Head.Text |> should equal "First paragraph.\n\n    code"

[<Fact>]
let ``Turn errors are separate error cells`` () =
    let s, t = sid (), tid ()

    let state =
        addUserMessage empty "hi"
        |> fun state -> apply state (TurnFailedEvent(s, t, at 2, stamp, "unavailable"))

    let cells = toSessionCells state

    cells
    |> List.map (fun cell -> cell.Style)
    |> should
        equal
        [
            Dot.DotShell.User
            Dot.DotShell.Error
        ]

[<Fact>]
let ``Text deltas accumulate into one assistant block`` () =
    let s, t = sid (), tid ()

    let state =
        empty
        |> fun current -> apply current (delta s t "hel") |> fun next -> apply next (delta s t "lo")

    state.Assistant |> should equal "hello"

    let lines = toViewportLines state

    lines
    |> List.exists (fun line -> line.Contains("hello", StringComparison.Ordinal))
    |> should equal true

[<Fact>]
let ``Null events and null deltas never throw`` () =
    let s, t = sid (), tid ()
    let state = apply empty Unchecked.defaultof<SessionEvent>

    state |> should equal empty

    let nullDelta =
        TextDeltaEvent(s, t, at 2, stamp, Unchecked.defaultof<string>) :> SessionEvent

    let next = apply empty nullDelta
    next.Assistant |> should equal ""

[<Fact>]
let ``Tool started output completed correlate by id`` () =
    let s, t = sid (), tid ()

    let state =
        empty
        |> fun current -> apply current (callStarted s t "call-9" "exec")
        |> fun current -> apply current (callOutput s t "call-9" "out-")
        |> fun current -> apply current (callOutput s t "call-9" "bytes")
        |> fun current -> apply current (callCompleted s t "call-9" null)

    state.Order |> should equal [ "call-9" ]

    match state.Tools.TryFind "call-9" with
    | None -> failwith "expected card call-9"
    | Some card ->
        card.Name |> should equal "exec"
        card.Output |> should equal "out-bytes"
        card.Status |> should equal Succeeded
        card.Overflow |> should equal 0

    let lines = toViewportLines state

    lines
    |> List.exists (fun line -> line.Contains("exec", StringComparison.Ordinal))
    |> should equal true

    lines
    |> List.exists (fun line -> line.Contains("done", StringComparison.Ordinal))
    |> should equal true

[<Fact>]
let ``Permission request opens an inline widget and resolve clears it`` () =
    let s, t = sid (), tid ()

    let asked =
        PermissionRequestedEvent(s, t, at 7, stamp, "req-7", "exec") :> SessionEvent

    let pending = apply empty asked

    hasPendingPermission pending |> should equal true

    firstPermission pending
    |> should
        equal
        (Some
            {
                RequestId = "req-7"
                ToolName = "exec"
            })

    let lines = toViewportLines pending

    lines
    |> List.exists (fun line -> line.Contains("req-7", StringComparison.Ordinal))
    |> should equal true

    lines
    |> List.exists (fun line -> line.Contains("[permission]", StringComparison.Ordinal))
    |> should equal true

    let resolved =
        PermissionResolvedEvent(s, t, at 8, stamp, "req-7", PermissionDecisionKind.Deny) :> SessionEvent

    let settled = apply pending resolved
    hasPendingPermission settled |> should equal false

[<Fact>]
let ``Question asked opens an answer field and answered clears it`` () =
    let s, t = sid (), tid ()

    let asked =
        QuestionAskedEvent(s, t, at 9, stamp, "q-7", "continue?") :> SessionEvent

    let pending = apply empty asked

    hasPendingQuestion pending |> should equal true

    firstQuestion pending
    |> should
        equal
        (Some
            {
                QuestionId = "q-7"
                Question = "continue?"
            })

    let lines = toViewportLines pending

    lines
    |> List.exists (fun line -> line.Contains("continue?", StringComparison.Ordinal))
    |> should equal true

    lines
    |> List.exists (fun line -> line.Contains("[answer]", StringComparison.Ordinal))
    |> should equal true

    let answered =
        QuestionAnsweredEvent(s, t, at 10, stamp, "q-7", "yes") :> SessionEvent

    let settled = apply pending answered
    hasPendingQuestion settled |> should equal false

// ──────────────────────────────────────────────────────────────────────────
// Markdown-lite plus truncation model

[<Fact>]
let ``Headings code fences and lists keep readable structure`` () =
    renderMarkdownLite "# Title" |> should equal [ "Title" ]
    renderMarkdownLite "## Section" |> should equal [ "Section" ]
    renderMarkdownLite "```fsharp" |> should equal [ "```fsharp" ]
    renderMarkdownLite "- item" |> should equal [ "• item" ]
    renderMarkdownLite "* item" |> should equal [ "• item" ]
    renderMarkdownLite "1. item" |> should equal [ "1. item" ]
    renderMarkdownLite "plain" |> should equal [ "plain" ]
    (renderMarkdownLite null |> List.isEmpty) |> should equal true
    (renderMarkdownLite "" |> List.isEmpty) |> should equal true

[<Fact>]
let ``Huge tool output truncates with an expansion affordance`` () =
    let s, t = sid (), tid ()
    let huge = String.replicate (maxToolOutputChars + 500) "x"

    let state =
        empty
        |> fun current -> apply current (callStarted s t "big" "exec")
        |> fun current -> apply current (callOutput s t "big" huge)

    match state.Tools.TryFind "big" with
    | None -> failwith "expected card big"
    | Some card ->
        card.Output.Length |> should equal maxToolOutputChars
        (card.Overflow > 0) |> should equal true

    let lines = toViewportLines state

    lines
    |> List.exists (fun line -> line.Contains("truncated", StringComparison.Ordinal))
    |> should equal true

    lines
    |> List.exists (fun line -> line.Contains("Ctrl+T", StringComparison.Ordinal))
    |> should equal true

    let expanded = toggleExpanded state "big"

    match expanded.Tools.TryFind "big" with
    | None -> failwith "expected card big"
    | Some card -> card.Expanded |> should equal true

[<Fact>]
let ``Huge assistant deltas cap instead of growing without bound`` () =
    let s, t = sid (), tid ()
    let huge = String.replicate (maxAssistantChars + 100) "y"
    let state = apply empty (delta s t huge)

    state.Assistant.Length |> should equal maxAssistantChars
    state.AssistantTruncated |> should equal true

    let lines = toViewportLines state

    lines
    |> List.exists (fun line -> line.Contains("truncated", StringComparison.Ordinal))
    |> should equal true

[<Fact>]
let ``Meta lines keep a tail window`` () =
    let mutable state = empty

    for n in 1 .. (maxMetaLines + 50) do
        state <- addLine state $"LINE {n}"

    state.Diagnostics.Length |> should equal maxMetaLines
    state.Diagnostics |> List.head |> should equal $"LINE 51"

    let lines = toViewportLines state

    lines
    |> List.exists (fun line -> line = $"LINE {maxMetaLines + 50}")
    |> should equal true

    lines |> List.exists (fun line -> line = "LINE 1") |> should equal false

// ──────────────────────────────────────────────────────────────────────────
// Tool-call cards

[<Fact>]
let ``Running completed and failed states paint distinctly`` () =
    let running: ToolCard =
        {
            Id = "a"
            Name = "exec"
            Status = Running
            Output = ""
            Overflow = 0
            Expanded = false
        }

    let doneCard = { running with Status = Succeeded }
    let failedCard = { running with Status = Failed "boom" }

    renderToolCard running |> List.head |> should equal "[tool exec running] id=a"
    renderToolCard doneCard |> List.head |> should equal "[tool exec done] id=a"

    let failedLines = renderToolCard failedCard
    failedLines |> List.head |> should equal "[tool exec failed] id=a"

    failedLines
    |> List.exists (fun line -> line.Contains("boom", StringComparison.Ordinal))
    |> should equal true

[<Fact>]
let ``Unknown tool output still cards by id`` () =
    let s, t = sid (), tid ()
    let state = apply empty (callOutput s t "orphan" "bytes")

    match state.Tools.TryFind "orphan" with
    | None -> failwith "expected orphan card"
    | Some card ->
        card.Name |> should equal "unknown"
        card.Output |> should equal "bytes"

// ──────────────────────────────────────────────────────────────────────────
// Turn lifecycle and parity checklist

[<Fact>]
let ``Lifecycle markers name running settled aborted and failed in place`` () =
    let s, t = sid (), tid ()

    let state =
        empty
        |> fun current -> apply current (started s t)
        |> fun current -> apply current (TurnCompletedEvent(s, t, at 14, stamp) :> SessionEvent)
        |> fun current ->
            apply current (TurnAbortedEvent(s, t, at 15, stamp, StopCause.ExplicitAbort, "nope") :> SessionEvent)
        |> fun current -> apply current (TurnFailedEvent(s, t, at 16, stamp, "boom") :> SessionEvent)
        |> fun current -> apply current (SessionClosedEvent(s, t, at 17, stamp) :> SessionEvent)

    let lines = toViewportLines state

    lines
    |> List.exists (fun line -> line.Contains("turn running", StringComparison.Ordinal))
    |> should equal true

    lines
    |> List.exists (fun line -> line.Contains("turn completed", StringComparison.Ordinal))
    |> should equal true

    lines
    |> List.exists (fun line -> line.Contains("nope", StringComparison.Ordinal))
    |> should equal true

    lines
    |> List.exists (fun line -> line.Contains("boom", StringComparison.Ordinal))
    |> should equal true

    lines
    |> List.exists (fun line -> line.Contains("session closed", StringComparison.Ordinal))
    |> should equal true

[<Fact>]
let ``Compaction prune usage skill and agent notices are retained`` () =
    let s, t = sid (), tid ()
    let state = applyAll empty (allEvents s t)
    let lines = toViewportLines state
    let joined = String.Join("\n", lines)

    joined.Contains("compacted before=100 after=40", StringComparison.Ordinal)
    |> should equal true

    joined.Contains("compaction failed: busy", StringComparison.Ordinal)
    |> should equal true

    joined.Contains("usage input-tokens=10 output-tokens=20", StringComparison.Ordinal)
    |> should equal true

    joined.Contains("pruned 2", StringComparison.Ordinal) |> should equal true
    joined.Contains("review", StringComparison.Ordinal) |> should equal true
    joined.Contains("agent switched", StringComparison.Ordinal) |> should equal true
    joined.Contains("user-message", StringComparison.Ordinal) |> should equal true

[<Fact>]
let ``Every SessionEvent subtype is covered by the viewport`` () =
    let s, t = sid (), tid ()

    for evt in allEvents s t do
        coversEvent evt |> should equal true

    coversEvent Unchecked.defaultof<SessionEvent> |> should equal false

[<Fact>]
let ``Parity tokens name the REPL detail for every subtype`` () =
    let s, t = sid (), tid ()

    for evt in allEvents s t do
        let tokens = parityTokens evt
        (tokens.IsEmpty) |> should equal false

    (parityTokens Unchecked.defaultof<SessionEvent> |> List.isEmpty)
    |> should equal true

[<Fact>]
let ``Engine diagnostic lines are retained for parity`` () =
    let prefixes =
        [
            "RESULT Completed"
            "END-RESULT"
            "DEADLINE timed out"
            "ERROR boom"
            "COMPACT completed 1->2"
            "ABORTED"
            "SESSION abc title"
            "SESSIONS 2"
            "TREE 3 events"
            "FORKED abc from def up-to 3"
            "RESUMED abc"
            "RESUME-FAILED nope"
            "MODEL scripted/scripted"
            "MODEL-SWITCHED anthropic/claude"
            "TEMPLATE review"
            "TEMPLATES review, commit"
            "UNKNOWN-COMMAND /nope"
            "EXPORTED 3 events to out.jsonl"
            "PERMISSION tool=exec id=req-1 [a]llow once"
            "QUESTION id=q-1: continue?"
            "ANSWER:"
            "Dot REPL (SQLite session store"
            "Commands: /new"
            "> "
        ]

    let state = addLines empty prefixes
    let lines = toViewportLines state

    for prefix in prefixes do
        let trimmed = prefix.Trim()

        if trimmed <> "" then
            lines
            |> List.exists (fun line -> line.Contains(trimmed, StringComparison.Ordinal))
            |> should equal true

    addLine empty null |> should equal empty
    addLine empty "   " |> should equal empty

// ──────────────────────────────────────────────────────────────────────────
// Inline permission prompts owning the #332 deferral

[<Fact>]
let ``Permission keys map a session deny without typing`` () =
    let key (value: char) : ConsoleKeyInfo =
        ConsoleKeyInfo(value, ConsoleKey.A, false, false, false)

    decisionForKey (key 'a') |> should equal (Some PermissionDecisionKind.AllowOnce)
    decisionForKey (key 'A') |> should equal (Some PermissionDecisionKind.AllowOnce)

    decisionForKey (key 's')
    |> should equal (Some PermissionDecisionKind.AllowForSession)

    decisionForKey (key 'S')
    |> should equal (Some PermissionDecisionKind.AllowForSession)

    decisionForKey (key 'd') |> should equal (Some PermissionDecisionKind.Deny)
    decisionForKey (key 'D') |> should equal (Some PermissionDecisionKind.Deny)
    decisionForKey (key 'x') |> should equal None

    decisionForKey (ConsoleKeyInfo('\u0000', ConsoleKey.Enter, false, false, false))
    |> should equal None

[<Fact>]
let ``Permission text matches the REPL console reader exactly`` () =
    decisionForText "s" |> should equal PermissionDecisionKind.AllowForSession
    decisionForText "session" |> should equal PermissionDecisionKind.AllowForSession
    decisionForText "d" |> should equal PermissionDecisionKind.Deny
    decisionForText "deny" |> should equal PermissionDecisionKind.Deny
    decisionForText "a" |> should equal PermissionDecisionKind.AllowOnce
    decisionForText "" |> should equal PermissionDecisionKind.AllowOnce
    decisionForText null |> should equal PermissionDecisionKind.AllowOnce
    decisionForText "anything-else" |> should equal PermissionDecisionKind.AllowOnce

// ──────────────────────────────────────────────────────────────────────────
// Inline question prompts

[<Fact>]
let ``Question answers resume verbatim like the REPL`` () =
    answerForSubmit "yes" |> should equal "yes"
    answerForSubmit "" |> should equal ""
    answerForSubmit null |> should equal ""
    answerForSubmit "  spaced  " |> should equal "  spaced  "

// ──────────────────────────────────────────────────────────────────────────
// Viewport and loop wiring

[<Fact>]
let ``Viewport blocks paint inside the DotShell frame budget`` () =
    let s, t = sid (), tid ()

    let state =
        empty
        |> fun current -> apply current (delta s t "# Title")
        |> fun current -> apply current (callStarted s t "call-1" "exec")
        |> fun current -> apply current (PermissionRequestedEvent(s, t, at 7, stamp, "req-1", "exec") :> SessionEvent)

    let viewport = toViewportLines state
    (viewport.IsEmpty) |> should equal false

    let frame =
        Dot.DotShell.renderFrameWithInput
            80
            24
            false
            viewport
            "session abc | model scripted/scripted | WaitingForInput"
            [ "> " ]

    frame.Contains("assistant:", StringComparison.Ordinal) |> should equal true

    frame.Contains("[tool exec running]", StringComparison.Ordinal)
    |> should equal true

    frame.Contains("[permission]", StringComparison.Ordinal) |> should equal true

[<Fact>]
let ``Colorless viewport strips every escape`` () =
    let s, t = sid (), tid ()
    let state = apply empty (delta s t "hello")

    let frame =
        Dot.DotShell.renderFrameWithInput 80 24 false (toViewportLines state) "status" [ "> " ]

    frame.Contains("\u001b") |> should equal false

[<Fact>]
let ``Toggling an unknown card keeps state`` () =
    toggleExpanded empty "missing" |> should equal empty
    toggleExpanded empty null |> should equal empty

// ──────────────────────────────────────────────────────────────────────────
// TUI ring fold and long-running progress (issue 334): the drain ring
// drops only live EVENT lines (they duplicate the OnEvent journal fold);
// TREE lines and every other diagnostic fold into the viewport, and
// /compact plus /export paint start markers before their completion
// lines land.

[<Fact>]
let ``Only live EVENT lines duplicate the journal fold`` () =
    isJournalDuplicate "EVENT seq=1 TurnStartedEvent" |> should equal true
    isJournalDuplicate "TREE 3 events" |> should equal false
    isJournalDuplicate "TREE seq=1 TurnStartedEvent" |> should equal false
    isJournalDuplicate "RESULT Completed" |> should equal false
    isJournalDuplicate "COMPACT completed 1->2" |> should equal false
    isJournalDuplicate "EXPORTED 3 events to out.jsonl" |> should equal false
    isJournalDuplicate "" |> should equal false
    isJournalDuplicate null |> should equal false

[<Fact>]
let ``Tree output folds into the viewport`` () =
    let state =
        addLines
            empty
            [
                "TREE 2 events"
                "TREE seq=1 TurnStartedEvent"
            ]

    let lines = toViewportLines state

    lines
    |> List.exists (fun line -> line.Contains("TREE 2 events", StringComparison.Ordinal))
    |> should equal true

    lines
    |> List.exists (fun line -> line.Contains("TREE seq=1", StringComparison.Ordinal))
    |> should equal true

[<Fact>]
let ``Compact progress paints before its completion line`` () =
    let state = markCompactRunning empty
    let lines = toViewportLines state

    lines
    |> List.exists (fun line -> line.Contains("compact running", StringComparison.Ordinal))
    |> should equal true

[<Fact>]
let ``Export progress names its target`` () =
    let state = markExportRunning empty "out.jsonl"
    let lines = toViewportLines state

    lines
    |> List.exists (fun line -> line.Contains("export running... out.jsonl", StringComparison.Ordinal))
    |> should equal true

    let bare = markExportRunning empty null
    let bareLines = toViewportLines bare

    bareLines
    |> List.exists (fun line -> line.Contains("export running...", StringComparison.Ordinal))
    |> should equal true

    let blank = markExportRunning empty "   "
    let blankLines = toViewportLines blank

    blankLines
    |> List.exists (fun line -> line = "export running...")
    |> should equal true

[<Fact>]
let ``Progress markers keep the diagnostics tail window`` () =
    let mutable state = empty

    for _ in 1 .. (maxMetaLines + 50) do
        state <- markCompactRunning state

    state.Diagnostics.Length |> should equal maxMetaLines
