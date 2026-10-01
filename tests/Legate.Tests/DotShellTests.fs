// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.DotShellTests

open System
open Legate
open Dot.DotShell
open FsUnit.Xunit
open Xunit

// Layout shell proofs (issue 331): the pure DotShell frame regions
// (banner, transcript viewport, status bar), the Subscribe-driven
// turn-state mapping, and the alternate-screen teardown constants. TTY-free:
// every case runs against pure width/height inputs, so resize reflow and
// the colorless path are proven without a terminal. The linked DotShell.fs
// compiles here without referencing the Dot exe.

let private freshIds () : SessionId * TurnId = SessionId.New(), TurnId.New()

let private stamp = DateTimeOffset.UtcNow

let private at (sequence: int) : Nullable<int64> = Nullable<int64>(int64 sequence)

let private started (sid: SessionId) (tid: TurnId) : SessionEvent =
    TurnStartedEvent(sid, tid, at 1, stamp) :> SessionEvent

let private delta (sid: SessionId) (tid: TurnId) : SessionEvent =
    TextDeltaEvent(sid, tid, at 2, stamp, "hello") :> SessionEvent

let private permissionAsked (sid: SessionId) (tid: TurnId) : SessionEvent =
    PermissionRequestedEvent(sid, tid, at 3, stamp, "req-1", "read_file") :> SessionEvent

let private permissionAnswered (sid: SessionId) (tid: TurnId) : SessionEvent =
    PermissionResolvedEvent(sid, tid, at 4, stamp, "req-1", PermissionDecisionKind.AllowOnce) :> SessionEvent

let private questionAsked (sid: SessionId) (tid: TurnId) : SessionEvent =
    QuestionAskedEvent(sid, tid, at 3, stamp, "q-1", "continue?") :> SessionEvent

let private questionAnswered (sid: SessionId) (tid: TurnId) : SessionEvent =
    QuestionAnsweredEvent(sid, tid, at 4, stamp, "q-1", "yes") :> SessionEvent

let private completed (sid: SessionId) (tid: TurnId) : SessionEvent =
    TurnCompletedEvent(sid, tid, at 5, stamp) :> SessionEvent

let private aborted (sid: SessionId) (tid: TurnId) : SessionEvent =
    TurnAbortedEvent(sid, tid, at 5, stamp, StopCause.ExplicitAbort, "aborted") :> SessionEvent

let private failed (sid: SessionId) (tid: TurnId) : SessionEvent =
    TurnFailedEvent(sid, tid, at 5, stamp, "boom") :> SessionEvent

let private statusFor (sid: SessionId) : string =
    statusText sid (ModelReference.Parse("scripted/scripted")) SessionState.Running

[<Fact>]
let ``Banner heads every frame above the transcript`` () =
    let frame =
        renderFrame 80 24 false [ "EVENT seq=1 TurnStartedEvent" ] (statusFor (SessionId.New()))

    frame.Contains("     _       _   ") |> should equal true
    frame.Contains(shellTitle) |> should equal true
    frame.Contains("EVENT seq=1 TurnStartedEvent") |> should equal true

[<Fact>]
let ``Short windows keep the banner plus the tail`` () =
    let sid = SessionId.New()
    let lines = [ for n in 1..50 -> $"line-%02d{n}" ]
    let frame = renderFrame 80 12 false lines (statusFor sid)

    frame.Contains("line-50") |> should equal true
    frame.Contains("line-01") |> should equal false
    frame.Contains("     _       _   ") |> should equal true
    frame.Contains(statusFor sid) |> should equal true

[<Fact>]
let ``Resize reflows without losing the tail`` () =
    let sid = SessionId.New()
    let status = statusFor sid
    let lines = [ for n in 1..50 -> $"line-%02d{n}" ]
    let wide = renderFrame 80 24 false lines status
    let narrow = renderFrame 40 10 false lines status

    wide.Contains("line-50") |> should equal true
    narrow.Contains("line-50") |> should equal true
    narrow.Contains("line-01") |> should equal false

    let longLine = String.replicate 100 "a"
    let trimmed = renderFrame 40 24 false [ longLine ] status

    trimmed.Contains(longLine.Substring(0, 39) + ">") |> should equal true
    trimmed.Contains(longLine) |> should equal false

[<Fact>]
let ``Degenerate and huge sizes clamp instead of throwing`` () =
    let sid = SessionId.New()
    let status = statusFor sid
    let small = renderFrame 0 0 false [] status
    let huge = renderFrame 10000 10000 false [ "ok" ] status

    small.Contains(status.Substring(0, 19) + ">") |> should equal true
    huge.Contains("ok") |> should equal true

    huge.Split('\n')
    |> Array.forall (fun line -> line.Length <= 240)
    |> should equal true

[<Fact>]
let ``Colorless path strips every escape`` () =
    let frame =
        renderFrame 80 24 false [ "EVENT seq=1 TurnStartedEvent" ] (statusFor (SessionId.New()))

    frame.Contains("\u001b") |> should equal false

[<Fact>]
let ``Color path paints the banner and the status bar`` () =
    let frame =
        renderFrame 80 24 true [ "EVENT seq=1 TurnStartedEvent" ] (statusFor (SessionId.New()))

    frame.Contains("\u001b") |> should equal true

[<Fact>]
let ``Null transcript lines never reach the viewport`` () =
    let frame =
        renderFrame 80 24 false [ Unchecked.defaultof<string>; "ok" ] (statusFor (SessionId.New()))

    frame.Contains("ok") |> should equal true

[<Fact>]
let ``Status names the short id model and turn state`` () =
    let sid = SessionId.New()

    let line =
        statusText sid (ModelReference.Parse("scripted/scripted")) SessionState.Running

    line.Contains(sid.Value.Substring(0, 8)) |> should equal true
    line.Contains("scripted/scripted") |> should equal true
    line.Contains("Running") |> should equal true

[<Fact>]
let ``Short id is the eight-character ULID prefix`` () =
    let sid = SessionId.New()

    shortSessionId sid |> should equal (sid.Value.Substring(0, 8))
    (shortSessionId sid).Length |> should equal 8

[<Fact>]
let ``Scripted turn walks Idle Running Waiting Running Idle`` () =
    let sid, tid = freshIds ()

    let events =
        [
            started sid tid
            delta sid tid
            permissionAsked sid tid
            permissionAnswered sid tid
            completed sid tid
        ]

    let states = events |> List.scan updateTurnState SessionState.Idle |> List.tail

    states
    |> should
        equal
        [
            SessionState.Running
            SessionState.Running
            SessionState.WaitingForInput
            SessionState.Running
            SessionState.Idle
        ]

[<Fact>]
let ``Question suspension waits for input then resumes`` () =
    let sid, tid = freshIds ()

    updateTurnState SessionState.Running (questionAsked sid tid)
    |> should equal SessionState.WaitingForInput

    updateTurnState SessionState.WaitingForInput (questionAnswered sid tid)
    |> should equal SessionState.Running

[<Fact>]
let ``Abort and failure return the bar to Idle`` () =
    let sid, tid = freshIds ()

    updateTurnState SessionState.Running (aborted sid tid)
    |> should equal SessionState.Idle

    updateTurnState SessionState.WaitingForInput (failed sid tid)
    |> should equal SessionState.Idle

[<Fact>]
let ``Deltas and null events keep the current state`` () =
    let sid, tid = freshIds ()

    updateTurnState SessionState.Idle (delta sid tid)
    |> should equal SessionState.Idle

    updateTurnState SessionState.Running (delta sid tid)
    |> should equal SessionState.Running

    updateTurnState SessionState.Idle Unchecked.defaultof<SessionEvent>
    |> should equal SessionState.Idle

[<Fact>]
let ``Teardown restores the cursor and the primary screen`` () =
    alternateExit.Contains("?25h") |> should equal true
    alternateExit.Contains("?1049l") |> should equal true
    alternateEnter.Contains("?1049h") |> should equal true
    alternateEnter.Contains("?25l") |> should equal true
    homeClear.Contains("[H") |> should equal true

[<Fact>]
let ``Status bar sits above the quit hint`` () =
    let sid = SessionId.New()
    let status = statusFor sid

    let lines =
        (renderFrame 80 24 false [ "EVENT seq=1 TurnStartedEvent" ] status).Split('\n')

    let statusIndex = lines |> Array.findIndex (fun line -> line = status)
    let quitIndex = lines |> Array.findIndex (fun line -> line = quitHint)

    (statusIndex < quitIndex) |> should equal true
    lines[lines.Length - 2] |> should equal quitHint
