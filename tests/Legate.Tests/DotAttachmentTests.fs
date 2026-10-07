// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.DotAttachmentTests

open System
open System.Collections.Generic
open System.IO
open System.Threading
open System.Threading.Tasks
open Legate
open Legate.Storage.InMemory
open Legate.Testing
open Dot
open FsUnit.Xunit
open Xunit

// Continuous attachment plus receipt-bound authoritative observation
// (issue 365): one logical attachment per selected session over the
// shared production public facade, receipt-bound waits starting at
// acceptance, explicit unknown/unavailable, post-deadline observation
// with usable controls, session- and turn-bound replies, and clean-start
// diagnostics. Deterministic and event-driven: scripted turns settle in
// milliseconds under generous bounds; deadline paths hold the turn on a
// blocking tool and fire the seam immediately, never sleeps.

// ──────────────────────────────────────────────────────────────────────────
// Pure shell mapping

[<Fact>]
let ``Operation status reads explicitly, never as running or success`` () =
    DotShell.describeOperationStatus OperationStatus.Pending
    |> fun text -> text.Contains("accepted-queued", StringComparison.Ordinal)
    |> should equal true

    DotShell.describeOperationStatus OperationStatus.Terminal
    |> fun text -> text.Contains("committed", StringComparison.Ordinal)
    |> should equal true

    DotShell.describeOperationStatus OperationStatus.Unknown
    |> fun text -> text.Contains("unknown", StringComparison.Ordinal)
    |> should equal true

    DotShell.describeOperationStatus OperationStatus.Unavailable
    |> fun text -> text.Contains("unavailable", StringComparison.Ordinal)
    |> should equal true

[<Fact>]
let ``Terminal status keeps committed failure and abort distinct from idle`` () =
    DotShell.describeTerminalStatus TurnStatus.Completed
    |> fun text -> text.Contains("committed-success", StringComparison.Ordinal)
    |> should equal true

    DotShell.describeTerminalStatus TurnStatus.Aborted
    |> fun text -> text.Contains("committed-abort", StringComparison.Ordinal)
    |> should equal true

    DotShell.describeTerminalStatus TurnStatus.Failed
    |> fun text -> text.Contains("committed-failure", StringComparison.Ordinal)
    |> should equal true

[<Fact>]
let ``Status bar carries explicit operation evidence or unknown`` () =
    let sid = SessionId.New()
    let model = ModelReference.Parse("scripted/scripted")

    let withOp =
        DotShell.statusTextWithOperation sid model SessionState.Running "accepted-queued op=3"

    withOp.Contains("accepted-queued op=3", StringComparison.Ordinal)
    |> should equal true

    let unknown = DotShell.statusTextWithOperation sid model SessionState.Idle null
    unknown.Contains("unknown", StringComparison.Ordinal) |> should equal true
    let blank = DotShell.statusTextWithOperation sid model SessionState.Idle "  "
    blank.Contains("unknown", StringComparison.Ordinal) |> should equal true

[<Fact>]
let ``Subscribe lifecycle mapping stays stable`` () =
    let sid = SessionId.New()
    let tid = TurnId.New()
    let stamp = DateTimeOffset.UtcNow
    let at (n: int) = Nullable<int64>(int64 n)
    let started = TurnStartedEvent(sid, tid, at 1, stamp) :> SessionEvent

    let asked =
        PermissionRequestedEvent(sid, tid, at 2, stamp, "r1", "exec") :> SessionEvent

    let completed = TurnCompletedEvent(sid, tid, at 3, stamp) :> SessionEvent

    DotShell.updateTurnState SessionState.Idle started
    |> should equal SessionState.Running

    DotShell.updateTurnState SessionState.Running asked
    |> should equal SessionState.WaitingForInput

    DotShell.updateTurnState SessionState.Running completed
    |> should equal SessionState.Idle

// ──────────────────────────────────────────────────────────────────────────
// Renderer parity for new diagnostics

[<Fact>]
let ``Renderer retains attachment and authoritative diagnostics`` () =
    let lines =
        [
            "ACCEPTED op=1 kind=Queue session=abc"
            "DEADLINE op=1 kind=Queue client waiting stopped after 00:00:05 (work continues; attachment still observing; STATE follows)"
            "STATE accepted-queued op=1 kind=Queue (no committed result yet)"
            "REPLY-REJECTED id=r1 reason=stale-or-resolved (no pending request; switching, resolve/abort, disconnect, or replay never reuses it)"
            "ATTACHED abc cursor=0 (continuous observation; waits, deadlines, and terminal results never detach)"
            "OBSERVE-FAILED session=abc reason=boom (attachment kept; reattach recovers retained events)"
            "UNSUPPORTED-CONTRACT session=abc reason=old store"
            "IGNORED-STALE-EVENT session=abc selected=def type=TurnCompletedEvent"
        ]

    let state = DotRender.addLines DotRender.empty lines

    for line in lines do
        state.Diagnostics |> should contain line

// ──────────────────────────────────────────────────────────────────────────
// Help and diagnostics text

[<Fact>]
let ``Wait help separates client waiting, queued time, execution budget, and suspension`` () =
    ReplEngine.waitSemanticsHelp.Contains("queued time", StringComparison.Ordinal)
    |> should equal true

    ReplEngine.waitSemanticsHelp.Contains("execution budget", StringComparison.Ordinal)
    |> should equal true

    ReplEngine.waitSemanticsHelp.ToLowerInvariant().Contains("suspension", StringComparison.Ordinal)
    |> should equal true

    ReplEngine.waitSemanticsHelp.Contains("client waiting stopped", StringComparison.Ordinal)
    |> should equal true

    ReplEngine.waitSemanticsHelp.Contains("never abort", StringComparison.Ordinal)
    |> should equal true

[<Fact>]
let ``Clean-start help requires explicit action and forbids fabrication`` () =
    ReplEngine.cleanStartHelp.ToLowerInvariant().Contains("clean start", StringComparison.Ordinal)
    |> should equal true

    ReplEngine.cleanStartHelp.Contains("UNSUPPORTED-CONTRACT", StringComparison.Ordinal)
    |> should equal true

    ReplEngine.cleanStartHelp.ToLowerInvariant().Contains("never", StringComparison.Ordinal)
    |> should equal true

// ──────────────────────────────────────────────────────────────────────────
// Pure operation-state mapping

let private receipt (pos: int64) (kind: OperationKind) =
    AcceptedOperation(SessionId.New(), pos, TurnId.New(), kind, DateTimeOffset.UtcNow)

let private terminalResult (status: TurnStatus) (text: string) : TurnResult =
    {
        AssistantText = text
        Status = status
        Iterations = 1
        Usage = { InputTokens = 0L; OutputTokens = 0L }
        Outcome = null
    }

let private operation (receipt: AcceptedOperation) (status: OperationStatus) (result: TurnResult | null) =
    let turn =
        if status = OperationStatus.Terminal && not (isNull (box result)) then
            Nullable<TurnId>(receipt.OperationId)
        else
            Nullable<TurnId>()

    OperationResult(receipt.SessionId, receipt.Position, receipt.Kind, status, turn, result)

[<Fact>]
let ``Describe maps pending, terminal outcomes, unknown, unavailable, and null`` () =
    let pending =
        operation (receipt 1L OperationKind.Queue) OperationStatus.Pending null

    ReplEngine.describeOperationState(pending).Contains("accepted-queued", StringComparison.Ordinal)
    |> should equal true

    let ok =
        operation (receipt 2L OperationKind.Queue) OperationStatus.Terminal (terminalResult TurnStatus.Completed "hi")

    ReplEngine.describeOperationState(ok).Contains("committed-success", StringComparison.Ordinal)
    |> should equal true

    let failed =
        operation
            (receipt 3L OperationKind.Interrupt)
            OperationStatus.Terminal
            (terminalResult TurnStatus.Failed "boom")

    ReplEngine.describeOperationState(failed).Contains("committed-failure", StringComparison.Ordinal)
    |> should equal true

    let aborted =
        operation (receipt 4L OperationKind.Queue) OperationStatus.Terminal (terminalResult TurnStatus.Aborted "stop")

    ReplEngine.describeOperationState(aborted).Contains("committed-abort", StringComparison.Ordinal)
    |> should equal true

    let unknown =
        operation (receipt 5L OperationKind.Queue) OperationStatus.Unknown null

    ReplEngine.describeOperationState(unknown).Contains("unknown", StringComparison.Ordinal)
    |> should equal true

    let unavailable =
        operation (receipt 6L OperationKind.Queue) OperationStatus.Unavailable null

    ReplEngine.describeOperationState(unavailable).Contains("unavailable", StringComparison.Ordinal)
    |> should equal true

    ReplEngine
        .describeOperationState(Unchecked.defaultof<OperationResult>)
        .Contains("unknown", StringComparison.Ordinal)
    |> should equal true

// ──────────────────────────────────────────────────────────────────────────
// Facade receipt-bound guarantees consumed by Dot

let private scripted (steps: ScriptStep list) : ScriptedChatClient =
    new ScriptedChatClient(ResizeArray<ScriptStep>(steps) :> IReadOnlyList<ScriptStep>)

let private sourced (tools: Microsoft.Extensions.AI.AITool list) : StaticToolSource =
    new StaticToolSource(
        ResizeArray<Microsoft.Extensions.AI.AITool>(tools) :> IReadOnlyList<Microsoft.Extensions.AI.AITool>
    )

type private NeverDelay() =
    interface ILlmDelay with
        member _.Delay(_, cancellationToken) =
            Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken)

let private makeClient (harness: SessionHarness) (waitDelay: ILlmDelay) (defaultBound: TimeSpan) : SessionClient =
    new SessionClient(
        harness.Store,
        harness.Tenant,
        (fun _ _ -> Task.FromResult(harness.Actor)),
        new SessionEventBus(harness.Journal),
        defaultBound,
        waitDelay,
        None
    )

let private awaitWhat (work: Task<'T>) (what: string) : Task<'T> =
    task {
        try
            return! work.WaitAsync(TimeSpan.FromSeconds(10.0), CancellationToken.None)
        with :? TimeoutException ->
            return raise (TimeoutException($"The test timed out waiting for {what}."))
    }

let private awaitUnit (work: Task) (what: string) : Task =
    task {
        try
            do! work.WaitAsync(TimeSpan.FromSeconds(10.0), CancellationToken.None)
        with :? TimeoutException ->
            return raise (TimeoutException($"The test timed out waiting for {what}."))
    }

[<Fact>]
let ``Concurrent receipts carry distinct positions`` () : Task =
    task {
        let! harness =
            SessionHarness.CreateAsync(
                scripted
                    [
                        ScriptStep.Text "a"
                        ScriptStep.Text "b"
                    ],
                sourced []
            )

        use _ = harness

        let client =
            makeClient harness (NeverDelay() :> ILlmDelay) (TimeSpan.FromSeconds(30.0))

        let! first =
            awaitWhat
                (client.PromptAsync(
                    harness.SessionId,
                    UserMessage.Text "one",
                    DeliveryMode.Queue,
                    CancellationToken.None
                ))
                "first accept"

        let! second =
            awaitWhat
                (client.PromptAsync(
                    harness.SessionId,
                    UserMessage.Text "two",
                    DeliveryMode.Queue,
                    CancellationToken.None
                ))
                "second accept"

        Assert.NotEqual(first.Position, second.Position)

        let! firstDone =
            awaitWhat
                (client.WaitForOperationAsync(first, TimeSpan.FromSeconds(30.0), CancellationToken.None))
                "first settle"

        let! secondDone =
            awaitWhat
                (client.WaitForOperationAsync(second, TimeSpan.FromSeconds(30.0), CancellationToken.None))
                "second settle"

        Assert.Equal(OperationStatus.Terminal, firstDone.Status)
        Assert.Equal(OperationStatus.Terminal, secondDone.Status)
        Assert.Equal(first.Position, firstDone.Position)
        Assert.Equal(second.Position, secondDone.Position)
    }

[<Fact>]
let ``Deadline abandons only observation and later wait still observes`` () : Task =
    task {
        let entered = new ManualResetEventSlim(false)
        let release = TaskCompletionSource<string>()

        let method =
            Func<Task<string>>(fun () ->
                entered.Set() |> ignore
                release.Task)

        let tool =
            Microsoft.Extensions.AI.AIFunctionFactory.Create(
                method,
                "block",
                Unchecked.defaultof<string>,
                Unchecked.defaultof<System.Text.Json.JsonSerializerOptions>
            )
            :> Microsoft.Extensions.AI.AITool

        let! harness =
            SessionHarness.CreateAsync(
                scripted
                    [
                        ScriptStep.ToolCall("c1", "block")
                        ScriptStep.Text "done"
                    ],
                sourced [ tool ]
            )

        use _ = harness
        // Real clock delay so the short bound lapses while the turn is
        // parked; NeverDelay would never fire the bound.
        let client =
            makeClient harness (SystemLlmDelay() :> ILlmDelay) (TimeSpan.FromSeconds(30.0))

        let! accepted =
            awaitWhat
                (client.PromptAsync(
                    harness.SessionId,
                    UserMessage.Text "go",
                    DeliveryMode.Queue,
                    CancellationToken.None
                ))
                "accept"

        Assert.True(entered.Wait(TimeSpan.FromSeconds(10.0)), "The blocking tool never ran.")

        try
            let! _ =
                awaitWhat
                    (client.WaitForOperationAsync(accepted, TimeSpan.FromMilliseconds(50.0), CancellationToken.None))
                    "deadline"

            Assert.Fail("The short wait should lapse while the turn keeps running.")
        with :? DeadlineExceededException ->
            ()

        let! pending = awaitWhat (client.GetOperationResultAsync(accepted, CancellationToken.None)) "pending read"
        Assert.Equal(OperationStatus.Pending, pending.Status)
        release.TrySetResult("free") |> ignore

        let! terminal =
            awaitWhat
                (client.WaitForOperationAsync(accepted, TimeSpan.FromSeconds(30.0), CancellationToken.None))
                "terminal"

        Assert.Equal(OperationStatus.Terminal, terminal.Status)

        let! again =
            awaitWhat
                (client.WaitForOperationAsync(accepted, TimeSpan.FromSeconds(30.0), CancellationToken.None))
                "second wait"

        Assert.Equal(OperationStatus.Terminal, again.Status)
        Assert.Equal(terminal.Position, again.Position)
    }

// ──────────────────────────────────────────────────────────────────────────
// Engine attachment over the facade

let private engineOptions () : ReplEngine.ProviderOption list =
    [
        {
            Id = "scripted"
            DefaultModel = "scripted/scripted"
            EnvVar = null
        }
    ]

let private makeEngine
    (client: SessionClient)
    (agents: IAgentStore)
    (packages: IAgentPackageStore)
    (input: string)
    (output: StringWriter)
    (waitBound: TimeSpan)
    : ReplEngine.Engine =
    ReplEngine.Engine(
        client,
        agents,
        packages,
        new StringReader(input) :> TextReader,
        output :> TextWriter,
        waitBound,
        ModelReference.Parse("scripted/scripted"),
        engineOptions ()
    )

let private pollFor (output: StringWriter) (marker: string) (what: string) : Task =
    task {
        let deadline = DateTimeOffset.UtcNow.AddSeconds(10.0)
        let mutable seen = false

        while not seen && DateTimeOffset.UtcNow < deadline do
            if output.ToString().Contains(marker, StringComparison.Ordinal) then
                seen <- true
            else
                do! Task.Delay(20, CancellationToken.None)

        Assert.True(
            seen,
            $"The engine output never carried '{marker}' waiting for {what}. Full output:{Environment.NewLine}{output}"
        )
    }

[<Fact>]
let ``Open establishes one continuous attachment`` () : Task =
    task {
        let options = SessionHarnessOptions()
        options.Tenant <- TenantId.Default
        let! harness = SessionHarness.CreateAsync(scripted [ ScriptStep.Text "hi" ], sourced [], options)
        use _ = harness

        let client =
            makeClient harness (NeverDelay() :> ILlmDelay) (TimeSpan.FromSeconds(30.0))

        let database = InMemoryDatabase()
        let agents = InMemoryAgentStore(database) :> IAgentStore
        let packages = InMemoryAgentPackageStore(database) :> IAgentPackageStore
        use output = new StringWriter()
        use cts = new CancellationTokenSource()

        let engine =
            makeEngine client agents packages "" output (TimeSpan.FromSeconds(30.0))

        do! awaitUnit (engine.OpenSessionAsync("dot", cts.Token)) "open"
        Assert.Equal(1, engine.ActiveConsumerCount)
        Assert.True(engine.AttachmentSessionId.IsSome, "Attachment should observe the opened session.")
        Assert.Contains("ATTACHED", output.ToString())
        cts.Cancel()
    }

[<Fact>]
let ``Prompt reports receipt then authoritative terminal state on one consumer`` () : Task =
    task {
        let options = SessionHarnessOptions()
        options.Tenant <- TenantId.Default
        let! harness = SessionHarness.CreateAsync(scripted [ ScriptStep.Text "hello" ], sourced [], options)
        use _ = harness

        let client =
            makeClient harness (NeverDelay() :> ILlmDelay) (TimeSpan.FromSeconds(30.0))

        let database = InMemoryDatabase()
        let agents = InMemoryAgentStore(database) :> IAgentStore
        let packages = InMemoryAgentPackageStore(database) :> IAgentPackageStore
        use output = new StringWriter()
        use cts = new CancellationTokenSource()

        let engine =
            makeEngine client agents packages "" output (TimeSpan.FromSeconds(30.0))

        do! awaitUnit (engine.OpenSessionAsync("dot", cts.Token)) "open"
        let! keepGoing = awaitWhat (engine.HandleLineAsync("hello", cts.Token)) "prompt"
        Assert.True(keepGoing)
        do! pollFor output "ACCEPTED op=" "accept"
        do! pollFor output "RESULT Completed" "settle"
        do! pollFor output "STATE committed-success" "authoritative state"
        do! pollFor output "END-RESULT" "end"
        Assert.Equal(1, engine.ActiveConsumerCount)
        Assert.False(engine.IsTurnRunning, "Authoritative flag should clear after committed success.")
        Assert.Contains("committed-success", engine.LastOperationText)
        cts.Cancel()
    }

[<Fact>]
let ``Failed turn stays visible as committed failure on idle`` () : Task =
    task {
        let options = SessionHarnessOptions()
        options.Tenant <- TenantId.Default

        let! harness =
            SessionHarness.CreateAsync(
                scripted
                    [
                        ScriptStep.Failure(Exception("boom"))
                    ],
                sourced [],
                options
            )

        use _ = harness

        let client =
            makeClient harness (NeverDelay() :> ILlmDelay) (TimeSpan.FromSeconds(30.0))

        let database = InMemoryDatabase()
        let agents = InMemoryAgentStore(database) :> IAgentStore
        let packages = InMemoryAgentPackageStore(database) :> IAgentPackageStore
        use output = new StringWriter()
        use cts = new CancellationTokenSource()

        let engine =
            makeEngine client agents packages "" output (TimeSpan.FromSeconds(30.0))

        do! awaitUnit (engine.OpenSessionAsync("dot", cts.Token)) "open"
        let! keepGoing = awaitWhat (engine.HandleLineAsync("go", cts.Token)) "prompt"
        Assert.True(keepGoing)
        do! pollFor output "STATE committed-failure" "authoritative failure"
        Assert.False(engine.IsTurnRunning, "Idle session retains its failed last operation without reading running.")
        Assert.Contains("committed-failure", engine.LastOperationText)
        cts.Cancel()
    }

[<Fact>]
let ``Switch keeps one consumer and stale replies never cross sessions`` () : Task =
    task {
        let options = SessionHarnessOptions()
        options.Tenant <- TenantId.Default

        let! harness =
            SessionHarness.CreateAsync(
                scripted
                    [
                        ScriptStep.Text "a"
                        ScriptStep.Text "b"
                    ],
                sourced [],
                options
            )

        use _ = harness

        let client =
            makeClient harness (NeverDelay() :> ILlmDelay) (TimeSpan.FromSeconds(30.0))

        let database = InMemoryDatabase()
        let agents = InMemoryAgentStore(database) :> IAgentStore
        let packages = InMemoryAgentPackageStore(database) :> IAgentPackageStore
        use output = new StringWriter()
        use cts = new CancellationTokenSource()

        let engine =
            makeEngine client agents packages "" output (TimeSpan.FromSeconds(30.0))

        do! awaitUnit (engine.OpenSessionAsync("one", cts.Token)) "open one"
        let first = engine.AttachmentSessionId
        do! awaitUnit (engine.OpenSessionAsync("two", cts.Token)) "open two"
        let second = engine.AttachmentSessionId
        Assert.NotEqual(first, second)
        Assert.Equal(1, engine.ActiveConsumerCount)
        Assert.Contains("ATTACHED", output.ToString())

        try
            do!
                awaitUnit
                    (engine.ReplyPermissionAsync("missing-req", PermissionDecisionKind.AllowOnce, cts.Token))
                    "stale reply"

            Assert.Fail("A stale reply should reject visibly.")
        with :? InvalidOperationException ->
            ()

        Assert.Contains("REPLY-REJECTED", output.ToString())
        cts.Cancel()
    }
