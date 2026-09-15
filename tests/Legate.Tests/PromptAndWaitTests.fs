// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.PromptAndWaitTests

open System
open System.Collections.Generic
open System.Reflection
open System.Runtime.CompilerServices
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open Akka.Actor
open Legate
open Legate.Testing
open Microsoft.Extensions.AI
open Xunit

let private scripted (steps: ScriptStep list) : ScriptedChatClient =
    new ScriptedChatClient(ResizeArray<ScriptStep>(steps) :> IReadOnlyList<ScriptStep>)

let private sourced (tools: AITool list) : StaticToolSource =
    new StaticToolSource(ResizeArray<AITool>(tools) :> IReadOnlyList<AITool>)

let private tool (name: string) (result: string) (invocations: string list ref) : AITool =
    let method =
        Func<string>(fun () ->
            invocations.Value <- invocations.Value @ [ name ]
            result)

    AIFunctionFactory.Create(method, name, Unchecked.defaultof<string>, Unchecked.defaultof<JsonSerializerOptions>)
    :> AITool

/// Permission policy asking once for the gated tool, allowing the rest.
type private AskPolicy(gated: string) =
    interface IPermissionPolicy with
        member _.Evaluate(request) =
            if request.ToolName = gated then
                PermissionVerdict.Ask
            else
                PermissionVerdict.Allow

/// A wait-bound seam that never fires unless its token cancels: the turn,
/// never the bound, decides these tests.
type private NeverDelay() =
    interface ILlmDelay with
        member _.Delay(_, cancellationToken) =
            Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken)

let private makeClient (harness: SessionHarness) (waitDelay: ILlmDelay) (defaultBound: TimeSpan) : SessionClient =
    // The bus is per-test over the harness journal and stays alive for the
    // test: Subscribe holds it, and an empty hub is inert process-wide.
    // Hubs are keyed by the harness's unique session id, so tests never
    // share one and no global clear is needed.
    new SessionClient(
        harness.Store,
        harness.Tenant,
        (fun _ _ -> Task.FromResult(harness.Actor)),
        new SessionEventBus(harness.Journal),
        defaultBound,
        waitDelay
    )

let private questionArgs (question: string) : IDictionary<string, obj> =
    let args = Dictionary<string, obj>()
    args["question"] <- question :> obj
    args :> IDictionary<string, obj>

[<Fact>]
let ``A settled turn returns its TurnResult`` () : Task =
    task {
        let client = scripted [ ScriptStep.Text "done" ]
        use! harness = SessionHarness.CreateAsync(client, sourced [])

        let waiter =
            makeClient harness (NeverDelay() :> ILlmDelay) (TimeSpan.FromSeconds 30.0)

        let! result = waiter.PromptAndWaitAsync(harness.SessionId, UserMessage.Text "hi", CancellationToken.None)

        Assert.Equal(TurnStatus.Completed, result.Status)
        Assert.Equal("done", result.AssistantText)
        Assert.Equal(1, result.Iterations)
        // The default harness session runs without the structured outcome
        // mode, so no outcome lands on the result.
        Assert.True(isNull result.Outcome)
        Assert.Equal(1, harness.SettledResults.Count)
        Assert.Equal("done", harness.SettledResults[0].AssistantText)

        let! session = harness.GetSessionAsync(CancellationToken.None)
        Assert.Equal(SessionState.Idle, session.State)
    }

[<Fact>]
let ``An Ask suspension throws the typed exception, then Reply plus re-wait settles`` () : Task =
    task {
        let invocations = ref []
        let exec = tool "exec" "out" invocations

        let client =
            scripted
                [
                    ScriptStep.ToolCall("c1", "exec")
                    ScriptStep.Text "finished"
                    ScriptStep.Text "second"
                ]

        let options = SessionHarnessOptions()
        options.Policy <- AskPolicy("exec") :> IPermissionPolicy
        use! harness = SessionHarness.CreateAsync(client, sourced [ exec ], options)

        let waiter =
            makeClient harness (NeverDelay() :> ILlmDelay) (TimeSpan.FromSeconds 30.0)

        let invoke () : Task =
            waiter.PromptAndWaitAsync(harness.SessionId, UserMessage.Text "run", CancellationToken.None) :> Task

        let! asked = Assert.ThrowsAsync<PermissionApprovalRequiredException>(invoke)

        Assert.Equal(harness.SessionId, asked.SessionId)
        Assert.Equal("exec", asked.ToolName)
        Assert.False(String.IsNullOrEmpty asked.RequestId)
        Assert.NotEqual(Unchecked.defaultof<TurnId>, asked.TurnId)

        let! suspended = harness.GetSessionAsync(CancellationToken.None)
        Assert.Equal(SessionState.WaitingForInput, suspended.State)

        // Re-wait before replying: the waiter queues ahead of the resume,
        // so the next settle it observes is the resumed turn, never the
        // follow-up prompt queued behind it.
        let rewait =
            waiter.PromptAndWaitAsync(harness.SessionId, UserMessage.Text "follow-up", CancellationToken.None)

        let! _ =
            harness.ReplyAsync(
                PermissionDecision(asked.RequestId, PermissionDecisionKind.AllowOnce),
                CancellationToken.None
            )

        let! resumed = rewait
        Assert.Equal(TurnStatus.Completed, resumed.Status)
        Assert.Equal("finished", resumed.AssistantText)
        Assert.Equal<string list>([ "exec" ], invocations.Value)

        // The follow-up prompt queued behind the suspended turn starts its
        // own turn once the resumed one settles: drain it before dispose so
        // no turn is left running. Spins without sleeping; the scripted
        // turn settles in milliseconds.
        let drainDeadline = DateTimeOffset.UtcNow.AddSeconds(10.0)

        while harness.SettledResults.Count < 2 && DateTimeOffset.UtcNow < drainDeadline do
            do! Task.Yield()

        Assert.Equal(2, harness.SettledResults.Count)
        Assert.Equal("finished", harness.SettledResults[0].AssistantText)
        Assert.Equal("second", harness.SettledResults[1].AssistantText)

        // Settle barrier: OnTurnSettled fires before the actor writes Idle,
        // so poll the store until the Idle write lands instead of reading
        // once. Spins without sleeping; the write lands in milliseconds.
        let stateDeadline = DateTimeOffset.UtcNow.AddSeconds(10.0)
        let mutable state = SessionState.Running

        while state <> SessionState.Idle && DateTimeOffset.UtcNow < stateDeadline do
            let! current = harness.GetSessionAsync(CancellationToken.None)
            state <- current.State
            do! Task.Yield()

        Assert.Equal(SessionState.Idle, state)
    }

[<Fact>]
let ``Cancelling the wait abandons it while the turn still settles`` () : Task =
    task {
        let client =
            scripted
                [
                    ScriptStep.ToolCall("q1", "ask_user", questionArgs "Which region?")
                    ScriptStep.Text "done"
                ]

        use! harness = SessionHarness.CreateAsync(client, sourced [ AskUserTool.Create() ])

        let waiter =
            makeClient harness (NeverDelay() :> ILlmDelay) (TimeSpan.FromSeconds 30.0)

        use cts = new CancellationTokenSource()

        let wait =
            waiter.PromptAndWaitAsync(harness.SessionId, UserMessage.Text "run", cts.Token)

        // The turn suspends on a question, which never settles the wait on
        // its own: cancelling now must abandon the wait, never the turn.
        let! requestId = harness.WaitForSuspensionAsync(CancellationToken.None)
        cts.Cancel()

        let abandon () : Task = wait :> Task
        let! _ = Assert.ThrowsAsync<OperationCanceledException>(abandon)

        // The turn keeps running: still suspended, nothing settled.
        let! suspended = harness.GetSessionAsync(CancellationToken.None)
        Assert.Equal(SessionState.WaitingForInput, suspended.State)
        Assert.Empty(harness.SettledResults)

        let! result = harness.ReplyAndSettleAsync(QuestionAnswer(requestId, "east"), CancellationToken.None)

        Assert.Equal(TurnStatus.Completed, result.Status)
        Assert.Equal("done", result.AssistantText)
    }

[<Fact>]
let ``A settle that already won still returns after cancellation`` () : Task =
    task {
        let client = scripted [ ScriptStep.Text "done" ]
        use! harness = SessionHarness.CreateAsync(client, sourced [])

        let waiter =
            makeClient harness (NeverDelay() :> ILlmDelay) (TimeSpan.FromSeconds 30.0)

        use cts = new CancellationTokenSource()

        let wait =
            waiter.PromptAndWaitAsync(harness.SessionId, UserMessage.Text "hi", cts.Token)

        // Spin without sleeping until the hub records the settle; the
        // waiter result lands under the same lock, so cancelling after
        // this point always loses to the settlement.
        let hub = PromptWaitHubs.GetOrAdd harness.SessionId
        let deadline = DateTimeOffset.UtcNow.AddSeconds(10.0)

        while hub.Settled.Count = 0 && DateTimeOffset.UtcNow < deadline do
            do! Task.Yield()

        Assert.True(hub.Settled.Count > 0, "The turn settled before the cancellation.")
        cts.Cancel()

        let! result = wait
        Assert.Equal(TurnStatus.Completed, result.Status)
        Assert.Equal("done", result.AssistantText)
    }

[<Fact>]
let ``Cancelling before the prompt prevents the prompt`` () : Task =
    task {
        let client = scripted [ ScriptStep.Text "done" ]
        use! harness = SessionHarness.CreateAsync(client, sourced [])

        let waiter =
            makeClient harness (NeverDelay() :> ILlmDelay) (TimeSpan.FromSeconds 30.0)

        use cts = new CancellationTokenSource()
        cts.Cancel()

        // The BCL surfaces a pre-prompt cancellation as its own
        // OperationCanceledException subtype (TaskCanceledException from
        // the gate): assert the base contract, not the exact subtype.
        let! canceled =
            task {
                try
                    let! _ = waiter.PromptAndWaitAsync(harness.SessionId, UserMessage.Text "hi", cts.Token)

                    return false
                with :? OperationCanceledException ->
                    return true
            }

        Assert.True(canceled, "A pre-prompt cancellation throws OperationCanceledException.")

        let! session = harness.GetSessionAsync(CancellationToken.None)
        Assert.Equal(SessionState.Idle, session.State)
        Assert.Empty(harness.SettledResults)
    }

[<Fact>]
let ``A lapsed wait bound throws while the turn keeps running`` () : Task =
    task {
        let client =
            scripted
                [
                    ScriptStep.ToolCall("q1", "ask_user", questionArgs "Which region?")
                    ScriptStep.Text "done"
                ]

        use! harness = SessionHarness.CreateAsync(client, sourced [ AskUserTool.Create() ])

        // The seam fires at once, so the bound lapses while the turn waits
        // on its question: the wait throws, the turn runs on.
        let delays = RecordingDelay()
        let bound = TimeSpan.FromSeconds 5.0
        let waiter = makeClient harness delays bound

        let invoke () : Task =
            waiter.PromptAndWaitAsync(harness.SessionId, UserMessage.Text "run", CancellationToken.None) :> Task

        let! exceeded = Assert.ThrowsAsync<DeadlineExceededException>(invoke)

        Assert.Equal("PromptAndWait", exceeded.OperationName)
        Assert.Equal(bound, Assert.Single(delays.Recorded))

        let! suspended = harness.GetSessionAsync(CancellationToken.None)
        Assert.Equal(SessionState.WaitingForInput, suspended.State)
        Assert.Empty(harness.SettledResults)

        let! requestId = harness.WaitForSuspensionAsync(CancellationToken.None)

        let! result = harness.ReplyAndSettleAsync(QuestionAnswer(requestId, "east"), CancellationToken.None)

        Assert.Equal(TurnStatus.Completed, result.Status)
        Assert.Equal("done", result.AssistantText)
    }

[<Fact>]
let ``Overlapping waits resolve in Queue settle order`` () : Task =
    task {
        let client =
            scripted
                [
                    ScriptStep.Text "one"
                    ScriptStep.Text "two"
                ]

        use! harness = SessionHarness.CreateAsync(client, sourced [])

        let waiter =
            makeClient harness (NeverDelay() :> ILlmDelay) (TimeSpan.FromSeconds 30.0)

        let first =
            waiter.PromptAndWaitAsync(harness.SessionId, UserMessage.Text "a", CancellationToken.None)

        let second =
            waiter.PromptAndWaitAsync(harness.SessionId, UserMessage.Text "b", CancellationToken.None)

        let! results = Task.WhenAll(first, second)

        // Whichever prompt won the enqueue gate settles first with the
        // first script step: the set is exact, so no waiter stole or lost
        // a settle.
        let texts = results |> Seq.map (fun result -> result.AssistantText) |> Set.ofSeq

        Assert.Equal<Set<string>>(Set.ofList [ "one"; "two" ], texts)

        for result in results do
            Assert.Equal(TurnStatus.Completed, result.Status)

        Assert.Equal(2, harness.SettledResults.Count)
    }

[<Fact>]
let ``An unknown session throws SessionNotFoundException`` () : Task =
    task {
        let client = scripted [ ScriptStep.Text "done" ]
        use! harness = SessionHarness.CreateAsync(client, sourced [])

        let waiter =
            makeClient harness (NeverDelay() :> ILlmDelay) (TimeSpan.FromSeconds 30.0)

        let invoke () : Task =
            waiter.PromptAndWaitAsync(SessionId.New(), UserMessage.Text "hi", CancellationToken.None) :> Task

        let! missing = Assert.ThrowsAsync<SessionNotFoundException>(invoke)
        Assert.NotEqual(harness.SessionId, missing.SessionId)
    }

[<Fact>]
let ``A closed session throws InvalidSessionStateException`` () : Task =
    task {
        let client = scripted [ ScriptStep.Text "done" ]
        use! harness = SessionHarness.CreateAsync(client, sourced [])

        let waiter =
            makeClient harness (NeverDelay() :> ILlmDelay) (TimeSpan.FromSeconds 30.0)

        let! _ = harness.CloseAsync(CancellationToken.None)

        let invoke () : Task =
            waiter.PromptAndWaitAsync(harness.SessionId, UserMessage.Text "hi", CancellationToken.None) :> Task

        let! closed = Assert.ThrowsAsync<InvalidSessionStateException>(invoke)
        Assert.Equal(harness.SessionId, closed.SessionId)
    }

[<Fact>]
let ``PromptAndWaitAsync stays BCL-only`` () =
    let method =
        typeof<SessionClientExtensions>.GetMethod("PromptAndWaitAsync", BindingFlags.Public ||| BindingFlags.Static)
        |> Option.ofObj
        |> Option.defaultWith (fun () -> raise (InvalidOperationException("PromptAndWaitAsync is missing.")))

    let extension = method.GetCustomAttribute<ExtensionAttribute>() |> Option.ofObj

    Assert.True(extension.IsSome, "PromptAndWaitAsync must carry ExtensionAttribute.")
    Assert.Equal(typeof<Task<TurnResult>>, method.ReturnType)

    let parameters =
        method.GetParameters()
        |> Seq.map (fun parameter -> parameter.ParameterType)
        |> List.ofSeq

    Assert.Equal<Type list>(
        [
            typeof<SessionClient>
            typeof<SessionId>
            typeof<UserMessage>
            typeof<CancellationToken>
        ],
        parameters
    )

    let fsharpCore = typeof<list<int>>.Assembly

    for exposed in method.ReturnType :: parameters do
        Assert.False(
            exposed.Assembly.Equals(fsharpCore),
            sprintf "The public surface leaks the F# core type %s." exposed.FullName
        )

[<Fact>]
let ``TurnFinished marks implicit synthesis explicitly`` () =
    let explicit = TurnFinished("summary")
    Assert.False(explicit.IsImplicit)

    let implicit = TurnFinished("text")
    implicit.IsImplicit <- true
    Assert.True(implicit.IsImplicit)
    Assert.Equal("text", implicit.Summary)
