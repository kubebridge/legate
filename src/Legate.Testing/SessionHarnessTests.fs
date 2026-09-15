// SPDX-License-Identifier: Apache-2.0
namespace Legate.Testing

open System
open System.Collections.Generic
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open Legate
open Microsoft.Extensions.AI
open Xunit

module SessionHarnessTests =

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

    let private optionsWithPolicy (policy: IPermissionPolicy) : SessionHarnessOptions =
        let options = SessionHarnessOptions()
        options.Policy <- policy
        options

    let private questionArgs (question: string) : IDictionary<string, obj> =
        let args = Dictionary<string, obj>()
        args["question"] <- question :> obj
        args :> IDictionary<string, obj>

    let private optionsWithAskUser (askUser: AskUserOptions) : SessionHarnessOptions =
        let options = SessionHarnessOptions()
        options.AskUser <- askUser
        options

    let private sequencesOf (events: IReadOnlyList<SessionEvent>) : int64 list =
        [
            for event in events do
                if event.Sequence.HasValue then
                    yield event.Sequence.Value
        ]

    [<Fact>]
    let ``A plain text turn settles with ordered events`` () : Task =
        task {
            let client = scripted [ ScriptStep.Text "done" ]
            use! harness = SessionHarness.CreateAsync(client, sourced [])

            let! collected = harness.PromptAndCollectAsync("hi", CancellationToken.None)

            Assert.Equal(TurnStatus.Completed, collected.Result.Status)
            Assert.Equal("done", collected.Result.AssistantText)
            Assert.Equal(1, collected.Result.Iterations)

            // The suspendable actor journals only suspend, resolve, and
            // timeout events: a plain turn leaves the journal empty.
            Assert.Empty(collected.Events)

            Assert.Equal(1, harness.SettledResults.Count)

            let! session = harness.GetSessionAsync(CancellationToken.None)
            Assert.Equal(SessionState.Idle, session.State)
            Assert.Equal(harness.Clock.Instant, session.CreatedAt)
        }

    [<Fact>]
    let ``Ask then Reply round-trips through the session actor`` () : Task =
        task {
            let invocations = ref []
            let exec = tool "exec" "out" invocations

            let client =
                scripted
                    [
                        ScriptStep.ToolCall("c1", "exec")
                        ScriptStep.Text "finished"
                    ]

            let options = optionsWithPolicy (AskPolicy("exec") :> IPermissionPolicy)
            use! harness = SessionHarness.CreateAsync(client, sourced [ exec ], options)

            // Prompt without waiting: the turn suspends on the Ask verdict.
            // Every wait below is event-driven; nothing sleeps.
            let! _ = harness.PromptAsync("run", CancellationToken.None)

            let! requestId = harness.WaitForSuspensionAsync(CancellationToken.None)
            Assert.False(String.IsNullOrEmpty requestId)

            // The loop armed its hard deadline before the first provider
            // call, so the shared RecordingDelay already holds it.
            Assert.Contains(options.TurnTimeout, harness.Delays.Recorded)

            let! suspended = harness.CollectEventsAsync(CancellationToken.None)

            let asked =
                suspended
                |> Seq.choose (fun event ->
                    match event with
                    | :? PermissionRequestedEvent as asked when not (isNull (box asked)) -> Some asked
                    | _ -> None)
                |> List.ofSeq

            Assert.Equal(1, asked.Length)
            Assert.Equal(requestId, asked[0].RequestId)
            Assert.Equal("exec", asked[0].ToolName)
            Assert.Equal<int64 list>([ 1L .. int64 suspended.Count ], sequencesOf suspended)

            // Reply resumes from the cursor and settles the turn.
            let! result =
                harness.ReplyAndSettleAsync(
                    PermissionDecision(requestId, PermissionDecisionKind.AllowOnce),
                    CancellationToken.None
                )

            Assert.Equal(TurnStatus.Completed, result.Status)
            Assert.Equal("finished", result.AssistantText)
            Assert.Equal<string list>([ "exec" ], invocations.Value)
            Assert.Equal(2, client.Calls)

            // The suspension armed the AskTimeout on the same shared delay.
            Assert.Contains(options.AskTimeout, harness.Delays.Recorded)

            let! events = harness.CollectEventsAsync(CancellationToken.None)

            let resolved =
                events
                |> Seq.choose (fun event ->
                    match event with
                    | :? PermissionResolvedEvent as resolved when not (isNull (box resolved)) -> Some resolved
                    | _ -> None)
                |> List.ofSeq

            Assert.Equal(1, resolved.Length)
            Assert.Equal(requestId, resolved[0].RequestId)
            Assert.Equal(PermissionDecisionKind.AllowOnce, resolved[0].Decision)
            Assert.Equal<int64 list>([ 1L .. int64 events.Count ], sequencesOf events)

            let! session = harness.GetSessionAsync(CancellationToken.None)
            Assert.Equal(SessionState.Idle, session.State)
        }

    [<Fact>]
    let ``A mismatched Reply rejects without resuming`` () : Task =
        task {
            let client =
                scripted
                    [
                        ScriptStep.ToolCall("c1", "exec")
                        ScriptStep.Text "never"
                    ]

            let options = optionsWithPolicy (AskPolicy("exec") :> IPermissionPolicy)
            use! harness = SessionHarness.CreateAsync(client, sourced [], options)

            let! _ = harness.PromptAsync("run", CancellationToken.None)
            let! _ = harness.WaitForSuspensionAsync(CancellationToken.None)

            let invoke () : Task =
                harness.ReplyAsync(
                    PermissionDecision("req-unknown", PermissionDecisionKind.AllowOnce),
                    CancellationToken.None
                )
                :> Task

            let! _ = Assert.ThrowsAsync<ReplyMismatchException>(invoke)

            Assert.Equal(1, client.Calls)
            Assert.Equal(0, harness.SettledResults.Count)
        }

    [<Fact>]
    let ``CreateAsync validates its inputs`` () =
        let client = scripted [ ScriptStep.Text "done" ]

        Assert.Throws<ArgumentNullException>(fun () ->
            SessionHarness.CreateAsync(Unchecked.defaultof<ScriptedChatClient>, sourced [])
            |> ignore)
        |> ignore

        Assert.Throws<ArgumentNullException>(fun () ->
            SessionHarness.CreateAsync(client, Unchecked.defaultof<StaticToolSource>)
            |> ignore)
        |> ignore

        let badTitle = SessionHarnessOptions()
        badTitle.Title <- "  "

        Assert.Throws<ArgumentException>(fun () -> SessionHarness.CreateAsync(client, sourced [], badTitle) |> ignore)
        |> ignore

        let badBudget = SessionHarnessOptions()
        badBudget.MaxIterations <- 0

        Assert.Throws<ArgumentOutOfRangeException>(fun () ->
            SessionHarness.CreateAsync(client, sourced [], badBudget) |> ignore)
        |> ignore

        let badAskUser = SessionHarnessOptions()
        badAskUser.AskUser <- AskUserOptions(Mode = AskUserMode.AnswerWith)

        Assert.Throws<ArgumentException>(fun () -> SessionHarness.CreateAsync(client, sourced [], badAskUser) |> ignore)
        |> ignore

    [<Fact>]
    let ``Question suspends and the matching answer resumes`` () : Task =
        task {
            let client =
                scripted
                    [
                        ScriptStep.ToolCall("q1", "ask_user", questionArgs "Which region?")
                        ScriptStep.Text "done"
                    ]

            use! harness = SessionHarness.CreateAsync(client, sourced [ AskUserTool.Create() ])

            // Prompt without waiting: the turn suspends on the question.
            // Every wait below is event-driven; nothing sleeps.
            let! _ = harness.PromptAsync("run", CancellationToken.None)

            let! requestId = harness.WaitForSuspensionAsync(CancellationToken.None)
            Assert.False(String.IsNullOrEmpty requestId)

            let! suspended = harness.CollectEventsAsync(CancellationToken.None)

            let asked =
                suspended
                |> Seq.choose (fun event ->
                    match event with
                    | :? QuestionAskedEvent as asked when not (isNull (box asked)) -> Some asked
                    | _ -> None)
                |> List.ofSeq

            Assert.Equal(1, asked.Length)
            Assert.Equal(requestId, asked[0].QuestionId)
            Assert.Equal("Which region?", asked[0].Question)

            // The matching answer resumes from the cursor and settles.
            let! result = harness.ReplyAndSettleAsync(QuestionAnswer(requestId, "east"), CancellationToken.None)

            Assert.Equal(TurnStatus.Completed, result.Status)
            Assert.Equal("done", result.AssistantText)

            let! events = harness.CollectEventsAsync(CancellationToken.None)

            let answered =
                events
                |> Seq.choose (fun event ->
                    match event with
                    | :? QuestionAnsweredEvent as answered when not (isNull (box answered)) -> Some answered
                    | _ -> None)
                |> List.ofSeq

            Assert.Equal(1, answered.Length)
            Assert.Equal(requestId, answered[0].QuestionId)
            Assert.Equal("east", answered[0].Answer)

            let! session = harness.GetSessionAsync(CancellationToken.None)
            Assert.Equal(SessionState.Idle, session.State)
        }

    [<Fact>]
    let ``Fail ask-user policy fails the turn without suspending`` () : Task =
        task {
            let client =
                scripted
                    [
                        ScriptStep.ToolCall("q1", "ask_user", questionArgs "Which region?")
                        ScriptStep.Text "never"
                    ]

            let options = optionsWithAskUser (AskUserOptions())
            use! harness = SessionHarness.CreateAsync(client, sourced [ AskUserTool.Create() ], options)

            let! result = harness.PromptAndSettleAsync("run", CancellationToken.None)

            Assert.Equal(TurnStatus.Failed, result.Status)

            match result.Outcome with
            | :? TurnFailed as failed -> Assert.Equal(TurnLoop.AskUserHeadlessFailMessage, failed.Reason)
            | _ -> Assert.Fail("The failed turn carries no TurnFailed outcome.")

            // Nothing suspended, so the journal holds no question events.
            let! events = harness.CollectEventsAsync(CancellationToken.None)
            Assert.Empty(events)

            Assert.Equal(1, harness.SettledResults.Count)
        }

    [<Fact>]
    let ``AnswerWith ask-user policy continues with the canned answer`` () : Task =
        task {
            let client =
                scripted
                    [
                        ScriptStep.ToolCall("q1", "ask_user", questionArgs "Which region?")
                        ScriptStep.Text "done"
                    ]

            let options =
                optionsWithAskUser (AskUserOptions(Mode = AskUserMode.AnswerWith, CannedAnswer = "canned-42"))

            use! harness = SessionHarness.CreateAsync(client, sourced [ AskUserTool.Create() ], options)

            // The canned answer resumes the turn without any host reply.
            let! result = harness.PromptAndSettleAsync("run", CancellationToken.None)

            Assert.Equal(TurnStatus.Completed, result.Status)
            Assert.Equal("done", result.AssistantText)

            let! events = harness.CollectEventsAsync(CancellationToken.None)
            Assert.Empty(events)

            Assert.Equal(1, harness.SettledResults.Count)
        }
