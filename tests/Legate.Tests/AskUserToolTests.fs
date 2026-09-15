// SPDX-License-Identifier: Apache-2.0
namespace Legate.Tests

open System
open System.Collections.Generic
open System.Threading
open FsUnit.Xunit
open Legate
open Legate.Testing
open Microsoft.Extensions.AI
open Xunit

module AskUserToolTests =

    let private questionArgs (question: string | null) : AIFunctionArguments =
        let args = AIFunctionArguments()
        args["question"] <- box question
        args

    let private sourced (tools: AITool list) : StaticToolSource =
        new StaticToolSource(ResizeArray<AITool>(tools) :> IReadOnlyList<AITool>)

    let private nullAskUser: AskUserOptions = Unchecked.defaultof<AskUserOptions>

    let private nullSession: SessionOptions = Unchecked.defaultof<SessionOptions>

    [<Fact>]
    let ``The factory serves the question-suspension name`` () =
        AskUserTool.ToolName |> should equal "ask_user"
        AskUserTool.ToolName |> should equal TurnLoop.AskUserToolName

        let tool = AskUserTool.Create()
        tool.Name |> should equal "ask_user"
        tool.Description |> should equal AskUserTool.Description
        String.IsNullOrWhiteSpace tool.Description |> should equal false

    [<Fact>]
    let ``The schema requires the question and keeps options optional`` () =
        let schema = AskUserTool.Create().JsonSchema

        schema.GetProperty("required").EnumerateArray()
        |> Seq.map (fun element -> element.GetString())
        |> List.ofSeq
        |> should equal [ "question" ]

        let properties = schema.GetProperty("properties")

        properties.GetProperty("question").GetProperty("type").GetString()
        |> should equal "string"

        let options = properties.GetProperty("options")
        options.GetProperty("type").GetString() |> should equal "array"

        options.GetProperty("items").GetProperty("type").GetString()
        |> should equal "string"

        schema.GetProperty("additionalProperties").GetBoolean() |> should equal false

    [<Fact>]
    let ``Direct invocation without a question fails validation`` () =
        let tool = AskUserTool.Create()

        Assert.Throws<ToolException>(fun () ->
            tool.InvokeAsync(AIFunctionArguments(), CancellationToken.None).GetAwaiter().GetResult()
            |> ignore)
        |> ignore

        Assert.Throws<ToolException>(fun () ->
            tool.InvokeAsync(questionArgs "  ", CancellationToken.None).GetAwaiter().GetResult()
            |> ignore)
        |> ignore

    [<Fact>]
    let ``Direct invocation with a question raises: the loop owns execution`` () =
        let tool = AskUserTool.Create()

        Assert.Throws<InvalidOperationException>(fun () ->
            tool.InvokeAsync(questionArgs "Which region?", CancellationToken.None).GetAwaiter().GetResult()
            |> ignore)
        |> ignore

    [<Fact>]
    let ``The toolbox lists and resolves ask_user by name`` () =
        let table = (sourced [ AskUserTool.Create() ]).AsDictionary()

        table.ContainsKey("ask_user") |> should equal true
        table["ask_user"].Name |> should equal TurnLoop.AskUserToolName

    // ────────────────────────────────────────────────────────────────────
    // Headless policy resolution: the session override wins, otherwise the
    // configured default (Fail when the configuration carries none).

    [<Fact>]
    let ``Resolve falls back to the configured default`` () =
        let configured = LegateOptions()

        AskUserPolicy.resolve configured null |> should equal configured.AskUser
        configured.AskUser.Mode |> should equal AskUserMode.Fail

    [<Fact>]
    let ``Resolve prefers the session override`` () =
        let configured = LegateOptions()

        let session = SessionOptions()
        session.AskUser <- AskUserOptions(Mode = AskUserMode.AnswerWith, CannedAnswer = "canned")

        AskUserPolicy.resolve configured session |> should equal session.AskUser

    [<Fact>]
    let ``Resolve defaults to Fail when the configuration carries none`` () =
        let configured = LegateOptions()
        configured.AskUser <- nullAskUser

        let resolved = AskUserPolicy.resolve configured nullSession
        resolved.Mode |> should equal AskUserMode.Fail
        resolved.Validate() |> should equal null
