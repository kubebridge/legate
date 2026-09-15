// SPDX-License-Identifier: Apache-2.0
namespace Legate.Testing

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.AI
open Xunit

module ScriptedChatClientTests =

    let private scripted (steps: ScriptStep list) : ScriptedChatClient =
        new ScriptedChatClient(ResizeArray<ScriptStep>(steps) :> IReadOnlyList<ScriptStep>)

    let private userHistory (text: string) : IList<ChatMessage> =
        ResizeArray<ChatMessage>([| ChatMessage(ChatRole.User, text) |]) :> IList<ChatMessage>

    let private collect (enumerable: IAsyncEnumerable<ChatResponseUpdate>) : Task<ChatResponseUpdate list> =
        task {
            let results = ResizeArray<ChatResponseUpdate>()
            use enumerator = enumerable.GetAsyncEnumerator()
            let mutable go = true

            while go do
                let! hasNext = enumerator.MoveNextAsync()

                if hasNext then
                    results.Add(enumerator.Current)
                else
                    go <- false

            return List.ofSeq results
        }

    let private usageOf (input: int64) (output: int64) : UsageDetails =
        let details = UsageDetails()
        details.InputTokenCount <- Nullable input
        details.OutputTokenCount <- Nullable output
        details

    [<Fact>]
    let ``A text step answers with the assistant text`` () : Task =
        task {
            let client = scripted [ ScriptStep.Text "done" ]
            let history = userHistory "hi"

            let! response = (client :> IChatClient).GetResponseAsync(history, ChatOptions(), CancellationToken.None)

            Assert.Equal("done", response.Text)
            Assert.Equal(1, client.Calls)
            Assert.Equal(1, client.ReceivedMessages.Count)
            Assert.Equal(ChatRole.User, client.ReceivedMessages[0].Role)
        }

    [<Fact>]
    let ``A text step carries its usage payload`` () : Task =
        task {
            let client = scripted [ ScriptStep.Text("done", 3L, 5L) ]

            let! response =
                (client :> IChatClient).GetResponseAsync(userHistory "hi", ChatOptions(), CancellationToken.None)

            match response.Usage with
            | null -> failwith "Expected the scripted usage payload."
            | usage ->
                Assert.Equal(Nullable 3L, usage.InputTokenCount)
                Assert.Equal(Nullable 5L, usage.OutputTokenCount)
        }

    [<Fact>]
    let ``A tool-call step answers with the scripted call`` () : Task =
        task {
            let client = scripted [ ScriptStep.ToolCall("c1", "lookup") ]

            let! response =
                (client :> IChatClient).GetResponseAsync(userHistory "hi", ChatOptions(), CancellationToken.None)

            let calls =
                response.Messages
                |> Seq.collect (fun message -> message.Contents)
                |> Seq.choose (fun content ->
                    match content with
                    | :? FunctionCallContent as call when not (isNull (box call)) -> Some call
                    | _ -> None)
                |> List.ofSeq

            Assert.Equal(1, calls.Length)
            Assert.Equal("c1", calls[0].CallId)
            Assert.Equal("lookup", calls[0].Name)

            match calls[0].Arguments with
            | null -> failwith "Expected an arguments dictionary."
            | args -> Assert.Equal(0, args.Count)
        }

    [<Fact>]
    let ``A multi-call step keeps the call order`` () : Task =
        task {
            let calls =
                ResizeArray<ScriptToolCall>(
                    [|
                        ScriptToolCall("c1", "tool_a")
                        ScriptToolCall("c2", "tool_b")
                    |]
                )
                :> IReadOnlyList<ScriptToolCall>

            let client = scripted [ ScriptStep.ToolCalls calls ]

            let! response =
                (client :> IChatClient).GetResponseAsync(userHistory "hi", ChatOptions(), CancellationToken.None)

            let names =
                response.Messages
                |> Seq.collect (fun message -> message.Contents)
                |> Seq.choose (fun content ->
                    match content with
                    | :? FunctionCallContent as call when not (isNull (box call)) -> Some call.Name
                    | _ -> None)
                |> List.ofSeq

            Assert.Equal<string list>([ "tool_a"; "tool_b" ], names)
        }

    [<Fact>]
    let ``A stream step yields one update per chunk`` () : Task =
        task {
            let chunks =
                ResizeArray<AIContent>(
                    [|
                        TextContent("Hel") :> AIContent
                        TextContent("lo") :> AIContent
                        UsageContent(usageOf 3L 5L) :> AIContent
                    |]
                )
                :> IReadOnlyList<AIContent>

            let client = scripted [ ScriptStep.Stream chunks ]

            let! updates =
                collect (
                    (client :> IChatClient)
                        .GetStreamingResponseAsync(userHistory "hi", ChatOptions(), CancellationToken.None)
                )

            Assert.Equal(3, updates.Length)

            let texts =
                updates
                |> List.choose (fun update ->
                    update.Contents
                    |> Seq.tryPick (fun content ->
                        match content with
                        | :? TextContent as text when not (isNull (box text)) -> Some text.Text
                        | _ -> None))

            Assert.Equal<string list>([ "Hel"; "lo" ], texts)
            Assert.Equal(1, client.Calls)
        }

    [<Fact>]
    let ``A direct response call accumulates a stream step`` () : Task =
        task {
            let chunks =
                ResizeArray<AIContent>(
                    [|
                        TextContent("Hel") :> AIContent
                        TextContent("lo") :> AIContent
                        UsageContent(usageOf 3L 5L) :> AIContent
                    |]
                )
                :> IReadOnlyList<AIContent>

            let client = scripted [ ScriptStep.Stream chunks ]

            let! response =
                (client :> IChatClient).GetResponseAsync(userHistory "hi", ChatOptions(), CancellationToken.None)

            Assert.Equal("Hello", response.Text)

            match response.Usage with
            | null -> failwith "Expected the accumulated usage payload."
            | usage ->
                Assert.Equal(Nullable 3L, usage.InputTokenCount)
                Assert.Equal(Nullable 5L, usage.OutputTokenCount)

            let kinds =
                response.Messages
                |> Seq.collect (fun message -> message.Contents)
                |> Seq.map (fun content -> content.GetType().Name)
                |> List.ofSeq

            Assert.Equal<string list>([ "TextContent"; "TextContent" ], kinds)
        }

    [<Fact>]
    let ``A stream raw representation lands on the first update`` () : Task =
        task {
            let payload = obj ()

            let chunks =
                ResizeArray<AIContent>([| TextContent("hi") :> AIContent |]) :> IReadOnlyList<AIContent>

            let client = scripted [ ScriptStep.Stream(chunks, payload) ]

            let! updates =
                collect (
                    (client :> IChatClient)
                        .GetStreamingResponseAsync(userHistory "hi", ChatOptions(), CancellationToken.None)
                )

            Assert.Equal(1, updates.Length)
            Assert.True(Object.ReferenceEquals(payload, updates[0].RawRepresentation))
        }

    [<Fact>]
    let ``A failure step raises from the response entry point`` () : Task =
        task {
            let client =
                scripted
                    [
                        ScriptStep.Failure(InvalidOperationException("boom"))
                    ]

            let invoke () : Task =
                (client :> IChatClient).GetResponseAsync(userHistory "hi", ChatOptions(), CancellationToken.None)
                :> Task

            let! _ = Assert.ThrowsAsync<InvalidOperationException>(invoke)
            return ()
        }

    [<Fact>]
    let ``A failure step raises from the streaming entry point`` () =
        let client =
            scripted
                [
                    ScriptStep.Failure(InvalidOperationException("boom"))
                ]

        Assert.Throws<InvalidOperationException>(fun () ->
            (client :> IChatClient).GetStreamingResponseAsync(userHistory "hi", ChatOptions(), CancellationToken.None)
            |> ignore)
        |> ignore

    [<Fact>]
    let ``A non-streaming step signals the documented fallback`` () =
        let client = scripted [ ScriptStep.Text "done" ]

        Assert.Throws<NotSupportedException>(fun () ->
            (client :> IChatClient).GetStreamingResponseAsync(userHistory "hi", ChatOptions(), CancellationToken.None)
            |> ignore)
        |> ignore

    [<Fact>]
    let ``An exhausted script fails naming the received messages`` () : Task =
        task {
            let client = scripted [ ScriptStep.Text "first" ]
            let history = userHistory "consult the oracle"

            let! _ = (client :> IChatClient).GetResponseAsync(history, ChatOptions(), CancellationToken.None)

            let invoke () : Task =
                (client :> IChatClient).GetResponseAsync(history, ChatOptions(), CancellationToken.None) :> Task

            let! ex = Assert.ThrowsAsync<InvalidOperationException>(invoke)

            Assert.Contains("consult the oracle", ex.Message)
            Assert.Contains("1 steps", ex.Message)
            Assert.Equal(2, client.Calls)
            Assert.Equal(2, client.ReceivedMessages.Count)
        }

    [<Fact>]
    let ``A cancelled token surfaces before the script`` () : Task =
        task {
            use cts = new CancellationTokenSource()
            cts.Cancel()
            let client = scripted [ ScriptStep.Text "never" ]

            let invoke () : Task =
                (client :> IChatClient).GetResponseAsync(userHistory "hi", ChatOptions(), cts.Token) :> Task

            let! _ = Assert.ThrowsAnyAsync<OperationCanceledException>(invoke)
            Assert.Equal(0, client.Calls)
        }

    [<Fact>]
    let ``A null script is rejected`` () =
        Assert.Throws<ArgumentNullException>(fun () ->
            new ScriptedChatClient(Unchecked.defaultof<IReadOnlyList<ScriptStep>>) |> ignore)
        |> ignore

    [<Fact>]
    let ``A null step is rejected`` () =
        let steps =
            ResizeArray<ScriptStep>([| Unchecked.defaultof<ScriptStep> |]) :> IReadOnlyList<ScriptStep>

        Assert.Throws<ArgumentException>(fun () -> new ScriptedChatClient(steps) |> ignore)
        |> ignore

    [<Fact>]
    let ``Bad step shapes are rejected`` () =
        let emptyCalls = ResizeArray<ScriptToolCall>() :> IReadOnlyList<ScriptToolCall>
        let emptyChunks = ResizeArray<AIContent>() :> IReadOnlyList<AIContent>

        Assert.Throws<ArgumentException>(fun () -> ScriptStep.ToolCalls emptyCalls |> ignore)
        |> ignore

        Assert.Throws<ArgumentException>(fun () -> ScriptStep.Stream emptyChunks |> ignore)
        |> ignore

        Assert.Throws<ArgumentNullException>(fun () -> ScriptStep.Failure(Unchecked.defaultof<Exception>) |> ignore)
        |> ignore

        Assert.Throws<ArgumentException>(fun () -> ScriptToolCall(Unchecked.defaultof<string>, "tool") |> ignore)
        |> ignore

        Assert.Throws<ArgumentException>(fun () -> ScriptToolCall("c1", "  ") |> ignore)
        |> ignore

        Assert.Throws<ArgumentOutOfRangeException>(fun () -> ScriptStep.Text("hi", -1L, 0L) |> ignore)
        |> ignore
