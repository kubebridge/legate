// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.TurnLoopTests

open System
open System.Collections.Generic
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Microsoft.Extensions.AI
open Xunit

// MEAI interop surfaces nulls (queued responses, result payloads); the
// doubles treat every one as empty rather than failing.

// ───────────────────────────────────────────────────────────────────────────
// Doubles

/// Scripted IChatClient: returns queued responses in order and records how
/// many provider calls ran. No Akka, no network.
type ScriptedChatClient(responses: ChatResponse list) =
    let mutable calls = 0

    interface IChatClient with
        member _.GetResponseAsync(_, _, _) =
            calls <- calls + 1
            let index = min (calls - 1) (responses.Length - 1)
            Task.FromResult(responses[index])

        member _.GetStreamingResponseAsync(_, _, _) = raise (NotImplementedException())

        member _.GetService(_, _) = null
        member _.Dispose() = ()

    member _.Calls = calls

let emptyArgs () : IDictionary<string, obj> =
    Dictionary<string, obj>() :> IDictionary<string, obj>

let callMessage (calls: (string * string) list) : ChatMessage =
    let contents =
        calls
        |> List.map (fun (callId, name) -> new FunctionCallContent(callId, name, emptyArgs ()) :> AIContent)
        |> ResizeArray<AIContent>
        :> IList<AIContent>

    new ChatMessage(ChatRole.Assistant, contents)

let textResponse (text: string) : ChatResponse =
    new ChatResponse(
        ResizeArray<ChatMessage>(
            [|
                new ChatMessage(ChatRole.Assistant, text)
            |]
        )
    )

let toolResultText (message: ChatMessage) : string =
    message.Contents
    |> Seq.choose (fun content ->
        match content with
        | :? FunctionResultContent as result when not (isNull (box result)) ->
            match result.Result with
            | :? string as text -> Some(if isNull (box text) then "" else text)
            | null -> Some("")
            | other ->
                match other.ToString() with
                | null -> Some("")
                | value -> Some(value)
        | _ -> None)
    |> Seq.tryHead
    |> Option.defaultValue ""

let toolMessages (history: IList<ChatMessage>) : ChatMessage list =
    [
        for message in history do
            if not (isNull (box message)) && message.Role = ChatRole.Tool then
                yield message
    ]

let alwaysLeased () = true

let makeTools (pairs: (string * AIFunction) list) : IReadOnlyDictionary<string, AITool> =
    let table = Dictionary<string, AITool>()

    for name, fn in pairs do
        table[name] <- fn :> AITool

    table :> IReadOnlyDictionary<string, AITool>

let stubTool (name: string) (result: string) (invocations: string list ref) : AIFunction =
    let method =
        Func<string>(fun () ->
            invocations.Value <- invocations.Value @ [ name ]
            result)

    AIFunctionFactory.Create(method, name, Unchecked.defaultof<string>, Unchecked.defaultof<JsonSerializerOptions>)

let failingTool (name: string) (message: string) : AIFunction =
    let method = Func<string>(fun () -> raise (InvalidOperationException(message)))

    AIFunctionFactory.Create(method, name, Unchecked.defaultof<string>, Unchecked.defaultof<JsonSerializerOptions>)

let private runLoop
    (client: ScriptedChatClient)
    (history: IList<ChatMessage>)
    (tools: IReadOnlyDictionary<string, AITool>)
    (options: TurnLoop.TurnLoopOptions)
    (token: CancellationToken)
    (isLeased: unit -> bool)
    : TurnResult =
    TurnLoop.runAsync (client :> IChatClient) history tools options token isLeased
    |> fun task -> task.GetAwaiter().GetResult()

// ───────────────────────────────────────────────────────────────────────────
// Task 1: skeleton completes with assistant text

[<Fact>]
let ``No tool calls completes with the assistant text`` () =
    let client = new ScriptedChatClient([ textResponse "done" ])

    let history =
        ResizeArray<ChatMessage>(
            [|
                new ChatMessage(ChatRole.User, "hi")
            |]
        )
        :> IList<ChatMessage>

    let tools = makeTools []

    let result =
        runLoop client history tools TurnLoop.TurnLoopOptions.Default CancellationToken.None alwaysLeased

    result.AssistantText |> should equal "done"
    result.Status |> should equal TurnStatus.Completed
    result.Iterations |> should equal 1
    result.Outcome |> should equal null
    client.Calls |> should equal 1

[<Fact>]
let ``Stop after tool round completes with the final text`` () =
    let invocations = ref []
    let fn = stubTool "lookup" "row-1" invocations

    let first =
        new ChatResponse(ResizeArray<ChatMessage>([| callMessage [ "c1", "lookup" ] |]))

    let client = new ScriptedChatClient([ first; textResponse "finished" ])
    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

    let result =
        runLoop
            client
            history
            (makeTools [ "lookup", fn ])
            TurnLoop.TurnLoopOptions.Default
            CancellationToken.None
            alwaysLeased

    result.AssistantText |> should equal "finished"
    result.Iterations |> should equal 2
    invocations.Value |> should equal [ "lookup" ]
    client.Calls |> should equal 2

// ───────────────────────────────────────────────────────────────────────────
// Task 2: sequential in-order execution

[<Fact>]
let ``Multiple calls execute sequentially in order with results appended in order`` () =
    let order = ref []
    let a = stubTool "tool_a" "a-out" order
    let b = stubTool "tool_b" "b-out" order

    let first =
        new ChatResponse(
            ResizeArray<ChatMessage>(
                [|
                    callMessage [ "c1", "tool_a"; "c2", "tool_b" ]
                |]
            )
        )

    let client = new ScriptedChatClient([ first; textResponse "ok" ])
    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

    let result =
        runLoop
            client
            history
            (makeTools [ "tool_a", a; "tool_b", b ])
            TurnLoop.TurnLoopOptions.Default
            CancellationToken.None
            alwaysLeased

    order.Value |> should equal [ "tool_a"; "tool_b" ]
    result.AssistantText |> should equal "ok"

    let results = toolMessages history |> List.map toolResultText
    results |> should equal [ "a-out"; "b-out" ]

// ───────────────────────────────────────────────────────────────────────────
// Task 3: unknown tool and exception mapping

[<Fact>]
let ``Unknown tool continues with Error Unknown tool`` () =
    let first =
        new ChatResponse(ResizeArray<ChatMessage>([| callMessage [ "c1", "missing" ] |]))

    let client = new ScriptedChatClient([ first; textResponse "recovered" ])
    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

    let result =
        runLoop client history (makeTools []) TurnLoop.TurnLoopOptions.Default CancellationToken.None alwaysLeased

    result.AssistantText |> should equal "recovered"

    let results = toolMessages history |> List.map toolResultText
    results |> should equal [ TurnLoop.UnknownToolMessage ]

[<Fact>]
let ``Tool exception continues with Error message only`` () =
    let fn = failingTool "boom_tool" "kaboom"

    let first =
        new ChatResponse(ResizeArray<ChatMessage>([| callMessage [ "c1", "boom_tool" ] |]))

    let client = new ScriptedChatClient([ first; textResponse "recovered" ])
    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

    let result =
        runLoop
            client
            history
            (makeTools [ "boom_tool", fn ])
            TurnLoop.TurnLoopOptions.Default
            CancellationToken.None
            alwaysLeased

    result.AssistantText |> should equal "recovered"

    let results = toolMessages history |> List.map toolResultText
    results |> should equal [ "Error: kaboom" ]

// ───────────────────────────────────────────────────────────────────────────
// Task 4: truncation

[<Fact>]
let ``Overlong tool result is truncated with the marker`` () =
    let options: TurnLoop.TurnLoopOptions = { MaxToolResultChars = 4 }
    let invocations = ref []
    let fn = stubTool "big" "abcdef" invocations

    let first =
        new ChatResponse(ResizeArray<ChatMessage>([| callMessage [ "c1", "big" ] |]))

    let client = new ScriptedChatClient([ first; textResponse "ok" ])
    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

    let result =
        runLoop client history (makeTools [ "big", fn ]) options CancellationToken.None alwaysLeased

    result.AssistantText |> should equal "ok"

    let results = toolMessages history |> List.map toolResultText
    results |> should equal [ "abcd" + TurnLoop.TruncationMarker ]

[<Fact>]
let ``Result exactly at the limit passes through without the marker`` () =
    let options: TurnLoop.TurnLoopOptions = { MaxToolResultChars = 4 }
    let invocations = ref []
    let fn = stubTool "exact" "abcd" invocations

    let first =
        new ChatResponse(ResizeArray<ChatMessage>([| callMessage [ "c1", "exact" ] |]))

    let client = new ScriptedChatClient([ first; textResponse "ok" ])
    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

    runLoop client history (makeTools [ "exact", fn ]) options CancellationToken.None alwaysLeased
    |> ignore

    let results = toolMessages history |> List.map toolResultText
    results |> should equal [ "abcd" ]

// ───────────────────────────────────────────────────────────────────────────
// Task 5: cancellation and lease loss

[<Fact>]
let ``Cancelled token propagates OperationCanceledException`` () =
    use cts = new CancellationTokenSource()
    cts.Cancel()
    let client = new ScriptedChatClient([ textResponse "never" ])
    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

    (fun () ->
        runLoop client history (makeTools []) TurnLoop.TurnLoopOptions.Default cts.Token alwaysLeased
        |> ignore)
    |> should throw typeof<OperationCanceledException>

    client.Calls |> should equal 0

[<Fact>]
let ``Lease loss stops the loop with zero further effects`` () =
    let invocations = ref []
    let fn = stubTool "lookup" "row-1" invocations

    let first =
        new ChatResponse(ResizeArray<ChatMessage>([| callMessage [ "c1", "lookup" ] |]))

    let second =
        new ChatResponse(ResizeArray<ChatMessage>([| callMessage [ "c2", "lookup" ] |]))

    let client = new ScriptedChatClient([ first; second; textResponse "never" ])
    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>
    let mutable calls = 0

    let isLeased () =
        calls <- calls + 1
        // First provider call and first tool run while leased; the second
        // iteration finds the lease lost before any further effect.
        calls <= 2

    (fun () ->
        runLoop
            client
            history
            (makeTools [ "lookup", fn ])
            TurnLoop.TurnLoopOptions.Default
            CancellationToken.None
            isLeased
        |> ignore)
    |> should throw typeof<TurnLoop.TurnLeaseLostException>

    client.Calls |> should equal 1
    invocations.Value |> should equal [ "lookup" ]
