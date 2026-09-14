// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.LlmStreamingTests

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Microsoft.Extensions.AI
open Xunit

// MEAI interop surfaces nulls (queued updates, content payloads); the
// doubles treat every one as empty rather than failing.

// ───────────────────────────────────────────────────────────────────────────
// Doubles

/// Minimal IAsyncEnumerable double over a fixed update list.
type EnumerableUpdates(items: ChatResponseUpdate list) =
    interface IAsyncEnumerable<ChatResponseUpdate> with
        member _.GetAsyncEnumerator(_) =
            let mutable rest = items
            let mutable current = Unchecked.defaultof<ChatResponseUpdate>

            { new IAsyncEnumerator<ChatResponseUpdate> with
                member _.Current = current

                member _.MoveNextAsync() =
                    ValueTask<bool>(
                        task {
                            match rest with
                            | [] -> return false
                            | head :: tail ->
                                rest <- tail
                                current <- head
                                return true
                        }
                    )

                member _.DisposeAsync() = ValueTask()
            }

/// Scripted streaming IChatClient: yields the queued updates for each call
/// in order and records how many provider calls ran. No Akka, no network.
type ScriptedStreamingClient(updatesPerCall: ChatResponseUpdate list list) =
    let mutable calls = 0

    interface IChatClient with
        member _.GetResponseAsync(_, _, _) =
            raise (NotImplementedException("streaming only"))

        member _.GetStreamingResponseAsync(_, _, _) =
            calls <- calls + 1
            let index = min (calls - 1) (updatesPerCall.Length - 1)
            EnumerableUpdates(updatesPerCall[index]) :> IAsyncEnumerable<ChatResponseUpdate>

        member _.GetService(_, _) = null
        member _.Dispose() = ()

    member _.Calls = calls

/// Non-streaming IChatClient: the streaming entry point raises, so the
/// helper must fall back to the queued response with a single delta.
type NonStreamingClient(response: ChatResponse) =
    interface IChatClient with
        member _.GetResponseAsync(_, _, _) = Task.FromResult(response)

        member _.GetStreamingResponseAsync(_, _, _) =
            raise (NotSupportedException("no streaming"))

        member _.GetService(_, _) = null
        member _.Dispose() = ()

let textUpdate (text: string) : ChatResponseUpdate =
    ChatResponseUpdate(Nullable ChatRole.Assistant, text)

let contentsUpdate (contents: AIContent list) : ChatResponseUpdate =
    ChatResponseUpdate(Nullable ChatRole.Assistant, ResizeArray<AIContent>(contents) :> IList<AIContent>)

let reasoningUpdate (text: string) : ChatResponseUpdate =
    contentsUpdate
        [
            TextReasoningContent(text) :> AIContent
        ]

let usageUpdate (input: int64) (output: int64) : ChatResponseUpdate =
    let details = UsageDetails()
    details.InputTokenCount <- Nullable input
    details.OutputTokenCount <- Nullable output
    contentsUpdate [ UsageContent(details) :> AIContent ]

let private runStream (updatesPerCall: ChatResponseUpdate list list) : string list * string list * ChatResponse =
    let client = new ScriptedStreamingClient(updatesPerCall)
    let texts = ResizeArray<string>()
    let reasonings = ResizeArray<string>()
    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

    let response =
        LlmStreaming.streamResponseAsync
            (client :> IChatClient)
            history
            (ChatOptions())
            CancellationToken.None
            (fun text -> texts.Add(text))
            (fun text -> reasonings.Add(text))
        |> fun task -> task.GetAwaiter().GetResult()

    List.ofSeq texts, List.ofSeq reasonings, response

let private runFallback (response: ChatResponse) : string list * string list * ChatResponse =
    let texts = ResizeArray<string>()
    let reasonings = ResizeArray<string>()
    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

    let result =
        LlmStreaming.streamResponseAsync
            (new NonStreamingClient(response) :> IChatClient)
            history
            (ChatOptions())
            CancellationToken.None
            (fun text -> texts.Add(text))
            (fun text -> reasonings.Add(text))
        |> fun task -> task.GetAwaiter().GetResult()

    List.ofSeq texts, List.ofSeq reasonings, result

// ───────────────────────────────────────────────────────────────────────────
// Task 1: text delta order and accumulation

[<Fact>]
let ``Text chunks emit ordered deltas and coalesce into one message`` () =
    let texts, reasonings, response =
        runStream
            [
                [
                    textUpdate "Hel"
                    textUpdate "lo"
                    textUpdate " world"
                ]
            ]

    texts |> should equal [ "Hel"; "lo"; " world" ]
    // Counts rather than `should equal []`: the matcher boxes a generic
    // empty list, which does not compare equal to a typed empty list.
    reasonings.Length |> should equal 0
    response.Text |> should equal "Hello world"
    response.Messages.Count |> should equal 1
    response.Messages[0].Role |> should equal ChatRole.Assistant

[<Fact>]
let ``Empty text chunks emit no delta`` () =
    let nullText = Unchecked.defaultof<string>

    let texts, _, response =
        runStream
            [
                [
                    textUpdate ""
                    textUpdate nullText
                    textUpdate "hi"
                ]
            ]

    texts |> should equal [ "hi" ]
    response.Text |> should equal "hi"

// ───────────────────────────────────────────────────────────────────────────
// Task 2: reasoning deltas and transcript exclusion

[<Fact>]
let ``Reasoning chunks emit reasoning deltas excluded from the text`` () =
    let texts, reasonings, response =
        runStream
            [
                [
                    reasoningUpdate "thinking"
                    textUpdate "done"
                    reasoningUpdate " more"
                ]
            ]

    texts |> should equal [ "done" ]
    reasonings |> should equal [ "thinking"; " more" ]
    response.Text |> should equal "done"

// ───────────────────────────────────────────────────────────────────────────
// Task 3: tool-call content never leaks into deltas

[<Fact>]
let ``Function call content accumulates without emitting deltas`` () =
    let args = Dictionary<string, obj>() :> IDictionary<string, obj>

    let updates =
        [
            textUpdate "calling"
            contentsUpdate
                [
                    FunctionCallContent("c1", "lookup", args) :> AIContent
                ]
        ]

    let texts, reasonings, response = runStream [ updates ]

    texts |> should equal [ "calling" ]
    reasonings.Length |> should equal 0

    let calls =
        response.Messages
        |> Seq.collect (fun message -> message.Contents)
        |> Seq.choose (fun content ->
            match content with
            | :? FunctionCallContent as call when not (isNull (box call)) -> Some call.Name
            | _ -> None)
        |> List.ofSeq

    calls |> should equal [ "lookup" ]

// ───────────────────────────────────────────────────────────────────────────
// Task 4: raw thought-signature preservation

[<Fact>]
let ``Raw representations survive verbatim into the accumulated messages`` () =
    let signature = obj ()
    let payload = obj ()

    let reasoning = TextReasoningContent("thinking")
    reasoning.RawRepresentation <- signature

    let update = contentsUpdate [ reasoning :> AIContent ]
    update.RawRepresentation <- payload

    let _, _, response = runStream [ [ update ] ]

    response.Messages.Count |> should equal 1
    let stored = response.Messages[0].Contents[0] :?> TextReasoningContent
    Object.ReferenceEquals(stored.RawRepresentation, signature) |> should equal true

    Object.ReferenceEquals(response.Messages[0].RawRepresentation, payload)
    |> should equal true

// ───────────────────────────────────────────────────────────────────────────
// Task 5: usage and message boundaries

[<Fact>]
let ``UsageContent sums into Usage and stays out of the contents`` () =
    let _, _, response =
        runStream
            [
                [
                    usageUpdate 3L 5L
                    textUpdate "hi"
                    usageUpdate 7L 11L
                ]
            ]

    match response.Usage with
    | null -> failwith "Expected accumulated usage."
    | usage ->
        usage.InputTokenCount |> should equal (Nullable 10L)
        usage.OutputTokenCount |> should equal (Nullable 16L)

    let kinds =
        response.Messages
        |> Seq.collect (fun message -> message.Contents)
        |> Seq.map (fun content -> content.GetType().Name)
        |> List.ofSeq

    kinds |> should equal [ "TextContent" ]

[<Fact>]
let ``A new MessageId starts a new accumulated message`` () =
    let first = textUpdate "one"
    first.MessageId <- "m1"
    let second = textUpdate "two"
    second.MessageId <- "m2"

    let _, _, response = runStream [ [ first; second ] ]

    response.Messages.Count |> should equal 2
    response.Messages[0].MessageId |> should equal "m1"
    response.Messages[1].MessageId |> should equal "m2"
    response.Messages[0].Text |> should equal "one"
    response.Messages[1].Text |> should equal "two"

// ───────────────────────────────────────────────────────────────────────────
// Task 6: single-delta fallback and null parity

[<Fact>]
let ``Non-streaming providers fall back to a single delta per kind`` () =
    let message =
        new ChatMessage(
            ChatRole.Assistant,
            ResizeArray<AIContent>(
                [|
                    TextContent("Hel") :> AIContent
                    TextContent("lo") :> AIContent
                    TextReasoningContent("why") :> AIContent
                |]
            )
            :> IList<AIContent>
        )

    let texts, reasonings, response =
        runFallback (new ChatResponse(ResizeArray<ChatMessage>([| message |])))

    texts |> should equal [ "Hello" ]
    reasonings |> should equal [ "why" ]
    response.Text |> should equal "Hello"

[<Fact>]
let ``Null fallback response returns null with no deltas`` () =
    let nullResponse = Unchecked.defaultof<ChatResponse>
    let texts, reasonings, response = runFallback nullResponse

    texts.Length |> should equal 0
    reasonings.Length |> should equal 0
    response |> box |> should equal null
