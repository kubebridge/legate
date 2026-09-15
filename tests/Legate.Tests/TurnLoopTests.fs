// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.TurnLoopTests

open System
open System.Collections.Generic
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Legate.Testing
open Legate.Tests.LlmStreamingTests
open Microsoft.Extensions.AI
open Microsoft.Extensions.Time.Testing
open Xunit

// MEAI interop surfaces nulls (queued responses, result payloads); the
// doubles treat every one as empty rather than failing.

// ───────────────────────────────────────────────────────────────────────────
// Doubles

/// An ILlmDelay that never elapses unless its token fires: the loop's hard
/// deadline stays pending, so tests that do not exercise the deadline run
/// without one. The deadline tests pass a clock-backed delay instead.
type NeverDelay() =
    interface ILlmDelay with
        member _.Delay(_, cancellationToken) =
            Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken)

/// An ILlmDelay that records every requested delay and waits on the given
/// clock: under a FakeTimeProvider the wait completes when the test
/// advances past it, so the deadline fires deterministically without
/// sleeping.
type RecordingClockDelay(clock: TimeProvider, recorded: ResizeArray<TimeSpan>) =
    interface ILlmDelay with
        member _.Delay(delay, cancellationToken) =
            recorded.Add(delay)
            Task.Delay(delay, clock, cancellationToken)

/// Wraps script steps in the shared scripted client.
let scripted (steps: ScriptStep list) : ScriptedChatClient =
    new ScriptedChatClient(ResizeArray<ScriptStep>(steps) :> IReadOnlyList<ScriptStep>)

/// One scripted assistant text step.
let textStep (text: string) : ScriptStep = ScriptStep.Text text

/// One scripted single tool-call step with empty arguments.
let callStep (callId: string) (name: string) : ScriptStep = ScriptStep.ToolCall(callId, name)

/// One scripted multi-call step from call-id/tool-name pairs.
let callSteps (calls: (string * string) list) : ScriptStep =
    ScriptStep.ToolCalls(
        ResizeArray<ScriptToolCall>(
            calls
            |> List.map (fun (callId, name) -> ScriptToolCall(callId, name))
            |> Array.ofList
        )
        :> IReadOnlyList<ScriptToolCall>
    )

/// One scripted streaming step from content chunks.
let streamStep (contents: AIContent list) : ScriptStep =
    ScriptStep.Stream(ResizeArray<AIContent>(contents) :> IReadOnlyList<AIContent>)

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

/// IChatClient double that treats the passed token as the work: it waits for
/// the token to fire (event-driven on the wait handle, no sleeps) and then
/// observes it, so a linked deadline surfaces as
/// OperationCanceledException from in-flight provider work.
type DeadlineObservingClient() =
    let mutable calls = 0

    interface IChatClient with
        member _.GetResponseAsync(_, _, ct) =
            calls <- calls + 1

            task {
                use handle = ct.WaitHandle
                handle.WaitOne(TimeSpan.FromSeconds 30.0) |> ignore
                ct.ThrowIfCancellationRequested()
                return textResponse "never"
            }

        member _.GetStreamingResponseAsync(_, _, _) = raise (NotImplementedException())

        member _.GetService(_, _) = null
        member _.Dispose() = ()

    member _.Calls = calls

/// IChatClient double that waits asynchronously for its token to fire: the
/// test thread stays free to advance the virtual seam clock while the
/// provider call is in flight, so a seam deadline surfaces as
/// OperationCanceledException from in-flight provider work. Unlike
/// DeadlineObservingClient (which blocks the calling thread in WaitOne and
/// suits real-clock deadlines), this client never blocks the test thread.
type AsyncDeadlineClient() =
    let mutable calls = 0

    interface IChatClient with
        member _.GetResponseAsync(_, _, ct) =
            calls <- calls + 1

            task {
                do! Task.Delay(Timeout.InfiniteTimeSpan, ct)
                return textResponse "never"
            }

        member _.GetStreamingResponseAsync(_, _, _) = raise (NotImplementedException())

        member _.GetService(_, _) = null
        member _.Dispose() = ()

    member _.Calls = calls

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
    TurnLoop.runAsync (client :> IChatClient) history tools options (NeverDelay() :> ILlmDelay) token isLeased
    |> fun task -> task.GetAwaiter().GetResult()

// ───────────────────────────────────────────────────────────────────────────
// Task 1: skeleton completes with assistant text

[<Fact>]
let ``No tool calls completes with the assistant text`` () =
    let client = scripted [ textStep "done" ]

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

    let first = callStep "c1" "lookup"

    let client = scripted [ first; textStep "finished" ]
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

    let client =
        scripted
            [
                callSteps [ "c1", "tool_a"; "c2", "tool_b" ]
                textStep "ok"
            ]

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
    let first = callStep "c1" "missing"

    let client = scripted [ first; textStep "recovered" ]
    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

    let result =
        runLoop client history (makeTools []) TurnLoop.TurnLoopOptions.Default CancellationToken.None alwaysLeased

    result.AssistantText |> should equal "recovered"

    let results = toolMessages history |> List.map toolResultText
    results |> should equal [ TurnLoop.UnknownToolMessage ]

[<Fact>]
let ``Tool exception continues with Error message only`` () =
    let fn = failingTool "boom_tool" "kaboom"

    let first = callStep "c1" "boom_tool"

    let client = scripted [ first; textStep "recovered" ]
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
    let options: TurnLoop.TurnLoopOptions =
        { TurnLoop.TurnLoopOptions.Default with
            MaxToolResultChars = 4
        }

    let invocations = ref []
    let fn = stubTool "big" "abcdef" invocations

    let first = callStep "c1" "big"

    let client = scripted [ first; textStep "ok" ]
    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

    let result =
        runLoop client history (makeTools [ "big", fn ]) options CancellationToken.None alwaysLeased

    result.AssistantText |> should equal "ok"

    let results = toolMessages history |> List.map toolResultText
    results |> should equal [ "abcd" + TurnLoop.TruncationMarker ]

[<Fact>]
let ``Result exactly at the limit passes through without the marker`` () =
    let options: TurnLoop.TurnLoopOptions =
        { TurnLoop.TurnLoopOptions.Default with
            MaxToolResultChars = 4
        }

    let invocations = ref []
    let fn = stubTool "exact" "abcd" invocations

    let first = callStep "c1" "exact"

    let client = scripted [ first; textStep "ok" ]
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
    let client = scripted [ textStep "never" ]
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

    let first = callStep "c1" "lookup"

    let second = callStep "c2" "lookup"

    let client = scripted [ first; second; textStep "never" ]
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

// ───────────────────────────────────────────────────────────────────────────
// Issue 40: delta streaming at the iteration call site

/// Streaming IChatClient that treats the passed token as the work: its
/// enumeration waits for the token to fire (no sleeps) and then observes
/// it, so a linked deadline surfaces as OperationCanceledException from
/// in-flight streaming work.
type DeadlineUpdates(cancellationToken: CancellationToken) =
    interface IAsyncEnumerable<ChatResponseUpdate> with
        member _.GetAsyncEnumerator(_) =
            { new IAsyncEnumerator<ChatResponseUpdate> with
                member _.Current = Unchecked.defaultof<ChatResponseUpdate>

                member _.MoveNextAsync() =
                    ValueTask<bool>(
                        task {
                            do! Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken)
                            return false
                        }
                    )

                member _.DisposeAsync() = ValueTask()
            }

/// Streaming IChatClient that treats the passed token as the work: it waits
/// for the token to fire (no sleeps) and then observes it, so a linked
/// deadline surfaces as OperationCanceledException from in-flight streaming
/// work.
type StreamingDeadlineClient() =
    let mutable calls = 0

    interface IChatClient with
        member _.GetResponseAsync(_, _, _) =
            raise (NotImplementedException("streaming only"))

        member _.GetStreamingResponseAsync(_, _, ct) =
            calls <- calls + 1
            DeadlineUpdates(ct) :> IAsyncEnumerable<ChatResponseUpdate>

        member _.GetService(_, _) = null
        member _.Dispose() = ()

    member _.Calls = calls

/// One scripted text chunk.
let textChunk (text: string) : AIContent = TextContent(text) :> AIContent

/// One scripted reasoning chunk.
let reasoningChunk (text: string) : AIContent = TextReasoningContent(text) :> AIContent

let private runLoopWithDeltas
    (client: IChatClient)
    (history: IList<ChatMessage>)
    (tools: IReadOnlyDictionary<string, AITool>)
    (options: TurnLoop.TurnLoopOptions)
    (token: CancellationToken)
    (isLeased: unit -> bool)
    : TurnResult * string list * string list =
    let texts = ResizeArray<string>()
    let reasonings = ResizeArray<string>()

    let result =
        TurnLoop.runAsyncWithDeltas
            client
            history
            tools
            options
            (NeverDelay() :> ILlmDelay)
            token
            isLeased
            (fun text -> texts.Add(text))
            (fun text -> reasonings.Add(text))
        |> fun task -> task.GetAwaiter().GetResult()

    result, List.ofSeq texts, List.ofSeq reasonings

let assistantTextOf (history: IList<ChatMessage>) : string =
    history
    |> Seq.collect (fun message ->
        if isNull (box message) || message.Role <> ChatRole.Assistant then
            Seq.empty
        else
            message.Contents
            |> Seq.choose (fun content ->
                match content with
                | :? TextContent as text when not (isNull (box text)) ->
                    Some(if isNull (box text.Text) then "" else text.Text)
                | _ -> None))
    |> String.concat ""

[<Fact>]
let ``Streaming chunks emit ordered deltas and coalesce into one history message`` () =
    let client =
        scripted
            [
                streamStep
                    [
                        textChunk "Hel"
                        textChunk "lo"
                        textChunk " world"
                    ]
            ]

    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

    let result, texts, reasonings =
        runLoopWithDeltas
            (client :> IChatClient)
            history
            (makeTools [])
            TurnLoop.TurnLoopOptions.Default
            CancellationToken.None
            alwaysLeased

    result.AssistantText |> should equal "Hello world"
    result.Status |> should equal TurnStatus.Completed
    result.Iterations |> should equal 1
    texts |> should equal [ "Hel"; "lo"; " world" ]
    // Counts rather than `should equal []`: the matcher boxes a generic
    // empty list, which does not compare equal to a typed empty list.
    reasonings.Length |> should equal 0
    client.Calls |> should equal 1
    assistantTextOf history |> should equal "Hello world"

[<Fact>]
let ``Streaming deltas fold into a single Assistant cell with reasoning excluded`` () =
    let client =
        scripted
            [
                streamStep
                    [
                        reasoningChunk "hmm"
                        textChunk "done"
                        reasoningChunk " more"
                    ]
            ]

    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

    let result, texts, reasonings =
        runLoopWithDeltas
            (client :> IChatClient)
            history
            (makeTools [])
            TurnLoop.TurnLoopOptions.Default
            CancellationToken.None
            alwaysLeased

    result.AssistantText |> should equal "done"
    texts |> should equal [ "done" ]
    reasonings |> should equal [ "hmm"; " more" ]

    let sessionId = SessionId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"
    let turnId = TurnId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"
    let stamp = DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero)
    let noSequence = Unchecked.defaultof<Nullable<int64>>

    let events =
        ResizeArray<SessionEvent>(
            texts
            |> List.map (fun text -> TextDeltaEvent(sessionId, turnId, noSequence, stamp, text) :> SessionEvent)
        )
        :> IReadOnlyList<SessionEvent>

    let cells = SessionCellDeriver.Fold(sessionId, turnId, null, stamp, events)
    cells.Count |> should equal 1
    cells[0].Kind |> should equal SessionCellKind.Assistant
    cells[0].Content |> should equal "done"

[<Fact>]
let ``Streaming preserves raw thought signatures into the history`` () =
    let signature = obj ()
    let payload = obj ()
    let reasoning = TextReasoningContent("thinking")
    reasoning.RawRepresentation <- signature

    let client =
        scripted
            [
                ScriptStep.Stream(
                    ResizeArray<AIContent>(
                        [|
                            reasoning :> AIContent
                            textChunk "done"
                        |]
                    )
                    :> IReadOnlyList<AIContent>,
                    payload
                )
            ]

    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

    let result, _, reasonings =
        runLoopWithDeltas
            (client :> IChatClient)
            history
            (makeTools [])
            TurnLoop.TurnLoopOptions.Default
            CancellationToken.None
            alwaysLeased

    result.AssistantText |> should equal "done"
    reasonings |> should equal [ "thinking" ]

    let stored =
        history
        |> Seq.collect (fun message -> message.Contents)
        |> Seq.choose (fun content ->
            match content with
            | :? TextReasoningContent as stored when not (isNull (box stored)) -> Some stored
            | _ -> None)
        |> List.ofSeq

    stored.Length |> should equal 1

    Object.ReferenceEquals(stored[0].RawRepresentation, signature)
    |> should equal true

    Object.ReferenceEquals(history[0].RawRepresentation, payload)
    |> should equal true

[<Fact>]
let ``Non-streaming clients fall back to a single delta`` () =
    let client = scripted [ textStep "done" ]
    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

    let result, texts, reasonings =
        runLoopWithDeltas
            (client :> IChatClient)
            history
            (makeTools [])
            TurnLoop.TurnLoopOptions.Default
            CancellationToken.None
            alwaysLeased

    result.AssistantText |> should equal "done"
    texts |> should equal [ "done" ]
    reasonings.Length |> should equal 0
    client.Calls |> should equal 1

[<Fact>]
let ``Streaming respects the iteration deadline`` () =
    let client = new StreamingDeadlineClient()
    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

    let options: TurnLoop.TurnLoopOptions =
        { TurnLoop.TurnLoopOptions.Default with
            Timeout = TimeSpan.FromMilliseconds 100.0
        }

    let result, texts, _ =
        let texts = ResizeArray<string>()
        let reasonings = ResizeArray<string>()

        let result =
            TurnLoop.runAsyncWithDeltas
                (client :> IChatClient)
                history
                (makeTools [])
                options
                (SystemLlmDelay() :> ILlmDelay)
                CancellationToken.None
                alwaysLeased
                (fun text -> texts.Add(text))
                (fun text -> reasonings.Add(text))
            |> fun task -> task.GetAwaiter().GetResult()

        result, List.ofSeq texts, List.ofSeq reasonings

    result.Status |> should equal TurnStatus.Failed
    result.AssistantText |> should equal ""
    texts.Length |> should equal 0
    client.Calls |> should equal 1

    match result.Outcome with
    | :? TurnFailed as failed -> failed.Reason |> should equal TurnLoop.TimeoutExceededMessage
    | _ -> failwith "Expected a TurnFailed outcome."

// ───────────────────────────────────────────────────────────────────────────
// Issue 39 Task 1: effective budget resolution

[<Fact>]
let ``Unset session knobs resolve to the Turns defaults`` () =
    let resolved = TurnLoop.resolveBudget (SessionOptions()) (TurnsOptions())

    resolved.MaxIterations |> should equal 50
    resolved.Timeout |> should equal (TimeSpan.FromMinutes 30.0)
    resolved.MaxToolResultChars |> should equal TurnLoop.DefaultMaxToolResultChars

[<Fact>]
let ``Per-session overrides win over the Turns defaults`` () =
    let session = SessionOptions()
    session.MaxIterations <- 7
    session.Timeout <- Nullable(TimeSpan.FromMinutes 5.0)

    let turns = TurnsOptions()
    turns.DefaultMaxIterations <- 10
    turns.DefaultTimeout <- TimeSpan.FromHours 1.0

    let resolved = TurnLoop.resolveBudget session turns

    resolved.MaxIterations |> should equal 7
    resolved.Timeout |> should equal (TimeSpan.FromMinutes 5.0)

[<Fact>]
let ``Negative session MaxIterations is rejected`` () =
    let session = SessionOptions()
    session.MaxIterations <- -1

    (fun () -> TurnLoop.resolveBudget session (TurnsOptions()) |> ignore)
    |> should throw typeof<ArgumentOutOfRangeException>

[<Fact>]
let ``Non-positive session Timeout is rejected`` () =
    let session = SessionOptions()
    session.Timeout <- Nullable(TimeSpan.Zero)

    (fun () -> TurnLoop.resolveBudget session (TurnsOptions()) |> ignore)
    |> should throw typeof<ArgumentOutOfRangeException>

[<Fact>]
let ``Non-positive Turns default iterations are rejected`` () =
    let turns = TurnsOptions(DefaultMaxIterations = 0)

    (fun () -> TurnLoop.resolveBudget (SessionOptions()) turns |> ignore)
    |> should throw typeof<ArgumentOutOfRangeException>

[<Fact>]
let ``Non-positive Turns default timeout is rejected`` () =
    let turns = TurnsOptions(DefaultTimeout = TimeSpan.Zero)

    (fun () -> TurnLoop.resolveBudget (SessionOptions()) turns |> ignore)
    |> should throw typeof<ArgumentOutOfRangeException>

// ───────────────────────────────────────────────────────────────────────────
// Issue 39 Task 2: iteration budget

[<Fact>]
let ``Iteration cap settles Failed with TurnFailed and makes no further provider call`` () =
    let invocations = ref []
    let fn = stubTool "lookup" "row-1" invocations

    let first = callStep "c1" "lookup"

    let second = callStep "c2" "lookup"

    let client = scripted [ first; second; textStep "never" ]
    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

    let options: TurnLoop.TurnLoopOptions =
        { TurnLoop.TurnLoopOptions.Default with
            MaxIterations = 1
        }

    let result =
        runLoop client history (makeTools [ "lookup", fn ]) options CancellationToken.None alwaysLeased

    result.Status |> should equal TurnStatus.Failed
    result.Iterations |> should equal 1
    result.AssistantText |> should equal ""
    client.Calls |> should equal 1
    invocations.Value |> should equal [ "lookup" ]

    match result.Outcome with
    | :? TurnFailed as failed -> failed.Reason |> should equal TurnLoop.MaxIterationsExceededMessage
    | _ -> failwith "Expected a TurnFailed outcome."

[<Fact>]
let ``Zero MaxIterations on the loop options is rejected`` () =
    let client = scripted [ textStep "never" ]
    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

    let options: TurnLoop.TurnLoopOptions =
        { TurnLoop.TurnLoopOptions.Default with
            MaxIterations = 0
        }

    (fun () ->
        runLoop client history (makeTools []) options CancellationToken.None alwaysLeased
        |> ignore)
    |> should throw typeof<ArgumentOutOfRangeException>

[<Fact>]
let ``Lease loss wins over an exhausted iteration budget`` () =
    let invocations = ref []
    let fn = stubTool "lookup" "row-1" invocations

    let first = callStep "c1" "lookup"

    let client = scripted [ first; textStep "never" ]
    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>
    let mutable checks = 0

    // Leased for the first provider call and its tool; the second loop
    // top finds both a dead lease and a spent budget, and the lease hook
    // stays first so the loser throws with zero further effects.
    let isLeased () =
        checks <- checks + 1
        checks <= 2

    let options: TurnLoop.TurnLoopOptions =
        { TurnLoop.TurnLoopOptions.Default with
            MaxIterations = 1
        }

    (fun () ->
        runLoop client history (makeTools [ "lookup", fn ]) options CancellationToken.None isLeased
        |> ignore)
    |> should throw typeof<TurnLoop.TurnLeaseLostException>

    client.Calls |> should equal 1
    invocations.Value |> should equal [ "lookup" ]

// ───────────────────────────────────────────────────────────────────────────
// Issue 39 Task 3: timeout budget

[<Fact>]
let ``Deadline expiry settles Failed as the hard-deadline stop and cancels in-flight work`` () =
    let client = new DeadlineObservingClient()
    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

    let options: TurnLoop.TurnLoopOptions =
        { TurnLoop.TurnLoopOptions.Default with
            Timeout = TimeSpan.FromMilliseconds 100.0
        }

    let result =
        TurnLoop.runAsync
            (client :> IChatClient)
            history
            (makeTools [])
            options
            (SystemLlmDelay() :> ILlmDelay)
            CancellationToken.None
            alwaysLeased
        |> fun task -> task.GetAwaiter().GetResult()

    result.Status |> should equal TurnStatus.Failed
    result.AssistantText |> should equal ""
    client.Calls |> should equal 1

    match result.Outcome with
    | :? TurnFailed as failed -> failed.Reason |> should equal TurnLoop.TimeoutExceededMessage
    | _ -> failwith "Expected a TurnFailed outcome."

// ───────────────────────────────────────────────────────────────────────────
// Issue 35: hard deadline off the injected delay seam

/// Spins until the condition holds or the bound lapses. Sleeps are the poll
/// cadence only: the deadline fires off the virtual clock, never a sleep.
let private waitForSeam (timeout: TimeSpan) (condition: unit -> bool) : bool =
    let deadline = DateTime.UtcNow + timeout
    let mutable holds = condition ()

    while not holds && DateTime.UtcNow < deadline do
        Thread.Sleep(10)
        holds <- condition ()

    holds

[<Fact>]
let ``Seam deadline fires only when the injected clock advances past the timeout`` () =
    let clock = FakeTimeProvider()
    let requested = ResizeArray<TimeSpan>()
    let delay = RecordingClockDelay(clock, requested) :> ILlmDelay
    let client = new AsyncDeadlineClient()
    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

    let options: TurnLoop.TurnLoopOptions =
        { TurnLoop.TurnLoopOptions.Default with
            Timeout = TimeSpan.FromMinutes 5.0
        }

    let runTask =
        TurnLoop.runAsync
            (client :> IChatClient)
            history
            (makeTools [])
            options
            delay
            CancellationToken.None
            alwaysLeased

    // The deadline arms on the seam clock before the provider call blocks:
    // poll for the recorded request, never sleep past it.
    let armed = waitForSeam (TimeSpan.FromSeconds 5.0) (fun () -> requested.Count = 1)

    armed |> should equal true
    requested |> List.ofSeq |> should equal [ TimeSpan.FromMinutes 5.0 ]

    // Still pending before the budget elapses: advancing short of it fires
    // nothing.
    clock.Advance(TimeSpan.FromMinutes 4.0)
    runTask.IsCompleted |> should equal false

    clock.Advance(TimeSpan.FromMinutes 1.0 + TimeSpan.FromSeconds 1.0)

    let finished = runTask.Wait(TimeSpan.FromSeconds 10.0)
    finished |> should equal true

    let result = runTask.GetAwaiter().GetResult()
    result.Status |> should equal TurnStatus.Failed
    result.AssistantText |> should equal ""
    client.Calls |> should equal 1

    match result.Outcome with
    | :? TurnFailed as failed -> failed.Reason |> should equal TurnLoop.TimeoutExceededMessage
    | _ -> failwith "Expected a TurnFailed outcome."

[<Fact>]
let ``External cancellation wins over a pending seam deadline`` () =
    use cts = new CancellationTokenSource()
    let clock = FakeTimeProvider()
    let delay = SystemLlmDelay(clock) :> ILlmDelay
    let client = new DeadlineObservingClient()
    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

    let options: TurnLoop.TurnLoopOptions =
        { TurnLoop.TurnLoopOptions.Default with
            Timeout = TimeSpan.FromMinutes 5.0
        }

    // The caller aborts before the seam clock ever advances: the
    // cancellation propagates instead of settling, so isTimeout never
    // conflates the abort with the deadline.
    cts.Cancel()

    (fun () ->
        TurnLoop.runAsync (client :> IChatClient) history (makeTools []) options delay cts.Token alwaysLeased
        |> fun task -> task.GetAwaiter().GetResult() |> ignore)
    |> should throw typeof<OperationCanceledException>

    client.Calls |> should equal 0

[<Fact>]
let ``A null delay seam is rejected`` () =
    let client = scripted [ textStep "never" ]
    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>
    let nullDelay = Unchecked.defaultof<ILlmDelay>

    (fun () ->
        TurnLoop.runAsync
            (client :> IChatClient)
            history
            (makeTools [])
            TurnLoop.TurnLoopOptions.Default
            nullDelay
            CancellationToken.None
            alwaysLeased
        |> fun task -> task.GetAwaiter().GetResult() |> ignore)
    |> should throw typeof<ArgumentNullException>

[<Fact>]
let ``External cancellation still propagates when a short deadline is configured`` () =
    use cts = new CancellationTokenSource()
    cts.Cancel()
    let client = scripted [ textStep "never" ]
    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

    let options: TurnLoop.TurnLoopOptions =
        { TurnLoop.TurnLoopOptions.Default with
            Timeout = TimeSpan.FromMilliseconds 100.0
        }

    (fun () -> runLoop client history (makeTools []) options cts.Token alwaysLeased |> ignore)
    |> should throw typeof<OperationCanceledException>

    client.Calls |> should equal 0

[<Fact>]
let ``Zero Timeout on the loop options is rejected`` () =
    let client = scripted [ textStep "never" ]
    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

    let options: TurnLoop.TurnLoopOptions =
        { TurnLoop.TurnLoopOptions.Default with
            Timeout = TimeSpan.Zero
        }

    (fun () ->
        runLoop client history (makeTools []) options CancellationToken.None alwaysLeased
        |> ignore)
    |> should throw typeof<ArgumentOutOfRangeException>

// ───────────────────────────────────────────────────────────────────────────
// Issue 39 Task 5: resolved session budget flows into the loop

[<Fact>]
let ``Resolved session budget flows into the loop`` () =
    let session = SessionOptions()
    session.MaxIterations <- 1
    let resolved = TurnLoop.resolveBudget session (TurnsOptions())

    let invocations = ref []
    let fn = stubTool "lookup" "row-1" invocations

    let first = callStep "c1" "lookup"

    let client = scripted [ first; textStep "never" ]
    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

    let result =
        runLoop client history (makeTools [ "lookup", fn ]) resolved CancellationToken.None alwaysLeased

    result.Status |> should equal TurnStatus.Failed
    result.Iterations |> should equal 1
    client.Calls |> should equal 1

    match result.Outcome with
    | :? TurnFailed as failed -> failed.Reason |> should equal TurnLoop.MaxIterationsExceededMessage
    | _ -> failwith "Expected a TurnFailed outcome."

// ───────────────────────────────────────────────────────────────────────────
// Issue 41: Inject fold at iteration boundaries

let sampleInjectSessionId () =
    SessionId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"

let injectStamp () =
    DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero)

let injectEntry (position: int64) (payload: InboxPayload) (delivery: DeliveryMode) : InboxEntry =
    {
        SessionId = sampleInjectSessionId ()
        Position = position
        Payload = payload
        Delivery = delivery
        Consumed = false
        AppendedAt = injectStamp ()
    }

let textInject (position: int64) (text: string) : InboxEntry =
    injectEntry position (UserMessagePayload(UserMessage.Text text) :> InboxPayload) DeliveryMode.Inject

let userMessagesOf (history: IList<ChatMessage>) : ChatMessage list =
    [
        for message in history do
            if not (isNull (box message)) && message.Role = ChatRole.User then
                yield message
    ]

let userTextsOf (history: IList<ChatMessage>) : string list =
    [
        for message in userMessagesOf history do
            for content in message.Contents do
                match content with
                | :? TextContent as text when not (isNull (box text)) ->
                    yield (if isNull (box text.Text) then "" else text.Text)
                | _ -> ()
    ]

let private runLoopWithInjects
    (client: ScriptedChatClient)
    (history: IList<ChatMessage>)
    (tools: IReadOnlyDictionary<string, AITool>)
    (options: TurnLoop.TurnLoopOptions)
    (token: CancellationToken)
    (isLeased: unit -> bool)
    (drain: TurnLoop.DrainInjected)
    (onJournaled: TurnLoop.JournalInjected)
    (onConsumed: TurnLoop.ConsumeInjected)
    : TurnLoop.TurnLoopCompletion =
    TurnLoop.runAsyncWithInjects
        (client :> IChatClient)
        history
        tools
        options
        (NeverDelay() :> ILlmDelay)
        token
        isLeased
        drain
        onJournaled
        onConsumed
    |> fun task -> task.GetAwaiter().GetResult()

[<Fact>]
let ``Injects fold after tool results before the next provider call in arrival order`` () =
    let invocations = ref []
    let fn = stubTool "lookup" "row-1" invocations

    let first = callStep "c1" "lookup"

    let client = scripted [ first; textStep "finished" ]
    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

    let firstInject = textInject 2L "steer-one"
    let secondInject = textInject 1L "steer-two"
    let journaled = ResizeArray<InboxEntry>()
    let consumed = ResizeArray<InboxEntry>()
    let mutable drains = 0

    // Empty before the first provider call; the two injects arrive while
    // the tool runs and fold at the next boundary. Positions arrive
    // out of order so the fold must sort them.
    let drain () : IReadOnlyList<InboxEntry> =
        drains <- drains + 1

        if drains = 2 then
            ResizeArray<InboxEntry>([| firstInject; secondInject |]) :> IReadOnlyList<InboxEntry>
        else
            ResizeArray<InboxEntry>() :> IReadOnlyList<InboxEntry>

    let completion =
        runLoopWithInjects
            client
            history
            (makeTools [ "lookup", fn ])
            TurnLoop.TurnLoopOptions.Default
            CancellationToken.None
            alwaysLeased
            drain
            journaled.Add
            consumed.Add

    completion.Result.AssistantText |> should equal "finished"
    completion.Result.Status |> should equal TurnStatus.Completed
    // Injects never spend the iteration budget: two provider calls only.
    completion.Result.Iterations |> should equal 2
    completion.HasPendingInjects |> should equal false
    client.Calls |> should equal 2

    // Placement: tool result, then the injects in position order, then
    // the final assistant message.
    let roles = history |> Seq.map (fun message -> message.Role) |> List.ofSeq

    roles
    |> should
        equal
        [
            ChatRole.Assistant
            ChatRole.Tool
            ChatRole.User
            ChatRole.User
            ChatRole.Assistant
        ]

    userTextsOf history |> should equal [ "steer-two"; "steer-one" ]
    journaled |> List.ofSeq |> should equal [ secondInject; firstInject ]
    consumed |> List.ofSeq |> should equal [ secondInject; firstInject ]

[<Fact>]
let ``Empty drain is a no-op`` () =
    let client = scripted [ textStep "done" ]
    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>
    let journaled = ResizeArray<InboxEntry>()
    let consumed = ResizeArray<InboxEntry>()

    let drain () : IReadOnlyList<InboxEntry> =
        ResizeArray<InboxEntry>() :> IReadOnlyList<InboxEntry>

    let completion =
        runLoopWithInjects
            client
            history
            (makeTools [])
            TurnLoop.TurnLoopOptions.Default
            CancellationToken.None
            alwaysLeased
            drain
            journaled.Add
            consumed.Add

    completion.Result.AssistantText |> should equal "done"
    completion.Result.Iterations |> should equal 1
    completion.HasPendingInjects |> should equal false
    journaled.Count |> should equal 0
    consumed.Count |> should equal 0
    userMessagesOf history |> List.length |> should equal 0

[<Fact>]
let ``Queue Interrupt and Reply entries are ignored and stay pending`` () =
    let client = scripted [ textStep "done" ]
    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

    let queued =
        injectEntry 1L (UserMessagePayload(UserMessage.Text "queued") :> InboxPayload) DeliveryMode.Queue

    let interrupted =
        injectEntry 2L (UserMessagePayload(UserMessage.Text "interrupted") :> InboxPayload) DeliveryMode.Interrupt

    let replyInject =
        injectEntry
            3L
            (ReplyPayload(PermissionDecision("req-1", PermissionDecisionKind.AllowOnce) :> Reply) :> InboxPayload)
            DeliveryMode.Inject

    let valid = textInject 4L "steer"
    let journaled = ResizeArray<InboxEntry>()
    let consumed = ResizeArray<InboxEntry>()

    let store =
        ResizeArray<InboxEntry>(
            [|
                queued
                interrupted
                replyInject
                valid
            |]
        )

    let drain () : IReadOnlyList<InboxEntry> =
        let pending =
            store
            |> Seq.filter (fun entry -> consumed |> Seq.exists (fun c -> c.Position = entry.Position) |> not)
            |> List.ofSeq

        ResizeArray<InboxEntry>(pending) :> IReadOnlyList<InboxEntry>

    let completion =
        runLoopWithInjects
            client
            history
            (makeTools [])
            TurnLoop.TurnLoopOptions.Default
            CancellationToken.None
            alwaysLeased
            drain
            journaled.Add
            consumed.Add

    completion.Result.AssistantText |> should equal "done"
    completion.HasPendingInjects |> should equal false
    // Only the Inject user message folds; the rest stay pending.
    userTextsOf history |> should equal [ "steer" ]
    journaled |> List.ofSeq |> should equal [ valid ]
    consumed |> List.ofSeq |> should equal [ valid ]

[<Fact>]
let ``Inject arriving during the final iteration stays pending with the new-turn signal`` () =
    let client = scripted [ textStep "done" ]
    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>
    let late = textInject 7L "late-steer"
    let journaled = ResizeArray<InboxEntry>()
    let consumed = ResizeArray<InboxEntry>()
    let mutable drains = 0

    // Empty at the iteration boundary, then the inject arrives while the
    // final provider call is in flight and is peeked on the
    // would-complete path.
    let drain () : IReadOnlyList<InboxEntry> =
        drains <- drains + 1

        if drains = 1 then
            ResizeArray<InboxEntry>() :> IReadOnlyList<InboxEntry>
        else
            ResizeArray<InboxEntry>([| late |]) :> IReadOnlyList<InboxEntry>

    let completion =
        runLoopWithInjects
            client
            history
            (makeTools [])
            TurnLoop.TurnLoopOptions.Default
            CancellationToken.None
            alwaysLeased
            drain
            journaled.Add
            consumed.Add

    completion.Result.AssistantText |> should equal "done"
    completion.Result.Status |> should equal TurnStatus.Completed
    completion.HasPendingInjects |> should equal true
    // Nothing folded: the message stays pending for the session actor.
    userMessagesOf history |> List.length |> should equal 0
    journaled.Count |> should equal 0
    consumed.Count |> should equal 0

[<Fact>]
let ``Inject appends raw parts verbatim and ignores metadata`` () =
    let client = scripted [ textStep "done" ]
    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

    let parts =
        ResizeArray<AIContent>(
            [|
                TextContent("first") :> AIContent
                TextContent("second") :> AIContent
            |]
        )
        :> IReadOnlyList<AIContent>

    let metadata = Dictionary<string, string>()
    metadata["source"] <- "host"
    let message = UserMessage(parts, metadata)

    let entry =
        injectEntry 1L (UserMessagePayload(message) :> InboxPayload) DeliveryMode.Inject

    let journaled = ResizeArray<InboxEntry>()
    let consumed = ResizeArray<InboxEntry>()

    let drain () : IReadOnlyList<InboxEntry> =
        if consumed.Count = 0 then
            ResizeArray<InboxEntry>([| entry |]) :> IReadOnlyList<InboxEntry>
        else
            ResizeArray<InboxEntry>() :> IReadOnlyList<InboxEntry>

    let completion =
        runLoopWithInjects
            client
            history
            (makeTools [])
            TurnLoop.TurnLoopOptions.Default
            CancellationToken.None
            alwaysLeased
            drain
            journaled.Add
            consumed.Add

    completion.HasPendingInjects |> should equal false
    userTextsOf history |> should equal [ "first"; "second" ]
    journaled |> List.ofSeq |> should equal [ entry ]
    consumed |> List.ofSeq |> should equal [ entry ]

// ──────────────────────────────────────────────────────────────────────────
// Suspend and resume (issue 36)

/// Scripted permission policy: answers per tool name, counting evaluations.
type ScriptPolicy(verdicts: Map<string, PermissionVerdict>) =
    let mutable evaluations = 0
    let seen = ResizeArray<string>()

    interface IPermissionPolicy with
        member _.Evaluate(request) =
            evaluations <- evaluations + 1
            seen.Add(request.ToolName)

            match verdicts.TryFind(request.ToolName) with
            | Some verdict -> verdict
            | None -> PermissionVerdict.Allow

    member _.Evaluations = evaluations
    member _.Seen = seen :> IReadOnlyList<string>

let private suspendIds () =
    let mutable next = 0

    fun () ->
        next <- next + 1
        $"req-%d{next}"

let private runSuspendable
    (client: ScriptedChatClient)
    (history: IList<ChatMessage>)
    (tools: IReadOnlyDictionary<string, AITool>)
    (policy: IPermissionPolicy)
    (newRequestId: unit -> string)
    (allowed: HashSet<string>)
    : TurnLoop.TurnLoopCompletion =
    TurnLoop.runSuspendableAsync
        (client :> IChatClient)
        history
        tools
        TurnLoop.TurnLoopOptions.Default
        (NeverDelay() :> ILlmDelay)
        CancellationToken.None
        alwaysLeased
        (fun () -> ResizeArray<InboxEntry>() :> IReadOnlyList<InboxEntry>)
        ignore
        ignore
        policy
        (SessionId.New())
        (TurnId.New())
        (Some newRequestId)
        allowed
    |> fun task -> task.GetAwaiter().GetResult()

/// One scripted ask_user question step: the question argument flows into
/// the suspension's question text.
let private questionStep (callId: string) (question: string) : ScriptStep =
    let args = Dictionary<string, obj>()
    args["question"] <- question :> obj

    ScriptStep.ToolCall(callId, TurnLoop.AskUserToolName, args)

[<Fact>]
let ``Ask suspends with the unified carrier and invokes nothing`` () =
    let invocations = ref []
    let fn = stubTool "exec" "out" invocations

    let first = callStep "c1" "exec"

    let client = scripted [ first; textStep "never" ]
    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

    let policy =
        ScriptPolicy(Map.ofList [ "exec", PermissionVerdict.Ask ]) :> IPermissionPolicy

    let completion =
        runSuspendable client history (makeTools [ "exec", fn ]) policy (suspendIds ()) (HashSet<string>())

    completion.Result.Status |> should equal TurnStatus.Suspended
    completion.HasPendingInjects |> should equal false
    completion.Suspension.IsSome |> should equal true

    let suspension = completion.Suspension.Value
    suspension.RequestId |> should equal "req-1"
    suspension.ToolName |> should equal "exec"
    suspension.ToolCallId |> should equal "c1"
    suspension.Kind |> should equal TurnLoop.PermissionSuspension
    suspension.PendingCall.CallId |> should equal "c1"
    invocations.Value.Length |> should equal 0
    client.Calls |> should equal 1

[<Fact>]
let ``Deny continues without the tool effect`` () =
    let invocations = ref []
    let fn = stubTool "exec" "out" invocations

    let first = callStep "c1" "exec"

    let client = scripted [ first; textStep "done" ]
    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

    let policy =
        ScriptPolicy(
            Map.ofList
                [
                    "exec", PermissionVerdict.Deny("no writes")
                ]
        )
        :> IPermissionPolicy

    let completion =
        runSuspendable client history (makeTools [ "exec", fn ]) policy (suspendIds ()) (HashSet<string>())

    completion.Result.Status |> should equal TurnStatus.Completed
    completion.Suspension.IsNone |> should equal true
    invocations.Value.Length |> should equal 0
    completion.Result.AssistantText |> should equal "done"

    let denied =
        toolMessages history
        |> List.tryHead
        |> Option.map toolResultText
        |> Option.defaultValue ""

    denied.Contains("no writes") |> should equal true

[<Fact>]
let ``AllowForSession memory skips Evaluate`` () =
    let invocations = ref []
    let fn = stubTool "exec" "out" invocations

    let first = callStep "c1" "exec"

    let client = scripted [ first; textStep "done" ]
    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>
    let scripted = ScriptPolicy(Map.ofList [ "exec", PermissionVerdict.Ask ])
    let allowed = HashSet<string>()
    allowed.Add("exec") |> ignore

    let completion =
        runSuspendable client history (makeTools [ "exec", fn ]) (scripted :> IPermissionPolicy) (suspendIds ()) allowed

    completion.Result.Status |> should equal TurnStatus.Completed
    scripted.Evaluations |> should equal 0
    invocations.Value |> should equal [ "exec" ]

[<Fact>]
let ``ask_user suspends as a question through the same carrier`` () =
    let client =
        scripted
            [
                questionStep "q1" "Which region?"
                textStep "never"
            ]

    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>
    let policy = ScriptPolicy(Map.empty) :> IPermissionPolicy

    let completion =
        runSuspendable client history (makeTools []) policy (suspendIds ()) (HashSet<string>())

    completion.Result.Status |> should equal TurnStatus.Suspended
    completion.Suspension.IsSome |> should equal true

    let suspension = completion.Suspension.Value
    suspension.Kind |> should equal TurnLoop.QuestionSuspension
    suspension.ToolName |> should equal TurnLoop.AskUserToolName
    suspension.QuestionText |> should equal "Which region?"
    suspension.ToolCallId |> should equal "q1"

[<Fact>]
let ``Resume with AllowOnce executes the same parked call`` () =
    let invocations = ref []
    let fn = stubTool "exec" "out" invocations

    let first = callStep "c1" "exec"

    let client = scripted [ first; textStep "finished" ]
    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

    let policy =
        ScriptPolicy(Map.ofList [ "exec", PermissionVerdict.Ask ]) :> IPermissionPolicy

    let allowed = HashSet<string>()

    let suspended =
        runSuspendable client history (makeTools [ "exec", fn ]) policy (suspendIds ()) allowed

    suspended.Suspension.IsSome |> should equal true
    invocations.Value.Length |> should equal 0

    let resumed =
        TurnLoop.resumePermissionAsync
            suspended.Suspension.Value
            PermissionDecisionKind.AllowOnce
            (client :> IChatClient)
            history
            (makeTools [ "exec", fn ])
            TurnLoop.TurnLoopOptions.Default
            (NeverDelay() :> ILlmDelay)
            CancellationToken.None
            alwaysLeased
            policy
            allowed
        |> fun task -> task.GetAwaiter().GetResult()

    resumed.Result.Status |> should equal TurnStatus.Completed
    resumed.Result.AssistantText |> should equal "finished"
    invocations.Value |> should equal [ "exec" ]
    client.Calls |> should equal 2

[<Fact>]
let ``Resume with Deny skips the parked call`` () =
    let invocations = ref []
    let fn = stubTool "exec" "out" invocations

    let first = callStep "c1" "exec"

    let client = scripted [ first; textStep "finished" ]
    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

    let policy =
        ScriptPolicy(Map.ofList [ "exec", PermissionVerdict.Ask ]) :> IPermissionPolicy

    let allowed = HashSet<string>()

    let suspended =
        runSuspendable client history (makeTools [ "exec", fn ]) policy (suspendIds ()) allowed

    suspended.Suspension.IsSome |> should equal true

    let resumed =
        TurnLoop.resumePermissionAsync
            suspended.Suspension.Value
            PermissionDecisionKind.Deny
            (client :> IChatClient)
            history
            (makeTools [ "exec", fn ])
            TurnLoop.TurnLoopOptions.Default
            (NeverDelay() :> ILlmDelay)
            CancellationToken.None
            alwaysLeased
            policy
            allowed
        |> fun task -> task.GetAwaiter().GetResult()

    resumed.Result.Status |> should equal TurnStatus.Completed
    invocations.Value.Length |> should equal 0
