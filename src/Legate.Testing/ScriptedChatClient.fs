// SPDX-License-Identifier: Apache-2.0
#nowarn "3261"

namespace Legate.Testing

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.AI

// Scripted IChatClient: answers provider calls from a queue of script steps
// (text and tool-call responses, streaming-chunk sequences, usage payloads,
// failure injection) and records every received message. No Akka, no
// network. When the script runs out, the next provider call fails with the
// received messages, so an unexpected call fails the test naming what the
// loop actually sent.
//
// MEAI interop surfaces nulls (queued updates, content payloads); the client
// treats a null script text as empty, but null steps, null chunks, and null
// failures are programmer errors and rejected at construction.

/// One tool call in a scripted tool-call response: the call id the result
/// answers plus the tool name the model called. Arguments default to empty;
/// pass a dictionary when the invoked tool reads them.
/// <param name="callId">The tool-call id. Must not be null or whitespace.</param>
/// <param name="toolName">The tool the model called. Must not be null or whitespace.</param>
/// <param name="arguments">The call arguments, or null for none.</param>
[<Sealed>]
type ScriptToolCall(callId: string, toolName: string, arguments: IDictionary<string, obj> | null) =

    do
        if String.IsNullOrWhiteSpace callId then
            raise (ArgumentException("A scripted tool call needs a non-empty call id.", nameof callId))

        if String.IsNullOrWhiteSpace toolName then
            raise (ArgumentException("A scripted tool call needs a non-empty tool name.", nameof toolName))

    /// Creates a scripted tool call with empty arguments.
    /// <param name="callId">The tool-call id. Must not be null or whitespace.</param>
    /// <param name="toolName">The tool the model called. Must not be null or whitespace.</param>
    new(callId: string, toolName: string) = ScriptToolCall(callId, toolName, null)

    /// The tool-call id the result answers.
    member _.CallId = callId

    /// The tool the model called.
    member _.ToolName = toolName

    /// The call arguments, or null when the script passed none.
    member _.Arguments: IDictionary<string, obj> | null = arguments

/// One scripted provider answer. Built through the static factories
/// (Text, ToolCall, ToolCalls, Stream, Failure); concrete shapes stay
/// internal so scripts read declaratively without naming them.
[<AbstractClass>]
type ScriptStep internal () =

    /// Scripts an assistant text response, optionally carrying a usage
    /// payload that flows into the turn totals.
    /// <param name="text">The assistant text. Null reads as empty.</param>
    /// <returns>The script step.</returns>
    static member Text(text: string) : ScriptStep = TextStep(text, 0L, 0L) :> ScriptStep

    /// Scripts an assistant text response carrying a usage payload.
    /// <param name="text">The assistant text. Null reads as empty.</param>
    /// <param name="inputTokens">The input tokens the response reports. Must not be negative.</param>
    /// <param name="outputTokens">The output tokens the response reports. Must not be negative.</param>
    /// <returns>The script step.</returns>
    static member Text(text: string, inputTokens: int64, outputTokens: int64) : ScriptStep =
        TextStep(text, inputTokens, outputTokens) :> ScriptStep

    /// Scripts a single tool-call response with empty arguments.
    /// <param name="callId">The tool-call id. Must not be null or whitespace.</param>
    /// <param name="toolName">The tool the model called. Must not be null or whitespace.</param>
    /// <returns>The script step.</returns>
    static member ToolCall(callId: string, toolName: string) : ScriptStep =
        ToolCallStep(
            (ResizeArray<ScriptToolCall>([| ScriptToolCall(callId, toolName) |]) :> IReadOnlyList<ScriptToolCall>),
            0L,
            0L
        )
        :> ScriptStep

    /// Scripts a single tool-call response with arguments.
    /// <param name="callId">The tool-call id. Must not be null or whitespace.</param>
    /// <param name="toolName">The tool the model called. Must not be null or whitespace.</param>
    /// <param name="arguments">The call arguments, or null for none.</param>
    /// <returns>The script step.</returns>
    static member ToolCall(callId: string, toolName: string, arguments: IDictionary<string, obj> | null) : ScriptStep =
        ToolCallStep(
            (ResizeArray<ScriptToolCall>(
                [|
                    ScriptToolCall(callId, toolName, arguments)
                |]
            )
            :> IReadOnlyList<ScriptToolCall>),
            0L,
            0L
        )
        :> ScriptStep

    /// Scripts a tool-call response carrying several calls in order.
    /// <param name="calls">The calls in order. Must not be null or empty and must hold no null entries.</param>
    /// <returns>The script step.</returns>
    static member ToolCalls(calls: IReadOnlyList<ScriptToolCall>) : ScriptStep =
        ToolCallStep(calls, 0L, 0L) :> ScriptStep

    /// Scripts a tool-call response carrying several calls plus a usage payload.
    /// <param name="calls">The calls in order. Must not be null or empty and must hold no null entries.</param>
    /// <param name="inputTokens">The input tokens the response reports. Must not be negative.</param>
    /// <param name="outputTokens">The output tokens the response reports. Must not be negative.</param>
    /// <returns>The script step.</returns>
    static member ToolCalls
        (calls: IReadOnlyList<ScriptToolCall>, inputTokens: int64, outputTokens: int64)
        : ScriptStep =
        ToolCallStep(calls, inputTokens, outputTokens) :> ScriptStep

    /// Scripts one streaming provider call: each chunk becomes one
    /// ChatResponseUpdate in order. TextContent chunks read as text deltas,
    /// TextReasoningContent as reasoning deltas, FunctionCallContent as tool
    /// calls, and UsageContent feeds the usage totals without landing in
    /// the message contents (the LlmStreaming accumulation contract).
    /// <param name="chunks">The chunks in order. Must not be null or empty and must hold no null entries.</param>
    /// <returns>The script step.</returns>
    static member Stream(chunks: IReadOnlyList<AIContent>) : ScriptStep = StreamStep(chunks, null) :> ScriptStep

    /// Scripts one streaming provider call with a raw representation
    /// stamped on the first update (provider raw blocks survive verbatim).
    /// <param name="chunks">The chunks in order. Must not be null or empty and must hold no null entries.</param>
    /// <param name="rawRepresentation">The raw representation for the first update, or null for none.</param>
    /// <returns>The script step.</returns>
    static member Stream(chunks: IReadOnlyList<AIContent>, rawRepresentation: obj | null) : ScriptStep =
        StreamStep(chunks, rawRepresentation) :> ScriptStep

    /// Scripts a provider failure: both entry points raise the error.
    /// <param name="error">The error the provider call raises. Must not be null.</param>
    /// <returns>The script step.</returns>
    static member Failure(error: Exception) : ScriptStep = FailureStep(error) :> ScriptStep

/// A scripted assistant text response with an optional usage payload.
and internal TextStep(text: string | null, inputTokens: int64, outputTokens: int64) =
    inherit ScriptStep()

    do
        if inputTokens < 0L then
            raise (ArgumentOutOfRangeException(nameof inputTokens, "Usage counts must not be negative."))

        if outputTokens < 0L then
            raise (ArgumentOutOfRangeException(nameof outputTokens, "Usage counts must not be negative."))

    /// The assistant text, or empty when the script passed null.
    member _.Text =
        match text with
        | null -> ""
        | value -> value

    /// The input tokens the response reports.
    member _.InputTokens = inputTokens

    /// The output tokens the response reports.
    member _.OutputTokens = outputTokens

/// A scripted tool-call response with an optional usage payload.
and internal ToolCallStep(calls: IReadOnlyList<ScriptToolCall>, inputTokens: int64, outputTokens: int64) =
    inherit ScriptStep()

    do
        ArgumentNullException.ThrowIfNull(calls)

        if calls.Count = 0 then
            raise (ArgumentException("A scripted tool-call response needs at least one call.", nameof calls))

        for call in calls do
            if isNull (box call) then
                raise (ArgumentException("A scripted tool-call response must hold no null calls.", nameof calls))

        if inputTokens < 0L then
            raise (ArgumentOutOfRangeException(nameof inputTokens, "Usage counts must not be negative."))

        if outputTokens < 0L then
            raise (ArgumentOutOfRangeException(nameof outputTokens, "Usage counts must not be negative."))

    /// The calls in order.
    member _.Calls = calls

    /// The input tokens the response reports.
    member _.InputTokens = inputTokens

    /// The output tokens the response reports.
    member _.OutputTokens = outputTokens

/// A scripted streaming provider call: an ordered chunk sequence.
and internal StreamStep(chunks: IReadOnlyList<AIContent>, rawRepresentation: obj | null) =
    inherit ScriptStep()

    do
        ArgumentNullException.ThrowIfNull(chunks)

        if chunks.Count = 0 then
            raise (ArgumentException("A scripted stream needs at least one chunk.", nameof chunks))

        for chunk in chunks do
            if isNull (box chunk) then
                raise (ArgumentException("A scripted stream must hold no null chunks.", nameof chunks))

    /// The chunks in order.
    member _.Chunks = chunks

    /// The raw representation for the first update, or null for none.
    member _.RawRepresentation: obj | null = rawRepresentation

/// A scripted provider failure.
and internal FailureStep(error: Exception) =
    inherit ScriptStep()

    do ArgumentNullException.ThrowIfNull(error)

    /// The error the provider call raises.
    member _.Error = error

/// Scripted IChatClient: answers provider calls from a queue of script
/// steps in order and records every received message. A scripted text or
/// tool-call step serves GetResponseAsync (the streaming entry point only
/// peeks at those steps and raises NotSupportedException without consuming
/// anything, so the loop's documented fallback serves the same step); a
/// stream step serves GetStreamingResponseAsync with one update per chunk
/// and accumulates the same chunks for a direct GetResponseAsync call. A
/// failure step raises from both entry points. When the script runs out,
/// the next served call raises InvalidOperationException naming the
/// received messages, so an unexpected call fails the test showing what the
/// loop actually sent. Thread-safe: takes serialize on a lock.
/// <param name="steps">The scripted answers in call order. Must not be null and must hold no null entries; may be empty (every call then fails as exhausted).</param>
[<Sealed>]
type ScriptedChatClient(steps: IReadOnlyList<ScriptStep>) =

    do ArgumentNullException.ThrowIfNull(steps)

    let script = Array.ofSeq steps

    do
        script
        |> Array.iteri (fun index step ->
            if isNull (box step) then
                raise (ArgumentException($"The script must hold no null steps (index {index}).", nameof steps)))

    let gate = obj ()
    let mutable takes = 0
    let mutable served = 0
    let received = ResizeArray<ChatMessage>()

    /// Renders one received message compactly: its role plus its text and
    /// tool-call names, truncated so an exhaustion failure stays readable.
    static member private RenderMessage(message: ChatMessage) : string =
        if isNull (box message) then
            "<null message>"
        else if isNull (box message.Contents) then
            $"{message.Role}: <null contents>"
        else
            let texts = ResizeArray<string>()
            let names = ResizeArray<string>()

            for content in message.Contents do
                if not (isNull (box content)) then
                    match content with
                    | :? TextContent as text when not (isNull (box text)) && not (String.IsNullOrEmpty text.Text) ->
                        texts.Add(text.Text)
                    | :? FunctionCallContent as call when not (isNull (box call)) ->
                        names.Add(if isNull (box call.Name) then "<null name>" else call.Name)
                    | _ -> ()

            let detail =
                String.concat "" texts
                + (if names.Count = 0 then
                       ""
                   else
                       " calls: " + String.concat "," names)

            let trimmed =
                if detail.Length > 200 then
                    detail.Substring(0, 200) + "..."
                else
                    detail

            $"{message.Role}: {trimmed}"

    /// Records the received history (the loop mutates histories in place,
    /// so each served call snapshots the list it saw). Call only under the
    /// gate.
    member private _.RecordLocked(history: IEnumerable<ChatMessage>) : unit =
        if not (isNull (box history)) then
            for message in history do
                received.Add(message)

    /// Renders the received messages for the exhaustion failure. Call only
    /// under the gate.
    member private _.RenderLocked() : string =
        let rendered =
            received
            |> Seq.mapi (fun index message -> $"[{index + 1}] {ScriptedChatClient.RenderMessage message}")
            |> Seq.truncate 25
            |> String.concat "; "

        let suffix =
            if received.Count > 25 then
                $" and {received.Count - 25} more"
            else
                ""

        $"Received messages ({received.Count} total): {rendered}{suffix}"

    /// Raises the exhaustion failure naming what arrived so far. Call
    /// only under the gate.
    member private this.FailExhausted<'T>() : 'T =
        raise (
            InvalidOperationException(
                $"The scripted chat client has no steps left for provider call {takes}: "
                + $"the script held {script.Length} steps. "
                + this.RenderLocked()
            )
        )

    /// Dequeues the next script step for a served call. A failed take
    /// still counts: the exhaustion failure names the take that ran out.
    /// Call only under the gate.
    member private this.TakeLocked() : ScriptStep =
        takes <- takes + 1

        if served < script.Length then
            let step = script[served]
            served <- served + 1
            step
        else
            this.FailExhausted()

    /// Builds the usage payload every scripted response carries.
    static member private UsageOf(inputTokens: int64, outputTokens: int64) : UsageDetails =
        let usage = UsageDetails()
        usage.InputTokenCount <- Nullable inputTokens
        usage.OutputTokenCount <- Nullable outputTokens
        usage

    /// Builds the non-streaming response for a scripted step.
    static member private ToResponse(step: ScriptStep) : ChatResponse =
        match step with
        | :? TextStep as text when not (isNull (box text)) ->
            let response =
                ChatResponse(
                    ResizeArray<ChatMessage>(
                        [|
                            ChatMessage(ChatRole.Assistant, text.Text)
                        |]
                    )
                )

            response.Usage <- ScriptedChatClient.UsageOf(text.InputTokens, text.OutputTokens)
            response
        | :? ToolCallStep as toolCalls when not (isNull (box toolCalls)) ->
            let contents =
                toolCalls.Calls
                |> Seq.map (fun call ->
                    let args =
                        match call.Arguments with
                        | null -> Dictionary<string, obj>() :> IDictionary<string, obj>
                        | provided -> provided

                    FunctionCallContent(call.CallId, call.ToolName, args) :> AIContent)
                |> Array.ofSeq

            let message =
                ChatMessage(ChatRole.Assistant, ResizeArray<AIContent>(contents) :> IList<AIContent>)

            let response = ChatResponse(ResizeArray<ChatMessage>([| message |]))
            response.Usage <- ScriptedChatClient.UsageOf(toolCalls.InputTokens, toolCalls.OutputTokens)
            response
        | :? StreamStep as stream when not (isNull (box stream)) ->
            let mutable inputTokens = 0L
            let mutable outputTokens = 0L
            let contents = ResizeArray<AIContent>()

            for chunk in stream.Chunks do
                match chunk with
                | :? UsageContent as usage when not (isNull (box usage)) && not (isNull usage.Details) ->
                    if usage.Details.InputTokenCount.HasValue then
                        inputTokens <- inputTokens + usage.Details.InputTokenCount.Value

                    if usage.Details.OutputTokenCount.HasValue then
                        outputTokens <- outputTokens + usage.Details.OutputTokenCount.Value
                | _ -> contents.Add(chunk)

            let message = ChatMessage(ChatRole.Assistant, contents :> IList<AIContent>)

            if not (isNull stream.RawRepresentation) then
                message.RawRepresentation <- stream.RawRepresentation

            let response = ChatResponse(ResizeArray<ChatMessage>([| message |]))
            response.Usage <- ScriptedChatClient.UsageOf(inputTokens, outputTokens)
            response
        | _ -> raise (InvalidOperationException("The scripted chat client met an unknown script step."))

    /// Builds the streaming updates for a scripted step: one update per
    /// chunk for stream steps; the documented fallback signal
    /// (NotSupportedException) for non-streaming steps, which the loop
    /// answers with a single GetResponseAsync call.
    static member private ToUpdates(step: ScriptStep) : ChatResponseUpdate list =
        match step with
        | :? StreamStep as stream when not (isNull (box stream)) ->
            stream.Chunks
            |> Seq.mapi (fun index chunk ->
                let update =
                    ChatResponseUpdate(
                        Nullable ChatRole.Assistant,
                        ResizeArray<AIContent>([| chunk |]) :> IList<AIContent>
                    )

                if index = 0 && not (isNull stream.RawRepresentation) then
                    update.RawRepresentation <- stream.RawRepresentation

                update)
            |> List.ofSeq
        | :? TextStep
        | :? ToolCallStep ->
            raise (
                NotSupportedException("The scripted step is non-streaming: the caller falls back to GetResponseAsync.")
            )
        | _ -> raise (InvalidOperationException("The scripted chat client met an unknown script step."))

    /// How many takes ran: served steps plus exhausted attempts. The
    /// loop's streaming probe for a non-streaming step consumes nothing
    /// and counts nothing, so one loop iteration counts once.
    member _.Calls = lock gate (fun () -> takes)

    /// Every message the provider calls received, in arrival order across
    /// calls.
    member _.ReceivedMessages: IReadOnlyList<ChatMessage> =
        lock gate (fun () -> ResizeArray<ChatMessage>(received) :> IReadOnlyList<ChatMessage>)

    interface IChatClient with
        member this.GetResponseAsync(history, _, cancellationToken) =
            try
                cancellationToken.ThrowIfCancellationRequested()

                lock gate (fun () ->
                    this.RecordLocked history

                    match this.TakeLocked() with
                    | :? FailureStep as failure when not (isNull (box failure)) ->
                        Task.FromException<ChatResponse>(failure.Error)
                    | step -> Task.FromResult(ScriptedChatClient.ToResponse step))
            with ex ->
                Task.FromException<ChatResponse>(ex)

        member this.GetStreamingResponseAsync(history, _, cancellationToken) =
            lock gate (fun () ->
                // Peek first: a non-streaming step raises the documented
                // fallback signal without consuming the script, so the
                // loop's follow-up GetResponseAsync serves the same step.
                // Only served calls record their history.
                let peek = if served < script.Length then Some script[served] else None

                match peek with
                | Some(:? FailureStep as failure) when not (isNull (box failure)) ->
                    this.RecordLocked history
                    this.TakeLocked() |> ignore
                    raise failure.Error
                | Some(:? StreamStep) ->
                    this.RecordLocked history
                    let step = this.TakeLocked()
                    let updates = ScriptedChatClient.ToUpdates step

                    { new IAsyncEnumerable<ChatResponseUpdate> with
                        member _.GetAsyncEnumerator(_) =
                            let mutable rest = updates
                            let mutable current = Unchecked.defaultof<ChatResponseUpdate>

                            { new IAsyncEnumerator<ChatResponseUpdate> with
                                member _.Current = current

                                member _.MoveNextAsync() =
                                    ValueTask<bool>(
                                        task {
                                            cancellationToken.ThrowIfCancellationRequested()

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
                    }
                | Some _ ->
                    raise (
                        NotSupportedException(
                            "The scripted step is non-streaming: the caller falls back to GetResponseAsync."
                        )
                    )
                | None ->
                    this.RecordLocked history
                    this.FailExhausted())

        member _.GetService(_, _) = null
        member _.Dispose() = ()
