// SPDX-License-Identifier: Apache-2.0
#nowarn "3261"

namespace Legate

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.AI

// Internal streaming LLM call-site helper. One iteration's provider call
// enumerates GetStreamingResponseAsync updates: every non-empty TextContent
// chunk fans out to the text-delta callback, every non-empty
// TextReasoningContent chunk to the reasoning-delta callback, while the
// helper accumulates the full ChatResponse for the conversation. Content
// objects move by reference into the accumulated messages, so provider raw
// blocks (Anthropic thought signatures and friends) survive verbatim for
// follow-up calls. UsageContent feeds the accumulated UsageDetails and
// never lands in the message contents, matching the non-streaming shape.
// Providers without streaming (GetStreamingResponseAsync raising
// NotSupportedException or NotImplementedException: MEAI exposes no
// SupportsStreaming flag, so the throw is the signal) fall back to one
// GetResponseAsync call with a single delta per kind.
//
// Nullness warning 3261 is suppressed in this file: MEAI interop surfaces
// nulls (null updates, contents, usage, role, and ids) that the F# nullable
// analysis cannot prove absent, and the helper treats every one as empty
// rather than failing.
module internal LlmStreaming =

    /// Adds usage counts, treating missing details and missing counters as
    /// zero. Mirrors the TurnLoop accumulator so streaming and
    /// non-streaming iterations report identically.
    let private addDetails (inputTokens: int64 byref) (outputTokens: int64 byref) (details: UsageDetails) =
        if not (isNull details) then
            if details.InputTokenCount.HasValue then
                inputTokens <- inputTokens + details.InputTokenCount.Value

            if details.OutputTokenCount.HasValue then
                outputTokens <- outputTokens + details.OutputTokenCount.Value

    /// True for text worth emitting as a delta: null and empty chunks are
    /// provider keepalives, never deltas.
    let private isDeltaText (text: string) : bool = not (String.IsNullOrEmpty text)

    /// Accumulates one ChatResponseUpdate into the open message list,
    /// fanning text and reasoning chunks out to the delta callbacks. All
    /// other content kinds (function calls included) accumulate verbatim
    /// without emitting a delta, so tool-call content never leaks into
    /// deltas. UsageContent feeds the usage totals and stays out of the
    /// message contents.
    let private accumulateUpdate
        (messages: ResizeArray<ChatMessage>)
        (current: ChatMessage option ref)
        (currentMessageId: string ref)
        (inputTokens: int64 byref)
        (outputTokens: int64 byref)
        (responseId: string ref)
        (conversationId: string ref)
        (modelId: string ref)
        (finishReason: Nullable<ChatFinishReason> ref)
        (responseRaw: obj ref)
        (onTextDelta: string -> unit)
        (onReasoningDelta: string -> unit)
        (update: ChatResponseUpdate)
        : unit =
        if isNull update then
            ()
        else
            let role =
                if update.Role.HasValue then
                    update.Role.Value
                else
                    ChatRole.Assistant

            let messageId = update.MessageId

            let startsNewMessage =
                match current.Value with
                | None -> true
                | Some openMessage ->
                    (not (isNull messageId) && messageId <> currentMessageId.Value)
                    || openMessage.Role <> role

            if startsNewMessage then
                let message = ChatMessage(role, ResizeArray<AIContent>() :> IList<AIContent>)

                message.MessageId <- messageId
                messages.Add(message)
                current.Value <- Some message
                currentMessageId.Value <- messageId

            let openMessage = current.Value.Value

            if not (isNull update.RawRepresentation) && isNull openMessage.RawRepresentation then
                openMessage.RawRepresentation <- update.RawRepresentation

            if not (isNull update.ResponseId) then
                responseId.Value <- update.ResponseId

            if not (isNull update.ConversationId) then
                conversationId.Value <- update.ConversationId

            if not (isNull update.ModelId) then
                modelId.Value <- update.ModelId

            if update.FinishReason.HasValue then
                finishReason.Value <- update.FinishReason

            if not (isNull update.RawRepresentation) then
                responseRaw.Value <- update.RawRepresentation

            if not (isNull update.Contents) then
                for content in update.Contents do
                    if not (isNull content) then
                        match content with
                        | :? UsageContent as usage when not (isNull usage) ->
                            addDetails &inputTokens &outputTokens usage.Details
                        | :? TextContent as text when not (isNull text) ->
                            if isDeltaText text.Text then
                                onTextDelta text.Text

                            openMessage.Contents.Add(content)
                        | :? TextReasoningContent as reasoning when not (isNull reasoning) ->
                            if isDeltaText reasoning.Text then
                                onReasoningDelta reasoning.Text

                            openMessage.Contents.Add(content)
                        | _ -> openMessage.Contents.Add(content)

    /// Builds the accumulated ChatResponse from the streamed messages and
    /// usage totals, stamping the last-seen response-level fields. The
    /// response Text concatenates TextContent only, so reasoning never
    /// reaches the transcript text.
    let private buildResponse
        (messages: ResizeArray<ChatMessage>)
        (inputTokens: int64)
        (outputTokens: int64)
        (responseId: string)
        (conversationId: string)
        (modelId: string)
        (finishReason: Nullable<ChatFinishReason>)
        (responseRaw: obj)
        : ChatResponse =
        let response = ChatResponse(messages :> IList<ChatMessage>)

        let usage = UsageDetails()
        usage.InputTokenCount <- Nullable inputTokens
        usage.OutputTokenCount <- Nullable outputTokens
        response.Usage <- usage

        if not (isNull responseId) then
            response.ResponseId <- responseId

        if not (isNull conversationId) then
            response.ConversationId <- conversationId

        if not (isNull modelId) then
            response.ModelId <- modelId

        if finishReason.HasValue then
            response.FinishReason <- finishReason

        if not (isNull responseRaw) then
            response.RawRepresentation <- responseRaw

        response

    /// Runs one streaming iteration: enumerates the provider's
    /// ChatResponseUpdates with the caller's cancellation token (the
    /// iteration's linked deadline token, so streaming respects the #39
    /// timeout), emits one delta per non-empty text/reasoning chunk, and
    /// returns the accumulated response for the conversation history.
    /// Cancellation during enumeration propagates to the caller's
    /// timeout/external handling; nothing is appended to the history here.
    let private streamAsync
        (client: IChatClient)
        (history: IList<ChatMessage>)
        (chatOptions: ChatOptions)
        (cancellationToken: CancellationToken)
        (onTextDelta: string -> unit)
        (onReasoningDelta: string -> unit)
        : Task<ChatResponse> =
        task {
            let messages = ResizeArray<ChatMessage>()
            let current = ref None
            let currentMessageId = ref Unchecked.defaultof<string>
            let mutable inputTokens = 0L
            let mutable outputTokens = 0L
            let responseId = ref Unchecked.defaultof<string>
            let conversationId = ref Unchecked.defaultof<string>
            let modelId = ref Unchecked.defaultof<string>
            let finishReason = ref Unchecked.defaultof<Nullable<ChatFinishReason>>
            let responseRaw = ref Unchecked.defaultof<obj>

            let updates =
                client.GetStreamingResponseAsync(history, chatOptions, cancellationToken)

            use enumerator = updates.GetAsyncEnumerator(cancellationToken)

            let mutable finished = false

            while not finished do
                let! hasNext = enumerator.MoveNextAsync()

                if hasNext then
                    accumulateUpdate
                        messages
                        current
                        currentMessageId
                        &inputTokens
                        &outputTokens
                        responseId
                        conversationId
                        modelId
                        finishReason
                        responseRaw
                        onTextDelta
                        onReasoningDelta
                        enumerator.Current
                else
                    finished <- true

            return
                buildResponse
                    messages
                    inputTokens
                    outputTokens
                    responseId.Value
                    conversationId.Value
                    modelId.Value
                    finishReason.Value
                    responseRaw.Value
        }

    /// Runs one non-streaming iteration as the fallback: a single
    /// GetResponseAsync call whose full text and reasoning each collapse
    /// to at most one delta, in content order.
    let private fallbackAsync
        (client: IChatClient)
        (history: IList<ChatMessage>)
        (chatOptions: ChatOptions)
        (cancellationToken: CancellationToken)
        (onTextDelta: string -> unit)
        (onReasoningDelta: string -> unit)
        : Task<ChatResponse> =
        task {
            let! response = client.GetResponseAsync(history, chatOptions, cancellationToken)

            if not (isNull response) && not (isNull response.Messages) then
                let textBuilder = Text.StringBuilder()
                let reasoningBuilder = Text.StringBuilder()

                for message in response.Messages do
                    if not (isNull message) && not (isNull message.Contents) then
                        for content in message.Contents do
                            if not (isNull content) then
                                match content with
                                | :? TextContent as text when not (isNull text) ->
                                    if isDeltaText text.Text then
                                        textBuilder.Append(text.Text) |> ignore
                                | :? TextReasoningContent as reasoning when not (isNull reasoning) ->
                                    if isDeltaText reasoning.Text then
                                        reasoningBuilder.Append(reasoning.Text) |> ignore
                                | _ -> ()

                if textBuilder.Length > 0 then
                    onTextDelta (textBuilder.ToString())

                if reasoningBuilder.Length > 0 then
                    onReasoningDelta (reasoningBuilder.ToString())

            return response
        }

    /// Streams one provider iteration into delta callbacks plus an
    /// accumulated response. Streaming providers enumerate
    /// GetStreamingResponseAsync; providers that raise
    /// NotSupportedException or NotImplementedException from the streaming
    /// entry point fall back to GetResponseAsync with a single delta per
    /// kind. Only those two exception types trigger the fallback: anything
    /// else (cancellation included) propagates.
    let streamResponseAsync
        (client: IChatClient)
        (history: IList<ChatMessage>)
        (chatOptions: ChatOptions)
        (cancellationToken: CancellationToken)
        (onTextDelta: string -> unit)
        (onReasoningDelta: string -> unit)
        : Task<ChatResponse> =
        ArgumentNullException.ThrowIfNull(client)
        ArgumentNullException.ThrowIfNull(history)
        ArgumentNullException.ThrowIfNull(chatOptions)
        ArgumentNullException.ThrowIfNull(onTextDelta)
        ArgumentNullException.ThrowIfNull(onReasoningDelta)

        task {
            try
                return! streamAsync client history chatOptions cancellationToken onTextDelta onReasoningDelta
            with
            | :? NotSupportedException ->
                return! fallbackAsync client history chatOptions cancellationToken onTextDelta onReasoningDelta
            | :? NotImplementedException ->
                return! fallbackAsync client history chatOptions cancellationToken onTextDelta onReasoningDelta
        }
