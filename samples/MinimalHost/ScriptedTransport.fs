// SPDX-License-Identifier: Apache-2.0
module MinimalHost.ScriptedTransport

open System
open System.Threading.Tasks
open Legate
open Microsoft.Extensions.AI

// Scripted transports for the offline default: an infinite echo chat
// client plus the stub provider satisfying startup validation. A fixed
// script (Legate.Testing.ScriptedChatClient) cannot serve a long-lived
// host: the queue exhausts and later turns fail. The echo client answers
// every provider call with canned text and declines streaming, so the loop
// falls back to GetResponseAsync, mirroring the Testing fallback
// semantics. LegateCli (#96) sets the same precedent with its own private
// scripted client. A test-only reply-delay knob
// (LEGATE_MINIMALHOST_REPLY_DELAY_MS, milliseconds) holds the response so
// the cluster smoke can stop a node mid-turn deterministically; absent or
// invalid means no delay and production behavior is unchanged.

/// Reads the test-only scripted reply delay in milliseconds: 0 when the
/// environment carries no positive integer.
let private replyDelayMs () : int =
    match Environment.GetEnvironmentVariable("LEGATE_MINIMALHOST_REPLY_DELAY_MS") with
    | null -> 0
    | raw ->
        let mutable value = 0

        if Int32.TryParse(raw.Trim(), &value) && value > 0 then
            value
        else
            0

/// Answers every provider call with canned assistant text, forever.
type EchoChatClient() =

    interface IChatClient with
        member _.GetResponseAsync(_, _, cancellationToken) =
            task {
                cancellationToken.ThrowIfCancellationRequested()

                let delay = replyDelayMs ()

                if delay > 0 then
                    do! Task.Delay(delay, cancellationToken)

                return ChatResponse(ChatMessage(ChatRole.Assistant, "minimalhost scripted reply"))
            }

        member _.GetStreamingResponseAsync(_, _, _) =
            raise (
                NotSupportedException(
                    "The scripted client is non-streaming: the caller falls back to GetResponseAsync."
                )
            )

        member _.GetService(_, _) = null
        member _.Dispose() = ()

/// Stub provider backing scripted mode: satisfies startup validation with
/// no key and serves the echo client for any model.
type StubScriptedProvider(client: IChatClient) =
    do ArgumentNullException.ThrowIfNull(client)

    interface ILlmProvider with
        member _.Id = "scripted"
        member _.DefaultModel = "scripted"

        member _.Capabilities =
            {
                Streaming = false
                Reasoning = false
                ToolCalling = true
            }

        member _.CreateChatClient(_model: ModelReference, _options: LlmProviderOptions | null) = client
