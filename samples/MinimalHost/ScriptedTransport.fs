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
// scripted client.

/// Answers every provider call with canned assistant text, forever.
type EchoChatClient() =

    interface IChatClient with
        member _.GetResponseAsync(_, _, cancellationToken) =
            task {
                cancellationToken.ThrowIfCancellationRequested()

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
