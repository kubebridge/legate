// SPDX-License-Identifier: Apache-2.0
namespace Legate.Llm.OpenAI

open System
open System.Collections.Generic
open System.Globalization
open System.Net
open System.Net.Http
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.AI

// Non-2xx to ProviderException mapping plus the IChatClient wrapper the
// providers return. HTTP failures surface before the OpenAI SDK parses them:
// the transport handler throws ProviderException carrying the status plus
// the parsed Retry-After (seconds or HTTP date; null when absent or
// unparseable), with messages naming only the provider and status. Response
// bodies, keys, and tool arguments never enter messages. SDK errors that
// escape the transport (ClientResultException) map the same way without a
// retry-after; aborts and deadline cancellations pass through untouched so
// the coordinator keeps owning retry, cooldown, and deadline semantics.

/// Parses Retry-After header values and maps transport failures. Internal:
/// hosts see only the resulting <see cref="T:Legate.ProviderException" />.
module internal ProviderErrors =

    /// Parses one Retry-After header value: delay seconds, or an HTTP date
    /// measured against now (clamped at zero when the date already passed).
    /// <param name="raw">The header value, or null.</param>
    /// <returns>The delay to respect, or null when absent or unparseable.</returns>
    let parseRetryAfter (raw: string | null) : Nullable<TimeSpan> =
        match raw with
        | null -> Nullable()
        | text when String.IsNullOrWhiteSpace text -> Nullable()
        | text ->
            let trimmed = text.Trim()
            let mutable seconds = 0L

            if Int64.TryParse(trimmed, NumberStyles.Integer, CultureInfo.InvariantCulture, &seconds) then
                if seconds < 0L then
                    Nullable()
                else
                    try
                        Nullable(TimeSpan.FromSeconds(float seconds))
                    with
                    | :? OverflowException -> Nullable()
                    | :? ArgumentException -> Nullable()
            else
                let mutable date = Unchecked.defaultof<DateTimeOffset>

                if DateTimeOffset.TryParse(trimmed, CultureInfo.InvariantCulture, DateTimeStyles.None, &date) then
                    let wait = date - DateTimeOffset.UtcNow
                    Nullable(if wait < TimeSpan.Zero then TimeSpan.Zero else wait)
                else
                    Nullable()

    /// Builds the provider failure for an HTTP status: names the provider
    /// and status only, never bodies, keys, or tool arguments.
    /// <param name="providerId">The provider that failed.</param>
    /// <param name="status">The HTTP status the provider returned.</param>
    /// <param name="retryAfter">The delay the provider asked callers to respect, or null.</param>
    /// <returns>The exception the coordinator classifies and possibly retries.</returns>
    let providerFailure (providerId: string) (status: int) (retryAfter: Nullable<TimeSpan>) : Legate.ProviderException =
        Legate.ProviderException(
            providerId,
            Nullable status,
            retryAfter,
            sprintf "The '%s' provider request failed with HTTP %d." providerId status
        )

    /// Maps an SDK client-result failure to a provider failure without a
    /// retry-after. Anything else passes through untouched: aborts,
    /// deadline cancellations, and network errors stay the coordinator's to
    /// classify.
    /// <param name="providerId">The provider that failed.</param>
    /// <param name="failure">The failure the chat client raised.</param>
    /// <returns>The mapped failure, or the original when it is not an SDK result failure.</returns>
    let mapClientFailure (providerId: string) (failure: exn) : exn =
        match failure with
        | :? Legate.ProviderException -> failure
        | :? System.ClientModel.ClientResultException as resultFailure ->
            providerFailure providerId resultFailure.Status (Nullable()) :> exn
        | _ -> failure

/// Transport handler mapping non-2xx responses to
/// <see cref="T:Legate.ProviderException" /> before the OpenAI SDK parses
/// them. Sits inside the SDK transport: the SDK retry policy above it is
/// disabled separately (MaxRetries zero), so the coordinator owns every
/// retry. Internal; scripted-transport tests drive it with canned payloads.
type internal ProviderErrorHandler(providerId: string, innerHandler: HttpMessageHandler) =
    inherit DelegatingHandler(innerHandler)

    do
        if String.IsNullOrWhiteSpace providerId then
            raise (ArgumentException("The provider id must be a non-empty string.", "providerId"))

        ArgumentNullException.ThrowIfNull(innerHandler)

    /// Sends the request and throws the mapped provider failure on
    /// non-2xx, disposing the response first so failed calls leak no
    /// sockets. Messages name the provider and status only.
    /// <param name="request">The outgoing chat-completions request.</param>
    /// <param name="cancellationToken">Abandons the send.</param>
    /// <returns>The successful response.</returns>
    override _.SendAsync
        (request: HttpRequestMessage, cancellationToken: CancellationToken)
        : Task<HttpResponseMessage> =
        // Bound outside the task expression: base members are unreachable
        // from inside the lambda the builder generates.
        let pending = base.SendAsync(request, cancellationToken)

        task {
            let! response = pending

            if response.IsSuccessStatusCode then
                return response
            else
                let status = int response.StatusCode

                let retryAfter =
                    let mutable values = Unchecked.defaultof<IEnumerable<string>>

                    if
                        response.Headers.TryGetValues("Retry-After", &values)
                        && not (isNull (box values))
                    then
                        values |> Seq.tryHead |> Option.toObj |> ProviderErrors.parseRetryAfter
                    else
                        Nullable()

                response.Dispose()
                return raise (ProviderErrors.providerFailure providerId status retryAfter)
        }

/// Streaming enumerator mapping SDK result failures during enumeration to
/// provider failures. Failures surface lazily mid-stream, so the sync
/// GetStreamingResponseAsync return cannot map them on its own.
[<Sealed>]
type private MappedStreamEnumerator(inner: IAsyncEnumerator<ChatResponseUpdate>, providerId: string) =
    do ArgumentNullException.ThrowIfNull(inner)

    interface IAsyncEnumerator<ChatResponseUpdate> with
        member _.Current = inner.Current

        member _.MoveNextAsync() : ValueTask<bool> =
            ValueTask<bool>(
                task {
                    try
                        return! inner.MoveNextAsync()
                    with failure ->
                        return raise (ProviderErrors.mapClientFailure providerId failure)
                }
            )

        member _.DisposeAsync() : ValueTask = inner.DisposeAsync()

/// Streaming enumerable carrying the provider id into each enumerator so
/// mid-stream SDK failures map to provider failures.
[<Sealed>]
type private MappedStreamEnumerable(source: IAsyncEnumerable<ChatResponseUpdate>, providerId: string) =
    do ArgumentNullException.ThrowIfNull(source)

    interface IAsyncEnumerable<ChatResponseUpdate> with
        member _.GetAsyncEnumerator(cancellationToken: CancellationToken) : IAsyncEnumerator<ChatResponseUpdate> =
            new MappedStreamEnumerator(source.GetAsyncEnumerator(cancellationToken), providerId)
            :> IAsyncEnumerator<ChatResponseUpdate>

/// IChatClient decorator mapping SDK result failures to
/// <see cref="T:Legate.ProviderException" /> while streaming, tool-call,
/// and usage shapes pass through untouched. Every provider in this package
/// returns this over the adapter client. Internal.
[<Sealed>]
type internal ErrorMappingChatClient(inner: IChatClient, providerId: string, owned: IDisposable | null) =

    do
        ArgumentNullException.ThrowIfNull(inner)

        if String.IsNullOrWhiteSpace providerId then
            raise (ArgumentException("The provider id must be a non-empty string.", "providerId"))

    interface IChatClient with
        member _.GetResponseAsync
            (messages: IEnumerable<ChatMessage>, options: ChatOptions | null, cancellationToken: CancellationToken)
            : Task<ChatResponse> =
            task {
                try
                    return! inner.GetResponseAsync(messages, options, cancellationToken)
                with failure ->
                    return raise (ProviderErrors.mapClientFailure providerId failure)
            }

        member _.GetStreamingResponseAsync
            (messages: IEnumerable<ChatMessage>, options: ChatOptions | null, cancellationToken: CancellationToken)
            : IAsyncEnumerable<ChatResponseUpdate> =
            try
                new MappedStreamEnumerable(
                    inner.GetStreamingResponseAsync(messages, options, cancellationToken),
                    providerId
                )
                :> IAsyncEnumerable<ChatResponseUpdate>
            with failure ->
                raise (ProviderErrors.mapClientFailure providerId failure)

        member _.GetService(serviceType: Type, serviceKey: obj | null) : obj | null =
            inner.GetService(serviceType, serviceKey)

        member _.Dispose() =
            inner.Dispose()

            match owned with
            | null -> ()
            | disposable -> disposable.Dispose()
