// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Generic
open System.Net.Http
open System.Net.Http.Headers
open System.Security.Cryptography
open System.Text
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.AI

// Signed invocation for agent custom tools (issue 74). Each call serializes
// the model arguments to JSON, checks the endpoint through the call-time
// SSRF guard from #75, signs the exact bytes sent with HMAC-SHA256 under
// X-Legate-Signature, and POSTs them with a 30 s bound linked to the turn
// token. Every outcome is a {content, isError} JSON envelope string: the
// function never throws and never logs secrets, headers, or arguments.
// Guard-surface drift stays isolated to CustomToolTransport.postAsync, the
// single function that touches SsrfGuard.

// The {content, isError} response contract every custom tool call returns
// as a JSON string: success carries the endpoint's content, every failure
// carries a bounded error string with isError true.
module internal CustomToolEnvelope =

    /// Serializes one envelope: content first, then the error flag.
    /// <param name="content">The response text or bounded error, or null for empty.</param>
    /// <param name="isError">True when the call failed.</param>
    /// <returns>The envelope JSON string.</returns>
    let private write (content: string | null) (isError: bool) : string =
        let safe =
            match Option.ofObj content with
            | Some text -> text
            | None -> ""

        JsonSerializer.Serialize({| content = safe; isError = isError |})

    /// Builds the success envelope from the endpoint's content.
    /// <param name="content">The endpoint's content text.</param>
    /// <param name="isError">The endpoint's own error flag.</param>
    /// <returns>The envelope JSON string.</returns>
    let success (content: string) (isError: bool) : string = write content isError

    /// Builds the failure envelope from a bounded error string.
    /// <param name="message">The bounded error, free of secrets, headers, and arguments.</param>
    /// <returns>The envelope JSON string.</returns>
    let failure (message: string) : string = write message true

// The signed POST transport: the only code that touches the SSRF guard.
// Per-call pinning needs a per-call handler (the guard's ConnectCallback
// captures the pinned endpoint), so each call builds its client over
// PinnedEndpoint.CreateDefaultHandler and disposes it afterwards: model
// call rates make pooling irrelevant, and sharing one handler across
// distinct pinned endpoints would defeat per-call pinning.
module internal CustomToolTransport =

    /// The signature header carrying the HMAC hex: sha256=&lt;hex&gt;.
    [<Literal>]
    let SignatureHeader = "X-Legate-Signature"

    /// The cap on endpoint response bodies: larger bodies become a bounded
    /// error instead of unbounded memory.
    [<Literal>]
    let MaxResponseBytes = 1000000

    /// The cap on mapped exception text: longer messages truncate.
    [<Literal>]
    let MaxErrorChars = 500

    /// Renders a timeout bound for error text: whole seconds at or above
    /// one second, milliseconds below.
    /// <param name="timeout">The bound that fired.</param>
    /// <returns>The bound's duration text.</returns>
    let private formatTimeout (timeout: TimeSpan) : string =
        if timeout.TotalSeconds >= 1.0 then
            sprintf "%d seconds" (int timeout.TotalSeconds)
        else
            sprintf "%d ms" (int timeout.TotalMilliseconds)

    /// Bounds exception text: null reads as empty, longer messages
    /// truncate. Callers never format secrets, headers, or arguments into
    /// the message, so truncation is the only treatment needed.
    /// <param name="text">The message to bound, or null.</param>
    /// <returns>The bounded text.</returns>
    let private truncate (text: string | null) : string =
        match Option.ofObj text with
        | None -> ""
        | Some value when value.Length > MaxErrorChars -> value.Substring(0, MaxErrorChars)
        | Some value -> value

    /// Copies the tool's configured headers onto the request: request
    /// headers first, content headers when the name belongs there. Names
    /// and values are never echoed: an unusable header maps to a fixed
    /// error string.
    /// <param name="request">The request to carry the headers.</param>
    /// <param name="content">The request body carrying content headers.</param>
    /// <param name="headers">The tool's configured headers, or null.</param>
    /// <returns>The bounded error envelope, or None when every header applied.</returns>
    let private applyHeaders
        (request: HttpRequestMessage)
        (content: ByteArrayContent)
        (headers: IReadOnlyDictionary<string, string> | null)
        : string option =
        if isNull (box headers) then
            None
        else
            let mutable failure = None

            for pair in headers do
                if Option.isNone failure then
                    if isNull (box pair.Key) || String.IsNullOrWhiteSpace pair.Key then
                        failure <-
                            Some(CustomToolEnvelope.failure "Error: the custom tool defines an invalid HTTP header.")
                    else
                        let value = if isNull (box pair.Value) then "" else pair.Value

                        if not (request.Headers.TryAddWithoutValidation(pair.Key, value)) then
                            if not (content.Headers.TryAddWithoutValidation(pair.Key, value)) then
                                failure <-
                                    Some(
                                        CustomToolEnvelope.failure
                                            "Error: the custom tool defines an invalid HTTP header."
                                    )

            failure

    /// Maps the endpoint's response body to the envelope: the body must be
    /// a JSON object carrying content, whose absence or shape failure is a
    /// bounded error. Non-string content serves as its raw JSON text; the
    /// error flag is true only for an explicit boolean true.
    /// <param name="body">The endpoint's response bytes.</param>
    /// <returns>The envelope JSON string.</returns>
    let private mapBody (body: byte[]) : string =
        try
            use document = JsonDocument.Parse body

            if document.RootElement.ValueKind <> JsonValueKind.Object then
                CustomToolEnvelope.failure "Error: the custom tool endpoint returned malformed JSON."
            else
                let mutable content = Unchecked.defaultof<JsonElement>

                if not (document.RootElement.TryGetProperty("content", &content)) then
                    CustomToolEnvelope.failure "Error: the custom tool endpoint returned malformed JSON."
                else
                    let text: string =
                        if content.ValueKind = JsonValueKind.String then
                            let raw: string | null = content.GetString()

                            match Option.ofObj raw with
                            | Some value -> value
                            | None -> ""
                        else
                            content.GetRawText()

                    let mutable flag = Unchecked.defaultof<JsonElement>

                    let isError =
                        document.RootElement.TryGetProperty("isError", &flag)
                        && flag.ValueKind = JsonValueKind.True

                    CustomToolEnvelope.success text isError
        with :? JsonException ->
            CustomToolEnvelope.failure "Error: the custom tool endpoint returned malformed JSON."

    /// Sends one signed POST through the pinned endpoint: builds the
    /// request over the endpoint's original URI (preserving Host and SNI),
    /// applies the tool headers, signs the exact body bytes, and maps the
    /// response. Assumes validated arguments; unexpected failures escape to
    /// postAsync's bounded mapping.
    /// <param name="pinned">The guard-approved pinned endpoint.</param>
    /// <param name="headers">The tool's configured headers, or null.</param>
    /// <param name="body">The exact request bytes, signed and sent.</param>
    /// <param name="secret">The tool's signing secret bytes.</param>
    /// <param name="cancellationToken">The linked call token (turn plus timeout bound).</param>
    /// <returns>The envelope JSON string.</returns>
    let private sendAsync
        (pinned: PinnedEndpoint)
        (headers: IReadOnlyDictionary<string, string> | null)
        (body: byte[])
        (secret: byte[])
        (cancellationToken: CancellationToken)
        : Task<string> =
        task {
            use handler = pinned.CreateDefaultHandler()
            use client = new HttpClient(handler, true)
            client.Timeout <- Timeout.InfiniteTimeSpan

            use request = new HttpRequestMessage(HttpMethod.Post, pinned.OriginalUri)
            use content = new ByteArrayContent(body)
            content.Headers.ContentType <- MediaTypeHeaderValue("application/json")
            request.Content <- content

            match applyHeaders request content headers with
            | Some failure -> return failure
            | None ->
                use hmac = new HMACSHA256(secret)

                let signature =
                    "sha256:" + Convert.ToHexString(hmac.ComputeHash(body)).ToLowerInvariant()

                request.Headers.Remove(SignatureHeader) |> ignore

                if not (request.Headers.TryAddWithoutValidation(SignatureHeader, signature)) then
                    return CustomToolEnvelope.failure "Error: the custom tool could not sign its request."
                else
                    use! response =
                        client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken)

                    if not response.IsSuccessStatusCode then
                        return
                            CustomToolEnvelope.failure (
                                sprintf
                                    "Error: the custom tool endpoint returned HTTP status %d."
                                    (int response.StatusCode)
                            )
                    else
                        let! bytes = response.Content.ReadAsByteArrayAsync(cancellationToken)

                        if bytes.Length > MaxResponseBytes then
                            return
                                CustomToolEnvelope.failure
                                    "Error: the custom tool response exceeded the 1 megabyte limit."
                        else
                            return mapBody bytes
        }

    /// Serializes the call arguments to the exact bytes the transport
    /// signs and sends: one shot over a plain dictionary so JsonElement
    /// values from the model and CLR values serialize alike. Null reads
    /// as the empty object.
    /// <param name="args">The call arguments, or null.</param>
    /// <returns>The UTF-8 JSON object bytes.</returns>
    let serializeArguments (args: AIFunctionArguments | null) : byte[] =
        if isNull (box args) then
            Encoding.UTF8.GetBytes("{}")
        else
            let table = Dictionary<string, obj>()

            for pair in args do
                let value: obj =
                    match box pair.Value with
                    | null -> Unchecked.defaultof<obj>
                    | live -> live

                table[pair.Key] <- value

            JsonSerializer.SerializeToUtf8Bytes(table)

    /// The single SSRF-guard mapping: checks the endpoint at call time and
    /// sends the signed POST only on approval. Denials return the guard's
    /// bounded string with nothing sent; cancellation (turn abort) and the
    /// timeout bound both map to bounded strings, never throws; anything
    /// else maps to a truncated failure. Null or empty secrets and null
    /// arguments are host misconfiguration and map to a fixed error.
    /// <param name="resolver">The injected address resolver seam.</param>
    /// <param name="options">The host-level SSRF allow/deny lists.</param>
    /// <param name="endpoint">The absolute endpoint URI to call.</param>
    /// <param name="headers">The tool's configured headers, or null.</param>
    /// <param name="body">The exact request bytes, signed and sent.</param>
    /// <param name="secret">The tool's signing secret bytes.</param>
    /// <param name="timeout">The per-call bound, linked with the turn token.</param>
    /// <param name="cancellationToken">The turn token.</param>
    /// <returns>The envelope JSON string, never a throw.</returns>
    let postAsync
        (resolver: IHostAddressResolver)
        (options: SsrfGuardOptions)
        (endpoint: Uri)
        (headers: IReadOnlyDictionary<string, string> | null)
        (body: byte[])
        (secret: byte[])
        (timeout: TimeSpan)
        (cancellationToken: CancellationToken)
        : Task<string> =
        task {
            let timedOut =
                CustomToolEnvelope.failure (
                    sprintf "Error: the custom tool call timed out after %s." (formatTimeout timeout)
                )

            try
                try
                    if
                        isNull (box resolver)
                        || isNull (box options)
                        || isNull (box endpoint)
                        || isNull (box body)
                        || isNull (box secret)
                        || secret.Length = 0
                    then
                        return
                            CustomToolEnvelope.failure "Error: the custom tool is misconfigured and cannot be called."
                    else
                        use linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken)
                        linked.CancelAfter(timeout)

                        let! guard = SsrfGuard.checkAsync resolver options endpoint linked.Token

                        match guard with
                        | Error reason -> return CustomToolEnvelope.failure (reason.ToBoundedString())
                        | Ok pinned -> return! sendAsync pinned headers body secret linked.Token
                with :? OperationCanceledException ->
                    if cancellationToken.IsCancellationRequested then
                        return CustomToolEnvelope.failure "Error: the custom tool call was cancelled."
                    else
                        return timedOut
            with failed ->
                return CustomToolEnvelope.failure ("Error: the custom tool call failed: " + truncate failed.Message)
        }

/// The custom tool function served to the model. Internal: hosts hold the
/// <see cref="T:Microsoft.Extensions.AI.AIFunction" /> the
/// <see cref="T:Legate.CustomToolSource" /> hands back, never this type.
/// Permission gating and the last-moment claim fence stay with TurnLoop's
/// per-tool hooks, exactly like every built-in: this function adds no
/// gating of its own and executes whenever invoked.
[<Sealed>]
type internal CustomToolFunction
    (
        name: string,
        description: string | null,
        schema: JsonDocument,
        endpoint: Uri,
        headers: IReadOnlyDictionary<string, string> | null,
        secret: byte[],
        resolver: IHostAddressResolver,
        options: SsrfGuardOptions,
        timeout: TimeSpan
    ) =
    inherit AIFunction()

    do
        ArgumentNullException.ThrowIfNull(name)
        ArgumentNullException.ThrowIfNull(schema)
        ArgumentNullException.ThrowIfNull(endpoint)
        ArgumentNullException.ThrowIfNull(resolver)
        ArgumentNullException.ThrowIfNull(options)

        if timeout <= TimeSpan.Zero then
            raise (ArgumentOutOfRangeException(nameof timeout, "The custom tool timeout must be positive."))

    // The schema document stays referenced here so the served RootElement
    // never borrows a collected document.
    let schema = schema

    let description =
        match Option.ofObj description with
        | Some text -> text
        | None -> sprintf "Invokes the custom tool '%s' over HTTP." name

    /// The default per-call bound: 30 seconds, linked with the turn token.
    static member DefaultTimeout: TimeSpan = TimeSpan.FromSeconds(30.0)

    /// This tool's sanitized model-facing name.
    override _.Name = name

    /// This tool's description for the model, carrying the schema-fallback
    /// diagnostic when the configured schema was invalid.
    override _.Description = description

    /// This tool's argument schema: configured when valid, permissive
    /// otherwise.
    override _.JsonSchema = schema.RootElement

    /// Invokes the tool: serializes the arguments, checks the endpoint
    /// through the call-time SSRF guard, signs the exact bytes, and POSTs
    /// them. Returns the {content, isError} envelope string on every path;
    /// never throws and never logs secrets, headers, or arguments.
    override _.InvokeCoreAsync
        (args: AIFunctionArguments, cancellationToken: CancellationToken)
        : ValueTask<obj | null> =
        ValueTask<obj | null>(
            task {
                let body =
                    try
                        CustomToolTransport.serializeArguments args
                    with _ ->
                        Unchecked.defaultof<byte[]>

                if isNull (box body) then
                    return box (CustomToolEnvelope.failure "Error: the custom tool arguments could not be serialized.")
                else
                    let! envelope =
                        CustomToolTransport.postAsync
                            resolver
                            options
                            endpoint
                            headers
                            body
                            secret
                            timeout
                            cancellationToken

                    return box envelope
            }
        )
