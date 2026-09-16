// SPDX-License-Identifier: Apache-2.0
module MinimalHost.Routes

open System
open System.Text.Json
open System.Threading.Tasks
open Giraffe
open Legate
open Microsoft.AspNetCore.Http
open Microsoft.Extensions.DependencyInjection

// Giraffe routes over the session client facade: open/prompt/reply/abort
// plus a per-session SSE event stream with Last-Event-ID resume. Error
// mapping is typed: unknown session 404, closed or mismatched-reply 409,
// expired journal 410, subscriber cap 429, bad ids and shapes 400. The
// stream ends cleanly on the session-closed event; a lagged slow consumer
// ends the stream instead of stalling the turn (bus caps 512 subscribers
// per session, 128 buffered events each).

// ──────────────────────────────────────────────────────────────────────────
// JSON shapes

/// Opens a session: the display title, or null for the runtime default.
[<CLIMutable>]
type OpenRequest = { Title: string | null }

/// Prompts a session: non-empty text plus queue, inject, or interrupt
/// delivery, defaulting to queue.
[<CLIMutable>]
type PromptRequest =
    {
        Text: string | null
        Delivery: string | null
    }

/// Aborts a session turn: explicitAbort or hostShutdown cause (default
/// explicitAbort) with a reason.
[<CLIMutable>]
type AbortRequest =
    {
        Cause: string | null
        Reason: string | null
    }

/// What open returns: the session id text plus its title and state.
[<CLIMutable>]
type SessionResponse =
    {
        SessionId: string
        Title: string
        State: string
    }

/// What prompt and reply return: the session id text, the appended inbox
/// position, and the delivery or reply kind.
[<CLIMutable>]
type InboxResponse =
    {
        SessionId: string
        Position: int64
        Delivery: string
    }

/// What abort returns: the session id text and the acknowledgement.
[<CLIMutable>]
type AbortResponse = { SessionId: string; Aborted: bool }

/// What every typed failure returns: the message plus the exception name.
[<CLIMutable>]
type ErrorResponse = { Error: string; Type: string }

// ──────────────────────────────────────────────────────────────────────────
// Shared helpers

/// Serialises SSE event payloads: camelCase, mirroring the endpoint bodies.
let private sseJson =
    JsonSerializerOptions(PropertyNamingPolicy = JsonNamingPolicy.CamelCase)

/// Renders a typed failure body.
let private failure (status: int) (error: string) (kind: string) : HttpHandler =
    setStatusCode status >=> json { Error = error; Type = kind }

/// Maps control-plane failures to typed statuses; anything unexpected is a
/// generic 500 so no stack or path leaks over HTTP.
let private mapError (ex: Exception) : HttpHandler =
    match ex with
    | :? SessionNotFoundException as notFound -> failure 404 notFound.Message "SessionNotFound"
    | :? InvalidSessionStateException as invalid -> failure 409 invalid.Message "InvalidSessionState"
    | :? ReplyMismatchException as mismatch -> failure 409 mismatch.Message "ReplyMismatch"
    | :? SessionJournalExpiredException as expired -> failure 410 expired.Message "SessionJournalExpired"
    | :? SessionSubscriptionLimitExceededException as limited -> failure 429 limited.Message "SubscriptionLimitExceeded"
    | :? ArgumentException as bad -> failure 400 bad.Message "BadRequest"
    | :? FormatException as bad -> failure 400 bad.Message "BadRequest"
    | _ -> failure 500 "An unexpected error occurred." "InternalError"

/// Parses a route session id or raises the 400-mapped ArgumentException.
let private parseSessionId (text: string) : SessionId =
    let mutable parsed = Unchecked.defaultof<SessionId>

    if String.IsNullOrWhiteSpace(text) then
        raise (ArgumentException("The route needs a session id."))
    elif SessionId.TryParse(text.Trim(), &parsed) then
        parsed
    else
        raise (ArgumentException($"'{text}' is not a session id."))

/// Parses prompt delivery: queue, inject, or interrupt, defaulting to queue.
let private parseDelivery (raw: string | null) : DeliveryMode =
    match raw with
    | null -> DeliveryMode.Queue
    | text when String.IsNullOrWhiteSpace(text) -> DeliveryMode.Queue
    | text when text.Trim().Equals("queue", StringComparison.OrdinalIgnoreCase) -> DeliveryMode.Queue
    | text when text.Trim().Equals("inject", StringComparison.OrdinalIgnoreCase) -> DeliveryMode.Inject
    | text when text.Trim().Equals("interrupt", StringComparison.OrdinalIgnoreCase) -> DeliveryMode.Interrupt
    | text -> raise (ArgumentException($"Unknown delivery '{text}': expected queue, inject, or interrupt."))

/// Parses abort cause: explicitAbort or hostShutdown, defaulting to
/// explicitAbort.
let private parseCause (raw: string | null) : StopCause =
    match raw with
    | null -> StopCause.ExplicitAbort
    | text when String.IsNullOrWhiteSpace(text) -> StopCause.ExplicitAbort
    | text when text.Trim().Equals("explicitAbort", StringComparison.OrdinalIgnoreCase) -> StopCause.ExplicitAbort
    | text when text.Trim().Equals("hostShutdown", StringComparison.OrdinalIgnoreCase) -> StopCause.HostShutdown
    | text -> raise (ArgumentException($"Unknown cause '{text}': expected explicitAbort or hostShutdown."))

// ──────────────────────────────────────────────────────────────────────────
// Endpoints

/// Opens a session: creates the Idle row and returns its id, title, and state.
let private openHandler: HttpHandler =
    bindJson<OpenRequest> (fun body next ctx ->
        task {
            let client = ctx.RequestServices.GetRequiredService<SessionClient>()

            try
                let options = SessionOptions()

                match body.Title with
                | null -> ()
                | title when String.IsNullOrWhiteSpace(title) -> ()
                | title -> options.Title <- title.Trim()

                let! created =
                    SessionClientOperations.OpenSessionAsync(client, AgentId.New(), options, ctx.RequestAborted)

                return!
                    json
                        {
                            SessionId = created.Id.ToString()
                            Title = created.Title
                            State = created.State.ToString()
                        }
                        next
                        ctx
            with ex ->
                return! mapError ex next ctx
        })

/// Prompts a session: appends the text and returns the inbox position.
let private promptHandler (sessionText: string) : HttpHandler =
    bindJson<PromptRequest> (fun body next ctx ->
        task {
            let client = ctx.RequestServices.GetRequiredService<SessionClient>()

            try
                let sessionId = parseSessionId sessionText

                let text =
                    match body.Text with
                    | null -> raise (ArgumentException("A prompt needs non-empty text."))
                    | candidate when String.IsNullOrWhiteSpace(candidate) ->
                        raise (ArgumentException("A prompt needs non-empty text."))
                    | candidate -> candidate.Trim()

                let delivery = parseDelivery body.Delivery

                let! entry =
                    SessionClientOperations.PromptAsync(
                        client,
                        sessionId,
                        UserMessage.Text(text),
                        delivery,
                        ctx.RequestAborted
                    )

                return!
                    json
                        {
                            SessionId = entry.SessionId.ToString()
                            Position = entry.Position
                            Delivery = entry.Delivery.ToString()
                        }
                        next
                        ctx
            with ex ->
                return! mapError ex next ctx
        })

/// Replies to a suspended turn: the body is a Reply with its $type
/// discriminator (permissionDecision or questionAnswer) and answers the
/// pending request id. Nothing pending answers 409.
let private replyHandler (sessionText: string) : HttpHandler =
    bindJson<Reply> (fun body next ctx ->
        task {
            let client = ctx.RequestServices.GetRequiredService<SessionClient>()

            try
                let sessionId = parseSessionId sessionText

                if isNull (box body) then
                    raise (ArgumentException("A reply needs a $type of permissionDecision or questionAnswer."))

                let! entry = SessionClientOperations.ReplyAsync(client, sessionId, body, ctx.RequestAborted)

                return!
                    json
                        {
                            SessionId = entry.SessionId.ToString()
                            Position = entry.Position
                            Delivery = entry.Delivery.ToString()
                        }
                        next
                        ctx
            with ex ->
                return! mapError ex next ctx
        })

/// Aborts the session's running turn: Idle and WaitingForInput no-op with
/// the same acknowledgement.
let private abortHandler (sessionText: string) : HttpHandler =
    bindJson<AbortRequest> (fun body next ctx ->
        task {
            let client = ctx.RequestServices.GetRequiredService<SessionClient>()

            try
                let sessionId = parseSessionId sessionText
                let cause = parseCause body.Cause

                let reason =
                    match body.Reason with
                    | null -> "minimalhost abort"
                    | candidate when String.IsNullOrWhiteSpace(candidate) -> "minimalhost abort"
                    | candidate -> candidate.Trim()

                do! SessionClientOperations.AbortAsync(client, sessionId, cause, reason, ctx.RequestAborted)

                return!
                    json
                        {
                            SessionId = sessionId.ToString()
                            Aborted = true
                        }
                        next
                        ctx
            with ex ->
                return! mapError ex next ctx
        })

// ──────────────────────────────────────────────────────────────────────────
// SSE stream

/// Writes one journaled event as an SSE frame: the sequence as id, the
/// event type as event, the polymorphic JSON as data.
let private writeEvent (ctx: HttpContext) (evt: SessionEvent) : Task =
    task {
        let idLine =
            if evt.Sequence.HasValue then
                $"id: {evt.Sequence.Value}\n"
            else
                ""

        let payload = JsonSerializer.Serialize<SessionEvent>(evt, sseJson)

        do! ctx.Response.WriteAsync($"{idLine}event: {evt.GetType().Name}\ndata: {payload}\n\n", ctx.RequestAborted)
        do! ctx.Response.Body.FlushAsync()
    }

/// Streams the session's events from the Last-Event-ID cursor (0 when
/// absent): replays the journal, then yields live publishes gap-free. The
/// session-closed event ends the stream; a lagged slow consumer ends it
/// too. Unknown sessions probe 404 before the stream headers go out.
let private sseHandler (sessionText: string) : HttpHandler =
    fun _next ctx ->
        task {
            let client = ctx.RequestServices.GetRequiredService<SessionClient>()

            let! outcome =
                task {
                    try
                        let sessionId = parseSessionId sessionText

                        let cursor =
                            match ctx.Request.Headers.TryGetValue("Last-Event-ID") with
                            | true, values ->
                                match Int64.TryParse(values.ToString().Trim()) with
                                | true, value when value >= 0L -> value
                                | _ ->
                                    raise (ArgumentException("Last-Event-ID must be a non-negative sequence number."))
                            | false, _ -> 0L

                        // Probe before the headers: unknown sessions 404
                        // instead of opening a dead stream.
                        let! _ =
                            SessionClientOperations.ReadEventsAsync(client, sessionId, cursor, 1, ctx.RequestAborted)

                        // Attach before the headers too: a full subscriber
                        // table maps 429 instead of a half-open stream.
                        let stream =
                            SessionClientOperations.Subscribe(client, sessionId, cursor, ctx.RequestAborted)

                        ctx.Response.StatusCode <- 200
                        ctx.Response.ContentType <- "text/event-stream"
                        ctx.Response.Headers.CacheControl <- "no-cache"

                        // Flush the headers now: Kestrel sends them with
                        // the first body write, and an idle journal would
                        // otherwise leave the subscriber waiting for
                        // headers until the first event. The comment frame
                        // carries no id and never resumes.
                        do! ctx.Response.WriteAsync(": connected\n\n", ctx.RequestAborted)
                        do! ctx.Response.Body.FlushAsync()

                        let enumerator = stream.GetAsyncEnumerator(ctx.RequestAborted)

                        try
                            let mutable go = true

                            while go do
                                try
                                    let! has = enumerator.MoveNextAsync().AsTask()

                                    if not has then
                                        go <- false
                                    else
                                        let evt = enumerator.Current

                                        if not (isNull (box evt)) then
                                            do! writeEvent ctx evt

                                            match evt with
                                            | :? SessionClosedEvent -> go <- false
                                            | _ -> ()
                                with
                                | :? OperationCanceledException -> go <- false
                                | :? SessionSubscriptionLaggedException -> go <- false
                        finally
                            try
                                enumerator.DisposeAsync().AsTask() |> ignore
                            with _ ->
                                ()

                        return None
                    with ex ->
                        if ctx.Response.HasStarted then
                            return None
                        else
                            return Some(mapError ex)
                }

            match outcome with
            | None -> return Some ctx
            | Some handler -> return! handler _next ctx
        }

/// Liveness probe for the smoke script and container checks.
let private healthHandler: HttpHandler = text "ok"

/// The Giraffe application: session endpoints, the per-session SSE
/// stream, and the liveness probe.
let webApp: HttpHandler =
    choose
        [
            POST
            >=> choose
                    [
                        route "/sessions" >=> openHandler
                        routef "/sessions/%s/prompt" promptHandler
                        routef "/sessions/%s/reply" replyHandler
                        routef "/sessions/%s/abort" abortHandler
                    ]
            GET
            >=> choose
                    [
                        route "/healthz" >=> healthHandler
                        routef "/sessions/%s/events" sseHandler
                    ]
            failure 404 "No such route." "NotFound"
        ]
