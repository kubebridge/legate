// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.WebhookCompletionSinkTests

open System
open System.Collections.Generic
open System.Diagnostics
open System.Net
open System.Net.Sockets
open System.Security.Cryptography
open System.Text
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Legate.Testing
open Microsoft.Extensions.Logging
open Microsoft.Extensions.Time.Testing
open Xunit

// Tests for the default webhook completion sink (issue 83). Every HTTP
// test runs against a local loopback TCP server only: no external
// network, no DNS (literal 127.0.0.1 answers the guard without a
// resolver). Backoff and timeout tests run on the injected ILlmDelay
// seam (RecordingDelay) and short bounds: never sleeps.

// ───────────────────────────────────────────────────────────────────────────
// Doubles

/// One observed loopback request: method, path, headers, and raw body.
type private ObservedRequest =
    {
        Method: string
        Path: string
        Headers: Dictionary<string, string>
        Body: byte[]
    }

/// A minimal loopback HTTP server over TcpListener: records every request
/// and answers request N with the Nth scripted status (the last status
/// repeats), optionally after a delay (an infinite delay models a silent
/// endpoint for the timeout and non-blocking tests). Cross-platform (no
/// HttpListener) and local only. Mirrors the CustomToolFunctionTests
/// server, plus scripted statuses for the retry tests.
type private ScriptedLoopbackServer(statuses: int list, ?responseDelay: TimeSpan) =
    let listener = new TcpListener(IPAddress.Loopback, 0)
    let lifetime = new CancellationTokenSource()
    let gate = obj ()
    let requests = ResizeArray<ObservedRequest>()
    let delay = defaultArg responseDelay TimeSpan.Zero

    let reasonPhrase (status: int) : string =
        match status with
        | 200 -> "OK"
        | 400 -> "Bad Request"
        | 408 -> "Request Timeout"
        | 429 -> "Too Many Requests"
        | 500 -> "Internal Server Error"
        | _ -> "Status"

    let findHeaderEnd (buffer: ResizeArray<byte>) : int =
        let mutable found = -1
        let mutable index = 0

        while found < 0 && index + 3 < buffer.Count do
            if
                buffer[index] = 13uy
                && buffer[index + 1] = 10uy
                && buffer[index + 2] = 13uy
                && buffer[index + 3] = 10uy
            then
                found <- index + 4

            index <- index + 1

        found

    let statusFor (index: int) : int =
        if index < statuses.Length then
            statuses[index]
        else
            statuses[statuses.Length - 1]

    let handle (client: TcpClient) : Task =
        task {
            use client = client

            try
                use stream = client.GetStream()
                let buffer = ResizeArray<byte>()
                let chunk = Array.zeroCreate<byte> 4096
                let mutable headerEnd = -1
                let mutable eof = false

                while headerEnd < 0 && not eof do
                    let! read = stream.ReadAsync(chunk, 0, chunk.Length)

                    if read = 0 then
                        eof <- true
                    else
                        buffer.AddRange(chunk[0 .. read - 1])
                        headerEnd <- findHeaderEnd buffer

                if headerEnd >= 0 then
                    let headerText = Encoding.ASCII.GetString(buffer.GetRange(0, headerEnd).ToArray())
                    let lines = headerText.Split([| "\r\n" |], StringSplitOptions.None)
                    let requestLine = lines[0].Split(' ')
                    let headers = Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)

                    for line in lines[1..] do
                        let colon = line.IndexOf(':')

                        if colon > 0 then
                            headers[line.Substring(0, colon).Trim()] <- line.Substring(colon + 1).Trim()

                    let mutable lengthText: string = "0"

                    if headers.TryGetValue("Content-Length", &lengthText) then
                        ()
                    else
                        lengthText <- "0"

                    let mutable contentLength = 0
                    Int32.TryParse(lengthText, &contentLength) |> ignore

                    let body = Array.zeroCreate<byte> (max 0 contentLength)
                    let buffered = buffer.GetRange(headerEnd, buffer.Count - headerEnd).ToArray()
                    let bufferedCount = min buffered.Length body.Length
                    Array.Copy(buffered, 0, body, 0, bufferedCount)

                    let mutable have = bufferedCount

                    while have < body.Length do
                        let! read = stream.ReadAsync(body, have, body.Length - have)

                        if read = 0 then
                            have <- body.Length
                        else
                            have <- have + read

                    let status =
                        lock gate (fun () ->
                            let index = requests.Count

                            requests.Add(
                                {
                                    Method = requestLine[0]
                                    Path = requestLine[1]
                                    Headers = headers
                                    Body = body
                                }
                            )

                            statusFor index)

                    if delay <> TimeSpan.Zero then
                        do! Task.Delay(delay, lifetime.Token)

                    let responseBytes = Encoding.UTF8.GetBytes("{}")

                    let head =
                        Encoding.ASCII.GetBytes(
                            sprintf
                                "HTTP/1.1 %d %s\r\nContent-Type: application/json\r\nContent-Length: %d\r\nConnection: close\r\n\r\n"
                                status
                                (reasonPhrase status)
                                responseBytes.Length
                        )

                    do! stream.WriteAsync(head, 0, head.Length)
                    do! stream.WriteAsync(responseBytes, 0, responseBytes.Length)
            with _ ->
                ()
        }

    do
        listener.Start()

        let rec loop () : Task =
            task {
                try
                    let! client = listener.AcceptTcpClientAsync()
                    handle client |> ignore
                    return! loop ()
                with _ ->
                    ()
            }

        loop () |> ignore

    /// The loopback port the server listens on.
    member _.Port = (listener.LocalEndpoint :?> IPEndPoint).Port

    /// The absolute URL for one path on this server.
    member this.Url(path: string) =
        sprintf "http://127.0.0.1:%d%s" this.Port path

    /// How many requests arrived so far.
    member _.RequestCount = lock gate (fun () -> requests.Count)

    /// Every request so far, oldest first.
    member _.Requests: ObservedRequest list = lock gate (fun () -> requests |> List.ofSeq)

    interface IDisposable with
        member _.Dispose() =
            lifetime.Cancel()
            listener.Stop()
            lifetime.Dispose()

/// A canned resolver seam: answers every host with loopback. Literal-IP
/// hosts never reach it (the guard answers those itself); the allow list
/// carries the tests past the reserved-address deny.
type private FakeResolver() =

    interface IHostAddressResolver with
        member _.ResolveAsync(_, _) =
            Task.FromResult([| IPAddress.Loopback |] :> IReadOnlyList<IPAddress>)

/// One captured log entry: level plus the rendered message.
type private LoggedEntry = { Level: LogLevel; Text: string }

/// An ILogger capturing every entry for the secret-hygiene and
/// attempt-count assertions.
type private TestLogger() =
    let gate = obj ()
    let entries = ResizeArray<LoggedEntry>()

    let scope =
        { new IDisposable with
            member _.Dispose() = ()
        }

    interface ILogger with
        member _.BeginScope<'TState when 'TState: not null>(_: 'TState) : IDisposable = scope
        member _.IsEnabled(_) = true

        member _.Log<'TState>
            (logLevel: LogLevel, _eventId: EventId, state: 'TState, ex: exn, formatter: Func<'TState, exn, string>)
            : unit =
            let text = formatter.Invoke(state, ex)

            lock gate (fun () -> entries.Add({ Level = logLevel; Text = text }))

    /// Every entry so far, oldest first.
    member _.Entries: LoggedEntry list = lock gate (fun () -> entries |> List.ofSeq)

// ───────────────────────────────────────────────────────────────────────────
// Helpers

let sampleSessionId = SessionId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"
let stamp = DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero)
let secretBytes = Encoding.UTF8.GetBytes("s3cr3t-marker-xyz")

let sampleMetadata () =
    let table = Dictionary<string, string>()
    table["correlation"] <- "delivery-marker-abc"
    table :> IReadOnlyDictionary<string, string>

let sampleTurnResult () : TurnResult =
    {
        AssistantText = "final text"
        Status = TurnStatus.Completed
        Iterations = 3
        Usage =
            {
                InputTokens = 120L
                OutputTokens = 45L
            }
        Outcome = TurnFinished("settled ok") :> TurnOutcome
    }

let sampleCompletion () : SessionCompletion =
    {
        SessionId = sampleSessionId
        TurnResult = sampleTurnResult ()
        Metadata = sampleMetadata ()
        IdempotencyKey = "delivery-1"
    }

/// The guard options the delivery tests run under: loopback explicitly
/// allowed so the loopback server passes the reserved-address deny.
let allowLocal () =
    let options = SsrfGuardOptions()
    options.AllowList.Add("127.0.0.1")
    options

/// Options pointing at one server with the shared secret.
let private makeOptions (server: ScriptedLoopbackServer) =
    let options = WebhookCompletionSinkOptions()
    options.Endpoint <- Uri(server.Url("/hook"))
    options.SigningSecret <- secretBytes
    options

/// A sink with injectable delay, clock, and log seams.
let private makeSink
    (options: WebhookCompletionSinkOptions)
    (guard: SsrfGuardOptions)
    (delay: ILlmDelay)
    (clock: TimeProvider)
    (logger: TestLogger)
    =
    new WebhookCompletionSink(options, FakeResolver() :> IHostAddressResolver, guard, delay, clock, logger :> ILogger)

/// A fixed clock pinned at the golden stamp.
let fixedClock () =
    let clock = new FakeTimeProvider()
    clock.SetUtcNow(stamp)
    clock

/// Blocks until the condition holds or 15 seconds pass, then fails with
/// what was awaited. Polling only: backoff and timeout behaviour comes
/// from the seams, never from these waits.
let waitFor (what: string) (condition: unit -> bool) : unit =
    let deadline = DateTime.UtcNow.AddSeconds(15.0)
    let mutable ok = condition ()

    while not ok && DateTime.UtcNow < deadline do
        Task.Delay(25).GetAwaiter().GetResult()
        ok <- condition ()

    if not ok then
        failwithf "timed out waiting for %s" what

/// Parses JSON without tripping FS0760 (piped construction per the
/// PermissionTests precedent).
let parse (json: string) : JsonDocument = json |> JsonDocument.Parse

/// Parses request bytes as JSON.
let parseBody (body: byte[]) : JsonDocument =
    body |> Encoding.UTF8.GetString |> parse

// ───────────────────────────────────────────────────────────────────────────
// Wire payload contract

[<Fact>]
let ``The wire payload carries the documented shape`` () =
    use server = new ScriptedLoopbackServer([ 200 ])
    let clock = fixedClock ()
    let logger = TestLogger()
    let delay = RecordingDelay()
    use sink = makeSink (makeOptions server) (allowLocal ()) delay clock logger

    (sink :> ISessionCompletionSink).Notify(sampleCompletion ())
    waitFor "the delivery" (fun () -> server.RequestCount = 1)

    let request = server.Requests[0]
    request.Method |> should equal "POST"
    request.Path |> should equal "/hook"
    request.Headers["Content-Type"] |> should equal "application/json"
    request.Headers["Idempotency-Key"] |> should equal "delivery-1"

    use body = parseBody request.Body
    let root = body.RootElement

    root.GetProperty("sessionId").GetString()
    |> should equal "01ARZ3NDEKTSV4RRFFQ69G5FAV"

    root.GetProperty("turnId").GetString() |> should equal ""
    root.GetProperty("status").GetString() |> should equal "Completed"

    let outcome = root.GetProperty("outcome")
    outcome.GetProperty("$type").GetString() |> should equal "turnFinished"
    outcome.GetProperty("summary").GetString() |> should equal "settled ok"

    root.GetProperty("summary").GetString() |> should equal "settled ok"
    root.GetProperty("outputFiles").GetArrayLength() |> should equal 0
    root.GetProperty("iterations").GetInt32() |> should equal 3

    root.GetProperty("usage").GetProperty("inputTokens").GetInt64()
    |> should equal 120L

    root.GetProperty("usage").GetProperty("outputTokens").GetInt64()
    |> should equal 45L

    root.GetProperty("startedAt").ValueKind |> should equal JsonValueKind.Null

    let completedAtText = root.GetProperty("completedAt").GetString()

    match Option.ofObj completedAtText with
    | None -> failwith "completedAt was null"
    | Some text -> DateTimeOffset.Parse(text) |> should equal stamp

    root.GetProperty("idempotencyKey").GetString() |> should equal "delivery-1"

[<Fact>]
let ``The signature equals HMAC-SHA256 over the exact bytes with the sha256 prefix`` () =
    use server = new ScriptedLoopbackServer([ 200 ])
    let clock = fixedClock ()
    let logger = TestLogger()
    let delay = RecordingDelay()
    use sink = makeSink (makeOptions server) (allowLocal ()) delay clock logger

    (sink :> ISessionCompletionSink).Notify(sampleCompletion ())
    waitFor "the delivery" (fun () -> server.RequestCount = 1)

    let request = server.Requests[0]
    request.Headers.ContainsKey("X-Legate-Signature") |> should equal true

    let header = request.Headers["X-Legate-Signature"]
    header.StartsWith("sha256=", StringComparison.Ordinal) |> should equal true

    use hmac = new HMACSHA256(secretBytes)

    let expected =
        "sha256="
        + Convert.ToHexString(hmac.ComputeHash(request.Body)).ToLowerInvariant()

    header |> should equal expected

[<Fact>]
let ``A null outcome falls back to the assistant text with a null wire outcome`` () =
    use server = new ScriptedLoopbackServer([ 200 ])
    let clock = fixedClock ()
    let logger = TestLogger()
    let delay = RecordingDelay()
    use sink = makeSink (makeOptions server) (allowLocal ()) delay clock logger

    let completion =
        { sampleCompletion () with
            TurnResult =
                { sampleTurnResult () with
                    Outcome = null
                }
        }

    (sink :> ISessionCompletionSink).Notify(completion)
    waitFor "the delivery" (fun () -> server.RequestCount = 1)

    use body = parseBody server.Requests[0].Body

    body.RootElement.GetProperty("outcome").ValueKind
    |> should equal JsonValueKind.Null

    body.RootElement.GetProperty("summary").GetString() |> should equal "final text"

[<Fact>]
let ``Abort and failure outcomes map their reasons to the summary`` () =
    use server = new ScriptedLoopbackServer([ 200; 200; 200 ])
    let clock = fixedClock ()
    let logger = TestLogger()
    let delay = RecordingDelay()
    use sink = makeSink (makeOptions server) (allowLocal ()) delay clock logger
    let notify = (sink :> ISessionCompletionSink).Notify

    notify
        { sampleCompletion () with
            TurnResult =
                { sampleTurnResult () with
                    Status = TurnStatus.Aborted
                    Outcome = TurnAborted(StopCause.ExplicitAbort, "the host stopped it") :> TurnOutcome
                }
        }

    notify
        { sampleCompletion () with
            TurnResult =
                { sampleTurnResult () with
                    Status = TurnStatus.Failed
                    Outcome = TurnFailed("the provider fell over") :> TurnOutcome
                }
        }

    notify
        { sampleCompletion () with
            TurnResult =
                { sampleTurnResult () with
                    Outcome = TurnPartiallyFinished("halfway there") :> TurnOutcome
                }
        }

    waitFor "the deliveries" (fun () -> server.RequestCount = 3)

    // One background delivery per Notify: arrival order is
    // intentionally unordered (no cross-session ordering), so compare
    // as sets.
    let summaries =
        server.Requests
        |> List.map (fun request ->
            use body = parseBody request.Body
            body.RootElement.GetProperty("summary").GetString())
        |> List.sort

    summaries
    |> should
        equal
        [
            "halfway there"
            "the host stopped it"
            "the provider fell over"
        ]

// ───────────────────────────────────────────────────────────────────────────
// Backoff, timeout, and attempts

[<Fact>]
let ``Retries back off exponentially over the delay seam`` () =
    use server = new ScriptedLoopbackServer([ 500; 500; 200 ])
    let clock = fixedClock ()
    let logger = TestLogger()
    let delay = RecordingDelay()
    use sink = makeSink (makeOptions server) (allowLocal ()) delay clock logger

    (sink :> ISessionCompletionSink).Notify(sampleCompletion ())
    waitFor "the retries" (fun () -> server.RequestCount = 3)

    delay.Recorded
    |> List.ofSeq
    |> should
        equal
        [
            TimeSpan.FromSeconds(2.0)
            TimeSpan.FromSeconds(4.0)
        ]

    logger.Entries
    |> List.exists (fun entry -> entry.Level = LogLevel.Error)
    |> should equal false

[<Fact>]
let ``Exhausted retries log the attempt count and stop at the budget`` () =
    use server = new ScriptedLoopbackServer([ 500 ])
    let clock = fixedClock ()
    let logger = TestLogger()
    let delay = RecordingDelay()

    let options = makeOptions server
    options.MaxDeliveryAttempts <- 3
    use sink = makeSink options (allowLocal ()) delay clock logger

    (sink :> ISessionCompletionSink).Notify(sampleCompletion ())

    waitFor "the permanent failure" (fun () ->
        logger.Entries |> List.exists (fun entry -> entry.Level = LogLevel.Error))

    server.RequestCount |> should equal (CompletionOptions().MaxDeliveryAttempts)
    server.RequestCount |> should equal 3

    delay.Recorded
    |> List.ofSeq
    |> should
        equal
        [
            TimeSpan.FromSeconds(2.0)
            TimeSpan.FromSeconds(4.0)
        ]

    let failure =
        logger.Entries |> List.find (fun entry -> entry.Level = LogLevel.Error)

    failure.Text.Contains("3 of 3") |> should equal true
    failure.Text.Contains("01ARZ3NDEKTSV4RRFFQ69G5FAV") |> should equal true
    failure.Text.Contains("delivery-1") |> should equal true

[<Fact>]
let ``A single attempt never waits`` () =
    use server = new ScriptedLoopbackServer([ 500 ])
    let clock = fixedClock ()
    let logger = TestLogger()
    let delay = RecordingDelay()

    let options = makeOptions server
    options.MaxDeliveryAttempts <- 1
    use sink = makeSink options (allowLocal ()) delay clock logger

    (sink :> ISessionCompletionSink).Notify(sampleCompletion ())

    waitFor "the permanent failure" (fun () ->
        logger.Entries |> List.exists (fun entry -> entry.Level = LogLevel.Error))

    server.RequestCount |> should equal 1
    delay.Recorded.Count |> should equal 0

    let failure =
        logger.Entries |> List.find (fun entry -> entry.Level = LogLevel.Error)

    failure.Text.Contains("1 of 1") |> should equal true

[<Fact>]
let ``A silent endpoint fails bounded by the timeout`` () =
    use server =
        new ScriptedLoopbackServer([ 200 ], responseDelay = Timeout.InfiniteTimeSpan)

    let clock = fixedClock ()
    let logger = TestLogger()
    let delay = RecordingDelay()

    let options = makeOptions server
    options.Timeout <- TimeSpan.FromMilliseconds(250.0)
    options.MaxDeliveryAttempts <- 1
    use sink = makeSink options (allowLocal ()) delay clock logger

    (sink :> ISessionCompletionSink).Notify(sampleCompletion ())

    waitFor "the request" (fun () -> server.RequestCount = 1)

    waitFor "the timeout failure" (fun () -> logger.Entries |> List.exists (fun entry -> entry.Level = LogLevel.Error))

    delay.Recorded.Count |> should equal 0

    let failure =
        logger.Entries |> List.find (fun entry -> entry.Level = LogLevel.Error)

    failure.Text.Contains("timed out") |> should equal true
    failure.Text.Contains("1 of 1") |> should equal true

[<Fact>]
let ``A client error stops without retry`` () =
    use server = new ScriptedLoopbackServer([ 400 ])
    let clock = fixedClock ()
    let logger = TestLogger()
    let delay = RecordingDelay()
    use sink = makeSink (makeOptions server) (allowLocal ()) delay clock logger

    (sink :> ISessionCompletionSink).Notify(sampleCompletion ())

    waitFor "the permanent failure" (fun () ->
        logger.Entries |> List.exists (fun entry -> entry.Level = LogLevel.Error))

    server.RequestCount |> should equal 1
    delay.Recorded.Count |> should equal 0

    let failure =
        logger.Entries |> List.find (fun entry -> entry.Level = LogLevel.Error)

    failure.Text.Contains("400") |> should equal true
    failure.Text.Contains("1 of 3") |> should equal true

// ───────────────────────────────────────────────────────────────────────────
// Non-blocking Notify

[<Fact>]
let ``Notify returns while the delivery is still in flight`` () =
    use server =
        new ScriptedLoopbackServer([ 200 ], responseDelay = Timeout.InfiniteTimeSpan)

    let clock = fixedClock ()
    let logger = TestLogger()
    let delay = RecordingDelay()

    let options = makeOptions server
    options.Timeout <- TimeSpan.FromMinutes(10.0)
    use sink = makeSink options (allowLocal ()) delay clock logger

    let watch = Stopwatch.StartNew()
    (sink :> ISessionCompletionSink).Notify(sampleCompletion ())
    watch.Stop()

    watch.Elapsed |> should be (lessThan (TimeSpan.FromSeconds(5.0)))

    // The background delivery started after Notify already returned: the
    // request arrives while the endpoint stays silent.
    waitFor "the background delivery" (fun () -> server.RequestCount = 1)

// ───────────────────────────────────────────────────────────────────────────
// Unsigned delivery and secret hygiene

[<Fact>]
let ``A missing secret without opt-in never POSTs`` () =
    use server = new ScriptedLoopbackServer([ 200 ])

    let options = WebhookCompletionSinkOptions()
    options.Endpoint <- Uri(server.Url("/hook"))

    match Option.ofObj (options.Validate()) with
    | Some text -> text.Contains("SigningSecret") |> should equal true
    | None -> failwith "expected a validation violation for the missing secret"

    (fun () -> new WebhookCompletionSink(options, FakeResolver(), SsrfGuardOptions()) |> ignore)
    |> should throw typeof<ArgumentException>

[<Fact>]
let ``Explicit unsigned opt-in sends with no signature header`` () =
    use server = new ScriptedLoopbackServer([ 200 ])
    let clock = fixedClock ()
    let logger = TestLogger()
    let delay = RecordingDelay()

    let options = WebhookCompletionSinkOptions()
    options.Endpoint <- Uri(server.Url("/hook"))
    options.AllowUnsignedDelivery <- true

    Option.ofObj (options.Validate()) |> Option.isNone |> should equal true

    use sink = makeSink options (allowLocal ()) delay clock logger

    (sink :> ISessionCompletionSink).Notify(sampleCompletion ())
    waitFor "the delivery" (fun () -> server.RequestCount = 1)

    let request = server.Requests[0]
    request.Headers.ContainsKey("X-Legate-Signature") |> should equal false
    request.Headers["Idempotency-Key"] |> should equal "delivery-1"

[<Fact>]
let ``An SSRF denial sends nothing and never retries`` () =
    use server = new ScriptedLoopbackServer([ 200 ])
    let clock = fixedClock ()
    let logger = TestLogger()
    let delay = RecordingDelay()
    // No allow entry: loopback stays reserved-denied.
    use sink = makeSink (makeOptions server) (SsrfGuardOptions()) delay clock logger

    (sink :> ISessionCompletionSink).Notify(sampleCompletion ())
    waitFor "the denial" (fun () -> logger.Entries.Length > 0)

    server.RequestCount |> should equal 0
    delay.Recorded.Count |> should equal 0

    let failure =
        logger.Entries |> List.find (fun entry -> entry.Level = LogLevel.Error)

    failure.Text.Contains("1 of 3") |> should equal true
    failure.Text.Contains("denied address") |> should equal true

[<Fact>]
let ``Secrets never reach logs, errors, or the wire`` () =
    use server = new ScriptedLoopbackServer([ 500; 500 ])
    let clock = fixedClock ()
    let logger = TestLogger()
    let delay = RecordingDelay()

    let options = makeOptions server
    options.MaxDeliveryAttempts <- 2
    use sink = makeSink options (allowLocal ()) delay clock logger

    (sink :> ISessionCompletionSink).Notify(sampleCompletion ())

    waitFor "the permanent failure" (fun () ->
        logger.Entries |> List.exists (fun entry -> entry.Level = LogLevel.Error))

    let secretText = Encoding.UTF8.GetString(secretBytes)
    let hexLower = Convert.ToHexString(secretBytes).ToLowerInvariant()
    let hexUpper = Convert.ToHexString(secretBytes)
    let base64 = Convert.ToBase64String(secretBytes)

    for entry in logger.Entries do
        entry.Text.Contains(secretText) |> should equal false
        entry.Text.Contains(hexLower) |> should equal false
        entry.Text.Contains(hexUpper) |> should equal false
        entry.Text.Contains(base64) |> should equal false
        entry.Text.Contains("delivery-marker-abc") |> should equal false

    for request in server.Requests do
        let wire = Encoding.UTF8.GetString(request.Body)
        wire.Contains(secretText) |> should equal false
        wire.Contains(hexLower) |> should equal false
        wire.Contains(base64) |> should equal false
        wire.Contains("delivery-marker-abc") |> should equal false

        use body = parse wire

        let hasMetadata, _ = body.RootElement.TryGetProperty("metadata")
        hasMetadata |> should equal false

        let hasSecret, _ = body.RootElement.TryGetProperty("signingSecret")
        hasSecret |> should equal false

// ───────────────────────────────────────────────────────────────────────────
// Options validation

[<Fact>]
let ``Defaults are valid and carry the documented knobs`` () =
    use server = new ScriptedLoopbackServer([ 200 ])
    let options = makeOptions server

    Option.ofObj (options.Validate()) |> Option.isNone |> should equal true
    options.Timeout |> should equal WebhookCompletionSinkOptions.DefaultTimeout
    options.Timeout |> should equal (TimeSpan.FromSeconds(30.0))
    options.BaseRetryDelay |> should equal (TimeSpan.FromSeconds(2.0))
    options.MaxRetryDelay |> should equal (TimeSpan.FromSeconds(30.0))
    options.MaxDeliveryAttempts |> should equal 0
    options.AllowUnsignedDelivery |> should equal false

[<Fact>]
let ``Validate rejects a missing endpoint`` () =
    let options = WebhookCompletionSinkOptions()
    options.SigningSecret <- secretBytes

    options.Validate()
    |> Option.ofObj
    |> should equal (Some "Endpoint must be set to the absolute webhook URL.")

[<Fact>]
let ``Validate rejects a non-HTTP endpoint`` () =
    let options = WebhookCompletionSinkOptions()
    options.Endpoint <- Uri("ftp://127.0.0.1/hook")
    options.SigningSecret <- secretBytes

    options.Validate()
    |> Option.ofObj
    |> should equal (Some "Endpoint must be an absolute http or https URL.")

[<Fact>]
let ``Validate rejects a non-positive timeout`` () =
    use server = new ScriptedLoopbackServer([ 200 ])
    let options = makeOptions server
    options.Timeout <- TimeSpan.Zero

    options.Validate()
    |> Option.ofObj
    |> should equal (Some "Timeout must be positive.")

[<Fact>]
let ``Validate rejects a negative attempt budget`` () =
    use server = new ScriptedLoopbackServer([ 200 ])
    let options = makeOptions server
    options.MaxDeliveryAttempts <- -1

    options.Validate()
    |> Option.ofObj
    |> should
        equal
        (Some "MaxDeliveryAttempts must not be negative: 0 falls back to CompletionOptions.MaxDeliveryAttempts.")

[<Fact>]
let ``Validate rejects a negative base retry delay`` () =
    use server = new ScriptedLoopbackServer([ 200 ])
    let options = makeOptions server
    options.BaseRetryDelay <- TimeSpan.FromSeconds(-1.0)

    options.Validate()
    |> Option.ofObj
    |> should equal (Some "BaseRetryDelay must not be negative.")

[<Fact>]
let ``Validate rejects a cap below the base retry delay`` () =
    use server = new ScriptedLoopbackServer([ 200 ])
    let options = makeOptions server
    options.MaxRetryDelay <- TimeSpan.FromSeconds(1.0)

    options.Validate()
    |> Option.ofObj
    |> should equal (Some "MaxRetryDelay must not be less than BaseRetryDelay.")
