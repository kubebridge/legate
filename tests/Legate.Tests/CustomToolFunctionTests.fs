// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.CustomToolFunctionTests

open System
open System.Collections.Generic
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
open Microsoft.Extensions.AI
open Xunit

// Tests for the signed custom tool invoker (issue 74, task 3) plus its
// wiring through the existing TurnLoop permission and claim-fence path
// (task 5). Every HTTP test runs against a local loopback TCP server only:
// no external network, no DNS. The permission section mirrors
// ExecToolTests: the tool adds no gating of its own, so Deny/Ask/fence
// cases prove zero requests leave the process.

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
/// and answers each with the scripted status and body, optionally after a
/// delay (infinite delay models a silent endpoint for the timeout test).
/// Cross-platform (no HttpListener) and local only.
type private LoopbackServer(responseStatus: int, responseBody: string, ?responseDelay: TimeSpan) =
    let listener = new TcpListener(IPAddress.Loopback, 0)
    let lifetime = new CancellationTokenSource()
    let gate = obj ()
    let requests = ResizeArray<ObservedRequest>()
    let delay = defaultArg responseDelay TimeSpan.Zero

    let reasonPhrase (status: int) : string =
        match status with
        | 200 -> "OK"
        | 400 -> "Bad Request"
        | 500 -> "Internal Server Error"
        | _ -> "Status"

    /// Finds the end of the header block: the index just past the first
    /// blank line, or -1 while the block is incomplete.
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

                    let mutable expect: string = ""
                    let mutable lengthText: string = "0"

                    if
                        headers.TryGetValue("Expect", &expect)
                        && expect.StartsWith("100", StringComparison.OrdinalIgnoreCase)
                    then
                        let interim = Encoding.ASCII.GetBytes("HTTP/1.1 100 Continue\r\n\r\n")
                        do! stream.WriteAsync(interim, 0, interim.Length)

                    let mutable contentLength = 0

                    if headers.TryGetValue("Content-Length", &lengthText) then
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

                    lock gate (fun () ->
                        requests.Add(
                            {
                                Method = requestLine[0]
                                Path = requestLine[1]
                                Headers = headers
                                Body = body
                            }
                        ))

                    if delay <> TimeSpan.Zero then
                        do! Task.Delay(delay, lifetime.Token)

                    let responseBytes = Encoding.UTF8.GetBytes(responseBody)

                    let head =
                        Encoding.ASCII.GetBytes(
                            sprintf
                                "HTTP/1.1 %d %s\r\nContent-Type: application/json\r\nContent-Length: %d\r\nConnection: close\r\n\r\n"
                                responseStatus
                                (reasonPhrase responseStatus)
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

/// The guard options the tests run under: loopback explicitly allowed so
/// the loopback server passes the reserved-address deny.
let private allowLocal () =
    let options = SsrfGuardOptions()
    options.AllowList.Add("127.0.0.1")
    options

/// An ILlmDelay that never elapses: the turn deadline stays pending so
/// wiring tests run without one.
type private NeverLlmDelay() =
    interface ILlmDelay with
        member _.Delay(_, cancellationToken) =
            Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken)

/// A permission policy returning one fixed verdict for every call.
type private FixedPolicy(verdict: PermissionVerdict) =
    interface IPermissionPolicy with
        member _.Evaluate(_) = verdict

// ───────────────────────────────────────────────────────────────────────────
// Helpers

/// Builds the function under test against one loopback server.
let private makeFunction
    (server: LoopbackServer)
    (secret: byte[])
    (timeout: TimeSpan)
    (headers: IReadOnlyDictionary<string, string> | null)
    (schema: JsonDocument)
    (options: SsrfGuardOptions)
    =
    CustomToolFunction(
        "custom_lookup",
        "Looks a value up over HTTP.",
        schema,
        Uri(server.Url("/invoke")),
        headers,
        secret,
        FakeResolver() :> IHostAddressResolver,
        options,
        timeout
    )

/// Invokes the function with the given arguments and returns the result
/// text.
let private invoke (fn: AIFunction) (args: (string * obj) list) (token: CancellationToken) : string =
    let table = Dictionary<string, obj>()

    for key, value in args do
        table[key] <- value

    let result =
        (fn.InvokeAsync(AIFunctionArguments(table :> IDictionary<string, obj>), token)).GetAwaiter().GetResult()

    match Option.ofObj result with
    | None -> ""
    | Some live ->
        let text: string | null = live.ToString()

        match Option.ofObj text with
        | Some value -> value
        | None -> ""

/// Parses the envelope JSON. Piped construction per the PermissionTests
/// precedent (a direct JsonDocument.Parse call trips FS0760 under
/// TreatWarningsAsErrors).
let private parse (json: string) : JsonDocument = json |> JsonDocument.Parse

/// Reads the envelope's content and error flag.
let private envelope (json: string) : string * bool =
    use doc = parse json
    let content: string | null = doc.RootElement.GetProperty("content").GetString()

    let text =
        match Option.ofObj content with
        | Some value -> value
        | None -> ""

    (text, doc.RootElement.GetProperty("isError").GetBoolean())

let private scriptedClient (steps: ScriptStep list) : ScriptedChatClient =
    new ScriptedChatClient(ResizeArray<ScriptStep>(steps) :> IReadOnlyList<ScriptStep>)

/// Converts a tool return value to text: null becomes empty, everything
/// else renders through ToString.
let private resultText (value: obj | null) : string =
    match value with
    | null -> ""
    | live ->
        let text: string | null = live.ToString()

        match Option.ofObj text with
        | Some value -> value
        | None -> ""

/// Collects every tool-result text in history order.
let private toolResultTexts (history: IList<ChatMessage>) : string list =
    [
        for message in history do
            if
                not (isNull (box message))
                && message.Role = ChatRole.Tool
                && not (isNull (box message.Contents))
            then
                for content in message.Contents do
                    if not (isNull (box content)) && content :? FunctionResultContent then
                        let result = content :?> FunctionResultContent
                        yield resultText result.Result
    ]

let private historyWith (fn: AIFunction) : Dictionary<string, AITool> =
    let tools = Dictionary<string, AITool>()
    tools["custom_lookup"] <- fn :> AITool
    tools

// ───────────────────────────────────────────────────────────────────────────
// Tool definition

[<Fact>]
let ``The tool carries its name, description, and schema`` () =
    use server = new LoopbackServer(200, """{"content":"ok","isError":false}""")

    use schema =
        JsonDocument.Parse """{"type":"object","properties":{"q":{"type":"string"}}}"""

    let fn =
        makeFunction server (Encoding.UTF8.GetBytes("secret")) (TimeSpan.FromSeconds(10.0)) null schema (allowLocal ())

    fn.Name |> should equal "custom_lookup"
    fn.Description |> should equal "Looks a value up over HTTP."

    (fn.JsonSchema.GetProperty("properties").GetProperty("q").GetProperty("type").GetString())
    |> should equal "string"

// ───────────────────────────────────────────────────────────────────────────
// Success and the signature contract

[<Fact>]
let ``A successful call returns the endpoint content with isError false`` () =
    use server =
        new LoopbackServer(200, """{"content":"served-value","isError":false}""")

    use schema = JsonDocument.Parse """{"type":"object"}"""

    let fn =
        makeFunction server (Encoding.UTF8.GetBytes("secret")) (TimeSpan.FromSeconds(10.0)) null schema (allowLocal ())

    let (content, isError) =
        invoke fn [ "q", "hello" :> obj ] CancellationToken.None |> envelope

    content |> should equal "served-value"
    isError |> should equal false
    server.RequestCount |> should equal 1

    let request = server.Requests[0]
    request.Method |> should equal "POST"
    request.Path |> should equal "/invoke"

[<Fact>]
let ``The signature header equals the HMAC of the exact bytes sent`` () =
    use server = new LoopbackServer(200, """{"content":"ok","isError":false}""")
    use schema = JsonDocument.Parse """{"type":"object"}"""
    let secret = Encoding.UTF8.GetBytes("hmac-test-secret")

    let headers =
        let table = Dictionary<string, string>()
        table["X-Custom-Marker"] <- "custom-header-value"
        table :> IReadOnlyDictionary<string, string>

    let fn =
        makeFunction server secret (TimeSpan.FromSeconds(10.0)) headers schema (allowLocal ())

    invoke fn [ "q", "hello" :> obj; "n", 7 :> obj ] CancellationToken.None
    |> ignore

    server.RequestCount |> should equal 1
    let request = server.Requests[0]
    request.Headers["Content-Type"] |> should equal "application/json"
    request.Headers["X-Custom-Marker"] |> should equal "custom-header-value"
    request.Headers.ContainsKey("X-Legate-Signature") |> should equal true

    let header = request.Headers["X-Legate-Signature"]
    header.StartsWith("sha256:", StringComparison.Ordinal) |> should equal true

    use hmac = new HMACSHA256(secret)

    let expected =
        "sha256:"
        + Convert.ToHexString(hmac.ComputeHash(request.Body)).ToLowerInvariant()

    header |> should equal expected

    // The signed bytes carry the model arguments.
    use body = JsonDocument.Parse(request.Body)
    body.RootElement.GetProperty("q").GetString() |> should equal "hello"
    body.RootElement.GetProperty("n").GetInt32() |> should equal 7

[<Fact>]
let ``JsonElement arguments serialize like CLR values`` () =
    use server = new LoopbackServer(200, """{"content":"ok","isError":false}""")
    use schema = JsonDocument.Parse """{"type":"object"}"""

    let fn =
        makeFunction server (Encoding.UTF8.GetBytes("secret")) (TimeSpan.FromSeconds(10.0)) null schema (allowLocal ())

    use argDoc = JsonDocument.Parse """{"nested":[1,2]}"""

    invoke fn [ "payload", argDoc.RootElement :> obj ] CancellationToken.None
    |> ignore

    server.RequestCount |> should equal 1
    use body = JsonDocument.Parse(server.Requests[0].Body)

    body.RootElement.GetProperty("payload").GetRawText()
    |> should equal """{"nested":[1,2]}"""

[<Fact>]
let ``A remote isError true survives with its content`` () =
    use server =
        new LoopbackServer(200, """{"content":"the endpoint failed softly","isError":true}""")

    use schema = JsonDocument.Parse """{"type":"object"}"""

    let fn =
        makeFunction server (Encoding.UTF8.GetBytes("secret")) (TimeSpan.FromSeconds(10.0)) null schema (allowLocal ())

    let (content, isError) =
        invoke fn [ "q", "hi" :> obj ] CancellationToken.None |> envelope

    content |> should equal "the endpoint failed softly"
    isError |> should equal true

[<Fact>]
let ``Non-string content serves as its raw JSON`` () =
    use server = new LoopbackServer(200, """{"content":[1,2]}""")
    use schema = JsonDocument.Parse """{"type":"object"}"""

    let fn =
        makeFunction server (Encoding.UTF8.GetBytes("secret")) (TimeSpan.FromSeconds(10.0)) null schema (allowLocal ())

    let (content, isError) =
        invoke fn [ "q", "hi" :> obj ] CancellationToken.None |> envelope

    content |> should equal "[1,2]"
    isError |> should equal false

// ───────────────────────────────────────────────────────────────────────────
// Bounded failures: never throws

[<Fact>]
let ``A non-2xx status maps to a bounded error with the status`` () =
    use server = new LoopbackServer(500, """{"error":"boom"}""")
    use schema = JsonDocument.Parse """{"type":"object"}"""

    let fn =
        makeFunction server (Encoding.UTF8.GetBytes("secret")) (TimeSpan.FromSeconds(10.0)) null schema (allowLocal ())

    let (content, isError) =
        invoke fn [ "q", "hi" :> obj ] CancellationToken.None |> envelope

    content.Contains("500") |> should equal true
    isError |> should equal true

[<Fact>]
let ``Malformed JSON maps to a bounded error`` () =
    use server = new LoopbackServer(200, "this is not json{{{")
    use schema = JsonDocument.Parse """{"type":"object"}"""

    let fn =
        makeFunction server (Encoding.UTF8.GetBytes("secret")) (TimeSpan.FromSeconds(10.0)) null schema (allowLocal ())

    let (content, isError) =
        invoke fn [ "q", "hi" :> obj ] CancellationToken.None |> envelope

    content.Contains("malformed") |> should equal true
    isError |> should equal true

[<Fact>]
let ``A missing content property maps to a bounded error`` () =
    use server = new LoopbackServer(200, """{"isError":false}""")
    use schema = JsonDocument.Parse """{"type":"object"}"""

    let fn =
        makeFunction server (Encoding.UTF8.GetBytes("secret")) (TimeSpan.FromSeconds(10.0)) null schema (allowLocal ())

    let (content, isError) =
        invoke fn [ "q", "hi" :> obj ] CancellationToken.None |> envelope

    content.Contains("malformed") |> should equal true
    isError |> should equal true

[<Fact>]
let ``A silent endpoint maps to a bounded timeout, never a throw`` () =
    // The server never responds, so only the timeout path can complete:
    // no sleep, no flakiness.
    use server =
        new LoopbackServer(200, """{"content":"late","isError":false}""", responseDelay = Timeout.InfiniteTimeSpan)

    use schema = JsonDocument.Parse """{"type":"object"}"""

    let fn =
        makeFunction
            server
            (Encoding.UTF8.GetBytes("secret"))
            (TimeSpan.FromMilliseconds(100.0))
            null
            schema
            (allowLocal ())

    let (content, isError) =
        invoke fn [ "q", "hi" :> obj ] CancellationToken.None |> envelope

    content.Contains("timed out") |> should equal true
    isError |> should equal true
    server.RequestCount |> should equal 1

[<Fact>]
let ``A cancelled turn maps to a bounded error with nothing sent`` () =
    use server = new LoopbackServer(200, """{"content":"ok","isError":false}""")
    use schema = JsonDocument.Parse """{"type":"object"}"""

    let fn =
        makeFunction server (Encoding.UTF8.GetBytes("secret")) (TimeSpan.FromSeconds(10.0)) null schema (allowLocal ())

    use cancelled = new CancellationTokenSource()
    cancelled.Cancel()

    let (content, isError) = invoke fn [ "q", "hi" :> obj ] cancelled.Token |> envelope

    content.Contains("cancelled") |> should equal true
    isError |> should equal true
    server.RequestCount |> should equal 0

[<Fact>]
let ``A guard denial returns the bounded string with nothing sent`` () =
    // No allow list: loopback resolves to a denied address, so the guard
    // denies before anything is signed or sent.
    use server = new LoopbackServer(200, """{"content":"ok","isError":false}""")
    use schema = JsonDocument.Parse """{"type":"object"}"""

    let fn =
        makeFunction
            server
            (Encoding.UTF8.GetBytes("secret"))
            (TimeSpan.FromSeconds(10.0))
            null
            schema
            (SsrfGuardOptions())

    let (content, isError) =
        invoke fn [ "q", "hi" :> obj ] CancellationToken.None |> envelope

    content |> should equal (SsrfDenyReason.AddressDenied.ToBoundedString())
    isError |> should equal true
    server.RequestCount |> should equal 0

[<Fact>]
let ``An empty secret maps to a bounded misconfiguration error`` () =
    use server = new LoopbackServer(200, """{"content":"ok","isError":false}""")
    use schema = JsonDocument.Parse """{"type":"object"}"""

    let fn =
        makeFunction server [||] (TimeSpan.FromSeconds(10.0)) null schema (allowLocal ())

    let (content, isError) =
        invoke fn [ "q", "hi" :> obj ] CancellationToken.None |> envelope

    content.Contains("misconfigured") |> should equal true
    isError |> should equal true
    server.RequestCount |> should equal 0

[<Fact>]
let ``Secrets headers and arguments never reach the envelope`` () =
    // Distinctive markers travel the secret, a header value, and an
    // argument; no envelope on any path may contain any of them.
    let secretMarker = "secret-marker-4d2a"
    let headerMarker = "header-marker-7b1e"
    let argumentMarker = "argument-marker-9c0f"

    let envelopes = ResizeArray<string>()

    use okServer = new LoopbackServer(200, """{"content":"fine","isError":false}""")
    use schema = JsonDocument.Parse """{"type":"object"}"""

    let headers =
        let table = Dictionary<string, string>()
        table["X-Trace"] <- headerMarker
        table :> IReadOnlyDictionary<string, string>

    let okFn =
        makeFunction
            okServer
            (Encoding.UTF8.GetBytes(secretMarker))
            (TimeSpan.FromSeconds(10.0))
            headers
            schema
            (allowLocal ())

    envelopes.Add(invoke okFn [ "q", argumentMarker :> obj ] CancellationToken.None)

    use badServer = new LoopbackServer(500, "boom")
    use badSchema = JsonDocument.Parse """{"type":"object"}"""

    let badFn =
        makeFunction
            badServer
            (Encoding.UTF8.GetBytes(secretMarker))
            (TimeSpan.FromSeconds(10.0))
            headers
            badSchema
            (allowLocal ())

    envelopes.Add(invoke badFn [ "q", argumentMarker :> obj ] CancellationToken.None)

    use malformedServer = new LoopbackServer(200, "nope{{{")
    use malformedSchema = JsonDocument.Parse """{"type":"object"}"""

    let malformedFn =
        makeFunction
            malformedServer
            (Encoding.UTF8.GetBytes(secretMarker))
            (TimeSpan.FromSeconds(10.0))
            headers
            malformedSchema
            (allowLocal ())

    envelopes.Add(invoke malformedFn [ "q", argumentMarker :> obj ] CancellationToken.None)

    let deniedFn =
        makeFunction
            okServer
            (Encoding.UTF8.GetBytes(secretMarker))
            (TimeSpan.FromSeconds(10.0))
            headers
            malformedSchema
            (SsrfGuardOptions())

    envelopes.Add(invoke deniedFn [ "q", argumentMarker :> obj ] CancellationToken.None)

    envelopes.Count |> should equal 4

    for envelopeText in envelopes do
        envelopeText.Contains(secretMarker) |> should equal false
        envelopeText.Contains(headerMarker) |> should equal false
        envelopeText.Contains(argumentMarker) |> should equal false

// ───────────────────────────────────────────────────────────────────────────
// Turn wiring through the existing permission and claim-fence path

[<Fact>]
let ``A Deny verdict appends denial text without sending anything`` () =
    use server = new LoopbackServer(200, """{"content":"served","isError":false}""")
    use schema = JsonDocument.Parse """{"type":"object"}"""

    let fn =
        makeFunction server (Encoding.UTF8.GetBytes("secret")) (TimeSpan.FromSeconds(10.0)) null schema (allowLocal ())

    let tools = historyWith fn

    let args = Dictionary<string, obj>()
    args["q"] <- "hi" :> obj

    let client =
        scriptedClient
            [
                ScriptStep.ToolCall("c1", "custom_lookup", args)
                ScriptStep.Text("done")
            ]

    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

    let completion =
        TurnLoop.runSuspendableAsync
            (client :> IChatClient)
            history
            (tools :> IReadOnlyDictionary<string, AITool>)
            TurnLoop.TurnLoopOptions.Default
            (NeverLlmDelay() :> ILlmDelay)
            CancellationToken.None
            (fun () -> true)
            (fun () -> ResizeArray<InboxEntry>() :> IReadOnlyList<InboxEntry>)
            ignore
            ignore
            (FixedPolicy(PermissionVerdict.Deny "host says no") :> IPermissionPolicy)
            (SessionId.New())
            (TurnId.New())
            None
            (HashSet<string>())
        |> fun task -> task.GetAwaiter().GetResult()

    completion.Result.Status |> should equal TurnStatus.Completed

    let texts = toolResultTexts history
    texts.Length |> should equal 1
    texts[0].Contains(TurnLoop.DenyResultPrefix) |> should equal true
    server.RequestCount |> should equal 0

[<Fact>]
let ``An Ask verdict suspends without sending anything`` () =
    use server = new LoopbackServer(200, """{"content":"served","isError":false}""")
    use schema = JsonDocument.Parse """{"type":"object"}"""

    let fn =
        makeFunction server (Encoding.UTF8.GetBytes("secret")) (TimeSpan.FromSeconds(10.0)) null schema (allowLocal ())

    let tools = historyWith fn

    let args = Dictionary<string, obj>()
    args["q"] <- "hi" :> obj

    let client =
        scriptedClient
            [
                ScriptStep.ToolCall("c1", "custom_lookup", args)
                ScriptStep.Text("never")
            ]

    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

    let completion =
        TurnLoop.runSuspendableAsync
            (client :> IChatClient)
            history
            (tools :> IReadOnlyDictionary<string, AITool>)
            TurnLoop.TurnLoopOptions.Default
            (NeverLlmDelay() :> ILlmDelay)
            CancellationToken.None
            (fun () -> true)
            (fun () -> ResizeArray<InboxEntry>() :> IReadOnlyList<InboxEntry>)
            ignore
            ignore
            (FixedPolicy(PermissionVerdict.Ask) :> IPermissionPolicy)
            (SessionId.New())
            (TurnId.New())
            None
            (HashSet<string>())
        |> fun task -> task.GetAwaiter().GetResult()

    completion.Result.Status |> should equal TurnStatus.Suspended
    server.RequestCount |> should equal 0

[<Fact>]
let ``An Allow verdict executes the custom tool once through the loop`` () =
    use server =
        new LoopbackServer(200, """{"content":"served-content","isError":false}""")

    use schema = JsonDocument.Parse """{"type":"object"}"""

    let fn =
        makeFunction server (Encoding.UTF8.GetBytes("secret")) (TimeSpan.FromSeconds(10.0)) null schema (allowLocal ())

    let tools = historyWith fn

    let args = Dictionary<string, obj>()
    args["q"] <- "hi" :> obj

    let client =
        scriptedClient
            [
                ScriptStep.ToolCall("c1", "custom_lookup", args)
                ScriptStep.Text("done")
            ]

    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

    let completion =
        TurnLoop.runSuspendableAsync
            (client :> IChatClient)
            history
            (tools :> IReadOnlyDictionary<string, AITool>)
            TurnLoop.TurnLoopOptions.Default
            (NeverLlmDelay() :> ILlmDelay)
            CancellationToken.None
            (fun () -> true)
            (fun () -> ResizeArray<InboxEntry>() :> IReadOnlyList<InboxEntry>)
            ignore
            ignore
            (FixedPolicy(PermissionVerdict.Allow) :> IPermissionPolicy)
            (SessionId.New())
            (TurnId.New())
            None
            (HashSet<string>())
        |> fun task -> task.GetAwaiter().GetResult()

    completion.Result.Status |> should equal TurnStatus.Completed
    server.RequestCount |> should equal 1

    let texts = toolResultTexts history
    texts.Length |> should equal 1
    texts[0].Contains("served-content") |> should equal true

[<Fact>]
let ``A fenced-out claim never sends the custom tool call`` () =
    // Takeover double-invoke: the loser loses the last-moment fence, so
    // zero effects come out of it. Covered by the existing VerifyClaim
    // path; the tool adds no gating of its own.
    use server = new LoopbackServer(200, """{"content":"served","isError":false}""")
    use schema = JsonDocument.Parse """{"type":"object"}"""

    let fn =
        makeFunction server (Encoding.UTF8.GetBytes("secret")) (TimeSpan.FromSeconds(10.0)) null schema (allowLocal ())

    let tools = historyWith fn

    let args = Dictionary<string, obj>()
    args["q"] <- "hi" :> obj

    let client =
        scriptedClient
            [
                ScriptStep.ToolCall("c1", "custom_lookup", args)
                ScriptStep.Text("never")
            ]

    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

    let options =
        { TurnLoop.TurnLoopOptions.Default with
            VerifyClaim = Some(fun () -> Task.FromResult(false))
        }

    (fun () ->
        TurnLoop.runAsync
            (client :> IChatClient)
            history
            (tools :> IReadOnlyDictionary<string, AITool>)
            options
            (NeverLlmDelay() :> ILlmDelay)
            CancellationToken.None
            (fun () -> true)
        |> fun task -> task.GetAwaiter().GetResult() |> ignore)
    |> should throw typeof<TurnLoop.TurnLeaseLostException>

    server.RequestCount |> should equal 0
