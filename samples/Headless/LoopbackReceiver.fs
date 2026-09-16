// SPDX-License-Identifier: Apache-2.0
module Headless.Loopback

open System
open System.Collections.Generic
open System.Net
open System.Net.Sockets
open System.Security.Cryptography
open System.Text
open System.Threading
open System.Threading.Tasks

// Loopback webhook receiver for the headless smoke run: a minimal HTTP
// server over TcpListener on 127.0.0.1 with an ephemeral port
// (cross-platform, no HttpListener URL ACLs, no DNS). Every POST is
// recorded; deliveries carrying a valid X-Legate-Signature prove the
// sink signed the exact bytes (the transferred issue 81 check), and
// Idempotency-Key values deduplicate the sink's at-least-once retries.
// Always answers 200 with "{}" so the sink never retries a received
// delivery; validity is asserted by the host, not the status code.

// ──────────────────────────────────────────────────────────────────────────
// Observed deliveries

/// One observed loopback request: method, path, headers, and raw body.
type ObservedDelivery =
    {
        /// The request method as sent.
        Method: string
        /// The request path as sent.
        Path: string
        /// The request headers, case-insensitive.
        Headers: Dictionary<string, string>
        /// The exact request body bytes the signature covers.
        Body: byte[]
    }

/// A loopback webhook receiver verifying HMAC-SHA256 signatures.
/// <param name="secret">The signing secret bytes. Must not be null or empty.</param>
type LoopbackReceiver(secret: byte[]) =
    do
        if isNull (box secret) || secret.Length = 0 then
            raise (ArgumentException("The loopback receiver needs a non-empty signing secret.", "secret"))

    let listener = new TcpListener(IPAddress.Loopback, 0)
    let lifetime = new CancellationTokenSource()
    let gate = obj ()
    let observed = ResizeArray<ObservedDelivery>()
    let seenKeys = HashSet<string>(StringComparer.Ordinal)

    let verified =
        TaskCompletionSource<ObservedDelivery>(TaskCreationOptions.RunContinuationsAsynchronously)

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

    /// Signs the exact body bytes the way the sink does: sha256=&lt;hex&gt;.
    /// <param name="body">The exact request bytes.</param>
    /// <returns>The expected signature header value.</returns>
    let expectedSignature (body: byte[]) : string =
        use hmac = new HMACSHA256(secret)
        "sha256=" + Convert.ToHexString(hmac.ComputeHash(body)).ToLowerInvariant()

    /// Whether the delivery's signature header matches the exact bytes.
    /// <param name="delivery">The observed delivery.</param>
    /// <returns>True when the signature is present and valid.</returns>
    let isValid (delivery: ObservedDelivery) : bool =
        let mutable header = ""
        let found = delivery.Headers.TryGetValue("X-Legate-Signature", &header)

        if found && not (String.IsNullOrEmpty header) then
            let expected = expectedSignature delivery.Body
            let left = Encoding.UTF8.GetBytes(header.Trim())
            let right = Encoding.UTF8.GetBytes(expected)

            left.Length = right.Length
            && CryptographicOperations.FixedTimeEquals(left, right)
        else
            false

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

                    let mutable lengthText = "0"
                    headers.TryGetValue("Content-Length", &lengthText) |> ignore
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

                    let delivery =
                        {
                            Method = if requestLine.Length > 0 then requestLine[0] else ""
                            Path = if requestLine.Length > 1 then requestLine[1] else ""
                            Headers = headers
                            Body = body
                        }

                    lock gate (fun () ->
                        observed.Add(delivery)

                        if isValid delivery then
                            let mutable key = ""
                            delivery.Headers.TryGetValue("Idempotency-Key", &key) |> ignore

                            if String.IsNullOrEmpty key || seenKeys.Add(key) then
                                verified.TrySetResult(delivery) |> ignore)

                    let responseBytes = Encoding.UTF8.GetBytes("{}")

                    let head =
                        Encoding.ASCII.GetBytes(
                            sprintf
                                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: %d\r\nConnection: close\r\n\r\n"
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

    /// The loopback port the receiver listens on.
    member _.Port = (listener.LocalEndpoint :?> IPEndPoint).Port

    /// The absolute URL for one path on this receiver.
    /// <param name="path">The path to serve the webhook on.</param>
    /// <returns>The absolute loopback URL.</returns>
    member this.Url(path: string) =
        sprintf "http://127.0.0.1:%d%s" this.Port path

    /// How many requests arrived so far, valid or not.
    member _.ObservedCount = lock gate (fun () -> observed.Count)

    /// How many distinct idempotency keys verified so far.
    member _.VerifiedCount = lock gate (fun () -> seenKeys.Count)

    /// Every observed delivery so far, oldest first.
    member _.Observed: ObservedDelivery list = lock gate (fun () -> observed |> List.ofSeq)

    /// Waits for the first signature-valid delivery, event-driven with a
    /// bound: no sleep, no poll of the sender.
    /// <param name="timeout">How long to wait for the delivery.</param>
    /// <returns>Some verified delivery, or None when the bound lapsed.</returns>
    member _.WaitForVerifiedAsync(timeout: TimeSpan) : Task<ObservedDelivery option> =
        task {
            let! winner = Task.WhenAny(verified.Task, Task.Delay(timeout))

            if
                Object.ReferenceEquals(winner, verified.Task)
                && verified.Task.IsCompletedSuccessfully
            then
                return Some verified.Task.Result
            else
                return None
        }

    /// Stops the listener.
    interface IDisposable with
        member _.Dispose() =
            lifetime.Cancel()
            listener.Stop()
            lifetime.Dispose()
