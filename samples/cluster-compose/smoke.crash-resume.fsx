// SPDX-License-Identifier: Apache-2.0
// Cluster crash-resume smoke for the compose harness (issue 145).
// Brings the stack up (sequenced by health gates), opens a session on
// node 1, prompts it with the scripted transport, subscribes to the event
// stream from node 2, stops node 1 mid-turn, and asserts the turn either
// fails or resumes with no duplicated side effects and no gap in the
// subscriber sequence. Opening on node 1 while subscribing from node 2
// also proves the nodes merged: on a split cluster the subscribe 404s
// and the smoke fails loudly instead of passing on singletons.
//
// Turns:CrashResume defaults to Fail while SessionOptions.OnCrashResume
// defaults to ResumeAttempt; the runtime crash path reads the per-session
// knob, so this script accepts either valid outcome (TurnFailedEvent for
// the Fail/FailAttempt path, TurnCompletedEvent for the
// RetryTurn/ResumeAttempt path) and requires one of them plus gap-free,
// duplicate-free sequences. Shard placement is hash-based, so the session
// entity may live on any node: when it lives on the victim the kill
// exercises crash-resume, otherwise the turn completes on the survivors
// and the run still proves node-loss survival plus cross-node observation.
// Either way the printed terminal payload names which path ran.
//
// Honest scope note: MinimalHost seeds no agents and opens sessions with a
// fresh random agent id, so every turn fails fast at agent load (before
// any LLM call, where the reply delay would hold it). The kill therefore
// races dispatch/pickup rather than an in-LLM-call turn: what this proves
// is cross-node inbox recovery (a survivor picks up the pending prompt),
// exactly-once terminal settlement, a gapless subscriber stream, and
// keep-majority survival. A deterministic mid-LLM-call kill needs a seeded
// smoke agent plus open-with-agent-id (a sample-contract change, filed as
// follow-up, not done here).
//
// The scripted reply delay makes the mid-turn kill deterministic: export
// it before running so compose picks it up (compose reads the environment
// at up time):
//
//   LEGATE_MINIMALHOST_REPLY_DELAY_MS=10000 dotnet fsi samples/cluster-compose/smoke.crash-resume.fsx
//
// On success the stack is torn down (clean slate for re-runs); on failure
// it stays up for inspection (`docker compose down -v` to clean).
// A re-run after a killed node 1 needs a fresh `down -v` first: the
// bootstrap node rejoins only itself, so restarting it into a live stack
// splits instead of merging (see docker-compose.yaml).
//
// BCL only: no NuGet restores, no test project.

open System
open System.Collections.Concurrent
open System.Diagnostics
open System.IO
open System.Net.Http
open System.Text
open System.Text.Json
open System.Threading
open System.Threading.Tasks

let node1 =
    Environment.GetEnvironmentVariable("LEGATE_SMOKE_NODE1")
    |> fun v ->
        if String.IsNullOrWhiteSpace(v) then
            "http://127.0.0.1:8081"
        else
            v.Trim()

let node2 =
    Environment.GetEnvironmentVariable("LEGATE_SMOKE_NODE2")
    |> fun v ->
        if String.IsNullOrWhiteSpace(v) then
            "http://127.0.0.1:8082"
        else
            v.Trim()

let node3 =
    Environment.GetEnvironmentVariable("LEGATE_SMOKE_NODE3")
    |> fun v ->
        if String.IsNullOrWhiteSpace(v) then
            "http://127.0.0.1:8083"
        else
            v.Trim()

let composeDir = Path.Combine(__SOURCE_DIRECTORY__, ".")

let json =
    JsonSerializerOptions(PropertyNamingPolicy = JsonNamingPolicy.CamelCase, PropertyNameCaseInsensitive = true)

let mutable stackTornDown = false

let fail (message: string) =
    eprintfn "smoke FAILED: %s" message
    eprintfn "smoke: stack left up for inspection; clean with: docker compose down -v"
    exit 1

let info (message: string) = printfn "smoke: %s" message

let runCompose (args: string) (timeoutMinutes: int) =
    info (sprintf "docker compose %s" args)

    // Streams the child output straight to this console: redirecting the
    // build output through a pipe deadlocks once the pipe buffer fills
    // (dotnet publish logs megabytes), hanging WaitForExit forever.
    let psi =
        ProcessStartInfo(
            "docker",
            sprintf "compose %s" args,
            WorkingDirectory = composeDir,
            RedirectStandardOutput = false,
            RedirectStandardError = false,
            UseShellExecute = false
        )

    use proc = Process.Start(psi)

    if not (proc.WaitForExit(TimeSpan.FromMinutes(float timeoutMinutes))) then
        fail (sprintf "docker compose %s timed out after %d minutes" args timeoutMinutes)

    if proc.ExitCode <> 0 then
        fail (sprintf "docker compose %s exited %d" args proc.ExitCode)

let client = new HttpClient(Timeout = TimeSpan.FromSeconds(120.0))

let getStatus (baseUrl: string) (path: string) : Async<int * string> =
    async {
        try
            use! response = client.GetAsync(sprintf "%s%s" baseUrl path) |> Async.AwaitTask
            let! body = response.Content.ReadAsStringAsync() |> Async.AwaitTask
            return (int response.StatusCode, body.Trim())
        with ex ->
            return (-1, ex.Message)
    }

let waitReady (minutes: float) =
    info "waiting for all three nodes /ready"
    let deadline = DateTimeOffset.UtcNow.AddMinutes(minutes)

    let rec loop () =
        async {
            let! results =
                [ node1; node2; node3 ]
                |> List.map (fun baseUrl -> getStatus baseUrl "/ready")
                |> Async.Parallel

            let allOk = results |> Array.forall (fun (code, _) -> code = 200)

            if allOk then
                info "all three nodes report ready"
                return ()
            elif DateTimeOffset.UtcNow > deadline then
                fail (sprintf "nodes never became ready: %A" results)
                return ()
            else
                do! Async.Sleep(5000)
                return! loop ()
        }

    Async.RunSynchronously(loop ())

let postJson (baseUrl: string) (path: string) (payload: string) : Async<JsonDocument> =
    async {
        use content = new StringContent(payload, Encoding.UTF8, "application/json")
        use! response = client.PostAsync(sprintf "%s%s" baseUrl path, content) |> Async.AwaitTask
        let! body = response.Content.ReadAsStringAsync() |> Async.AwaitTask

        if not response.IsSuccessStatusCode then
            fail (sprintf "POST %s%s -> %d: %s" baseUrl path (int response.StatusCode) body)

        return JsonDocument.Parse(body)
    }

// The reply delay is read by the containers at up time: without it the
// turn settles before the kill and the smoke proves nothing.
match Environment.GetEnvironmentVariable("LEGATE_MINIMALHOST_REPLY_DELAY_MS") with
| null -> fail "set LEGATE_MINIMALHOST_REPLY_DELAY_MS=10000 in this shell before running the smoke"
| raw ->
    let mutable value = 0

    if not (Int32.TryParse(raw.Trim(), &value) && value > 0) then
        fail "LEGATE_MINIMALHOST_REPLY_DELAY_MS must be a positive integer of milliseconds (e.g. 10000)"

runCompose "up --build -d" 20
waitReady 10.0

info (sprintf "opening session on %s" node1)

let openDoc =
    postJson node1 "/sessions" """{"title":"smoke crash-resume"}"""
    |> Async.RunSynchronously

let sessionId = openDoc.RootElement.GetProperty("sessionId").GetString()

if String.IsNullOrWhiteSpace(sessionId) then
    fail "open returned no sessionId"

info (sprintf "session %s" sessionId)

// Subscribe from node 2 before prompting so the replay covers the whole
// turn. A split cluster fails here (unknown session on node 2) instead
// of passing on singletons.
let seenIds = ConcurrentQueue<int64>()
let seenEvents = ConcurrentQueue<string>()
let seenPayloads = ConcurrentQueue<string * string>()
let subscriberCts = new CancellationTokenSource()
let subscriberDone = TaskCompletionSource<bool>()

let subscribeTask =
    task {
        try
            use request =
                new HttpRequestMessage(HttpMethod.Get, sprintf "%s/sessions/%s/events" node2 sessionId)

            use! response = client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, subscriberCts.Token)
            response.EnsureSuccessStatusCode() |> ignore
            use! stream = response.Content.ReadAsStreamAsync(subscriberCts.Token)
            use reader = new StreamReader(stream, Encoding.UTF8)
            let mutable currentId = ""
            let mutable currentEvent = ""
            let mutable currentData = ""

            while not subscriberCts.IsCancellationRequested do
                let! line = reader.ReadLineAsync(subscriberCts.Token)

                if isNull line then
                    subscriberDone.TrySetResult(true) |> ignore
                else
                    let trimmed = line.Trim()

                    if trimmed.StartsWith("id:", StringComparison.Ordinal) then
                        currentId <- trimmed.Substring(3).Trim()
                    elif trimmed.StartsWith("event:", StringComparison.Ordinal) then
                        currentEvent <- trimmed.Substring(6).Trim()
                    elif trimmed.StartsWith("data:", StringComparison.Ordinal) then
                        currentData <- trimmed.Substring(5).Trim()
                    elif trimmed = "" then
                        if not (String.IsNullOrWhiteSpace(currentEvent)) then
                            seenEvents.Enqueue(currentEvent)
                            seenPayloads.Enqueue((currentEvent, currentData))

                            match Int64.TryParse(currentId) with
                            | true, value -> seenIds.Enqueue(value)
                            | _ -> ()

                            if currentEvent = "SessionClosedEvent" then
                                subscriberDone.TrySetResult(true) |> ignore

                        currentId <- ""
                        currentEvent <- ""
                        currentData <- ""
                    else
                        ()
        with
        | :? OperationCanceledException -> subscriberDone.TrySetResult(true) |> ignore
        | ex -> subscriberDone.TrySetException(ex) |> ignore
    }

info (sprintf "prompting session from %s (scripted reply)" node1)

let promptTask =
    postJson node1 (sprintf "/sessions/%s/prompt" sessionId) """{"text":"smoke mid-turn kill","delivery":"queue"}"""
    |> Async.StartAsTask

// Kill node 1 the moment the prompt appends: the turn has not run yet
// (dispatch polls every few seconds), so a survivor must pick the pending
// inbox entry up cross-node. Any sleep here only lets the victim process
// the turn itself; the recovery path under test is survivor pickup.
info "stopping legate-1 the moment the prompt appends"
runCompose "stop legate-1" 5 |> ignore

let promptResult =
    try
        promptTask.Wait(TimeSpan.FromSeconds(60.0)) |> ignore

        if promptTask.IsCompletedSuccessfully then
            Some promptTask.Result
        else
            None
    with _ ->
        None

match promptResult with
| Some _ -> info "prompt append acknowledged"
| None ->
    info "prompt append did not acknowledge after the kill (node 1 was already down); continuing on the subscriber"

// Observe from node 2 until a terminal turn event or the timeout.
let deadline = DateTimeOffset.UtcNow.AddMinutes(4.0)
let mutable terminal: string option = None

while terminal.IsNone && DateTimeOffset.UtcNow < deadline do
    Thread.Sleep(2000)

    for name in seenEvents do
        if name = "TurnFailedEvent" || name = "TurnCompletedEvent" then
            terminal <- Some name

match terminal with
| Some name -> info (sprintf "terminal turn event: %s" name)
| None ->
    // Fall back to the replay cursor: the journal is shared (Postgres), so a
    // replay from node 2 must still show the turn outcome even if the live
    // stream ended at the kill.
    info "no terminal event on the live stream yet; checking replay from node 2"
    Thread.Sleep(5000)

    for name in seenEvents do
        if name = "TurnFailedEvent" || name = "TurnCompletedEvent" then
            terminal <- Some name

match terminal with
| None -> fail "no TurnFailedEvent or TurnCompletedEvent observed from node 2 within the timeout"
| Some name ->
    for (eventName, payload) in seenPayloads do
        if eventName = name then
            info (sprintf "terminal payload: %s" payload)

    let terminalPayload =
        seenPayloads.ToArray()
        |> Array.tryFind (fun (eventName, _) -> eventName = name)
        |> Option.map snd
        |> Option.defaultValue ""

    if name = "TurnFailedEvent" then
        if terminalPayload.Contains("No agent ") then
            info
                "turn failed at agent load (MinimalHost seeds no agents): the kill raced dispatch, proving cross-node pickup, not an LLM-mid-call resume"
        else
            info "crash path: Fail/FailAttempt (Turns:CrashResume=Fail default)"
    else
        info "crash path: RetryTurn/ResumeAttempt (per-session resume)"

// No-gap, no-dup over the subscriber sequence.
let ids = seenIds.ToArray()

if ids.Length = 0 then
    fail "subscriber saw no sequenced frames"

let sorted = ids |> Array.sort
let dups = sorted |> Array.pairwise |> Array.exists (fun (a, b) -> a = b)

if dups then
    fail (sprintf "subscriber saw duplicated sequence ids: %A" ids)

let gaps = sorted |> Array.pairwise |> Array.filter (fun (a, b) -> b <> a + 1L)

if gaps.Length > 0 then
    fail (sprintf "subscriber saw sequence gaps: %A (all=%A)" gaps ids)

info (
    sprintf
        "subscriber saw %d frames, sequences %d..%d gap-free and duplicate-free"
        ids.Length
        sorted[0]
        sorted[sorted.Length - 1]
)

// Side-effect dedup: the scripted transport runs no tools, so the check is
// sequence-level (no redelivered id) plus the single terminal outcome above.
// A resumed turn is one new attempt; a failed turn settles once.
let terminals =
    seenEvents.ToArray()
    |> Array.filter (fun n -> n = "TurnFailedEvent" || n = "TurnCompletedEvent")

if terminals.Length > 1 then
    // One retry plus one settle can legitimately emit both across attempts;
    // more than two terminal frames means a duplicated settle.
    if terminals.Length > 2 then
        fail (sprintf "too many terminal turn frames (possible duplicated settle): %A" terminals)
    else
        info (sprintf "note: both terminal frames observed across attempts: %A" terminals)
else
    info "single terminal turn frame: no duplicated settle"

subscriberCts.Cancel()

try
    subscriberDone.Task.Wait(TimeSpan.FromSeconds(10.0)) |> ignore
with _ ->
    ()

// The survivors (2 of 3, a keep-majority quorum) must still serve. The
// kill leaves the victim unreachable, so readiness flips until the
// split-brain resolver downs it (stable-after 20 s): poll /ready on both
// survivors first, then prove the turn path with a fresh open and prompt.
info "checking survivors still serve (polling past the downing window)"

let survivorDeadline = DateTimeOffset.UtcNow.AddMinutes(3.0)

let rec pollSurvivors () =
    async {
        let! results =
            [ node2; node3 ]
            |> List.map (fun baseUrl -> getStatus baseUrl "/ready")
            |> Async.Parallel

        if results |> Array.forall (fun (code, _) -> code = 200) then
            return ()
        elif DateTimeOffset.UtcNow > survivorDeadline then
            fail (sprintf "survivors never recovered ready after the kill: %A" results)
            return ()
        else
            do! Async.Sleep(5000)
            return! pollSurvivors ()
    }

Async.RunSynchronously(pollSurvivors ())

let survivorOpen =
    postJson node2 "/sessions" """{"title":"smoke survivor"}"""
    |> Async.RunSynchronously

let survivorId = survivorOpen.RootElement.GetProperty("sessionId").GetString()

if String.IsNullOrWhiteSpace(survivorId) then
    fail "survivor open returned no sessionId"

postJson node2 (sprintf "/sessions/%s/prompt" survivorId) """{"text":"survivor check","delivery":"queue"}"""
|> Async.RunSynchronously
|> ignore

info "survivors serve: ready on node 2 and 3, open and prompt ack on node 2"

runCompose "down -v" 5 |> ignore
stackTornDown <- true

info "smoke PASSED"
