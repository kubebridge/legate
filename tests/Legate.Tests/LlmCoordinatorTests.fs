// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.LlmCoordinatorTests

open System
open System.Collections.Generic
open System.Net.Http
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Legate.Testing
open Microsoft.Extensions.AI
open Microsoft.Extensions.Time.Testing
open Xunit

// Local per-identity coordinator (issue 54): the registry resolves by
// provider segment, and the coordinator enforces concurrency, RPM/TPM
// windows, the capped FIFO queue, the shared 429 cooldown, jittered
// transient retry, and the turn deadline. Every wait runs off the injected
// seams, so no fact sleeps: capacity waits ride the admission pulse,
// rate/cooldown/backoff waits ride a clock-backed recording delay (the test
// advances the FakeTimeProvider past them), backoff values ride
// RecordingDelay (immediate) plus SeededRandom, and the deadline rides a
// one-shot off the injected clock.

type private StringOutcome = LlmCoordination.LlmCallOutcome<string>

/// A chat client the coordinator hands to invoke lambdas; most doubles
/// ignore it and script on closures, while the wiring fact asserts the
/// instance is the provider's own.
type StubChatClient() =
    interface IChatClient with
        member _.GetResponseAsync(_, _, _) = raise (NotImplementedException())
        member _.GetStreamingResponseAsync(_, _, _) = raise (NotImplementedException())
        member _.GetService(_, _) = null
        member _.Dispose() = ()

/// A provider that counts client builds: missing-key fail-fast must leave
/// the count at zero, and wiring must hand invoke the built instance.
type StubProvider(id: string, defaultModel: string) =
    let gate = obj ()
    let mutable builds = 0
    let clients = ResizeArray<IChatClient>()

    member _.Builds = lock gate (fun () -> builds)

    member _.Clients = lock gate (fun () -> clients |> List.ofSeq)

    interface ILlmProvider with
        member _.Id = id
        member _.DefaultModel = defaultModel

        member _.Capabilities =
            {
                Streaming = false
                Reasoning = false
                ToolCalling = false
            }

        member _.CreateChatClient(_model, _options) =
            lock gate (fun () ->
                builds <- builds + 1
                let client = new StubChatClient() :> IChatClient
                clients.Add(client)
                client)

/// An API key provider backed by a (providerId, tenantValue) table.
/// Missing entries resolve to null, like a host with no key for the pair.
type TableKeyProvider(keys: Map<string * string, string>) =
    interface IApiKeyProvider with
        member _.GetApiKey(providerId, tenantId) =
            Map.tryFind (providerId, tenantId.Value) keys |> Option.toObj

/// An ILlmDelay that never elapses unless its token fires: capacity waits
/// ride the admission pulse, so facts that never need a timed wait run
/// without one.
type NeverDelay() =
    interface ILlmDelay with
        member _.Delay(_, cancellationToken) =
            Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken)

/// An ILlmDelay that records every requested wait and waits on the given
/// clock: under a FakeTimeProvider the wait completes when the fact
/// advances past it, so windows, cooldowns, and backoffs stay
/// deterministic without sleeping.
type RecordingClockDelay(clock: TimeProvider, recorded: ResizeArray<TimeSpan>) =
    interface ILlmDelay with
        member _.Delay(requested, cancellationToken) =
            recorded.Add(requested)
            Task.Delay(requested, clock, cancellationToken)

/// One scripted attempt: a success with usage, or a failure to classify.
type PlannedOutcome =
    | SucceedWith of UsageSummary * string
    | FailWith of exn

/// Runs a plan of scripted outcomes in attempt order, recording attempt
/// numbers. Thread-safe: concurrent identities may share a coordinator.
let private runPlan (plan: ResizeArray<PlannedOutcome>) (attempts: ResizeArray<int>) =
    let gate = obj ()

    Func<IChatClient, CancellationToken, Task<StringOutcome>>(fun _ _ ->
        let step =
            lock gate (fun () ->
                attempts.Add(attempts.Count + 1)

                if plan.Count = 0 then
                    failwith "The plan ran out of scripted outcomes."

                let head = plan[0]
                plan.RemoveAt(0)
                head)

        match step with
        | SucceedWith(usage, value) -> Task.FromResult({ Value = value; Usage = usage })
        | FailWith failure -> Task.FromException<StringOutcome>(failure))

/// Tracks concurrent invoke entries: the peak proves caps and isolation.
type ParallelTracker() =
    let gate = obj ()
    let mutable active = 0
    let mutable peak = 0

    member _.Enter() =
        lock gate (fun () ->
            active <- active + 1
            peak <- max peak active)

    member _.Exit() =
        lock gate (fun () -> active <- active - 1)

    member _.Peak = lock gate (fun () -> peak)

/// An invoke blocked on the given completion source, observing cancellation
/// so deadline facts settle instead of hanging.
let private gatedInvoke (tracker: ParallelTracker) (gate: TaskCompletionSource<StringOutcome>) =
    Func<IChatClient, CancellationToken, Task<StringOutcome>>(fun _ cancellationToken ->
        task {
            tracker.Enter()

            try
                let! outcome = gate.Task.WaitAsync(cancellationToken)
                return outcome
            finally
                tracker.Exit()
        })

/// An invoke that never completes unless its token fires: the running-call
/// deadline fact.
let private hangingInvoke (tracker: ParallelTracker) =
    Func<IChatClient, CancellationToken, Task<StringOutcome>>(fun _ cancellationToken ->
        task {
            tracker.Enter()

            try
                do! Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken)

                let late: StringOutcome =
                    {
                        Value = "late"
                        Usage = { InputTokens = 0L; OutputTokens = 0L }
                    }

                return late
            finally
                tracker.Exit()
        })

/// An invoke that succeeds immediately with the given usage.
let private instantInvoke (tracker: ParallelTracker) (usage: UsageSummary) (value: string) =
    Func<IChatClient, CancellationToken, Task<StringOutcome>>(fun _ _ ->
        tracker.Enter()
        tracker.Exit()

        Task.FromResult({ Value = value; Usage = usage }))

let tenantA = TenantId.Create "tenant-a"
let tenantB = TenantId.Create "tenant-b"

let usageOf (inputTokens: int64) (outputTokens: int64) : UsageSummary =
    {
        InputTokens = inputTokens
        OutputTokens = outputTokens
    }

let private outcomeOf (value: string) (inputTokens: int64) (outputTokens: int64) : StringOutcome =
    {
        Value = value
        Usage = usageOf inputTokens outputTokens
    }

let providerFailure (status: int) : ProviderException =
    ProviderException("acme", Nullable status, Nullable<TimeSpan>(), sprintf "Status %d." status)

let providerFailureWithRetryAfter (status: int) (retryAfter: TimeSpan) : ProviderException =
    ProviderException("acme", Nullable status, Nullable retryAfter, sprintf "Status %d." status)

let makeOptions
    (setupCoordination: LlmCoordinationOptions -> unit)
    (staticKeys: (string * string) list)
    (distributed: bool)
    : LlmOptions =
    let coordination = LlmCoordinationOptions()
    setupCoordination coordination
    let options = LlmOptions()
    options.Coordination <- coordination

    for providerId, key in staticKeys do
        options.Providers.Add(providerId, LlmProviderOptions(ApiKey = key))

    options.DistributedCoordination <- distributed
    options

let makeRegistry (providers: ILlmProvider list) : ILlmProviderRegistry =
    let registry = LlmCoordination.LlmProviderRegistry()

    for provider in providers do
        registry.Register(provider)

    registry :> ILlmProviderRegistry

let private makeCoordinator
    (options: LlmOptions)
    (registry: ILlmProviderRegistry)
    (keys: Map<string * string, string>)
    (clock: TimeProvider)
    (delay: ILlmDelay)
    (random: ILlmRandom)
    : LlmCoordination.LlmCoordinator =
    LlmCoordination.LlmCoordinator(options, registry, TableKeyProvider(keys) :> IApiKeyProvider, clock, delay, random)

let private execute
    (coordinator: LlmCoordination.LlmCoordinator)
    (reference: string)
    (tenant: TenantId)
    (estimate: int64)
    (invoke: Func<IChatClient, CancellationToken, Task<StringOutcome>>)
    : Task<string> =
    coordinator.ExecuteAsync(ModelReference.Parse(reference), tenant, estimate, invoke, CancellationToken.None)

let private executeWithToken
    (coordinator: LlmCoordination.LlmCoordinator)
    (reference: string)
    (tenant: TenantId)
    (estimate: int64)
    (invoke: Func<IChatClient, CancellationToken, Task<StringOutcome>>)
    (cancellationToken: CancellationToken)
    : Task<string> =
    coordinator.ExecuteAsync(ModelReference.Parse(reference), tenant, estimate, invoke, cancellationToken)

// ───────────────────────────────────────────────────────────────────────────
// Registry

[<Fact>]
let ``Registry resolves by provider segment and rejects unknown providers`` () =
    let acme = StubProvider("acme", "fast") :> ILlmProvider
    let other = StubProvider("other", "mini") :> ILlmProvider
    let registry = makeRegistry [ other; acme ]

    let resolved = registry.Resolve(ModelReference.Parse("acme/fast"))
    resolved |> should equal acme

    registry.RegisteredProviders |> List.ofSeq |> should equal [ "acme"; "other" ]

    try
        registry.Resolve(ModelReference.Parse("missing/model")) |> ignore
        failwith "expected ProviderNotRegisteredException"
    with :? ProviderNotRegisteredException as missing ->
        missing.ProviderId |> should equal "missing"
        missing.Message.Contains("acme") |> should equal true
        missing.Message.Contains("sk-") |> should equal false

[<Fact>]
let ``Registry validates registrations and replaces duplicates`` () =
    let registry = LlmCoordination.LlmProviderRegistry()

    (fun () -> registry.Register(Unchecked.defaultof<ILlmProvider>) |> ignore)
    |> should throw typeof<ArgumentNullException>

    let first = StubProvider("acme", "fast") :> ILlmProvider
    let second = StubProvider("acme", "faster") :> ILlmProvider
    registry.Register(first)
    registry.Register(second)

    (registry :> ILlmProviderRegistry).Resolve(ModelReference.Parse("acme/fast"))
    |> should equal second

// ───────────────────────────────────────────────────────────────────────────
// Identity scope

[<Fact>]
let ``Static scope shares one bucket across tenants`` () =
    let provider = StubProvider("acme", "fast")
    let registry = makeRegistry [ provider :> ILlmProvider ]

    let options =
        makeOptions (fun coordination -> coordination.MaxConcurrentRequests <- 1) [ "acme", "sk-static" ] false

    let clock = FakeTimeProvider()
    let tracker = ParallelTracker()

    let coordinator =
        makeCoordinator options registry Map.empty clock (NeverDelay() :> ILlmDelay) (SeededRandom(1) :> ILlmRandom)

    let gate = new TaskCompletionSource<StringOutcome>()
    let taskA = execute coordinator "acme/fast" tenantA 10L (gatedInvoke tracker gate)
    taskA.IsCompleted |> should equal false

    let taskB =
        execute coordinator "acme/fast" tenantB 10L (instantInvoke tracker (usageOf 1L 1L) "b")

    taskB.IsCompleted |> should equal false

    gate.SetResult(outcomeOf "a" 1L 1L)
    taskA.GetAwaiter().GetResult() |> should equal "a"
    taskB.GetAwaiter().GetResult() |> should equal "b"
    tracker.Peak |> should equal 1

[<Fact>]
let ``Per-tenant scopes isolate buckets`` () =
    let provider = StubProvider("acme", "fast")
    let registry = makeRegistry [ provider :> ILlmProvider ]

    let options =
        makeOptions (fun coordination -> coordination.MaxConcurrentRequests <- 1) [] false

    let keys =
        Map.ofList
            [
                (("acme", "tenant-a"), "key-a")
                (("acme", "tenant-b"), "key-b")
            ]

    let clock = FakeTimeProvider()
    let tracker = ParallelTracker()

    let coordinator =
        makeCoordinator options registry keys clock (NeverDelay() :> ILlmDelay) (SeededRandom(1) :> ILlmRandom)

    let gate = new TaskCompletionSource<StringOutcome>()
    let taskA = execute coordinator "acme/fast" tenantA 10L (gatedInvoke tracker gate)
    taskA.IsCompleted |> should equal false

    let taskB =
        execute coordinator "acme/fast" tenantB 10L (instantInvoke tracker (usageOf 1L 1L) "b")

    taskB.GetAwaiter().GetResult() |> should equal "b"
    taskA.IsCompleted |> should equal false

    gate.SetResult(outcomeOf "a" 1L 1L)
    taskA.GetAwaiter().GetResult() |> should equal "a"
    tracker.Peak |> should equal 2

[<Fact>]
let ``Invoke receives the chat client the provider built`` () =
    let provider = StubProvider("acme", "fast")
    let registry = makeRegistry [ provider :> ILlmProvider ]
    let options = makeOptions ignore [ "acme", "sk-static" ] false
    let clock = FakeTimeProvider()
    let tracker = ParallelTracker()

    let coordinator =
        makeCoordinator options registry Map.empty clock (NeverDelay() :> ILlmDelay) (SeededRandom(1) :> ILlmRandom)

    let mutable seen: IChatClient = Unchecked.defaultof<IChatClient>

    let invoke =
        Func<IChatClient, CancellationToken, Task<StringOutcome>>(fun client _ ->
            seen <- client
            Task.FromResult(outcomeOf "ok" 1L 1L))

    execute coordinator "acme/fast" tenantA 10L invoke
    |> fun task -> task.GetAwaiter().GetResult() |> ignore

    provider.Builds |> should equal 1
    seen |> should equal (provider.Clients |> List.head)

    tracker.Peak |> should equal 0

// ───────────────────────────────────────────────────────────────────────────
// Concurrency and the FIFO queue

[<Fact>]
let ``Queue full rejects with AdmissionRejectedException`` () =
    let provider = StubProvider("acme", "fast")
    let registry = makeRegistry [ provider :> ILlmProvider ]

    let options =
        makeOptions
            (fun coordination ->
                coordination.MaxConcurrentRequests <- 1
                coordination.MaxQueuedRequests <- 1)
            [ "acme", "sk-static" ]
            false

    let clock = FakeTimeProvider()
    let tracker = ParallelTracker()

    let coordinator =
        makeCoordinator options registry Map.empty clock (NeverDelay() :> ILlmDelay) (SeededRandom(1) :> ILlmRandom)

    let gate = new TaskCompletionSource<StringOutcome>()
    let taskA = execute coordinator "acme/fast" tenantA 10L (gatedInvoke tracker gate)
    taskA.IsCompleted |> should equal false

    let taskB =
        execute coordinator "acme/fast" tenantA 10L (instantInvoke tracker (usageOf 1L 1L) "b")

    taskB.IsCompleted |> should equal false

    let taskC =
        execute coordinator "acme/fast" tenantA 10L (instantInvoke tracker (usageOf 1L 1L) "c")

    let rejection =
        try
            taskC.GetAwaiter().GetResult() |> ignore
            None
        with :? AdmissionRejectedException as rejected ->
            Some rejected

    rejection.IsSome |> should equal true
    rejection.Value.Reason |> should equal "queueFull"
    rejection.Value.Message.Contains("acme/static") |> should equal true
    rejection.Value.Message.Contains("sk-static") |> should equal false

    gate.SetResult(outcomeOf "a" 1L 1L)
    taskA.GetAwaiter().GetResult() |> should equal "a"
    taskB.GetAwaiter().GetResult() |> should equal "b"
    tracker.Peak |> should equal 1

[<Fact>]
let ``Caller abort while queued propagates instead of settling`` () =
    let provider = StubProvider("acme", "fast")
    let registry = makeRegistry [ provider :> ILlmProvider ]

    let options =
        makeOptions (fun coordination -> coordination.MaxConcurrentRequests <- 1) [ "acme", "sk-static" ] false

    let clock = FakeTimeProvider()
    let tracker = ParallelTracker()

    let coordinator =
        makeCoordinator options registry Map.empty clock (NeverDelay() :> ILlmDelay) (SeededRandom(1) :> ILlmRandom)

    let gate = new TaskCompletionSource<StringOutcome>()
    let taskA = execute coordinator "acme/fast" tenantA 10L (gatedInvoke tracker gate)
    taskA.IsCompleted |> should equal false

    use aborted = new CancellationTokenSource()

    let taskB =
        executeWithToken coordinator "acme/fast" tenantA 10L (instantInvoke tracker (usageOf 1L 1L) "b") aborted.Token

    taskB.IsCompleted |> should equal false
    aborted.Cancel()

    (fun () -> taskB.GetAwaiter().GetResult() |> ignore)
    |> should throw typeof<OperationCanceledException>

    gate.SetResult(outcomeOf "a" 1L 1L)
    taskA.GetAwaiter().GetResult() |> should equal "a"

// ───────────────────────────────────────────────────────────────────────────
// RPM and TPM windows

[<Fact>]
let ``RPM window throttles on the clock`` () =
    let provider = StubProvider("acme", "fast")
    let registry = makeRegistry [ provider :> ILlmProvider ]

    let options =
        makeOptions (fun coordination -> coordination.RequestsPerMinute <- 1) [ "acme", "sk-static" ] false

    let clock = FakeTimeProvider()
    let recorded = ResizeArray<TimeSpan>()
    let tracker = ParallelTracker()

    let coordinator =
        makeCoordinator
            options
            registry
            Map.empty
            clock
            (RecordingClockDelay(clock, recorded) :> ILlmDelay)
            (SeededRandom(1) :> ILlmRandom)

    execute coordinator "acme/fast" tenantA 10L (instantInvoke tracker (usageOf 10L 10L) "a")
    |> fun task -> task.GetAwaiter().GetResult() |> should equal "a"

    let taskB =
        execute coordinator "acme/fast" tenantA 10L (instantInvoke tracker (usageOf 10L 10L) "b")

    taskB.IsCompleted |> should equal false
    recorded |> List.ofSeq |> should equal [ TimeSpan.FromMinutes 1.0 ]

    clock.Advance(TimeSpan.FromMinutes 1.0)
    taskB.GetAwaiter().GetResult() |> should equal "b"

[<Fact>]
let ``TPM reservation blocks until reconciliation frees budget`` () =
    let provider = StubProvider("acme", "fast")
    let registry = makeRegistry [ provider :> ILlmProvider ]

    let options =
        makeOptions
            (fun coordination ->
                coordination.TokensPerMinute <- Nullable 150
                coordination.EstimatedOutputTokens <- 10)
            [ "acme", "sk-static" ]
            false

    let clock = FakeTimeProvider()
    let recorded = ResizeArray<TimeSpan>()
    let tracker = ParallelTracker()

    let coordinator =
        makeCoordinator
            options
            registry
            Map.empty
            clock
            (RecordingClockDelay(clock, recorded) :> ILlmDelay)
            (SeededRandom(1) :> ILlmRandom)

    let gate = new TaskCompletionSource<StringOutcome>()
    let taskA = execute coordinator "acme/fast" tenantA 90L (gatedInvoke tracker gate)
    taskA.IsCompleted |> should equal false

    let taskB =
        execute coordinator "acme/fast" tenantA 90L (instantInvoke tracker (usageOf 7L 3L) "b")

    taskB.IsCompleted |> should equal false
    recorded |> List.ofSeq |> should equal [ TimeSpan.FromMinutes 1.0 ]

    gate.SetResult(outcomeOf "a" 10L 5L)
    taskA.GetAwaiter().GetResult() |> should equal "a"
    taskB.GetAwaiter().GetResult() |> should equal "b"
    recorded |> List.ofSeq |> should equal [ TimeSpan.FromMinutes 1.0 ]

[<Fact>]
let ``TPM window slides on the clock`` () =
    let provider = StubProvider("acme", "fast")
    let registry = makeRegistry [ provider :> ILlmProvider ]

    let options =
        makeOptions
            (fun coordination ->
                coordination.TokensPerMinute <- Nullable 100
                coordination.EstimatedOutputTokens <- 10)
            [ "acme", "sk-static" ]
            false

    let clock = FakeTimeProvider()
    let recorded = ResizeArray<TimeSpan>()
    let tracker = ParallelTracker()

    let coordinator =
        makeCoordinator
            options
            registry
            Map.empty
            clock
            (RecordingClockDelay(clock, recorded) :> ILlmDelay)
            (SeededRandom(1) :> ILlmRandom)

    execute coordinator "acme/fast" tenantA 0L (instantInvoke tracker (usageOf 60L 40L) "a")
    |> fun task -> task.GetAwaiter().GetResult() |> should equal "a"

    let taskB =
        execute coordinator "acme/fast" tenantA 0L (instantInvoke tracker (usageOf 1L 1L) "b")

    taskB.IsCompleted |> should equal false
    recorded |> List.ofSeq |> should equal [ TimeSpan.FromMinutes 1.0 ]

    clock.Advance(TimeSpan.FromMinutes 1.0)
    taskB.GetAwaiter().GetResult() |> should equal "b"

[<Fact>]
let ``TPM reservation is the deterministic estimate plus the output budget`` () =
    let provider = StubProvider("acme", "fast")
    let registry = makeRegistry [ provider :> ILlmProvider ]

    let cells =
        ResizeArray<SessionCell>(
            [
                {
                    Id = CellId.New()
                    SessionId = SessionId.New()
                    TurnId = TurnId.New()
                    Kind = SessionCellKind.User
                    Content = String('u', 400)
                    ToolName = null
                    ToolCallId = null
                    IsError = false
                    Iteration = 0
                    Metadata = null
                    Artifacts = null
                    Timestamp = DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero)
                }
                {
                    Id = CellId.New()
                    SessionId = SessionId.New()
                    TurnId = TurnId.New()
                    Kind = SessionCellKind.Assistant
                    Content = String('a', 100)
                    ToolName = null
                    ToolCallId = null
                    IsError = false
                    Iteration = 1
                    Metadata = null
                    Artifacts = null
                    Timestamp = DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero)
                }
            ]
        )
        :> IReadOnlyList<SessionCell>

    let estimate = ContextPruning.Estimate(cells)
    estimate |> should be (greaterThan 10L)

    // Twice the estimate plus one output budget: the second concurrent
    // call fits only when the reservation omits the output budget, so a
    // queued second call proves the budget is reserved.
    let outputBudget = 20

    let options =
        makeOptions
            (fun coordination ->
                coordination.TokensPerMinute <- Nullable(int (2L * estimate + int64 outputBudget))
                coordination.EstimatedOutputTokens <- outputBudget)
            [ "acme", "sk-static" ]
            false

    let clock = FakeTimeProvider()
    let recorded = ResizeArray<TimeSpan>()
    let tracker = ParallelTracker()

    let coordinator =
        makeCoordinator
            options
            registry
            Map.empty
            clock
            (RecordingClockDelay(clock, recorded) :> ILlmDelay)
            (SeededRandom(1) :> ILlmRandom)

    let gate = new TaskCompletionSource<StringOutcome>()

    let taskA =
        execute coordinator "acme/fast" tenantA estimate (gatedInvoke tracker gate)

    taskA.IsCompleted |> should equal false

    let taskB =
        execute coordinator "acme/fast" tenantA estimate (instantInvoke tracker (usageOf 5L 5L) "b")

    taskB.IsCompleted |> should equal false
    recorded |> List.ofSeq |> should equal [ TimeSpan.FromMinutes 1.0 ]

    gate.SetResult(outcomeOf "a" 5L 5L)
    taskA.GetAwaiter().GetResult() |> should equal "a"
    taskB.GetAwaiter().GetResult() |> should equal "b"

// ───────────────────────────────────────────────────────────────────────────
// Shared 429 cooldown

[<Fact>]
let ``HTTP 429 sets the shared cooldown pause`` () =
    let provider = StubProvider("acme", "fast")
    let registry = makeRegistry [ provider :> ILlmProvider ]

    let options =
        makeOptions (fun coordination -> coordination.RetryCount <- 0) [ "acme", "sk-static" ] false

    let clock = FakeTimeProvider()
    let recorded = ResizeArray<TimeSpan>()
    let tracker = ParallelTracker()

    let coordinator =
        makeCoordinator
            options
            registry
            Map.empty
            clock
            (RecordingClockDelay(clock, recorded) :> ILlmDelay)
            (SeededRandom(1) :> ILlmRandom)

    let plan = ResizeArray<PlannedOutcome>([ FailWith(providerFailure 429) ])
    let attempts = ResizeArray<int>()

    (fun () ->
        execute coordinator "acme/fast" tenantA 10L (runPlan plan attempts)
        |> fun task -> task.GetAwaiter().GetResult() |> ignore)
    |> should throw typeof<ProviderException>

    attempts |> List.ofSeq |> should equal [ 1 ]
    recorded.Count |> should equal 0

    let taskB =
        execute coordinator "acme/fast" tenantA 10L (instantInvoke tracker (usageOf 1L 1L) "b")

    taskB.IsCompleted |> should equal false
    recorded |> List.ofSeq |> should equal [ TimeSpan.FromSeconds 30.0 ]

    clock.Advance(TimeSpan.FromSeconds 30.0)
    taskB.GetAwaiter().GetResult() |> should equal "b"

[<Fact>]
let ``Retry-After outlasting the pause sets the cooldown per identity`` () =
    let acme = StubProvider("acme", "fast")
    let other = StubProvider("other", "mini")

    let registry =
        makeRegistry
            [
                acme :> ILlmProvider
                other :> ILlmProvider
            ]

    let options =
        makeOptions
            (fun coordination ->
                coordination.RetryCount <- 0
                coordination.RequestTimeoutSeconds <- 600)
            [
                "acme", "sk-static"
                "other", "sk-other"
            ]
            false

    let clock = FakeTimeProvider()
    let recorded = ResizeArray<TimeSpan>()
    let tracker = ParallelTracker()

    let coordinator =
        makeCoordinator
            options
            registry
            Map.empty
            clock
            (RecordingClockDelay(clock, recorded) :> ILlmDelay)
            (SeededRandom(1) :> ILlmRandom)

    let plan =
        ResizeArray<PlannedOutcome>(
            [
                FailWith(providerFailureWithRetryAfter 429 (TimeSpan.FromSeconds 120.0))
            ]
        )

    let attempts = ResizeArray<int>()

    (fun () ->
        execute coordinator "acme/fast" tenantA 10L (runPlan plan attempts)
        |> fun task -> task.GetAwaiter().GetResult() |> ignore)
    |> should throw typeof<ProviderException>

    // Another provider's identity is unaffected by acme's cooldown.
    execute coordinator "other/mini" tenantA 10L (instantInvoke tracker (usageOf 1L 1L) "other")
    |> fun task -> task.GetAwaiter().GetResult() |> should equal "other"

    recorded.Count |> should equal 0

    let taskB =
        execute coordinator "acme/fast" tenantA 10L (instantInvoke tracker (usageOf 1L 1L) "b")

    taskB.IsCompleted |> should equal false
    recorded |> List.ofSeq |> should equal [ TimeSpan.FromSeconds 120.0 ]

    clock.Advance(TimeSpan.FromSeconds 120.0)
    taskB.GetAwaiter().GetResult() |> should equal "b"

// ───────────────────────────────────────────────────────────────────────────
// Jittered retry

[<Fact>]
let ``Jittered backoff retries transient failures then succeeds`` () =
    let provider = StubProvider("acme", "fast")
    let registry = makeRegistry [ provider :> ILlmProvider ]

    let options =
        makeOptions (fun coordination -> coordination.RetryCount <- 2) [ "acme", "sk-static" ] false

    let clock = FakeTimeProvider()
    let delay = RecordingDelay()
    let tracker = ParallelTracker()

    let coordinator =
        makeCoordinator options registry Map.empty clock (delay :> ILlmDelay) (SeededRandom(7) :> ILlmRandom)

    let plan =
        ResizeArray<PlannedOutcome>(
            [
                FailWith(providerFailure 500)
                FailWith(providerFailure 503)
                SucceedWith(usageOf 4L 6L, "ok")
            ]
        )

    let attempts = ResizeArray<int>()

    execute coordinator "acme/fast" tenantA 10L (runPlan plan attempts)
    |> fun task -> task.GetAwaiter().GetResult() |> should equal "ok"

    attempts |> List.ofSeq |> should equal [ 1; 2; 3 ]

    let waits = delay.Recorded |> List.ofSeq
    waits.Length |> should equal 2

    for wait in waits do
        wait >= TimeSpan.FromMilliseconds 250.0 |> should equal true
        wait <= TimeSpan.FromSeconds 10.0 |> should equal true

    // The same seed replays the same backoff sequence.
    let replayDelay = RecordingDelay()

    let replayCoordinator =
        makeCoordinator options registry Map.empty clock (replayDelay :> ILlmDelay) (SeededRandom(7) :> ILlmRandom)

    let replayPlan =
        ResizeArray<PlannedOutcome>(
            [
                FailWith(providerFailure 500)
                FailWith(providerFailure 503)
                SucceedWith(usageOf 4L 6L, "ok")
            ]
        )

    execute replayCoordinator "acme/fast" tenantA 10L (runPlan replayPlan (ResizeArray<int>()))
    |> fun task -> task.GetAwaiter().GetResult() |> ignore

    replayDelay.Recorded |> List.ofSeq |> should equal waits
    tracker.Peak |> should equal 0

[<Fact>]
let ``Exhausted retries surface the provider failure`` () =
    let provider = StubProvider("acme", "fast")
    let registry = makeRegistry [ provider :> ILlmProvider ]

    let options =
        makeOptions (fun coordination -> coordination.RetryCount <- 1) [ "acme", "sk-static" ] false

    let clock = FakeTimeProvider()

    let coordinator =
        makeCoordinator options registry Map.empty clock (RecordingDelay() :> ILlmDelay) (SeededRandom(1) :> ILlmRandom)

    let terminal = providerFailure 500
    let plan = ResizeArray<PlannedOutcome>([ FailWith terminal; FailWith terminal ])
    let attempts = ResizeArray<int>()

    let caught =
        try
            execute coordinator "acme/fast" tenantA 10L (runPlan plan attempts)
            |> fun task -> task.GetAwaiter().GetResult() |> ignore

            None
        with :? ProviderException as failure ->
            Some failure

    caught.IsSome |> should equal true
    Object.ReferenceEquals(caught.Value, terminal) |> should equal true
    attempts |> List.ofSeq |> should equal [ 1; 2 ]

[<Theory>]
[<InlineData(408)>]
[<InlineData(429)>]
[<InlineData(500)>]
[<InlineData(503)>]
[<InlineData(599)>]
let ``Transient statuses retry before succeeding`` (status: int) =
    let provider = StubProvider("acme", "fast")
    let registry = makeRegistry [ provider :> ILlmProvider ]

    let options =
        makeOptions (fun coordination -> coordination.RetryCount <- 1) [ "acme", "sk-static" ] false

    let clock = FakeTimeProvider()

    let coordinator =
        makeCoordinator options registry Map.empty clock (RecordingDelay() :> ILlmDelay) (SeededRandom(3) :> ILlmRandom)

    let plan =
        ResizeArray<PlannedOutcome>(
            [
                FailWith(providerFailure status)
                SucceedWith(usageOf 1L 1L, "ok")
            ]
        )

    let attempts = ResizeArray<int>()

    execute coordinator "acme/fast" tenantA 10L (runPlan plan attempts)
    |> fun task -> task.GetAwaiter().GetResult() |> should equal "ok"

    attempts |> List.ofSeq |> should equal [ 1; 2 ]

[<Theory>]
[<InlineData(400)>]
[<InlineData(401)>]
[<InlineData(403)>]
[<InlineData(404)>]
let ``Other 4xx surface immediately without retry`` (status: int) =
    let provider = StubProvider("acme", "fast")
    let registry = makeRegistry [ provider :> ILlmProvider ]

    let options =
        makeOptions (fun coordination -> coordination.RetryCount <- 2) [ "acme", "sk-static" ] false

    let clock = FakeTimeProvider()

    let coordinator =
        makeCoordinator options registry Map.empty clock (RecordingDelay() :> ILlmDelay) (SeededRandom(1) :> ILlmRandom)

    let terminal = providerFailure status

    let plan =
        ResizeArray<PlannedOutcome>(
            [
                FailWith terminal
                SucceedWith(usageOf 1L 1L, "unreached")
            ]
        )

    let attempts = ResizeArray<int>()

    let caught =
        try
            execute coordinator "acme/fast" tenantA 10L (runPlan plan attempts)
            |> fun task -> task.GetAwaiter().GetResult() |> ignore

            None
        with :? ProviderException as failure ->
            Some failure

    caught.IsSome |> should equal true
    Object.ReferenceEquals(caught.Value, terminal) |> should equal true
    attempts |> List.ofSeq |> should equal [ 1 ]

[<Fact>]
let ``Network errors retry and wrap without a status`` () =
    let provider = StubProvider("acme", "fast")
    let registry = makeRegistry [ provider :> ILlmProvider ]

    let options =
        makeOptions (fun coordination -> coordination.RetryCount <- 1) [ "acme", "sk-static" ] false

    let clock = FakeTimeProvider()

    let coordinator =
        makeCoordinator options registry Map.empty clock (RecordingDelay() :> ILlmDelay) (SeededRandom(1) :> ILlmRandom)

    let plan =
        ResizeArray<PlannedOutcome>(
            [
                FailWith(HttpRequestException("connection reset"))
                SucceedWith(usageOf 1L 1L, "ok")
            ]
        )

    let attempts = ResizeArray<int>()

    execute coordinator "acme/fast" tenantA 10L (runPlan plan attempts)
    |> fun task -> task.GetAwaiter().GetResult() |> should equal "ok"

    attempts |> List.ofSeq |> should equal [ 1; 2 ]

[<Fact>]
let ``Network errors exhausted surface ProviderException without a status`` () =
    let provider = StubProvider("acme", "fast")
    let registry = makeRegistry [ provider :> ILlmProvider ]

    let options =
        makeOptions (fun coordination -> coordination.RetryCount <- 0) [ "acme", "sk-static" ] false

    let clock = FakeTimeProvider()

    let coordinator =
        makeCoordinator options registry Map.empty clock (RecordingDelay() :> ILlmDelay) (SeededRandom(1) :> ILlmRandom)

    let plan =
        ResizeArray<PlannedOutcome>(
            [
                FailWith(HttpRequestException("connection reset"))
            ]
        )

    let attempts = ResizeArray<int>()

    let caught =
        try
            execute coordinator "acme/fast" tenantA 10L (runPlan plan attempts)
            |> fun task -> task.GetAwaiter().GetResult() |> ignore

            None
        with :? ProviderException as failure ->
            Some failure

    caught.IsSome |> should equal true
    caught.Value.ProviderId |> should equal "acme"
    caught.Value.Status.HasValue |> should equal false
    caught.Value.Message.Contains("HttpRequestException") |> should equal true
    attempts |> List.ofSeq |> should equal [ 1 ]

[<Fact>]
let ``Ambiguous cancellation retries like a network timeout`` () =
    let provider = StubProvider("acme", "fast")
    let registry = makeRegistry [ provider :> ILlmProvider ]

    let options =
        makeOptions (fun coordination -> coordination.RetryCount <- 1) [ "acme", "sk-static" ] false

    let clock = FakeTimeProvider()

    let coordinator =
        makeCoordinator options registry Map.empty clock (RecordingDelay() :> ILlmDelay) (SeededRandom(1) :> ILlmRandom)

    // Neither the caller nor the deadline token fired: the provider timed
    // out on its own watch, which is transient.
    let plan =
        ResizeArray<PlannedOutcome>(
            [
                FailWith(TaskCanceledException())
                SucceedWith(usageOf 1L 1L, "ok")
            ]
        )

    let attempts = ResizeArray<int>()

    execute coordinator "acme/fast" tenantA 10L (runPlan plan attempts)
    |> fun task -> task.GetAwaiter().GetResult() |> should equal "ok"

    attempts |> List.ofSeq |> should equal [ 1; 2 ]

[<Fact>]
let ``Host bugs propagate unwrapped`` () =
    let provider = StubProvider("acme", "fast")
    let registry = makeRegistry [ provider :> ILlmProvider ]

    let options =
        makeOptions (fun coordination -> coordination.RetryCount <- 2) [ "acme", "sk-static" ] false

    let clock = FakeTimeProvider()

    let coordinator =
        makeCoordinator options registry Map.empty clock (RecordingDelay() :> ILlmDelay) (SeededRandom(1) :> ILlmRandom)

    let mutable attempts = 0
    let boom = InvalidOperationException("boom")

    let invoke =
        Func<IChatClient, CancellationToken, Task<StringOutcome>>(fun _ _ ->
            attempts <- attempts + 1
            raise boom)

    let caught =
        try
            execute coordinator "acme/fast" tenantA 10L invoke
            |> fun task -> task.GetAwaiter().GetResult() |> ignore

            None
        with :? InvalidOperationException as failure ->
            Some failure

    caught.IsSome |> should equal true
    Object.ReferenceEquals(caught.Value, boom) |> should equal true
    attempts |> should equal 1

[<Fact>]
let ``Null provider result fails loudly`` () =
    let provider = StubProvider("acme", "fast")
    let registry = makeRegistry [ provider :> ILlmProvider ]
    let options = makeOptions ignore [ "acme", "sk-static" ] false
    let clock = FakeTimeProvider()

    let coordinator =
        makeCoordinator options registry Map.empty clock (RecordingDelay() :> ILlmDelay) (SeededRandom(1) :> ILlmRandom)

    let invoke =
        Func<IChatClient, CancellationToken, Task<StringOutcome>>(fun _ _ ->
            Task.FromResult(Unchecked.defaultof<StringOutcome>))

    (fun () ->
        execute coordinator "acme/fast" tenantA 10L invoke
        |> fun task -> task.GetAwaiter().GetResult() |> ignore)
    |> should throw typeof<InvalidOperationException>

// ───────────────────────────────────────────────────────────────────────────
// Turn deadline

[<Fact>]
let ``Deadline fires for queued and running calls`` () =
    let provider = StubProvider("acme", "fast")
    let registry = makeRegistry [ provider :> ILlmProvider ]

    let options =
        makeOptions
            (fun coordination ->
                coordination.RequestsPerMinute <- 1
                coordination.RequestTimeoutSeconds <- 600)
            [ "acme", "sk-static" ]
            false

    let clock = FakeTimeProvider()
    let tracker = ParallelTracker()

    let coordinator =
        makeCoordinator options registry Map.empty clock (NeverDelay() :> ILlmDelay) (SeededRandom(1) :> ILlmRandom)

    let taskA = execute coordinator "acme/fast" tenantA 10L (hangingInvoke tracker)
    taskA.IsCompleted |> should equal false

    let taskB =
        execute coordinator "acme/fast" tenantA 10L (instantInvoke tracker (usageOf 1L 1L) "b")

    taskB.IsCompleted |> should equal false

    clock.Advance(TimeSpan.FromSeconds 601.0)

    for task in [ taskA; taskB ] do
        let caught =
            try
                task.GetAwaiter().GetResult() |> ignore
                None
            with :? DeadlineExceededException as exhausted ->
                Some exhausted

        caught.IsSome |> should equal true
        caught.Value.OperationName |> should equal "Execute"
        caught.Value.Message.Contains("acme/static") |> should equal true

// ───────────────────────────────────────────────────────────────────────────
// Identity key fail-fast and the gate

[<Fact>]
let ``Missing key fails fast before queueing`` () =
    let provider = StubProvider("acme", "fast")
    let registry = makeRegistry [ provider :> ILlmProvider ]
    let options = makeOptions ignore [] false
    let clock = FakeTimeProvider()
    let delay = RecordingDelay()
    let tracker = ParallelTracker()

    let coordinator =
        makeCoordinator options registry Map.empty clock (delay :> ILlmDelay) (SeededRandom(1) :> ILlmRandom)

    let caught =
        try
            execute coordinator "acme/fast" tenantA 10L (instantInvoke tracker (usageOf 1L 1L) "unreached")
            |> fun task -> task.GetAwaiter().GetResult() |> ignore

            None
        with :? ProviderException as failure ->
            Some failure

    caught.IsSome |> should equal true
    caught.Value.ProviderId |> should equal "acme"
    caught.Value.Status.HasValue |> should equal false
    provider.Builds |> should equal 0
    delay.Recorded.Count |> should equal 0

    // A null key provider behaves the same: no per-tenant keys exist.
    let nullKeyed =
        LlmCoordination.LlmCoordinator(
            options,
            registry,
            null,
            clock,
            (delay :> ILlmDelay),
            (SeededRandom(1) :> ILlmRandom)
        )

    (fun () ->
        execute nullKeyed "acme/fast" tenantA 10L (instantInvoke tracker (usageOf 1L 1L) "unreached")
        |> fun task -> task.GetAwaiter().GetResult() |> ignore)
    |> should throw typeof<ProviderException>

[<Fact>]
let ``Invalid coordination options fail before a turn runs`` () =
    let provider = StubProvider("acme", "fast")
    let registry = makeRegistry [ provider :> ILlmProvider ]

    let options =
        makeOptions (fun coordination -> coordination.MaxConcurrentRequests <- 0) [ "acme", "sk-static" ] false

    let clock = FakeTimeProvider()

    let coordinator =
        makeCoordinator options registry Map.empty clock (NeverDelay() :> ILlmDelay) (SeededRandom(1) :> ILlmRandom)

    (fun () ->
        execute coordinator "acme/fast" tenantA 10L (instantInvoke (ParallelTracker()) (usageOf 1L 1L) "unreached")
        |> fun task -> task.GetAwaiter().GetResult() |> ignore)
    |> should throw typeof<ArgumentException>

    provider.Builds |> should equal 0

[<Fact>]
let ``Distributed coordination fails fast`` () =
    let provider = StubProvider("acme", "fast")
    let registry = makeRegistry [ provider :> ILlmProvider ]
    let options = makeOptions ignore [ "acme", "sk-static" ] true
    let clock = FakeTimeProvider()

    let coordinator =
        makeCoordinator options registry Map.empty clock (NeverDelay() :> ILlmDelay) (SeededRandom(1) :> ILlmRandom)

    let caught =
        try
            execute coordinator "acme/fast" tenantA 10L (instantInvoke (ParallelTracker()) (usageOf 1L 1L) "unreached")
            |> fun task -> task.GetAwaiter().GetResult() |> ignore

            None
        with :? InvalidOperationException as failure ->
            Some failure

    caught.IsSome |> should equal true
    caught.Value.Message.Contains("DistributedCoordination") |> should equal true
    provider.Builds |> should equal 0
