// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.LlmCoordinatorTests

open System
open System.Collections.Generic
open System.Diagnostics.Metrics
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

// ───────────────────────────────────────────────────────────────────────────
// Model policy and usage observation (issue 55)

/// A model policy that returns the configured decision and records the
/// calls it saw. A null decision exercises the null-guard.
type ScriptedModelPolicy(decision: ModelPolicyDecision) =
    let calls = ResizeArray<TenantId * string * string>()

    interface IModelPolicy with
        member _.Authorize(tenant: TenantId, provider: string, model: string) =
            calls.Add((tenant, provider, model))
            decision

    /// The calls Authorize received, oldest first.
    member _.Calls = calls |> List.ofSeq

/// A model decision shape the coordinator does not know: the
/// unknown-shape guard must reject it.
type UnknownModelDecision() =
    inherit ModelPolicyDecision()

/// A usage observer that records every delivery in arrival order.
type RecordingUsageObserver() =
    let checkpoints = ResizeArray<UsageCheckpoint>()
    let settlements = ResizeArray<UsageSettlement>()

    interface IUsageObserver with
        member _.OnCheckpoint(usage: UsageCheckpoint) = checkpoints.Add(usage)
        member _.OnSettled(usage: UsageSettlement) = settlements.Add(usage)

    /// The checkpoints in arrival order.
    member _.Checkpoints = checkpoints |> List.ofSeq

    /// The settlements in arrival order.
    member _.Settlements = settlements |> List.ofSeq

/// A usage observer that always throws: guarded delivery must never fail
/// the call.
type ThrowingUsageObserver() =
    interface IUsageObserver with
        member _.OnCheckpoint(_: UsageCheckpoint) =
            raise (InvalidOperationException("observer boom"))

        member _.OnSettled(_: UsageSettlement) =
            raise (InvalidOperationException("observer boom"))

/// A usage observer that deduplicates at-least-once redeliveries on the
/// idempotency key: the second delivery with the same key has no effect.
type DedupUsageObserver() =
    let seen = HashSet<string>()
    let mutable effects = 0

    interface IUsageObserver with
        member _.OnCheckpoint(usage: UsageCheckpoint) =
            if seen.Add("checkpoint:" + usage.IdempotencyKey) then
                effects <- effects + 1

        member _.OnSettled(usage: UsageSettlement) =
            if seen.Add("settled:" + usage.IdempotencyKey) then
                effects <- effects + 1

    /// The deduplicated effect count.
    member _.Effects = effects

let private executeObserved
    (coordinator: LlmCoordination.LlmCoordinator)
    (reference: string)
    (tenant: TenantId)
    (estimate: int64)
    (invoke: Func<IChatClient, CancellationToken, Task<StringOutcome>>)
    (policy: #IModelPolicy)
    (observer: #IUsageObserver)
    (sessionId: SessionId)
    (turnId: TurnId)
    (attempt: int)
    : Task<string> =
    coordinator.ExecuteAsync(
        ModelReference.Parse(reference),
        tenant,
        estimate,
        invoke,
        CancellationToken.None,
        policy,
        observer,
        sessionId,
        turnId,
        attempt
    )

let private observedCoordinator
    (providers: ILlmProvider list)
    (staticKeys: (string * string) list)
    (keys: Map<string * string, string>)
    (setupCoordination: LlmCoordinationOptions -> unit)
    : LlmCoordination.LlmCoordinator * FakeTimeProvider =
    let registry = makeRegistry providers
    let options = makeOptions setupCoordination staticKeys false
    let clock = FakeTimeProvider()

    makeCoordinator options registry keys clock (NeverDelay() :> ILlmDelay) (SeededRandom(1) :> ILlmRandom), clock

[<Fact>]
let ``Deny throws before provider build with the client-safe message`` () =
    let provider = StubProvider("acme", "fast")

    let coordinator, _ =
        observedCoordinator [ provider :> ILlmProvider ] [ "acme", "sk-static" ] Map.empty ignore

    let policy =
        ScriptedModelPolicy(ModelPolicyDecision.Deny("Model is not on your plan."))

    let observer = RecordingUsageObserver()
    let sessionId = SessionId.New()
    let turnId = TurnId.New()
    let tracker = ParallelTracker()

    let caught =
        try
            executeObserved
                coordinator
                "acme/fast"
                tenantA
                10L
                (instantInvoke tracker (usageOf 1L 1L) "unreached")
                policy
                observer
                sessionId
                turnId
                1
            |> fun task -> task.GetAwaiter().GetResult() |> ignore

            None
        with :? ModelDeniedException as denied ->
            Some denied

    caught.IsSome |> should equal true
    caught.Value.ProviderId |> should equal "acme"
    caught.Value.Model |> should equal "fast"
    caught.Value.Message |> should equal "Model is not on your plan."
    // Denied calls never reach the network: no client is built.
    provider.Builds |> should equal 0
    // The client-safe message carries no topology or secrets.
    caught.Value.Message.Contains("sk-static") |> should equal false
    caught.Value.Message.Contains("Registered providers") |> should equal false
    // Deny reports nothing.
    observer.Checkpoints.Length |> should equal 0
    observer.Settlements.Length |> should equal 0
    // The policy saw the tenant, provider, and model.
    policy.Calls |> should equal [ (tenantA, "acme", "fast") ]

[<Fact>]
let ``Deny outranks unregistered provider`` () =
    let coordinator, _ = observedCoordinator [] [ "acme", "sk-static" ] Map.empty ignore

    let policy =
        ScriptedModelPolicy(ModelPolicyDecision.Deny("Model is not on your plan."))

    let observer = RecordingUsageObserver()

    let caught =
        try
            executeObserved
                coordinator
                "missing/model"
                tenantA
                10L
                (instantInvoke (ParallelTracker()) (usageOf 1L 1L) "unreached")
                policy
                observer
                (SessionId.New())
                (TurnId.New())
                1
            |> fun task -> task.GetAwaiter().GetResult() |> ignore

            None
        with :? ModelDeniedException as denied ->
            Some denied

    caught.IsSome |> should equal true
    caught.Value.ProviderId |> should equal "missing"
    caught.Value.Message |> should equal "Model is not on your plan."
    // The registered-id list never leaks through a deny.
    caught.Value.Message.Contains("acme") |> should equal false
    observer.Checkpoints.Length |> should equal 0
    observer.Settlements.Length |> should equal 0

[<Fact>]
let ``Deny outranks missing key`` () =
    let provider = StubProvider("acme", "fast")

    let coordinator, _ =
        observedCoordinator [ provider :> ILlmProvider ] [] Map.empty ignore

    let policy =
        ScriptedModelPolicy(ModelPolicyDecision.Deny("Model is not on your plan."))

    let observer = RecordingUsageObserver()

    let caught =
        try
            executeObserved
                coordinator
                "acme/fast"
                tenantA
                10L
                (instantInvoke (ParallelTracker()) (usageOf 1L 1L) "unreached")
                policy
                observer
                (SessionId.New())
                (TurnId.New())
                1
            |> fun task -> task.GetAwaiter().GetResult() |> ignore

            None
        with :? ModelDeniedException as denied ->
            Some denied

    caught.IsSome |> should equal true
    caught.Value.Message |> should equal "Model is not on your plan."
    provider.Builds |> should equal 0
    observer.Checkpoints.Length |> should equal 0
    observer.Settlements.Length |> should equal 0

[<Fact>]
let ``Null and unknown policy decisions fail loudly without contacting the provider`` () =
    let provider = StubProvider("acme", "fast")

    let coordinator, _ =
        observedCoordinator [ provider :> ILlmProvider ] [ "acme", "sk-static" ] Map.empty ignore

    let observer = RecordingUsageObserver()

    (fun () ->
        executeObserved
            coordinator
            "acme/fast"
            tenantA
            10L
            (instantInvoke (ParallelTracker()) (usageOf 1L 1L) "unreached")
            (ScriptedModelPolicy(Unchecked.defaultof<ModelPolicyDecision>))
            observer
            (SessionId.New())
            (TurnId.New())
            1
        |> fun task -> task.GetAwaiter().GetResult() |> ignore)
    |> should throw typeof<InvalidOperationException>

    (fun () ->
        executeObserved
            coordinator
            "acme/fast"
            tenantA
            10L
            (instantInvoke (ParallelTracker()) (usageOf 1L 1L) "unreached")
            (ScriptedModelPolicy(UnknownModelDecision() :> ModelPolicyDecision))
            observer
            (SessionId.New())
            (TurnId.New())
            1
        |> fun task -> task.GetAwaiter().GetResult() |> ignore)
    |> should throw typeof<InvalidOperationException>

    provider.Builds |> should equal 0
    observer.Checkpoints.Length |> should equal 0
    observer.Settlements.Length |> should equal 0

[<Fact>]
let ``Success emits exactly one checkpoint with outcome tokens and a fresh key`` () =
    let provider = StubProvider("acme", "fast")

    let coordinator, _ =
        observedCoordinator [ provider :> ILlmProvider ] [ "acme", "sk-static" ] Map.empty ignore

    let policy = ScriptedModelPolicy(ModelPolicyDecision.Allow)
    let observer = RecordingUsageObserver()
    let sessionId = SessionId.New()
    let turnId = TurnId.New()
    let tracker = ParallelTracker()

    let result =
        executeObserved
            coordinator
            "acme/fast"
            tenantA
            10L
            (instantInvoke tracker (usageOf 7L 5L) "ok")
            policy
            observer
            sessionId
            turnId
            2
        |> fun task -> task.GetAwaiter().GetResult()

    result |> should equal "ok"
    observer.Checkpoints.Length |> should equal 1
    observer.Settlements.Length |> should equal 0

    let checkpoint = observer.Checkpoints.Head
    checkpoint.Tenant |> should equal tenantA
    checkpoint.SessionId |> should equal sessionId
    checkpoint.TurnId |> should equal turnId
    checkpoint.Attempt |> should equal 2
    checkpoint.Provider |> should equal "acme"
    checkpoint.Model |> should equal "fast"
    checkpoint.InputTokens |> should equal 7L
    checkpoint.OutputTokens |> should equal 5L
    String.IsNullOrWhiteSpace(checkpoint.IdempotencyKey) |> should equal false

[<Fact>]
let ``Null usage reports as zero`` () =
    let provider = StubProvider("acme", "fast")

    let coordinator, _ =
        observedCoordinator [ provider :> ILlmProvider ] [ "acme", "sk-static" ] Map.empty ignore

    let observer = RecordingUsageObserver()

    let invoke =
        Func<IChatClient, CancellationToken, Task<StringOutcome>>(fun _ _ ->
            Task.FromResult(
                {
                    Value = "ok"
                    Usage = Unchecked.defaultof<UsageSummary>
                }
            ))

    let result =
        executeObserved
            coordinator
            "acme/fast"
            tenantA
            10L
            invoke
            (ScriptedModelPolicy(ModelPolicyDecision.Allow))
            observer
            (SessionId.New())
            (TurnId.New())
            1
        |> fun task -> task.GetAwaiter().GetResult()

    result |> should equal "ok"
    observer.Checkpoints.Length |> should equal 1
    observer.Checkpoints.Head.InputTokens |> should equal 0L
    observer.Checkpoints.Head.OutputTokens |> should equal 0L
    observer.Settlements.Length |> should equal 0

[<Fact>]
let ``Terminal post-send failure emits exactly one zero-token settlement`` () =
    let provider = StubProvider("acme", "fast")

    let coordinator, _ =
        observedCoordinator [ provider :> ILlmProvider ] [ "acme", "sk-static" ] Map.empty ignore

    let observer = RecordingUsageObserver()
    let sessionId = SessionId.New()
    let turnId = TurnId.New()

    let plan = ResizeArray<PlannedOutcome>([ FailWith(providerFailure 400) ])
    let attempts = ResizeArray<int>()

    let caught =
        try
            executeObserved
                coordinator
                "acme/fast"
                tenantA
                10L
                (runPlan plan attempts)
                (ScriptedModelPolicy(ModelPolicyDecision.Allow))
                observer
                sessionId
                turnId
                3
            |> fun task -> task.GetAwaiter().GetResult() |> ignore

            None
        with :? ProviderException as failure ->
            Some failure

    caught.IsSome |> should equal true
    caught.Value.Status |> should equal (Nullable 400)
    attempts |> List.ofSeq |> should equal [ 1 ]
    // The abandoned call settles exactly once with zero tokens; the host
    // correlates the settlement via the propagated exception.
    observer.Checkpoints.Length |> should equal 0
    observer.Settlements.Length |> should equal 1

    let settlement = observer.Settlements.Head
    settlement.Tenant |> should equal tenantA
    settlement.SessionId |> should equal sessionId
    settlement.TurnId |> should equal turnId
    settlement.Attempt |> should equal 3
    settlement.Provider |> should equal "acme"
    settlement.Model |> should equal "fast"
    settlement.InputTokens |> should equal 0L
    settlement.OutputTokens |> should equal 0L
    String.IsNullOrWhiteSpace(settlement.IdempotencyKey) |> should equal false

[<Fact>]
let ``Pre-send failures report nothing`` () =
    let provider = StubProvider("acme", "fast")
    // No keys: the allow-policy call fails on the missing key before send.
    let coordinator, _ =
        observedCoordinator [ provider :> ILlmProvider ] [] Map.empty ignore

    let observer = RecordingUsageObserver()

    (fun () ->
        executeObserved
            coordinator
            "acme/fast"
            tenantA
            10L
            (instantInvoke (ParallelTracker()) (usageOf 1L 1L) "unreached")
            (ScriptedModelPolicy(ModelPolicyDecision.Allow))
            observer
            (SessionId.New())
            (TurnId.New())
            1
        |> fun task -> task.GetAwaiter().GetResult() |> ignore)
    |> should throw typeof<ProviderException>

    provider.Builds |> should equal 0
    observer.Checkpoints.Length |> should equal 0
    observer.Settlements.Length |> should equal 0

[<Fact>]
let ``Throwing observer never fails the call`` () =
    let provider = StubProvider("acme", "fast")

    let coordinator, _ =
        observedCoordinator [ provider :> ILlmProvider ] [ "acme", "sk-static" ] Map.empty ignore

    let policy = ScriptedModelPolicy(ModelPolicyDecision.Allow)
    let throwing = ThrowingUsageObserver()

    let result =
        executeObserved
            coordinator
            "acme/fast"
            tenantA
            10L
            (instantInvoke (ParallelTracker()) (usageOf 2L 3L) "ok")
            policy
            throwing
            (SessionId.New())
            (TurnId.New())
            1
        |> fun task -> task.GetAwaiter().GetResult()

    result |> should equal "ok"

    // A throwing observer never masks the original post-send failure either.
    let plan = ResizeArray<PlannedOutcome>([ FailWith(providerFailure 400) ])

    let caught =
        try
            executeObserved
                coordinator
                "acme/fast"
                tenantA
                10L
                (runPlan plan (ResizeArray<int>()))
                policy
                throwing
                (SessionId.New())
                (TurnId.New())
                1
            |> fun task -> task.GetAwaiter().GetResult() |> ignore

            None
        with :? ProviderException as failure ->
            Some failure

    caught.IsSome |> should equal true
    caught.Value.Status |> should equal (Nullable 400)
    caught.Value.Message.Contains("observer boom") |> should equal false

[<Fact>]
let ``Null policy allows and null observer skips`` () =
    let provider = StubProvider("acme", "fast")

    let coordinator, _ =
        observedCoordinator [ provider :> ILlmProvider ] [ "acme", "sk-static" ] Map.empty ignore

    let result =
        coordinator.ExecuteAsync(
            ModelReference.Parse("acme/fast"),
            tenantA,
            10L,
            (instantInvoke (ParallelTracker()) (usageOf 1L 2L) "ok"),
            CancellationToken.None,
            null,
            null,
            SessionId.New(),
            TurnId.New(),
            1
        )
        |> fun task -> task.GetAwaiter().GetResult()

    result |> should equal "ok"
    provider.Builds |> should equal 1

[<Fact>]
let ``Retried then succeeded call still emits exactly once`` () =
    let provider = StubProvider("acme", "fast")
    let registry = makeRegistry [ provider :> ILlmProvider ]

    let options =
        makeOptions (fun coordination -> coordination.RetryCount <- 2) [ "acme", "sk-static" ] false

    let clock = FakeTimeProvider()

    let coordinator =
        makeCoordinator options registry Map.empty clock (RecordingDelay() :> ILlmDelay) (SeededRandom(1) :> ILlmRandom)

    let observer = RecordingUsageObserver()

    let plan =
        ResizeArray<PlannedOutcome>(
            [
                FailWith(providerFailure 500)
                SucceedWith(usageOf 3L 4L, "ok")
            ]
        )

    let attempts = ResizeArray<int>()

    let result =
        executeObserved
            coordinator
            "acme/fast"
            tenantA
            10L
            (runPlan plan attempts)
            (ScriptedModelPolicy(ModelPolicyDecision.Allow))
            observer
            (SessionId.New())
            (TurnId.New())
            1
        |> fun task -> task.GetAwaiter().GetResult()

    result |> should equal "ok"
    attempts |> List.ofSeq |> should equal [ 1; 2 ]
    // The transient retry emits nothing: the terminal outcome reports once.
    observer.Checkpoints.Length |> should equal 1
    observer.Checkpoints.Head.InputTokens |> should equal 3L
    observer.Checkpoints.Head.OutputTokens |> should equal 4L
    observer.Settlements.Length |> should equal 0

[<Fact>]
let ``Streaming shaped invoke reports once on completion`` () =
    let provider = StubProvider("acme", "fast")

    let coordinator, _ =
        observedCoordinator [ provider :> ILlmProvider ] [ "acme", "sk-static" ] Map.empty ignore

    let observer = RecordingUsageObserver()
    let deltas = ResizeArray<string>()

    // One invoke that emits several streaming chunks internally before it
    // returns its single terminal outcome.
    let invoke =
        Func<IChatClient, CancellationToken, Task<StringOutcome>>(fun _ _ ->
            for chunk in [ "he"; "ll"; "o" ] do
                deltas.Add(chunk)

            Task.FromResult(
                {
                    Value = "hello"
                    Usage = usageOf 9L 6L
                }
            ))

    let result =
        executeObserved
            coordinator
            "acme/fast"
            tenantA
            10L
            invoke
            (ScriptedModelPolicy(ModelPolicyDecision.Allow))
            observer
            (SessionId.New())
            (TurnId.New())
            1
        |> fun task -> task.GetAwaiter().GetResult()

    result |> should equal "hello"
    deltas |> List.ofSeq |> should equal [ "he"; "ll"; "o" ]
    observer.Checkpoints.Length |> should equal 1
    observer.Checkpoints.Head.InputTokens |> should equal 9L
    observer.Checkpoints.Head.OutputTokens |> should equal 6L
    observer.Settlements.Length |> should equal 0

[<Fact>]
let ``Duplicate settlement with the same key dedups`` () =
    let dedup = DedupUsageObserver()
    let observer = dedup :> IUsageObserver
    let sessionId = SessionId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"
    let turnId = TurnId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"

    let settlement: UsageSettlement =
        {
            Tenant = tenantA
            SessionId = sessionId
            TurnId = turnId
            Attempt = 1
            Provider = "acme"
            Model = "fast"
            InputTokens = 0L
            OutputTokens = 0L
            IdempotencyKey = "abandoned-1"
        }

    // At-least-once redelivery of the same settlement has no second effect.
    observer.OnSettled(settlement)
    observer.OnSettled(settlement)
    dedup.Effects |> should equal 1

    // A fresh key is a distinct delivery.
    observer.OnSettled(
        { settlement with
            IdempotencyKey = "abandoned-2"
        }
    )

    dedup.Effects |> should equal 2

[<Fact>]
let ``Deliveries carry fresh distinct idempotency keys`` () =
    let provider = StubProvider("acme", "fast")

    let coordinator, _ =
        observedCoordinator [ provider :> ILlmProvider ] [ "acme", "sk-static" ] Map.empty ignore

    let observer = RecordingUsageObserver()
    let policy = ScriptedModelPolicy(ModelPolicyDecision.Allow)

    for _ in [ 1; 2 ] do
        executeObserved
            coordinator
            "acme/fast"
            tenantA
            10L
            (instantInvoke (ParallelTracker()) (usageOf 1L 1L) "ok")
            policy
            observer
            (SessionId.New())
            (TurnId.New())
            1
        |> fun task -> task.GetAwaiter().GetResult() |> ignore

    observer.Checkpoints.Length |> should equal 2

    let keys =
        observer.Checkpoints |> List.map (fun checkpoint -> checkpoint.IdempotencyKey)

    keys
    |> List.forall (fun key -> String.IsNullOrWhiteSpace(key) |> not)
    |> should equal true

    keys[0] |> should not' (equal keys[1])

// ──────────────────────────────────────────────────────────────────────────
// Logging scopes (issue 93)

/// One captured log line with the scopes active when it logged.
type private LoggedLine =
    {
        Level: string
        Text: string
        Scopes: (string * obj) list
    }

/// An ILogger capturing every entry with the scopes active at log time.
type private ScopeCapturingLogger() =
    let gate = obj ()
    let entries = ResizeArray<LoggedLine>()
    let stack = ResizeArray<(string * obj) list>()

    let toPairs (state: obj | null) : (string * obj) list =
        if isNull (box state) then
            []
        else
            match state with
            | :? IReadOnlyList<KeyValuePair<string, obj>> as kvs ->
                kvs |> Seq.map (fun kv -> kv.Key, kv.Value) |> List.ofSeq
            | :? IEnumerable<KeyValuePair<string, obj>> as kvs ->
                kvs |> Seq.map (fun kv -> kv.Key, kv.Value) |> List.ofSeq
            | _ -> []

    interface Microsoft.Extensions.Logging.ILogger with
        member _.BeginScope<'TState when 'TState: not null>(state: 'TState) : IDisposable =
            let pairs = toPairs (box state)
            lock gate (fun () -> stack.Add(pairs))

            { new IDisposable with
                member _.Dispose() =
                    lock gate (fun () ->
                        if stack.Count > 0 then
                            stack.RemoveAt(stack.Count - 1))
            }

        member _.IsEnabled(_) = true

        member _.Log<'TState>
            (
                logLevel: Microsoft.Extensions.Logging.LogLevel,
                _eventId: Microsoft.Extensions.Logging.EventId,
                state: 'TState,
                ex: exn,
                formatter: Func<'TState, exn, string>
            ) : unit =
            let text = formatter.Invoke(state, ex)
            let scopes = lock gate (fun () -> stack |> Seq.concat |> List.ofSeq)

            lock gate (fun () ->
                entries.Add(
                    {
                        Level = logLevel.ToString()
                        Text = text
                        Scopes = scopes
                    }
                ))

    /// Every captured line, oldest first.
    member _.Entries: LoggedLine list = lock gate (fun () -> entries |> List.ofSeq)

[<Fact>]
let ``Coordinator admit logs carry all six scopes and never carry keys`` () =
    let logger = ScopeCapturingLogger()

    let options =
        makeOptions (fun _ -> ()) [ "acme", "sk-static-coordinator-key" ] false

    let registry =
        makeRegistry
            [
                StubProvider("acme", "fast") :> ILlmProvider
            ]

    let clock = FakeTimeProvider()
    let tracker = ParallelTracker()

    let coordinator =
        LlmCoordination.LlmCoordinator(
            options,
            registry,
            TableKeyProvider(Map.empty) :> IApiKeyProvider,
            clock,
            (NeverDelay() :> ILlmDelay),
            (SeededRandom(1) :> ILlmRandom),
            (logger :> Microsoft.Extensions.Logging.ILogger)
        )

    let invoke = instantInvoke tracker (usageOf 1L 1L) "ok"

    let value =
        coordinator.ExecuteAsync(
            ModelReference.Parse("acme/fast"),
            tenantA,
            10L,
            invoke,
            CancellationToken.None,
            null,
            null,
            SessionId.New(),
            TurnId.New(),
            1
        )
        |> fun task -> task.GetAwaiter().GetResult()

    value |> should equal "ok"

    let entries = logger.Entries
    entries |> should not' (equal [])

    for entry in entries do
        for key in
            [
                LoggingScopes.SessionIdKey
                LoggingScopes.TurnIdKey
                LoggingScopes.AgentIdKey
                LoggingScopes.TenantIdKey
                LoggingScopes.AttemptKey
                LoggingScopes.ClaimOwnerKey
            ] do
            entry.Scopes |> List.exists (fun (name, _) -> name = key) |> should equal true

        entry.Text.Contains("sk-static-coordinator-key") |> should equal false

// ───────────────────────────────────────────────────────────────────────────
// Distributed admission (issue 136)

/// One scripted acquire step: an outcome to return, or a failure to raise.
type private ScriptedAcquire =
    | AcquireOutcome of DistributedAdmissionOutcome
    | AcquireThrow of exn

/// A scripted IDistributedLlmAdmission: dequeues scripted acquire outcomes
/// in call order and records every acquire, release, and cooldown for
/// assertions. Renew and complete report false (fenced or missing).
type private FakeAdmission(script: ResizeArray<ScriptedAcquire>) =
    let gate = obj ()
    let acquireCalls = ResizeArray<string * string * int * TimeSpan * TimeSpan>()
    let releases = ResizeArray<string * string>()
    let cooldowns = ResizeArray<string * TimeSpan>()

    /// The acquire calls seen, oldest first: identity, owner, max
    /// concurrency, lease TTL, waiter TTL.
    member _.AcquireCalls = lock gate (fun () -> acquireCalls |> List.ofSeq)

    /// The releases seen, oldest first: identity and owner.
    member _.Releases = lock gate (fun () -> releases |> List.ofSeq)

    /// The cooldown propagations seen, oldest first: identity and pause.
    member _.Cooldowns = lock gate (fun () -> cooldowns |> List.ofSeq)

    interface IDistributedLlmAdmission with
        member _.AcquireAsync(identity, ownerId, maxConcurrency, leaseTtl, waiterTtl, _) =
            lock gate (fun () -> acquireCalls.Add((identity, ownerId, maxConcurrency, leaseTtl, waiterTtl)))

            let step =
                lock gate (fun () ->
                    if script.Count = 0 then
                        failwith "The acquire script ran out of scripted outcomes."

                    let head = script[0]
                    script.RemoveAt(0)
                    head)

            match step with
            | AcquireOutcome outcome -> Task.FromResult(outcome)
            | AcquireThrow failure -> Task.FromException<DistributedAdmissionOutcome>(failure)

        member _.RenewAsync(_, _, _, _) = Task.FromResult(false)

        member _.ReleaseAsync(identity, ownerId, _) =
            lock gate (fun () -> releases.Add((identity, ownerId)))
            Task.FromResult(true)

        member _.CompleteAsync(_, _, _) = Task.FromResult(false)

        member _.StartCooldownAsync(identity, cooldown, _) =
            lock gate (fun () -> cooldowns.Add((identity, cooldown)))
            Task.CompletedTask

/// Builds distributed coordination options: Redis mode on the loopback
/// connection with the given fail policy.
let private makeDistOptions (failClosed: bool) : DistributedCoordinationOptions =
    let dist = DistributedCoordinationOptions()
    dist.Mode <- DistributedCoordinationMode.Redis
    dist.ConnectionString <- "127.0.0.1:6379"
    dist.FailClosed <- failClosed
    dist

/// Builds a coordinator wired to the fake seam.
let private makeDistributedCoordinator
    (options: LlmOptions)
    (registry: ILlmProviderRegistry)
    (clock: TimeProvider)
    (delay: ILlmDelay)
    (random: ILlmRandom)
    (admission: IDistributedLlmAdmission | null)
    (distOptions: DistributedCoordinationOptions | null)
    : LlmCoordination.LlmCoordinator =
    LlmCoordination.LlmCoordinator(
        options,
        registry,
        TableKeyProvider(Map.empty) :> IApiKeyProvider,
        clock,
        delay,
        random,
        null,
        admission,
        distOptions
    )

/// Runs one call under a known session, turn, and attempt: the owner id
/// the coordinator derives stays assertable.
let private executeKnownTurn
    (coordinator: LlmCoordination.LlmCoordinator)
    (tenant: TenantId)
    (invoke: Func<IChatClient, CancellationToken, Task<StringOutcome>>)
    (sessionId: SessionId)
    (turnId: TurnId)
    (attempt: int)
    : Task<string> =
    coordinator.ExecuteAsync(
        ModelReference.Parse("acme/fast"),
        tenant,
        10L,
        invoke,
        CancellationToken.None,
        null,
        null,
        sessionId,
        turnId,
        attempt
    )

/// An ILlmDelay that advances the fake clock by the requested wait instead
/// of parking on it: deadline-bound distributed waits stay deterministic
/// without interleaving advances from the fact.
type private AdvancingDelay(clock: FakeTimeProvider, recorded: ResizeArray<TimeSpan>) =
    interface ILlmDelay with
        member _.Delay(requested, cancellationToken) =
            recorded.Add(requested)
            cancellationToken.ThrowIfCancellationRequested()
            clock.Advance(requested)
            Task.CompletedTask

/// An ILlmDelay that cancels the caller's source on the first wait: the
/// fact proves caller cancellation maps to cancel, never to fail policy.
type private CancelOnFirstDelay(owner: CancellationTokenSource) =
    interface ILlmDelay with
        member _.Delay(_, _) =
            owner.Cancel()
            Task.CompletedTask

/// Counts legate.provider.fail_open_admissions points for the provider
/// observed while emit runs.
let private countFailOpenAdmissions (provider: string) (emit: unit -> unit) : int64 =
    let gate = obj ()
    let mutable total = 0L
    use listener = new MeterListener()

    listener.InstrumentPublished <-
        Action<Instrument, MeterListener>(fun instrument _ ->
            if instrument.Name = Telemetry.FailOpenAdmissionsName then
                listener.EnableMeasurementEvents(instrument, null) |> ignore)

    listener.SetMeasurementEventCallback<int64>(fun instrument measurement tags _ ->
        if instrument.Name = Telemetry.FailOpenAdmissionsName then
            let mutable matched = false

            for index in 0 .. tags.Length - 1 do
                if tags[index].Key = Telemetry.ProviderTag && string tags[index].Value = provider then
                    matched <- true

            if matched then
                lock gate (fun () -> total <- total + measurement))

    listener.Start()
    emit ()
    lock gate (fun () -> total)

[<Fact>]
let ``Distributed admit invokes under one lease and releases on settle`` () =
    let provider = StubProvider("acme", "fast")
    let registry = makeRegistry [ provider :> ILlmProvider ]
    let options = makeOptions ignore [ "acme", "sk-static" ] true
    let dist = makeDistOptions true
    let clock = FakeTimeProvider()
    let delay = RecordingDelay()
    let tracker = ParallelTracker()

    let admission =
        FakeAdmission(
            ResizeArray(
                [
                    AcquireOutcome(DistributedAdmissionOutcome.Acquired())
                ]
            )
        )

    let coordinator =
        makeDistributedCoordinator
            options
            registry
            clock
            (delay :> ILlmDelay)
            (SeededRandom(1) :> ILlmRandom)
            admission
            dist

    let sessionId = SessionId.New()
    let turnId = TurnId.New()

    let value =
        executeKnownTurn coordinator tenantA (instantInvoke tracker (usageOf 1L 1L) "ok") sessionId turnId 1
        |> fun task -> task.GetAwaiter().GetResult()

    value |> should equal "ok"

    let calls = admission.AcquireCalls
    calls.Length |> should equal 1
    let identity, owner, cap, leaseTtl, waiterTtl = calls[0]
    identity |> should equal "acme/static"
    owner.StartsWith(turnId.Value + "/attempt-1/") |> should equal true
    owner.Split('/').Length |> should equal 3
    cap |> should equal 4
    leaseTtl |> should equal (TimeSpan.FromSeconds 120.0)
    waiterTtl |> should equal (TimeSpan.FromSeconds 120.0)

    admission.Releases |> should equal [ (identity, owner) ]
    admission.Cooldowns |> should be Empty

[<Fact>]
let ``Distributed flag with Disabled mode or no client fails fast`` () =
    let provider = StubProvider("acme", "fast")
    let registry = makeRegistry [ provider :> ILlmProvider ]
    let clock = FakeTimeProvider()
    let tracker = ParallelTracker()

    // Mode Disabled with a client: names the mode, never the seam.
    let disabled = makeDistOptions true
    disabled.Mode <- DistributedCoordinationMode.Disabled

    let modeMismatch =
        makeDistributedCoordinator
            (makeOptions ignore [ "acme", "sk-static" ] true)
            registry
            clock
            (NeverDelay() :> ILlmDelay)
            (SeededRandom(1) :> ILlmRandom)
            (FakeAdmission(ResizeArray()) :> IDistributedLlmAdmission)
            disabled

    let modeCaught =
        try
            execute modeMismatch "acme/fast" tenantA 10L (instantInvoke tracker (usageOf 1L 1L) "unreached")
            |> fun task -> task.GetAwaiter().GetResult() |> ignore

            None
        with :? InvalidOperationException as failure ->
            Some failure

    modeCaught.IsSome |> should equal true

    modeCaught.Value.Message.Contains("DistributedCoordination")
    |> should equal true

    provider.Builds |> should equal 0

    // Mode Redis with no client: names the missing client.
    let missingClient =
        makeDistributedCoordinator
            (makeOptions ignore [ "acme", "sk-static" ] true)
            registry
            clock
            (NeverDelay() :> ILlmDelay)
            (SeededRandom(1) :> ILlmRandom)
            null
            (makeDistOptions true)

    let missingCaught =
        try
            execute missingClient "acme/fast" tenantA 10L (instantInvoke tracker (usageOf 1L 1L) "unreached")
            |> fun task -> task.GetAwaiter().GetResult() |> ignore

            None
        with :? InvalidOperationException as failure ->
            Some failure

    missingCaught.IsSome |> should equal true

    missingCaught.Value.Message.Contains("IDistributedLlmAdmission")
    |> should equal true

    provider.Builds |> should equal 0

[<Fact>]
let ``Fail-closed rejects without contacting the provider`` () =
    let provider = StubProvider("acme", "fast")
    let registry = makeRegistry [ provider :> ILlmProvider ]
    let options = makeOptions ignore [ "acme", "sk-static" ] true
    let dist = makeDistOptions true
    let clock = FakeTimeProvider()
    let delay = RecordingDelay()

    let admission =
        FakeAdmission(
            ResizeArray(
                [
                    AcquireThrow(InvalidOperationException("redis down"))
                ]
            )
        )

    let coordinator =
        makeDistributedCoordinator
            options
            registry
            clock
            (delay :> ILlmDelay)
            (SeededRandom(1) :> ILlmRandom)
            admission
            dist

    let attempts = ResizeArray<int>()

    let plan =
        ResizeArray<PlannedOutcome>(
            [
                SucceedWith(usageOf 1L 1L, "unreached")
            ]
        )

    let caught =
        try
            executeKnownTurn coordinator tenantA (runPlan plan attempts) (SessionId.New()) (TurnId.New()) 1
            |> fun task -> task.GetAwaiter().GetResult() |> ignore

            None
        with :? AdmissionRejectedException as rejected ->
            Some rejected

    caught.IsSome |> should equal true
    caught.Value.Reason |> should equal LlmCoordination.DistributedUnavailableReason
    // The invoke lambda never ran: the provider was never contacted.
    attempts |> List.ofSeq |> should be Empty
    // The give-up path still released the (possibly landed) lease.
    admission.AcquireCalls.Length |> should equal 1
    admission.Releases.Length |> should equal 1

[<Fact>]
let ``Fail-open admits one concurrent call with a queue of sixteen and records the metric`` () =
    let provider = StubProvider("acme", "fast")
    let registry = makeRegistry [ provider :> ILlmProvider ]
    let options = makeOptions ignore [ "acme", "sk-static" ] true
    let dist = makeDistOptions false
    let clock = FakeTimeProvider()
    let tracker = ParallelTracker()

    // Every acquire fails: every call takes the emergency path.
    let admission =
        FakeAdmission(
            ResizeArray(
                [
                    for _ in 1..32 -> AcquireThrow(InvalidOperationException("redis down"))
                ]
            )
        )

    let coordinator =
        makeDistributedCoordinator
            options
            registry
            clock
            (NeverDelay() :> ILlmDelay)
            (SeededRandom(1) :> ILlmRandom)
            admission
            dist

    let gate = new TaskCompletionSource<StringOutcome>()

    let admitted =
        countFailOpenAdmissions "acme" (fun () ->
            let holder = execute coordinator "acme/fast" tenantA 10L (gatedInvoke tracker gate)

            let waiters =
                [
                    for _ in 1..16 ->
                        execute coordinator "acme/fast" tenantA 10L (instantInvoke tracker (usageOf 1L 1L) "w")
                ]

            // One emergency slot held, sixteen queued: none settled.
            waiters |> List.forall (fun task -> not task.IsCompleted) |> should equal true

            // The seventeenth waiter overflows the emergency queue cap.
            let rejected =
                try
                    execute coordinator "acme/fast" tenantA 10L (instantInvoke tracker (usageOf 1L 1L) "unreached")
                    |> fun task -> task.GetAwaiter().GetResult() |> ignore

                    None
                with :? AdmissionRejectedException as failure ->
                    Some failure

            rejected.IsSome |> should equal true
            rejected.Value.Reason |> should equal LlmCoordination.QueueFullReason
            rejected.Value.Message.Contains("16") |> should equal true

            gate.SetResult(outcomeOf "held" 1L 1L)
            holder.GetAwaiter().GetResult() |> should equal "held"

            for waiter in waiters do
                waiter.GetAwaiter().GetResult() |> should equal "w")

    // One point per locally-admitted call: the holder plus sixteen
    // waiters. The rejected seventeenth records nothing.
    admitted |> should equal 17L

[<Fact>]
let ``Queued polls within the deadline then admits`` () =
    let provider = StubProvider("acme", "fast")
    let registry = makeRegistry [ provider :> ILlmProvider ]
    let options = makeOptions ignore [ "acme", "sk-static" ] true
    let dist = makeDistOptions true
    let clock = FakeTimeProvider()
    let recorded = ResizeArray<TimeSpan>()
    let tracker = ParallelTracker()

    let admission =
        FakeAdmission(
            ResizeArray(
                [
                    AcquireOutcome(DistributedAdmissionOutcome.Queued(0))
                    AcquireOutcome(DistributedAdmissionOutcome.Acquired())
                ]
            )
        )

    let coordinator =
        makeDistributedCoordinator
            options
            registry
            clock
            (AdvancingDelay(clock, recorded) :> ILlmDelay)
            (SeededRandom(1) :> ILlmRandom)
            admission
            dist

    let value =
        execute coordinator "acme/fast" tenantA 10L (instantInvoke tracker (usageOf 1L 1L) "ok")
        |> fun task -> task.GetAwaiter().GetResult()

    value |> should equal "ok"
    recorded |> List.ofSeq |> should equal [ TimeSpan.FromSeconds 1.0 ]
    admission.AcquireCalls.Length |> should equal 2
    // Retries hold the one lease under the one owner.
    let _, firstOwner, _, _, _ = admission.AcquireCalls[0]
    let _, secondOwner, _, _, _ = admission.AcquireCalls[1]
    secondOwner |> should equal firstOwner
    // Settle released the lease once.
    admission.Releases |> should equal [ ("acme/static", firstOwner) ]

[<Fact>]
let ``Queued past the deadline raises DeadlineExceeded`` () =
    let provider = StubProvider("acme", "fast")
    let registry = makeRegistry [ provider :> ILlmProvider ]

    let options =
        makeOptions (fun coordination -> coordination.RequestTimeoutSeconds <- 3) [ "acme", "sk-static" ] true

    let dist = makeDistOptions true
    let clock = FakeTimeProvider()
    let recorded = ResizeArray<TimeSpan>()
    let tracker = ParallelTracker()

    let admission =
        FakeAdmission(
            ResizeArray(
                [
                    for _ in 1..10 -> AcquireOutcome(DistributedAdmissionOutcome.Queued(0))
                ]
            )
        )

    let coordinator =
        makeDistributedCoordinator
            options
            registry
            clock
            (AdvancingDelay(clock, recorded) :> ILlmDelay)
            (SeededRandom(1) :> ILlmRandom)
            admission
            dist

    (fun () ->
        execute coordinator "acme/fast" tenantA 10L (instantInvoke tracker (usageOf 1L 1L) "unreached")
        |> fun task -> task.GetAwaiter().GetResult() |> ignore)
    |> should throw typeof<DeadlineExceededException>

    recorded
    |> List.ofSeq
    |> should
        equal
        [
            TimeSpan.FromSeconds 1.0
            TimeSpan.FromSeconds 1.0
        ]

    admission.AcquireCalls.Length |> should equal 3
    // The give-up path released the queued waiter entry.
    admission.Releases.Length |> should equal 1

[<Fact>]
let ``CooldownActive waits the pause then admits without touching the local cooldown`` () =
    let provider = StubProvider("acme", "fast")
    let registry = makeRegistry [ provider :> ILlmProvider ]
    let options = makeOptions ignore [ "acme", "sk-static" ] true
    let dist = makeDistOptions true
    let clock = FakeTimeProvider()
    let recorded = ResizeArray<TimeSpan>()
    let tracker = ParallelTracker()

    let admission =
        FakeAdmission(
            ResizeArray(
                [
                    AcquireOutcome(DistributedAdmissionOutcome.CooldownActive())
                    AcquireOutcome(DistributedAdmissionOutcome.Acquired())
                ]
            )
        )

    let coordinator =
        makeDistributedCoordinator
            options
            registry
            clock
            (AdvancingDelay(clock, recorded) :> ILlmDelay)
            (SeededRandom(1) :> ILlmRandom)
            admission
            dist

    let value =
        execute coordinator "acme/fast" tenantA 10L (instantInvoke tracker (usageOf 1L 1L) "ok")
        |> fun task -> task.GetAwaiter().GetResult()

    value |> should equal "ok"
    // The shorter of RateLimitCooldown (30 s) and the remaining deadline.
    recorded |> List.ofSeq |> should equal [ TimeSpan.FromSeconds 30.0 ]
    admission.AcquireCalls.Length |> should equal 2
    // No lease was ever held, so settle released exactly once; nothing
    // propagated a cooldown the seam never reported.
    admission.Releases.Length |> should equal 1
    admission.Cooldowns |> should be Empty

[<Fact>]
let ``CooldownActive past the deadline raises DeadlineExceeded`` () =
    let provider = StubProvider("acme", "fast")
    let registry = makeRegistry [ provider :> ILlmProvider ]

    let options =
        makeOptions (fun coordination -> coordination.RequestTimeoutSeconds <- 5) [ "acme", "sk-static" ] true

    let dist = makeDistOptions true
    let clock = FakeTimeProvider()
    let recorded = ResizeArray<TimeSpan>()
    let tracker = ParallelTracker()

    let admission =
        FakeAdmission(
            ResizeArray(
                [
                    for _ in 1..3 -> AcquireOutcome(DistributedAdmissionOutcome.CooldownActive())
                ]
            )
        )

    let coordinator =
        makeDistributedCoordinator
            options
            registry
            clock
            (AdvancingDelay(clock, recorded) :> ILlmDelay)
            (SeededRandom(1) :> ILlmRandom)
            admission
            dist

    (fun () ->
        execute coordinator "acme/fast" tenantA 10L (instantInvoke tracker (usageOf 1L 1L) "unreached")
        |> fun task -> task.GetAwaiter().GetResult() |> ignore)
    |> should throw typeof<DeadlineExceededException>

    // min(RateLimitCooldown 30 s, remaining 5 s).
    recorded |> List.ofSeq |> should equal [ TimeSpan.FromSeconds 5.0 ]
    admission.AcquireCalls.Length |> should equal 1
    // No lease was ever held, but the deadline give-up still releases
    // best-effort: every give-up path releases.
    admission.Releases.Length |> should equal 1

[<Fact>]
let ``Post-acquire rate trip releases the lease and waits`` () =
    let provider = StubProvider("acme", "fast")
    let registry = makeRegistry [ provider :> ILlmProvider ]

    let options =
        makeOptions (fun coordination -> coordination.RequestsPerMinute <- 1) [ "acme", "sk-static" ] true

    let dist = makeDistOptions true
    let clock = FakeTimeProvider()
    let recorded = ResizeArray<TimeSpan>()
    let tracker = ParallelTracker()

    let admission =
        FakeAdmission(
            ResizeArray(
                [
                    AcquireOutcome(DistributedAdmissionOutcome.Acquired())
                    AcquireOutcome(DistributedAdmissionOutcome.Acquired())
                    AcquireOutcome(DistributedAdmissionOutcome.Acquired())
                ]
            )
        )

    let coordinator =
        makeDistributedCoordinator
            options
            registry
            clock
            (AdvancingDelay(clock, recorded) :> ILlmDelay)
            (SeededRandom(1) :> ILlmRandom)
            admission
            dist

    let first =
        execute coordinator "acme/fast" tenantA 10L (instantInvoke tracker (usageOf 1L 1L) "one")
        |> fun task -> task.GetAwaiter().GetResult()

    first |> should equal "one"

    // The first call filled the per-process RPM window, so the second
    // call trips the post-acquire check, releases, waits out the window,
    // and re-acquires under the same owner.
    let second =
        execute coordinator "acme/fast" tenantA 10L (instantInvoke tracker (usageOf 1L 1L) "two")
        |> fun task -> task.GetAwaiter().GetResult()

    second |> should equal "two"
    recorded |> List.ofSeq |> should equal [ TimeSpan.FromMinutes 1.0 ]
    admission.AcquireCalls.Length |> should equal 3

    let _, firstOwner, _, _, _ = admission.AcquireCalls[0]
    let _, secondOwner, _, _, _ = admission.AcquireCalls[1]
    let _, thirdOwner, _, _, _ = admission.AcquireCalls[2]
    secondOwner |> should equal thirdOwner
    (secondOwner = firstOwner) |> should equal false

    admission.Releases
    |> should
        equal
        [
            ("acme/static", firstOwner)
            ("acme/static", secondOwner)
            ("acme/static", secondOwner)
        ]

[<Fact>]
let ``HTTP 429 propagates the same pause to the seam`` () =
    let provider = StubProvider("acme", "fast")
    let registry = makeRegistry [ provider :> ILlmProvider ]

    let options =
        makeOptions (fun coordination -> coordination.RequestTimeoutSeconds <- 600) [ "acme", "sk-static" ] true

    let dist = makeDistOptions true
    let clock = FakeTimeProvider()
    let recorded = ResizeArray<TimeSpan>()

    let admission =
        FakeAdmission(
            ResizeArray(
                [
                    AcquireOutcome(DistributedAdmissionOutcome.Acquired())
                ]
            )
        )

    let coordinator =
        makeDistributedCoordinator
            options
            registry
            clock
            (AdvancingDelay(clock, recorded) :> ILlmDelay)
            (SeededRandom(1) :> ILlmRandom)
            admission
            dist

    let plan =
        ResizeArray<PlannedOutcome>(
            [
                FailWith(providerFailureWithRetryAfter 429 (TimeSpan.FromSeconds 120.0))
                SucceedWith(usageOf 1L 1L, "ok")
            ]
        )

    let attempts = ResizeArray<int>()

    let value =
        execute coordinator "acme/fast" tenantA 10L (runPlan plan attempts)
        |> fun task -> task.GetAwaiter().GetResult()

    value |> should equal "ok"
    attempts |> List.ofSeq |> should equal [ 1; 2 ]
    // The retry-after outlasts the configured pause: local and seam share
    // the same 120-second pause.
    admission.Cooldowns
    |> should
        equal
        [
            ("acme/static", TimeSpan.FromSeconds 120.0)
        ]

    recorded |> List.ofSeq |> should equal [ TimeSpan.FromSeconds 120.0 ]

[<Fact>]
let ``Caller cancel during a distributed wait maps to cancel, never to fail policy`` () =
    let provider = StubProvider("acme", "fast")
    let registry = makeRegistry [ provider :> ILlmProvider ]
    let options = makeOptions ignore [ "acme", "sk-static" ] true
    let dist = makeDistOptions true
    let clock = FakeTimeProvider()
    let tracker = ParallelTracker()
    use caller = new CancellationTokenSource()

    let admission =
        FakeAdmission(
            ResizeArray(
                [
                    for _ in 1..5 -> AcquireOutcome(DistributedAdmissionOutcome.Queued(0))
                ]
            )
        )

    let coordinator =
        makeDistributedCoordinator
            options
            registry
            clock
            (CancelOnFirstDelay(caller) :> ILlmDelay)
            (SeededRandom(1) :> ILlmRandom)
            admission
            dist

    let mutable sawAdmissionRejected = false
    let mutable sawDeadline = false

    let caught =
        try
            executeWithToken
                coordinator
                "acme/fast"
                tenantA
                10L
                (instantInvoke tracker (usageOf 1L 1L) "unreached")
                caller.Token
            |> fun task -> task.GetAwaiter().GetResult() |> ignore

            None
        with
        | :? AdmissionRejectedException ->
            sawAdmissionRejected <- true
            None
        | :? DeadlineExceededException ->
            sawDeadline <- true
            None
        | :? OperationCanceledException as canceled -> Some canceled

    sawAdmissionRejected |> should equal false
    sawDeadline |> should equal false
    caught.IsSome |> should equal true
    // The cancelled wait released its queued waiter entry.
    admission.Releases.Length |> should equal 1
