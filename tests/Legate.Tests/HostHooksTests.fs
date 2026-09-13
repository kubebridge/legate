// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.HostHooksTests

open System
open System.Collections.Generic
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Xunit

let jsonOptions = JsonSerializerOptions()

// The generic Deserialize<'T> overload is annotated to return 'T | null,
// which trips FS3261 for value-bearing records; the Type-based overload
// avoids it. Same helper as PoliciesTests.
let deserialize<'T> (json: string) : 'T =
    JsonSerializer.Deserialize(json, typeof<'T>) |> unbox

let sampleSessionId = SessionId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"
let sampleTurnId = TurnId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"
let stamp = DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero)

let sampleTurnResult: TurnResult =
    {
        AssistantText = "done"
        Status = TurnStatus.Completed
        Iterations = 3
        Usage =
            {
                InputTokens = 120L
                OutputTokens = 45L
            }
        Outcome = null
    }

let sampleMetadata () =
    let table = Dictionary<string, string>()
    table["correlation"] <- "webhook-1"
    table :> IReadOnlyDictionary<string, string>

let sampleCompletion () : SessionCompletion =
    {
        SessionId = sampleSessionId
        TurnResult = sampleTurnResult
        Metadata = sampleMetadata ()
        IdempotencyKey = "completion-1"
    }

let sampleRequest: ArtifactQuotaRequest =
    {
        Tenant = TenantId.Create "acme"
        SessionId = sampleSessionId
        RequestedBytes = 2048L
    }

let sampleAgent () =
    {
        Id = AgentId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"
        Tenant = TenantId.Create "acme"
        Name = "checkout"
        Description = null
        Model = ModelReference.Parse "anthropic/claude-sonnet"
        SystemPrompt = "You help with checkout."
        EnvironmentVariables = null
        PermissionDefaults = null
        ToolSelection = null
        PackageReference = null
        Enabled = true
        Schedule = null
        RowVersion = 0UL
        CreatedAt = stamp
        UpdatedAt = stamp
    }

/// Awaits a task from the xUnit sync context.
let await (task: Task) = task.GetAwaiter().GetResult()

/// Awaits a Task<ArtifactQuotaDecision> to its decision.
let awaitDecision (task: Task<ArtifactQuotaDecision>) : ArtifactQuotaDecision = task.GetAwaiter().GetResult()

/// A completion sink that records every delivery it receives.
type FakeCompletionSink() =
    let mutable completions: SessionCompletion list = []

    interface ISessionCompletionSink with
        member _.Notify(completion: SessionCompletion) =
            completions <- completion :: completions

    /// The completions in arrival order.
    member _.Completions = List.rev completions

/// An audit sink that records the agents it was notified about.
type FakeAuditSink() =
    let mutable notified: Agent list = []

    interface IAgentAuditSink with
        member _.OnAgentChanged(agent: Agent, _cancellationToken: CancellationToken) =
            notified <- agent :: notified
            Task.CompletedTask

    /// The agents in arrival order.
    member _.Notified = List.rev notified

/// An artifact quota with an in-memory ledger, enough to exercise the
/// reserve/commit/release lifecycle from outside the assembly.
type CountingQuota(freeBytes: int64) =
    let mutable remaining = freeBytes
    let mutable committed = 0L

    interface IArtifactQuota with
        member _.Reserve
            (request: ArtifactQuotaRequest, _cancellationToken: CancellationToken)
            : Task<ArtifactQuotaDecision> =
            if request.RequestedBytes <= remaining then
                remaining <- remaining - request.RequestedBytes
                Task.FromResult(ArtifactQuotaDecision.Grant(sprintf "res-%d" request.RequestedBytes))
            else
                Task.FromResult(ArtifactQuotaDecision.Exhausted(request.RequestedBytes, remaining))

        member _.Commit(_reservationId: string, _cancellationToken: CancellationToken) : Task =
            committed <- committed + 1L
            Task.CompletedTask

        member _.Release(_reservationId: string, _cancellationToken: CancellationToken) : Task =
            remaining <- remaining + freeBytes
            Task.CompletedTask

    /// How many commit calls landed.
    member _.Commits = committed

// ───────────────────────────────────────────────────────────────────────────
// SessionCompletion JSON

[<Fact>]
let ``SessionCompletion JSON round-trip preserves every field`` () =
    let completion = sampleCompletion ()

    let json = JsonSerializer.Serialize(completion, jsonOptions)
    let restored = deserialize<SessionCompletion> json

    restored.SessionId |> should equal completion.SessionId
    restored.TurnResult.AssistantText |> should equal "done"
    restored.TurnResult.Status |> should equal TurnStatus.Completed
    restored.TurnResult.Iterations |> should equal 3
    restored.TurnResult.Usage.InputTokens |> should equal 120L
    restored.TurnResult.Usage.OutputTokens |> should equal 45L

    let metadata = restored.Metadata |> Option.ofObj
    metadata.Value["correlation"] |> should equal "webhook-1"

    restored.IdempotencyKey |> should equal "completion-1"

[<Fact>]
let ``SessionCompletion JSON serialises identifiers as plain strings`` () =
    let document =
        JsonSerializer.Serialize(sampleCompletion (), jsonOptions) |> JsonDocument.Parse

    document.RootElement.GetProperty("SessionId").GetString()
    |> should equal "01ARZ3NDEKTSV4RRFFQ69G5FAV"

    document.RootElement.GetProperty("IdempotencyKey").GetString()
    |> should equal "completion-1"

[<Fact>]
let ``SessionCompletion JSON serialises the turn result's status as a number`` () =
    let document =
        JsonSerializer.Serialize(sampleCompletion (), jsonOptions) |> JsonDocument.Parse

    document.RootElement.GetProperty("TurnResult").GetProperty("Status").GetInt32()
    |> should equal 3

// ───────────────────────────────────────────────────────────────────────────
// ISessionCompletionSink

[<Fact>]
let ``A sink receives every completion delivered to it`` () =
    let fake = FakeCompletionSink()
    let sink = fake :> ISessionCompletionSink

    sink.Notify(sampleCompletion ())

    sink.Notify
        { sampleCompletion () with
            IdempotencyKey = "completion-2"
        }

    fake.Completions
    |> List.map (fun c -> c.IdempotencyKey)
    |> should equal [ "completion-1"; "completion-2" ]

    fake.Completions[0].SessionId |> should equal sampleSessionId

[<Fact>]
let ``Deliveries carry the idempotency key sinks deduplicate on`` () =
    let fake = FakeCompletionSink()
    let sink = fake :> ISessionCompletionSink

    sink.Notify(sampleCompletion ())

    fake.Completions[0].IdempotencyKey |> should equal "completion-1"

[<Fact>]
let ``SessionOptions.CompletionSink keeps the ISessionCompletionSink type identity`` () =
    let fake = FakeCompletionSink()
    let sink = fake :> ISessionCompletionSink

    let options = SessionOptions(CompletionSink = sink)

    options.CompletionSink |> should equal sink

    match options.CompletionSink with
    | null -> failwith "CompletionSink was null"
    | stored -> box stored :? ISessionCompletionSink |> should equal true

// ───────────────────────────────────────────────────────────────────────────
// IAgentAuditSink

[<Fact>]
let ``An audit sink records the agents it is notified about`` () =
    let fake = FakeAuditSink()
    let sink = fake :> IAgentAuditSink
    let agent = sampleAgent ()

    sink.OnAgentChanged(agent, CancellationToken.None) |> await

    fake.Notified |> should equal [ agent ]

[<Fact>]
let ``An audit sink receives the agent after the change`` () =
    let fake = FakeAuditSink()
    let sink = fake :> IAgentAuditSink

    let stored = { sampleAgent () with Name = "renamed" }

    sink.OnAgentChanged(stored, CancellationToken.None) |> await

    fake.Notified[0].Name |> should equal "renamed"

// ───────────────────────────────────────────────────────────────────────────
// ArtifactQuotaDecision discriminators

[<Fact>]
let ``Grant round-trips to QuotaGranted via $type`` () =
    let json =
        JsonSerializer.Serialize(ArtifactQuotaDecision.Grant "res-1", jsonOptions)

    json.Contains("\"$type\":\"quotaGranted\"") |> should equal true

    match JsonSerializer.Deserialize<ArtifactQuotaDecision>(json, jsonOptions) with
    | null -> failwith "deserialised to null"
    | restored ->
        (restored :? QuotaGranted) |> should equal true
        (restored :?> QuotaGranted).ReservationId |> should equal "res-1"

[<Fact>]
let ``Exhausted round-trips to QuotaExhausted carrying its sizes via $type`` () =
    let json =
        JsonSerializer.Serialize(ArtifactQuotaDecision.Exhausted(2048L, 512L), jsonOptions)

    json.Contains("\"$type\":\"quotaExhausted\"") |> should equal true

    match JsonSerializer.Deserialize<ArtifactQuotaDecision>(json, jsonOptions) with
    | null -> failwith "deserialised to null"
    | restored ->
        (restored :? QuotaExhausted) |> should equal true

        let exhausted = restored :?> QuotaExhausted
        exhausted.RequestedBytes |> should equal 2048L
        exhausted.AllowedBytes |> should equal 512L

// ───────────────────────────────────────────────────────────────────────────
// ArtifactQuotaRequest JSON

[<Fact>]
let ``ArtifactQuotaRequest JSON round-trip preserves every field`` () =
    let json = JsonSerializer.Serialize(sampleRequest, jsonOptions)
    let restored = deserialize<ArtifactQuotaRequest> json

    restored.Tenant |> should equal sampleRequest.Tenant
    restored.SessionId |> should equal sampleRequest.SessionId
    restored.RequestedBytes |> should equal 2048L

[<Fact>]
let ``ArtifactQuotaRequest JSON serialises identifiers as plain strings`` () =
    let document =
        JsonSerializer.Serialize(sampleRequest, jsonOptions) |> JsonDocument.Parse

    document.RootElement.GetProperty("Tenant").GetString() |> should equal "acme"

    document.RootElement.GetProperty("SessionId").GetString()
    |> should equal "01ARZ3NDEKTSV4RRFFQ69G5FAV"

    document.RootElement.GetProperty("RequestedBytes").GetInt64()
    |> should equal 2048L

// ───────────────────────────────────────────────────────────────────────────
// IArtifactQuota

[<Fact>]
let ``A quota grants a fitting reservation and receives its commit`` () =
    let fake = CountingQuota(4096L)
    let quota = fake :> IArtifactQuota

    match quota.Reserve(sampleRequest, CancellationToken.None) |> awaitDecision with
    | :? QuotaGranted as granted -> granted.ReservationId |> should equal "res-2048"
    | other -> failwithf "expected QuotaGranted, got %A" other

    quota.Commit("res-2048", CancellationToken.None) |> await
    fake.Commits |> should equal 1L

[<Fact>]
let ``A quota denies a reservation that exceeds its free bytes`` () =
    let fake = CountingQuota(512L)
    let quota = fake :> IArtifactQuota

    match quota.Reserve(sampleRequest, CancellationToken.None) |> awaitDecision with
    | :? QuotaExhausted as exhausted ->
        exhausted.RequestedBytes |> should equal 2048L
        exhausted.AllowedBytes |> should equal 512L
    | other -> failwithf "expected QuotaExhausted, got %A" other

[<Fact>]
let ``A released reservation gives its bytes back`` () =
    let fake = CountingQuota(4096L)
    let quota = fake :> IArtifactQuota

    quota.Release("res-2048", CancellationToken.None) |> await

    match
        quota.Reserve(
            {
                RequestedBytes = 4096L
                Tenant = TenantId.Create "acme"
                SessionId = sampleSessionId
            },
            CancellationToken.None
        )
        |> awaitDecision
    with
    | :? QuotaGranted -> ()
    | other -> failwithf "expected QuotaGranted after release, got %A" other

// ───────────────────────────────────────────────────────────────────────────
// UnlimitedArtifactQuota

[<Fact>]
let ``UnlimitedArtifactQuota grants every reservation`` () =
    let quota = UnlimitedArtifactQuota() :> IArtifactQuota

    match quota.Reserve(sampleRequest, CancellationToken.None) |> awaitDecision with
    | :? QuotaGranted -> ()
    | other -> failwithf "expected QuotaGranted, got %A" other

    match
        quota.Reserve(
            { sampleRequest with
                RequestedBytes = 9223372036854775807L
            },
            CancellationToken.None
        )
        |> awaitDecision
    with
    | :? QuotaGranted -> ()
    | other -> failwithf "expected QuotaGranted, got %A" other

[<Fact>]
let ``UnlimitedArtifactQuota commit and release are no-ops`` () =
    let quota = UnlimitedArtifactQuota() :> IArtifactQuota

    quota.Commit("any", CancellationToken.None) |> await
    quota.Release("any", CancellationToken.None) |> await

    match quota.Reserve(sampleRequest, CancellationToken.None) |> awaitDecision with
    | :? QuotaGranted -> ()
    | other -> failwithf "expected QuotaGranted, got %A" other

[<Fact>]
let ``UnlimitedArtifactQuota never yields QuotaExhausted`` () =
    let quota = UnlimitedArtifactQuota() :> IArtifactQuota

    for size in [ 0L; 1L; 1024L; 9223372036854775807L ] do
        match
            quota.Reserve(
                { sampleRequest with
                    RequestedBytes = size
                },
                CancellationToken.None
            )
            |> awaitDecision
        with
        | :? QuotaExhausted -> failwithf "unlimited quota denied %d bytes" size
        | _ -> ()
