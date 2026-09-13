// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.PoliciesTests

open System
open System.Text.Json
open FsUnit.Xunit
open Legate
open Xunit

let jsonOptions = JsonSerializerOptions()

// The generic Deserialize<'T> overload is annotated to return 'T | null,
// which trips FS3261 for value-bearing records; the Type-based overload
// avoids it. Same helper as PermissionTests.
let deserialize<'T> (json: string) : 'T =
    JsonSerializer.Deserialize(json, typeof<'T>) |> unbox

let sampleSessionId = SessionId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"
let sampleTurnId = TurnId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"
let sampleAgentId = AgentId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"

let sampleContext phase : SessionAdmissionContext =
    {
        Tenant = TenantId.Create "acme"
        SessionId = sampleSessionId
        AgentId = sampleAgentId
        Phase = phase
    }

let sampleCheckpoint: UsageCheckpoint =
    {
        Tenant = TenantId.Create "acme"
        SessionId = sampleSessionId
        TurnId = sampleTurnId
        Attempt = 2
        Provider = "anthropic"
        Model = "claude-sonnet"
        InputTokens = 120L
        OutputTokens = 45L
        IdempotencyKey = "ckpt-1"
    }

let sampleSettlement: UsageSettlement =
    {
        Tenant = TenantId.Create "acme"
        SessionId = sampleSessionId
        TurnId = sampleTurnId
        Attempt = 2
        Provider = "anthropic"
        Model = "claude-sonnet"
        InputTokens = 340L
        OutputTokens = 130L
        IdempotencyKey = "settled-1"
    }

/// A usage observer that records every delivery in arrival order, tagged by
/// kind, plus the payloads themselves.
type FakeUsageObserver() =
    let mutable deliveries: string list = []
    let mutable checkpoints: UsageCheckpoint list = []
    let mutable settlements: UsageSettlement list = []

    interface IUsageObserver with
        member _.OnCheckpoint(usage: UsageCheckpoint) =
            deliveries <- "checkpoint" :: deliveries
            checkpoints <- usage :: checkpoints

        member _.OnSettled(usage: UsageSettlement) =
            deliveries <- "settled" :: deliveries
            settlements <- usage :: settlements

    /// The delivery kinds in arrival order.
    member _.DeliveryOrder = List.rev deliveries

    /// The checkpoints in arrival order.
    member _.Checkpoints = List.rev checkpoints

    /// The settlements in arrival order.
    member _.Settlements = List.rev settlements

// ───────────────────────────────────────────────────────────────────────────
// SessionAdmissionDecision discriminators

[<Fact>]
let ``Allow round-trips to AdmissionAllowed via $type`` () =
    let json = JsonSerializer.Serialize(SessionAdmissionDecision.Allow, jsonOptions)

    json.Contains("\"$type\":\"allowed\"") |> should equal true

    match JsonSerializer.Deserialize<SessionAdmissionDecision>(json, jsonOptions) with
    | null -> failwith "deserialised to null"
    | restored -> (restored :? AdmissionAllowed) |> should equal true

[<Fact>]
let ``Reject round-trips to AdmissionRejected carrying its reason via $type`` () =
    let json =
        JsonSerializer.Serialize(SessionAdmissionDecision.Reject("tenant is over quota"), jsonOptions)

    json.Contains("\"$type\":\"rejected\"") |> should equal true

    match JsonSerializer.Deserialize<SessionAdmissionDecision>(json, jsonOptions) with
    | null -> failwith "deserialised to null"
    | restored ->
        (restored :? AdmissionRejected) |> should equal true

        (restored :?> AdmissionRejected).Reason |> should equal "tenant is over quota"

[<Fact>]
let ``AdmissionRejected carries the reason it was built with`` () =
    let rejected =
        SessionAdmissionDecision.Reject("maintenance window") :?> AdmissionRejected

    rejected.Reason |> should equal "maintenance window"

// ───────────────────────────────────────────────────────────────────────────
// SessionAdmissionContext JSON

[<Fact>]
let ``SessionAdmissionContext JSON round-trip preserves every field`` () =
    let context = sampleContext SessionAdmissionPhase.Prompt

    let json = JsonSerializer.Serialize(context, jsonOptions)
    let restored = deserialize<SessionAdmissionContext> json

    restored.Tenant |> should equal context.Tenant
    restored.SessionId |> should equal context.SessionId
    restored.AgentId |> should equal context.AgentId
    restored.Phase |> should equal SessionAdmissionPhase.Prompt

[<Fact>]
let ``SessionAdmissionContext JSON serialises identifiers as plain strings`` () =
    let document =
        JsonSerializer.Serialize(sampleContext SessionAdmissionPhase.Open, jsonOptions)
        |> JsonDocument.Parse

    document.RootElement.GetProperty("Tenant").GetString() |> should equal "acme"

    document.RootElement.GetProperty("SessionId").GetString()
    |> should equal "01ARZ3NDEKTSV4RRFFQ69G5FAV"

    document.RootElement.GetProperty("AgentId").GetString()
    |> should equal "01ARZ3NDEKTSV4RRFFQ69G5FAV"

    document.RootElement.GetProperty("Phase").GetInt32() |> should equal 0

// ───────────────────────────────────────────────────────────────────────────
// ISessionAdmissionPolicy

/// An admission policy that returns the decision it was configured with and
/// records the contexts it saw.
type FakeAdmissionPolicy(decision: SessionAdmissionDecision) =
    let mutable contexts: SessionAdmissionContext list = []

    interface ISessionAdmissionPolicy with
        member _.Authorize(context: SessionAdmissionContext) =
            contexts <- context :: contexts
            decision

    /// The contexts Authorize received, oldest first.
    member _.Contexts = List.rev contexts

[<Fact>]
let ``An admission policy receives the context and returns its decision`` () =
    let fake =
        FakeAdmissionPolicy(SessionAdmissionDecision.Reject("closed for maintenance"))

    let policy = fake :> ISessionAdmissionPolicy
    let context = sampleContext SessionAdmissionPhase.Open

    match policy.Authorize(context) with
    | :? AdmissionRejected as rejected -> rejected.Reason |> should equal "closed for maintenance"
    | other -> failwithf "expected AdmissionRejected, got %A" other

    fake.Contexts |> should equal [ context ]

[<Fact>]
let ``AllowAllAdmissionPolicy admits every phase`` () =
    let policy = AllowAllAdmissionPolicy() :> ISessionAdmissionPolicy

    for phase in
        [
            SessionAdmissionPhase.Open
            SessionAdmissionPhase.Prompt
        ] do
        match policy.Authorize(sampleContext phase) with
        | :? AdmissionAllowed -> ()
        | other -> failwithf "expected AdmissionAllowed for %A, got %A" phase other

// ───────────────────────────────────────────────────────────────────────────
// UsageCheckpoint and UsageSettlement JSON

[<Fact>]
let ``UsageCheckpoint JSON round-trip preserves every field`` () =
    let json = JsonSerializer.Serialize(sampleCheckpoint, jsonOptions)
    let restored = deserialize<UsageCheckpoint> json

    restored.Tenant |> should equal sampleCheckpoint.Tenant
    restored.SessionId |> should equal sampleCheckpoint.SessionId
    restored.TurnId |> should equal sampleCheckpoint.TurnId
    restored.Attempt |> should equal 2
    restored.Provider |> should equal "anthropic"
    restored.Model |> should equal "claude-sonnet"
    restored.InputTokens |> should equal 120L
    restored.OutputTokens |> should equal 45L
    restored.IdempotencyKey |> should equal "ckpt-1"

[<Fact>]
let ``UsageSettlement JSON round-trip preserves every field`` () =
    let json = JsonSerializer.Serialize(sampleSettlement, jsonOptions)
    let restored = deserialize<UsageSettlement> json

    restored.Tenant |> should equal sampleSettlement.Tenant
    restored.SessionId |> should equal sampleSettlement.SessionId
    restored.TurnId |> should equal sampleSettlement.TurnId
    restored.Attempt |> should equal 2
    restored.Provider |> should equal "anthropic"
    restored.Model |> should equal "claude-sonnet"
    restored.InputTokens |> should equal 340L
    restored.OutputTokens |> should equal 130L
    restored.IdempotencyKey |> should equal "settled-1"

[<Fact>]
let ``UsageCheckpoint and UsageSettlement are distinct types`` () =
    // The settled case must be statically distinct so a host cannot mistake
    // a checkpoint for the settled summary.
    typeof<UsageCheckpoint> |> should not' (equal typeof<UsageSettlement>)

// ───────────────────────────────────────────────────────────────────────────
// IUsageObserver

[<Fact>]
let ``An observer records checkpoint and settled deliveries in order`` () =
    let fake = FakeUsageObserver()
    let observer = fake :> IUsageObserver

    observer.OnCheckpoint sampleCheckpoint

    observer.OnCheckpoint
        { sampleCheckpoint with
            IdempotencyKey = "ckpt-2"
        }

    observer.OnSettled sampleSettlement

    fake.DeliveryOrder
    |> should
        equal
        [
            "checkpoint"
            "checkpoint"
            "settled"
        ]

    fake.Checkpoints
    |> should
        equal
        [
            sampleCheckpoint
            { sampleCheckpoint with
                IdempotencyKey = "ckpt-2"
            }
        ]

    fake.Settlements |> should equal [ sampleSettlement ]

[<Fact>]
let ``Deliveries carry distinct idempotency keys per delivery`` () =
    let fake = FakeUsageObserver()
    let observer = fake :> IUsageObserver

    observer.OnCheckpoint sampleCheckpoint
    observer.OnSettled sampleSettlement

    fake.Checkpoints
    |> List.map (fun c -> c.IdempotencyKey)
    |> should equal [ "ckpt-1" ]

    fake.Settlements
    |> List.map (fun s -> s.IdempotencyKey)
    |> should equal [ "settled-1" ]

// ───────────────────────────────────────────────────────────────────────────
// ModelPolicyDecision and IModelPolicy

[<Fact>]
let ``Model Allow round-trips to ModelAllowed via $type`` () =
    let json = JsonSerializer.Serialize(ModelPolicyDecision.Allow, jsonOptions)

    json.Contains("\"$type\":\"allowed\"") |> should equal true

    match JsonSerializer.Deserialize<ModelPolicyDecision>(json, jsonOptions) with
    | null -> failwith "deserialised to null"
    | restored -> (restored :? ModelAllowed) |> should equal true

[<Fact>]
let ``Model Deny round-trips to ModelDenied carrying its message via $type`` () =
    let json =
        JsonSerializer.Serialize(ModelPolicyDecision.Deny("model is not on your plan"), jsonOptions)

    json.Contains("\"$type\":\"denied\"") |> should equal true

    match JsonSerializer.Deserialize<ModelPolicyDecision>(json, jsonOptions) with
    | null -> failwith "deserialised to null"
    | restored ->
        (restored :? ModelDenied) |> should equal true

        (restored :?> ModelDenied).Message |> should equal "model is not on your plan"

[<Fact>]
let ``ModelDenied carries the client-safe message it was built with`` () =
    let denied = ModelPolicyDecision.Deny("model is not on your plan") :?> ModelDenied

    denied.Message |> should equal "model is not on your plan"

/// A model policy that returns the decision it was configured with and
/// records the calls it saw.
type FakeModelPolicy(decision: ModelPolicyDecision) =
    let mutable calls: (TenantId * string * string) list = []

    interface IModelPolicy with
        member _.Authorize(tenant: TenantId, provider: string, model: string) =
            calls <- (tenant, provider, model) :: calls
            decision

    /// The calls Authorize received, oldest first.
    member _.Calls = List.rev calls

[<Fact>]
let ``A model policy receives tenant, provider, and model and returns its decision`` () =
    let fake = FakeModelPolicy(ModelPolicyDecision.Deny("model is not on your plan"))
    let policy = fake :> IModelPolicy

    match policy.Authorize(TenantId.Default, "anthropic", "claude-sonnet") with
    | :? ModelDenied as denied -> denied.Message |> should equal "model is not on your plan"
    | other -> failwithf "expected ModelDenied, got %A" other

    fake.Calls
    |> should
        equal
        [
            (TenantId.Default, "anthropic", "claude-sonnet")
        ]

[<Fact>]
let ``AllowAllModelPolicy allows every tenant, provider, and model`` () =
    let policy = AllowAllModelPolicy() :> IModelPolicy

    match policy.Authorize(TenantId.Create "acme", "openai", "gpt-4o") with
    | :? ModelAllowed -> ()
    | other -> failwithf "expected ModelAllowed, got %A" other

    match policy.Authorize(TenantId.Default, "anthropic", "claude-sonnet") with
    | :? ModelAllowed -> ()
    | other -> failwithf "expected ModelAllowed, got %A" other
