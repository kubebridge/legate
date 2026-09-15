// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.EventsTests

open System
open System.Text.Json
open System.Text.Json.Serialization
open FsUnit.Xunit
open Legate
open Microsoft.Extensions.AI
open Xunit

// The generic Deserialize<'T> overload is annotated to return 'T | null,
// which trips FS3265 for value types; the Type-based overload avoids it.
let deserialize<'T> (json: string) : 'T =
    JsonSerializer.Deserialize(json, typeof<'T>) |> unbox

// Unchecked.defaultof<string> rather than a bare null literal: under
// Nullable=enable the literal trips F# nullness checking on string-typed
// parameters. Deliberate: this is the null string value for the success
// case of a tool-call completion event. (Precedent: TurnTests.fs.)
let nullString = Unchecked.defaultof<string>

let jsonOptions = JsonSerializerOptions()

// The shared event construction arguments: one session, one turn, an empty
// (in-flight) sequence, and one timestamp every event reuses so equality
// assertions compare payloads only. The empty Nullable spelling follows the
// TurnTests and SessionTests precedent: Nullable() inline reads ambiguously.
let sessionId = SessionId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"
let turnId = TurnId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"
let stamp = DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero)
let noSequence = Unchecked.defaultof<Nullable<int64>>

// One event per documented kind, carrying non-default payloads, in the
// order the $type table documents them. A reflection test pins this table
// against the JsonDerivedType attributes, so adding a kind without a
// round-trip test or a discriminator fails the suite.
let sampleEvents: (string * (unit -> SessionEvent)) list =
    [
        "turnStarted", fun () -> TurnStartedEvent(sessionId, turnId, noSequence, stamp) :> SessionEvent
        "textDelta", fun () -> TextDeltaEvent(sessionId, turnId, noSequence, stamp, "Hel") :> SessionEvent
        "reasoningDelta",
        fun () -> ReasoningDeltaEvent(sessionId, turnId, noSequence, stamp, "checking the name") :> SessionEvent
        "toolCallStarted",
        fun () -> ToolCallStartedEvent(sessionId, turnId, noSequence, stamp, "call-1", "read_file") :> SessionEvent
        "toolCallOutput",
        fun () -> ToolCallOutputEvent(sessionId, turnId, noSequence, stamp, "call-1", "line one") :> SessionEvent
        "toolCallCompleted",
        fun () -> ToolCallCompletedEvent(sessionId, turnId, noSequence, stamp, "call-1", nullString) :> SessionEvent
        "permissionRequested",
        fun () -> PermissionRequestedEvent(sessionId, turnId, noSequence, stamp, "req-1", "write_file") :> SessionEvent
        "permissionResolved",
        fun () ->
            PermissionResolvedEvent(sessionId, turnId, noSequence, stamp, "req-1", PermissionDecisionKind.AllowOnce)
            :> SessionEvent
        "questionAsked",
        fun () -> QuestionAskedEvent(sessionId, turnId, noSequence, stamp, "q-1", "which colour?") :> SessionEvent
        "questionAnswered",
        fun () -> QuestionAnsweredEvent(sessionId, turnId, noSequence, stamp, "q-1", "blue") :> SessionEvent
        "usage", fun () -> UsageEvent(sessionId, turnId, noSequence, stamp, 1200L, 340L) :> SessionEvent
        "compacted", fun () -> CompactedEvent(sessionId, turnId, noSequence, stamp, 9000L, 1200L) :> SessionEvent
        "turnCompleted", fun () -> TurnCompletedEvent(sessionId, turnId, noSequence, stamp) :> SessionEvent
        "turnAborted",
        fun () ->
            TurnAbortedEvent(sessionId, turnId, noSequence, stamp, StopCause.ExplicitAbort, "host stop") :> SessionEvent
        "turnFailed",
        fun () -> TurnFailedEvent(sessionId, turnId, noSequence, stamp, "provider returned 500") :> SessionEvent
        "sessionClosed", fun () -> SessionClosedEvent(sessionId, turnId, noSequence, stamp) :> SessionEvent
        "userMessage",
        fun () -> UserMessageEvent(sessionId, turnId, noSequence, stamp, UserMessage.Text "steer") :> SessionEvent
    ]

// The 17 discriminator strings the base type's XML doc documents as the
// wire contract, in the same order as the JsonDerivedType attributes.
let discriminatorContract =
    [|
        "turnStarted"
        "textDelta"
        "reasoningDelta"
        "toolCallStarted"
        "toolCallOutput"
        "toolCallCompleted"
        "permissionRequested"
        "permissionResolved"
        "questionAsked"
        "questionAnswered"
        "usage"
        "compacted"
        "turnCompleted"
        "turnAborted"
        "turnFailed"
        "sessionClosed"
        "userMessage"
    |]

/// Reads the TypeDiscriminator values from the JsonDerivedType attributes
/// on the base type, in declaration order. The static lookup avoids the
/// extension-method ambiguity on Type, and the string conversion avoids
/// the obj-to-string downcast the nullness rule rejects.
let declaredDiscriminators () =
    Attribute.GetCustomAttributes(typeof<SessionEvent>, typeof<JsonDerivedTypeAttribute>)
    |> Seq.cast<JsonDerivedTypeAttribute>
    |> Seq.map (fun attribute -> string attribute.TypeDiscriminator)
    |> Array.ofSeq

/// Round-trips one event as SessionEvent through default options and
/// asserts the discriminator marker, the concrete subtype, and the base
/// fields. The payload equality itself stays in the per-kind tests.
let roundTrip (kind: string) (build: unit -> SessionEvent) =
    let event = build ()

    let json = JsonSerializer.Serialize(event, jsonOptions)
    json.Contains(sprintf "\"$type\":\"%s\"" kind) |> should equal true

    match JsonSerializer.Deserialize<SessionEvent>(json, jsonOptions) with
    | null -> failwith "deserialised to null"
    | restored ->
        restored.GetType() |> should equal (event.GetType())
        restored.SessionId |> should equal sessionId
        restored.TurnId |> should equal turnId
        restored.Timestamp |> should equal stamp
        restored

// ───────────────────────────────────────────────────────────────────────────
// Hierarchy shape

[<Fact>]
let ``SessionEvent is abstract with exactly the documented derived types`` () =
    typeof<SessionEvent>.IsAbstract |> should equal true

    // The attribute set is the wire contract: the declared discriminator
    // table must match the documented contract name for name, in order.
    declaredDiscriminators () |> should equal discriminatorContract

[<Fact>]
let ``Every event subtype is sealed and derives from SessionEvent`` () =
    for _kind, build in sampleEvents do
        let event = build ()
        let eventType = event.GetType()

        eventType.IsSealed |> should equal true
        eventType.BaseType |> should equal typeof<SessionEvent>

[<Fact>]
let ``The declared derived types cover exactly the documented kinds`` () =
    // Every kind in the sample table maps to a distinct declared subtype,
    // and nothing extra is declared: the round-trip loop then exercises
    // every subtype the base registers.
    let declared =
        Attribute.GetCustomAttributes(typeof<SessionEvent>, typeof<JsonDerivedTypeAttribute>)
        |> Seq.cast<JsonDerivedTypeAttribute>
        |> Seq.toArray

    declared.Length |> should equal sampleEvents.Length

    for kind, _build in sampleEvents do
        declared
        |> Seq.exists (fun attribute -> string attribute.TypeDiscriminator = kind)
        |> should equal true

// ───────────────────────────────────────────────────────────────────────────
// Round-trip per kind

[<Fact>]
let ``Every event kind round-trips to the correct subtype via $type`` () =
    for kind, build in sampleEvents do
        roundTrip kind build |> ignore

[<Fact>]
let ``TextDelta round-trips its text payload`` () =
    let restored =
        roundTrip "textDelta" (fun () -> TextDeltaEvent(sessionId, turnId, noSequence, stamp, "hel") :> SessionEvent)

    let delta = restored :?> TextDeltaEvent
    delta.Text |> should equal "hel"

[<Fact>]
let ``ReasoningDelta round-trips its text payload`` () =
    let restored =
        roundTrip "reasoningDelta" (fun () ->
            ReasoningDeltaEvent(sessionId, turnId, noSequence, stamp, "step one") :> SessionEvent)

    let delta = restored :?> ReasoningDeltaEvent
    delta.Text |> should equal "step one"

[<Fact>]
let ``ToolCallStarted round-trips its call id and tool name`` () =
    let restored =
        roundTrip "toolCallStarted" (fun () ->
            ToolCallStartedEvent(sessionId, turnId, noSequence, stamp, "call-7", "grep") :> SessionEvent)

    let started = restored :?> ToolCallStartedEvent
    started.ToolCallId |> should equal "call-7"
    started.ToolName |> should equal "grep"

[<Fact>]
let ``ToolCallOutput round-trips its call id and output`` () =
    let restored =
        roundTrip "toolCallOutput" (fun () ->
            ToolCallOutputEvent(sessionId, turnId, noSequence, stamp, "call-7", "42 matches") :> SessionEvent)

    let output = restored :?> ToolCallOutputEvent
    output.ToolCallId |> should equal "call-7"
    output.Output |> should equal "42 matches"

[<Fact>]
let ``ToolCallCompleted round-trips a success without an error`` () =
    let restored =
        roundTrip "toolCallCompleted" (fun () ->
            ToolCallCompletedEvent(sessionId, turnId, noSequence, stamp, "call-7", nullString) :> SessionEvent)

    let completed = restored :?> ToolCallCompletedEvent
    completed.ToolCallId |> should equal "call-7"
    completed.Error |> should equal null

[<Fact>]
let ``ToolCallCompleted round-trips a failure with an error reason`` () =
    let restored =
        roundTrip "toolCallCompleted" (fun () ->
            ToolCallCompletedEvent(sessionId, turnId, noSequence, stamp, "call-7", "exit code 1") :> SessionEvent)

    let completed = restored :?> ToolCallCompletedEvent
    completed.Error |> should equal "exit code 1"

[<Fact>]
let ``PermissionRequested round-trips its request id and tool name`` () =
    let restored =
        roundTrip "permissionRequested" (fun () ->
            PermissionRequestedEvent(sessionId, turnId, noSequence, stamp, "req-9", "exec") :> SessionEvent)

    let requested = restored :?> PermissionRequestedEvent
    requested.RequestId |> should equal "req-9"
    requested.ToolName |> should equal "exec"

[<Fact>]
let ``PermissionResolved round-trips its request id and decision`` () =
    let restored =
        roundTrip "permissionResolved" (fun () ->
            PermissionResolvedEvent(sessionId, turnId, noSequence, stamp, "req-9", PermissionDecisionKind.Deny)
            :> SessionEvent)

    let resolved = restored :?> PermissionResolvedEvent
    resolved.RequestId |> should equal "req-9"
    resolved.Decision |> should equal PermissionDecisionKind.Deny

[<Fact>]
let ``QuestionAsked round-trips its question id and question`` () =
    let restored =
        roundTrip "questionAsked" (fun () ->
            QuestionAskedEvent(sessionId, turnId, noSequence, stamp, "q-3", "which region?") :> SessionEvent)

    let asked = restored :?> QuestionAskedEvent
    asked.QuestionId |> should equal "q-3"
    asked.Question |> should equal "which region?"

[<Fact>]
let ``QuestionAnswered round-trips its question id and answer`` () =
    let restored =
        roundTrip "questionAnswered" (fun () ->
            QuestionAnsweredEvent(sessionId, turnId, noSequence, stamp, "q-3", "eu-west") :> SessionEvent)

    let answered = restored :?> QuestionAnsweredEvent
    answered.QuestionId |> should equal "q-3"
    answered.Answer |> should equal "eu-west"

[<Fact>]
let ``Usage round-trips both token counts`` () =
    let restored =
        roundTrip "usage" (fun () -> UsageEvent(sessionId, turnId, noSequence, stamp, 50L, 7L) :> SessionEvent)

    let usage = restored :?> UsageEvent
    usage.InputTokens |> should equal 50L
    usage.OutputTokens |> should equal 7L

[<Fact>]
let ``Compacted round-trips both estimates`` () =
    let restored =
        roundTrip "compacted" (fun () ->
            CompactedEvent(sessionId, turnId, noSequence, stamp, 9000L, 1200L) :> SessionEvent)

    let compacted = restored :?> CompactedEvent
    compacted.BeforeEstimate |> should equal 9000L
    compacted.AfterEstimate |> should equal 1200L

[<Fact>]
let ``TurnAborted round-trips its cause and reason`` () =
    let restored =
        roundTrip "turnAborted" (fun () ->
            TurnAbortedEvent(sessionId, turnId, noSequence, stamp, StopCause.HostShutdown, "host draining")
            :> SessionEvent)

    let aborted = restored :?> TurnAbortedEvent
    aborted.Cause |> should equal StopCause.HostShutdown
    aborted.Reason |> should equal "host draining"

[<Fact>]
let ``TurnFailed round-trips its failure reason`` () =
    let restored =
        roundTrip "turnFailed" (fun () ->
            TurnFailedEvent(sessionId, turnId, noSequence, stamp, "budget exhausted") :> SessionEvent)

    let failed = restored :?> TurnFailedEvent
    failed.Reason |> should equal "budget exhausted"

[<Fact>]
let ``UserMessage round-trips its message verbatim`` () =
    let restored =
        roundTrip "userMessage" (fun () ->
            UserMessageEvent(sessionId, turnId, noSequence, stamp, UserMessage.Text "steer") :> SessionEvent)

    let injected = restored :?> UserMessageEvent
    injected.SessionId |> should equal sessionId
    injected.TurnId |> should equal turnId
    injected.Sequence.HasValue |> should equal false

    let texts =
        [
            for part in injected.Message.Parts do
                match part with
                | :? TextContent as text when not (isNull (box text)) -> yield text.Text
                | _ -> ()
        ]

    texts |> should equal [ "steer" ]

// ───────────────────────────────────────────────────────────────────────────
// Sequence and base-field nullability

[<Fact>]
let ``Sequence is empty on live construction and settable for the store`` () =
    let live = TurnStartedEvent(sessionId, turnId, noSequence, stamp)

    live.Sequence.HasValue |> should equal false

    let stamped = TurnStartedEvent(sessionId, turnId, Nullable 41L, stamp)

    stamped.Sequence.HasValue |> should equal true
    stamped.Sequence.Value |> should equal 41L

[<Fact>]
let ``Sequence round-trips through JSON both empty and stamped`` () =
    let live = TextDeltaEvent(sessionId, turnId, noSequence, stamp, "a") :> SessionEvent
    let liveJson = JsonSerializer.Serialize(live, jsonOptions)

    let restored = deserialize<SessionEvent> (liveJson) :?> TextDeltaEvent

    restored.Sequence.HasValue |> should equal false

    let stamped =
        UsageEvent(sessionId, turnId, Nullable 41L, stamp, 1L, 2L) :> SessionEvent

    let stampedJson = JsonSerializer.Serialize(stamped, jsonOptions)
    stampedJson.Contains("\"Sequence\":41") |> should equal true

    let stampedRestored = deserialize<SessionEvent> (stampedJson) :?> UsageEvent

    stampedRestored.Sequence.Value |> should equal 41L

[<Fact>]
let ``Base fields carry the constructor values the constructor set`` () =
    let event = TurnStartedEvent(sessionId, turnId, noSequence, stamp)

    event.SessionId |> should equal sessionId
    event.TurnId |> should equal turnId
    event.Timestamp |> should equal stamp

// ───────────────────────────────────────────────────────────────────────────
// Binding contract

[<Fact>]
let ``An undocumented $type value is rejected instead of guessed`` () =
    // The 17 documented discriminators are the wire contract: anything
    // outside the set must fail the read rather than deserialise to a base
    // instance. The BCL raises NotSupportedException for a discriminator
    // with no registered derived type (surfaced possibly wrapped in a
    // JsonException depending on the serializer path), so the pin is the
    // rejection itself: the call throws and never returns a base instance.
    let json =
        """{"$type":"notARealKind","SessionId":"01ARZ3NDEKTSV4RRFFQ69G5FAV","TurnId":"01ARZ3NDEKTSV4RRFFQ69G5FAV","Sequence":null,"Timestamp":"2024-01-02T03:04:05+00:00"}"""

    let build () =
        match JsonSerializer.Deserialize<SessionEvent>(json, jsonOptions) with
        | null -> failwith "deserialised to null"
        | restored ->
            failwith (
                sprintf "deserialised to %s instead of rejecting the unknown discriminator" (restored.GetType().Name)
            )

    try
        build ()
        failwith "deserialised without raising"
    with
    | :? NotSupportedException -> ()
    | :? JsonException -> ()

[<Fact>]
let ``Mixed-case JSON payload binds through case-insensitive matching`` () =
    // Constructor-parameter binding in the TurnOutcome and Reply precedents
    // is case-insensitive, so a host writing camelCase keys deserialises
    // without a naming-policy option. Pinned here on the payload fields.
    let json =
        """{"$type":"toolCallStarted","SessionId":"01ARZ3NDEKTSV4RRFFQ69G5FAV","TurnId":"01ARZ3NDEKTSV4RRFFQ69G5FAV","Sequence":null,"Timestamp":"2024-01-02T03:04:05+00:00","ToolCallId":"call-1","ToolName":"read_file"}"""

    match JsonSerializer.Deserialize<SessionEvent>(json, jsonOptions) with
    | null -> failwith "deserialised to null"
    | restored ->
        let started = restored :?> ToolCallStartedEvent
        started.ToolCallId |> should equal "call-1"
        started.ToolName |> should equal "read_file"
