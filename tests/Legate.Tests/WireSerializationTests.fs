// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.WireSerializationTests

open System
open System.Collections.Generic
open System.Diagnostics.Metrics
open System.Text.Json
open System.Threading
open Akka.Actor
open Akka.Configuration
open FsUnit.Xunit
open Legate
open Microsoft.Extensions.AI
open Xunit

// Versioned envelope (issue 130): one token-less DTO per wire case, string
// manifests of the shape legate.<family>.<MessageName>.v<version> with a
// per-case byte bound, and a DTO-only serializer that fails closed with
// legate.serialization.rejected plus a warning log. CancellationToken values
// never cross (the receiver re-attaches its own scope) and live exceptions
// cross as reason strings. The subscription and event families stay reserved
// for issues 132/133.

// ────────────────── Helpers ──────────────────

/// Builds a Queue inbox entry carrying one text user message.
let private textEntry (text: string) (position: int64) : InboxEntry =
    {
        SessionId = SessionId.New()
        Position = position
        Payload = UserMessagePayload(UserMessage.Text(text)) :> InboxPayload
        Delivery = DeliveryMode.Queue
        Consumed = false
        AppendedAt = DateTimeOffset.UtcNow
    }

/// Builds a stored session with null host hooks, the cluster-safe shape.
let private storedSession () : Session =
    {
        Id = SessionId.New()
        Tenant = TenantId.Default
        AgentId = AgentId.New()
        Title = "wire probe"
        State = SessionState.Idle
        CurrentTurnId = Nullable<TurnId>()
        CreatedAt = DateTimeOffset.UtcNow
        UpdatedAt = DateTimeOffset.UtcNow
        ClosedAt = Nullable<DateTimeOffset>()
        WorkspaceBinding = null
        Options = SessionOptions()
        PermissionGrants = ResizeArray<string>() :> IReadOnlyList<string>
    }

/// Builds a Completed turn result.
let private completedResult () : TurnResult =
    {
        AssistantText = "done"
        Status = TurnStatus.Completed
        Iterations = 1
        Usage = { InputTokens = 10L; OutputTokens = 5L }
        Outcome = TurnFinished("done") :> TurnOutcome
    }

/// Builds a settled suspendable completion (no suspension).
let private settledCompletion () : TurnLoop.TurnLoopCompletion =
    {
        Result = completedResult ()
        HasPendingInjects = false
        Suspension = None
    }

/// Builds a live suspend cursor over one tool call, with no nested resume.
let private liveCursor () : TurnLoop.TurnLoopSuspension =
    let args = Dictionary<string, obj>() :> IDictionary<string, obj>

    let history =
        ResizeArray<ChatMessage>([| ChatMessage(ChatRole.User, "go") |]) :> IList<ChatMessage>

    {
        RequestId = "req-1"
        ToolName = "probe-tool"
        ToolCallId = "call-1"
        Kind = TurnLoop.SuspensionKind.PermissionSuspension
        QuestionText = ""
        QuestionOptions = []
        HistorySnapshot = history
        InputTokens = 3L
        OutputTokens = 7L
        Iterations = 2
        PendingCall = FunctionCallContent("call-1", "probe-tool", args)
        Nested = None
    }

/// Builds every live actor-protocol message: all nine SessionActorMessage
/// cases, all thirteen SuspendableActorMessage cases, every reply case, the
/// snapshot, a stored session, and the router message.
let private everyLiveMessage () : obj list =
    let entry = textEntry "wire probe" 7L
    let session = storedSession ()
    let agentId = AgentId.New()
    let error = InvalidOperationException("boom") :> Exception

    let mismatch =
        ReplyMismatchException(session.Id, "req-9", "No pending request 'req-9'.")

    let allowed = HashSet<string>([| "probe-tool" |])

    let finished =
        SessionActor.SuspendableFinished(entry, settledCompletion (), 1, allowed)

    let suspended =
        SessionActor.SuspendableFinished(
            entry,
            {
                Result = completedResult ()
                HasPendingInjects = true
                Suspension = Some(liveCursor ())
            },
            2,
            allowed
        )

    [
        QueuePrompt(entry.Payload, CancellationToken.None) :> obj
        InjectPrompt(entry.Payload, CancellationToken.None) :> obj
        InterruptPrompt(entry.Payload, CancellationToken.None) :> obj
        CloseSession(CancellationToken.None) :> obj
        AbortSession(StopCause.ExplicitAbort, "host abort", CancellationToken.None) :> obj
        CompactSession(CancellationToken.None) :> obj
        GetSnapshot :> obj
        SessionTurnSettled(entry, completedResult ()) :> obj
        SessionTurnFaulted(entry, error) :> obj
        SessionActor.SuspendableQueuePrompt(entry.Payload, CancellationToken.None) :> obj
        SessionActor.SuspendableInjectPrompt(entry.Payload, CancellationToken.None) :> obj
        SessionActor.SuspendableInterruptPrompt(entry.Payload, CancellationToken.None) :> obj
        finished :> obj
        suspended :> obj
        SessionActor.SuspendableFaulted(entry, error, 1) :> obj
        SessionActor.ReplyEntry(entry) :> obj
        SessionActor.SuspendableGetSnapshot :> obj
        SessionActor.SuspendTimedOut("req-1") :> obj
        SessionActor.SuspendableCloseSession(CancellationToken.None) :> obj
        SessionActor.SuspendableAbortSession(StopCause.HostShutdown, "shutting down", CancellationToken.None) :> obj
        SessionActor.SuspendableCompactSession(CancellationToken.None) :> obj
        SessionActor.SuspendableCheckInbox :> obj
        SessionActor.SuspendableSetAgent(agentId, CancellationToken.None) :> obj
        PromptAccepted(entry) :> obj
        PromptRejected(SessionState.Closed) :> obj
        CompactCompleted(100L, 40L) :> obj
        CompactNotNeeded :> obj
        CompactDeferred :> obj
        CompactFenced :> obj
        CompactRejected(SessionState.Closed) :> obj
        SessionActor.ReplyAccepted(entry) :> obj
        SessionActor.ReplyRejected(mismatch) :> obj
        SessionActor.SetAgentApplied(session) :> obj
        SessionActor.SetAgentPending(session) :> obj
        SessionActor.SetAgentRejected(SessionState.Closed) :> obj
        {
            SessionId = session.Id
            State = SessionState.Running
            PendingCount = 2
            RunningPosition = Some 7L
            PendingRequestId = "req-1"
        }
        :> obj
        session :> obj
        SessionRouterMessage.ResolveSession(session.Id.ToString()) :> obj
    ]

/// Runs emit under a MeterListener scoped to the serialization rejected
/// counter and returns every observed reason tag.
let private collectRejections (emit: unit -> unit) : string list =
    let gate = obj ()
    let observed = ResizeArray<string>()

    use listener = new MeterListener()

    listener.InstrumentPublished <-
        Action<Instrument, MeterListener>(fun instrument _ ->
            if
                instrument.Meter.Name = Telemetry.MeterName
                && instrument.Name = Telemetry.SerializationRejectedName
            then
                listener.EnableMeasurementEvents(instrument, null) |> ignore)

    listener.SetMeasurementEventCallback<int64>(fun instrument measurement tags _ ->
        if instrument.Name = Telemetry.SerializationRejectedName then
            for index in 0 .. tags.Length - 1 do
                if tags[index].Key = Telemetry.ReasonTag then
                    let value =
                        match tags[index].Value with
                        | null -> ""
                        | live ->
                            match live.ToString() with
                            | null -> ""
                            | text -> text

                    lock gate (fun () -> observed.Add(value)))

    listener.Start()
    emit ()
    lock gate (fun () -> observed |> List.ofSeq)

/// Creates a local actor system with the wire HOCON fragment inlined. The
/// caller terminates the system.
let private createWireSystem () : ActorSystem =
    let hocon =
        """
        akka {
          actor {
            provider = "local"
          }
          loglevel = "WARNING"
          stdout-loglevel = "WARNING"
        }
        """
        + WireSerialization.hoconFragment WireManifests.DefaultMaxWirePayloadBytes

    ActorSystem.Create("legate-wire-test", ConfigurationFactory.ParseString(hocon))

/// Resolves the envelope serializer for a prompt message.
let private envelopeSerializerOf (system: ActorSystem) : WireSerializer =
    let message =
        QueuePrompt(UserMessagePayload(UserMessage.Text("probe")) :> InboxPayload, CancellationToken.None)

    match system.Serialization.FindSerializerFor(message) with
    | :? WireSerializer as wire -> wire
    | other ->
        failwith $"Expected the wire serializer but resolved '{other.GetType().FullName}'."
        Unchecked.defaultof<WireSerializer>

/// Serialises one live message to its DTO bytes through the envelope.
let private toWireBytes (serializer: WireSerializer) (message: obj) : byte[] * string =
    let manifest = serializer.Manifest(message)
    (serializer.ToBinary(message), manifest)

// ────────────────── Manifest table ──────────────────

[<Fact>]
let ``Manifest table carries one unique legate manifest per wire case at v1`` () =
    let manifests = WireManifests.cases |> List.map WireManifests.manifestOf

    manifests.Length |> should equal 37
    manifests |> List.distinct |> List.length |> should equal manifests.Length

    for wireCase in WireManifests.cases do
        let manifest = WireManifests.manifestOf wireCase
        manifest.StartsWith("legate.", StringComparison.Ordinal) |> should equal true
        manifest.EndsWith(".v1", StringComparison.Ordinal) |> should equal true
        wireCase.Version |> should equal 1
        (wireCase.MaxBytes >= 1) |> should equal true
        wireCase.DtoType.IsClass |> should equal true

    let dtoTypes = WireManifests.cases |> List.map (fun wireCase -> wireCase.DtoType)
    dtoTypes |> List.distinct |> List.length |> should equal dtoTypes.Length

[<Fact>]
let ``Reserved manifests pin the subscription and event namespaces`` () =
    WireManifests.reservedManifests
    |> should
        equal
        [
            "legate.subscription.Subscribe.v1"
            "legate.event.SessionEvent.v1"
        ]

    for manifest in WireManifests.reservedManifests do
        (WireManifests.tryParseManifest manifest).IsSome |> should equal true

        let family, _, _ = WireManifests.tryParseManifest manifest |> Option.get
        WireManifests.tryFindCase family (manifest.Split('.')[2]) |> should equal None

[<Fact>]
let ``Manifest parsing accepts the shape and refuses anything else`` () =
    WireManifests.tryParseManifest "legate.actor.QueuePrompt.v1"
    |> should equal (Some("actor", "QueuePrompt", 1))

    WireManifests.tryParseManifest null |> should equal None
    WireManifests.tryParseManifest "" |> should equal None
    WireManifests.tryParseManifest "legate.actor.QueuePrompt" |> should equal None
    WireManifests.tryParseManifest "other.actor.QueuePrompt.v1" |> should equal None

    WireManifests.tryParseManifest "legate.actor.QueuePrompt.vx"
    |> should equal None

    WireManifests.tryParseManifest "legate.actor.QueuePrompt.v1.extra"
    |> should equal None

// ────────────────── DTO translation ──────────────────

[<Fact>]
let ``Every protocol case maps to a DTO with a manifest`` () =
    let messages = everyLiveMessage ()
    messages.Length |> should equal 38

    for message in messages do
        let dto = WireDtos.toWire message
        (isNull (box dto)) |> should equal false

        match WireManifests.tryFindByDtoType (dto.GetType()) with
        | None -> failwith $"No wire case registers the DTO '{dto.GetType().FullName}'."
        | Some _ -> ()

    let dtos = messages |> List.map WireDtos.toWire
    let types = dtos |> List.map (fun dto -> dto.GetType())

    // 38 live messages over 37 DTOs: the settled and the suspended
    // SuspendableFinished share one wire case.
    types |> List.distinct |> List.length |> should equal 37

[<Fact>]
let ``Prompt DTO round-trips its payload and drops its token`` () =
    let payload = UserMessagePayload(UserMessage.Text("wire probe")) :> InboxPayload
    let dto = WireDtos.toWire (QueuePrompt(payload, CancellationToken.None))

    let json =
        JsonSerializer.Serialize(dto, dto.GetType(), WireSerialization.wireOptions ())

    json.Contains("cancellationToken", StringComparison.OrdinalIgnoreCase)
    |> should equal false

    let back =
        match JsonSerializer.Deserialize(json, dto.GetType(), WireSerialization.wireOptions ()) with
        | null -> failwith "Expected the DTO JSON to deserialise."
        | live -> live

    match WireDtos.ofWire back with
    | :? SessionActorMessage as message ->
        match message with
        | QueuePrompt(roundTripped, token) ->
            let userMessage = roundTripped :?> UserMessagePayload
            userMessage.Message.Parts.Count |> should equal 1
            (userMessage.Message.Parts[0] :?> TextContent).Text |> should equal "wire probe"
            token |> should equal CancellationToken.None
        | other -> failwith $"Expected QueuePrompt but rebuilt '{other.GetType().Name}'."
    | other -> failwith $"Expected a SessionActorMessage but rebuilt '{other.GetType().Name}'."

[<Fact>]
let ``Abort DTO carries its cause and reason`` () =
    let dto =
        WireDtos.toWire (AbortSession(StopCause.HostShutdown, "shutting down", CancellationToken.None))
        :?> WireDtos.AbortSessionDto

    dto.Cause |> should equal StopCause.HostShutdown
    dto.Reason |> should equal "shutting down"

    match WireDtos.ofWire dto with
    | :? SessionActorMessage as message ->
        match message with
        | AbortSession(cause, reason, token) ->
            cause |> should equal StopCause.HostShutdown
            reason |> should equal "shutting down"
            token |> should equal CancellationToken.None
        | other -> failwith $"Expected AbortSession but rebuilt '{other.GetType().Name}'."
    | other -> failwith $"Expected a SessionActorMessage but rebuilt '{other.GetType().Name}'."

[<Fact>]
let ``Fault DTO maps exceptions to reason strings and rebuilds the fault path`` () =
    let entry = textEntry "fault probe" 3L

    let dto =
        WireDtos.toWire (SessionTurnFaulted(entry, InvalidOperationException("boom"))) :?> WireDtos.TurnFaultedDto

    dto.Reason |> should equal "boom"
    dto.Entry.Position |> should equal 3L

    match WireDtos.ofWire dto with
    | :? SessionActorMessage as message ->
        match message with
        | SessionTurnFaulted(roundTripped, error) ->
            roundTripped.Position |> should equal 3L
            error.Message |> should equal "boom"
        | other -> failwith $"Expected SessionTurnFaulted but rebuilt '{other.GetType().Name}'."
    | other -> failwith $"Expected a SessionActorMessage but rebuilt '{other.GetType().Name}'."

    let nameless =
        WireDtos.toWire (SessionActor.SuspendableFaulted(entry, Exception(""), 2)) :?> WireDtos.SuspendableFaultedDto

    nameless.Reason |> should equal "Exception"
    nameless.Attempt |> should equal 2

[<Fact>]
let ``Every reply DTO round-trips through JSON`` () =
    let entry = textEntry "reply probe" 5L
    let session = storedSession ()

    let mismatch =
        ReplyMismatchException(session.Id, "req-9", "No pending request 'req-9'.")

    let replies: obj list =
        [
            PromptAccepted(entry) :> obj
            PromptRejected(SessionState.Closed) :> obj
            CompactCompleted(100L, 40L) :> obj
            CompactNotNeeded :> obj
            CompactDeferred :> obj
            CompactFenced :> obj
            CompactRejected(SessionState.Closed) :> obj
            SessionActor.ReplyAccepted(entry) :> obj
            SessionActor.ReplyRejected(mismatch) :> obj
            SessionActor.SetAgentApplied(session) :> obj
            SessionActor.SetAgentPending(session) :> obj
            SessionActor.SetAgentRejected(SessionState.Closed) :> obj
            {
                SessionId = session.Id
                State = SessionState.WaitingForInput
                PendingCount = 1
                RunningPosition = None
                PendingRequestId = "req-1"
            }
            :> obj
            session :> obj
        ]

    for reply in replies do
        let dto = WireDtos.toWire reply

        let json =
            JsonSerializer.Serialize(dto, dto.GetType(), WireSerialization.wireOptions ())

        let back =
            match JsonSerializer.Deserialize(json, dto.GetType(), WireSerialization.wireOptions ()) with
            | null -> failwith "Expected the DTO JSON to deserialise."
            | live -> live

        let roundTripped = WireDtos.ofWire back
        roundTripped.GetType() |> should equal (reply.GetType())

    match WireDtos.ofWire (WireDtos.toWire (SessionActor.ReplyRejected(mismatch))) with
    | :? SessionActor.SessionReplyReply as reply ->
        match reply with
        | SessionActor.ReplyRejected rebuilt ->
            rebuilt.SessionId |> should equal mismatch.SessionId
            rebuilt.RequestId |> should equal "req-9"
            rebuilt.Message |> should equal "No pending request 'req-9'."
        | other -> failwith $"Expected ReplyRejected but rebuilt '{other.GetType().Name}'."
    | other -> failwith $"Expected a SessionReplyReply but rebuilt '{other.GetType().Name}'."

[<Fact>]
let ``Suspended cursor round-trips without its nested resume`` () =
    let entry = textEntry "suspend probe" 9L
    let allowed = HashSet<string>([| "probe-tool" |])

    let completion: TurnLoop.TurnLoopCompletion =
        {
            Result = completedResult ()
            HasPendingInjects = true
            Suspension = Some(liveCursor ())
        }

    let dto =
        WireDtos.toWire (SessionActor.SuspendableFinished(entry, completion, 2, allowed))
        :?> WireDtos.SuspendableFinishedDto

    (isNull (box dto.Suspension)) |> should equal false
    dto.Suspension.RequestId |> should equal "req-1"
    dto.Suspension.Kind |> should equal "permission"
    dto.Suspension.PendingCall.Name |> should equal "probe-tool"

    let json = JsonSerializer.Serialize(dto, WireSerialization.wireOptions ())

    let back =
        match JsonSerializer.Deserialize<WireDtos.SuspendableFinishedDto>(json, WireSerialization.wireOptions ()) with
        | null -> failwith "Expected the finished DTO JSON to deserialise."
        | live -> live

    match WireDtos.ofWire back with
    | :? SessionActor.SuspendableActorMessage as message ->
        match message with
        | SessionActor.SuspendableFinished(roundTripped, rebuilt, attempt, granted) ->
            roundTripped.Position |> should equal 9L
            attempt |> should equal 2
            granted.Contains("probe-tool") |> should equal true
            rebuilt.HasPendingInjects |> should equal true

            match rebuilt.Suspension with
            | None -> failwith "Expected the suspended cursor to survive the wire."
            | Some cursor ->
                cursor.RequestId |> should equal "req-1"
                cursor.ToolCallId |> should equal "call-1"
                cursor.Kind |> should equal TurnLoop.SuspensionKind.PermissionSuspension
                cursor.InputTokens |> should equal 3L
                cursor.HistorySnapshot.Count |> should equal 1
                cursor.Nested |> should equal None
        | other -> failwith $"Expected SuspendableFinished but rebuilt '{other.GetType().Name}'."
    | other -> failwith $"Expected a SuspendableActorMessage but rebuilt '{other.GetType().Name}'."

// ────────────────── Fail-closed envelope ──────────────────

[<Fact>]
let ``Unknown manifest fails closed with unknownManifest`` () =
    let system = createWireSystem ()

    try
        let serializer = envelopeSerializerOf system

        let reasons =
            collectRejections (fun () ->
                try
                    serializer.FromBinary([||], "legate.actor.NoSuchCase.v1") |> ignore
                    failwith "Expected the unknown manifest to refuse."
                with :? WireManifests.WireRejectedException as rejected ->
                    rejected.Reason |> should equal Telemetry.RejectionUnknownManifest)

        reasons |> should equal [ Telemetry.RejectionUnknownManifest ]
    finally
        system.Terminate().GetAwaiter().GetResult() |> ignore

[<Fact>]
let ``Newer version fails closed with newerVersion and older records failed`` () =
    let system = createWireSystem ()

    try
        let serializer = envelopeSerializerOf system

        let message =
            QueuePrompt(UserMessagePayload(UserMessage.Text("probe")) :> InboxPayload, CancellationToken.None)

        let bytes, manifest = toWireBytes serializer message
        manifest |> should equal "legate.actor.QueuePrompt.v1"

        let newerReasons =
            collectRejections (fun () ->
                try
                    serializer.FromBinary(bytes, "legate.actor.QueuePrompt.v2") |> ignore
                    failwith "Expected the newer version to refuse."
                with :? WireManifests.WireRejectedException as rejected ->
                    rejected.Reason |> should equal Telemetry.RejectionNewerVersion)

        newerReasons |> should equal [ Telemetry.RejectionNewerVersion ]

        let olderReasons =
            collectRejections (fun () ->
                try
                    serializer.FromBinary(bytes, "legate.actor.QueuePrompt.v0") |> ignore
                    failwith "Expected the older version to refuse."
                with :? WireManifests.WireRejectedException as rejected ->
                    rejected.Reason |> should equal Telemetry.RejectionFailed)

        olderReasons |> should equal [ Telemetry.RejectionFailed ]
    finally
        system.Terminate().GetAwaiter().GetResult() |> ignore

[<Fact>]
let ``Oversized payload is refused before deserialising`` () =
    let system = createWireSystem ()

    try
        let serializer = envelopeSerializerOf system
        let bytes, manifest = toWireBytes serializer GetSnapshot
        manifest |> should equal "legate.actor.GetSnapshot.v1"

        let padded =
            Array.append bytes (Array.create (WireManifests.SmallWireBytes + 1) (byte ' '))

        let reasons =
            collectRejections (fun () ->
                try
                    serializer.FromBinary(padded, manifest) |> ignore
                    failwith "Expected the oversized payload to refuse."
                with :? WireManifests.WireRejectedException as rejected ->
                    rejected.Reason |> should equal Telemetry.RejectionOversized)

        reasons |> should equal [ Telemetry.RejectionOversized ]
    finally
        system.Terminate().GetAwaiter().GetResult() |> ignore

[<Fact>]
let ``ToBinary refuses oversized payloads after serialising`` () =
    let system = createWireSystem ()

    try
        let serializer = envelopeSerializerOf system
        let bigText = String('x', WireManifests.LargeWireBytes + 1)

        let message =
            QueuePrompt(UserMessagePayload(UserMessage.Text(bigText)) :> InboxPayload, CancellationToken.None)

        let reasons =
            collectRejections (fun () ->
                try
                    serializer.ToBinary(message) |> ignore
                    failwith "Expected the oversized serialisation to refuse."
                with :? WireManifests.WireRejectedException as rejected ->
                    rejected.Reason |> should equal Telemetry.RejectionOversized)

        reasons |> should equal [ Telemetry.RejectionOversized ]
    finally
        system.Terminate().GetAwaiter().GetResult() |> ignore

[<Fact>]
let ``Known manifest with an unknown inner dollar-type fails closed`` () =
    let system = createWireSystem ()

    try
        let serializer = envelopeSerializerOf system
        let entry = textEntry "type probe" 11L
        let message = SessionTurnSettled(entry, completedResult ())
        let bytes, manifest = toWireBytes serializer message

        let json = Text.Encoding.UTF8.GetString(bytes)
        json.Contains("$type", StringComparison.Ordinal) |> should equal true

        let tampered =
            Text.Encoding.UTF8.GetBytes(
                json.Replace("$type\":\"userMessage", "$type\":\"bogusType", StringComparison.Ordinal)
            )

        let reasons =
            collectRejections (fun () ->
                try
                    serializer.FromBinary(tampered, manifest) |> ignore
                    failwith "Expected the unknown inner type to refuse."
                with :? WireManifests.WireRejectedException as rejected ->
                    rejected.Reason |> should equal Telemetry.RejectionFailed)

        reasons |> should equal [ Telemetry.RejectionFailed ]
    finally
        system.Terminate().GetAwaiter().GetResult() |> ignore

// ────────────────── Akka wiring ──────────────────

[<Fact>]
let ``Prompt path round-trips through the Akka serializer API`` () =
    let system = createWireSystem ()

    try
        let serializer = envelopeSerializerOf system

        let payload = UserMessagePayload(UserMessage.Text("akka probe")) :> InboxPayload
        let message = QueuePrompt(payload, CancellationToken.None)

        let manifest = serializer.Manifest(message)
        manifest |> should equal "legate.actor.QueuePrompt.v1"

        let back = serializer.FromBinary(serializer.ToBinary(message), manifest)

        match back with
        | :? SessionActorMessage as roundTripped ->
            match roundTripped with
            | QueuePrompt(roundTrippedPayload, token) ->
                let userMessage = roundTrippedPayload :?> UserMessagePayload
                (userMessage.Message.Parts[0] :?> TextContent).Text |> should equal "akka probe"
                token |> should equal CancellationToken.None
            | other -> failwith $"Expected QueuePrompt but rebuilt '{other.GetType().Name}'."
        | other -> failwith $"Expected a SessionActorMessage but rebuilt '{other.GetType().Name}'."
    finally
        system.Terminate().GetAwaiter().GetResult() |> ignore

[<Fact>]
let ``HOCON fragment binds every protocol type to the wire serializer`` () =
    let fragment = WireSerialization.hoconFragment 262144

    fragment.Contains("max-payload-bytes = 262144", StringComparison.Ordinal)
    |> should equal true

    fragment.Contains("Legate.WireSerializer, Legate", StringComparison.Ordinal)
    |> should equal true

    typeof<WireSerializer>.FullName |> should equal "Legate.WireSerializer"

    for boundType in WireSerialization.boundTypes do
        let key = $"{boundType.FullName}, {boundType.Assembly.GetName().Name}"
        fragment.Contains(key, StringComparison.Ordinal) |> should equal true

    WireSerialization.boundTypes.Length |> should equal 9

[<Fact>]
let ``Cluster HOCON carries the wire maximum from options`` () =
    let options = ClusterOptions(Mode = ClusterMode.StaticSeeds)
    options.SeedNodes.Add("127.0.0.1:5115") |> ignore
    options.MaxWirePayloadBytes <- 12345

    let config =
        ConfigurationFactory.ParseString(ClusterActorSystem.buildClusterHocon options 0)

    config.GetInt("legate.wire.max-payload-bytes") |> should equal 12345

    let defaults = ClusterOptions()

    defaults.MaxWirePayloadBytes
    |> should equal WireManifests.DefaultMaxWirePayloadBytes

    let defaultConfig =
        ConfigurationFactory.ParseString(ClusterActorSystem.buildClusterHocon defaults 0)

    defaultConfig.GetInt("legate.wire.max-payload-bytes")
    |> should equal WireManifests.DefaultMaxWirePayloadBytes
