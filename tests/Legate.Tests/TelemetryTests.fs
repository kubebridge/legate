// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.TelemetryTests

open System
open System.Collections.Generic
open System.Diagnostics
open System.Diagnostics.Metrics
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Legate.Testing
open Microsoft.Extensions.AI
open Xunit

// Metrics and tracing (issue 94): one internal Telemetry module owns
// Meter("Legate") counters/histograms and ActivitySource("Legate") spans at
// the turn, provider-call, and tool-call boundaries. Metric tags stay bounded
// (status, provider, model, tool, outcome: names only); span tags mirror the
// issue 93 canonical scope names. A MeterListener test proves every
// instrument emits, an ActivityListener test proves all three spans carry
// the id tags, and a mirror test pins the tag names to LoggingScopes.

/// One observed metric measurement with its tags.
type private ObservedMeasurement =
    {
        Instrument: string
        Value: string
        Tags: (string * string) list
    }

/// Runs emit under a MeterListener scoped to Meter("Legate") and returns
/// every measurement the listener observed.
/// Renders a tag value for assertions: null renders empty, and a
/// null-returning ToString renders empty too, so the comparison never sees null.
let private displayText (value: obj | null) : string =
    match value with
    | null -> ""
    | live ->
        match live.ToString() with
        | null -> ""
        | text -> text

let private collectMeasurements (emit: unit -> unit) : ObservedMeasurement list =
    let gate = obj ()
    let observed = ResizeArray<ObservedMeasurement>()

    use listener = new MeterListener()

    listener.InstrumentPublished <-
        Action<Instrument, MeterListener>(fun instrument _ ->
            if instrument.Meter.Name = Telemetry.MeterName then
                listener.EnableMeasurementEvents(instrument, null) |> ignore)

    listener.SetMeasurementEventCallback<int64>(fun instrument measurement tags _ ->
        let pairs = ResizeArray<string * string>()

        for index in 0 .. tags.Length - 1 do
            pairs.Add(tags[index].Key, displayText tags[index].Value)

        let entry =
            {
                Instrument = instrument.Name
                Value = string measurement
                Tags = pairs |> List.ofSeq
            }

        lock gate (fun () -> observed.Add(entry)))

    listener.SetMeasurementEventCallback<double>(fun instrument measurement tags _ ->
        let pairs = ResizeArray<string * string>()

        for index in 0 .. tags.Length - 1 do
            pairs.Add(tags[index].Key, displayText tags[index].Value)

        let entry =
            {
                Instrument = instrument.Name
                Value = string measurement
                Tags = pairs |> List.ofSeq
            }

        lock gate (fun () -> observed.Add(entry)))

    listener.Start()
    emit ()
    lock gate (fun () -> observed |> List.ofSeq)

/// One stopped span with its tags.
type private StoppedSpan =
    {
        Operation: string
        Tags: (string * string) list
    }

/// Runs emit under an ActivityListener scoped to ActivitySource("Legate")
/// and returns every span that stopped inside the emit.
let private collectStoppedSpans (emit: unit -> unit) : StoppedSpan list =
    let gate = obj ()
    let stopped = ResizeArray<StoppedSpan>()

    use listener = new ActivityListener()
    listener.ShouldListenTo <- fun source -> source.Name = Telemetry.ActivitySourceName
    listener.Sample <- fun _ -> ActivitySamplingResult.AllDataAndRecorded

    listener.ActivityStopped <-
        fun activity ->
            let tags =
                activity.TagObjects
                |> Seq.map (fun entry -> entry.Key, displayText entry.Value)
                |> List.ofSeq

            lock gate (fun () ->
                stopped.Add(
                    {
                        Operation = activity.OperationName
                        Tags = tags
                    }
                ))

    ActivitySource.AddActivityListener(listener)
    emit ()
    lock gate (fun () -> stopped |> List.ofSeq)

/// An ILlmDelay that never elapses: the turn runs without a deadline.
type private NeverDelay() =
    interface ILlmDelay with
        member _.Delay(_, cancellationToken) =
            Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken)

/// Asserts the measurements hold at least one entry with the instrument name
/// whose tags carry every expected pair.
let private shouldHold
    (measurements: ObservedMeasurement list)
    (instrument: string)
    (expected: (string * string) list)
    =
    measurements
    |> List.exists (fun entry ->
        entry.Instrument = instrument
        && expected
           |> List.forall (fun (key, value) -> entry.Tags |> List.contains (key, value)))
    |> should equal true

// ──────────────────────────────────────────────────────────────────────────
// Task 1: instrument and span names

[<Fact>]
let ``Instrument names carry the documented legate names`` () =
    Telemetry.MeterName |> should equal "Legate"
    Telemetry.ActivitySourceName |> should equal "Legate"
    Telemetry.TurnsStartedName |> should equal "legate.turns.started"
    Telemetry.TurnsSettledName |> should equal "legate.turns.settled"
    Telemetry.ProviderCallsName |> should equal "legate.provider.calls"
    Telemetry.ProviderLatencyName |> should equal "legate.provider.latency"
    Telemetry.ToolCallsName |> should equal "legate.tool.calls"
    Telemetry.ToolLatencyName |> should equal "legate.tool.latency"
    Telemetry.QueueDepthName |> should equal "legate.session.queue.depth"
    Telemetry.LeaseRenewalsName |> should equal "legate.lease.renewals"
    Telemetry.TurnActivityName |> should equal "Legate.Turn"
    Telemetry.ProviderActivityName |> should equal "Legate.ProviderCall"
    Telemetry.ToolActivityName |> should equal "Legate.ToolCall"

[<Fact>]
let ``Canonical tag names mirror the LoggingScopes keys`` () =
    Telemetry.SessionIdTag |> should equal LoggingScopes.SessionIdKey
    Telemetry.TurnIdTag |> should equal LoggingScopes.TurnIdKey
    Telemetry.AgentIdTag |> should equal LoggingScopes.AgentIdKey
    Telemetry.TenantIdTag |> should equal LoggingScopes.TenantIdKey
    Telemetry.AttemptTag |> should equal LoggingScopes.AttemptKey
    Telemetry.ClaimOwnerTag |> should equal LoggingScopes.ClaimOwnerKey
    Telemetry.CanonicalTagNames |> should equal LoggingScopes.AllKeys

[<Fact>]
let ``idTagsFromScope maps scope values and reads null scopes as empty`` () =
    let scope =
        LoggingScopes.createScope "acme" "session-1" "turn-1" "agent-1" 2 "owner-1"

    let tags = Telemetry.idTagsFromScope scope
    let table = tags |> Seq.map (fun entry -> entry.Key, entry.Value) |> Map.ofSeq
    table["SessionId"] |> should equal ("session-1" :> obj)
    table["TurnId"] |> should equal ("turn-1" :> obj)
    table["AgentId"] |> should equal ("agent-1" :> obj)
    table["TenantId"] |> should equal ("acme" :> obj)
    table["Attempt"] |> should equal ("2" :> obj)
    table["ClaimOwner"] |> should equal ("owner-1" :> obj)

    let empty = Telemetry.idTagsFromScope null
    let emptyTable = empty |> Seq.map (fun entry -> entry.Key, entry.Value) |> Map.ofSeq
    emptyTable.Count |> should equal 6
    emptyTable["SessionId"] |> should equal ("" :> obj)

[<Fact>]
let ``The latency seam reads non-negative without sleeping`` () =
    let start = Telemetry.timestamp ()
    let elapsed = Telemetry.elapsedMilliseconds start
    elapsed |> should be (greaterThanOrEqualTo 0.0)

// ──────────────────────────────────────────────────────────────────────────
// Task 5: MeterListener emission over every instrument

[<Fact>]
let ``MeterListener observes every counter and histogram with its tags`` () =
    let measurements =
        collectMeasurements (fun () ->
            Telemetry.recordTurnStarted ()
            Telemetry.recordTurnSettled "Completed"
            Telemetry.recordProviderCall "probe-provider" "probe-model" Telemetry.StatusOk
            Telemetry.recordProviderLatency 12.5 "probe-provider" "probe-model"
            Telemetry.recordToolCall "probe-tool" Telemetry.StatusOk
            Telemetry.recordToolLatency 3.25 "probe-tool"
            Telemetry.addQueueDepth 1
            Telemetry.addQueueDepth -1
            Telemetry.recordLeaseRenewal Telemetry.LeaseContinued)

    shouldHold measurements Telemetry.TurnsStartedName []
    shouldHold measurements Telemetry.TurnsSettledName [ "status", "Completed" ]

    shouldHold
        measurements
        Telemetry.ProviderCallsName
        [
            "provider", "probe-provider"
            "model", "probe-model"
            "status", Telemetry.StatusOk
        ]

    shouldHold
        measurements
        Telemetry.ProviderLatencyName
        [
            "provider", "probe-provider"
            "model", "probe-model"
        ]

    shouldHold
        measurements
        Telemetry.ToolCallsName
        [
            "tool", "probe-tool"
            "status", Telemetry.StatusOk
        ]

    shouldHold measurements Telemetry.ToolLatencyName [ "tool", "probe-tool" ]
    shouldHold measurements Telemetry.QueueDepthName []

    shouldHold measurements Telemetry.LeaseRenewalsName [ "outcome", Telemetry.LeaseContinued ]

    let latency =
        measurements
        |> List.find (fun entry -> entry.Instrument = Telemetry.ProviderLatencyName)

    Double.Parse(latency.Value) |> should be (greaterThanOrEqualTo 0.0)

// ──────────────────────────────────────────────────────────────────────────
// Task 5: ActivityListener spans carry the id tags

[<Fact>]
let ``ActivityListener observes turn provider and tool spans with the id tags`` () =
    let session = "telemetry-session-1"
    let turn = "telemetry-turn-1"

    let spans =
        collectStoppedSpans (fun () ->
            let ids =
                Telemetry.idTagsFor session turn "telemetry-agent-1" "telemetry-tenant-1" "3" "telemetry-owner-1"

            use _turnScope = Telemetry.startTurnScope ids

            use _providerScope = Telemetry.startProviderScope "probe-provider" "probe-model" ids

            use _toolScope = Telemetry.startToolScope "probe-tool" ids
            ())

    let expectedIds =
        [
            "SessionId", session
            "TurnId", turn
            "AgentId", "telemetry-agent-1"
            "TenantId", "telemetry-tenant-1"
            "Attempt", "3"
            "ClaimOwner", "telemetry-owner-1"
        ]

    for operation in
        [
            Telemetry.TurnActivityName
            Telemetry.ProviderActivityName
            Telemetry.ToolActivityName
        ] do
        spans
        |> List.exists (fun span ->
            span.Operation = operation
            && expectedIds |> List.forall (fun pair -> span.Tags |> List.contains pair))
        |> should equal true

    let tool =
        spans
        |> List.find (fun span ->
            span.Operation = Telemetry.ToolActivityName
            && span.Tags |> List.contains ("SessionId", session))

    tool.Tags |> List.contains ("tool", "probe-tool") |> should equal true

    let provider =
        spans
        |> List.find (fun span ->
            span.Operation = Telemetry.ProviderActivityName
            && span.Tags |> List.contains ("SessionId", session))

    provider.Tags
    |> List.contains ("provider", "probe-provider")
    |> should equal true

// ──────────────────────────────────────────────────────────────────────────
// Tasks 2 and 4: the wired boundaries emit through the listeners

[<Fact>]
let ``The turn boundary emits started and settled through the listener`` () =
    let client =
        new ScriptedChatClient(ResizeArray<ScriptStep>([| ScriptStep.Text "done" |]) :> IReadOnlyList<ScriptStep>)

    let history =
        ResizeArray<ChatMessage>([| ChatMessage(ChatRole.User, "hi") |]) :> IList<ChatMessage>

    let measurements =
        collectMeasurements (fun () ->
            TurnLoop.runAsync
                (client :> IChatClient)
                history
                (Dictionary<string, AITool>() :> IReadOnlyDictionary<string, AITool>)
                TurnLoop.TurnLoopOptions.Default
                (NeverDelay() :> ILlmDelay)
                CancellationToken.None
                (fun () -> true)
            |> fun task -> task.GetAwaiter().GetResult() |> ignore)

    shouldHold measurements Telemetry.TurnsStartedName []
    shouldHold measurements Telemetry.TurnsSettledName [ "status", "Completed" ]

[<Fact>]
let ``The tool path emits the tool call and latency through the listener`` () =
    let method = Func<string>(fun () -> "probe output")

    let fn =
        AIFunctionFactory.Create(
            method,
            "telemetry-probe",
            Unchecked.defaultof<string>,
            Unchecked.defaultof<System.Text.Json.JsonSerializerOptions>
        )

    let tools =
        let table = Dictionary<string, AITool>()
        table["telemetry-probe"] <- fn :> AITool
        table :> IReadOnlyDictionary<string, AITool>

    let client =
        new ScriptedChatClient(
            ResizeArray<ScriptStep>(
                [|
                    ScriptStep.ToolCall("call-1", "telemetry-probe")
                    ScriptStep.Text "done"
                |]
            )
            :> IReadOnlyList<ScriptStep>
        )

    let history =
        ResizeArray<ChatMessage>([| ChatMessage(ChatRole.User, "hi") |]) :> IList<ChatMessage>

    let measurements =
        collectMeasurements (fun () ->
            let result =
                TurnLoop.runAsync
                    (client :> IChatClient)
                    history
                    tools
                    TurnLoop.TurnLoopOptions.Default
                    (NeverDelay() :> ILlmDelay)
                    CancellationToken.None
                    (fun () -> true)
                |> fun task -> task.GetAwaiter().GetResult()

            result.Status |> should equal TurnStatus.Completed)

    shouldHold
        measurements
        Telemetry.ToolCallsName
        [
            "tool", "telemetry-probe"
            "status", Telemetry.StatusOk
        ]

    shouldHold measurements Telemetry.ToolLatencyName [ "tool", "telemetry-probe" ]
