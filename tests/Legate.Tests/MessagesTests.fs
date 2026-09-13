// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.MessagesTests

open System
open System.Collections.Generic
open System.Text.Json
open FsUnit.Xunit
open Legate
open Microsoft.Extensions.AI
open Xunit

let jsonOptions = JsonSerializerOptions()

// ───────────────────────────────────────────────────────────────────────────
// DeliveryMode

[<Fact>]
let ``DeliveryMode values serialise as their documented numbers`` () =
    int DeliveryMode.Queue |> should equal 0
    int DeliveryMode.Inject |> should equal 1
    int DeliveryMode.Interrupt |> should equal 2

    JsonSerializer.Serialize(DeliveryMode.Inject, jsonOptions) |> should equal "1"

// ───────────────────────────────────────────────────────────────────────────
// UserMessage

[<Fact>]
let ``UserMessage.Text produces a single TextContent part`` () =
    let message = UserMessage.Text("hello there")

    message.Parts.Count |> should equal 1
    (message.Parts[0] :?> TextContent).Text |> should equal "hello there"
    message.Metadata |> should equal null

[<Fact>]
let ``UserMessage.Text rejects empty text`` () =
    let build () = UserMessage.Text("   ") |> ignore

    should throw typeof<ArgumentException> build

[<Fact>]
let ``UserMessage.WithFile produces a DataContent part with media type`` () =
    let bytes = ReadOnlyMemory<byte>([| 1uy; 2uy; 3uy |])
    let message = UserMessage.WithFile(bytes, "application/pdf")

    message.Parts.Count |> should equal 1
    let file = message.Parts[0] :?> DataContent
    file.MediaType |> should equal "application/pdf"
    file.Data.ToArray() |> should equal [| 1uy; 2uy; 3uy |]

[<Fact>]
let ``UserMessage.WithFile rejects an empty media type`` () =
    let build () =
        UserMessage.WithFile(ReadOnlyMemory<byte>.Empty, "") |> ignore

    should throw typeof<ArgumentException> build

[<Fact>]
let ``UserMessage copies parts so later caller changes never reach it`` () =
    let parts = ResizeArray<AIContent>([ TextContent("one") :> AIContent ])
    let message = UserMessage(parts, null)
    parts.Add(TextContent("two") :> AIContent)

    message.Parts.Count |> should equal 1

[<Fact>]
let ``UserMessage preserves metadata across JSON round-trip`` () =
    let metadata =
        Dictionary<string, string>(dict [ ("source", "cli"); ("attempt", "3") ]) :> IReadOnlyDictionary<string, string>

    let message =
        UserMessage(
            [
                TextContent("with metadata") :> AIContent
            ],
            metadata
        )

    let json = JsonSerializer.Serialize(message, jsonOptions)

    match JsonSerializer.Deserialize<UserMessage>(json, jsonOptions) with
    | null -> failwith "deserialised to null"
    | restored ->
        restored.Parts.Count |> should equal 1
        (restored.Parts[0] :?> TextContent).Text |> should equal "with metadata"

        match restored.Metadata with
        | null -> failwith "metadata was lost in the round-trip"
        | meta ->
            meta["source"] |> should equal "cli"
            meta["attempt"] |> should equal "3"

[<Fact>]
let ``UserMessage JSON round-trips through the AIContent polymorphic contract`` () =
    let message =
        UserMessage(
            [
                TextContent("say hi") :> AIContent
                DataContent(ReadOnlyMemory<byte>([| 9uy |]), "image/png") :> AIContent
            ],
            null
        )

    let json = JsonSerializer.Serialize(message, jsonOptions)

    match JsonSerializer.Deserialize<UserMessage>(json, jsonOptions) with
    | null -> failwith "deserialised to null"
    | restored ->
        restored.Parts.Count |> should equal 2
        (restored.Parts[0] :?> TextContent).Text |> should equal "say hi"
        (restored.Parts[1] :?> DataContent).MediaType |> should equal "image/png"

// ───────────────────────────────────────────────────────────────────────────
// Reply polymorphism

[<Fact>]
let ``PermissionDecision round-trips to the correct subtype via $type`` () =
    let reply =
        PermissionDecision("req-1", PermissionDecisionKind.AllowForSession) :> Reply

    let json = JsonSerializer.Serialize(reply, jsonOptions)
    json.Contains("\"$type\":\"permissionDecision\"") |> should equal true

    match JsonSerializer.Deserialize<Reply>(json, jsonOptions) with
    | null -> failwith "deserialised to null"
    | restored ->
        (restored :? PermissionDecision) |> should equal true

        let decision = restored :?> PermissionDecision
        decision.RequestId |> should equal "req-1"
        decision.Decision |> should equal PermissionDecisionKind.AllowForSession

[<Fact>]
let ``QuestionAnswer round-trips to the correct subtype via $type`` () =
    let reply = QuestionAnswer("q-7", "blue") :> Reply

    let json = JsonSerializer.Serialize(reply, jsonOptions)
    json.Contains("\"$type\":\"questionAnswer\"") |> should equal true

    match JsonSerializer.Deserialize<Reply>(json, jsonOptions) with
    | null -> failwith "deserialised to null"
    | restored ->
        (restored :? QuestionAnswer) |> should equal true

        let answer = restored :?> QuestionAnswer
        answer.QuestionId |> should equal "q-7"
        answer.Answer |> should equal "blue"

[<Fact>]
let ``PermissionDecisionKind values serialise as documented`` () =
    int PermissionDecisionKind.AllowOnce |> should equal 0
    int PermissionDecisionKind.AllowForSession |> should equal 1
    int PermissionDecisionKind.Deny |> should equal 2

    let json =
        JsonSerializer.Serialize(PermissionDecision("r", PermissionDecisionKind.Deny), jsonOptions)

    json.Contains("\"Decision\":2") |> should equal true

[<Fact>]
let ``C#-friendly construction accepts enum and string arguments`` () =
    let decision = PermissionDecision("req-2", PermissionDecisionKind.AllowOnce)
    let answer = QuestionAnswer("q-8", "go ahead")

    decision.RequestId |> should equal "req-2"
    decision.Decision |> should equal PermissionDecisionKind.AllowOnce
    answer.QuestionId |> should equal "q-8"
    answer.Answer |> should equal "go ahead"
