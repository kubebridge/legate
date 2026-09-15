// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.McpProjectionTests

open System
open System.Collections.Generic
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Legate.Mcp
open Microsoft.Extensions.AI
open Xunit

// Unchecked.defaultof<string> rather than a bare null literal: under
// Nullable=enable the literal trips F# nullness checking on string-typed
// fields. Deliberate: these are the null optional request fields.
let private nullString = Unchecked.defaultof<string>

/// One discovered tool with the destructive hint under test.
let private discoveredTool (destructive: bool) : McpDiscovery.McpDiscoveredTool =
    {
        ServerName = "alpha"
        ToolName = "wipe"
        Title = "Wipe"
        Description = "Wipes the scratch area."
        InputSchema = McpDiscovery.DefaultInputSchema
        OutputSchema = Some(JsonDocument.Parse("""{"type":"string"}""").RootElement)
        DestructiveHint = destructive
        ReadOnlyHint = Some false
        IdempotentHint = None
        OpenWorldHint = None
        AnnotationTitle = null
    }

/// An invoker returning a fixed answer, recording the call it received.
let private stubInvoker (answer: string) (calls: ResizeArray<string>) =
    Func<AIFunctionArguments, CancellationToken, Task<string>>(fun _ _ ->
        calls.Add("called")
        Task.FromResult(answer))

let private project (destructive: bool) =
    McpProjectionFactory.create "alpha_wipe" (discoveredTool destructive) (stubInvoker "done" (ResizeArray<string>()))

[<Fact>]
let ``Destructive hint projects as boolean true`` () =
    let fn = project true
    let mutable boxed = Unchecked.defaultof<obj>

    fn.AdditionalProperties.TryGetValue(McpProjection.DestructiveHintKey, &boxed)
    |> should equal true

    boxed :?> bool |> should equal true

[<Fact>]
let ``Non-destructive tools carry no destructive trigger`` () =
    let fn = project false
    let mutable boxed = Unchecked.defaultof<obj>

    if fn.AdditionalProperties.TryGetValue(McpProjection.DestructiveHintKey, &boxed) then
        boxed :?> bool |> should equal false

[<Fact>]
let ``Destructive projection makes AskForWritesAndExec ask`` () =
    let fn = project true
    let table = Dictionary<string, AITool>(StringComparer.Ordinal)
    table["alpha_wipe"] <- fn :> AITool

    let policy =
        AskForWritesAndExecPermissionPolicy(table :> IReadOnlyDictionary<string, AITool>) :> IPermissionPolicy

    let request: PermissionRequest =
        {
            SessionId = SessionId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"
            TurnId = TurnId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"
            ToolName = "alpha_wipe"
            ToolSourceId = nullString
            ArgumentPreview = nullString
            RequestId = "req-1"
        }

    policy.Evaluate(request) :? AskVerdict |> should equal true

[<Fact>]
let ``Non-destructive projection stays allowed`` () =
    let fn = project false
    let table = Dictionary<string, AITool>(StringComparer.Ordinal)
    table["alpha_wipe"] <- fn :> AITool

    let policy =
        AskForWritesAndExecPermissionPolicy(table :> IReadOnlyDictionary<string, AITool>) :> IPermissionPolicy

    let request: PermissionRequest =
        {
            SessionId = SessionId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"
            TurnId = TurnId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"
            ToolName = "alpha_wipe"
            ToolSourceId = nullString
            ArgumentPreview = nullString
            RequestId = "req-2"
        }

    policy.Evaluate(request) :? AllowVerdict |> should equal true

[<Fact>]
let ``Title description and schemas forward onto the function`` () =
    let fn = project true
    fn.Name |> should equal "alpha_wipe"
    fn.Description |> should equal "Wipes the scratch area."
    fn.JsonSchema.GetRawText() |> should equal """{"type":"object"}"""
    fn.ReturnJsonSchema.HasValue |> should equal true
    fn.ReturnJsonSchema.Value.GetRawText() |> should equal """{"type":"string"}"""

    let mutable title = Unchecked.defaultof<obj>
    fn.AdditionalProperties.TryGetValue("title", &title) |> should equal true
    title :?> string |> should equal "Wipe"

[<Fact>]
let ``Annotation hints forward as properties`` () =
    let fn = project true
    let mutable readOnly = Unchecked.defaultof<obj>

    fn.AdditionalProperties.TryGetValue("readOnlyHint", &readOnly)
    |> should equal true

    readOnly :?> bool |> should equal false

[<Fact>]
let ``Invoke forwards to the owning session invoker`` () =
    let calls = ResizeArray<string>()

    let fn =
        McpProjectionFactory.create "alpha_wipe" (discoveredTool false) (stubInvoker "session-answer" calls)

    let result =
        fn.InvokeAsync(AIFunctionArguments(), CancellationToken.None).GetAwaiter().GetResult()

    match box result with
    | :? string as text -> text |> should equal "session-answer"
    | _ -> Assert.Fail("Invoke must return the session answer.") |> ignore

    calls.Count |> should equal 1

[<Fact>]
let ``Destructive hint key is the documented string`` () =
    McpProjection.DestructiveHintKey |> should equal "destructiveHint"
