// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.ToolSourcesTests

open System
open System.Collections.Generic
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Microsoft.Extensions.AI
open Xunit

/// The generic Deserialize<'T> overload is annotated to return 'T | null,
/// which trips FS3265 for value types; the Type-based overload avoids it.
let deserialize<'T> (json: string) : 'T =
    JsonSerializer.Deserialize(json, typeof<'T>) |> unbox

// Unchecked.defaultof<string> rather than a bare null literal: under
// Nullable=enable the literal trips F# nullness checking on string-typed
// parameters. Deliberate: this is the null string value for rejection cases.
let nullString = Unchecked.defaultof<string>

/// Builds the common context shape for resolution and round-trip tests.
let sampleContext () =
    {
        Tenant = TenantId.Create "acme"
        AgentId = AgentId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"
        SessionId = SessionId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"
    }

// ───────────────────────────────────────────────────────────────────────────
// ToolNameRules

[<Fact>]
let ``Pattern matches the documented shape`` () =
    ToolNameRules.Pattern |> should equal @"\A[a-zA-Z0-9_-]{1,128}\z"

[<Fact>]
let ``Validate accepts the documented shapes`` () =
    ToolNameRules.Validate "a" |> should equal "a"
    ToolNameRules.Validate "A" |> should equal "A"
    ToolNameRules.Validate "0" |> should equal "0"
    ToolNameRules.Validate "read_file" |> should equal "read_file"

    ToolNameRules.Validate "mcp_getServerStatus"
    |> should equal "mcp_getServerStatus"

    ToolNameRules.Validate "-dash_" |> should equal "-dash_"

    ToolNameRules.Validate(String.replicate 128 "a")
    |> should equal (String.replicate 128 "a")

[<Fact>]
let ``TryValidate mirrors Validate for accepted names`` () =
    ToolNameRules.TryValidate "a" |> should equal true
    ToolNameRules.TryValidate "A" |> should equal true
    ToolNameRules.TryValidate "0" |> should equal true
    ToolNameRules.TryValidate "read_file" |> should equal true
    ToolNameRules.TryValidate "mcp_getServerStatus" |> should equal true
    ToolNameRules.TryValidate "-dash_" |> should equal true

    ToolNameRules.TryValidate(String.replicate 128 "a") |> should equal true

[<Fact>]
let ``Validate rejects malformed names`` () =
    (fun () -> ToolNameRules.Validate nullString |> ignore)
    |> should throw typeof<ArgumentNullException>

    (fun () -> ToolNameRules.Validate "" |> ignore)
    |> should throw typeof<ArgumentException>

    (fun () -> ToolNameRules.Validate(String.replicate 129 "a") |> ignore)
    |> should throw typeof<ArgumentException>

    (fun () -> ToolNameRules.Validate "has space" |> ignore)
    |> should throw typeof<ArgumentException>

    (fun () -> ToolNameRules.Validate "name.dot" |> ignore)
    |> should throw typeof<ArgumentException>

    (fun () -> ToolNameRules.Validate "name/slash" |> ignore)
    |> should throw typeof<ArgumentException>

    (fun () -> ToolNameRules.Validate "name\u00E9" |> ignore)
    |> should throw typeof<ArgumentException>

    // $ without \z also matches immediately before a trailing line feed,
    // which would admit a newline-suffixed name and contradict the doc.
    (fun () -> ToolNameRules.Validate "name\n" |> ignore)
    |> should throw typeof<ArgumentException>

    (fun () -> ToolNameRules.Validate "name\n " |> ignore)
    |> should throw typeof<ArgumentException>

[<Fact>]
let ``TryValidate returns false for malformed names without throwing`` () =
    ToolNameRules.TryValidate nullString |> should equal false
    ToolNameRules.TryValidate "" |> should equal false

    ToolNameRules.TryValidate(String.replicate 129 "a") |> should equal false

    ToolNameRules.TryValidate "has space" |> should equal false
    ToolNameRules.TryValidate "name.dot" |> should equal false
    ToolNameRules.TryValidate "name/slash" |> should equal false
    ToolNameRules.TryValidate "name\u00E9" |> should equal false
    ToolNameRules.TryValidate "name\n" |> should equal false
    ToolNameRules.TryValidate "name\n " |> should equal false

[<Fact>]
let ``Validate performs no trimming`` () =
    (fun () -> ToolNameRules.Validate " name " |> ignore)
    |> should throw typeof<ArgumentException>

[<Fact>]
let ``Validate compares ordinally across the Unicode class`` () =
    // A fullwidth letter is not [a-zA-Z0-9] under an ordinal,
    // culture-invariant match; culture-sensitive matching could accept it.
    (fun () -> ToolNameRules.Validate "ｎａｍｅ" |> ignore)
    |> should throw typeof<ArgumentException>

// ───────────────────────────────────────────────────────────────────────────
// ToolSourceContext

[<Fact>]
let ``ToolSourceContext record constructs with F# syntax and compares structurally`` () =
    let context = sampleContext ()

    let changed =
        { context with
            Tenant = TenantId.Create "other"
        }

    changed |> should not' (equal context)

    let copy =
        { context with
            Tenant = TenantId.Create "acme"
        }

    copy |> should equal context

[<Fact>]
let ``ToolSourceContext CLIMutable setters drive construction`` () =
    let context =
        {
            Tenant = TenantId.Create "acme"
            AgentId = AgentId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"
            SessionId = SessionId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"
        }

    context.Tenant.Value |> should equal "acme"
    context.AgentId.Value |> should equal "01ARZ3NDEKTSV4RRFFQ69G5FAV"
    context.SessionId.Value |> should equal "01ARZ3NDEKTSV4RRFFQ69G5FAV"

[<Fact>]
let ``ToolSourceContext JSON round-trip preserves every field`` () =
    let context = sampleContext ()

    let json = JsonSerializer.Serialize context
    let roundTripped = deserialize<ToolSourceContext> json

    roundTripped |> should equal context

[<Fact>]
let ``ToolSourceContext serialises identifiers as plain strings`` () =
    let document = JsonSerializer.Serialize(sampleContext ()) |> JsonDocument.Parse

    document.RootElement.GetProperty("Tenant").GetString() |> should equal "acme"

    document.RootElement.GetProperty("AgentId").GetString()
    |> should equal "01ARZ3NDEKTSV4RRFFQ69G5FAV"

    document.RootElement.GetProperty("SessionId").GetString()
    |> should equal "01ARZ3NDEKTSV4RRFFQ69G5FAV"

// ───────────────────────────────────────────────────────────────────────────
// IToolSource and IToolSourceLifecycle

/// A bare AITool: the base type is abstract with only virtual plumbing
/// members, which is all a test double needs to carry a name.
type NamedTool(name: string) =
    inherit AITool()

    override _.Name = name

/// Test double implementing both contracts, recording resolutions and
/// lifecycle calls internally and answering from a scripted tool list.
type FakeToolSource(tools: NamedTool list) =

    let lockObject = obj ()
    let mutable resolved: ToolSourceContext list = []
    let mutable starts: CancellationToken list = []
    let mutable stops: CancellationToken list = []

    interface IToolSource with
        member _.GetTools(context) =
            lock lockObject (fun () -> resolved <- context :: resolved)

            let list = ResizeArray<AITool>()

            for tool in tools do
                list.Add(tool :> AITool)

            Task.FromResult(list :> IReadOnlyList<AITool>)

    interface IToolSourceLifecycle with
        member _.StartAsync(cancellationToken) =
            lock lockObject (fun () -> starts <- cancellationToken :: starts)

            Task.CompletedTask

        member _.StopAsync(cancellationToken) =
            lock lockObject (fun () -> stops <- cancellationToken :: stops)

            Task.CompletedTask

    member _.Resolved = lock lockObject (fun () -> List.rev resolved)

    member _.Started = lock lockObject (fun () -> List.rev starts)

    member _.Stopped = lock lockObject (fun () -> List.rev stops)

[<Fact>]
let ``Source resolves tools for the requested context`` () =
    let source = FakeToolSource([ NamedTool("read_file") ])
    let contract = source :> IToolSource

    let context = sampleContext ()
    let resolved = contract.GetTools(context).GetAwaiter().GetResult()

    resolved.Count |> should equal 1
    resolved[0].Name |> should equal "read_file"
    source.Resolved |> should equal [ context ]

[<Fact>]
let ``Source returns an empty list on degraded conditions and the runtime reads empty as empty`` () =
    let source = FakeToolSource([]) :> IToolSource

    let resolved = source.GetTools(sampleContext ()).GetAwaiter().GetResult()

    resolved |> should not' (be null)
    resolved.Count |> should equal 0

[<Fact>]
let ``Lifecycle start and stop run in order per source instance`` () =
    let source = FakeToolSource([])
    let lifecycle = source :> IToolSourceLifecycle

    lifecycle.StartAsync(CancellationToken.None).GetAwaiter().GetResult()
    lifecycle.StartAsync(CancellationToken.None).GetAwaiter().GetResult()

    lifecycle.StopAsync(CancellationToken.None).GetAwaiter().GetResult()

    source.Started.Length |> should equal 2
    source.Stopped.Length |> should equal 1

[<Fact>]
let ``Lifecycle tokens reach the source`` () =
    use cts = new CancellationTokenSource()
    let token = cts.Token

    let source = FakeToolSource([])
    let lifecycle = source :> IToolSourceLifecycle

    lifecycle.StartAsync(token).GetAwaiter().GetResult()
    lifecycle.StopAsync(token).GetAwaiter().GetResult()

    source.Started |> should equal [ token ]
    source.Stopped |> should equal [ token ]

[<Fact>]
let ``A source can implement IToolSource without IToolSourceLifecycle`` () =
    // The lifecycle is optional: a source that owns no resources answers
    // GetTools and nothing else. Implementability is the assertion; the
    // source never exercises its lifecycle, which stays uncalled.
    let source = FakeToolSource([ NamedTool("read_file") ])
    let contract = source :> IToolSource

    // Counts rather than `should equal []`: the matcher boxes a generic
    // empty list, which does not compare equal to a typed empty list.
    source.Started.Length |> should equal 0
    source.Stopped.Length |> should equal 0

    let resolved = contract.GetTools(sampleContext ()).GetAwaiter().GetResult()

    resolved.Count |> should equal 1
    source.Resolved.Length |> should equal 1
