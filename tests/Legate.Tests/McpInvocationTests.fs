// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.McpInvocationTests

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate.Mcp
open ModelContextProtocol
open Xunit

// Scripted sessions only: no subprocess, no socket. The scripted session
// answers from memory, records the token it received, and optionally
// raises, so error mapping, cancellation passthrough, and observation
// stamping are verified without live servers.

// ──────────────────────────
// Scripted session

/// One scripted session: answers every call with the scripted result,
/// records the token it received, and raises the scripted failure instead
/// when one is set.
type internal ScriptedInvokeSession(answer: McpCallResult, failure: exn | null) =

    let mutable seen: CancellationToken option = None

    interface IMcpServerSession with
        member _.ServerName: string = "alpha"

        member _.ListToolsAsync
            (_cancellationToken: CancellationToken)
            : Task<IReadOnlyList<McpDiscovery.McpDiscoveredTool>> =
            task {
                return ResizeArray<McpDiscovery.McpDiscoveredTool>() :> IReadOnlyList<McpDiscovery.McpDiscoveredTool>
            }

        member _.CallToolAsync
            (_toolName: string, _arguments: IReadOnlyDictionary<string, obj>, cancellationToken: CancellationToken)
            : Task<McpCallResult> =
            task {
                seen <- Some cancellationToken

                match box failure with
                | :? exn as toRaise -> return! Task.FromException<McpCallResult>(toRaise)
                | _ ->
                    // A cancelled turn abandons the call: wait until the
                    // token fires so cancellation propagates instead of the
                    // answer returning.
                    do! Task.Delay(Timeout.Infinite, cancellationToken)
                    return answer
            }

    interface IAsyncDisposable with
        member _.DisposeAsync() : ValueTask = ValueTask.CompletedTask

    /// The token the session received on its latest call, when called.
    member _.Seen: CancellationToken option = seen

/// A session answering immediately without waiting on the token.
type internal ImmediateSession(answer: McpCallResult, failure: exn | null) =

    let mutable seen: CancellationToken option = None

    interface IMcpServerSession with
        member _.ServerName: string = "alpha"

        member _.ListToolsAsync
            (_cancellationToken: CancellationToken)
            : Task<IReadOnlyList<McpDiscovery.McpDiscoveredTool>> =
            task {
                return ResizeArray<McpDiscovery.McpDiscoveredTool>() :> IReadOnlyList<McpDiscovery.McpDiscoveredTool>
            }

        member _.CallToolAsync
            (_toolName: string, _arguments: IReadOnlyDictionary<string, obj>, cancellationToken: CancellationToken)
            : Task<McpCallResult> =
            task {
                seen <- Some cancellationToken

                match box failure with
                | :? exn as toRaise -> return! Task.FromException<McpCallResult>(toRaise)
                | _ -> return answer
            }

    interface IAsyncDisposable with
        member _.DisposeAsync() : ValueTask = ValueTask.CompletedTask

    /// The token the session received on its latest call, when called.
    member _.Seen: CancellationToken option = seen

// ──────────────────────────
// Helpers

/// The empty call arguments.
let private noArguments () : IReadOnlyDictionary<string, obj> =
    Dictionary<string, obj>(StringComparer.Ordinal) :> IReadOnlyDictionary<string, obj>

/// Invokes through the scripted session, capturing observations.
let private invoke
    (session: IMcpServerSession)
    (observations: ResizeArray<McpInvocation.McpCallObservation>)
    (cancellationToken: CancellationToken)
    : string =
    let sink =
        Action<McpInvocation.McpCallObservation>(fun observation -> observations.Add(observation))

    let pending =
        McpInvocation.invokeAsync session "alpha_read" "read" (noArguments ()) cancellationToken sink

    pending.GetAwaiter().GetResult()

/// The single stamped observation, failing when the count differs.
let private single (observations: ResizeArray<McpInvocation.McpCallObservation>) : McpInvocation.McpCallObservation =
    observations.Count |> should equal 1
    observations[0]

// ──────────────────────────
// Mapping and observations

[<Fact>]
let ``Success returns the text and stamps a clean observation`` () =
    let observations = ResizeArray<McpInvocation.McpCallObservation>()
    let session = ImmediateSession({ Text = "ok"; IsError = false }, null)

    invoke (session :> IMcpServerSession) observations CancellationToken.None
    |> should equal "ok"

    let stamped = single observations
    stamped.ToolName |> should equal "alpha_read"
    stamped.Text |> should equal "ok"
    stamped.IsError |> should equal false
    (stamped.Duration >= TimeSpan.Zero) |> should equal true

[<Fact>]
let ``IsError maps to Error from carrying the tool name`` () =
    let observations = ResizeArray<McpInvocation.McpCallObservation>()

    let session =
        ImmediateSession(
            {
                Text = "denied by policy"
                IsError = true
            },
            null
        )

    invoke (session :> IMcpServerSession) observations CancellationToken.None
    |> should equal "Error from alpha_read: denied by policy"

    let stamped = single observations
    stamped.ToolName |> should equal "alpha_read"
    stamped.Text |> should equal "Error from alpha_read: denied by policy"
    stamped.IsError |> should equal true
    (stamped.Duration >= TimeSpan.Zero) |> should equal true

[<Fact>]
let ``Transport failure maps to Error calling with the exception shape`` () =
    let observations = ResizeArray<McpInvocation.McpCallObservation>()

    let session =
        ImmediateSession({ Text = ""; IsError = false }, InvalidOperationException("connection reset"))

    invoke (session :> IMcpServerSession) observations CancellationToken.None
    |> should equal "Error calling alpha_read: InvalidOperationException: connection reset"

    let stamped = single observations
    stamped.IsError |> should equal true

    stamped.Text
    |> should equal "Error calling alpha_read: InvalidOperationException: connection reset"

    (stamped.Duration >= TimeSpan.Zero) |> should equal true

[<Fact>]
let ``Elicitation decline maps to Error calling text and the turn continues`` () =
    let observations = ResizeArray<McpInvocation.McpCallObservation>()

    // The SDK surfaces a declined elicitation as a call failure: the fake
    // raises the SDK exception type with the decline message, and invoke
    // returns the mapped text instead of throwing, so the turn continues.
    let session =
        ImmediateSession({ Text = ""; IsError = false }, McpException("The elicitation request was declined."))

    let text = invoke (session :> IMcpServerSession) observations CancellationToken.None

    text.StartsWith("Error calling alpha_read: McpException:", StringComparison.Ordinal)
    |> should equal true

    text.Contains("declined") |> should equal true

    let stamped = single observations
    stamped.IsError |> should equal true
    stamped.Text |> should equal text

[<Fact>]
let ``Null sink observes nothing`` () =
    let session = ImmediateSession({ Text = "ok"; IsError = false }, null)

    let pending =
        McpInvocation.invokeAsync
            (session :> IMcpServerSession)
            "alpha_read"
            "read"
            (noArguments ())
            CancellationToken.None
            null

    pending.GetAwaiter().GetResult() |> should equal "ok"

// ──────────────────────────
// Cancellation

[<Fact>]
let ``Cancel aborts the call and stamps nothing`` () =
    let observations = ResizeArray<McpInvocation.McpCallObservation>()
    let session = ScriptedInvokeSession({ Text = "ok"; IsError = false }, null)

    use cts = new CancellationTokenSource()

    let pending =
        McpInvocation.invokeAsync
            (session :> IMcpServerSession)
            "alpha_read"
            "read"
            (noArguments ())
            cts.Token
            (Action<McpInvocation.McpCallObservation>(fun observation -> observations.Add(observation)))

    cts.Cancel()

    (fun () -> pending.GetAwaiter().GetResult() |> ignore)
    |> should throw typeof<OperationCanceledException>

    observations.Count |> should equal 0

[<Fact>]
let ``Turn token reaches CallToolAsync`` () =
    let session = ImmediateSession({ Text = "ok"; IsError = false }, null)
    use cts = new CancellationTokenSource()

    let pending =
        McpInvocation.invokeAsync (session :> IMcpServerSession) "alpha_read" "read" (noArguments ()) cts.Token null

    pending.GetAwaiter().GetResult() |> should equal "ok"

    match session.Seen with
    | Some seen -> seen |> should equal cts.Token
    | None -> Assert.Fail("The session never received the call.") |> ignore
