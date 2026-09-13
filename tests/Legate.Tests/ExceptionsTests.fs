// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.ExceptionsTests

open System
open System.Collections.Generic
open FsUnit.Xunit
open Legate
open Xunit

let session = SessionId.New()

[<Fact>]
let ``Every subtype derives from LegateException`` () =
    let assertDerived (ty: Type) =
        ty.IsSubclassOf(typeof<LegateException>) |> should equal true

    assertDerived typeof<SessionNotFoundException>
    assertDerived typeof<InvalidSessionStateException>
    assertDerived typeof<AdmissionRejectedException>
    assertDerived typeof<ProviderException>
    assertDerived typeof<DeadlineExceededException>
    assertDerived typeof<WorkspaceException>
    assertDerived typeof<ToolException>
    assertDerived typeof<ProviderNotRegisteredException>
    assertDerived typeof<InvalidBlobKeyException>
    assertDerived typeof<AgentNotFoundException>
    assertDerived typeof<ReadOnlyAgentStoreException>

[<Fact>]
let ``SessionNotFoundException carries the session id and message`` () =
    let ex = SessionNotFoundException(session, "Session 01ABCD not found.")

    ex.SessionId |> should equal session
    ex.Message |> should equal "Session 01ABCD not found."

[<Fact>]
let ``InvalidSessionStateException carries the session id and state`` () =
    let ex = InvalidSessionStateException(session, "Closed", "Session is closed.")

    ex.SessionId |> should equal session
    ex.CurrentState |> should equal "Closed"
    ex.Message |> should equal "Session is closed."

[<Fact>]
let ``AdmissionRejectedException carries the reason`` () =
    let ex = AdmissionRejectedException("saturated", "Request rejected.")

    ex.Reason |> should equal "saturated"
    ex.Message |> should equal "Request rejected."

[<Fact>]
let ``ProviderException carries provider id, status, and retry-after`` () =
    let ex =
        ProviderException("openai", Nullable 429, Nullable(TimeSpan.FromSeconds 30.), "Rate limited.")

    ex.ProviderId |> should equal "openai"
    ex.Status |> should equal (Nullable 429)
    ex.RetryAfter |> should equal (Nullable(TimeSpan.FromSeconds 30.))
    ex.Message |> should equal "Rate limited."

[<Fact>]
let ``ProviderException status and retry-after are nullable`` () =
    let ex =
        ProviderException("openai", Nullable<int>(), Nullable<TimeSpan>(), "Upstream error.")

    ex.Status |> should equal (Nullable<int>())
    ex.RetryAfter |> should equal (Nullable<TimeSpan>())
    ex.Status.HasValue |> should equal false
    ex.RetryAfter.HasValue |> should equal false

[<Fact>]
let ``DeadlineExceededException carries the operation name`` () =
    let ex = DeadlineExceededException("PromptAndWait", "The wait timed out.")

    ex.OperationName |> should equal "PromptAndWait"
    ex.Message |> should equal "The wait timed out."

[<Fact>]
let ``WorkspaceException carries a nullable workspace path`` () =
    let tied = WorkspaceException("/workspaces/abc", "Bind failed.")
    let untied = WorkspaceException(null |> box |> unbox<string>, "Teardown failed.")

    tied.WorkspacePath |> should equal "/workspaces/abc"
    tied.Message |> should equal "Bind failed."
    untied.WorkspacePath |> should equal null
    untied.Message |> should equal "Teardown failed."

[<Fact>]
let ``ToolException carries the tool name`` () =
    let ex = ToolException("mcp:github", "MCP server failed to start.")

    ex.ToolName |> should equal "mcp:github"
    ex.Message |> should equal "MCP server failed to start."

[<Fact>]
let ``ProviderNotRegisteredException carries the provider id and registered list`` () =
    let registered = [ "anthropic"; "openai" ] :> IReadOnlyList<string>

    let ex =
        ProviderNotRegisteredException("google", registered, "No LLM provider is registered under 'google'.")

    ex.ProviderId |> should equal "google"
    ex.RegisteredProviders |> should equal registered
    ex.Message |> should equal "No LLM provider is registered under 'google'."

[<Fact>]
let ``InvalidBlobKeyException carries the offending key`` () =
    let ex =
        InvalidBlobKeyException("../escape", "A blob key may not contain '..' segments.")

    ex.Key |> should equal "../escape"
    ex.Message |> should equal "A blob key may not contain '..' segments."

[<Fact>]
let ``AgentNotFoundException carries the agent id`` () =
    let agent = AgentId.New()

    let ex = AgentNotFoundException(agent, "Agent 01ABCD not found.")

    ex.AgentId |> should equal agent
    ex.Message |> should equal "Agent 01ABCD not found."

[<Fact>]
let ``ReadOnlyAgentStoreException carries the refused operation`` () =
    let ex = ReadOnlyAgentStoreException("UpsertCustomTool", "The store is read-only.")

    ex.Operation |> should equal "UpsertCustomTool"
    ex.Message |> should equal "The store is read-only."

[<Fact>]
let ``Messages are returned exactly as given without appended data`` () =
    let exs: exn list =
        [
            SessionNotFoundException(session, "msg-a")
            InvalidSessionStateException(session, "Running", "msg-b")
            AdmissionRejectedException("saturated", "msg-c")
            ProviderException("openai", Nullable 429, Nullable(TimeSpan.FromSeconds 30.), "msg-d")
            DeadlineExceededException("PromptAndWait", "msg-e")
            WorkspaceException("/workspaces/abc", "msg-f")
            ToolException("mcp:github", "msg-g")
            ProviderNotRegisteredException(
                "google",
                ([ "anthropic" ] :> System.Collections.Generic.IReadOnlyList<string>),
                "msg-h"
            )
            InvalidBlobKeyException("../escape", "msg-i")
            AgentNotFoundException(AgentId.New(), "msg-j")
            ReadOnlyAgentStoreException("DeleteAgent", "msg-k")
        ]

    exs
    |> List.map (fun e -> e.Message)
    |> should
        equal
        [
            "msg-a"
            "msg-b"
            "msg-c"
            "msg-d"
            "msg-e"
            "msg-f"
            "msg-g"
            "msg-h"
            "msg-i"
            "msg-j"
            "msg-k"
        ]

[<Fact>]
let ``Exceptions are catchable as LegateException`` () =
    (fun () ->
        try
            raise (InvalidSessionStateException(session, "Closed", "Session is closed."))
        with :? LegateException as e ->
            box e |> ignore)
    |> should not' (throw typeof<LegateException>)
