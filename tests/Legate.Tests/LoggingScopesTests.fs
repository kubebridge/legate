// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.LoggingScopesTests

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Legate.Storage.InMemory
open Legate.Testing
open Microsoft.Extensions.AI
open Microsoft.Extensions.Logging
open Xunit

// Logging scopes and redaction (issue 93): one internal LoggingScopes
// module carries the six canonical scope keys (SessionId, TurnId, AgentId,
// TenantId, Attempt, ClaimOwner) every runtime log line carries, plus a
// redactForLog wrapper over JournalWriter.redactText. Nullable ILogger
// threads through the hot path with the NullLogger fallback.

/// One captured log line with the scopes active when it logged.
type private LoggedLine =
    {
        Level: string
        Text: string
        Scopes: (string * obj) list
    }

/// An ILogger capturing every entry with the scopes active at log time.
type private ScopeCapturingLogger() =
    let gate = obj ()
    let entries = ResizeArray<LoggedLine>()
    let stack = ResizeArray<(string * obj) list>()

    let toPairs (state: obj | null) : (string * obj) list =
        if isNull (box state) then
            []
        else
            match state with
            | :? IReadOnlyList<KeyValuePair<string, obj>> as kvs ->
                kvs |> Seq.map (fun kv -> kv.Key, kv.Value) |> List.ofSeq
            | :? IEnumerable<KeyValuePair<string, obj>> as kvs ->
                kvs |> Seq.map (fun kv -> kv.Key, kv.Value) |> List.ofSeq
            | _ -> []

    interface ILogger with
        member _.BeginScope<'TState when 'TState: not null>(state: 'TState) : IDisposable =
            let pairs = toPairs (box state)
            lock gate (fun () -> stack.Add(pairs))

            { new IDisposable with
                member _.Dispose() =
                    lock gate (fun () ->
                        if stack.Count > 0 then
                            stack.RemoveAt(stack.Count - 1))
            }

        member _.IsEnabled(_) = true

        member _.Log<'TState>
            (logLevel: LogLevel, _eventId: EventId, state: 'TState, ex: exn, formatter: Func<'TState, exn, string>)
            : unit =
            let text = formatter.Invoke(state, ex)
            let scopes = lock gate (fun () -> stack |> Seq.concat |> List.ofSeq)

            lock gate (fun () ->
                entries.Add(
                    {
                        Level = logLevel.ToString()
                        Text = text
                        Scopes = scopes
                    }
                ))

    /// Every captured line, oldest first.
    member _.Entries: LoggedLine list = lock gate (fun () -> entries |> List.ofSeq)

let private requiredKeys =
    [
        LoggingScopes.SessionIdKey
        LoggingScopes.TurnIdKey
        LoggingScopes.AgentIdKey
        LoggingScopes.TenantIdKey
        LoggingScopes.AttemptKey
        LoggingScopes.ClaimOwnerKey
    ]

/// Asserts every captured line carries all six scope keys.
let private shouldCarryAllScopes (entries: LoggedLine list) =
    entries |> should not' (equal [])

    for entry in entries do
        for key in requiredKeys do
            entry.Scopes |> List.exists (fun (name, _) -> name = key) |> should equal true

// ──────────────────────────────────────────────────────────────────────────
// Task 1: the module

[<Fact>]
let ``The canonical scope keys carry the agreed names`` () =
    LoggingScopes.SessionIdKey |> should equal "SessionId"
    LoggingScopes.TurnIdKey |> should equal "TurnId"
    LoggingScopes.AgentIdKey |> should equal "AgentId"
    LoggingScopes.TenantIdKey |> should equal "TenantId"
    LoggingScopes.AttemptKey |> should equal "Attempt"
    LoggingScopes.ClaimOwnerKey |> should equal "ClaimOwner"

    LoggingScopes.AllKeys
    |> should
        equal
        [|
            "SessionId"
            "TurnId"
            "AgentId"
            "TenantId"
            "Attempt"
            "ClaimOwner"
        |]

[<Fact>]
let ``createScope always carries all six keys with the given values`` () =
    let scope =
        LoggingScopes.createScope "acme" "session-1" "turn-1" "agent-1" 2 "owner-1"

    let table = scope |> Seq.map (fun kv -> kv.Key, kv.Value) |> Map.ofSeq
    table["SessionId"] |> should equal ("session-1" :> obj)
    table["TurnId"] |> should equal ("turn-1" :> obj)
    table["AgentId"] |> should equal ("agent-1" :> obj)
    table["TenantId"] |> should equal ("acme" :> obj)
    table["Attempt"] |> should equal (2 :> obj)
    table["ClaimOwner"] |> should equal ("owner-1" :> obj)

[<Fact>]
let ``createScope maps unknown values to empty keys`` () =
    let scope = LoggingScopes.createScope null null null null 0 null
    let table = scope |> Seq.map (fun kv -> kv.Key, kv.Value) |> Map.ofSeq
    table.Count |> should equal 6
    table["SessionId"] |> should equal ("" :> obj)
    table["Attempt"] |> should equal (0 :> obj)

[<Fact>]
let ``redactForLog matches redactText shapes including null passthrough`` () =
    LoggingScopes.redactForLog Unchecked.defaultof<string> |> should equal null

    LoggingScopes.redactForLog "plain checkout text"
    |> should equal "plain checkout text"

    let keyed = "call with sk-ant-secret-key-12345678 inside"
    let redacted = LoggingScopes.redactForLog keyed
    redacted.Contains("sk-ant-secret-key-12345678") |> should equal false
    redacted.Contains(JournalWriter.RedactedText) |> should equal true
    let keyedAgain = JournalWriter.redactText keyed
    redacted |> should equal keyedAgain
    let passworded = "connection password=hunter2-secret"

    LoggingScopes.redactForLog passworded
    |> should equal (JournalWriter.redactText passworded)

[<Fact>]
let ``resolveLogger falls back to the NullLogger for null`` () =
    let resolved = LoggingScopes.resolveLogger null
    resolved |> should not' (equal null)
    let logger = ScopeCapturingLogger() :> ILogger
    LoggingScopes.resolveLogger logger |> should equal logger

[<Fact>]
let ``beginScope carries the six keys onto every line`` () =
    let logger = ScopeCapturingLogger()

    let scope =
        LoggingScopes.createScope "acme" "session-1" "turn-1" "agent-1" 1 "owner-1"

    let live = LoggingScopes.resolveLogger (logger :> ILogger)
    use _scope = LoggingScopes.beginScope live scope
    live.LogInformation("scoped line with {Secret}", LoggingScopes.redactForLog "sk-ant-secret-key-12345678")
    let entries = logger.Entries
    entries.Length |> should equal 1
    shouldCarryAllScopes entries
    entries[0].Text.Contains("sk-ant-secret-key-12345678") |> should equal false

// ──────────────────────────────────────────────────────────────────────────
// Task 5: end-to-end acceptance over one turn

/// An ILlmDelay that never elapses: the turn runs without a deadline.
type private NeverDelay() =
    interface ILlmDelay with
        member _.Delay(_, cancellationToken) =
            Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken)

[<Fact>]
let ``One turn carries all six scopes on every line and leaks no fixture secret`` () =
    let tenant = TenantId.Create "acme"
    let sessionId = SessionId.New()
    let turnId = TurnId.New()
    let agentId = AgentId.New()
    let secret = "sk-ant-acceptance-secret-99999999"
    let logger = ScopeCapturingLogger()

    let scope =
        LoggingScopes.createScope
            (tenant.ToString())
            (sessionId.ToString())
            (turnId.ToString())
            (agentId.ToString())
            1
            "owner-acceptance"

    let method = Func<string>(fun () -> sprintf "tool output holding %s inside" secret)

    let fn =
        AIFunctionFactory.Create(
            method,
            "lookup",
            Unchecked.defaultof<string>,
            Unchecked.defaultof<System.Text.Json.JsonSerializerOptions>
        )

    let tools =
        let table = Dictionary<string, AITool>()
        table["lookup"] <- fn :> AITool
        table :> IReadOnlyDictionary<string, AITool>

    let client =
        new ScriptedChatClient(
            ResizeArray<ScriptStep>(
                [|
                    ScriptStep.ToolCall("call-1", "lookup")
                    ScriptStep.Text "done"
                |]
            )
            :> IReadOnlyList<ScriptStep>
        )

    let history =
        ResizeArray<ChatMessage>([| ChatMessage(ChatRole.User, "hi") |]) :> IList<ChatMessage>

    let options =
        { TurnLoop.TurnLoopOptions.Default with
            Logger = logger :> ILogger
            LogScope = scope
        }

    let result =
        TurnLoop.runAsync
            (client :> IChatClient)
            history
            tools
            options
            (NeverDelay() :> ILlmDelay)
            CancellationToken.None
            (fun () -> true)
        |> fun task -> task.GetAwaiter().GetResult()

    result.Status |> should equal TurnStatus.Completed

    let clock = TestClock(DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero))
    let database = InMemoryDatabase(clock)
    let sessions = InMemorySessionStore(database) :> ISessionStore
    let events = InMemorySessionEventStore(database) :> ISessionEventStore

    let session =
        {
            Id = sessionId
            Tenant = tenant
            AgentId = agentId
            Title = "acceptance"
            State = SessionState.Idle
            CurrentTurnId = Unchecked.defaultof<Nullable<TurnId>>
            CreatedAt = DateTimeOffset.MinValue
            UpdatedAt = DateTimeOffset.MinValue
            ClosedAt = Unchecked.defaultof<Nullable<DateTimeOffset>>
            WorkspaceBinding = null
            Options = SessionOptions()
            PermissionGrants = ResizeArray<string>() :> IReadOnlyList<string>
        }

    sessions.CreateSession(tenant, session, CancellationToken.None).GetAwaiter().GetResult()
    |> ignore

    let payload = UserMessagePayload(UserMessage.Text("acceptance")) :> InboxPayload

    sessions
        .AppendInboxMessage(tenant, sessionId, payload, DeliveryMode.Queue, CancellationToken.None)
        .GetAwaiter()
        .GetResult()
    |> ignore

    let claim =
        match
            sessions
                .ClaimNextTurn(tenant, sessionId, "owner-acceptance", TimeSpan.FromMinutes(5.0), CancellationToken.None)
                .GetAwaiter()
                .GetResult()
        with
        | :? TurnLeaseRenewed as renewed -> renewed.Claim
        | state -> failwith $"Expected a granted claim, observed %s{state.GetType().Name}."

    let textEvent =
        TextDeltaEvent(
            sessionId,
            claim.TurnId,
            Nullable<int64>(),
            DateTimeOffset.UtcNow,
            sprintf "note holding %s inside" secret
        )
        :> SessionEvent

    let batch =
        ResizeArray<SessionEvent>([| textEvent |]) :> IReadOnlyList<SessionEvent>

    let outcome =
        JournalWriter.appendWithTokenAsyncWithLogger
            events
            tenant
            sessionId
            claim.Token
            batch
            CancellationToken.None
            (logger :> ILogger)
            scope
        |> fun task -> task.GetAwaiter().GetResult()

    match outcome with
    | JournalWriter.JournalWriteResult.JournalAppended _ -> ()
    | _ -> failwith "Expected the acceptance append to land."

    let entries = logger.Entries
    shouldCarryAllScopes entries

    for entry in entries do
        entry.Text.Contains(secret) |> should equal false
