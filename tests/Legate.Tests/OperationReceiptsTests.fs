// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.OperationReceiptsTests

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Legate.Storage.InMemory
open Legate.Storage.Sqlite
open Legate.Testing
open Microsoft.Extensions.AI
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Hosting
open Xunit

// Accepted-operation receipts and authoritative result lookup (issue
// 381): the public receipt promoted from the #374 durable turn identity
// plus position, served behind the #363 winning execution_settlements
// row. No new table, id, migration, or backfill. Live waits stay in #383.

let private waitBound = TimeSpan.FromSeconds 10.0

let private awaitWhat (work: Task<'T>) (what: string) : Task<'T> =
    task {
        try
            return! work.WaitAsync(waitBound, CancellationToken.None)
        with :? TimeoutException ->
            let ex = TimeoutException($"The test timed out waiting for {what}.")
            return raise ex
    }

let private scripted (steps: ScriptStep list) : ScriptedChatClient =
    new ScriptedChatClient(ResizeArray<ScriptStep>(steps) :> IReadOnlyList<ScriptStep>)

let private sourced (tools: AITool list) : StaticToolSource =
    new StaticToolSource(ResizeArray<AITool>(tools) :> IReadOnlyList<AITool>)

let private createServices (chatClient: ScriptedChatClient) (tools: StaticToolSource) : IServiceCollection =
    let database = InMemoryDatabase()
    let services = ServiceCollection() :> IServiceCollection

    LegateServiceCollectionExtensions.AddLegate(
        services,
        ?configure =
            Some(fun (builder: LegateBuilder) ->
                builder.Llm.AddProvider(BuilderTests.StubLlmProvider()) |> ignore
                builder.Storage.UseSessionStore(InMemorySessionStore(database)) |> ignore
                builder.Workspace.UseRuntime(BuilderTests.StubWorkspaceRuntime()) |> ignore
                builder.Tools.AddSource(tools) |> ignore)
    )
    |> ignore

    services.AddSingleton<ISessionEventStore>(InMemorySessionEventStore(database))
    |> ignore

    services.AddSingleton<IChatClient>(chatClient) |> ignore
    services

let private actorServiceOf (provider: IServiceProvider) : LocalActorSystemService =
    provider.GetServices<IHostedService>()
    |> Seq.pick (fun service ->
        match service with
        | :? LocalActorSystemService as local -> Some local
        | _ -> None)

let private stopQuietly (service: LocalActorSystemService) : Task =
    task {
        try
            do! (service :> IHostedService).StopAsync(CancellationToken.None)
        with _ ->
            ()
    }

let private withClient (provider: IServiceProvider) (work: SessionClient -> Task<'T>) : Task<'T> =
    task {
        let service = actorServiceOf provider
        do! (service :> IHostedService).StartAsync(CancellationToken.None)
        let client = provider.GetRequiredService<SessionClient>()

        try
            let! outcome = work client
            do! stopQuietly service
            return outcome
        with ex ->
            do! stopQuietly service
            let inner = ex
            return raise inner
    }

let private openSession (client: SessionClient) : Task<Session> =
    SessionClientOperations.OpenSessionAsync(client, AgentId.New(), null, CancellationToken.None)

let private settleWaiter (sessionId: SessionId) : TaskCompletionSource<TurnResult> =
    PromptWaitHubs.GetOrAdd(sessionId).EnqueueSettle()

/// Asserts non-null and unboxes nullable references for member access.
let private presentEntry (value: InboxEntry | null) : InboxEntry =
    Assert.False(isNull (box value))
    unbox<InboxEntry> (box value)

/// Asserts non-null and unboxes a nullable settlement outcome.
let private presentOutcome (value: SessionSettlementOutcome | null) : SessionSettlementOutcome =
    Assert.False(isNull (box value))
    unbox<SessionSettlementOutcome> (box value)

/// Asserts non-null and unboxes a nullable turn result.
let private presentResult (value: TurnResult | null) : TurnResult =
    Assert.False(isNull (box value))
    unbox<TurnResult> (box value)

/// Permission policy asking once for the gated tool, allowing the rest.
type private AskReceiptPolicy(gated: string) =
    interface IPermissionPolicy with
        member _.Evaluate(request) =
            if request.ToolName = gated then
                PermissionVerdict.Ask
            else
                PermissionVerdict.Allow

// ──────────────────────────────────────────────────────────────
// Store reads: InMemory
// ──────────────────────────────────────────────────────────────

type InMemoryOperationReadTests() =

    let tenant = TenantId.Create "op-receipts"
    let database = InMemoryDatabase(TimeProvider.System, InMemoryStoreOptions())
    let store = InMemorySessionStore(database) :> ISessionStore
    let settlement = store :?> ISessionSettlementStore

    let freshSession () =
        task {
            let options = SessionOptions()
            options.FormatVersion <- 1

            let session =
                {
                    Id = SessionId.New()
                    Tenant = tenant
                    AgentId = AgentId.New()
                    Title = "receipts"
                    State = SessionState.Idle
                    CurrentTurnId = Nullable()
                    CreatedAt = DateTimeOffset.UtcNow
                    UpdatedAt = DateTimeOffset.UtcNow
                    ClosedAt = Nullable()
                    WorkspaceBinding = null
                    Options = options
                    PermissionGrants = Array.empty<string> :> IReadOnlyList<string>
                }

            let! _ = store.CreateSession(tenant, session, CancellationToken.None)
            return session
        }

    [<Fact>]
    member _.``entry read returns the accepted row and committed is null before settlement``() =
        task {
            let! session = freshSession ()

            let! entry =
                store.AppendInboxMessage(
                    tenant,
                    session.Id,
                    UserMessagePayload(UserMessage.Text "hi"),
                    DeliveryMode.Queue,
                    CancellationToken.None
                )

            let! foundRaw = settlement.TryReadEntry(tenant, session.Id, entry.Position, CancellationToken.None)
            let found = presentEntry foundRaw
            Assert.Equal(entry.Position, found.Position)
            Assert.Equal(entry.TurnId, found.TurnId)

            let! committed = settlement.TryReadCommitted(tenant, session.Id, entry.Position, CancellationToken.None)
            Assert.True(isNull (box committed))
        }
        :> Task

    [<Fact>]
    member _.``committed read serves the winner and losing reports never replace it``() =
        task {
            let! session = freshSession ()

            let! entry =
                store.AppendInboxMessage(
                    tenant,
                    session.Id,
                    UserMessagePayload(UserMessage.Text "hi"),
                    DeliveryMode.Queue,
                    CancellationToken.None
                )

            let! lease =
                store.ClaimNextTurn(tenant, session.Id, "owner", TimeSpan.FromMinutes 5.0, CancellationToken.None)

            let claim =
                match lease with
                | :? TurnLeaseHeld as held -> held.Claim
                | :? TurnLeaseRenewed as renewed -> renewed.Claim
                | _ -> failwith "Expected a held claim."

            Assert.True(
                settlement.AdmitExecution(tenant, session.Id, entry.Position, claim, CancellationToken.None).Result
            )

            let okResult =
                {
                    AssistantText = "done"
                    Status = TurnStatus.Completed
                    Iterations = 1
                    Usage = { InputTokens = 1L; OutputTokens = 1L }
                    Outcome = TurnFinished("done") :> TurnOutcome
                }

            let key = Guid.NewGuid().ToString("N")

            let request =
                SessionSettlementRequest(session.Id, entry.Position, claim, Nullable(), okResult, key, null)

            let! applied = settlement.SettleExecution(tenant, request, CancellationToken.None)
            Assert.Equal(SessionSettlementStatus.Applied, applied.Status)

            let! winnerRaw = settlement.TryReadCommitted(tenant, session.Id, entry.Position, CancellationToken.None)
            let winner = presentOutcome winnerRaw
            let winnerResult = presentResult winner.Result
            Assert.Equal("done", winnerResult.AssistantText)

            let badResult =
                { okResult with
                    Status = TurnStatus.Failed
                }

            let bad =
                SessionSettlementRequest(
                    session.Id,
                    entry.Position,
                    claim,
                    Nullable(),
                    badResult,
                    Guid.NewGuid().ToString("N"),
                    null
                )

            let! rejected = settlement.SettleExecution(tenant, bad, CancellationToken.None)
            Assert.Equal(SessionSettlementStatus.Rejected, rejected.Status)

            let! stillRaw = settlement.TryReadCommitted(tenant, session.Id, entry.Position, CancellationToken.None)

            let stillWinner = presentOutcome stillRaw
            let stillResult = presentResult stillWinner.Result
            Assert.Equal("done", stillResult.AssistantText)
            Assert.Equal(TurnStatus.Completed, stillResult.Status)
        }
        :> Task

    [<Fact>]
    member _.``reads are tenant scoped``() =
        task {
            let! session = freshSession ()

            let! entry =
                store.AppendInboxMessage(
                    tenant,
                    session.Id,
                    UserMessagePayload(UserMessage.Text "hi"),
                    DeliveryMode.Queue,
                    CancellationToken.None
                )

            let other = TenantId.Create "op-other"

            let! foreignEntry = settlement.TryReadEntry(other, session.Id, entry.Position, CancellationToken.None)
            Assert.True(isNull (box foreignEntry))

            let! foreignCommitted =
                settlement.TryReadCommitted(other, session.Id, entry.Position, CancellationToken.None)

            Assert.True(isNull (box foreignCommitted))
        }
        :> Task

    [<Fact>]
    member _.``reply entries keep the sentinel and never fabricate a turn``() =
        task {
            let! session = freshSession ()
            let reply = PermissionDecision("req", PermissionDecisionKind.AllowOnce) :> Reply

            let! entry =
                store.AppendInboxMessage(
                    tenant,
                    session.Id,
                    ReplyPayload(reply),
                    DeliveryMode.Queue,
                    CancellationToken.None
                )

            Assert.Equal(Unchecked.defaultof<TurnId>, entry.TurnId)

            let! foundRaw = settlement.TryReadEntry(tenant, session.Id, entry.Position, CancellationToken.None)
            let found = presentEntry foundRaw
            Assert.Equal(Unchecked.defaultof<TurnId>, found.TurnId)
        }
        :> Task

// ──────────────────────────────────────────────────────────────
// Store reads: SQLite including file reopen durability
// ──────────────────────────────────────────────────────────────

type SqliteOperationReadTests() =

    let tenant = TenantId.Create "op-sqlite"

    let openDatabase () =
        let path =
            System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"legate-op-receipts-{Guid.NewGuid():N}.db")

        SqliteDatabase.Open(path), path

    let freshSession (store: ISessionStore) =
        task {
            let options = SessionOptions()
            options.FormatVersion <- 1

            let session =
                {
                    Id = SessionId.New()
                    Tenant = tenant
                    AgentId = AgentId.New()
                    Title = "receipts"
                    State = SessionState.Idle
                    CurrentTurnId = Nullable()
                    CreatedAt = DateTimeOffset.UtcNow
                    UpdatedAt = DateTimeOffset.UtcNow
                    ClosedAt = Nullable()
                    WorkspaceBinding = null
                    Options = options
                    PermissionGrants = Array.empty<string> :> IReadOnlyList<string>
                }

            let! _ = store.CreateSession(tenant, session, CancellationToken.None)
            return session
        }

    [<Fact>]
    member _.``sqlite entry and committed reads match inmemory semantics``() =
        task {
            let database, path = openDatabase ()

            try
                let store = SqliteSessionStore(database) :> ISessionStore
                let settlement = SqliteSessionSettlementStore(database) :> ISessionSettlementStore
                let! session = freshSession store

                let! entry =
                    store.AppendInboxMessage(
                        tenant,
                        session.Id,
                        UserMessagePayload(UserMessage.Text "hi"),
                        DeliveryMode.Queue,
                        CancellationToken.None
                    )

                let! foundRaw = settlement.TryReadEntry(tenant, session.Id, entry.Position, CancellationToken.None)
                let found = presentEntry foundRaw
                Assert.Equal(entry.TurnId, found.TurnId)

                let! before = settlement.TryReadCommitted(tenant, session.Id, entry.Position, CancellationToken.None)
                Assert.True(isNull (box before))
            finally
                (database :> IDisposable).Dispose()

                try
                    System.IO.File.Delete path
                with _ ->
                    ()
        }
        :> Task

    [<Fact>]
    member _.``sqlite winner survives file reopen``() =
        task {
            let database, path = openDatabase ()
            let mutable sessionId = SessionId.New()
            let mutable position = 0L

            try
                let store = SqliteSessionStore(database) :> ISessionStore
                let settlement = SqliteSessionSettlementStore(database) :> ISessionSettlementStore
                let! session = freshSession store
                sessionId <- session.Id

                let! entry =
                    store.AppendInboxMessage(
                        tenant,
                        session.Id,
                        UserMessagePayload(UserMessage.Text "hi"),
                        DeliveryMode.Queue,
                        CancellationToken.None
                    )

                position <- entry.Position

                let! lease =
                    store.ClaimNextTurn(tenant, session.Id, "owner", TimeSpan.FromMinutes 5.0, CancellationToken.None)

                let claim =
                    match lease with
                    | :? TurnLeaseHeld as held -> held.Claim
                    | :? TurnLeaseRenewed as renewed -> renewed.Claim
                    | _ -> failwith "Expected a held claim."

                Assert.True(
                    settlement.AdmitExecution(tenant, session.Id, entry.Position, claim, CancellationToken.None).Result
                )

                let okResult =
                    {
                        AssistantText = "persisted"
                        Status = TurnStatus.Completed
                        Iterations = 1
                        Usage = { InputTokens = 1L; OutputTokens = 1L }
                        Outcome = TurnFinished("persisted") :> TurnOutcome
                    }

                let request =
                    SessionSettlementRequest(
                        session.Id,
                        entry.Position,
                        claim,
                        Nullable(),
                        okResult,
                        Guid.NewGuid().ToString("N"),
                        null
                    )

                let! applied = settlement.SettleExecution(tenant, request, CancellationToken.None)
                Assert.Equal(SessionSettlementStatus.Applied, applied.Status)
            finally
                (database :> IDisposable).Dispose()

            let reopened = SqliteDatabase.Open(path)

            try
                let settlement = SqliteSessionSettlementStore(reopened) :> ISessionSettlementStore
                let! winnerRaw = settlement.TryReadCommitted(tenant, sessionId, position, CancellationToken.None)
                let winner = presentOutcome winnerRaw
                let winnerResult = presentResult winner.Result
                Assert.Equal("persisted", winnerResult.AssistantText)

                let! entryRaw = settlement.TryReadEntry(tenant, sessionId, position, CancellationToken.None)
                let entry = presentEntry entryRaw
                Assert.Equal(position, entry.Position)
            finally
                (reopened :> IDisposable).Dispose()

                try
                    System.IO.File.Delete path
                with _ ->
                    ()
        }
        :> Task

// ──────────────────────────────────────────────────────────────
// Facade receipts and lookup
// ──────────────────────────────────────────────────────────────

[<Fact>]
let ``Prompt returns a durable receipt with position and turn identity`` () : Task =
    task {
        use provider =
            (createServices (scripted [ ScriptStep.Text "hello back" ]) (sourced [])).BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSession client
                    let waiter = settleWaiter created.Id

                    let! receipt =
                        SessionClientOperations.PromptAsync(
                            client,
                            created.Id,
                            UserMessage.Text "hello",
                            DeliveryMode.Queue,
                            CancellationToken.None
                        )

                    Assert.False(isNull (box receipt))
                    Assert.Equal(created.Id, receipt.SessionId)
                    Assert.True(receipt.Position >= 1L)
                    Assert.Equal(OperationKind.Queue, receipt.Kind)
                    Assert.NotEqual(Unchecked.defaultof<TurnId>, receipt.OperationId)

                    let! pending =
                        SessionClientOperations.GetOperationResultAsync(client, receipt, CancellationToken.None)

                    Assert.True(
                        pending.Status = OperationStatus.Pending
                        || pending.Status = OperationStatus.Terminal
                    )

                    let! _ = awaitWhat waiter.Task "the turn to settle"

                    let! terminal =
                        SessionClientOperations.GetOperationResultAsync(client, receipt, CancellationToken.None)

                    Assert.Equal(OperationStatus.Terminal, terminal.Status)
                    let terminalResult = presentResult terminal.Result
                    Assert.Equal(TurnStatus.Completed, terminalResult.Status)
                    Assert.Equal("hello back", terminalResult.AssistantText)
                })
    }

[<Fact>]
let ``Concurrent prompts carry distinguishable receipts`` () : Task =
    task {
        use provider =
            (createServices
                (scripted
                    [
                        ScriptStep.Text "one"
                        ScriptStep.Text "two"
                    ])
                (sourced []))
                .BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSession client
                    let firstWaiter = settleWaiter created.Id

                    let! first =
                        SessionClientOperations.PromptAsync(
                            client,
                            created.Id,
                            UserMessage.Text "one",
                            DeliveryMode.Queue,
                            CancellationToken.None
                        )

                    let! second =
                        SessionClientOperations.PromptAsync(
                            client,
                            created.Id,
                            UserMessage.Text "two",
                            DeliveryMode.Queue,
                            CancellationToken.None
                        )

                    Assert.NotEqual(first.Position, second.Position)
                    Assert.NotEqual(first.OperationId, second.OperationId)
                    Assert.Equal(OperationKind.Queue, second.Kind)

                    let! _ = awaitWhat firstWaiter.Task "the first turn to settle"

                    let! firstResult =
                        SessionClientOperations.GetOperationResultAsync(client, first, CancellationToken.None)

                    let! secondResult =
                        SessionClientOperations.GetOperationResultAsync(client, second, CancellationToken.None)

                    Assert.Equal(OperationStatus.Terminal, firstResult.Status)
                    let firstWinner = presentResult firstResult.Result
                    Assert.False(String.IsNullOrEmpty(firstWinner.AssistantText))

                    Assert.True(
                        secondResult.Status = OperationStatus.Pending
                        || secondResult.Status = OperationStatus.Terminal
                    )
                })
    }

[<Fact>]
let ``Inject receipt promises no independent turn but stays observable`` () : Task =
    task {
        let entered = new ManualResetEventSlim(false)
        let release = TaskCompletionSource<string>()

        let blockingMethod =
            Func<Task<string>>(fun () ->
                entered.Set() |> ignore
                release.Task)

        let blockingTool =
            AIFunctionFactory.Create(
                blockingMethod,
                "blocker",
                Unchecked.defaultof<string>,
                Unchecked.defaultof<System.Text.Json.JsonSerializerOptions>
            )
            :> AITool

        let chat =
            scripted
                [
                    ScriptStep.ToolCall("c1", "blocker")
                    ScriptStep.Text "steered"
                ]

        use provider =
            (createServices chat (sourced [ blockingTool ])).BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSession client
                    let waiter = settleWaiter created.Id

                    let! _ =
                        awaitWhat
                            (SessionClientOperations.PromptAsync(
                                client,
                                created.Id,
                                UserMessage.Text "start",
                                DeliveryMode.Queue,
                                CancellationToken.None
                            ))
                            "the first prompt to land"

                    Assert.True(entered.Wait(waitBound))

                    let! injected =
                        SessionClientOperations.PromptAsync(
                            client,
                            created.Id,
                            UserMessage.Text "steer this",
                            DeliveryMode.Inject,
                            CancellationToken.None
                        )

                    Assert.Equal(OperationKind.Inject, injected.Kind)

                    release.TrySetResult("unblocked") |> ignore
                    let! _ = awaitWhat waiter.Task "the turn to settle"

                    let! observed =
                        SessionClientOperations.GetOperationResultAsync(client, injected, CancellationToken.None)

                    Assert.True(
                        observed.Status = OperationStatus.Pending
                        || observed.Status = OperationStatus.Terminal
                    )
                })
    }

[<Fact>]
let ``Interrupt receipt is unconfusable with the displaced turn`` () : Task =
    task {
        let entered = new ManualResetEventSlim(false)
        let release = TaskCompletionSource<string>()

        let blockingMethod =
            Func<Task<string>>(fun () ->
                entered.Set() |> ignore
                release.Task)

        let blockingTool =
            AIFunctionFactory.Create(
                blockingMethod,
                "blocker",
                Unchecked.defaultof<string>,
                Unchecked.defaultof<System.Text.Json.JsonSerializerOptions>
            )
            :> AITool

        let chat =
            scripted
                [
                    ScriptStep.ToolCall("c1", "blocker")
                    ScriptStep.Text "tail"
                    ScriptStep.Text "second"
                ]

        use provider =
            (createServices chat (sourced [ blockingTool ])).BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSession client
                    let waiter = settleWaiter created.Id

                    let! first =
                        awaitWhat
                            (SessionClientOperations.PromptAsync(
                                client,
                                created.Id,
                                UserMessage.Text "start",
                                DeliveryMode.Queue,
                                CancellationToken.None
                            ))
                            "the first prompt to land"

                    Assert.True(entered.Wait(waitBound))

                    let! interrupted =
                        SessionClientOperations.PromptAsync(
                            client,
                            created.Id,
                            UserMessage.Text "stop that",
                            DeliveryMode.Interrupt,
                            CancellationToken.None
                        )

                    Assert.Equal(OperationKind.Interrupt, interrupted.Kind)
                    Assert.NotEqual(first.Position, interrupted.Position)
                    Assert.NotEqual(first.OperationId, interrupted.OperationId)

                    // Queue the second waiter before releasing the blocker:
                    // the hub is FIFO, so the first waiter observes the
                    // pre-empted abort and the second observes the interrupt
                    // turn's own settlement.
                    let secondWaiter = settleWaiter created.Id
                    release.TrySetResult("unblocked") |> ignore
                    let! _ = awaitWhat waiter.Task "the pre-empted turn to settle"
                    let! _ = awaitWhat secondWaiter.Task "the interrupt turn to settle"

                    let! displaced =
                        SessionClientOperations.GetOperationResultAsync(client, first, CancellationToken.None)

                    let! winner =
                        SessionClientOperations.GetOperationResultAsync(client, interrupted, CancellationToken.None)

                    // Unconfusable: the displaced receipt never reads the
                    // interrupt's Completed winner. It reads Pending or its
                    // own Aborted terminal (#363 commits the pre-empted abort
                    // through its own path; the lookup serves only committed
                    // execution_settlements rows and never fabricates).
                    Assert.True(
                        displaced.Status = OperationStatus.Pending
                        || displaced.Status = OperationStatus.Terminal
                    )

                    if displaced.Status = OperationStatus.Terminal then
                        let displacedResult = presentResult displaced.Result
                        Assert.Equal(TurnStatus.Aborted, displacedResult.Status)

                    Assert.Equal(OperationStatus.Terminal, winner.Status)
                    let winnerResult = presentResult winner.Result
                    Assert.Equal(TurnStatus.Completed, winnerResult.Status)
                })
    }

[<Fact>]
let ``Reply receipt resumes without an independent turn`` () : Task =
    task {
        let chat =
            scripted
                [
                    ScriptStep.ToolCall("c1", "gated")
                    ScriptStep.Text "resumed"
                ]

        let invocations = ref []

        let gatedMethod =
            Func<string>(fun () ->
                invocations.Value <- invocations.Value @ [ "gated" ]
                "gated-result")

        let gated =
            AIFunctionFactory.Create(
                gatedMethod,
                "gated",
                Unchecked.defaultof<string>,
                Unchecked.defaultof<System.Text.Json.JsonSerializerOptions>
            )
            :> AITool

        let services = createServices chat (sourced [ gated ])
        services.AddSingleton<IPermissionPolicy>(AskReceiptPolicy("gated")) |> ignore
        use provider = services.BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSession client
                    let waiter = settleWaiter created.Id

                    let! _ =
                        awaitWhat
                            (SessionClientOperations.PromptAsync(
                                client,
                                created.Id,
                                UserMessage.Text "run it",
                                DeliveryMode.Queue,
                                CancellationToken.None
                            ))
                            "the prompt to land"

                    let mutable requestId = ""
                    let deadline = DateTimeOffset.UtcNow.Add(waitBound)

                    while String.IsNullOrEmpty(requestId) && DateTimeOffset.UtcNow < deadline do
                        let! events =
                            SessionClientOperations.ReadEventsAsync(
                                client,
                                created.Id,
                                0L,
                                100,
                                CancellationToken.None
                            )

                        for event in events do
                            match event with
                            | :? PermissionRequestedEvent as asked when not (isNull (box asked)) ->
                                requestId <- asked.RequestId
                            | _ -> ()

                        if String.IsNullOrEmpty(requestId) then
                            do! Task.Delay(50, CancellationToken.None)

                    Assert.False(String.IsNullOrEmpty(requestId))

                    let! receipt =
                        SessionClientOperations.ReplyAsync(
                            client,
                            created.Id,
                            PermissionDecision(requestId, PermissionDecisionKind.AllowOnce),
                            CancellationToken.None
                        )

                    Assert.Equal(OperationKind.Reply, receipt.Kind)

                    let! _ = awaitWhat waiter.Task "the resumed turn to settle"

                    let! observed =
                        SessionClientOperations.GetOperationResultAsync(client, receipt, CancellationToken.None)

                    Assert.True(
                        observed.Status = OperationStatus.Pending
                        || observed.Status = OperationStatus.Terminal
                    )
                })
    }

[<Fact>]
let ``Unknown receipt and missing session are distinguishable`` () : Task =
    task {
        use provider =
            (createServices (scripted [ ScriptStep.Text "hi" ]) (sourced [])).BuildServiceProvider()

        return!
            withClient provider (fun client ->
                task {
                    let! created = openSession client

                    let! _ =
                        awaitWhat
                            (SessionClientOperations.PromptAsync(
                                client,
                                created.Id,
                                UserMessage.Text "hi",
                                DeliveryMode.Queue,
                                CancellationToken.None
                            ))
                            "the prompt to land"

                    let fabricated =
                        AcceptedOperation(created.Id, 9999L, TurnId.New(), OperationKind.Queue, DateTimeOffset.UtcNow)

                    let! unknown =
                        SessionClientOperations.GetOperationResultAsync(client, fabricated, CancellationToken.None)

                    Assert.Equal(OperationStatus.Unknown, unknown.Status)
                    Assert.True(isNull (box unknown.Result))

                    let missingReceipt =
                        AcceptedOperation(
                            SessionId.New(),
                            1L,
                            TurnId.New(),
                            OperationKind.Queue,
                            DateTimeOffset.UtcNow
                        )

                    let! missingError =
                        Assert.ThrowsAsync<SessionNotFoundException>(fun () ->
                            SessionClientOperations.GetOperationResultAsync(
                                client,
                                missingReceipt,
                                CancellationToken.None
                            ))

                    Assert.False(isNull (box missingError))
                })
    }

[<Fact>]
let ``Second client over the same store observes the committed winner`` () : Task =
    task {
        let database = InMemoryDatabase()
        let chat = scripted [ ScriptStep.Text "shared" ]

        let buildProvider () =
            let services = ServiceCollection() :> IServiceCollection

            LegateServiceCollectionExtensions.AddLegate(
                services,
                ?configure =
                    Some(fun (builder: LegateBuilder) ->
                        builder.Llm.AddProvider(BuilderTests.StubLlmProvider()) |> ignore
                        builder.Storage.UseSessionStore(InMemorySessionStore(database)) |> ignore
                        builder.Workspace.UseRuntime(BuilderTests.StubWorkspaceRuntime()) |> ignore
                        builder.Tools.AddSource(sourced []) |> ignore)
            )
            |> ignore

            services.AddSingleton<ISessionEventStore>(InMemorySessionEventStore(database))
            |> ignore

            services.AddSingleton<IChatClient>(chat) |> ignore
            services.BuildServiceProvider()

        use firstProvider = buildProvider ()
        use secondProvider = buildProvider ()
        let firstService = actorServiceOf firstProvider
        let secondService = actorServiceOf secondProvider
        do! (firstService :> IHostedService).StartAsync(CancellationToken.None)
        do! (secondService :> IHostedService).StartAsync(CancellationToken.None)

        let! observedStatus =
            task {
                try
                    let first = firstProvider.GetRequiredService<SessionClient>()
                    let second = secondProvider.GetRequiredService<SessionClient>()
                    let! created = openSession first
                    let waiter = settleWaiter created.Id

                    let! receipt =
                        SessionClientOperations.PromptAsync(
                            first,
                            created.Id,
                            UserMessage.Text "hi",
                            DeliveryMode.Queue,
                            CancellationToken.None
                        )

                    let! _ = awaitWhat waiter.Task "the turn to settle"

                    let! observed =
                        SessionClientOperations.GetOperationResultAsync(second, receipt, CancellationToken.None)

                    Assert.Equal(OperationStatus.Terminal, observed.Status)
                    let observedResult = presentResult observed.Result
                    Assert.Equal("shared", observedResult.AssistantText)
                    return true
                with ex ->
                    do! stopQuietly firstService
                    do! stopQuietly secondService
                    let inner = ex
                    return raise inner
            }

        Assert.True(observedStatus)
        do! stopQuietly firstService
        do! stopQuietly secondService
    }
