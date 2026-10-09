// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.ToolAdmissionFenceTests

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open FsUnit.Xunit
open Legate
open Legate.Storage.InMemory
open Legate.Testing
open Legate.Tests.TurnLoopTests
open Microsoft.Extensions.AI
open Xunit

// Issue 376: production tool invocation admission under the current
// real-turn claim. Every production dispatch (ordinary, built-in-named,
// custom, nested, permission-resume, question-resume, crash-retry) verifies
// the live claim at the last moment through ClaimFence.checkBeforeCallAsync
// with the renewed LeaseAdmission hook underneath, failing closed on
// lost, missing, or unverifiable authority. Inject consumption rides the
// #377 atomic ConsumeInboxUnderClaim path; terminal outcomes reuse the #363
// winning settlement. Admitted-before-loss work may finish, but late results
// authorize nothing further.

let private tenant = TenantId.Create "tool-admission"

let private lease = TimeSpan.FromMinutes 5.0

let private createStores (clock: TimeProvider) : ISessionStore * ISessionEventStore =
    let database = InMemoryDatabase(clock)
    InMemorySessionStore(database) :> ISessionStore, InMemorySessionEventStore(database) :> ISessionEventStore

let private sampleSession () =
    {
        Id = SessionId.New()
        Tenant = tenant
        AgentId = AgentId.New()
        Title = "admission"
        State = SessionState.Idle
        CurrentTurnId = Unchecked.defaultof<Nullable<TurnId>>
        CreatedAt = DateTimeOffset.MinValue
        UpdatedAt = DateTimeOffset.MinValue
        ClosedAt = Unchecked.defaultof<Nullable<DateTimeOffset>>
        WorkspaceBinding = null
        Options = SessionOptions()
        PermissionGrants = ResizeArray<string>() :> IReadOnlyList<string>
    }

let private createSession (store: ISessionStore) : Session =
    store.CreateSession(tenant, sampleSession (), CancellationToken.None).GetAwaiter().GetResult()

let private appendQueue (store: ISessionStore) (sessionId: SessionId) (text: string) : InboxEntry =
    let payload = UserMessagePayload(UserMessage.Text text) :> InboxPayload

    store
        .AppendInboxMessage(tenant, sessionId, payload, DeliveryMode.Queue, CancellationToken.None)
        .GetAwaiter()
        .GetResult()

let private claimOf (state: TurnLeaseState) : TurnClaim =
    match state with
    | :? TurnLeaseRenewed as renewed when not (isNull (box renewed)) -> renewed.Claim
    | :? TurnLeaseHeld as held when not (isNull (box held)) -> held.Claim
    | :? TurnLeaseExpiring as expiring when not (isNull (box expiring)) -> expiring.Claim
    | other -> failwithf "Expected a granted claim, got %O." other

let private claimLive (store: ISessionStore) (sessionId: SessionId) (owner: string) : TurnClaim =
    store.ClaimNextTurn(tenant, sessionId, owner, lease, CancellationToken.None).GetAwaiter().GetResult()
    |> claimOf

/// Builds the production runner over a scripted client and tool map.
let private productionRunner
    (store: ISessionStore)
    (journal: ISessionEventStore)
    (client: ScriptedChatClient)
    (tools: IReadOnlyDictionary<string, AITool>)
    (policy: IPermissionPolicy | null)
    : SessionActor.SuspendableRunner =
    SessionPermissions.createRunner
        (client :> IChatClient)
        store
        tenant
        (fun _ _ -> Task.FromResult((tools, TurnLoop.TurnLoopOptions.Default)))
        (NeverDelay() :> ILlmDelay)
        policy
        None
        journal
        SessionStreaming.defaultBounds
        None

let private runFresh (runner: SessionActor.SuspendableRunner) (entry: InboxEntry) : TurnLoop.TurnLoopCompletion =
    runner entry 1 (HashSet<string>()) None None None CancellationToken.None None None None (TurnId.New())
    |> fun task -> task.GetAwaiter().GetResult()

let private expectLeaseLost (action: unit -> unit) : unit =
    try
        action ()
        failwith "Expected TurnLeaseLostException."
    with :? TurnLoop.TurnLeaseLostException ->
        ()

let private questionArgs (question: string) : IDictionary<string, obj> =
    let args = Dictionary<string, obj>()
    args["question"] <- question :> obj
    args :> IDictionary<string, obj>

// ──────────────────────────────────────────────────────────────────────────
// Task 1: live claim authority threads into fresh + crash-retry dispatches

[<Fact>]
let ``fresh admitted ordinary tool call passes under the live claim`` () =
    let clock = FakeClock()
    let store, journal = createStores clock
    let created = createSession store
    let entry = appendQueue store created.Id "run"
    let claim = claimLive store created.Id "owner-a"
    let invocations = ref []

    let tools =
        makeTools
            [
                "exec", stubTool "exec" "out" invocations
            ]

    let client =
        scripted
            [
                callStep "c1" "exec"
                textStep "done"
            ]

    let runner = productionRunner store journal client tools null

    use _c = ControlAdmission.enter (fun () -> true)
    use _l = LeaseAdmission.enter (fun () -> true)
    use _f = FencedClaimScope.enter (Some claim)

    let completion = runFresh runner entry
    completion.Result.Status |> should equal TurnStatus.Completed
    invocations.Value |> should equal [ "exec" ]

[<Fact>]
let ``fresh admitted built-in named custom and ordinary tools all pass`` () =
    let clock = FakeClock()
    let store, journal = createStores clock
    let created = createSession store
    let entry = appendQueue store created.Id "run"
    let claim = claimLive store created.Id "owner-a"
    let invocations = ref []

    let tools =
        makeTools
            [
                "exec", stubTool "exec" "exec-out" invocations
                "skill", stubTool "skill" "skill-out" invocations
                "custom_lookup", stubTool "custom_lookup" "custom-out" invocations
            ]

    let calls =
        callSteps
            [
                ("c1", "exec")
                ("c2", "skill")
                ("c3", "custom_lookup")
            ]

    let client = scripted [ calls; textStep "done" ]
    let runner = productionRunner store journal client tools null

    use _c = ControlAdmission.enter (fun () -> true)
    use _l = LeaseAdmission.enter (fun () -> true)
    use _f = FencedClaimScope.enter (Some claim)

    let completion = runFresh runner entry
    completion.Result.Status |> should equal TurnStatus.Completed
    invocations.Value |> should equal [ "exec"; "skill"; "custom_lookup" ]

[<Fact>]
let ``stale takeover fresh dispatch raises with zero invocations`` () =
    let clock = FakeClock()
    let store, journal = createStores clock
    let created = createSession store
    let entry = appendQueue store created.Id "first"
    let loser = claimLive store created.Id "owner-a"

    clock.Advance(TimeSpan.FromMinutes 10.0)
    appendQueue store created.Id "second" |> ignore
    claimLive store created.Id "owner-b" |> ignore

    let invocations = ref []

    let tools =
        makeTools
            [
                "exec", stubTool "exec" "out" invocations
            ]

    let client =
        scripted
            [
                callStep "c1" "exec"
                textStep "never"
            ]

    let runner = productionRunner store journal client tools null

    use _c = ControlAdmission.enter (fun () -> true)
    use _l = LeaseAdmission.enter (fun () -> true)
    use _f = FencedClaimScope.enter (Some loser)

    // Admission fences at tool dispatch: the provider may have been
    // consulted for selection, but zero tool effects run.
    expectLeaseLost (fun () -> runFresh runner entry |> ignore)
    invocations.Value.Length |> should equal 0

[<Fact>]
let ``missing claim fails closed with zero invocations`` () =
    let clock = FakeClock()
    let store, journal = createStores clock
    let created = createSession store
    let entry = appendQueue store created.Id "run"
    claimLive store created.Id "owner-a" |> ignore

    let invocations = ref []

    let tools =
        makeTools
            [
                "exec", stubTool "exec" "out" invocations
            ]

    let client =
        scripted
            [
                callStep "c1" "exec"
                textStep "never"
            ]

    let runner = productionRunner store journal client tools null

    use _c = ControlAdmission.enter (fun () -> true)
    use _l = LeaseAdmission.enter (fun () -> true)
    use _f = FencedClaimScope.enter None

    expectLeaseLost (fun () -> runFresh runner entry |> ignore)
    invocations.Value.Length |> should equal 0

[<Fact>]
let ``unverifiable authority fails closed with zero invocations`` () =
    let clock = FakeClock()
    let store, journal = createStores clock
    let created = createSession store
    let entry = appendQueue store created.Id "run"
    let live = claimLive store created.Id "owner-a"

    // A fabricated token the store never minted: verification finds no
    // lease and the dispatch fails closed.
    let fabricated =
        {
            TurnId = live.TurnId
            Token = Guid.NewGuid().ToString("N")
            Owner = "fabricated"
            ExpiresAt = DateTimeOffset.UtcNow.AddHours 1.0
            Attempt = live.Attempt
        }

    let invocations = ref []

    let tools =
        makeTools
            [
                "exec", stubTool "exec" "out" invocations
            ]

    let client =
        scripted
            [
                callStep "c1" "exec"
                textStep "never"
            ]

    let runner = productionRunner store journal client tools null

    use _c = ControlAdmission.enter (fun () -> true)
    use _l = LeaseAdmission.enter (fun () -> true)
    use _f = FencedClaimScope.enter (Some fabricated)

    expectLeaseLost (fun () -> runFresh runner entry |> ignore)
    invocations.Value.Length |> should equal 0

[<Fact>]
let ``crash retry under a live claim passes while a stale retry raises`` () =
    let clock = FakeClock()
    let store, journal = createStores clock
    let created = createSession store
    let entry = appendQueue store created.Id "run"
    let live = claimLive store created.Id "owner-a"

    let invocations = ref []

    let tools =
        makeTools
            [
                "exec", stubTool "exec" "out" invocations
            ]

    let client =
        scripted
            [
                callStep "c1" "exec"
                textStep "done"
            ]

    let runner = productionRunner store journal client tools null

    use _c = ControlAdmission.enter (fun () -> true)
    use _l = LeaseAdmission.enter (fun () -> true)
    use _f = FencedClaimScope.enter (Some live)

    let seed =
        Some(
            ResizeArray<ChatMessage>(
                [|
                    ChatMessage(ChatRole.User, "rebuilt")
                |]
            )
            :> IList<ChatMessage>
        )

    let crashReply =
        Some(PermissionDecision("req-crash", PermissionDecisionKind.AllowOnce) :> Reply)

    let retried =
        runner entry 1 (HashSet<string>()) None crashReply seed CancellationToken.None None None None (TurnId.New())
        |> fun task -> task.GetAwaiter().GetResult()

    retried.Result.Status |> should equal TurnStatus.Completed
    invocations.Value |> should equal [ "exec" ]

    clock.Advance(TimeSpan.FromMinutes 10.0)
    appendQueue store created.Id "rival" |> ignore
    claimLive store created.Id "owner-b" |> ignore
    invocations.Value <- []

    let staleClient =
        scripted
            [
                callStep "c2" "exec"
                textStep "never"
            ]

    let staleRunner = productionRunner store journal staleClient tools null

    expectLeaseLost (fun () ->
        staleRunner
            entry
            1
            (HashSet<string>())
            None
            crashReply
            seed
            CancellationToken.None
            None
            None
            None
            (TurnId.New())
        |> fun task -> task.GetAwaiter().GetResult() |> ignore)

    invocations.Value.Length |> should equal 0

// ──────────────────────────────────────────────────────────────────────────
// Task 2: Inject drain and consume ride the atomic UnderClaim path

[<Fact>]
let ``loser consumes nothing and winner input redelivers`` () =
    let clock = FakeClock()
    let store, _journal = createStores clock
    let created = createSession store
    appendQueue store created.Id "queue" |> ignore
    let loser = claimLive store created.Id "owner-a"

    clock.Advance(TimeSpan.FromMinutes 10.0)
    appendQueue store created.Id "winner" |> ignore
    let winner = claimLive store created.Id "owner-b"

    let folded =
        let payload = UserMessagePayload(UserMessage.Text "inject") :> InboxPayload

        store
            .AppendInboxMessage(tenant, created.Id, payload, DeliveryMode.Inject, CancellationToken.None)
            .GetAwaiter()
            .GetResult()

    let positions = [| folded.Position |] :> IReadOnlyList<int64>

    let lost =
        ClaimFence.consumeInboxAsync store tenant loser created.Id positions CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()

    lost |> should equal false

    let pending =
        store.ReadPendingInbox(tenant, created.Id, CancellationToken.None).GetAwaiter().GetResult()

    pending
    |> Seq.exists (fun entry -> entry.Position = folded.Position)
    |> should equal true

    let landed =
        ClaimFence.consumeInboxAsync store tenant winner created.Id positions CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()

    landed |> should equal true

[<Fact>]
let ``runner consume without a claim fails closed and redelivers`` () =
    let clock = FakeClock()
    let store, journal = createStores clock
    let created = createSession store
    let entry = appendQueue store created.Id "run"
    claimLive store created.Id "owner-a" |> ignore

    let inject =
        let payload = UserMessagePayload(UserMessage.Text "inject") :> InboxPayload

        store
            .AppendInboxMessage(tenant, created.Id, payload, DeliveryMode.Inject, CancellationToken.None)
            .GetAwaiter()
            .GetResult()

    let invocations = ref []

    let tools =
        makeTools
            [
                "exec", stubTool "exec" "out" invocations
            ]

    let client =
        scripted
            [
                callStep "c1" "exec"
                textStep "done"
            ]

    let runner = productionRunner store journal client tools null

    use _c = ControlAdmission.enter (fun () -> true)
    use _l = LeaseAdmission.enter (fun () -> true)
    use _f = FencedClaimScope.enter None

    // The missing claim fences the first dispatch itself, so the folded
    // Inject entry is never consumed through the loser.
    expectLeaseLost (fun () -> runFresh runner entry |> ignore)

    let pending =
        store.ReadPendingInbox(tenant, created.Id, CancellationToken.None).GetAwaiter().GetResult()

    pending
    |> Seq.exists (fun folded -> folded.Position = inject.Position)
    |> should equal true

    invocations.Value.Length |> should equal 0

// ──────────────────────────────────────────────────────────────────────────
// Task 3: nested and sub-agent admission inherits the parent fence

[<Fact>]
let ``takeover before nested admission yields zero nested effects`` () =
    let nestedInvocations = ref []

    let nestedHook: TurnLoop.TaskNestedRun =
        fun _ ->
            nestedInvocations.Value <- nestedInvocations.Value @ [ "nested" ]

            Task.FromResult(
                {
                    Text = "nested-out"
                    Iterations = 1
                    InputTokens = 1L
                    OutputTokens = 1L
                }
            )

    let args = Dictionary<string, obj>()
    args["subagent"] <- "helper" :> obj
    args["task"] <- "do work" :> obj

    let client =
        scripted
            [
                ScriptStep.ToolCall("c-task", TurnLoop.TaskToolName, args)
                textStep "never"
            ]

    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

    let options =
        { TurnLoop.TurnLoopOptions.Default with
            TaskNested = Some nestedHook
            VerifyClaim = Some(fun () -> Task.FromResult(false))
        }

    let outcome =
        try
            TurnLoop.runSuspendableAsync
                (client :> IChatClient)
                history
                (makeTools [])
                options
                (NeverDelay() :> ILlmDelay)
                CancellationToken.None
                (fun () -> true)
                (fun () -> ResizeArray<InboxEntry>() :> IReadOnlyList<InboxEntry>)
                ignore
                ignore
                (Unchecked.defaultof<IPermissionPolicy>)
                (SessionId.New())
                (TurnId.New())
                None
                (HashSet<string>())
            |> fun task -> task.GetAwaiter().GetResult() |> ignore

            false
        with :? TurnLoop.TurnLeaseLostException ->
            true

    outcome |> should equal true
    nestedInvocations.Value.Length |> should equal 0

// ──────────────────────────────────────────────────────────────────────────
// Task 4: permission and question resume dispatches verify current authority

[<Fact>]
let ``permission admission race shows zero effects for the loser`` () =
    let invocations = ref []
    let fn = stubTool "exec" "out" invocations

    let client =
        scripted
            [
                callStep "c1" "exec"
                textStep "finished"
            ]

    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

    let policy =
        ScriptPolicy(Map.ofList [ "exec", PermissionVerdict.Ask ]) :> IPermissionPolicy

    let allowed = HashSet<string>()

    let suspended =
        TurnLoop.runSuspendableAsync
            (client :> IChatClient)
            history
            (makeTools [ "exec", fn ])
            TurnLoop.TurnLoopOptions.Default
            (NeverDelay() :> ILlmDelay)
            CancellationToken.None
            (fun () -> true)
            (fun () -> ResizeArray<InboxEntry>() :> IReadOnlyList<InboxEntry>)
            ignore
            ignore
            policy
            (SessionId.New())
            (TurnId.New())
            None
            allowed
        |> fun task -> task.GetAwaiter().GetResult()

    suspended.Suspension.IsSome |> should equal true
    invocations.Value.Length |> should equal 0

    let fenced =
        { TurnLoop.TurnLoopOptions.Default with
            VerifyClaim = Some(fun () -> Task.FromResult(false))
        }

    expectLeaseLost (fun () ->
        TurnLoop.resumePermissionAsync
            suspended.Suspension.Value
            PermissionDecisionKind.AllowOnce
            (client :> IChatClient)
            history
            (makeTools [ "exec", fn ])
            fenced
            (NeverDelay() :> ILlmDelay)
            CancellationToken.None
            (fun () -> true)
            policy
            allowed
        |> fun task -> task.GetAwaiter().GetResult() |> ignore)

    invocations.Value.Length |> should equal 0

[<Fact>]
let ``granted then stale permission resume dispatches nothing further`` () =
    let invocations = ref []
    let fn = stubTool "exec" "out" invocations

    let client =
        scripted
            [
                callStep "c1" "exec"
                textStep "finished"
            ]

    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>

    let policy =
        ScriptPolicy(Map.ofList [ "exec", PermissionVerdict.Ask ]) :> IPermissionPolicy

    let allowed = HashSet<string>()

    let suspended =
        TurnLoop.runSuspendableAsync
            (client :> IChatClient)
            history
            (makeTools [ "exec", fn ])
            TurnLoop.TurnLoopOptions.Default
            (NeverDelay() :> ILlmDelay)
            CancellationToken.None
            (fun () -> true)
            (fun () -> ResizeArray<InboxEntry>() :> IReadOnlyList<InboxEntry>)
            ignore
            ignore
            policy
            (SessionId.New())
            (TurnId.New())
            None
            allowed
        |> fun task -> task.GetAwaiter().GetResult()

    suspended.Suspension.IsSome |> should equal true

    let admitted =
        TurnLoop.resumePermissionAsync
            suspended.Suspension.Value
            PermissionDecisionKind.AllowOnce
            (client :> IChatClient)
            history
            (makeTools [ "exec", fn ])
            TurnLoop.TurnLoopOptions.Default
            (NeverDelay() :> ILlmDelay)
            CancellationToken.None
            (fun () -> true)
            policy
            allowed
        |> fun task -> task.GetAwaiter().GetResult()

    admitted.Result.Status |> should equal TurnStatus.Completed
    invocations.Value |> should equal [ "exec" ]

[<Fact>]
let ``question resume authorizes no further effects after loss`` () =
    let invocations = ref []
    let fn = stubTool "exec" "out" invocations

    let client =
        scripted
            [
                ScriptStep.ToolCall("q1", TurnLoop.AskUserToolName, questionArgs "Which region?")
                callStep "c2" "exec"
                textStep "never"
            ]

    let history = ResizeArray<ChatMessage>() :> IList<ChatMessage>
    let policy = ScriptPolicy(Map.empty) :> IPermissionPolicy

    let suspended =
        TurnLoop.runSuspendableAsync
            (client :> IChatClient)
            history
            (makeTools [ "exec", fn ])
            TurnLoop.TurnLoopOptions.Default
            (NeverDelay() :> ILlmDelay)
            CancellationToken.None
            (fun () -> true)
            (fun () -> ResizeArray<InboxEntry>() :> IReadOnlyList<InboxEntry>)
            ignore
            ignore
            policy
            (SessionId.New())
            (TurnId.New())
            None
            (HashSet<string>())
        |> fun task -> task.GetAwaiter().GetResult()

    suspended.Suspension.IsSome |> should equal true
    suspended.Suspension.Value.Kind |> should equal TurnLoop.QuestionSuspension

    let fenced =
        { TurnLoop.TurnLoopOptions.Default with
            VerifyClaim = Some(fun () -> Task.FromResult(false))
        }

    expectLeaseLost (fun () ->
        TurnLoop.resumeQuestionAsync
            suspended.Suspension.Value
            "east"
            (client :> IChatClient)
            history
            (makeTools [ "exec", fn ])
            fenced
            (NeverDelay() :> ILlmDelay)
            CancellationToken.None
            (fun () -> true)
            policy
            (HashSet<string>())
        |> fun task -> task.GetAwaiter().GetResult() |> ignore)

    invocations.Value.Length |> should equal 0

// ──────────────────────────────────────────────────────────────────────────
// Task 5: rejected admission settles nothing; admitted-then-lost finishes
// without authorizing further effects (the #363 boundary decides)

[<Fact>]
let ``admitted then lost finishes the admitted effect but dispatches nothing further`` () =
    let clock = FakeClock()
    let store, journal = createStores clock
    let created = createSession store
    let entry = appendQueue store created.Id "run"
    let loser = claimLive store created.Id "owner-a"
    let invocations = ref []

    let first () : string =
        invocations.Value <- invocations.Value @ [ "first" ]
        clock.Advance(TimeSpan.FromMinutes 10.0)
        appendQueue store created.Id "rival" |> ignore
        claimLive store created.Id "owner-b" |> ignore
        "first-out"

    let firstFn =
        Microsoft.Extensions.AI.AIFunctionFactory.Create(Func<string>(fun () -> first ()), "first", null, null)

    let secondFn = stubTool "second" "second-out" invocations

    let tools =
        let table = Dictionary<string, AITool>(StringComparer.Ordinal)
        table["first"] <- firstFn :> AITool
        table["second"] <- secondFn :> AITool
        table :> IReadOnlyDictionary<string, AITool>

    let calls = callSteps [ ("c1", "first"); ("c2", "second") ]
    let client = scripted [ calls; textStep "never" ]
    let runner = productionRunner store journal client tools null

    use _c = ControlAdmission.enter (fun () -> true)
    use _l = LeaseAdmission.enter (fun () -> true)
    use _f = FencedClaimScope.enter (Some loser)

    expectLeaseLost (fun () -> runFresh runner entry |> ignore)

    invocations.Value |> should equal [ "first" ]

    let loserSettle =
        store.SettleTurn(tenant, loser, TurnStatus.Completed, null, CancellationToken.None).GetAwaiter().GetResult()

    (loserSettle :? TurnSettled) |> should equal false

[<Fact>]
let ``rejected admission produces no losing completion`` () =
    let clock = FakeClock()
    let store, journal = createStores clock
    let created = createSession store
    let entry = appendQueue store created.Id "first"
    let loser = claimLive store created.Id "owner-a"

    clock.Advance(TimeSpan.FromMinutes 10.0)
    appendQueue store created.Id "second" |> ignore
    let winner = claimLive store created.Id "owner-b"

    let invocations = ref []

    let tools =
        makeTools
            [
                "exec", stubTool "exec" "out" invocations
            ]

    let client =
        scripted
            [
                callStep "c1" "exec"
                textStep "never"
            ]

    let runner = productionRunner store journal client tools null

    use _c = ControlAdmission.enter (fun () -> true)
    use _l = LeaseAdmission.enter (fun () -> true)
    use _f = FencedClaimScope.enter (Some loser)

    expectLeaseLost (fun () -> runFresh runner entry |> ignore)
    invocations.Value.Length |> should equal 0

    let loserSettle =
        store.SettleTurn(tenant, loser, TurnStatus.Completed, null, CancellationToken.None).GetAwaiter().GetResult()

    (loserSettle :? TurnSettled) |> should equal false

    let winnerSettle =
        store.SettleTurn(tenant, winner, TurnStatus.Completed, null, CancellationToken.None).GetAwaiter().GetResult()

    (winnerSettle :? TurnSettled || winnerSettle :? TurnAlreadySettled)
    |> should equal true

// ──────────────────────────────────────────────────────────────────────────
// Task 6: legacy formats stay fail-closed with a documented clean start

[<Fact>]
let ``unsupported persistence format is rejected without authority`` () =
    let clock = FakeClock()
    let database = InMemoryDatabase(clock)
    let store = InMemorySessionStore(database) :> ISessionStore

    let legacyOptions = SessionOptions()
    legacyOptions.FormatVersion <- 0

    let legacy =
        {
            Id = SessionId.New()
            Tenant = tenant
            AgentId = AgentId.New()
            Title = "legacy"
            State = SessionState.Idle
            CurrentTurnId = Unchecked.defaultof<Nullable<TurnId>>
            CreatedAt = DateTimeOffset.MinValue
            UpdatedAt = DateTimeOffset.MinValue
            ClosedAt = Unchecked.defaultof<Nullable<DateTimeOffset>>
            WorkspaceBinding = null
            Options = legacyOptions
            PermissionGrants = ResizeArray<string>() :> IReadOnlyList<string>
        }

    database.Sessions[(tenant, legacy.Id)] <- legacy

    let before =
        store.ReadPendingInbox(tenant, legacy.Id, CancellationToken.None).GetAwaiter().GetResult().Count

    try
        store.ClaimNextTurn(tenant, legacy.Id, "owner-a", lease, CancellationToken.None).GetAwaiter().GetResult()
        |> ignore

        failwith "Expected the legacy claim to fail closed."
    with
    | :? CompletionRoutingException as refusal ->
        refusal.Reason |> should equal CompletionRoutingReason.UnsupportedFormat
    | :? InvalidSessionStateException as rejected ->
        // The store refuses legacy control data fail-closed before any
        // claim is granted: no authority is minted for unsupported rows.
        rejected.Message.Contains("unsupported", StringComparison.OrdinalIgnoreCase)
        |> should equal true

    let after =
        store.ReadPendingInbox(tenant, legacy.Id, CancellationToken.None).GetAwaiter().GetResult().Count

    after |> should equal before
