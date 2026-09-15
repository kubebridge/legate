// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.SessionActorTests

open System
open System.Collections.Generic
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open Akka.Actor
open Akka.FSharp
open FsUnit.Xunit
open Legate
open Legate.Storage.InMemory
open Legate.Testing
open Microsoft.Extensions.AI
open Microsoft.Extensions.Options
open Microsoft.Extensions.Hosting
open Xunit

// Session state machine and durable inbox (issue 32): an Akka.FSharp child
// owning Idle -> Running -> (Idle | WaitingForInput | Closed) over the
// in-memory ISessionStore, driving TurnLoop.runAsync for Queue only. Invalid
// transitions throw InvalidSessionStateException at the client boundary,
// never inside the actor.

// ──────────────────────────────────────────────────────────────────────────
// Helpers

let tenant = TenantId.Create "acme"

/// An empty clock-free store: every test owns its database.
let private createStore () : ISessionStore =
    InMemorySessionStore(InMemoryDatabase()) :> ISessionStore

/// A minimal Idle session row, mirroring the conformance suite sample.
let private sampleSession () =
    {
        Id = SessionId.New()
        Tenant = tenant
        AgentId = AgentId.New()
        Title = "checkout"
        State = SessionState.Idle
        CurrentTurnId = Unchecked.defaultof<Nullable<TurnId>>
        CreatedAt = DateTimeOffset.MinValue
        UpdatedAt = DateTimeOffset.MinValue
        ClosedAt = Unchecked.defaultof<Nullable<DateTimeOffset>>
        WorkspaceBinding = null
        Options = SessionOptions()
        PermissionGrants = ResizeArray<string>() :> IReadOnlyList<string>
    }

/// Creates the session row and returns it.
let private createSession (store: ISessionStore) : Session =
    store.CreateSession(tenant, sampleSession (), CancellationToken.None).GetAwaiter().GetResult()

/// Appends one Queue user message straight to the store (pre-existing
/// inbox, or crash-orphaned work).
let private appendStored (store: ISessionStore) (sessionId: SessionId) (text: string) : InboxEntry =
    let payload = UserMessagePayload(UserMessage.Text(text)) :> InboxPayload

    store
        .AppendInboxMessage(tenant, sessionId, payload, DeliveryMode.Queue, CancellationToken.None)
        .GetAwaiter()
        .GetResult()

/// Starts a local actor system for one test.
let private createSystem () : ActorSystem = LocalActorSystem.createSystem ()

/// Terminates a test system, bounding the drain.
let private stopSystem (system: ActorSystem) : unit =
    system.Terminate() |> ignore
    system.WhenTerminated.Wait(TimeSpan.FromSeconds 10.0) |> ignore

/// Spawns a session actor with a fresh name on a test system.
let private spawnSession
    (system: ActorSystem)
    (store: ISessionStore)
    (sessionId: SessionId)
    (runTurn: InboxEntry -> CancellationToken -> Task<TurnResult>)
    : IActorRef =
    let props: SessionActorProps =
        {
            Store = store
            Tenant = tenant
            SessionId = sessionId
            RunTurn = runTurn
            OnTurnSettled = None
            OnInjectJournaled = None
            Logger = null
            Compact = None
        }

    spawn system $"test-{Guid.NewGuid():N}" (SessionActor.behavior props)

/// Spawns a session actor observing each settled result into the probe: the
/// carried result, or the abort-mapped Aborted result when a stop won.
let private spawnSessionWithProbe
    (system: ActorSystem)
    (store: ISessionStore)
    (sessionId: SessionId)
    (runTurn: InboxEntry -> CancellationToken -> Task<TurnResult>)
    (probe: TurnResult -> unit)
    : IActorRef =
    let props: SessionActorProps =
        {
            Store = store
            Tenant = tenant
            SessionId = sessionId
            RunTurn = runTurn
            OnTurnSettled = Some probe
            OnInjectJournaled = None
            Logger = null
            Compact = None
        }

    spawn system $"test-{Guid.NewGuid():N}" (SessionActor.behavior props)

/// Prompts through the client boundary and blocks for the ack.
let private prompt (store: ISessionStore) (sessionId: SessionId) (session: IActorRef) (text: string) : InboxEntry =
    let call =
        SessionActor.promptQueueAsync store tenant sessionId session (UserMessage.Text text) CancellationToken.None

    call.GetAwaiter().GetResult()

/// Closes through the client boundary and blocks for the stored session.
let private close (store: ISessionStore) (sessionId: SessionId) (session: IActorRef) : Session =
    let call =
        SessionActor.closeAsync store tenant sessionId session CancellationToken.None

    call.GetAwaiter().GetResult()

/// Aborts through the client boundary and blocks for the snapshot.
let private abort
    (store: ISessionStore)
    (sessionId: SessionId)
    (session: IActorRef)
    (cause: StopCause)
    (reason: string)
    : SessionSnapshot =
    let call =
        SessionActor.abortAsync store tenant sessionId session cause reason CancellationToken.None

    call.GetAwaiter().GetResult()

/// Reads the actor snapshot, blocking.
let private snapshotOf (session: IActorRef) : SessionSnapshot =
    let call = SessionActor.getSnapshotAsync session CancellationToken.None
    call.GetAwaiter().GetResult()

/// Reads the store's pending inbox, blocking.
let private pendingOf (store: ISessionStore) (sessionId: SessionId) : IReadOnlyList<InboxEntry> =
    store.ReadPendingInbox(tenant, sessionId, CancellationToken.None).GetAwaiter().GetResult()

/// Reads the stored session row, blocking. Tests only read rows they
/// created, so a missing row is a test bug.
let private storedOf (store: ISessionStore) (sessionId: SessionId) : Session =
    let found =
        store.GetSession(tenant, sessionId, CancellationToken.None).GetAwaiter().GetResult()

    match found with
    | null -> failwith "Expected the session row to exist."
    | session -> session

/// Polls a condition until it holds or the timeout lapses. Sleeps are the
/// poll cadence only: every assertion below is eventual, never timing.
let private waitFor (timeout: TimeSpan) (condition: unit -> bool) : bool =
    let deadline = DateTime.UtcNow + timeout
    let mutable holds = condition ()

    while not holds && DateTime.UtcNow < deadline do
        Thread.Sleep(25)
        holds <- condition ()

    holds

/// The text carried by a Queue inbox entry.
let private entryText (entry: InboxEntry) : string =
    match entry.Payload with
    | :? UserMessagePayload as user ->
        user.Message.Parts
        |> Seq.choose (fun part ->
            match part with
            | :? TextContent as text when not (isNull (box text)) -> Some text.Text
            | _ -> None)
        |> String.concat ""
    | _ -> failwith "Expected a user message payload."

/// A completed TurnResult carrying assistant text.
let private completed text =
    {
        AssistantText = text
        Status = TurnStatus.Completed
        Iterations = 1
        Usage = { InputTokens = 0L; OutputTokens = 0L }
        Outcome = null
    }

// ──────────────────────────────────────────────────────────────────────────
// Doubles

/// Scripted turn runner: answers every turn immediately and records entries
/// in execution order.
type ScriptedRunner(texts: string list) =
    let mutable calls = 0
    let entries = ResizeArray<InboxEntry>()

    /// How many turns ran.
    member _.Calls = calls

    /// The entries turns executed, in execution order.
    member _.Entries = entries :> IReadOnlyList<InboxEntry>

    /// Runs one turn for an entry.
    member _.Run(entry: InboxEntry, _cancellationToken: CancellationToken) : Task<TurnResult> =
        calls <- calls + 1
        entries.Add(entry)
        let index = min (calls - 1) (texts.Length - 1)
        Task.FromResult(completed texts[index])

    /// The runner as the actor's delegate.
    member this.Func: (InboxEntry -> CancellationToken -> Task<TurnResult>) =
        fun entry cancellationToken -> this.Run(entry, cancellationToken)

/// Gated turn runner: the first turn waits for Release, later turns
/// complete at once. Signals Started when the first turn begins.
type GatedRunner(texts: string list) =
    let started = new TaskCompletionSource<unit>()
    let release = new TaskCompletionSource<unit>()
    let mutable calls = 0
    let entries = ResizeArray<InboxEntry>()

    /// Fires when the first turn begins.
    member _.Started = started.Task

    /// Releases the first turn to complete.
    member _.Release() = release.TrySetResult() |> ignore

    /// How many turns ran.
    member _.Calls = calls

    /// The entries turns executed, in execution order.
    member _.Entries = entries :> IReadOnlyList<InboxEntry>

    /// Runs one turn for an entry.
    member _.Run(entry: InboxEntry, _cancellationToken: CancellationToken) : Task<TurnResult> =
        task {
            calls <- calls + 1
            entries.Add(entry)

            if calls = 1 then
                started.TrySetResult() |> ignore
                do! release.Task

            let index = min (calls - 1) (texts.Length - 1)
            return completed texts[index]
        }

    /// The runner as the actor's delegate.
    member this.Func: (InboxEntry -> CancellationToken -> Task<TurnResult>) =
        fun entry cancellationToken -> this.Run(entry, cancellationToken)

/// IChatClient double that blocks until its token fires, so the default
/// TurnLoop runner proves Close aborts the turn through cancellation.
type BlockingChatClient(ended: TaskCompletionSource<unit>) =

    interface IChatClient with
        member _.GetResponseAsync(_, _, cancellationToken) =
            task {
                try
                    do! Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken)
                    return Unchecked.defaultof<ChatResponse>
                finally
                    ended.TrySetResult() |> ignore
            }

        member _.GetStreamingResponseAsync(_, _, _) = raise (NotImplementedException())

        member _.GetService(_, _) = null
        member _.Dispose() = ()

/// No tools for the default-runner path.
let private noTools () : IReadOnlyDictionary<string, AITool> =
    Dictionary<string, AITool>() :> IReadOnlyDictionary<string, AITool>

// ──────────────────────────────────────────────────────────────────────────
// Queue while Idle starts a turn

[<Fact>]
let ``Queue while Idle starts a turn and returns to Idle`` () =
    use system = createSystem ()
    let store = createStore ()
    let created = createSession store
    let runner = ScriptedRunner([ "hello" ])
    let session = spawnSession system store created.Id runner.Func

    try
        let entry = prompt store created.Id session "hello"

        let drained =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                runner.Calls = 1 && (snapshotOf session).State = SessionState.Idle)

        drained |> should equal true
        runner.Calls |> should equal 1
        runner.Entries[0].Position |> should equal entry.Position
        entryText runner.Entries[0] |> should equal "hello"
        (storedOf store created.Id).State |> should equal SessionState.Idle
        (pendingOf store created.Id).Count |> should equal 0
    finally
        stopSystem system

// ──────────────────────────────────────────────────────────────────────────
// Queue while Running waits then drains on settle

[<Fact>]
let ``Queue while Running waits in the inbox then drains on settle`` () =
    use system = createSystem ()
    let store = createStore ()
    let created = createSession store
    let runner = GatedRunner([ "first"; "second" ])
    let session = spawnSession system store created.Id runner.Func

    try
        prompt store created.Id session "first" |> ignore

        let running =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () -> (snapshotOf session).State = SessionState.Running)

        running |> should equal true

        prompt store created.Id session "second" |> ignore

        (snapshotOf session).State |> should equal SessionState.Running
        // Two pending: the Running turn's entry is consumed only at
        // settle, plus the queued second entry.
        (pendingOf store created.Id).Count |> should equal 2

        runner.Release()

        let drained =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                runner.Calls = 2 && (snapshotOf session).State = SessionState.Idle)

        drained |> should equal true

        runner.Entries
        |> Seq.map entryText
        |> List.ofSeq
        |> should equal [ "first"; "second" ]

        (pendingOf store created.Id).Count |> should equal 0
    finally
        stopSystem system

// ──────────────────────────────────────────────────────────────────────────
// Close on Running aborts the turn first, Close is idempotent

[<Fact>]
let ``Close on Running aborts the turn first through cancellation`` () =
    use system = createSystem ()
    let store = createStore ()
    let created = createSession store
    let ended = new TaskCompletionSource<unit>()
    let client = new BlockingChatClient(ended) :> IChatClient

    let inner =
        SessionActor.createTurnRunner
            client
            (noTools ())
            TurnLoop.TurnLoopOptions.Default
            (TurnLoopTests.NeverDelay() :> ILlmDelay)

    let mutable observed = 0

    let runner entry cancellationToken =
        task {
            try
                return! inner entry cancellationToken
            finally
                Interlocked.Increment(&observed) |> ignore
        }

    let session = spawnSession system store created.Id runner

    try
        prompt store created.Id session "doomed" |> ignore

        let running =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () -> (snapshotOf session).State = SessionState.Running)

        running |> should equal true

        let closed = close store created.Id session
        closed.State |> should equal SessionState.Closed
        (snapshotOf session).State |> should equal SessionState.Closed

        let aborted = ended.Task.Wait(TimeSpan.FromSeconds 10.0)
        aborted |> should equal true

        // The abort observation propagates from the client up through the
        // loop after the client fires: poll, do not assert immediately.
        let settled = waitFor (TimeSpan.FromSeconds 10.0) (fun () -> observed = 1)
        settled |> should equal true
    finally
        stopSystem system

[<Fact>]
let ``Close is idempotent`` () =
    use system = createSystem ()
    let store = createStore ()
    let created = createSession store
    let runner = ScriptedRunner([ "done" ])
    let session = spawnSession system store created.Id runner.Func

    try
        let first = close store created.Id session
        first.State |> should equal SessionState.Closed

        let second = close store created.Id session
        second.State |> should equal SessionState.Closed
    finally
        stopSystem system

// ──────────────────────────────────────────────────────────────────────────
// Invalid transitions throw at the boundary

[<Fact>]
let ``Prompt on Closed throws InvalidSessionStateException at the boundary`` () =
    use system = createSystem ()
    let store = createStore ()
    let created = createSession store
    let runner = ScriptedRunner([ "done" ])
    let session = spawnSession system store created.Id runner.Func

    try
        close store created.Id session |> ignore

        let ex =
            Assert.Throws<InvalidSessionStateException>(fun () -> prompt store created.Id session "late" |> ignore)

        ex.SessionId |> should equal created.Id
        ex.CurrentState |> should equal (SessionState.Closed.ToString())
        runner.Calls |> should equal 0
    finally
        stopSystem system

[<Fact>]
let ``Prompt on an unknown session throws SessionNotFoundException`` () =
    use system = createSystem ()
    let store = createStore ()
    let created = createSession store
    let runner = ScriptedRunner([ "done" ])
    let session = spawnSession system store created.Id runner.Func
    let missing = SessionId.New()

    try
        Assert.Throws<SessionNotFoundException>(fun () -> prompt store missing session "ghost" |> ignore)
        |> ignore
    finally
        stopSystem system

[<Fact>]
let ``Close on an unknown session throws SessionNotFoundException`` () =
    use system = createSystem ()
    let store = createStore ()
    let created = createSession store
    let runner = ScriptedRunner([ "done" ])
    let session = spawnSession system store created.Id runner.Func
    let missing = SessionId.New()

    try
        Assert.Throws<SessionNotFoundException>(fun () -> close store missing session |> ignore)
        |> ignore
    finally
        stopSystem system

// ──────────────────────────────────────────────────────────────────────────
// WaitingForInput is modelled only: prompts queue, no turn starts

[<Fact>]
let ``Prompt while WaitingForInput queues without starting a turn`` () =
    use system = createSystem ()
    let store = createStore ()
    let created = createSession store

    store
        .UpdateSessionState(tenant, created.Id, SessionState.WaitingForInput, CancellationToken.None)
        .GetAwaiter()
        .GetResult()
    |> ignore

    let runner = ScriptedRunner([ "waiting" ])
    let session = spawnSession system store created.Id runner.Func

    try
        (snapshotOf session).State |> should equal SessionState.WaitingForInput

        prompt store created.Id session "while waiting" |> ignore

        (snapshotOf session).State |> should equal SessionState.WaitingForInput
        (pendingOf store created.Id).Count |> should equal 1

        let started =
            Task.WhenAny(Task.Delay(TimeSpan.FromMilliseconds 500.0))
            |> fun delay -> delay.GetAwaiter().GetResult()

        started |> ignore
        runner.Calls |> should equal 0
    finally
        stopSystem system

// ──────────────────────────────────────────────────────────────────────────
// Restart rebuilds state from the store

[<Fact>]
let ``Restart rebuilds Idle with pending inbox intact`` () =
    use system = createSystem ()
    let store = createStore ()
    let created = createSession store
    ignore (appendStored store created.Id "before restart")

    let runner = ScriptedRunner([ "first"; "second" ])
    let session = spawnSession system store created.Id runner.Func

    try
        let snapshot = snapshotOf session
        snapshot.State |> should equal SessionState.Idle
        snapshot.PendingCount |> should equal 1
        snapshot.RunningPosition |> should equal None
    finally
        stopSystem system

[<Fact>]
let ``Restart releases an orphaned Running state to Idle`` () =
    use system = createSystem ()
    let store = createStore ()
    let created = createSession store

    store.UpdateSessionState(tenant, created.Id, SessionState.Running, CancellationToken.None).GetAwaiter().GetResult()
    |> ignore

    ignore (appendStored store created.Id "orphaned")

    let runner = ScriptedRunner([ "recovered" ])
    let session = spawnSession system store created.Id runner.Func

    try
        let snapshot = snapshotOf session
        snapshot.State |> should equal SessionState.Idle
        snapshot.PendingCount |> should equal 1
        (storedOf store created.Id).State |> should equal SessionState.Idle
    finally
        stopSystem system

[<Fact>]
let ``Restart preserves Closed`` () =
    use system = createSystem ()
    let store = createStore ()
    let created = createSession store

    store.CloseSession(tenant, created.Id, CancellationToken.None).GetAwaiter().GetResult()
    |> ignore

    let runner = ScriptedRunner([ "never" ])
    let session = spawnSession system store created.Id runner.Func

    try
        (snapshotOf session).State |> should equal SessionState.Closed

        Assert.Throws<InvalidSessionStateException>(fun () -> prompt store created.Id session "late" |> ignore)
        |> ignore

        runner.Calls |> should equal 0
    finally
        stopSystem system

[<Fact>]
let ``After restart the next prompt drains the oldest pending entry first`` () =
    use system = createSystem ()
    let store = createStore ()
    let created = createSession store
    ignore (appendStored store created.Id "first")

    let runner = ScriptedRunner([ "first"; "second" ])
    let session = spawnSession system store created.Id runner.Func

    try
        prompt store created.Id session "second" |> ignore

        let drained =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                runner.Calls = 2 && (snapshotOf session).State = SessionState.Idle)

        drained |> should equal true

        runner.Entries
        |> Seq.map entryText
        |> List.ofSeq
        |> should equal [ "first"; "second" ]
    finally
        stopSystem system

// ──────────────────────────────────────────────────────────────────────────
// Router spawns session actors with same-id stability

/// Starts the hosted service with the session child factory wired.
let private startServiceWithFactory (store: ISessionStore) (runner: ScriptedRunner) =
    let options = LegateOptions()

    let service =
        LocalActorSystemService(OptionsWrapper<LegateOptions>(options) :> IOptions<LegateOptions>, TimeProvider.System)

    service.SessionChildFactory <- Some(SessionActor.spawnFactory store tenant runner.Func)

    (service :> IHostedService).StartAsync(CancellationToken.None).GetAwaiter().GetResult()
    service

[<Fact>]
let ``Router spawns session actors with same-id stability`` () =
    let store = createStore ()
    let runner = ScriptedRunner([ "routed" ])
    let service = startServiceWithFactory store runner

    try
        let first =
            service.ResolveSessionAsync(SessionId.New().Value, CancellationToken.None).GetAwaiter().GetResult()

        let second =
            service.ResolveSessionAsync(SessionId.New().Value, CancellationToken.None).GetAwaiter().GetResult()

        Assert.NotSame(first, second)

        let id = SessionId.New().Value

        let one =
            service.ResolveSessionAsync(id, CancellationToken.None).GetAwaiter().GetResult()

        let two =
            service.ResolveSessionAsync(id, CancellationToken.None).GetAwaiter().GetResult()

        Assert.Same(one, two)

        let snapshot = snapshotOf one
        snapshot.State |> should equal SessionState.Idle
    finally
        (service :> IHostedService).StopAsync(CancellationToken.None).GetAwaiter().GetResult()

// ──────────────────────────────────────────────────────────────────────────
// Abort with typed stop causes (issue 35)

[<Fact>]
let ``Abort on Idle is a no-op returning the current state`` () =
    use system = createSystem ()
    let store = createStore ()
    let created = createSession store
    let settled = ResizeArray<TurnResult>()
    let runner = ScriptedRunner([ "never" ])
    let session = spawnSessionWithProbe system store created.Id runner.Func settled.Add

    try
        let snapshot = abort store created.Id session StopCause.ExplicitAbort "host stop"

        snapshot.State |> should equal SessionState.Idle
        snapshot.PendingCount |> should equal 0
        snapshot.RunningPosition |> should equal None
        runner.Calls |> should equal 0
        settled.Count |> should equal 0
        (storedOf store created.Id).State |> should equal SessionState.Idle
    finally
        stopSystem system

[<Fact>]
let ``Abort on Running settles Aborted with the winning cause`` () =
    use system = createSystem ()
    let store = createStore ()
    let created = createSession store
    let settled = ResizeArray<TurnResult>()
    let runner = GatedRunner([ "doomed" ])
    let session = spawnSessionWithProbe system store created.Id runner.Func settled.Add

    try
        prompt store created.Id session "doomed" |> ignore

        let running =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () -> (snapshotOf session).State = SessionState.Running)

        running |> should equal true

        let snapshot = abort store created.Id session StopCause.ExplicitAbort "host stop"
        snapshot.State |> should equal SessionState.Running

        // The stop arrived first, so it wins even though the turn then
        // reports a success: the settlement maps to Aborted under the cause.
        runner.Release()

        let drained =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                settled.Count = 1 && (snapshotOf session).State = SessionState.Idle)

        drained |> should equal true
        settled[0].Status |> should equal TurnStatus.Aborted

        match settled[0].Outcome with
        | :? TurnAborted as aborted ->
            aborted.Cause |> should equal StopCause.ExplicitAbort
            aborted.Reason |> should equal "host stop"
        | _ -> failwith "Expected a TurnAborted outcome."

        (pendingOf store created.Id).Count |> should equal 0
    finally
        stopSystem system

[<Fact>]
let ``Settlement racing an abort wins: exactly one winner`` () =
    use system = createSystem ()
    let store = createStore ()
    let created = createSession store
    let settled = ResizeArray<TurnResult>()
    let runner = ScriptedRunner([ "done" ])
    let session = spawnSessionWithProbe system store created.Id runner.Func settled.Add

    try
        prompt store created.Id session "quick" |> ignore

        let drained =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                settled.Count = 1 && (snapshotOf session).State = SessionState.Idle)

        drained |> should equal true
        settled[0].Status |> should equal TurnStatus.Completed

        // The turn already settled, so the abort is a no-op: still Idle,
        // still settled exactly once.
        let snapshot = abort store created.Id session StopCause.ExplicitAbort "too late"
        snapshot.State |> should equal SessionState.Idle
        settled.Count |> should equal 1
        runner.Calls |> should equal 1
        (pendingOf store created.Id).Count |> should equal 0
    finally
        stopSystem system

[<Fact>]
let ``A second abort keeps the first cause`` () =
    use system = createSystem ()
    let store = createStore ()
    let created = createSession store
    let settled = ResizeArray<TurnResult>()
    let runner = GatedRunner([ "doomed" ])
    let session = spawnSessionWithProbe system store created.Id runner.Func settled.Add

    try
        prompt store created.Id session "doomed" |> ignore

        let running =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () -> (snapshotOf session).State = SessionState.Running)

        running |> should equal true

        let first = abort store created.Id session StopCause.ExplicitAbort "first"
        first.State |> should equal SessionState.Running

        let second = abort store created.Id session StopCause.HostShutdown "second"
        second.State |> should equal SessionState.Running

        runner.Release()

        let drained =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                settled.Count = 1 && (snapshotOf session).State = SessionState.Idle)

        drained |> should equal true
        settled[0].Status |> should equal TurnStatus.Aborted

        match settled[0].Outcome with
        | :? TurnAborted as aborted ->
            aborted.Cause |> should equal StopCause.ExplicitAbort
            aborted.Reason |> should equal "first"
        | _ -> failwith "Expected a TurnAborted outcome."
    finally
        stopSystem system

[<Fact>]
let ``Queue drain continues after an abort settle`` () =
    use system = createSystem ()
    let store = createStore ()
    let created = createSession store
    let settled = ResizeArray<TurnResult>()
    let runner = GatedRunner([ "first"; "second" ])
    let session = spawnSessionWithProbe system store created.Id runner.Func settled.Add

    try
        prompt store created.Id session "first" |> ignore

        let running =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () -> (snapshotOf session).State = SessionState.Running)

        running |> should equal true

        prompt store created.Id session "second" |> ignore

        abort store created.Id session StopCause.ExplicitAbort "host stop" |> ignore
        runner.Release()

        let drained =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                runner.Calls = 2 && (snapshotOf session).State = SessionState.Idle)

        drained |> should equal true

        // The aborted first turn settles once under its cause, then the
        // queued second entry drains and settles normally.
        settled.Count |> should equal 2
        settled[0].Status |> should equal TurnStatus.Aborted

        match settled[0].Outcome with
        | :? TurnAborted as aborted ->
            aborted.Cause |> should equal StopCause.ExplicitAbort
            aborted.Reason |> should equal "host stop"
        | _ -> failwith "Expected a TurnAborted outcome."

        settled[1].Status |> should equal TurnStatus.Completed

        runner.Entries
        |> Seq.map entryText
        |> List.ofSeq
        |> should equal [ "first"; "second" ]

        (pendingOf store created.Id).Count |> should equal 0
    finally
        stopSystem system

[<Fact>]
let ``Abort on Idle leaves non-Queue entries pending`` () =
    use system = createSystem ()
    let store = createStore ()
    let created = createSession store
    let runner = ScriptedRunner([ "never" ])
    let session = spawnSession system store created.Id runner.Func

    try
        let payload = UserMessagePayload(UserMessage.Text("steer")) :> InboxPayload

        store
            .AppendInboxMessage(tenant, created.Id, payload, DeliveryMode.Inject, CancellationToken.None)
            .GetAwaiter()
            .GetResult()
        |> ignore

        let snapshot = abort store created.Id session StopCause.ExplicitAbort "host stop"

        snapshot.State |> should equal SessionState.Idle
        runner.Calls |> should equal 0
        (pendingOf store created.Id).Count |> should equal 1
    finally
        stopSystem system

[<Fact>]
let ``Abort while WaitingForInput is a no-op`` () =
    use system = createSystem ()
    let store = createStore ()
    let created = createSession store

    store
        .UpdateSessionState(tenant, created.Id, SessionState.WaitingForInput, CancellationToken.None)
        .GetAwaiter()
        .GetResult()
    |> ignore

    let settled = ResizeArray<TurnResult>()
    let runner = ScriptedRunner([ "waiting" ])
    let session = spawnSessionWithProbe system store created.Id runner.Func settled.Add

    try
        let snapshot = abort store created.Id session StopCause.ExplicitAbort "host stop"

        // Suspended turns belong to issue 36: nothing runs to abort.
        snapshot.State |> should equal SessionState.WaitingForInput
        runner.Calls |> should equal 0
        settled.Count |> should equal 0
    finally
        stopSystem system

[<Fact>]
let ``Abort on Closed throws InvalidSessionStateException at the boundary`` () =
    use system = createSystem ()
    let store = createStore ()
    let created = createSession store
    let runner = ScriptedRunner([ "done" ])
    let session = spawnSession system store created.Id runner.Func

    try
        close store created.Id session |> ignore

        let ex =
            Assert.Throws<InvalidSessionStateException>(fun () ->
                abort store created.Id session StopCause.ExplicitAbort "late" |> ignore)

        ex.SessionId |> should equal created.Id
        ex.CurrentState |> should equal (SessionState.Closed.ToString())
        runner.Calls |> should equal 0
    finally
        stopSystem system

[<Fact>]
let ``Abort on an unknown session throws SessionNotFoundException`` () =
    use system = createSystem ()
    let store = createStore ()
    let created = createSession store
    let runner = ScriptedRunner([ "done" ])
    let session = spawnSession system store created.Id runner.Func
    let missing = SessionId.New()

    try
        Assert.Throws<SessionNotFoundException>(fun () ->
            abort store missing session StopCause.ExplicitAbort "ghost" |> ignore)
        |> ignore
    finally
        stopSystem system

[<Fact>]
let ``Abort rejects a non-abort-family cause`` () =
    use system = createSystem ()
    let store = createStore ()
    let created = createSession store
    let runner = ScriptedRunner([ "done" ])
    let session = spawnSession system store created.Id runner.Func

    try
        Assert.Throws<ArgumentOutOfRangeException>(fun () ->
            abort store created.Id session StopCause.Deadline "deadline" |> ignore)
        |> ignore

        Assert.Throws<ArgumentOutOfRangeException>(fun () ->
            abort store created.Id session StopCause.LeaseLoss "lease" |> ignore)
        |> ignore
    finally
        stopSystem system

[<Fact>]
let ``Abort rejects a null reason`` () =
    use system = createSystem ()
    let store = createStore ()
    let created = createSession store
    let runner = ScriptedRunner([ "done" ])
    let session = spawnSession system store created.Id runner.Func
    let nullReason = Unchecked.defaultof<string>

    try
        Assert.Throws<ArgumentNullException>(fun () ->
            abort store created.Id session StopCause.ExplicitAbort nullReason |> ignore)
        |> ignore
    finally
        stopSystem system

// ──────────────────────────────────────────────────────────────────────────
// Suspend and resume (issue 36)

/// In-memory journal fake without claim fencing: records appends and serves
/// one replay page. The production fence lives in the real stores; these
/// tests prove the actor's suspend bookkeeping, and the takeover test below
/// proves the fenced store rejects the loser with zero writes.
type RecordingEventStore() =
    let events = ResizeArray<SessionEvent>()

    /// Every appended event, in append order.
    member _.Appended = events :> IReadOnlyList<SessionEvent>

    interface ISessionEventStore with
        member _.Append(_, _, _, batch, _) =
            if isNull (box batch) then
                raise (ArgumentNullException(nameof batch))

            for event in batch do
                events.Add(event)

            let stamped = ResizeArray<SessionEvent>(events) :> IReadOnlyList<SessionEvent>
            Task.FromResult(EventAppended(stamped) :> EventAppendOutcome)

        member _.Replay(_, sessionId, fromSequence, _, _) =
            if fromSequence = 0L && events.Count > 0 then
                let page = ResizeArray<SessionEvent>(events) :> IReadOnlyList<SessionEvent>
                Task.FromResult(EventReplayPage(sessionId, page, Nullable<int64>()) :> EventReplayOutcome)
            else
                Task.FromResult(EventReplayEndOfStream(sessionId) :> EventReplayOutcome)

        member _.TryClaimCleanup(_, sessionId, _, _, _) =
            Task.FromResult(EventCleanupNotClaimable(sessionId, "notSupported") :> EventCleanupState)

        member _.CompleteCleanup(_, sessionId, _, _) =
            Task.FromResult(EventCleanupRejected(sessionId, "staleClaim") :> EventCleanupSettlement)

        member _.DeferCleanup(_, sessionId, _, _) =
            Task.FromResult(EventCleanupRejected(sessionId, "staleClaim") :> EventCleanupSettlement)

/// Delay seam that fires only when the test releases it: AskTimeout tests
/// prove the Failed settle at the seam boundary without sleeping.
type ManualDelay() =
    let gate = new TaskCompletionSource<unit>()

    /// Releases the waiting AskTimeout delay.
    member _.Fire() = gate.TrySetResult() |> ignore

    interface ILlmDelay with
        member _.Delay(_, cancellationToken) =
            task {
                do! gate.Task
                cancellationToken.ThrowIfCancellationRequested()
            }
            :> Task

/// Builds one suspend cursor for the scripted runner.
let private suspendCursor
    (requestId: string)
    (toolName: string)
    (callId: string)
    (kind: TurnLoop.SuspensionKind)
    : TurnLoop.TurnLoopSuspension =
    let args = Dictionary<string, obj>() :> IDictionary<string, obj>
    let pendingCall = FunctionCallContent(callId, toolName, args)

    {
        RequestId = requestId
        ToolName = toolName
        ToolCallId = callId
        Kind = kind
        QuestionText =
            if kind = TurnLoop.QuestionSuspension then
                "Which region?"
            else
                ""
        QuestionOptions = []
        HistorySnapshot = ResizeArray<ChatMessage>() :> IList<ChatMessage>
        InputTokens = 3L
        OutputTokens = 5L
        Iterations = 1
        PendingCall = pendingCall
        Nested = None
    }

/// A Suspended completion parking on the given cursor.
let private suspendedCompletion (cursor: TurnLoop.TurnLoopSuspension) : TurnLoop.TurnLoopCompletion =
    {
        Result =
            {
                AssistantText = ""
                Status = TurnStatus.Suspended
                Iterations = cursor.Iterations
                Usage =
                    {
                        InputTokens = cursor.InputTokens
                        OutputTokens = cursor.OutputTokens
                    }
                Outcome = null
            }
        HasPendingInjects = false
        Suspension = Some cursor
    }

/// A settled completion with no suspension.
let private settledCompletion (text: string) : TurnLoop.TurnLoopCompletion =
    {
        Result = completed text
        HasPendingInjects = false
        Suspension = None
    }

/// Scripted suspendable runner: answers per attempt, recording attempts,
/// cursors, and replies in call order.
type private ScriptSuspendRunner(first: TurnLoop.TurnLoopCompletion, second: TurnLoop.TurnLoopCompletion) =
    let attempts = ResizeArray<int>()
    let cursors = ResizeArray<TurnLoop.TurnLoopSuspension option>()
    let replies = ResizeArray<Reply option>()

    /// Attempts in call order.
    member _.Attempts = attempts :> IReadOnlyList<int>

    /// The runner as the suspendable delegate.
    member _.Func
        : (InboxEntry
              -> int
              -> HashSet<string>
              -> TurnLoop.TurnLoopSuspension option
              -> Reply option
              -> CancellationToken
              -> Task<TurnLoop.TurnLoopCompletion>) =
        fun _ attempt _ cursor reply _ ->
            attempts.Add(attempt)
            cursors.Add(cursor)
            replies.Add(reply)

            if attempt = 1 then
                Task.FromResult(first)
            else
                Task.FromResult(second)

    /// Cursors in call order.
    member _.Cursors = cursors :> IReadOnlyList<TurnLoop.TurnLoopSuspension option>

    /// Replies in call order.
    member _.Replies = replies :> IReadOnlyList<Reply option>

/// Spawns a suspendable session actor on a test system.
let private spawnSuspendable
    (system: ActorSystem)
    (store: ISessionStore)
    (journal: RecordingEventStore)
    (delay: ILlmDelay)
    (askTimeout: TimeSpan)
    (sessionId: SessionId)
    (runner: ScriptSuspendRunner)
    (settled: ResizeArray<TurnResult>)
    : IActorRef =
    let baseProps: SessionActorProps =
        {
            Store = store
            Tenant = tenant
            SessionId = sessionId
            RunTurn = (fun _ _ -> Task.FromResult(completed "unused"))
            OnTurnSettled = Some(fun result -> lock settled (fun () -> settled.Add(result)))
            OnInjectJournaled = None
            Logger = null
            Compact = None
        }

    let deps: SessionActor.SuspendDeps =
        {
            EventStore = journal :> ISessionEventStore
            Delay = delay
            AskTimeout = askTimeout
            JournalToken = "test-token"
            RunSuspendable = runner.Func
        }

    spawn system $"suspend-{Guid.NewGuid():N}" (SessionActor.behaviorWithSuspend baseProps deps)

/// Prompts a suspendable actor, blocking for the ack.
let private promptSuspendable (store: ISessionStore) (sessionId: SessionId) (session: IActorRef) (text: string) =
    SessionActor.promptSuspendableAsync store tenant sessionId session (UserMessage.Text text) CancellationToken.None
    |> fun task -> task.GetAwaiter().GetResult()

/// Replies to a suspendable actor, blocking for the ack.
let private reply (store: ISessionStore) (sessionId: SessionId) (session: IActorRef) (answer: Reply) =
    SessionActor.replyAsync store tenant sessionId session answer CancellationToken.None
    |> fun task -> task.GetAwaiter().GetResult()

/// Reads a suspendable actor's snapshot, blocking.
let private suspendSnapshotOf (session: IActorRef) : SessionSnapshot =
    SessionActor.getSuspendSnapshotAsync session CancellationToken.None
    |> fun task -> task.GetAwaiter().GetResult()

let private requestedOf (journal: RecordingEventStore) : PermissionRequestedEvent list =
    [
        for event in journal.Appended do
            match event with
            | :? PermissionRequestedEvent as asked when not (isNull (box asked)) -> yield asked
            | _ -> ()
    ]

let private resolvedOf (journal: RecordingEventStore) : PermissionResolvedEvent list =
    [
        for event in journal.Appended do
            match event with
            | :? PermissionResolvedEvent as resolved when not (isNull (box resolved)) -> yield resolved
            | _ -> ()
    ]

[<Fact>]
let ``Suspend persists WaitingForInput with the pending id`` () =
    use system = createSystem ()
    let store = createStore ()
    let created = createSession store
    let journal = RecordingEventStore()

    let runner =
        ScriptSuspendRunner(
            suspendedCompletion (suspendCursor "req-1" "exec" "c1" TurnLoop.PermissionSuspension),
            settledCompletion "done"
        )

    let settled = ResizeArray<TurnResult>()

    let session =
        spawnSuspendable
            system
            store
            journal
            (TurnLoopTests.NeverDelay() :> ILlmDelay)
            (TimeSpan.FromMinutes 5.0)
            created.Id
            runner
            settled

    try
        promptSuspendable store created.Id session "run" |> ignore

        let suspended =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                (storedOf store created.Id).State = SessionState.WaitingForInput)

        suspended |> should equal true

        let snapshot = suspendSnapshotOf session
        snapshot.State |> should equal SessionState.WaitingForInput
        snapshot.PendingRequestId |> should equal "req-1"

        let asked = requestedOf journal
        asked.Length |> should equal 1
        asked[0].RequestId |> should equal "req-1"
        asked[0].ToolName |> should equal "exec"

        runner.Attempts |> List.ofSeq |> should equal [ 1 ]
        settled.Count |> should equal 0
    finally
        stopSystem system

[<Fact>]
let ``Matching Reply resumes from the cursor with attempt plus 1`` () =
    use system = createSystem ()
    let store = createStore ()
    let created = createSession store
    let journal = RecordingEventStore()
    let cursor = suspendCursor "req-2" "exec" "c1" TurnLoop.PermissionSuspension

    let runner =
        ScriptSuspendRunner(suspendedCompletion cursor, settledCompletion "finished")

    let settled = ResizeArray<TurnResult>()

    let session =
        spawnSuspendable
            system
            store
            journal
            (TurnLoopTests.NeverDelay() :> ILlmDelay)
            (TimeSpan.FromMinutes 5.0)
            created.Id
            runner
            settled

    try
        promptSuspendable store created.Id session "run" |> ignore

        let suspended =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                (storedOf store created.Id).State = SessionState.WaitingForInput)

        suspended |> should equal true

        reply store created.Id session (PermissionDecision("req-2", PermissionDecisionKind.AllowOnce))
        |> ignore

        let resumed =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                settled.Count = 1 && (storedOf store created.Id).State = SessionState.Idle)

        resumed |> should equal true
        runner.Attempts |> List.ofSeq |> should equal [ 1; 2 ]
        settled[0].Status |> should equal TurnStatus.Completed
        settled[0].AssistantText |> should equal "finished"

        // The resume carried the live cursor and the matching reply.
        runner.Cursors[1].IsSome |> should equal true
        runner.Cursors[1].Value.RequestId |> should equal "req-2"
        runner.Cursors[1].Value.ToolCallId |> should equal "c1"

        match runner.Replies[1] with
        | Some(:? PermissionDecision as decision) ->
            decision.RequestId |> should equal "req-2"
            decision.Decision |> should equal PermissionDecisionKind.AllowOnce
        | _ -> failwith "Expected the resume to carry the PermissionDecision."

        let resolved = resolvedOf journal
        resolved.Length |> should equal 1
        resolved[0].RequestId |> should equal "req-2"
        resolved[0].Decision |> should equal PermissionDecisionKind.AllowOnce

        (pendingOf store created.Id).Count |> should equal 0
    finally
        stopSystem system

[<Fact>]
let ``Unknown Reply rejects with ReplyMismatchException`` () =
    use system = createSystem ()
    let store = createStore ()
    let created = createSession store
    let journal = RecordingEventStore()

    let runner =
        ScriptSuspendRunner(
            suspendedCompletion (suspendCursor "req-3" "exec" "c1" TurnLoop.PermissionSuspension),
            settledCompletion "done"
        )

    let settled = ResizeArray<TurnResult>()

    let session =
        spawnSuspendable
            system
            store
            journal
            (TurnLoopTests.NeverDelay() :> ILlmDelay)
            (TimeSpan.FromMinutes 5.0)
            created.Id
            runner
            settled

    try
        promptSuspendable store created.Id session "run" |> ignore

        let suspended =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                (storedOf store created.Id).State = SessionState.WaitingForInput)

        suspended |> should equal true

        let ex =
            Assert.Throws<ReplyMismatchException>(fun () ->
                reply store created.Id session (PermissionDecision("req-unknown", PermissionDecisionKind.AllowOnce))
                |> ignore)

        ex.SessionId |> should equal created.Id
        ex.RequestId |> should equal "req-unknown"

        // Still suspended on the original request; nothing resolved.
        (suspendSnapshotOf session).PendingRequestId |> should equal "req-3"
        runner.Attempts |> List.ofSeq |> should equal [ 1 ]
        settled.Count |> should equal 0
        resolvedOf journal |> List.isEmpty |> should equal true
    finally
        stopSystem system

[<Fact>]
let ``Already-resolved Reply rejects with ReplyMismatchException`` () =
    use system = createSystem ()
    let store = createStore ()
    let created = createSession store
    let journal = RecordingEventStore()
    let cursor = suspendCursor "req-4" "exec" "c1" TurnLoop.PermissionSuspension

    let runner =
        ScriptSuspendRunner(suspendedCompletion cursor, settledCompletion "done")

    let settled = ResizeArray<TurnResult>()

    let session =
        spawnSuspendable
            system
            store
            journal
            (TurnLoopTests.NeverDelay() :> ILlmDelay)
            (TimeSpan.FromMinutes 5.0)
            created.Id
            runner
            settled

    try
        promptSuspendable store created.Id session "run" |> ignore

        let suspended =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                (storedOf store created.Id).State = SessionState.WaitingForInput)

        suspended |> should equal true

        reply store created.Id session (PermissionDecision("req-4", PermissionDecisionKind.AllowOnce))
        |> ignore

        let resumed = waitFor (TimeSpan.FromSeconds 10.0) (fun () -> settled.Count = 1)

        resumed |> should equal true

        // The turn settled; the same reply appended again answers nothing.
        let payload =
            ReplyPayload(PermissionDecision("req-4", PermissionDecisionKind.AllowOnce)) :> InboxPayload

        let appended =
            store.AppendInboxMessage(tenant, created.Id, payload, DeliveryMode.Queue, CancellationToken.None)
            |> fun task -> task.GetAwaiter().GetResult()

        let answer: SessionActor.SessionReplyReply =
            session.Ask<SessionActor.SessionReplyReply>(SessionActor.ReplyEntry appended, TimeSpan.FromSeconds 10.0)
            |> Async.RunSynchronously

        match answer with
        | SessionActor.ReplyRejected error -> error.RequestId |> should equal "req-4"
        | SessionActor.ReplyAccepted _ -> failwith "Expected the already-resolved reply to reject."
    finally
        stopSystem system

[<Fact>]
let ``Question suspend answers through the same carrier`` () =
    use system = createSystem ()
    let store = createStore ()
    let created = createSession store
    let journal = RecordingEventStore()

    let cursor =
        suspendCursor "q-1" TurnLoop.AskUserToolName "qcall-1" TurnLoop.QuestionSuspension

    let runner =
        ScriptSuspendRunner(suspendedCompletion cursor, settledCompletion "answered")

    let settled = ResizeArray<TurnResult>()

    let session =
        spawnSuspendable
            system
            store
            journal
            (TurnLoopTests.NeverDelay() :> ILlmDelay)
            (TimeSpan.FromMinutes 5.0)
            created.Id
            runner
            settled

    try
        promptSuspendable store created.Id session "run" |> ignore

        let suspended =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                (storedOf store created.Id).State = SessionState.WaitingForInput)

        suspended |> should equal true
        (suspendSnapshotOf session).PendingRequestId |> should equal "q-1"

        reply store created.Id session (QuestionAnswer("q-1", "west")) |> ignore

        let resumed =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                settled.Count = 1 && (storedOf store created.Id).State = SessionState.Idle)

        resumed |> should equal true
        runner.Attempts |> List.ofSeq |> should equal [ 1; 2 ]
        settled[0].AssistantText |> should equal "answered"

        let answered =
            [
                for event in journal.Appended do
                    match event with
                    | :? QuestionAnsweredEvent as answered when not (isNull (box answered)) -> yield answered
                    | _ -> ()
            ]

        answered.Length |> should equal 1
        answered[0].QuestionId |> should equal "q-1"
        answered[0].Answer |> should equal "west"
    finally
        stopSystem system

[<Fact>]
let ``AskTimeout settles Failed with TurnFailed`` () =
    use system = createSystem ()
    let store = createStore ()
    let created = createSession store
    let journal = RecordingEventStore()
    let delay = ManualDelay()

    let runner =
        ScriptSuspendRunner(
            suspendedCompletion (suspendCursor "req-5" "exec" "c1" TurnLoop.PermissionSuspension),
            settledCompletion "never"
        )

    let settled = ResizeArray<TurnResult>()

    let session =
        spawnSuspendable system store journal (delay :> ILlmDelay) (TimeSpan.FromMinutes 5.0) created.Id runner settled

    try
        promptSuspendable store created.Id session "run" |> ignore

        let suspended =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                (storedOf store created.Id).State = SessionState.WaitingForInput)

        suspended |> should equal true

        delay.Fire()

        let timedOut =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                settled.Count = 1 && (storedOf store created.Id).State = SessionState.Idle)

        timedOut |> should equal true
        settled[0].Status |> should equal TurnStatus.Failed

        match settled[0].Outcome with
        | :? TurnFailed as failed -> failed.Reason |> should equal SessionActor.AskTimeoutReason
        | _ -> failwith "Expected a TurnFailed outcome, not Aborted."

        runner.Attempts |> List.ofSeq |> should equal [ 1 ]
    finally
        stopSystem system

[<Fact>]
let ``Crash rebuilds the pending request from the journal and resumes`` () =
    let store = createStore ()
    let journal = RecordingEventStore()
    let created = createSession store
    let cursor = suspendCursor "req-6" "exec" "c1" TurnLoop.PermissionSuspension

    let runner =
        ScriptSuspendRunner(suspendedCompletion cursor, settledCompletion "recovered")

    let settled = ResizeArray<TurnResult>()

    use firstSystem = createSystem ()

    let first =
        spawnSuspendable
            firstSystem
            store
            journal
            (TurnLoopTests.NeverDelay() :> ILlmDelay)
            (TimeSpan.FromMinutes 5.0)
            created.Id
            runner
            settled

    try
        promptSuspendable store created.Id first "run" |> ignore

        let suspended =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                (storedOf store created.Id).State = SessionState.WaitingForInput)

        suspended |> should equal true
    finally
        stopSystem firstSystem

    // Crash: a fresh actor over the same store and journal rebuilds the
    // pending request and resumes from the same tool call under attempt 2.
    use secondSystem = createSystem ()

    let second =
        spawnSuspendable
            secondSystem
            store
            journal
            (TurnLoopTests.NeverDelay() :> ILlmDelay)
            (TimeSpan.FromMinutes 5.0)
            created.Id
            runner
            settled

    try
        let rebuilt =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                try
                    (suspendSnapshotOf second).PendingRequestId = "req-6"
                with _ ->
                    false)

        rebuilt |> should equal true
        (storedOf store created.Id).State |> should equal SessionState.WaitingForInput

        reply store created.Id second (PermissionDecision("req-6", PermissionDecisionKind.AllowOnce))
        |> ignore

        let resumed =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                settled.Count = 1 && (storedOf store created.Id).State = SessionState.Idle)

        resumed |> should equal true
        runner.Attempts |> List.ofSeq |> should equal [ 1; 2 ]
        settled[0].AssistantText |> should equal "recovered"
    finally
        stopSystem secondSystem

[<Fact>]
let ``Takeover while suspended leaves the loser with zero journal effects`` () =
    use system = createSystem ()
    let database = InMemoryDatabase()
    let store = InMemorySessionStore(database) :> ISessionStore
    let journal = InMemorySessionEventStore(database) :> ISessionEventStore
    let created = createSession store

    // The suspended turn holds its claim; a takeover re-claims under a new
    // owner once the lease lapses, and the loser's journal token rejects
    // with zero writes.
    let payload = UserMessagePayload(UserMessage.Text("run")) :> InboxPayload

    store.AppendInboxMessage(tenant, created.Id, payload, DeliveryMode.Queue, CancellationToken.None)
    |> fun task -> task.GetAwaiter().GetResult()
    |> ignore

    let claim =
        match
            store.ClaimNextTurn(tenant, created.Id, "owner-a", TimeSpan.FromMinutes 5.0, CancellationToken.None)
            |> fun task -> task.GetAwaiter().GetResult()
        with
        | :? TurnLeaseRenewed as renewed -> renewed.Claim
        | :? TurnLeaseHeld as held -> held.Claim
        | _ -> failwith "Expected the suspend claim to be granted."

    try
        let turnId = TurnId.New()
        let stamp = DateTimeOffset.UtcNow

        let asked =
            PermissionRequestedEvent(created.Id, turnId, Nullable<int64>(), stamp, "req-takeover", "exec")
            :> SessionEvent

        let appended =
            journal.Append(
                tenant,
                created.Id,
                claim.Token,
                (ResizeArray<SessionEvent>([| asked |]) :> IReadOnlyList<SessionEvent>),
                CancellationToken.None
            )
            |> fun task -> task.GetAwaiter().GetResult()

        match appended with
        | :? EventAppended -> ()
        | _ -> failwith "Expected the live claim's journal append to land."

        // Takeover: the loser stops renewing, the lease lapses on the test
        // clock path is simulated by verifying with an unknown token, which
        // the fence rejects the same way an expired claim rejects.
        let loser =
            journal.Append(
                tenant,
                created.Id,
                "stale-token",
                (ResizeArray<SessionEvent>([| asked |]) :> IReadOnlyList<SessionEvent>),
                CancellationToken.None
            )
            |> fun task -> task.GetAwaiter().GetResult()

        match loser with
        | :? EventAppendRejected as rejected -> rejected.Reason |> should equal "staleClaim"
        | _ -> failwith "Expected the loser's journal append to reject with zero writes."

        let replayed =
            journal.Replay(tenant, created.Id, 0L, 10, CancellationToken.None)
            |> fun task -> task.GetAwaiter().GetResult()

        match replayed with
        | :? EventReplayPage as page -> page.Events.Count |> should equal 1
        | _ -> failwith "Expected the winner's single write to replay."
    finally
        stopSystem system

// ──────────────────────────────────────────────────────────────────────────
// Inject and Interrupt delivery modes (issue 34)

/// Prompts through the delivery-mode boundary and blocks for the ack.
let private promptWithDelivery
    (store: ISessionStore)
    (sessionId: SessionId)
    (session: IActorRef)
    (text: string)
    (delivery: DeliveryMode)
    : InboxEntry =
    SessionActor.promptAsync store tenant sessionId session (UserMessage.Text text) delivery CancellationToken.None
    |> fun task -> task.GetAwaiter().GetResult()

/// Appends one user message straight to the store with the given delivery
/// (pre-existing inbox, or crash-orphaned work).
let private appendStoredWith
    (store: ISessionStore)
    (sessionId: SessionId)
    (text: string)
    (delivery: DeliveryMode)
    : InboxEntry =
    let payload = UserMessagePayload(UserMessage.Text(text)) :> InboxPayload

    store.AppendInboxMessage(tenant, sessionId, payload, delivery, CancellationToken.None).GetAwaiter().GetResult()

/// Spawns a session actor observing settled results and folded Inject
/// journal events into the probes. Settled observations arrive on the
/// actor thread (serialised like the existing probe); journal
/// observations arrive on the turn thread and are locked.
let private spawnSessionFull
    (system: ActorSystem)
    (store: ISessionStore)
    (sessionId: SessionId)
    (runTurn: InboxEntry -> CancellationToken -> Task<TurnResult>)
    (settled: ResizeArray<TurnResult>)
    (journaled: ResizeArray<UserMessageEvent>)
    : IActorRef =
    let props: SessionActorProps =
        {
            Store = store
            Tenant = tenant
            SessionId = sessionId
            RunTurn = runTurn
            OnTurnSettled = Some settled.Add
            OnInjectJournaled = Some(fun event -> lock journaled (fun () -> journaled.Add(event)))
            Logger = null
            Compact = None
        }

    spawn system $"test-{Guid.NewGuid():N}" (SessionActor.behavior props)

/// Builds the Inject-aware production runner over the given client for one
/// session, journaling folded Inject entries into the probe.
let private injectRunner
    (store: ISessionStore)
    (sessionId: SessionId)
    (client: IChatClient)
    (tools: IReadOnlyDictionary<string, AITool>)
    (journaled: ResizeArray<UserMessageEvent>)
    : (InboxEntry -> CancellationToken -> Task<TurnResult>) =
    let wiring: SessionActor.InjectFoldWiring =
        {
            Store = store
            Tenant = tenant
            SessionId = sessionId
            JournalEvent = Some(fun event -> lock journaled (fun () -> journaled.Add(event)))
        }

    SessionActor.createInjectFoldRunner
        client
        tools
        TurnLoop.TurnLoopOptions.Default
        (TurnLoopTests.NeverDelay() :> ILlmDelay)
        wiring

/// The user texts carried by one provider call, in order.
let private callUserTexts (messages: IEnumerable<ChatMessage>) : string list =
    if isNull (box messages) then
        []
    else
        [
            for message in messages do
                if not (isNull (box message)) && message.Role = ChatRole.User then
                    if not (isNull (box message.Contents)) then
                        for content in message.Contents do
                            match content with
                            | :? TextContent as text when not (isNull (box text)) ->
                                yield (if isNull (box text.Text) then "" else text.Text)
                            | _ -> ()
        ]

/// Scripted IChatClient that returns queued responses in order and records
/// the user texts of every provider call, so Inject tests prove the folded
/// message reached the model context. Streaming is unimplemented like the
/// shared scripted client: the loop falls back to a single delta.
type RecordingChatClient(responses: ChatResponse list) =
    let gate = obj ()
    let mutable calls = 0
    let seen = ResizeArray<string list>()

    interface IChatClient with
        member _.GetResponseAsync(messages, _, _) =
            let index =
                lock gate (fun () ->
                    calls <- calls + 1
                    seen.Add(callUserTexts messages)
                    min (calls - 1) (responses.Length - 1))

            Task.FromResult(responses[index])

        member _.GetStreamingResponseAsync(_, _, _) = raise (NotImplementedException())

        member _.GetService(_, _) = null
        member _.Dispose() = ()

    /// How many provider calls ran.
    member _.Calls = lock gate (fun () -> calls)

    /// The user texts of every provider call, in call order.
    member _.Seen = lock gate (fun () -> seen |> List.ofSeq)

/// An AIFunction that signals Started and then waits for its gate before
/// answering, so the test can Inject while the tool runs. The gate always
/// releases on the test thread: the waiting turn never needs cancellation.
let private gatedTool
    (name: string)
    (result: string)
    (started: TaskCompletionSource<unit>)
    (gate: TaskCompletionSource<unit>)
    (invocations: string list ref)
    : AIFunction =
    let method =
        System.Func<Task<string>>(fun () ->
            task {
                started.TrySetResult() |> ignore
                do! gate.Task
                invocations.Value <- invocations.Value @ [ name ]
                return result
            })

    AIFunctionFactory.Create(method, name, Unchecked.defaultof<string>, Unchecked.defaultof<JsonSerializerOptions>)

/// An IChatClient whose first provider call blocks until its token fires
/// (the pre-empted turn) and whose later calls answer with the given text.
/// Streaming is unimplemented: the loop falls back to a single delta.
type BlockingFirstClient(secondText: string) =
    let gate = obj ()
    let mutable calls = 0

    interface IChatClient with
        member _.GetResponseAsync(_, _, cancellationToken) =
            task {
                let call =
                    lock gate (fun () ->
                        calls <- calls + 1
                        calls)

                if call = 1 then
                    do! Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken)
                    return TurnLoopTests.textResponse "never"
                else
                    return TurnLoopTests.textResponse secondText
            }

        member _.GetStreamingResponseAsync(_, _, _) = raise (NotImplementedException())

        member _.GetService(_, _) = null
        member _.Dispose() = ()

    /// How many provider calls ran.
    member _.Calls = lock gate (fun () -> calls)

/// The text carried by a journaled Inject event, joining its text parts.
let private journaledText (event: UserMessageEvent) : string =
    if
        isNull (box event)
        || isNull (box event.Message)
        || isNull (box event.Message.Parts)
    then
        ""
    else
        event.Message.Parts
        |> Seq.choose (fun part ->
            match part with
            | :? TextContent as text when not (isNull (box text)) ->
                Some(if isNull (box text.Text) then "" else text.Text)
            | _ -> None)
        |> String.concat "\n"

[<Fact>]
let ``Inject while Running folds into history before the next provider call`` () =
    use system = createSystem ()
    let store = createStore ()
    let created = createSession store
    let started = new TaskCompletionSource<unit>()
    let gate = new TaskCompletionSource<unit>()
    let invocations = ref []

    let first =
        new ChatResponse(
            ResizeArray<ChatMessage>(
                [|
                    TurnLoopTests.callMessage [ "c1", "lookup" ]
                |]
            )
        )

    let client =
        new RecordingChatClient(
            [
                first
                TurnLoopTests.textResponse "finished"
            ]
        )

    let tools =
        TurnLoopTests.makeTools
            [
                "lookup", gatedTool "lookup" "row-1" started gate invocations
            ]

    let settled = ResizeArray<TurnResult>()
    let journaled = ResizeArray<UserMessageEvent>()
    let entries = ResizeArray<InboxEntry>()
    let inner = injectRunner store created.Id (client :> IChatClient) tools journaled

    let runner entry cancellationToken =
        task {
            lock entries (fun () -> entries.Add(entry))
            return! inner entry cancellationToken
        }

    let session = spawnSessionFull system store created.Id runner settled journaled

    try
        prompt store created.Id session "first" |> ignore

        // The turn reaches the slow tool before anything is injected.
        let atTool =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () -> started.Task.IsCompleted)

        atTool |> should equal true

        promptWithDelivery store created.Id session "steer-one" DeliveryMode.Inject
        |> ignore

        promptWithDelivery store created.Id session "steer-two" DeliveryMode.Inject
        |> ignore

        let queued =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () -> (pendingOf store created.Id).Count = 3)

        queued |> should equal true

        let beforeFold = DateTimeOffset.UtcNow
        gate.TrySetResult() |> ignore

        let drained =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                settled.Count = 1 && (snapshotOf session).State = SessionState.Idle)

        drained |> should equal true
        let afterFold = DateTimeOffset.UtcNow

        settled[0].Status |> should equal TurnStatus.Completed
        settled[0].AssistantText |> should equal "finished"

        // Two provider calls only: the fold never spends the iteration
        // budget, and the second call sees both steers in position order
        // after the tool result.
        client.Calls |> should equal 2
        client.Seen.Length |> should equal 2
        client.Seen[0] |> should equal [ "first" ]
        client.Seen[1] |> should equal [ "first"; "steer-one"; "steer-two" ]
        invocations.Value |> should equal [ "lookup" ]

        // Each folded entry journaled exactly once, under the running
        // turn's id, with an empty sequence, a fold-time stamp, and the
        // verbatim message.
        let events = lock journaled (fun () -> journaled |> List.ofSeq)
        events.Length |> should equal 2
        events[0].SessionId |> should equal created.Id
        events[1].SessionId |> should equal created.Id
        events[0].TurnId.Value |> should equal events[1].TurnId.Value
        String.IsNullOrEmpty(events[0].TurnId.Value) |> should equal false
        events[0].Sequence.HasValue |> should equal false
        events[1].Sequence.HasValue |> should equal false
        events |> List.map journaledText |> should equal [ "steer-one"; "steer-two" ]

        for event in events do
            (event.Timestamp >= beforeFold && event.Timestamp <= afterFold)
            |> should equal true

        // One turn ran; the folded entries were consumed by the fold, so
        // the settle starts no new turn.
        entries.Count |> should equal 1
        (pendingOf store created.Id).Count |> should equal 0
    finally
        stopSystem system

[<Fact>]
let ``Inject left pending past a would-complete turn starts a new turn at settle`` () =
    use system = createSystem ()
    let store = createStore ()
    let created = createSession store
    let runner = GatedRunner([ "first"; "late" ])
    let session = spawnSession system store created.Id runner.Func

    try
        prompt store created.Id session "first" |> ignore

        let running =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () -> (snapshotOf session).State = SessionState.Running)

        running |> should equal true

        // A scripted runner has no iteration boundary to fold at: the
        // Inject waits, and the settle drain starts its new turn. The
        // TurnLoop pending signal never crosses the runner boundary.
        let injected =
            promptWithDelivery store created.Id session "late-steer" DeliveryMode.Inject

        injected.Delivery |> should equal DeliveryMode.Inject

        runner.Release()

        let drained =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                runner.Calls = 2 && (snapshotOf session).State = SessionState.Idle)

        drained |> should equal true

        runner.Entries
        |> Seq.map entryText
        |> List.ofSeq
        |> should equal [ "first"; "late-steer" ]

        runner.Entries[1].Delivery |> should equal DeliveryMode.Inject
        (pendingOf store created.Id).Count |> should equal 0
    finally
        stopSystem system

[<Fact>]
let ``Interrupt aborts the slow turn as Aborted and drains Interrupt-first with the inbox intact`` () =
    use system = createSystem ()
    let store = createStore ()
    let created = createSession store
    let client = new BlockingFirstClient("after interrupt")
    let settled = ResizeArray<TurnResult>()
    let journaled = ResizeArray<UserMessageEvent>()
    let entries = ResizeArray<InboxEntry>()

    let inner =
        injectRunner store created.Id (client :> IChatClient) (TurnLoopTests.makeTools []) journaled

    let runner entry cancellationToken =
        task {
            lock entries (fun () -> entries.Add(entry))
            return! inner entry cancellationToken
        }

    let session = spawnSessionFull system store created.Id runner settled journaled

    try
        prompt store created.Id session "first" |> ignore

        let running =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                client.Calls = 1 && (snapshotOf session).State = SessionState.Running)

        running |> should equal true

        // Queued before the interrupt: survives the abort and drains after it.
        prompt store created.Id session "second" |> ignore

        promptWithDelivery store created.Id session "stop that" DeliveryMode.Interrupt
        |> ignore

        let drained =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                settled.Count = 3 && (snapshotOf session).State = SessionState.Idle)

        drained |> should equal true

        // The pre-empted turn settles once under the winning cause, then
        // the Interrupt entry drains first with the queued entry intact
        // behind it.
        settled[0].Status |> should equal TurnStatus.Aborted

        match settled[0].Outcome with
        | :? TurnAborted as aborted ->
            aborted.Cause |> should equal StopCause.ExplicitAbort
            aborted.Reason |> should equal SessionActor.InterruptReason
        | _ -> failwith "Expected a TurnAborted outcome."

        settled[1].Status |> should equal TurnStatus.Completed
        settled[1].AssistantText |> should equal "after interrupt"
        settled[2].Status |> should equal TurnStatus.Completed
        settled[2].AssistantText |> should equal "after interrupt"

        lock entries (fun () -> entries |> Seq.map entryText |> List.ofSeq)
        |> should equal [ "first"; "stop that"; "second" ]

        client.Calls |> should equal 3
        (pendingOf store created.Id).Count |> should equal 0
    finally
        stopSystem system

[<Fact>]
let ``Interrupt while Idle starts a turn normally`` () =
    use system = createSystem ()
    let store = createStore ()
    let created = createSession store
    let runner = ScriptedRunner([ "stopped" ])
    let session = spawnSession system store created.Id runner.Func

    try
        let entry =
            promptWithDelivery store created.Id session "stop" DeliveryMode.Interrupt

        entry.Delivery |> should equal DeliveryMode.Interrupt

        let drained =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                runner.Calls = 1 && (snapshotOf session).State = SessionState.Idle)

        drained |> should equal true
        runner.Entries[0].Position |> should equal entry.Position
        entryText runner.Entries[0] |> should equal "stop"
        (pendingOf store created.Id).Count |> should equal 0
    finally
        stopSystem system

[<Fact>]
let ``Inject and Interrupt while WaitingForInput append and wait`` () =
    use system = createSystem ()
    let store = createStore ()
    let created = createSession store

    store
        .UpdateSessionState(tenant, created.Id, SessionState.WaitingForInput, CancellationToken.None)
        .GetAwaiter()
        .GetResult()
    |> ignore

    let runner = ScriptedRunner([ "waiting" ])
    let session = spawnSession system store created.Id runner.Func

    try
        (snapshotOf session).State |> should equal SessionState.WaitingForInput

        promptWithDelivery store created.Id session "steer" DeliveryMode.Inject
        |> ignore

        promptWithDelivery store created.Id session "stop" DeliveryMode.Interrupt
        |> ignore

        (snapshotOf session).State |> should equal SessionState.WaitingForInput
        (pendingOf store created.Id).Count |> should equal 2

        let quiet =
            Task.WhenAny(Task.Delay(TimeSpan.FromMilliseconds 500.0))
            |> fun delay -> delay.GetAwaiter().GetResult()

        quiet |> ignore
        runner.Calls |> should equal 0
    finally
        stopSystem system

[<Fact>]
let ``Settle drains Interrupt first then Queue-plus-Inject in position order, Reply never`` () =
    use system = createSystem ()
    let store = createStore ()
    let created = createSession store

    // Orphaned inbox in tier-shuffling position order: the Queue entry is
    // oldest, the Interrupt newest, and a Reply entry must never drain.
    ignore (appendStoredWith store created.Id "queued" DeliveryMode.Queue)
    ignore (appendStoredWith store created.Id "steered" DeliveryMode.Inject)
    let interrupt = appendStoredWith store created.Id "stopped" DeliveryMode.Interrupt

    let replyPayload =
        ReplyPayload(PermissionDecision("req-1", PermissionDecisionKind.AllowOnce)) :> InboxPayload

    let replyEntry =
        store
            .AppendInboxMessage(tenant, created.Id, replyPayload, DeliveryMode.Queue, CancellationToken.None)
            .GetAwaiter()
            .GetResult()

    let runner = ScriptedRunner([ "one"; "two"; "three"; "four" ])
    let session = spawnSession system store created.Id runner.Func

    try
        (snapshotOf session).State |> should equal SessionState.Idle
        (snapshotOf session).PendingCount |> should equal 4

        prompt store created.Id session "trigger" |> ignore

        let drained =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () ->
                runner.Calls = 4 && (snapshotOf session).State = SessionState.Idle)

        drained |> should equal true

        // Interrupt tier first in position order, then Queue-plus-Inject
        // in position order; the Reply entry never starts a turn.
        runner.Entries
        |> Seq.map entryText
        |> List.ofSeq
        |> should
            equal
            [
                "stopped"
                "queued"
                "steered"
                "trigger"
            ]

        runner.Entries[0].Position |> should equal interrupt.Position

        let pending = pendingOf store created.Id
        pending.Count |> should equal 1
        pending[0].Position |> should equal replyEntry.Position

        match pending[0].Payload with
        | :? ReplyPayload -> ()
        | _ -> failwith "Expected the remaining pending entry to be the Reply."
    finally
        stopSystem system

// ──────────────────────────────────────────────────────────────────────────
// Fold projection for folded Inject messages (issue 34)

[<Fact>]
let ``Fold derives one User cell per matching-turn UserMessageEvent`` () =
    let sessionId = SessionId.New()
    let turnId = TurnId.New()
    let eventStamp = DateTimeOffset(2024, 5, 6, 7, 8, 9, TimeSpan.Zero)
    let noSequence = Unchecked.defaultof<Nullable<int64>>

    let metadata = Dictionary<string, string>()
    metadata["source"] <- "inject"

    let parts =
        ResizeArray<AIContent>(
            [|
                TextContent("first") :> AIContent
                TextContent("second") :> AIContent
            |]
        )
        :> IReadOnlyList<AIContent>

    let injected =
        UserMessageEvent(sessionId, turnId, noSequence, eventStamp, UserMessage(parts, metadata))

    let cells =
        SessionCellDeriver.Fold(sessionId, turnId, null, eventStamp, [ injected :> SessionEvent ])

    cells.Count |> should equal 1
    cells[0].Kind |> should equal SessionCellKind.User
    cells[0].Content |> should equal "first\nsecond"
    cells[0].Iteration |> should equal 0
    cells[0].Timestamp |> should equal eventStamp
    cells[0].IsError |> should equal false
    cells[0].SessionId |> should equal sessionId
    cells[0].TurnId |> should equal turnId
    cells[0].ToolName |> should equal null
    cells[0].ToolCallId |> should equal null
    cells[0].Artifacts |> should equal null

    match cells[0].Metadata with
    | null -> failwith "injected message metadata was lost"
    | meta -> meta["source"] |> should equal "inject"

[<Fact>]
let ``Fold ignores UserMessageEvents from other turns`` () =
    let sessionId = SessionId.New()
    let turnId = TurnId.New()
    let eventStamp = DateTimeOffset(2024, 5, 6, 7, 8, 9, TimeSpan.Zero)
    let noSequence = Unchecked.defaultof<Nullable<int64>>

    let foreign =
        UserMessageEvent(sessionId, TurnId.New(), noSequence, eventStamp, UserMessage.Text "other")

    let cells =
        SessionCellDeriver.Fold(sessionId, turnId, null, eventStamp, [ foreign :> SessionEvent ])

    cells.Count |> should equal 0

[<Fact>]
let ``Fold orders the initial user cell before the injected one`` () =
    let sessionId = SessionId.New()
    let turnId = TurnId.New()
    let eventStamp = DateTimeOffset(2024, 5, 6, 7, 8, 9, TimeSpan.Zero)
    let noSequence = Unchecked.defaultof<Nullable<int64>>

    let injected =
        UserMessageEvent(sessionId, turnId, noSequence, eventStamp, UserMessage.Text "steer")

    let cells =
        SessionCellDeriver.Fold(sessionId, turnId, UserMessage.Text "hello", eventStamp, [ injected :> SessionEvent ])

    cells.Count |> should equal 2
    cells[0].Content |> should equal "hello"
    cells[1].Content |> should equal "steer"
    cells[1].Kind |> should equal SessionCellKind.User
    cells[1].Iteration |> should equal 0

// ──────────────────────────────────────────────────────────────────────────
// Compaction wiring (issue 45)

/// Fresh session + event stores over one database so appends fence on the
/// session store's live claim token.
let private createJournalStores () =
    let database = InMemoryDatabase()
    (InMemorySessionStore(database) :> ISessionStore, InMemorySessionEventStore(database) :> ISessionEventStore)

/// Claims the next turn, failing the test unless the store grants it.
let private claimTurn (store: ISessionStore) (sessionId: SessionId) (owner: string) : TurnClaim =
    match
        store.ClaimNextTurn(tenant, sessionId, owner, TimeSpan.FromSeconds 120.0, CancellationToken.None)
        |> fun task -> task.GetAwaiter().GetResult()
    with
    | :? TurnLeaseRenewed as renewed -> renewed.Claim
    | state -> failwith $"Expected a granted claim, observed %s{state.GetType().Name}."

let private compactionEntry () : ModelCatalogEntry =
    {
        Model = ModelReference.Parse "test/session-model"
        ContextWindowTokens = 1500
        ReservedOutputTokens = 200
        MaxOutputTokens = 200
        Capabilities =
            {
                Streaming = true
                Reasoning = false
                ToolCalling = true
            }
    }

type private FakeCompactionCatalog(entry: ModelCatalogEntry | null) =
    interface ILlmModelCatalog with
        member _.GetEntry(_) = entry
        member _.HasEntry(_) = not (isNull (box entry))

type private DenyCompactionPolicy(message: string) =
    interface IModelPolicy with
        member _.Authorize(_, _, _) =
            ModelDenied(message) :> ModelPolicyDecision

/// Six 1000-char turns after a system message: over the wiring's 1200
/// threshold, compacted to four messages with keep two.
let private compactionHistory () : IList<ChatMessage> =
    ResizeArray<ChatMessage>(
        [|
            ChatMessage(ChatRole.System, "sys")
            ChatMessage(ChatRole.User, String('x', 1000))
            ChatMessage(ChatRole.Assistant, String('x', 1000))
            ChatMessage(ChatRole.User, String('x', 1000))
            ChatMessage(ChatRole.Assistant, String('x', 1000))
            ChatMessage(ChatRole.User, String('x', 1000))
            ChatMessage(ChatRole.Assistant, String('x', 1000))
        |]
    )
    :> IList<ChatMessage>

let private buildWiring
    (sessionId: SessionId)
    (client: IChatClient)
    (policy: IModelPolicy | null)
    (journal: ISessionEventStore)
    (token: string)
    : SessionActor.CompactionWiring =
    let llm = LlmOptions()
    llm.CompactionKeepMessages <- 2

    {
        Llm = llm
        ReservedBufferTokens = 100
        SessionModel = ModelReference.Parse "test/session-model"
        Catalog = FakeCompactionCatalog(compactionEntry ()) :> ILlmModelCatalog
        Client = client
        Observer = Unchecked.defaultof<IUsageObserver>
        Policy = policy
        Tenant = tenant
        SessionId = sessionId
        TurnId = TurnId.New()
        Attempt = 1
        EventStore = journal
        JournalToken = token
        IsLeaseValid = (fun () -> true)
    }

let private replayEvents (journal: ISessionEventStore) (sessionId: SessionId) : IReadOnlyList<SessionEvent> =
    match journal.Replay(tenant, sessionId, 0L, 10, CancellationToken.None).GetAwaiter().GetResult() with
    | :? EventReplayPage as page -> page.Events
    | _ -> failwith "Expected the compacted journal page."

[<Fact>]
let ``Compaction wiring compacts and journals CompactedEvent under the token`` () =
    let store, journal = createJournalStores ()
    let session = createSession store
    appendStored store session.Id "run" |> ignore
    let claim = claimTurn store session.Id "owner-a"

    let client =
        new ScriptedChatClient(
            ResizeArray<ScriptStep>(
                [|
                    ScriptStep.Text("wiring gist", 4L, 6L)
                |]
            )
        )

    let history = compactionHistory ()

    let hook =
        SessionActor.buildCompactionHook (
            buildWiring session.Id (client :> IChatClient) Unchecked.defaultof<IModelPolicy> journal claim.Token
        )

    let inputTokens, outputTokens =
        hook history 0L 0L CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()

    inputTokens |> should equal 4L
    outputTokens |> should equal 6L

    // The rewrite landed: system, marked summary, last two.
    history.Count |> should equal 4
    history[0].Text |> should equal "sys"

    (history[1].Text.StartsWith(Compaction.SummaryMarker, StringComparison.Ordinal))
    |> should equal true

    (history[1].Text.Contains("wiring gist")) |> should equal true

    let events = replayEvents journal session.Id
    events.Count |> should equal 1

    let compacted = events[0] :?> CompactedEvent
    (compacted.BeforeEstimate > compacted.AfterEstimate) |> should equal true

[<Fact>]
let ``Compaction wiring continues on denial with CompactionFailedEvent`` () =
    let store, journal = createJournalStores ()
    let session = createSession store
    appendStored store session.Id "run" |> ignore
    let claim = claimTurn store session.Id "owner-a"

    let client = new ScriptedChatClient(ResizeArray<ScriptStep>([||]))
    let history = compactionHistory ()

    let hook =
        SessionActor.buildCompactionHook (
            buildWiring
                session.Id
                (client :> IChatClient)
                (DenyCompactionPolicy("quota spent") :> IModelPolicy)
                journal
                claim.Token
        )

    let inputTokens, outputTokens =
        hook history 0L 0L CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()

    inputTokens |> should equal 0L
    outputTokens |> should equal 0L
    client.Calls |> should equal 0
    history.Count |> should equal 7

    let events = replayEvents journal session.Id
    events.Count |> should equal 1

    let failed = events[0] :?> CompactionFailedEvent
    failed.Reason |> should equal "quota spent"

// ──────────────────────────────────────────────────────────────────────────
// On-demand Compact (issue 46)

/// Compacts through the client boundary and blocks for the reply.
let private compact (store: ISessionStore) (sessionId: SessionId) (session: IActorRef) : SessionCompactReply =
    SessionActor.compactAsync store tenant sessionId session CancellationToken.None
    |> fun task -> task.GetAwaiter().GetResult()

/// Builds on-demand compaction dependencies sharing the force cell with
/// the turn's force-aware hook.
let private compactDeps
    (client: IChatClient)
    (policy: IModelPolicy | null)
    (journal: ISessionEventStore)
    (token: string)
    (force: Compaction.CompactForce)
    : CompactDeps =
    let llm = LlmOptions()
    llm.CompactionKeepMessages <- 2

    {
        Llm = llm
        ReservedBufferTokens = 100
        SessionModel = ModelReference.Parse "test/session-model"
        Catalog = FakeCompactionCatalog(compactionEntry ()) :> ILlmModelCatalog
        Client = client
        Observer = Unchecked.defaultof<IUsageObserver>
        Policy = policy
        EventStore = journal
        JournalToken = token
        Force = force
    }

/// Spawns a session actor carrying on-demand compaction dependencies.
let private spawnSessionWithCompact
    (system: ActorSystem)
    (store: ISessionStore)
    (sessionId: SessionId)
    (runTurn: InboxEntry -> CancellationToken -> Task<TurnResult>)
    (compact: CompactDeps)
    : IActorRef =
    let props: SessionActorProps =
        {
            Store = store
            Tenant = tenant
            SessionId = sessionId
            RunTurn = runTurn
            OnTurnSettled = None
            OnInjectJournaled = None
            Logger = null
            Compact = Some compact
        }

    spawn system $"test-{Guid.NewGuid():N}" (SessionActor.behavior props)

/// Seeds the journal with one user message per text under the token.
let private seedJournal
    (journal: ISessionEventStore)
    (sessionId: SessionId)
    (token: string)
    (texts: string list)
    : unit =
    let events =
        ResizeArray<SessionEvent>(
            [|
                for text in texts ->
                    UserMessageEvent(
                        sessionId,
                        TurnId.New(),
                        Nullable<int64>(),
                        DateTimeOffset.UtcNow,
                        UserMessage.Text(text)
                    )
                    :> SessionEvent
            |]
        )
        :> IReadOnlyList<SessionEvent>

    match
        JournalWriter.appendWithTokenAsync journal tenant sessionId token events CancellationToken.None
        |> fun task -> task.GetAwaiter().GetResult()
    with
    | JournalWriter.JournalAppended _ -> ()
    | JournalWriter.JournalRejected rejection ->
        failwith $"Expected the seed append to land, observed rejection: %s{rejection}."
    | JournalWriter.JournalFailed failure ->
        failwith $"Expected the seed append to land, observed failure: %s{failure}."

/// Six 1000-char user messages: over the 1200 wiring threshold, so an
/// Idle compact summarises.
let private overThresholdTexts () : string list = [ for _ in 1..6 -> String('x', 1000) ]

[<Fact>]
let ``Compact on Idle replays the journal and compacts without starting a turn`` () =
    use system = createSystem ()
    let store, journal = createJournalStores ()
    let created = createSession store
    appendStored store created.Id "run" |> ignore
    let claim = claimTurn store created.Id "owner-a"
    seedJournal journal created.Id claim.Token (overThresholdTexts ())

    let client =
        new ScriptedChatClient(
            ResizeArray<ScriptStep>(
                [|
                    ScriptStep.Text("idle gist", 4L, 6L)
                |]
            )
        )

    let deps =
        compactDeps
            (client :> IChatClient)
            Unchecked.defaultof<IModelPolicy>
            journal
            claim.Token
            (Compaction.CompactForce())

    let session =
        spawnSessionWithCompact system store created.Id (fun _ _ -> Task.FromResult(completed "unused")) deps

    try
        match compact store created.Id session with
        | CompactCompleted(beforeEstimate, afterEstimate) -> (beforeEstimate > afterEstimate) |> should equal true
        | reply -> failwith $"Expected CompactCompleted, observed %A{reply}."

        // No turn started: the actor and the store stayed Idle with no
        // running entry.
        let snapshot = snapshotOf session
        snapshot.State |> should equal SessionState.Idle
        snapshot.RunningPosition.IsNone |> should equal true
        (storedOf store created.Id).State |> should equal SessionState.Idle

        // Six seeded messages plus the one CompactedEvent, and exactly one
        // summariser call.
        let events = replayEvents journal created.Id
        events.Count |> should equal 7
        (events[6] :? CompactedEvent) |> should equal true
        client.Calls |> should equal 1
    finally
        stopSystem system

[<Fact>]
let ``Compact on Idle under threshold makes no summariser call`` () =
    use system = createSystem ()
    let store, journal = createJournalStores ()
    let created = createSession store
    appendStored store created.Id "run" |> ignore
    let claim = claimTurn store created.Id "owner-a"
    seedJournal journal created.Id claim.Token [ "hello" ]

    let client = new ScriptedChatClient(ResizeArray<ScriptStep>([||]))

    let deps =
        compactDeps
            (client :> IChatClient)
            Unchecked.defaultof<IModelPolicy>
            journal
            claim.Token
            (Compaction.CompactForce())

    let session =
        spawnSessionWithCompact system store created.Id (fun _ _ -> Task.FromResult(completed "unused")) deps

    try
        match compact store created.Id session with
        | CompactNotNeeded -> ()
        | reply -> failwith $"Expected CompactNotNeeded, observed %A{reply}."

        client.Calls |> should equal 0
        (replayEvents journal created.Id).Count |> should equal 1
        (snapshotOf session).State |> should equal SessionState.Idle
    finally
        stopSystem system

[<Fact>]
let ``Compact on Idle with a denied model journals CompactionFailedEvent and continues`` () =
    use system = createSystem ()
    let store, journal = createJournalStores ()
    let created = createSession store
    appendStored store created.Id "run" |> ignore
    let claim = claimTurn store created.Id "owner-a"
    seedJournal journal created.Id claim.Token (overThresholdTexts ())

    let client = new ScriptedChatClient(ResizeArray<ScriptStep>([||]))

    let deps =
        compactDeps
            (client :> IChatClient)
            (DenyCompactionPolicy("quota spent") :> IModelPolicy)
            journal
            claim.Token
            (Compaction.CompactForce())

    let session =
        spawnSessionWithCompact system store created.Id (fun _ _ -> Task.FromResult(completed "unused")) deps

    try
        match compact store created.Id session with
        | CompactNotNeeded -> ()
        | reply -> failwith $"Expected CompactNotNeeded, observed %A{reply}."

        client.Calls |> should equal 0

        let events = replayEvents journal created.Id
        events.Count |> should equal 7

        let failed = events[6] :?> CompactionFailedEvent
        failed.Reason |> should equal "quota spent"
        (snapshotOf session).State |> should equal SessionState.Idle
    finally
        stopSystem system

[<Fact>]
let ``Compact on Idle with a stale claim journals nothing`` () =
    use system = createSystem ()
    let store, journal = createJournalStores ()
    let created = createSession store
    appendStored store created.Id "run" |> ignore
    let claim = claimTurn store created.Id "owner-a"
    seedJournal journal created.Id claim.Token (overThresholdTexts ())

    let client =
        new ScriptedChatClient(ResizeArray<ScriptStep>([| ScriptStep.Text("loser gist") |]))

    // The takeover winner re-claimed elsewhere: these dependencies carry a
    // token the store no longer honors.
    let deps =
        compactDeps
            (client :> IChatClient)
            Unchecked.defaultof<IModelPolicy>
            journal
            "bogus-token"
            (Compaction.CompactForce())

    let session =
        spawnSessionWithCompact system store created.Id (fun _ _ -> Task.FromResult(completed "unused")) deps

    try
        match compact store created.Id session with
        | CompactFenced -> ()
        | reply -> failwith $"Expected CompactFenced, observed %A{reply}."

        // The summariser ran (the fence checks at the last moment before
        // the journal write), but the loser journaled nothing.
        client.Calls |> should equal 1
        (replayEvents journal created.Id).Count |> should equal 6
        (snapshotOf session).State |> should equal SessionState.Idle
    finally
        stopSystem system

[<Fact>]
let ``Compact on Running defers to the next boundary exactly once`` () =
    use system = createSystem ()
    let store, journal = createJournalStores ()
    let created = createSession store
    appendStored store created.Id "run" |> ignore
    let claim = claimTurn store created.Id "owner-a"

    let client =
        new ScriptedChatClient(ResizeArray<ScriptStep>([| ScriptStep.Text("running gist") |]))

    let force = Compaction.CompactForce()

    let deps =
        compactDeps (client :> IChatClient) Unchecked.defaultof<IModelPolicy> journal claim.Token force

    // The scripted turn honors the shared force cell at two scripted
    // boundaries, then settles: the armed request fires once.
    let gate = new TaskCompletionSource<unit>()

    let runTurn _ _ =
        task {
            do! gate.Task

            let hook =
                SessionActor.buildForcedCompactionHook
                    force
                    (buildWiring
                        created.Id
                        (client :> IChatClient)
                        Unchecked.defaultof<IModelPolicy>
                        journal
                        claim.Token)

            let history =
                ResizeArray<ChatMessage>(
                    [|
                        ChatMessage(ChatRole.User, "alpha")
                        ChatMessage(ChatRole.Assistant, "beta")
                        ChatMessage(ChatRole.User, "gamma")
                    |]
                )
                :> IList<ChatMessage>

            let! _ = hook history 0L 0L CancellationToken.None
            let! _ = hook history 0L 0L CancellationToken.None
            return completed "done"
        }

    let session = spawnSessionWithCompact system store created.Id runTurn deps

    try
        prompt store created.Id session "go" |> ignore

        let running =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () -> (snapshotOf session).State = SessionState.Running)

        running |> should equal true

        match compact store created.Id session with
        | CompactDeferred -> ()
        | reply -> failwith $"Expected CompactDeferred, observed %A{reply}."

        gate.TrySetResult() |> ignore

        let drained =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () -> (snapshotOf session).State = SessionState.Idle)

        drained |> should equal true

        // One summariser call across both boundaries: the first consumed
        // the armed flag, the second found it spent.
        client.Calls |> should equal 1

        let events = replayEvents journal created.Id
        events.Count |> should equal 1
        (events[0] :? CompactedEvent) |> should equal true
    finally
        stopSystem system

[<Fact>]
let ``Compact on WaitingForInput no-ops without touching the journal`` () =
    use system = createSystem ()
    let store, journal = createJournalStores ()
    let created = createSession store
    appendStored store created.Id "run" |> ignore
    let claim = claimTurn store created.Id "owner-a"
    seedJournal journal created.Id claim.Token [ "parked" ]

    store
        .UpdateSessionState(tenant, created.Id, SessionState.WaitingForInput, CancellationToken.None)
        .GetAwaiter()
        .GetResult()
    |> ignore

    let client = new ScriptedChatClient(ResizeArray<ScriptStep>([||]))

    let deps =
        compactDeps
            (client :> IChatClient)
            Unchecked.defaultof<IModelPolicy>
            journal
            claim.Token
            (Compaction.CompactForce())

    let session =
        spawnSessionWithCompact system store created.Id (fun _ _ -> Task.FromResult(completed "unused")) deps

    try
        match compact store created.Id session with
        | CompactNotNeeded -> ()
        | reply -> failwith $"Expected CompactNotNeeded, observed %A{reply}."

        client.Calls |> should equal 0
        (replayEvents journal created.Id).Count |> should equal 1
        (snapshotOf session).State |> should equal SessionState.WaitingForInput
    finally
        stopSystem system

[<Fact>]
let ``Compact without configured wiring no-ops`` () =
    use system = createSystem ()
    let store = createStore ()
    let created = createSession store

    let session =
        spawnSession system store created.Id (fun _ _ -> Task.FromResult(completed "unused"))

    try
        match compact store created.Id session with
        | CompactNotNeeded -> ()
        | reply -> failwith $"Expected CompactNotNeeded, observed %A{reply}."

        (snapshotOf session).State |> should equal SessionState.Idle
    finally
        stopSystem system

[<Fact>]
let ``Compact on Closed throws InvalidSessionStateException at the boundary`` () =
    use system = createSystem ()
    let store = createStore ()
    let created = createSession store

    let session =
        spawnSession system store created.Id (fun _ _ -> Task.FromResult(completed "unused"))

    try
        close store created.Id session |> ignore

        let ex =
            Assert.Throws<InvalidSessionStateException>(fun () -> compact store created.Id session |> ignore)

        ex.SessionId |> should equal created.Id
        ex.CurrentState |> should equal (SessionState.Closed.ToString())
    finally
        stopSystem system

// ───────────────────────────────────────────────────────────────────────────
// AutoClose (issue 82)

/// Creates a session row with AutoClose set, mirroring createSession.
let private createAutoCloseSession (store: ISessionStore) : Session =
    let options = SessionOptions()
    options.AutoClose <- true

    let template = sampleSession ()
    let session = { template with Options = options }

    store.CreateSession(tenant, session, CancellationToken.None).GetAwaiter().GetResult()

/// An Aborted TurnResult as a settled report: the carried result stands.
let private abortedResult (text: string) =
    { completed text with
        Status = TurnStatus.Aborted
        Outcome = TurnAborted(StopCause.ExplicitAbort, "host stop") :> TurnOutcome
    }

/// A Failed TurnResult as a settled report.
let private failedTurnResult (text: string) (reason: string) =
    { completed text with
        Status = TurnStatus.Failed
        Outcome = TurnFailed(reason) :> TurnOutcome
    }

/// Turn runner answering every turn with one fixed result.
type FixedRunner(result: TurnResult) =
    let mutable calls = 0

    /// How many turns ran.
    member _.Calls = calls

    /// Runs one turn for an entry.
    member _.Run(_entry: InboxEntry, _cancellationToken: CancellationToken) : Task<TurnResult> =
        calls <- calls + 1
        Task.FromResult(result)

    /// The runner as the actor's delegate.
    member this.Func: (InboxEntry -> CancellationToken -> Task<TurnResult>) =
        fun entry cancellationToken -> this.Run(entry, cancellationToken)

[<Fact>]
let ``AutoClose closes the session after the first Completed turn`` () =
    use system = createSystem ()
    let store = createStore ()
    let created = createAutoCloseSession store
    let runner = ScriptedRunner([ "hello" ])
    let session = spawnSession system store created.Id runner.Func

    try
        prompt store created.Id session "hello" |> ignore

        let closed =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () -> (snapshotOf session).State = SessionState.Closed)

        closed |> should equal true
        (storedOf store created.Id).State |> should equal SessionState.Closed
        (pendingOf store created.Id).Count |> should equal 0

        // A prompt racing the close rejects at the boundary.
        let ex =
            Assert.Throws<InvalidSessionStateException>(fun () -> prompt store created.Id session "late" |> ignore)

        ex.SessionId |> should equal created.Id
    finally
        stopSystem system

[<Fact>]
let ``AutoClose leaves an Aborted turn open`` () =
    use system = createSystem ()
    let store = createStore ()
    let created = createAutoCloseSession store
    let runner = FixedRunner(abortedResult "stopped")
    let session = spawnSession system store created.Id runner.Func

    try
        prompt store created.Id session "stop me" |> ignore

        let settled =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () -> (snapshotOf session).State = SessionState.Idle)

        settled |> should equal true
        (storedOf store created.Id).State |> should equal SessionState.Idle
        (pendingOf store created.Id).Count |> should equal 0
    finally
        stopSystem system

[<Fact>]
let ``AutoClose leaves a Failed turn open`` () =
    use system = createSystem ()
    let store = createStore ()
    let created = createAutoCloseSession store
    let runner = FixedRunner(failedTurnResult "" "the tool exploded")
    let session = spawnSession system store created.Id runner.Func

    try
        prompt store created.Id session "break me" |> ignore

        let settled =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () -> (snapshotOf session).State = SessionState.Idle)

        settled |> should equal true
        (storedOf store created.Id).State |> should equal SessionState.Idle
        (pendingOf store created.Id).Count |> should equal 0
    finally
        stopSystem system

[<Fact>]
let ``AutoClose wins over a queued second prompt`` () =
    use system = createSystem ()
    let store = createStore ()
    let created = createAutoCloseSession store
    let runner = GatedRunner([ "first"; "second" ])
    let session = spawnSession system store created.Id runner.Func

    try
        prompt store created.Id session "first" |> ignore

        let running =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () -> (snapshotOf session).State = SessionState.Running)

        running |> should equal true

        prompt store created.Id session "second" |> ignore
        runner.Release()

        let closed =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () -> (snapshotOf session).State = SessionState.Closed)

        closed |> should equal true
        // The settle-then-close sequencing never started the second turn.
        runner.Calls |> should equal 1
        (storedOf store created.Id).State |> should equal SessionState.Closed
        (pendingOf store created.Id).Count |> should equal 1
    finally
        stopSystem system

[<Fact>]
let ``AutoClose closes a suspendable session after its first Completed turn`` () =
    use system = createSystem ()
    let store = createStore ()
    let created = createAutoCloseSession store
    let journal = RecordingEventStore()

    let runner =
        ScriptSuspendRunner(settledCompletion "done", settledCompletion "never")

    let settled = ResizeArray<TurnResult>()

    let session =
        spawnSuspendable
            system
            store
            journal
            (TurnLoopTests.NeverDelay() :> ILlmDelay)
            (TimeSpan.FromMinutes 5.0)
            created.Id
            runner
            settled

    try
        promptSuspendable store created.Id session "run" |> ignore

        let closed =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () -> (storedOf store created.Id).State = SessionState.Closed)

        closed |> should equal true
        settled.Count |> should equal 1
        settled[0].AssistantText |> should equal "done"
        (pendingOf store created.Id).Count |> should equal 0
    finally
        stopSystem system

// ──────────────────────────────────────────────────────────────────────────
// Completion outbox (issue 84)

/// Recording completion sink: keeps every Notify payload in call order.
type FakeCompletionSink() =
    let completions = ResizeArray<SessionCompletion>()

    interface ISessionCompletionSink with
        member _.Notify(completion: SessionCompletion) = completions.Add(completion)

    /// Every Notify payload, in call order.
    member _.Completions: IReadOnlyList<SessionCompletion> =
        completions :> IReadOnlyList<SessionCompletion>

/// Creates a session row carrying the completion sink, mirroring
/// createAutoCloseSession.
let private createSinkSession (store: ISessionStore) (sink: ISessionCompletionSink) : Session =
    let options = SessionOptions(CompletionSink = sink)

    let template = sampleSession ()
    let session = { template with Options = options }

    store.CreateSession(tenant, session, CancellationToken.None).GetAwaiter().GetResult()

[<Fact>]
let ``Settlement writes the outbox row in the same step under the shared inline key`` () =
    use system = createSystem ()
    let store = createStore ()
    let sink = FakeCompletionSink()
    let created = createSinkSession store (sink :> ISessionCompletionSink)
    let runner = ScriptedRunner([ "hello" ])
    let session = spawnSession system store created.Id runner.Func

    try
        prompt store created.Id session "hello" |> ignore

        let settled =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () -> (snapshotOf session).State = SessionState.Idle)

        settled |> should equal true

        // The settlement step consumed the entry, stored the outbox row,
        // and notified inline together: no intermediate state is
        // observable afterwards.
        (pendingOf store created.Id).Count |> should equal 0
        sink.Completions.Count |> should equal 1

        let rows =
            store
                .ClaimCompletionOutbox("probe", 10, TimeSpan.FromMinutes 5., CancellationToken.None)
                .GetAwaiter()
                .GetResult()

        rows.Count |> should equal 1
        rows[0].IdempotencyKey |> should equal sink.Completions[0].IdempotencyKey
        rows[0].Completion.TurnResult.Status |> should equal TurnStatus.Completed
        rows[0].Delivered |> should equal false

        let delivered = sink.Completions[0]
        delivered.SessionId |> should equal created.Id
        String.IsNullOrWhiteSpace(delivered.IdempotencyKey) |> should equal false
    finally
        stopSystem system

[<Fact>]
let ``Settlement without a sink stores no outbox row`` () =
    use system = createSystem ()
    let store = createStore ()
    let created = createSession store
    let runner = ScriptedRunner([ "hello" ])
    let session = spawnSession system store created.Id runner.Func

    try
        prompt store created.Id session "hello" |> ignore

        let settled =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () -> (snapshotOf session).State = SessionState.Idle)

        settled |> should equal true

        let rows =
            store
                .ClaimCompletionOutbox("probe", 10, TimeSpan.FromMinutes 5., CancellationToken.None)
                .GetAwaiter()
                .GetResult()

        rows.Count |> should equal 0
    finally
        stopSystem system

// ──────────────────────────────────────────────────────────────────────────
// Logging scopes (issue 93)

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

    interface Microsoft.Extensions.Logging.ILogger with
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
            (
                logLevel: Microsoft.Extensions.Logging.LogLevel,
                _eventId: Microsoft.Extensions.Logging.EventId,
                state: 'TState,
                ex: exn,
                formatter: Func<'TState, exn, string>
            ) : unit =
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

[<Fact>]
let ``Session actor prompt and settle carry all six scopes and leak no secret`` () =
    use system = createSystem ()
    let store = createStore ()
    let created = createSession store
    let logger = ScopeCapturingLogger()
    let runner = ScriptedRunner([ "done" ])

    let props: SessionActorProps =
        {
            Store = store
            Tenant = tenant
            SessionId = created.Id
            RunTurn = runner.Func
            OnTurnSettled = None
            OnInjectJournaled = None
            Logger = logger :> Microsoft.Extensions.Logging.ILogger
            Compact = None
        }

    let session = spawn system $"test-{Guid.NewGuid():N}" (SessionActor.behavior props)
    let secret = "sk-ant-session-secret-11111111"

    try
        prompt store created.Id session $"hello {secret}" |> ignore

        let idle =
            waitFor (TimeSpan.FromSeconds 10.0) (fun () -> (snapshotOf session).State = SessionState.Idle)

        idle |> should equal true

        let entries = logger.Entries
        entries |> should not' (equal [])

        for entry in entries do
            for key in
                [
                    LoggingScopes.SessionIdKey
                    LoggingScopes.TurnIdKey
                    LoggingScopes.AgentIdKey
                    LoggingScopes.TenantIdKey
                    LoggingScopes.AttemptKey
                    LoggingScopes.ClaimOwnerKey
                ] do
                entry.Scopes |> List.exists (fun (name, _) -> name = key) |> should equal true

            entry.Text.Contains(secret) |> should equal false
    finally
        stopSystem system
