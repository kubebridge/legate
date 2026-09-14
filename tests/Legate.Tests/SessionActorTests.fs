// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.SessionActorTests

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open Akka.Actor
open Akka.FSharp
open FsUnit.Xunit
open Legate
open Legate.Storage.InMemory
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
