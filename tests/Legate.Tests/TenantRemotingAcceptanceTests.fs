// SPDX-License-Identifier: Apache-2.0
module Legate.Tests.TenantRemotingAcceptanceTests

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Diagnostics
open System.IO
open System.Linq
open System.Net
open System.Net.Sockets
open System.Reflection
open System.Runtime.ExceptionServices
open System.Runtime.CompilerServices
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open Akka.Actor
open Akka.Cluster
open Akka.Cluster.Sharding
open Akka.FSharp
open Legate
open Legate.Storage.Sqlite
open Legate.Testing
open Microsoft.Extensions.AI
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Options
open Xunit

// The acceptance rig deliberately does not use UseSqlite: that registration
// owns one live database per provider. These proxies open the real SQLite
// backend only for the duration of each async store call, which lets two
// production DI graphs in this process safely exercise the same durable file.

type private StoreProxyCounters() =
    let invocations = ConcurrentDictionary<string, int>(StringComparer.Ordinal)
    let mutations = ConcurrentDictionary<string, int>(StringComparer.Ordinal)

    let mutationPrefixes =
        [|
            "Create"
            "Update"
            "Close"
            "Grant"
            "Set"
            "Append"
            "Mark"
            "Claim"
            "Renew"
            "Observe"
            "Verify"
            "Checkpoint"
            "Settle"
            "Abort"
            "Enqueue"
            "Complete"
            "Purge"
            "TryRecover"
            "RequestHost"
            "BindControl"
            "CheckControl"
            "TryDecide"
            "Retire"
            "Defer"
        |]

    member _.Record(methodName: string) =
        invocations.AddOrUpdate(methodName, 1, fun _ count -> count + 1) |> ignore

        if mutationPrefixes |> Array.exists methodName.StartsWith then
            mutations.AddOrUpdate(methodName, 1, fun _ count -> count + 1) |> ignore

    member _.MutationCount = mutations.Values |> Seq.sum

    member _.Snapshot() =
        invocations |> Seq.map (fun pair -> pair.Key, pair.Value) |> dict

type private StoreProxyKind =
    | Session
    | Event

type private CombinedSessionStore =
    interface
        inherit ISessionStore
        inherit ISessionAbortControlStore
        inherit ISessionSettlementStore
    end

type private OnDemandStoreProxy() =
    inherit DispatchProxy()

    static let pathGates =
        ConcurrentDictionary<string, SemaphoreSlim>(StringComparer.OrdinalIgnoreCase)

    let mutable path = ""
    let mutable kind = Session
    let mutable counters = Unchecked.defaultof<StoreProxyCounters>

    member this.Configure(databasePath: string, proxyKind: StoreProxyKind, observed: StoreProxyCounters) =
        path <- Path.GetFullPath databasePath
        kind <- proxyKind
        counters <- observed

    member _.DatabasePath = path

    static member private Finish(pending: Task, database: SqliteDatabase, gate: SemaphoreSlim) : Task =
        task {
            try
                do! pending
            finally
                (database :> IDisposable).Dispose()
                gate.Release() |> ignore
        }

    static member private FinishGeneric<'T>
        (pending: Task<'T>, database: SqliteDatabase, gate: SemaphoreSlim)
        : Task<'T> =
        task {
            try
                return! pending
            finally
                (database :> IDisposable).Dispose()
                gate.Release() |> ignore
        }

    member private _.OpenInner(database: SqliteDatabase) : obj =
        match kind with
        | Session -> SqliteStoreFactory.sessionStore database :> obj
        | Event -> SqliteStoreFactory.eventStore database :> obj

    override this.Invoke(methodInfo: MethodInfo | null, args: obj[] | null) : obj | null =
        match methodInfo, args with
        | null, _ -> raise (InvalidOperationException("A store proxy invocation had no method."))
        | _, null -> raise (InvalidOperationException("A store proxy invocation had no arguments."))
        | methodInfo, _ when methodInfo.Name = "ToString" -> box $"OnDemandStoreProxy({path})"
        | methodInfo, _ when methodInfo.Name = "GetHashCode" -> box (RuntimeHelpers.GetHashCode(this))
        | methodInfo, args when methodInfo.Name = "Equals" -> box (obj.ReferenceEquals(this, args[0]))
        | methodInfo, args when methodInfo.Name = "SupportsSettlementJournal" ->
            match args[0] with
            | :? OnDemandStoreProxy as journal ->
                box (String.Equals(path, journal.DatabasePath, StringComparison.OrdinalIgnoreCase))
            | _ -> box false
        | methodInfo, args ->
            counters.Record methodInfo.Name

            let gate = pathGates.GetOrAdd(path, fun _ -> new SemaphoreSlim(1, 1))
            gate.Wait()

            let mutable databaseOpt: SqliteDatabase option = None
            let mutable handedOff = false

            let release () =
                match databaseOpt with
                | Some database -> (database :> IDisposable).Dispose()
                | None -> ()

                gate.Release() |> ignore

            try
                let database = SqliteDatabase.Open(path, TimeProvider.System)
                databaseOpt <- Some database
                let inner = this.OpenInner database
                let result = methodInfo.Invoke(inner, args)

                match result with
                | null ->
                    release ()
                    raise (InvalidOperationException($"Store method {methodInfo.Name} did not return Task."))
                | result ->
                    match result with
                    | :? Task as pending when methodInfo.ReturnType = typeof<Task> ->
                        handedOff <- true
                        box (OnDemandStoreProxy.Finish(pending, database, gate))
                    | :? Task as pending when
                        methodInfo.ReturnType.IsGenericType
                        && methodInfo.ReturnType.GetGenericTypeDefinition() = typedefof<Task<_>>
                        ->
                        let resultType = methodInfo.ReturnType.GetGenericArguments()[0]

                        let helper =
                            match
                                typeof<OnDemandStoreProxy>
                                    .GetMethod("FinishGeneric", BindingFlags.Static ||| BindingFlags.NonPublic)
                            with
                            | null -> raise (MissingMethodException("The store proxy completion helper was not found."))
                            | found -> found

                        handedOff <- true

                        let wrapped =
                            helper
                                .MakeGenericMethod(resultType)
                                .Invoke(
                                    null,
                                    [|
                                        box pending
                                        box database
                                        box gate
                                    |]
                                )

                        if isNull (box wrapped) then
                            raise (InvalidOperationException("The store proxy completion helper returned null."))

                        unbox<obj> wrapped
                    | _ ->
                        release ()
                        raise (InvalidOperationException($"Store method {methodInfo.Name} did not return Task."))
            with error ->
                if not handedOff then
                    release ()

                match error with
                | :? TargetInvocationException as target ->
                    match target.InnerException with
                    | null -> raise target
                    | inner ->
                        ExceptionDispatchInfo.Capture(inner).Throw()
                        Unchecked.defaultof<obj>
                | _ -> reraise ()

let private createSessionProxy (databasePath: string) =
    let counters = StoreProxyCounters()
    let proxy = DispatchProxy.Create<CombinedSessionStore, OnDemandStoreProxy>()
    let implementation = proxy :?> OnDemandStoreProxy
    implementation.Configure(databasePath, Session, counters)
    let store: ISessionStore = proxy
    let abort: ISessionAbortControlStore = proxy
    store, abort, counters

let private createEventProxy (databasePath: string) =
    let counters = StoreProxyCounters()
    let proxy = DispatchProxy.Create<ISessionEventStore, OnDemandStoreProxy>()
    let implementation = proxy :?> OnDemandStoreProxy
    implementation.Configure(databasePath, Event, counters)
    proxy, counters

type private LabeledScriptedChatClient(label: string, steps: IReadOnlyList<ScriptStep>) =
    let scripted = new ScriptedChatClient(steps)
    let outputs = ConcurrentQueue<string>()

    member _.Label = label
    member _.Calls = scripted.Calls
    member _.Outputs = outputs.ToArray()

    interface IChatClient with
        member _.GetResponseAsync(history, options, cancellationToken) =
            task {
                let! response = (scripted :> IChatClient).GetResponseAsync(history, options, cancellationToken)

                outputs.Enqueue(response.Text)

                return response
            }

        member _.GetStreamingResponseAsync(history, options, cancellationToken) =
            (scripted :> IChatClient).GetStreamingResponseAsync(history, options, cancellationToken)

        member _.GetService(serviceType, serviceKey) =
            (scripted :> IChatClient).GetService(serviceType, serviceKey)

        member _.Dispose() = (scripted :> IDisposable).Dispose()

type private ExecutionRig =
    {
        Provider: ServiceProvider
        Store: ISessionStore
        Abort: ISessionAbortControlStore
        Events: ISessionEventStore
        StoreCounters: StoreProxyCounters
        EventCounters: StoreProxyCounters
        Chat: LabeledScriptedChatClient
    }

type private RecordingPermissionPolicy(toolName: string, verdict: PermissionVerdict) =
    interface IPermissionPolicy with
        member _.Evaluate(request) =
            if request.ToolName = toolName then
                verdict
            else
                PermissionVerdict.Allow

type private RecordingEffectTool(label: string) =
    let calls = ConcurrentQueue<string>()

    let invoked =
        System.Threading.Tasks.TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously)

    let functionValue =
        let implementation =
            System.Func<string>(fun () ->
                calls.Enqueue(label)
                invoked.TrySetResult(label) |> ignore
                label)

        AIFunctionFactory.Create(
            implementation,
            "record_effect",
            Unchecked.defaultof<string>,
            Unchecked.defaultof<JsonSerializerOptions>
        )

    member _.Tool = functionValue :> AITool
    member _.Calls = calls.ToArray()
    member _.WaitForCallAsync(token: CancellationToken) = invoked.Task.WaitAsync(token)

type private ExecutionScript =
    {
        Steps: IReadOnlyList<ScriptStep>
        Tools: IReadOnlyList<AITool>
        Policy: IPermissionPolicy option
    }

type private NodeRig =
    {
        Provider: ServiceProvider
        Cluster: ClusterActorSystemService
        BindingA: SessionHostBinding
        BindingB: SessionHostBinding
        A: ExecutionRig
        B: ExecutionRig
        Port: int
    }

let private cancellation = CancellationToken.None

let private freePort () =
    use listener = new TcpListener(IPAddress.Loopback, 0)
    listener.Start()
    let port = (listener.LocalEndpoint :?> IPEndPoint).Port
    listener.Stop()
    port

let private configureCluster (port: int) (seeds: string list) (roles: string list) (options: LegateOptions) =
    options.Cluster.Mode <- ClusterMode.StaticSeeds
    options.Cluster.RemotingPort <- port
    options.Cluster.RemotingHostname <- "127.0.0.1"
    options.Cluster.SeedNodes.Clear()
    seeds |> List.iter (fun seed -> options.Cluster.SeedNodes.Add(seed) |> ignore)
    options.Cluster.Roles.Clear()
    roles |> List.iter (fun role -> options.Cluster.Roles.Add(role) |> ignore)
    options.Cluster.JoinTimeout <- TimeSpan.FromSeconds 20.0
    options.Cluster.MinimumMembers <- 1
    options.Cluster.ShutdownGraceSeconds <- TimeSpan.FromSeconds 2.0
    options.Cluster.HostExitDeadline <- TimeSpan.FromSeconds 10.0

let private buildExecutionProviderWith
    (tenant: TenantId)
    (databasePath: string)
    (label: string)
    (script: ExecutionScript)
    : ExecutionRig =
    let sessionStore, abort, storeCounters = createSessionProxy databasePath
    let eventStore, eventCounters = createEventProxy databasePath

    let chat = new LabeledScriptedChatClient(label, script.Steps)

    let clientOptions = SessionClientOptions()
    clientOptions.Tenant <- tenant

    if label.Contains("restart", StringComparison.OrdinalIgnoreCase) then
        // The crash/rebind acceptance uses a deliberately short durable
        // claim so the fresh owner can take over after the old node is
        // terminated without a wall-clock sleep in the test.
        clientOptions.LeaseDuration <- TimeSpan.FromSeconds 1.0

    let services = ServiceCollection()
    LegateServiceCollectionExtensions.AddLegate(services) |> ignore

    services.Configure<LegateOptions>(
        Action<LegateOptions>(fun options ->
            options.Cluster.Mode <- ClusterMode.StaticSeeds
            options.Cluster.SeedNodes.Add("127.0.0.1:1") |> ignore)
    )
    |> ignore

    services.AddSingleton(clientOptions) |> ignore
    services.AddSingleton<ISessionStore>(sessionStore) |> ignore
    services.AddSingleton<ISessionEventStore>(eventStore) |> ignore
    services.AddSingleton<IChatClient>(chat :> IChatClient) |> ignore
    services.AddSingleton<ILlmProvider>(BuilderTests.StubLlmProvider()) |> ignore

    services.AddSingleton<IWorkspaceRuntime>(BuilderTests.StubWorkspaceRuntime())
    |> ignore

    services.AddSingleton<IToolSource>(StaticToolSource(script.Tools) :> IToolSource)
    |> ignore

    match script.Policy with
    | Some policy -> services.AddSingleton<IPermissionPolicy>(policy) |> ignore
    | None -> ()

    {
        Provider = services.BuildServiceProvider(ServiceProviderOptions(ValidateScopes = true))
        Store = sessionStore
        Abort = abort
        Events = eventStore
        StoreCounters = storeCounters
        EventCounters = eventCounters
        Chat = chat
    }

let private buildExecutionProvider
    (tenant: TenantId)
    (databasePath: string)
    (label: string)
    (response: string)
    : ExecutionRig =
    buildExecutionProviderWith
        tenant
        databasePath
        label
        {
            Steps = ResizeArray<ScriptStep>([| ScriptStep.Text response |]) :> IReadOnlyList<ScriptStep>
            Tools = ResizeArray<AITool>() :> IReadOnlyList<AITool>
            Policy = None
        }

let private clusterService (provider: ServiceProvider) =
    provider.GetServices<IHostedService>()
    |> Seq.pick (function
        | :? ClusterActorSystemService as cluster -> Some cluster
        | _ -> None)

let private buildNodeWithOptions
    (port: int)
    (seeds: string list)
    (roles: string list)
    (tenantA: TenantId)
    (tenantB: TenantId)
    (executionA: ExecutionRig)
    (executionB: ExecutionRig)
    (maxSubscribers: int option)
    (extraBinding: SessionHostBinding option)
    : NodeRig =
    let bindingA =
        SessionHostBinding(
            tenantA,
            System.Func<IServiceProvider, IServiceProvider>(fun _ -> executionA.Provider :> IServiceProvider)
        )

    let bindingB =
        SessionHostBinding(
            tenantB,
            System.Func<IServiceProvider, IServiceProvider>(fun _ -> executionB.Provider :> IServiceProvider)
        )

    let services = ServiceCollection()
    LegateServiceCollectionExtensions.AddLegate(services) |> ignore

    services.Configure<LegateOptions>(
        Action<LegateOptions>(fun options ->
            configureCluster port seeds roles options

            match maxSubscribers with
            | Some limit -> options.Sessions.MaxSubscribersPerSession <- limit
            | None -> ())
    )
    |> ignore

    services.AddLegateSessionBinding(bindingA).AddLegateSessionBinding(bindingB)
    |> ignore

    match extraBinding with
    | Some binding -> services.AddLegateSessionBinding(binding) |> ignore
    | None -> ()

    let provider =
        services.BuildServiceProvider(ServiceProviderOptions(ValidateScopes = true))

    let cluster = clusterService provider

    {
        Provider = provider
        Cluster = cluster
        BindingA = bindingA
        BindingB = bindingB
        A = executionA
        B = executionB
        Port = port
    }

let private buildNode
    (port: int)
    (seeds: string list)
    (roles: string list)
    (tenantA: TenantId)
    (tenantB: TenantId)
    (executionA: ExecutionRig)
    (executionB: ExecutionRig)
    : NodeRig =
    buildNodeWithOptions port seeds roles tenantA tenantB executionA executionB None None

let private seedSession
    (databasePath: string)
    (tenant: TenantId)
    (sessionId: SessionId)
    (agentId: AgentId)
    (destination: string)
    =
    let database = SqliteDatabase.Open(databasePath, TimeProvider.System)

    try
        let store = SqliteStoreFactory.sessionStore database
        let options = SessionOptions()
        let title = $"seed-{destination}"
        options.Title <- title
        let metadata = Dictionary<string, string>()
        metadata["destination"] <- destination
        metadata["database"] <- databasePath
        options.Metadata <- metadata :> IReadOnlyDictionary<string, string>

        let now = DateTimeOffset.UtcNow

        let session =
            {
                Id = sessionId
                Tenant = tenant
                AgentId = agentId
                Title = title
                State = SessionState.Idle
                CurrentTurnId = Nullable()
                CreatedAt = now
                UpdatedAt = now
                ClosedAt = Nullable()
                WorkspaceBinding = destination
                Options = options
                PermissionGrants = Array.empty<string> :> IReadOnlyList<string>
            }

        store.CreateSession(tenant, session, cancellation).GetAwaiter().GetResult()
        |> ignore
    finally
        (database :> IDisposable).Dispose()

let private awaitMembers (cluster: ClusterActorSystemService) (expected: int) : Task =
    task {
        use timeout = new CancellationTokenSource(TimeSpan.FromSeconds 30.0)
        let mutable members = 0

        while members < expected do
            timeout.Token.ThrowIfCancellationRequested()
            members <- Cluster.Get(cluster.System).State.Members.Count

            if members < expected then
                do! Task.Delay(25, timeout.Token)
    }

let private allEvents (store: ISessionEventStore) (tenant: TenantId) (sessionId: SessionId) =
    task {
        let events = ResizeArray<SessionEvent>()
        let mutable cursor = 0L
        let mutable finished = false

        while not finished do
            let! outcome = store.Replay(tenant, sessionId, cursor, 100, cancellation)

            match outcome with
            | :? EventReplayPage as page ->
                for event in page.Events do
                    events.Add event

                if page.NextCursor.HasValue then
                    cursor <- page.NextCursor.Value
                else
                    finished <- true
            | :? EventReplayEndOfStream -> finished <- true
            | :? EventReplayUnknownSession -> finished <- true
            | :? EventReplayJournalExpired -> finished <- true
            | _ -> raise (InvalidOperationException("The SQLite journal returned an unknown replay outcome."))

        return events :> IReadOnlyList<SessionEvent>
    }

let private awaitPersistedTurn
    (client: SessionClient)
    (store: ISessionStore)
    (tenant: TenantId)
    (sessionId: SessionId)
    (chat: LabeledScriptedChatClient)
    (fromSequence: int64)
    : Task<Session * IReadOnlyList<SessionEvent>> =
    task {
        use timeout = new CancellationTokenSource(TimeSpan.FromSeconds 45.0)
        let collected = ResizeArray<SessionEvent>()

        let stream =
            SessionClientOperations.Subscribe(client, sessionId, fromSequence, timeout.Token)

        let enumerator = stream.GetAsyncEnumerator(timeout.Token)

        try
            let mutable settled = false

            while not settled do
                timeout.Token.ThrowIfCancellationRequested()
                let! moved = enumerator.MoveNextAsync().AsTask()

                if not moved then
                    raise (
                        InvalidOperationException(
                            $"The routed event stream ended before the turn settled for {tenant.Value}; chatCalls={chat.Calls}."
                        )
                    )

                let event = enumerator.Current
                collected.Add event
                settled <- event :? TurnCompletedEvent

            // The terminal journal row is published before the actor's
            // durable settle continuation updates the coarse session row.
            // A routed suspend snapshot is the seam that drains that actor
            // mailbox without reintroducing a polling sleep.
            let! actor = client.Resolve(sessionId, timeout.Token)
            let! _ = SessionActor.getSuspendSnapshotAsync actor timeout.Token
            let! stored = store.GetSession(tenant, sessionId, cancellation)

            let session =
                match stored with
                | null ->
                    raise (SessionNotFoundException(sessionId, "The acceptance session disappeared after completion."))
                | present -> present

            if session.State <> SessionState.Idle || session.CurrentTurnId.HasValue then
                raise (
                    InvalidOperationException(
                        $"The completion event arrived before the persisted session became idle for {tenant.Value}."
                    )
                )

            return session, collected :> IReadOnlyList<SessionEvent>
        finally
            enumerator.DisposeAsync().AsTask().GetAwaiter().GetResult()
    }

let private persistedFingerprint
    (store: ISessionStore)
    (abort: ISessionAbortControlStore)
    (events: ISessionEventStore)
    (tenant: TenantId)
    (sessionId: SessionId)
    : Task<string> =
    task {
        let! session = store.GetSession(tenant, sessionId, cancellation)
        let! target = abort.ReadAbortTarget(tenant, sessionId, cancellation)
        let! pending = store.ReadPendingInbox(tenant, sessionId, cancellation)
        let! journal = allEvents events tenant sessionId

        return
            String.concat
                "|"
                [
                    JsonSerializer.Serialize(session)
                    JsonSerializer.Serialize(target)
                    JsonSerializer.Serialize(pending)
                    JsonSerializer.Serialize(journal)
                ]
    }

let private requireSession (sessionId: SessionId) (session: Session | null) =
    match session with
    | null -> raise (SessionNotFoundException(sessionId, "The acceptance session was not found."))
    | present -> present

let private destinationOf (session: Session) =
    match session.Options.Metadata with
    | null -> ""
    | metadata -> metadata["destination"]

let private awaitSessionEvent
    (client: SessionClient)
    (sessionId: SessionId)
    (fromSequence: int64)
    (matches: SessionEvent -> bool)
    : Task<SessionEvent> =
    task {
        use timeout = new CancellationTokenSource(TimeSpan.FromSeconds 30.0)

        let stream =
            SessionClientOperations.Subscribe(client, sessionId, fromSequence, timeout.Token)

        let enumerator = stream.GetAsyncEnumerator(timeout.Token)

        try
            let mutable found: SessionEvent option = None

            while found.IsNone do
                let! moved = enumerator.MoveNextAsync().AsTask()

                if not moved then
                    raise (InvalidOperationException("The routed event stream ended before the expected event."))

                if matches enumerator.Current then
                    found <- Some enumerator.Current

            return found.Value
        finally
            enumerator.DisposeAsync().AsTask().GetAwaiter().GetResult()
    }

let private awaitPermissionRequest (client: SessionClient) (sessionId: SessionId) : Task<string> =
    task {
        let! event = awaitSessionEvent client sessionId 0L (fun event -> event :? PermissionRequestedEvent)
        return (event :?> PermissionRequestedEvent).RequestId
    }

let private lastSequence (events: IReadOnlyList<SessionEvent>) =
    events
    |> Seq.choose (fun event ->
        if event.Sequence.HasValue then
            Some event.Sequence.Value
        else
            None)
    |> Seq.fold (fun current sequence -> max current sequence) 0L

let private cursorOf (events: ISessionEventStore) (tenant: TenantId) (sessionId: SessionId) : Task<int64> =
    task {
        let! journal = allEvents events tenant sessionId
        return lastSequence journal
    }

let private directRegionAsk
    (cluster: ClusterActorSystemService)
    (envelopeKey: string)
    (messageAddress: string)
    (scope: string)
    (payload: obj)
    : Task<SessionRouteResponse> =
    task {
        use timeout = new CancellationTokenSource(TimeSpan.FromSeconds 30.0)

        let request: SessionRouteRequest =
            {
                Address = messageAddress
                Scope = scope
                Payload = payload
            }

        let! raw = cluster.Region.Ask<obj>(ShardingEnvelope(envelopeKey, request :> obj), timeout.Token)

        return raw :?> SessionRouteResponse
    }

let private assertNoMutations (nodes: NodeRig list) =
    for node in nodes do
        Assert.Equal(0, node.A.StoreCounters.MutationCount)
        Assert.Equal(0, node.B.StoreCounters.MutationCount)
        Assert.Equal(0, node.A.EventCounters.MutationCount)
        Assert.Equal(0, node.B.EventCounters.MutationCount)

type private RefusalExpectation =
    | ScopeRefusal of SessionScopeRejectionReason
    | UnknownSession

type private RefusalSnapshot =
    {
        FingerprintA: string
        FingerprintB: string
        StoreCountersA: string
        StoreCountersB: string
        EventCountersA: string
        EventCountersB: string
        StoreMutationsA: int
        StoreMutationsB: int
        EventMutationsA: int
        EventMutationsB: int
        ModelCallsA: int
        ModelCallsB: int
        ToolCallsA: int
        ToolCallsB: int
    }

let private refusalSnapshot
    (node: NodeRig)
    (tenantA: TenantId)
    (sessionA: SessionId)
    (tenantB: TenantId)
    (sessionB: SessionId)
    (toolA: RecordingEffectTool)
    (toolB: RecordingEffectTool)
    : Task<RefusalSnapshot> =
    task {
        let! fingerprintA = persistedFingerprint node.A.Store node.A.Abort node.A.Events tenantA sessionA
        let! fingerprintB = persistedFingerprint node.B.Store node.B.Abort node.B.Events tenantB sessionB

        return
            {
                FingerprintA = fingerprintA
                FingerprintB = fingerprintB
                StoreCountersA = JsonSerializer.Serialize(node.A.StoreCounters.Snapshot())
                StoreCountersB = JsonSerializer.Serialize(node.B.StoreCounters.Snapshot())
                EventCountersA = JsonSerializer.Serialize(node.A.EventCounters.Snapshot())
                EventCountersB = JsonSerializer.Serialize(node.B.EventCounters.Snapshot())
                StoreMutationsA = node.A.StoreCounters.MutationCount
                StoreMutationsB = node.B.StoreCounters.MutationCount
                EventMutationsA = node.A.EventCounters.MutationCount
                EventMutationsB = node.B.EventCounters.MutationCount
                ModelCallsA = node.A.Chat.Calls
                ModelCallsB = node.B.Chat.Calls
                ToolCallsA = toolA.Calls.Length
                ToolCallsB = toolB.Calls.Length
            }
    }

let private refusalSnapshotBeforeReads
    (node: NodeRig)
    (tenantA: TenantId)
    (sessionA: SessionId)
    (tenantB: TenantId)
    (sessionB: SessionId)
    (toolA: RecordingEffectTool)
    (toolB: RecordingEffectTool)
    : Task<RefusalSnapshot> =
    task {
        let storeCountersA = JsonSerializer.Serialize(node.A.StoreCounters.Snapshot())
        let storeCountersB = JsonSerializer.Serialize(node.B.StoreCounters.Snapshot())
        let eventCountersA = JsonSerializer.Serialize(node.A.EventCounters.Snapshot())
        let eventCountersB = JsonSerializer.Serialize(node.B.EventCounters.Snapshot())
        let storeMutationsA = node.A.StoreCounters.MutationCount
        let storeMutationsB = node.B.StoreCounters.MutationCount
        let eventMutationsA = node.A.EventCounters.MutationCount
        let eventMutationsB = node.B.EventCounters.MutationCount
        let modelCallsA = node.A.Chat.Calls
        let modelCallsB = node.B.Chat.Calls
        let toolCallsA = toolA.Calls.Length
        let toolCallsB = toolB.Calls.Length
        let! fingerprintA = persistedFingerprint node.A.Store node.A.Abort node.A.Events tenantA sessionA
        let! fingerprintB = persistedFingerprint node.B.Store node.B.Abort node.B.Events tenantB sessionB

        return
            {
                FingerprintA = fingerprintA
                FingerprintB = fingerprintB
                StoreCountersA = storeCountersA
                StoreCountersB = storeCountersB
                EventCountersA = eventCountersA
                EventCountersB = eventCountersB
                StoreMutationsA = storeMutationsA
                StoreMutationsB = storeMutationsB
                EventMutationsA = eventMutationsA
                EventMutationsB = eventMutationsB
                ModelCallsA = modelCallsA
                ModelCallsB = modelCallsB
                ToolCallsA = toolCallsA
                ToolCallsB = toolCallsB
            }
    }

let private assertRefusalSnapshotUnchanged before after =
    Assert.Equal(before.FingerprintA, after.FingerprintA)
    Assert.Equal(before.FingerprintB, after.FingerprintB)
    Assert.Equal(before.StoreMutationsA, after.StoreMutationsA)
    Assert.Equal(before.StoreMutationsB, after.StoreMutationsB)
    Assert.Equal(before.EventMutationsA, after.EventMutationsA)
    Assert.Equal(before.EventMutationsB, after.EventMutationsB)
    Assert.Equal(before.ModelCallsA, after.ModelCallsA)
    Assert.Equal(before.ModelCallsB, after.ModelCallsB)
    Assert.Equal(before.ToolCallsA, after.ToolCallsA)
    Assert.Equal(before.ToolCallsB, after.ToolCallsB)

let private assertScopedRefusal (owner: string) (expected: RefusalExpectation) (response: SessionRouteResponse) =
    Assert.Equal(owner, response.Owner)

    match expected with
    | ScopeRefusal reason ->
        match response.Payload with
        | :? SessionScopeRejectedException as error -> Assert.Equal(reason, error.Reason)
        | other ->
            let actualType =
                if isNull (box other) then
                    "<null>"
                else
                    other.GetType().ToString()

            failwith $"Expected a scoped refusal {reason}, but received {actualType}."
    | UnknownSession ->
        match response.Payload with
        | :? SessionNotFoundException -> ()
        | other ->
            let actualType =
                if isNull (box other) then
                    "<null>"
                else
                    other.GetType().ToString()

            failwith $"Expected an unknown-session refusal, but received {actualType}."

[<Fact>]
let ``issue395 real wire refusal matrix crosses the production two node boundary`` () : Task =
    task {
        let tenantA = TenantId.Create "tenant-a-395-wire"
        let tenantB = TenantId.Create "tenant-b-395-wire"
        let tenantC = TenantId.Create "tenant-c-395-wire"
        let tenantUnknown = TenantId.Create "tenant-unknown-395-wire"
        let sessionId = SessionId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAE"
        let unknownSessionId = SessionId.New()
        let agentA = AgentId.New()
        let agentB = AgentId.New()

        let pathA =
            Path.Combine(Path.GetTempPath(), "legate-issue395-wire-a-" + Guid.NewGuid().ToString("N") + ".db")

        let pathB =
            Path.Combine(Path.GetTempPath(), "legate-issue395-wire-b-" + Guid.NewGuid().ToString("N") + ".db")

        let pathC =
            Path.Combine(Path.GetTempPath(), "legate-issue395-wire-c-" + Guid.NewGuid().ToString("N") + ".db")

        let portA = freePort ()
        let portB = freePort ()
        let addressA = SessionAddress(tenantA, sessionId)
        let addressB = SessionAddress(tenantB, sessionId)
        let unknownAddress = SessionAddress(tenantA, unknownSessionId)
        let unavailableAddress = SessionAddress(tenantUnknown, sessionId)

        seedSession pathA tenantA sessionId agentA "wire-A"
        seedSession pathB tenantB sessionId agentB "wire-B"
        seedSession pathC tenantC sessionId (AgentId.New()) "wire-C"

        let toolA = RecordingEffectTool("wire-effect-A")
        let toolB = RecordingEffectTool("wire-effect-B")
        let emptyTools = ResizeArray<AITool>() :> IReadOnlyList<AITool>

        let toolSet (tool: RecordingEffectTool) =
            ResizeArray<AITool>([| tool.Tool |]) :> IReadOnlyList<AITool>

        let script (steps: ScriptStep array) (tools: IReadOnlyList<AITool>) (policy: IPermissionPolicy option) =
            {
                Steps = ResizeArray<ScriptStep>(steps) :> IReadOnlyList<ScriptStep>
                Tools = tools
                Policy = policy
            }

        let node1A =
            buildExecutionProviderWith tenantA pathA "wire-node1-A" (script [||] emptyTools None)

        let node1B =
            buildExecutionProviderWith tenantB pathB "wire-node1-B" (script [||] emptyTools None)

        let node1C =
            buildExecutionProviderWith tenantC pathC "wire-node1-C" (script [||] emptyTools None)

        let node2A =
            buildExecutionProviderWith
                tenantA
                pathA
                "wire-node2-A"
                (script [| ScriptStep.Text "wire-settled-A" |] (toolSet toolA) None)

        let node2B =
            buildExecutionProviderWith
                tenantB
                pathB
                "wire-node2-B"
                (script [| ScriptStep.Text "wire-settled-B" |] (toolSet toolB) None)

        let bindingC =
            SessionHostBinding(
                tenantC,
                System.Func<IServiceProvider, IServiceProvider>(fun _ -> node1C.Provider :> IServiceProvider)
            )

        let node1 =
            buildNodeWithOptions
                portA
                [ $"127.0.0.1:%d{portA}" ]
                [ "api" ]
                tenantA
                tenantB
                node1A
                node1B
                (Some 1)
                (Some bindingC)

        let node2 =
            buildNodeWithOptions
                portB
                [ $"127.0.0.1:%d{portA}" ]
                [ "session" ]
                tenantA
                tenantB
                node2A
                node2B
                (Some 1)
                None

        let stopCluster (node: NodeRig) =
            try
                match node.Cluster.System with
                | null -> ()
                | system ->
                    system.Terminate().GetAwaiter().GetResult()
                    system.WhenTerminated.Wait(TimeSpan.FromSeconds 30.0) |> ignore
            with _ ->
                ()

        let disposeExecution (executions: ExecutionRig list) =
            for execution in executions do
                execution.Provider.Dispose()

        let validPayload operation =
            match operation with
            | "Queue" ->
                SessionActor.SuspendableQueuePrompt(
                    UserMessagePayload(UserMessage.Text "wire") :> InboxPayload,
                    cancellation
                )
                :> obj
            | "Inject" ->
                SessionActor.SuspendableInjectPrompt(
                    UserMessagePayload(UserMessage.Text "wire") :> InboxPayload,
                    cancellation
                )
                :> obj
            | "Interrupt" ->
                SessionActor.SuspendableInterruptPrompt(
                    UserMessagePayload(UserMessage.Text "wire") :> InboxPayload,
                    cancellation
                )
                :> obj
            | "Reply" ->
                SessionActor.SessionReplyPayload(PermissionDecision("wire-request", PermissionDecisionKind.AllowOnce))
                :> obj
            | "Snapshot" -> SessionActor.SuspendableGetSnapshot :> obj
            | "Compact" -> SessionActor.SuspendableCompactSession cancellation :> obj
            | "SetAgent" -> SessionActor.SuspendableSetAgent(AgentId.New(), cancellation) :> obj
            | "Close" -> SessionActor.SuspendableCloseSession cancellation :> obj
            | "CheckInbox" -> SessionActor.SuspendableCheckInbox :> obj
            | "Subscribe" ->
                ({
                    Tenant = tenantA
                    SessionId = sessionId
                    FromSequence = 0L
                    SubscriberToken = "wire-token"
                }
                : CrossNodeSubscriptions.CrossNodeSubscribeRequest)
                :> obj
            | "Unsubscribe" ->
                ({
                    Tenant = tenantA
                    SessionId = sessionId
                    SubscriberToken = "wire-token"
                }
                : CrossNodeSubscriptions.CrossNodeUnsubscribe)
                :> obj
            | "Exact393Hint" -> SessionActor.SuspendableObserveHostAbort(tenantA, sessionId, TurnId.New()) :> obj
            | other -> failwith $"Unknown real-wire operation {other}."

        let subscriptionPayload operation tenant session =
            match operation with
            | "Subscribe" ->
                ({
                    Tenant = tenant
                    SessionId = session
                    FromSequence = 0L
                    SubscriberToken = "wire-token"
                }
                : CrossNodeSubscriptions.CrossNodeSubscribeRequest)
                :> obj
            | "Unsubscribe" ->
                ({
                    Tenant = tenant
                    SessionId = session
                    SubscriberToken = "wire-token"
                }
                : CrossNodeSubscriptions.CrossNodeUnsubscribe)
                :> obj
            | _ -> validPayload operation

        let operations =
            [
                "Queue"
                "Inject"
                "Interrupt"
                "Reply"
                "Snapshot"
                "Compact"
                "SetAgent"
                "Close"
                "CheckInbox"
                "Subscribe"
                "Unsubscribe"
                "Exact393Hint"
            ]

        let makeCases () =
            let common operation =
                [
                    (sprintf "%s/addressA-messageAddressB" operation,
                     addressA.Key,
                     addressB.Key,
                     tenantA.Value,
                     validPayload operation,
                     ScopeRefusal SessionScopeRejectionReason.AddressMismatch,
                     true)
                    (sprintf "%s/addressA-scopeB" operation,
                     addressA.Key,
                     addressA.Key,
                     tenantB.Value,
                     validPayload operation,
                     ScopeRefusal SessionScopeRejectionReason.AddressMismatch,
                     true)
                    (sprintf "%s/emptyScope" operation,
                     addressA.Key,
                     addressA.Key,
                     "",
                     validPayload operation,
                     ScopeRefusal SessionScopeRejectionReason.AddressMismatch,
                     true)
                    (sprintf "%s/unknownScope" operation,
                     addressA.Key,
                     addressA.Key,
                     tenantUnknown.Value,
                     validPayload operation,
                     ScopeRefusal SessionScopeRejectionReason.AddressMismatch,
                     true)
                    (sprintf "%s/unknownReceiverBinding" operation,
                     unavailableAddress.Key,
                     unavailableAddress.Key,
                     tenantUnknown.Value,
                     (match operation with
                      | "Exact393Hint" ->
                          SessionActor.SuspendableObserveHostAbort(tenantUnknown, sessionId, TurnId.New()) :> obj
                      | "Subscribe"
                      | "Unsubscribe" -> subscriptionPayload operation tenantUnknown sessionId
                      | _ -> validPayload operation),
                     ScopeRefusal SessionScopeRejectionReason.ScopeUnavailable,
                     true)
                    (sprintf "%s/oldSessionOnlyEntityKey" operation,
                     sessionId.Value,
                     addressA.Key,
                     tenantA.Value,
                     validPayload operation,
                     ScopeRefusal SessionScopeRejectionReason.InvalidScope,
                     true)
                    (sprintf "%s/invalidEntityKey" operation,
                     "not-an-address",
                     addressA.Key,
                     tenantA.Value,
                     validPayload operation,
                     ScopeRefusal SessionScopeRejectionReason.InvalidScope,
                     true)
                    (sprintf "%s/authorizedUnknownSession" operation,
                     unknownAddress.Key,
                     unknownAddress.Key,
                     tenantA.Value,
                     (match operation with
                      | "Exact393Hint" ->
                          SessionActor.SuspendableObserveHostAbort(tenantA, unknownSessionId, TurnId.New()) :> obj
                      | "Subscribe"
                      | "Unsubscribe" -> subscriptionPayload operation tenantA unknownSessionId
                      | _ -> validPayload operation),
                     UnknownSession,
                     false)
                ]

            let subscriptionCases operation =
                [
                    (sprintf "%s/subscriptionPayloadTenantB-equalId" operation,
                     addressA.Key,
                     addressA.Key,
                     tenantA.Value,
                     subscriptionPayload operation tenantB sessionId,
                     ScopeRefusal SessionScopeRejectionReason.AddressMismatch,
                     true)
                    (sprintf "%s/subscriptionPayloadTenantB-IDmismatch" operation,
                     addressA.Key,
                     addressA.Key,
                     tenantA.Value,
                     subscriptionPayload operation tenantB unknownSessionId,
                     ScopeRefusal SessionScopeRejectionReason.AddressMismatch,
                     true)
                ]

            operations
            |> List.collect (fun operation ->
                let selected = common operation

                if operation = "Subscribe" || operation = "Unsubscribe" then
                    selected @ subscriptionCases operation
                else
                    selected)

        let runMatrix (label: string) =
            task {
                let owner = Cluster.Get(node2.Cluster.System).SelfAddress.ToString()

                for name, envelopeKey, messageAddress, scope, payload, expected, noStore in makeCases () do
                    let! before = refusalSnapshot node2 tenantA sessionId tenantB sessionId toolA toolB

                    let! response = directRegionAsk node1.Cluster envelopeKey messageAddress scope payload

                    Assert.False(String.IsNullOrWhiteSpace response.Owner, $"{label}/{name} had no physical owner.")
                    assertScopedRefusal owner expected response

                    let! after = refusalSnapshotBeforeReads node2 tenantA sessionId tenantB sessionId toolA toolB
                    assertRefusalSnapshotUnchanged before after

                    if noStore then
                        Assert.Equal(before.StoreCountersA, after.StoreCountersA)
                        Assert.Equal(before.StoreCountersB, after.StoreCountersB)
                        Assert.Equal(before.EventCountersA, after.EventCountersA)
                        Assert.Equal(before.EventCountersB, after.EventCountersB)
            }

        try
            for execution in [ node1A; node1B; node2A; node2B ] do
                execution.Provider.GetRequiredService<IOptions<LegateOptions>>().Value.Sessions.MaxSubscribersPerSession <-
                    1

            do! node1.Provider.GetRequiredService<ISessionHostContexts>().InitializeAsync cancellation
            do! node2.Provider.GetRequiredService<ISessionHostContexts>().InitializeAsync cancellation
            do! (node1.Cluster :> IHostedService).StartAsync cancellation
            do! (node2.Cluster :> IHostedService).StartAsync cancellation
            do! awaitMembers node1.Cluster 2
            do! awaitMembers node2.Cluster 2

            // A probe warms only the route. The first matrix therefore proves
            // malformed decodable messages are refused on a cold entity before
            // a session child, claim, recovery, model, or tool can be created.
            let! probe =
                directRegionAsk node1.Cluster addressA.Key addressA.Key tenantA.Value (SessionRouteProbe :> obj)

            Assert.IsType<SessionRouteAccepted>(probe.Payload) |> ignore

            let clientC = bindingC.Client
            let! beforeC = persistedFingerprint node1C.Store node1C.Abort node1C.Events tenantC sessionId

            try
                let! _ =
                    SessionClientOperations.PromptAsync(
                        clientC,
                        sessionId,
                        UserMessage.Text "receiver-binding-is-missing",
                        DeliveryMode.Queue,
                        cancellation
                    )

                failwith "The public bound client unexpectedly forwarded to a receiver without tenant C."
            with :? SessionScopeRejectedException as error ->
                Assert.Equal(SessionScopeRejectionReason.ScopeUnavailable, error.Reason)

            let! afterC = persistedFingerprint node1C.Store node1C.Abort node1C.Events tenantC sessionId
            Assert.Equal(beforeC, afterC)
            do! runMatrix "cold"

            let senderA = node1.BindingA.Client

            let! _ =
                SessionClientOperations.PromptAsync(
                    senderA,
                    sessionId,
                    UserMessage.Text "settle-before-live-matrix",
                    DeliveryMode.Queue,
                    cancellation
                )

            let! settled = awaitPersistedTurn senderA node2A.Store tenantA sessionId node2A.Chat 0L
            Assert.Equal(SessionState.Idle, (fst settled).State)
            Assert.Equal(1, node2A.Chat.Calls)

            // The same table now crosses the same production wire with a live
            // actor. Refusal must remain a gate decision, not a failed turn or
            // a local fallback, and tenant B must remain untouched.
            do! runMatrix "live"

            let subscribe tenant session token : obj =
                ({
                    Tenant = tenant
                    SessionId = session
                    FromSequence = 0L
                    SubscriberToken = token
                }
                : CrossNodeSubscriptions.CrossNodeSubscribeRequest)
                :> obj

            let unsubscribe tenant session token : obj =
                ({
                    Tenant = tenant
                    SessionId = session
                    SubscriberToken = token
                }
                : CrossNodeSubscriptions.CrossNodeUnsubscribe)
                :> obj

            let! attachedA =
                directRegionAsk
                    node1.Cluster
                    addressA.Key
                    addressA.Key
                    tenantA.Value
                    (subscribe tenantA sessionId "same-token")

            Assert.IsType<CrossNodeSubscriptions.CrossNodeEventBatch>(attachedA.Payload)
            |> ignore

            let! attachedB =
                directRegionAsk
                    node1.Cluster
                    addressB.Key
                    addressB.Key
                    tenantB.Value
                    (subscribe tenantB sessionId "same-token")

            Assert.IsType<CrossNodeSubscriptions.CrossNodeEventBatch>(attachedB.Payload)
            |> ignore

            let! wrongDetach =
                directRegionAsk
                    node1.Cluster
                    addressA.Key
                    addressA.Key
                    tenantA.Value
                    (unsubscribe tenantB sessionId "same-token")

            match wrongDetach.Payload with
            | :? SessionScopeRejectedException as error ->
                Assert.Equal(SessionScopeRejectionReason.AddressMismatch, error.Reason)
            | other -> failwith $"The cross-tenant unsubscribe was forwarded as {other.GetType().Name}."

            let! stillAttachedA =
                directRegionAsk
                    node1.Cluster
                    addressA.Key
                    addressA.Key
                    tenantA.Value
                    (subscribe tenantA sessionId "second-A-token")

            Assert.IsType<SessionSubscriptionLimitExceededException>(stillAttachedA.Payload)
            |> ignore

            let! detachedA =
                directRegionAsk
                    node1.Cluster
                    addressA.Key
                    addressA.Key
                    tenantA.Value
                    (unsubscribe tenantA sessionId "same-token")

            Assert.IsType<SessionRouteAccepted>(detachedA.Payload) |> ignore

            let! reattachedA =
                directRegionAsk
                    node1.Cluster
                    addressA.Key
                    addressA.Key
                    tenantA.Value
                    (subscribe tenantA sessionId "second-A-token")

            Assert.IsType<CrossNodeSubscriptions.CrossNodeEventBatch>(reattachedA.Payload)
            |> ignore

            let! stillAttachedB =
                directRegionAsk
                    node1.Cluster
                    addressB.Key
                    addressB.Key
                    tenantB.Value
                    (subscribe tenantB sessionId "second-B-token")

            Assert.IsType<SessionSubscriptionLimitExceededException>(stillAttachedB.Payload)
            |> ignore

            let! detachedB =
                directRegionAsk
                    node1.Cluster
                    addressB.Key
                    addressB.Key
                    tenantB.Value
                    (unsubscribe tenantB sessionId "same-token")

            Assert.IsType<SessionRouteAccepted>(detachedB.Payload) |> ignore
        finally
            stopCluster node2
            stopCluster node1

            disposeExecution
                [
                    node2B
                    node2A
                    node1C
                    node1B
                    node1A
                ]

            SqliteTestFixture.deleteDatabaseFiles pathA
            SqliteTestFixture.deleteDatabaseFiles pathB
            SqliteTestFixture.deleteDatabaseFiles pathC
    }

[<Fact>]
let ``issue395 bound proxy rejects wrong responses over the production wire`` () : Task =
    task {
        let tenantA = TenantId.Create "tenant-a-395-response"
        let tenantB = TenantId.Create "tenant-b-395-response"
        let sessionId = SessionId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAF"

        let pathA =
            Path.Combine(Path.GetTempPath(), "legate-issue395-response-a-" + Guid.NewGuid().ToString("N") + ".db")

        let pathB =
            Path.Combine(Path.GetTempPath(), "legate-issue395-response-b-" + Guid.NewGuid().ToString("N") + ".db")

        let portA = freePort ()
        let portB = freePort ()

        seedSession pathA tenantA sessionId (AgentId.New()) "response-A"
        seedSession pathB tenantB sessionId (AgentId.New()) "response-B"

        let emptyTools = ResizeArray<AITool>() :> IReadOnlyList<AITool>

        let script (steps: ScriptStep array) (tools: IReadOnlyList<AITool>) (policy: IPermissionPolicy option) =
            {
                Steps = ResizeArray<ScriptStep>(steps) :> IReadOnlyList<ScriptStep>
                Tools = tools
                Policy = policy
            }

        let node1A =
            buildExecutionProviderWith tenantA pathA "response-node1-A" (script [||] emptyTools None)

        let node1B =
            buildExecutionProviderWith tenantB pathB "response-node1-B" (script [||] emptyTools None)

        let node2A =
            buildExecutionProviderWith tenantA pathA "response-node2-A" (script [||] emptyTools None)

        let node2B =
            buildExecutionProviderWith tenantB pathB "response-node2-B" (script [||] emptyTools None)

        let node1 =
            buildNode portA [ $"127.0.0.1:%d{portA}" ] [ "api" ] tenantA tenantB node1A node1B

        let node2 =
            buildNode portB [ $"127.0.0.1:%d{portA}" ] [ "session" ] tenantA tenantB node2A node2B

        let addressA = SessionAddress(tenantA, sessionId)

        let eventFor session =
            TextDeltaEvent(session, TurnId.New(), Nullable<int64>(1L), DateTimeOffset.UtcNow, "controlled-wire-event")
            :> SessionEvent

        let batch tenant session token events : CrossNodeSubscriptions.CrossNodeEventBatch =
            {
                Tenant = tenant
                SessionId = session
                SubscriberToken = token
                Events = events :> IReadOnlyList<SessionEvent>
                NextCursor = 1L
                EndOfStream = true
            }

        let stopCluster (node: NodeRig) =
            try
                match node.Cluster.System with
                | null -> ()
                | system ->
                    system.Terminate().GetAwaiter().GetResult()
                    system.WhenTerminated.Wait(TimeSpan.FromSeconds 30.0) |> ignore
            with _ ->
                ()

        let disposeExecution (executions: ExecutionRig list) =
            for execution in executions do
                execution.Provider.Dispose()

        try
            do! node1.Provider.GetRequiredService<ISessionHostContexts>().InitializeAsync cancellation
            do! node2.Provider.GetRequiredService<ISessionHostContexts>().InitializeAsync cancellation
            do! (node1.Cluster :> IHostedService).StartAsync cancellation
            do! (node2.Cluster :> IHostedService).StartAsync cancellation
            do! awaitMembers node1.Cluster 2
            do! awaitMembers node2.Cluster 2

            let wrongResponses: SessionRouteResponse array =
                [|
                    {
                        Address = addressA.Key
                        Owner = "controlled"
                        Payload = batch tenantB sessionId "same-token" [| eventFor sessionId |] :> obj
                    }
                    {
                        Address = addressA.Key
                        Owner = "controlled"
                        Payload = batch tenantA sessionId "same-token" [| eventFor (SessionId.New()) |] :> obj
                    }
                    {
                        Address = addressA.Key
                        Owner = "controlled"
                        Payload = batch tenantA sessionId "wrong-token" [| eventFor sessionId |] :> obj
                    }
                    {
                        Address = SessionAddress(tenantB, sessionId).Key
                        Owner = "controlled"
                        Payload = SessionRouteAccepted :> obj
                    }
                |]

            let mutable responseIndex = 0

            let controlledName = "controlled-response-" + Guid.NewGuid().ToString("N")

            let node2System =
                match node2.Cluster.System with
                | null -> failwith "The production response-wire receiver cluster did not start."
                | system -> system

            let node1System =
                match node1.Cluster.System with
                | null -> failwith "The production response-wire sender cluster did not start."
                | system -> system

            let controlled =
                spawn node2System controlledName (fun mailbox ->
                    let rec loop () =
                        actor {
                            let! _ = mailbox.Receive()
                            let response = wrongResponses[responseIndex]
                            responseIndex <- responseIndex + 1
                            mailbox.Sender() <! response
                            return! loop ()
                        }

                    loop ())

            let beforeA = JsonSerializer.Serialize(node2.A.StoreCounters.Snapshot())
            let beforeB = JsonSerializer.Serialize(node2.B.StoreCounters.Snapshot())

            let proxyName = "controlled-bound-proxy-" + Guid.NewGuid().ToString("N")

            let proxy =
                spawn
                    node1System
                    proxyName
                    (SessionRouting.boundProxy addressA (fun request sender -> controlled.Tell(request, sender)))

            let subscribeProbe: obj =
                ({
                    Tenant = tenantA
                    SessionId = sessionId
                    FromSequence = 0L
                    SubscriberToken = "same-token"
                }
                : CrossNodeSubscriptions.CrossNodeSubscribeRequest)
                :> obj

            for _ in wrongResponses do
                let currentResponseIndex = responseIndex

                try
                    let! result = proxy.Ask<obj>(subscribeProbe, cancellation)

                    failwith
                        $"Controlled response {currentResponseIndex} unexpectedly passed the bound proxy as {result.GetType().FullName}."
                with :? SessionScopeRejectedException as error ->
                    Assert.Equal(SessionScopeRejectionReason.ResponseMismatch, error.Reason)

            Assert.Equal(beforeA, JsonSerializer.Serialize(node2.A.StoreCounters.Snapshot()))
            Assert.Equal(beforeB, JsonSerializer.Serialize(node2.B.StoreCounters.Snapshot()))
        finally
            stopCluster node2
            stopCluster node1
            disposeExecution [ node2B; node2A; node1B; node1A ]
            SqliteTestFixture.deleteDatabaseFiles pathA
            SqliteTestFixture.deleteDatabaseFiles pathB
    }

[<Fact>]
let ``issue395 production two-node tenant routing keeps scope and placement`` () : Task =
    task {
        let tenantA = TenantId.Create "tenant-a-395"
        let tenantB = TenantId.Create "tenant-b-395"
        let tenantUnknown = TenantId.Create "tenant-unknown-395"
        let sessionId = SessionId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"
        let agentA = AgentId.New()
        let agentB = AgentId.New()

        let pathA =
            Path.Combine(Path.GetTempPath(), "legate-issue395-a-" + Guid.NewGuid().ToString("N") + ".db")

        let pathB =
            Path.Combine(Path.GetTempPath(), "legate-issue395-b-" + Guid.NewGuid().ToString("N") + ".db")

        let portA = freePort ()
        let portB = freePort ()

        seedSession pathA tenantA sessionId agentA "destination-A"
        seedSession pathB tenantB sessionId agentB "destination-B"

        let node1A =
            buildExecutionProvider tenantA pathA "node1-tenantA" "node1-tenantA-unused"

        let node1B =
            buildExecutionProvider tenantB pathB "node1-tenantB" "node1-tenantB-unused"

        let node2A =
            buildExecutionProvider tenantA pathA "node2-tenantA" "node2-tenantA-output"

        let node2B =
            buildExecutionProvider tenantB pathB "node2-tenantB" "node2-tenantB-output"

        let node1 =
            buildNode portA [ $"127.0.0.1:%d{portA}" ] [ "api" ] tenantA tenantB node1A node1B

        let node2 =
            buildNode portB [ $"127.0.0.1:%d{portA}" ] [ "session" ] tenantA tenantB node2A node2B

        let disposeExecution () =
            for execution in [ node2B; node2A; node1B; node1A ] do
                execution.Provider.Dispose()

        let stopCluster (node: NodeRig) =
            try
                (node.Cluster :> IHostedService).StopAsync(CancellationToken.None).GetAwaiter().GetResult()
            with _ ->
                ()

        try
            // Eager binding initialization assembles the real scoped
            // serializer/resolver contexts but never starts borrowed hosted
            // services. Do not touch a receiving node's facade before its
            // cluster is live: all pre-traffic client access belongs to the
            // sender node.
            do! node1.Provider.GetRequiredService<ISessionHostContexts>().InitializeAsync cancellation

            do! node2.Provider.GetRequiredService<ISessionHostContexts>().InitializeAsync cancellation

            Assert.Equal(
                ClusterMode.StaticSeeds,
                node1.Provider.GetRequiredService<IOptions<LegateOptions>>().Value.Cluster.Mode
            )

            Assert.Equal(
                ClusterMode.StaticSeeds,
                node2.Provider.GetRequiredService<IOptions<LegateOptions>>().Value.Cluster.Mode
            )

            do! (node1.Cluster :> IHostedService).StartAsync cancellation
            do! (node2.Cluster :> IHostedService).StartAsync cancellation
            do! awaitMembers node1.Cluster 2
            do! awaitMembers node2.Cluster 2

            let clients = node1.Provider.GetRequiredService<ISessionClientFactory>()
            let senderA = clients.GetClient(node1.BindingA.Tenant)
            let senderB = clients.GetClient(node1.BindingB.Tenant)

            let addressA = SessionAddress(tenantA, sessionId)
            let resolver1 = node1.Cluster :> ISessionResolver

            // The public resolver performs the cold, address-scoped probe;
            // the direct region ask below proves the wire owner physically.
            let! resolved = resolver1.ResolveSessionAsync(addressA, cancellation)
            let! resolvedReply = resolved.Ask<obj>(SessionRouteProbe, cancellation)
            Assert.IsType<SessionRouteAccepted>(resolvedReply) |> ignore

            let! resolvedResponse =
                directRegionAsk node1.Cluster addressA.Key addressA.Key tenantA.Value (SessionRouteProbe :> obj)

            let node2Address = Cluster.Get(node2.Cluster.System).SelfAddress.ToString()
            Assert.Equal(node2Address, resolvedResponse.Owner)
            Assert.NotEqual<string>(Cluster.Get(node1.Cluster.System).SelfAddress.ToString(), resolvedResponse.Owner)

            let! fingerprintA = persistedFingerprint node1A.Store node1A.Abort node1A.Events tenantA sessionId
            let! fingerprintB = persistedFingerprint node1B.Store node1B.Abort node1B.Events tenantB sessionId

            let! unknownError =
                Assert.ThrowsAsync<SessionScopeRejectedException>(fun () ->
                    resolver1.ResolveSessionAsync(SessionAddress(tenantUnknown, sessionId), cancellation) :> Task)

            Assert.Equal(SessionScopeRejectionReason.ScopeUnavailable, unknownError.Reason)

            // Cold and cached direct-region mismatches both refuse before
            // the tenant lookup and therefore cannot mutate either file.
            for _ in 1..2 do
                let! refusal =
                    directRegionAsk node1.Cluster addressA.Key addressA.Key tenantB.Value (SessionRouteProbe :> obj)

                let typed = refusal.Payload :?> SessionScopeRejectedException
                Assert.Equal(SessionScopeRejectionReason.AddressMismatch, typed.Reason)

            let! afterA = persistedFingerprint node2A.Store node2A.Abort node2A.Events tenantA sessionId
            let! afterB = persistedFingerprint node2B.Store node2B.Abort node2B.Events tenantB sessionId

            Assert.Equal(fingerprintA, afterA)
            Assert.Equal(fingerprintB, afterB)
            assertNoMutations [ node1; node2 ]

            let! _ =
                SessionClientOperations.PromptAsync(
                    senderA,
                    sessionId,
                    UserMessage.Text "prompt-for-A",
                    DeliveryMode.Queue,
                    cancellation
                )

            let! completedA = awaitPersistedTurn senderA node2A.Store tenantA sessionId node2A.Chat 0L

            let sessionA, _eventsA = completedA
            Assert.Equal(SessionState.Idle, sessionA.State)
            Assert.False(sessionA.CurrentTurnId.HasValue)
            Assert.Equal("destination-A", sessionA.WorkspaceBinding)
            Assert.Equal("destination-A", destinationOf sessionA)
            Assert.Contains("node2-tenantA-output", node2A.Chat.Outputs)
            Assert.DoesNotContain("node2-tenantA-output", node1A.Chat.Outputs)
            Assert.Equal(0, node1A.Chat.Calls)
            Assert.Equal(1, node2A.Chat.Calls)
            Assert.Equal(0, node1B.Chat.Calls)
            Assert.Equal(0, node2B.Chat.Calls)
            Assert.True(node2A.StoreCounters.MutationCount > 0)
            Assert.Equal(0, node1A.StoreCounters.MutationCount)
            Assert.Equal(0, node1B.StoreCounters.MutationCount)
            Assert.Equal(0, node2B.StoreCounters.MutationCount)

            let! _ =
                SessionClientOperations.PromptAsync(
                    senderB,
                    sessionId,
                    UserMessage.Text "prompt-for-B",
                    DeliveryMode.Queue,
                    cancellation
                )

            let! completedB = awaitPersistedTurn senderB node2B.Store tenantB sessionId node2B.Chat 0L

            let sessionB, _eventsB = completedB
            Assert.Equal(SessionState.Idle, sessionB.State)
            Assert.False(sessionB.CurrentTurnId.HasValue)
            Assert.Equal("destination-B", sessionB.WorkspaceBinding)
            Assert.Equal("destination-B", destinationOf sessionB)
            Assert.Contains("node2-tenantB-output", node2B.Chat.Outputs)
            Assert.DoesNotContain("node2-tenantB-output", node1B.Chat.Outputs)
            Assert.Equal(0, node1A.Chat.Calls)
            Assert.Equal(1, node2A.Chat.Calls)
            Assert.Equal(0, node1B.Chat.Calls)
            Assert.Equal(1, node2B.Chat.Calls)
            Assert.Equal(0, node1B.StoreCounters.MutationCount)
            Assert.True(node2B.StoreCounters.MutationCount > 0)
        finally
            stopCluster node2
            stopCluster node1
            disposeExecution ()
            SqliteTestFixture.deleteDatabaseFiles pathA
            SqliteTestFixture.deleteDatabaseFiles pathB
    }

[<Fact>]
let ``issue395 routed operations fence replies, effects, subscriptions, and restart`` () : Task =
    task {
        let tenantA = TenantId.Create "tenant-a-395-ops"
        let tenantB = TenantId.Create "tenant-b-395-ops"
        let sessionId = SessionId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV"
        let agentA = AgentId.New()
        let agentB = AgentId.New()

        let pathA =
            Path.Combine(Path.GetTempPath(), "legate-issue395-ops-a-" + Guid.NewGuid().ToString("N") + ".db")

        let pathB =
            Path.Combine(Path.GetTempPath(), "legate-issue395-ops-b-" + Guid.NewGuid().ToString("N") + ".db")

        let portA = freePort ()
        let portB = freePort ()

        seedSession pathA tenantA sessionId agentA "destination-A"
        seedSession pathB tenantB sessionId agentB "destination-B"

        let senderEffect = RecordingEffectTool("sender-effect")
        let receiverEffect = RecordingEffectTool("receiver-effect")

        let askedPolicy () =
            Some(RecordingPermissionPolicy("record_effect", PermissionVerdict.Ask) :> IPermissionPolicy)

        let emptyTools = ResizeArray<AITool>() :> IReadOnlyList<AITool>

        let receiverTools =
            ResizeArray<AITool>([| receiverEffect.Tool |]) :> IReadOnlyList<AITool>

        let senderTools =
            ResizeArray<AITool>([| senderEffect.Tool |]) :> IReadOnlyList<AITool>

        let script (steps: ScriptStep array) (tools: IReadOnlyList<AITool>) (policy: IPermissionPolicy option) =
            {
                Steps = ResizeArray<ScriptStep>(steps) :> IReadOnlyList<ScriptStep>
                Tools = tools
                Policy = policy
            }

        let node1A =
            buildExecutionProviderWith tenantA pathA "ops-node1-tenantA" (script [||] emptyTools None)

        let node1B =
            buildExecutionProviderWith tenantB pathB "ops-node1-tenantB" (script [||] senderTools (askedPolicy ()))

        let node2A =
            buildExecutionProviderWith
                tenantA
                pathA
                "ops-node2-tenantA"
                (script
                    [|
                        ScriptStep.Text "queue-A"
                        ScriptStep.Text "inject-A"
                        ScriptStep.Text "interrupt-A"
                    |]
                    emptyTools
                    None)

        let node2B =
            buildExecutionProviderWith
                tenantB
                pathB
                "ops-node2-tenantB"
                (script
                    [|
                        ScriptStep.ToolCall("main-call", "record_effect")
                        ScriptStep.Text "main-complete"
                    |]
                    receiverTools
                    (askedPolicy ()))

        let node1 =
            buildNode portA [ $"127.0.0.1:%d{portA}" ] [ "api" ] tenantA tenantB node1A node1B

        let node2 =
            buildNode portB [ $"127.0.0.1:%d{portA}" ] [ "session" ] tenantA tenantB node2A node2B

        let stopCluster (node: NodeRig) =
            try
                match node.Cluster.System with
                | null -> ()
                | system ->
                    // This fixture is testing durable crash/rebind recovery:
                    // terminate the actor system itself so shutdown drain
                    // cannot turn a deliberately suspended target into a
                    // HostShutdown control decision before the fresh node
                    // rehydrates it.
                    system.Terminate().GetAwaiter().GetResult()
                    system.WhenTerminated.Wait(TimeSpan.FromSeconds 30.0) |> ignore
            with _ ->
                ()

        let disposeExecution (executions: ExecutionRig list) =
            for execution in executions do
                execution.Provider.Dispose()

        try
            do! node1.Provider.GetRequiredService<ISessionHostContexts>().InitializeAsync cancellation
            do! node2.Provider.GetRequiredService<ISessionHostContexts>().InitializeAsync cancellation
            do! (node1.Cluster :> IHostedService).StartAsync cancellation
            do! (node2.Cluster :> IHostedService).StartAsync cancellation
            do! awaitMembers node1.Cluster 2
            do! awaitMembers node2.Cluster 2

            let senderA = node1.BindingA.Client
            let senderB = node1.BindingB.Client
            let addressA = SessionAddress(tenantA, sessionId)
            let addressB = SessionAddress(tenantB, sessionId)

            let runPrompt client store tenant id chat message delivery cursor =
                task {
                    let! _ =
                        SessionClientOperations.PromptAsync(
                            client,
                            id,
                            UserMessage.Text message,
                            delivery,
                            cancellation
                        )

                    return! awaitPersistedTurn client store tenant id chat cursor
                }

            let! _ = runPrompt senderA node2A.Store tenantA sessionId node2A.Chat "queue" DeliveryMode.Queue 0L
            let! cursorA1 = cursorOf node2A.Events tenantA sessionId
            let! _ = runPrompt senderA node2A.Store tenantA sessionId node2A.Chat "inject" DeliveryMode.Inject cursorA1
            let! cursorA2 = cursorOf node2A.Events tenantA sessionId

            let! thirdA =
                runPrompt senderA node2A.Store tenantA sessionId node2A.Chat "interrupt" DeliveryMode.Interrupt cursorA2

            Assert.Equal("destination-A", destinationOf (fst thirdA))
            Assert.Equal<string list>([ "queue-A"; "inject-A"; "interrupt-A" ], node2A.Chat.Outputs |> Array.toList)
            Assert.Empty(node1A.Chat.Outputs)

            let! snapshotResponse =
                directRegionAsk
                    node1.Cluster
                    addressA.Key
                    addressA.Key
                    tenantA.Value
                    (SessionActor.SuspendableGetSnapshot :> obj)

            Assert.Equal(Cluster.Get(node2.Cluster.System).SelfAddress.ToString(), snapshotResponse.Owner)
            Assert.IsType<SessionSnapshot>(snapshotResponse.Payload) |> ignore

            let! compacted = SessionClientOperations.CompactAsync(senderA, sessionId, cancellation)
            Assert.NotNull(compacted)

            let reboundAgent = AgentId.New()
            let! rebound = SessionClientOperations.SetAgentAsync(senderA, sessionId, reboundAgent, cancellation)
            Assert.Equal(reboundAgent, rebound.AgentId)

            let! closeResponse =
                directRegionAsk
                    node1.Cluster
                    addressA.Key
                    addressA.Key
                    tenantA.Value
                    (SessionActor.SuspendableCloseSession cancellation :> obj)

            let closedA = closeResponse.Payload :?> Session
            Assert.Equal(SessionState.Closed, closedA.State)

            let closedPrompt =
                SessionClientOperations.PromptAsync(
                    senderA,
                    sessionId,
                    UserMessage.Text "closed",
                    DeliveryMode.Queue,
                    cancellation
                )

            let! _ = Assert.ThrowsAsync<InvalidSessionStateException>(fun () -> closedPrompt :> Task)

            let! _ =
                SessionClientOperations.PromptAsync(
                    senderB,
                    sessionId,
                    UserMessage.Text "permission",
                    DeliveryMode.Queue,
                    cancellation
                )

            let! requestId = awaitPermissionRequest senderB sessionId
            let! targetRead = SessionClientOperations.ReadAbortTargetAsync(senderB, sessionId, cancellation)

            let target =
                match targetRead with
                | null -> failwith "The suspended routed session had no abort target."
                | present -> present

            Assert.Equal(ControlTargetState.Active, target.State)
            Assert.Null(target.Stop)

            let! beforeWrong = persistedFingerprint node2B.Store node2B.Abort node2B.Events tenantB sessionId

            let! _ =
                Assert.ThrowsAsync<ReplyMismatchException>(fun () ->
                    SessionClientOperations.ReplyAsync(
                        senderB,
                        sessionId,
                        PermissionDecision("wrong-request", PermissionDecisionKind.AllowOnce),
                        cancellation
                    )
                    :> Task)

            let! afterWrong = persistedFingerprint node2B.Store node2B.Abort node2B.Events tenantB sessionId
            Assert.Equal(beforeWrong, afterWrong)
            Assert.Empty(receiverEffect.Calls)

            let! _ =
                SessionClientOperations.ReplyAsync(
                    senderB,
                    sessionId,
                    PermissionDecision(requestId, PermissionDecisionKind.AllowOnce),
                    cancellation
                )

            let! initialEffect = receiverEffect.WaitForCallAsync(cancellation)
            Assert.Equal("receiver-effect", initialEffect)
            let! _ = awaitPersistedTurn senderB node2B.Store tenantB sessionId node2B.Chat 0L
            Assert.Empty(senderEffect.Calls)

            stopCluster node2
            stopCluster node1
            disposeExecution [ node2B; node2A; node1B; node1A ]

            let fresh1Effect = RecordingEffectTool("fresh-receiver-effect")
            let fresh2Effect = RecordingEffectTool("fresh-sender-effect")

            let fresh1A =
                buildExecutionProviderWith tenantA pathA "fresh-node1-tenantA" (script [||] emptyTools None)

            let fresh1B =
                buildExecutionProviderWith
                    tenantB
                    pathB
                    "fresh-node1-tenantB"
                    (script
                        [|
                            ScriptStep.Text "restart-complete"
                            ScriptStep.ToolCall("abort-call", "record_effect")
                            ScriptStep.Text "abort-never"
                        |]
                        (ResizeArray<AITool>([| fresh1Effect.Tool |]) :> IReadOnlyList<AITool>)
                        (askedPolicy ()))

            let fresh2A =
                buildExecutionProviderWith tenantA pathA "fresh-node2-tenantA" (script [||] emptyTools None)

            let fresh2B =
                buildExecutionProviderWith
                    tenantB
                    pathB
                    "fresh-node2-tenantB"
                    (script [||] (ResizeArray<AITool>([| fresh2Effect.Tool |]) :> IReadOnlyList<AITool>) None)

            let freshNode1 =
                buildNode portA [ $"127.0.0.1:%d{portB}" ] [ "session" ] tenantA tenantB fresh1A fresh1B

            let freshNode2 =
                buildNode portB [ $"127.0.0.1:%d{portB}" ] [ "api" ] tenantA tenantB fresh2A fresh2B

            try
                do! freshNode1.Provider.GetRequiredService<ISessionHostContexts>().InitializeAsync cancellation
                do! freshNode2.Provider.GetRequiredService<ISessionHostContexts>().InitializeAsync cancellation
                do! (freshNode2.Cluster :> IHostedService).StartAsync cancellation
                do! (freshNode1.Cluster :> IHostedService).StartAsync cancellation
                do! awaitMembers freshNode1.Cluster 2
                do! awaitMembers freshNode2.Cluster 2

                let clients = freshNode2.Provider.GetRequiredService<ISessionClientFactory>()
                let freshSenderA = clients.GetClient(freshNode2.BindingA.Tenant)
                let freshSenderB = clients.GetClient(freshNode2.BindingB.Tenant)
                let! resolved = (freshNode2.Cluster :> ISessionResolver).ResolveSessionAsync(addressB, cancellation)
                let! probe = resolved.Ask<obj>(SessionRouteProbe, cancellation)
                Assert.IsType<SessionRouteAccepted>(probe) |> ignore

                let! ownerAfter =
                    directRegionAsk
                        freshNode2.Cluster
                        addressB.Key
                        addressB.Key
                        tenantB.Value
                        (SessionRouteProbe :> obj)

                Assert.Equal<string>(Cluster.Get(freshNode1.Cluster.System).SelfAddress.ToString(), ownerAfter.Owner)
                Assert.NotEqual<string>(Cluster.Get(freshNode2.Cluster.System).SelfAddress.ToString(), ownerAfter.Owner)

                // Rebind a subscriber from both tenants after the restart;
                // disposing the enumerators is the detach seam.
                let! _ =
                    awaitSessionEvent freshSenderA (SessionId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAV") 0L (fun _ -> true)

                let! _ = awaitSessionEvent freshSenderB sessionId 0L (fun event -> event :? PermissionRequestedEvent)

                let! freshStored = fresh1B.Store.GetSession(tenantB, sessionId, cancellation)
                let freshSession = requireSession sessionId freshStored
                Assert.NotEqual(SessionState.Closed, freshSession.State)
                let! freshCursor = cursorOf fresh1B.Events tenantB sessionId

                let! _ =
                    SessionClientOperations.PromptAsync(
                        freshSenderB,
                        sessionId,
                        UserMessage.Text "after-restart",
                        DeliveryMode.Queue,
                        cancellation
                    )

                let! completed =
                    awaitPersistedTurn freshSenderB fresh1B.Store tenantB sessionId fresh1B.Chat freshCursor

                Assert.Equal(SessionState.Idle, (fst completed).State)
                Assert.Empty(fresh2Effect.Calls)
                Assert.Contains("restart-complete", fresh1B.Chat.Outputs)
                Assert.Empty(fresh2B.Chat.Outputs)

                Assert.Empty(fresh1Effect.Calls)
            finally
                stopCluster freshNode2
                stopCluster freshNode1
                disposeExecution [ fresh2B; fresh2A; fresh1B; fresh1A ]
        finally
            stopCluster node2
            stopCluster node1
            disposeExecution [ node2B; node2A; node1B; node1A ]
            SqliteTestFixture.deleteDatabaseFiles pathA
            SqliteTestFixture.deleteDatabaseFiles pathB
    }

[<Fact>]
let ``issue395 shared SQLite keeps distinct tenant sessions inbox and control reads isolated`` () : Task =
    task {
        let tenantA = TenantId.Create "tenant-a-395-shared"
        let tenantB = TenantId.Create "tenant-b-395-shared"
        let sessionA = SessionId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAA"
        let sessionB = SessionId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAB"
        let agentA = AgentId.New()
        let agentB = AgentId.New()

        let path =
            Path.Combine(Path.GetTempPath(), "legate-issue395-shared-" + Guid.NewGuid().ToString("N") + ".db")

        try
            seedSession path tenantA sessionA agentA "shared-A"
            seedSession path tenantB sessionB agentB "shared-B"

            let database = SqliteDatabase.Open(path, TimeProvider.System)

            try
                let store = SqliteStoreFactory.sessionStore database
                let events = SqliteStoreFactory.eventStore database
                let control = store :?> ISessionAbortControlStore

                let! storedA = store.GetSession(tenantA, sessionA, cancellation)
                let! storedB = store.GetSession(tenantB, sessionB, cancellation)
                Assert.Equal("shared-A", destinationOf (requireSession sessionA storedA))
                Assert.Equal("shared-B", destinationOf (requireSession sessionB storedB))

                let! crossA = store.GetSession(tenantA, sessionB, cancellation)
                let! crossB = store.GetSession(tenantB, sessionA, cancellation)
                Assert.Null(crossA)
                Assert.Null(crossB)

                let! _ =
                    store.AppendInboxMessage(
                        tenantA,
                        sessionA,
                        UserMessagePayload(UserMessage.Text "only-A"),
                        DeliveryMode.Queue,
                        cancellation
                    )

                let! pendingA = store.ReadPendingInbox(tenantA, sessionA, cancellation)
                let! pendingB = store.ReadPendingInbox(tenantB, sessionB, cancellation)
                Assert.Single(pendingA) |> ignore
                Assert.Empty(pendingB)

                let! eventsA = events.Replay(tenantA, sessionB, 0L, 100, cancellation)
                let! eventsB = events.Replay(tenantB, sessionA, 0L, 100, cancellation)
                Assert.IsType<EventReplayUnknownSession>(eventsA) |> ignore
                Assert.IsType<EventReplayUnknownSession>(eventsB) |> ignore

                let! controlA = control.ReadAbortTarget(tenantA, sessionA, cancellation)
                let! controlB = control.ReadAbortTarget(tenantB, sessionB, cancellation)
                Assert.Null(controlA)
                Assert.Null(controlB)
            finally
                (database :> IDisposable).Dispose()

            let reopened = SqliteDatabase.Open(path, TimeProvider.System)

            try
                let store = SqliteStoreFactory.sessionStore reopened
                let! afterReopenA = store.GetSession(tenantA, sessionA, cancellation)
                let! afterReopenB = store.GetSession(tenantB, sessionB, cancellation)
                Assert.Equal("shared-A", destinationOf (requireSession sessionA afterReopenA))
                Assert.Equal("shared-B", destinationOf (requireSession sessionB afterReopenB))
            finally
                (reopened :> IDisposable).Dispose()
        finally
            SqliteTestFixture.deleteDatabaseFiles path
    }

[<Fact>]
let ``issue395 malformed decodable subscription scope reaches typed routing refusal`` () =
    let address = SessionAddress(TenantId.Create "tenant-wire-395", SessionId.New())

    let subscribe = WireDtos.SubscribeDto()
    subscribe.Tenant <- ""
    subscribe.SessionId <- "not-a-session"
    subscribe.SubscriberToken <- ""

    let decodedSubscribe =
        WireDtos.ofWire subscribe :?> CrossNodeSubscriptions.CrossNodeSubscribeRequest

    let subscribeError =
        Assert.Throws<SessionScopeRejectedException>(fun () ->
            SessionRouting.validatePayload address (decodedSubscribe :> obj))

    Assert.Equal(SessionScopeRejectionReason.AddressMismatch, subscribeError.Reason)

    let batch = WireDtos.EventBatchDto()
    batch.SessionId <- "not-a-session"
    batch.SubscriberToken <- ""
    batch.Events <- [||]

    let decodedBatch =
        WireDtos.ofWire batch :?> CrossNodeSubscriptions.CrossNodeEventBatch

    let batchError =
        Assert.Throws<SessionScopeRejectedException>(fun () ->
            SessionRouting.validateResponse address (decodedBatch :> obj))

    Assert.Equal(SessionScopeRejectionReason.ResponseMismatch, batchError.Reason)

[<Fact>]
let issue395FreshRoleswapRepliesToPersistedSuspendedOriginalRequest () : Task =
    task {
        let tenantA = TenantId.Create "tenant-a-395-restart"
        let tenantB = TenantId.Create "tenant-b-395-restart"
        let sessionId = SessionId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAD"

        let pathA =
            Path.Combine(Path.GetTempPath(), "legate-issue395-restart-a-" + Guid.NewGuid().ToString("N") + ".db")

        let pathB =
            Path.Combine(Path.GetTempPath(), "legate-issue395-restart-b-" + Guid.NewGuid().ToString("N") + ".db")

        let portA = freePort ()
        let portB = freePort ()

        seedSession pathA tenantA sessionId (AgentId.New()) "restart-destination-A"
        seedSession pathB tenantB sessionId (AgentId.New()) "restart-destination-B"

        let effectBefore = RecordingEffectTool("before-restart")

        let effectTools =
            ResizeArray<AITool>([| effectBefore.Tool |]) :> IReadOnlyList<AITool>

        let emptyTools = ResizeArray<AITool>() :> IReadOnlyList<AITool>

        let asked =
            Some(RecordingPermissionPolicy("record_effect", PermissionVerdict.Ask) :> IPermissionPolicy)

        let script (steps: ScriptStep array) (selectedTools: IReadOnlyList<AITool>) (policy: IPermissionPolicy option) =
            {
                Steps = ResizeArray<ScriptStep>(steps) :> IReadOnlyList<ScriptStep>
                Tools = selectedTools
                Policy = policy
            }

        let node1A =
            buildExecutionProviderWith tenantA pathA "restart-node1-A" (script [||] emptyTools None)

        let node1B =
            buildExecutionProviderWith tenantB pathB "restart-node1-B" (script [||] emptyTools None)

        let node2A =
            buildExecutionProviderWith tenantA pathA "restart-node2-A" (script [||] emptyTools None)

        let node2B =
            buildExecutionProviderWith
                tenantB
                pathB
                "restart-node2-B"
                (script
                    [|
                        ScriptStep.ToolCall("before", "record_effect")
                    |]
                    effectTools
                    asked)

        let node1 =
            buildNode portA [ $"127.0.0.1:%d{portA}" ] [ "api" ] tenantA tenantB node1A node1B

        let node2 =
            buildNode portB [ $"127.0.0.1:%d{portA}" ] [ "session" ] tenantA tenantB node2A node2B

        let stopCluster (node: NodeRig) =
            try
                match node.Cluster.System with
                | null -> ()
                | system ->
                    system.Terminate().GetAwaiter().GetResult()
                    system.WhenTerminated.Wait(TimeSpan.FromSeconds 30.0) |> ignore
            with _ ->
                ()

        let disposeExecution (executions: ExecutionRig list) =
            for execution in executions do
                execution.Provider.Dispose()

        try
            do! node1.Provider.GetRequiredService<ISessionHostContexts>().InitializeAsync cancellation
            do! node2.Provider.GetRequiredService<ISessionHostContexts>().InitializeAsync cancellation
            do! (node1.Cluster :> IHostedService).StartAsync cancellation
            do! (node2.Cluster :> IHostedService).StartAsync cancellation
            do! awaitMembers node1.Cluster 2
            do! awaitMembers node2.Cluster 2

            let sender = node1.BindingB.Client
            let address = SessionAddress(tenantB, sessionId)
            let! beforeA = persistedFingerprint node2A.Store node2A.Abort node2A.Events tenantA sessionId

            let! _ =
                SessionClientOperations.PromptAsync(
                    sender,
                    sessionId,
                    UserMessage.Text "needs-permission",
                    DeliveryMode.Queue,
                    cancellation
                )

            let! originalRequestId = awaitPermissionRequest sender sessionId
            let! stored = node2B.Store.GetSession(tenantB, sessionId, cancellation)
            let suspended = requireSession sessionId stored
            Assert.Equal(SessionState.WaitingForInput, suspended.State)
            Assert.Equal("restart-destination-B", destinationOf suspended)
            Assert.Empty(effectBefore.Calls)

            stopCluster node2
            stopCluster node1
            disposeExecution [ node2B; node2A; node1B; node1A ]

            let persistedDestination =
                let database = SqliteDatabase.Open(pathB, TimeProvider.System)

                try
                    let store = SqliteStoreFactory.sessionStore database

                    let stored =
                        store.GetSession(tenantB, sessionId, cancellation).GetAwaiter().GetResult()

                    destinationOf (requireSession sessionId stored)
                finally
                    (database :> IDisposable).Dispose()

            let effectAfter = RecordingEffectTool("rehydrated-" + persistedDestination)

            let fresh1A =
                buildExecutionProviderWith tenantA pathA "fresh-restart-node1-A" (script [||] emptyTools None)

            let fresh1BTools =
                ResizeArray<AITool>([| effectAfter.Tool |]) :> IReadOnlyList<AITool>

            let fresh1B =
                buildExecutionProviderWith
                    tenantB
                    pathB
                    "fresh-restart-node1-B"
                    (script
                        [|
                            ScriptStep.ToolCall("after", "record_effect")
                            ScriptStep.Text "restart-reply-complete"
                        |]
                        fresh1BTools
                        (Some(RecordingPermissionPolicy("record_effect", PermissionVerdict.Allow) :> IPermissionPolicy)))

            let fresh2A =
                buildExecutionProviderWith tenantA pathA "fresh-restart-node2-A" (script [||] emptyTools None)

            let fresh2B =
                buildExecutionProviderWith tenantB pathB "fresh-restart-node2-B" (script [||] emptyTools None)

            let freshNode1 =
                buildNode portA [ $"127.0.0.1:%d{portB}" ] [ "session" ] tenantA tenantB fresh1A fresh1B

            let freshNode2 =
                buildNode portB [ $"127.0.0.1:%d{portB}" ] [ "api" ] tenantA tenantB fresh2A fresh2B

            try
                do! freshNode1.Provider.GetRequiredService<ISessionHostContexts>().InitializeAsync cancellation
                do! freshNode2.Provider.GetRequiredService<ISessionHostContexts>().InitializeAsync cancellation
                do! (freshNode2.Cluster :> IHostedService).StartAsync cancellation
                do! (freshNode1.Cluster :> IHostedService).StartAsync cancellation
                do! awaitMembers freshNode1.Cluster 2
                do! awaitMembers freshNode2.Cluster 2

                let freshSender = freshNode2.BindingB.Client
                let! resolved = (freshNode2.Cluster :> ISessionResolver).ResolveSessionAsync(address, cancellation)
                let! probe = resolved.Ask<obj>(SessionRouteProbe, cancellation)
                Assert.IsType<SessionRouteAccepted>(probe) |> ignore

                let! owner =
                    directRegionAsk freshNode2.Cluster address.Key address.Key tenantB.Value (SessionRouteProbe :> obj)

                Assert.Equal<string>(Cluster.Get(freshNode1.Cluster.System).SelfAddress.ToString(), owner.Owner)
                Assert.NotEqual<string>(Cluster.Get(freshNode2.Cluster.System).SelfAddress.ToString(), owner.Owner)

                let! freshStored = fresh1B.Store.GetSession(tenantB, sessionId, cancellation)
                let freshSession = requireSession sessionId freshStored
                Assert.Equal("restart-destination-B", destinationOf freshSession)
                Assert.Equal("restart-destination-B", persistedDestination)
                let! freshTarget = fresh1B.Abort.ReadAbortTarget(tenantB, sessionId, cancellation)

                match freshTarget with
                | null -> failwith "The suspended target disappeared during the roleswap."
                | target ->
                    Assert.Equal(ControlTargetState.Active, target.State)
                    Assert.Null(target.Stop)

                let! freshCursor = cursorOf fresh1B.Events tenantB sessionId

                let! _ =
                    SessionClientOperations.ReplyAsync(
                        freshSender,
                        sessionId,
                        PermissionDecision(originalRequestId, PermissionDecisionKind.AllowOnce),
                        cancellation
                    )

                let! completed = awaitPersistedTurn freshSender fresh1B.Store tenantB sessionId fresh1B.Chat freshCursor
                Assert.Equal(SessionState.Idle, (fst completed).State)
                Assert.Contains("rehydrated-restart-destination-B", effectAfter.Calls)
                Assert.Empty(fresh2B.Chat.Outputs)
                let! afterA = persistedFingerprint fresh1A.Store fresh1A.Abort fresh1A.Events tenantA sessionId
                Assert.Equal(beforeA, afterA)
            finally
                stopCluster freshNode2
                stopCluster freshNode1
                disposeExecution [ fresh2B; fresh2A; fresh1B; fresh1A ]
        finally
            stopCluster node2
            stopCluster node1
            disposeExecution [ node2B; node2A; node1B; node1A ]
            SqliteTestFixture.deleteDatabaseFiles pathA
            SqliteTestFixture.deleteDatabaseFiles pathB
    }

[<Fact>]
let ``issue395 shared SQLite rejects duplicate id and preserves data control and event rows after reopen`` () : Task =
    task {
        let tenant = TenantId.Create "tenant-duplicate-395"
        let otherTenant = TenantId.Create "tenant-duplicate-395-other"
        let sessionId = SessionId.Parse "01ARZ3NDEKTSV4RRFFQ69G5FAC"

        let path =
            Path.Combine(Path.GetTempPath(), "legate-issue395-duplicate-" + Guid.NewGuid().ToString("N") + ".db")

        try
            let database = SqliteDatabase.Open(path, TimeProvider.System)

            try
                let store = SqliteStoreFactory.sessionStore database
                let events = SqliteStoreFactory.eventStore database
                let control = store :?> ISessionAbortControlStore
                let options = SessionOptions()
                let metadata = Dictionary<string, string>()
                metadata["destination"] <- "duplicate-preserved"
                options.Metadata <- metadata :> IReadOnlyDictionary<string, string>

                let original =
                    {
                        Id = sessionId
                        Tenant = tenant
                        AgentId = AgentId.New()
                        Title = "original"
                        State = SessionState.Idle
                        CurrentTurnId = Nullable()
                        CreatedAt = DateTimeOffset.MinValue
                        UpdatedAt = DateTimeOffset.MinValue
                        ClosedAt = Nullable()
                        WorkspaceBinding = "duplicate-preserved"
                        Options = options
                        PermissionGrants = Array.empty<string> :> IReadOnlyList<string>
                    }

                let! _ = store.CreateSession(tenant, original, cancellation)

                let duplicateAcrossTenants =
                    { original with
                        Tenant = otherTenant
                        Title = "must-not-overwrite"
                        WorkspaceBinding = "must-not-overwrite"
                        Options = SessionOptions()
                    }

                let! duplicateError =
                    Assert.ThrowsAsync<SqliteStorageException>(fun () ->
                        store.CreateSession(otherTenant, duplicateAcrossTenants, cancellation) :> Task)

                Assert.Contains("UNIQUE", duplicateError.Message, StringComparison.OrdinalIgnoreCase)

                let! inbox =
                    store.AppendInboxMessage(
                        tenant,
                        sessionId,
                        UserMessagePayload(UserMessage.Text "preserved-inbox"),
                        DeliveryMode.Queue,
                        cancellation
                    )

                let! lease =
                    store.ClaimNextTurn(tenant, sessionId, "duplicate-owner", TimeSpan.FromMinutes 5.0, cancellation)

                let claim = (lease :?> TurnLeaseRenewed).Claim
                let! _ = control.BindControlTarget(tenant, sessionId, claim.TurnId, inbox.Position, claim, cancellation)

                let! queued =
                    store.AppendInboxMessage(
                        tenant,
                        sessionId,
                        UserMessagePayload(UserMessage.Text "preserved-pending"),
                        DeliveryMode.Interrupt,
                        cancellation
                    )

                let event =
                    TextDeltaEvent(
                        sessionId,
                        claim.TurnId,
                        Nullable<int64>(1L),
                        DateTimeOffset.UtcNow,
                        "preserved-event"
                    )
                    :> SessionEvent

                let! _ = events.Append(tenant, sessionId, claim.Token, [| event |], cancellation)

                let! _ =
                    Assert.ThrowsAsync<InvalidSessionStateException>(fun () ->
                        store.CreateSession(tenant, { original with Title = "duplicate" }, cancellation) :> Task)

                let! stored = store.GetSession(tenant, sessionId, cancellation)
                let! rejectedTenantRead = store.GetSession(otherTenant, sessionId, cancellation)
                let! pending = store.ReadPendingInbox(tenant, sessionId, cancellation)

                let! rejectedTenantPending =
                    Assert.ThrowsAsync<SessionNotFoundException>(fun () ->
                        store.ReadPendingInbox(otherTenant, sessionId, cancellation) :> Task)

                let! target = control.ReadAbortTarget(tenant, sessionId, cancellation)
                let! replay = events.Replay(tenant, sessionId, 0L, 100, cancellation)
                let! rejectedTenantReplay = events.Replay(otherTenant, sessionId, 0L, 100, cancellation)

                Assert.Equal("duplicate-preserved", destinationOf (requireSession sessionId stored))
                Assert.Null(rejectedTenantRead)
                Assert.Equal(sessionId, rejectedTenantPending.SessionId)
                Assert.Contains(pending, fun row -> row.Position = queued.Position)

                match target with
                | null -> failwith "The duplicate rejection lost the control target."
                | target -> Assert.Equal(ControlTargetState.Active, target.State)

                let page = Assert.IsType<EventReplayPage>(replay)
                Assert.Contains(page.Events, fun row -> row :? TextDeltaEvent)
                Assert.IsType<EventReplayUnknownSession>(rejectedTenantReplay) |> ignore
            finally
                (database :> IDisposable).Dispose()

            let reopened = SqliteDatabase.Open(path, TimeProvider.System)

            try
                let store = SqliteStoreFactory.sessionStore reopened
                let events = SqliteStoreFactory.eventStore reopened
                let control = store :?> ISessionAbortControlStore
                let! stored = store.GetSession(tenant, sessionId, cancellation)
                let! rejectedTenantRead = store.GetSession(otherTenant, sessionId, cancellation)
                let! pending = store.ReadPendingInbox(tenant, sessionId, cancellation)

                let! rejectedTenantPending =
                    Assert.ThrowsAsync<SessionNotFoundException>(fun () ->
                        store.ReadPendingInbox(otherTenant, sessionId, cancellation) :> Task)

                let! target = control.ReadAbortTarget(tenant, sessionId, cancellation)
                let! replay = events.Replay(tenant, sessionId, 0L, 100, cancellation)
                let! rejectedTenantReplay = events.Replay(otherTenant, sessionId, 0L, 100, cancellation)

                Assert.Equal("duplicate-preserved", destinationOf (requireSession sessionId stored))
                Assert.Null(rejectedTenantRead)
                Assert.Equal(sessionId, rejectedTenantPending.SessionId)
                Assert.Contains(pending, fun row -> row.Payload :? UserMessagePayload)

                match target with
                | null -> failwith "The reopened duplicate rejection lost the control target."
                | target -> Assert.Equal(ControlTargetState.Active, target.State)

                let page = Assert.IsType<EventReplayPage>(replay)
                Assert.Contains(page.Events, fun row -> row :? TextDeltaEvent)
                Assert.IsType<EventReplayUnknownSession>(rejectedTenantReplay) |> ignore
            finally
                (reopened :> IDisposable).Dispose()
        finally
            SqliteTestFixture.deleteDatabaseFiles path
    }
