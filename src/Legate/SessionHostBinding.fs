// SPDX-License-Identifier: Apache-2.0
namespace Legate

open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.AI
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Options

/// A trusted tenant bound to an existing host-owned, long-lived execution provider.
/// Stop the node before disposing the provider. A descriptor belongs to one node lifetime.
[<Sealed>]
type SessionHostBinding(tenant: TenantId, provider: Func<IServiceProvider, IServiceProvider>) =
    do ArgumentNullException.ThrowIfNull(provider)

    do
        if String.IsNullOrWhiteSpace tenant.Value then
            invalidArg (nameof tenant) "A host binding requires an explicit tenant."

    let gate = obj ()
    let mutable owner: obj option = None
    let mutable client: Lazy<obj> option = None

    /// The host-authorized immutable tenant identity.
    member _.Tenant = tenant

    /// The tenant-bound client, available only after successful node initialization.
    member _.Client: SessionClient =
        lock gate (fun () ->
            match client with
            | Some cached -> cached.Value :?> SessionClient
            | None -> invalidOp "Start the Legate node successfully before accessing a bound client.")

    member internal _.Claim(node: obj) =
        lock gate (fun () ->
            if owner.IsSome then
                invalidOp "A SessionHostBinding cannot be reused on another node lifetime."

            owner <- Some node)

    member internal _.Bind(_node: obj, root: IServiceProvider) =
        let borrowed =
            try
                provider.Invoke root
            with error ->
                reraise ()

        if isNull (box borrowed) then
            invalidOp "A SessionHostBinding callback returned no provider."

        borrowed

    member internal _.Publish(cached: Lazy<obj>) =
        lock gate (fun () -> client <- Some cached)

type internal SessionHostContextRegistry(root: IServiceProvider) as this =
    let gate = obj ()
    let mutable initializing: Task option = None

    let mutable contexts: IReadOnlyDictionary<TenantId, SessionExecutionContext> option =
        None

    let mutable defaultTenant = TenantId.Default

    let mutable closed = false

    let materialize () =
        let descriptors = root.GetServices<SessionHostBinding>() |> Seq.toArray
        let tenant = root.GetRequiredService<SessionClientOptions>().Tenant
        let nodeMode = root.GetRequiredService<IOptions<LegateOptions>>().Value.Cluster.Mode

        // A receiving-only node deliberately has no execution binding.  Do
        // not manufacture an identity binding: doing so turns a missing
        // registry into an accidental root-provider dependency.
        // With no explicit descriptors the root is exactly one deferred
        // self-binding.  It is still a complete execution binding; startup
        // validation reports every missing dependency before this callback
        // can publish anything.  An empty root is therefore a
        // misconfiguration, never a silently started receiver.
        let bindings =
            if descriptors.Length = 0 then
                [|
                    SessionHostBinding(tenant, Func<IServiceProvider, IServiceProvider>(id))
                |]
            else
                descriptors

        let seen = HashSet<TenantId>()

        for binding in bindings do
            if isNull (box binding) || not (seen.Add binding.Tenant) then
                invalidOp "Session host bindings must be non-null and have distinct tenants."

        // Claim every descriptor before any callback or dependency work.  A
        // descriptor that participated in a failed initialization is never
        // silently reusable on a later node.
        for binding in bindings do
            binding.Claim(this)

        let resolver =
            { new ISessionResolver with
                member _.ResolveSessionAsync(key, token) =
                    let hosted = root.GetServices<Microsoft.Extensions.Hosting.IHostedService>()

                    let selected =
                        hosted
                        |> Seq.pick (fun service ->
                            match nodeMode, service with
                            | ClusterMode.Local, (:? LocalActorSystemService as local) ->
                                Some(local :> ISessionResolver)
                            | (ClusterMode.StaticSeeds | ClusterMode.Kubernetes),
                              (:? ClusterActorSystemService as cluster) -> Some(cluster :> ISessionResolver)
                            | _ -> None)

                    selected.ResolveSessionAsync(key, token)
            }

        let temporary = Dictionary<TenantId, SessionExecutionContext>()
        let temporaryLifetimes = ResizeArray<SessionSubscriptionLifetime>()
        let temporaryTrackers = ResizeArray<ExecutionWorkTracker>()
        let validatedProviders = ResizeArray<TenantId * IServiceProvider>()
        let mutable published = false

        let validateExecutionProvider (provider: IServiceProvider) : unit =
            let missing = ResizeArray<string>()
            let store = provider.GetService<ISessionStore>()

            if isNull (box store) then
                missing.Add("ISessionStore")
            elif not (store :? ISessionAbortControlStore) then
                missing.Add("ISessionAbortControlStore")

            if isNull (box (provider.GetService<ISessionEventStore>())) then
                missing.Add("ISessionEventStore")

            if isNull (box (provider.GetService<IChatClient>())) then
                missing.Add("IChatClient")

            if isNull (box (provider.GetService<IWorkspaceRuntime>())) then
                missing.Add("IWorkspaceRuntime")

            if Seq.isEmpty (provider.GetServices<ILlmProvider>()) then
                missing.Add("ILlmProvider")

            if missing.Count > 0 then
                raise (
                    InvalidOperationException(
                        "Legate execution binding is missing required registrations: "
                        + String.Join(", ", missing)
                    )
                )

        try
            // Resolve and validate every callback first.  No provider-owned
            // event bus or client graph is materialized until every binding
            // has passed, so a later callback/dependency failure cannot leave
            // a partial journal hook or execution context behind.
            for binding in bindings do
                let provider = binding.Bind(this, root)
                let options = provider.GetRequiredService<SessionClientOptions>()

                if options.Tenant <> binding.Tenant then
                    invalidOp "A SessionHostBinding tenant must match its provider's SessionClientOptions.Tenant."

                // Every binding, including the deferred root self-binding,
                // is a complete execution boundary.  Never fall back to the
                // node root for a missing dependency.
                validateExecutionProvider provider

                let options = provider.GetRequiredService<IOptions<LegateOptions>>().Value

                match options.Validate() with
                | null -> ()
                | _ -> invalidOp "A bound execution provider has invalid LegateOptions."

                SessionExpiryStartup.requireDurableBlobStore provider

                let sessions =
                    if isNull (box options.Sessions) then
                        SessionsOptions()
                    else
                        options.Sessions

                let subscriptionOptions = SessionClientWiring.subscriptionOptionsOf sessions

                match subscriptionOptions.Validate() with
                | null -> ()
                | violation -> invalidArg "Sessions" violation

                validatedProviders.Add(binding.Tenant, provider)

            for tenant, provider in validatedProviders do
                let lifetime = new SessionSubscriptionLifetime()
                let tracker = new ExecutionWorkTracker()
                temporaryLifetimes.Add(lifetime)
                temporaryTrackers.Add(tracker)

                let context =
                    SessionClientWiring.assembleContext provider resolver nodeMode lifetime tracker

                temporary.Add(tenant, context)

            // Force every lazy client before publishing any descriptor.  A
            // late provider/client construction failure must not leave an
            // earlier binding visible through either the registry or its
            // descriptor.
            let clients =
                bindings |> Array.map (fun binding -> binding, temporary[binding.Tenant].Client)

            lock gate (fun () ->
                if closed then
                    raise (SessionScopeRejectedException(SessionScopeRejectionReason.NodeStopping))

                for binding, client in clients do
                    binding.Publish(client)

                contexts <- Some(temporary :> IReadOnlyDictionary<_, _>)
                defaultTenant <- tenant)

            published <- true
        with _ when not published ->
            for tracker in temporaryTrackers do
                tracker.CloseAdmission()

            for lifetime in temporaryLifetimes do
                lifetime.BeginClose()

            reraise ()

    interface ISessionHostContexts with
        member _.HasDeclaredBindings =
            not (root.GetServices<SessionHostBinding>() |> Seq.isEmpty)

        member _.InitializeAsync(_token) =
            lock gate (fun () ->
                match initializing with
                | Some started -> started
                | None ->
                    let completion =
                        TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)

                    initializing <- Some completion.Task

                    try
                        materialize ()

                        completion.SetResult()
                    with error ->
                        completion.SetException error

                    completion.Task)

        member _.DefaultTenant = lock gate (fun () -> defaultTenant)

        member _.Get(tenant) =
            lock gate (fun () ->
                if closed then
                    raise (SessionScopeRejectedException(SessionScopeRejectionReason.NodeStopping))

                match contexts with
                | Some available ->
                    match available.TryGetValue tenant with
                    | true, context -> context
                    | _ -> raise (SessionScopeRejectedException(SessionScopeRejectionReason.ScopeUnavailable))
                | None -> invalidOp "The Legate session host contexts have not initialized successfully.")

        member _.All =
            lock gate (fun () ->
                match contexts with
                | Some available -> available.Values |> Seq.toArray
                | None -> Array.empty)

        member _.OpenAdmission() =
            lock gate (fun () ->
                if closed then
                    raise (SessionScopeRejectedException(SessionScopeRejectionReason.NodeStopping)))

        member _.CloseAdmission() =
            let current =
                lock gate (fun () ->
                    closed <- true

                    match contexts with
                    | Some available -> available.Values |> Seq.toArray
                    | None -> Array.empty)

            for context in current do
                context.WorkTracker.CloseAdmission()
                context.SubscriptionLifetime.BeginClose()

        member _.DrainAsync(bound, timeProvider, cancellationToken) =
            let current =
                lock gate (fun () ->
                    match contexts with
                    | Some available -> available.Values |> Seq.toArray
                    | None -> Array.empty)

            let drains: Task[] =
                current
                |> Array.map (fun context ->
                    let cleanup: Task =
                        task {
                            do! context.WorkTracker.DrainAsync "SessionExecution" bound timeProvider cancellationToken

                            do! context.SubscriptionLifetime.CloseAsync bound timeProvider cancellationToken
                        }

                    cleanup)

            let all = Task.WhenAll drains

            NodeBoundedWait.awaitTask "SessionNodeDrain" all bound timeProvider cancellationToken
