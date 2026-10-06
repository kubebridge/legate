// SPDX-License-Identifier: Apache-2.0
namespace Legate.AspNetCore

open System
open System.IO
open System.Runtime.CompilerServices
open System.Threading.Tasks
open Legate
open Legate.Storage.InMemory
open Legate.Workspace.Process
open Microsoft.AspNetCore.Builder
open Microsoft.AspNetCore.Http
open Microsoft.Extensions.AI
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.DependencyInjection.Extensions
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.Logging
open Microsoft.Extensions.Options

/// ASP.NET Core tenant selection. Authentication and authorization remain application responsibilities.
[<Sealed>]
type LegateAspNetCoreOptions() =
    /// The fixed application-authorized tenant, used when no trusted resolver is configured.
    /// Defaults to TenantId.Default. Non-default tenants require an explicit execution binding.
    member val Tenant: TenantId = TenantId.Default with get, set
    /// A code-only trusted resolver; may read request services during selection only.
    /// Return only an application-authorized, explicitly declared tenant. No header/claim inference is automatic.
    member val ResolveTenant: Func<HttpContext, TenantId> | null = null with get, set

type internal DefaultProvenance() =
    member val Store: obj | null = null with get, set
    member val Journal: obj | null = null with get, set
    member val Workspace: obj | null = null with get, set

type internal RequestBinding(tenant: TenantId, client: SessionClient) =
    member _.Tenant = tenant
    member _.Client = client

type internal AdapterRegistration(services: IServiceCollection) =
    member val ClientDescriptor: ServiceDescriptor | null = null with get, set

    member this.ValidateClient() =
        let clients =
            services
            |> Seq.filter (fun d -> not d.IsKeyedService && d.ServiceType = typeof<SessionClient>)
            |> Seq.toArray

        if
            clients.Length <> 1
            || not (Object.ReferenceEquals(clients[0], this.ClientDescriptor))
        then
            invalidOp "ASP.NET Core integration conflicts with a custom SessionClient registration."

module internal Composition =
    let options (provider: IServiceProvider) =
        provider.GetRequiredService<IOptions<LegateOptions>>().Value

    let requireLocal provider =
        if (options provider).Cluster.Mode <> ClusterMode.Local then
            invalidOp
                "Cluster mode requires explicit session/event stores and workspace runtime; adapter local defaults are not distributed."

    let selectModel (provider: IServiceProvider) =
        let providers = provider.GetServices<ILlmProvider>() |> Seq.toArray

        if providers.Length = 0 then
            invalidOp "Register an ILlmProvider and an explicit model."

        let identities = providers |> Array.map (fun p -> p.Id)

        if identities |> Array.exists String.IsNullOrWhiteSpace then
            invalidOp "ILlmProvider identities must be nonempty."

        if (identities |> Array.distinct).Length <> identities.Length then
            invalidOp "Duplicate ILlmProvider identity."

        let facade = provider.GetRequiredService<SessionClientOptions>()
        let configured = (options provider).Llm.DefaultModel

        let raw =
            if not (String.IsNullOrWhiteSpace facade.DefaultModel) then
                facade.DefaultModel
            elif not (String.IsNullOrWhiteSpace configured) then
                configured
            elif
                providers.Length = 1
                && not (String.IsNullOrWhiteSpace providers[0].DefaultModel)
            then
                providers[0].Id + "/" + providers[0].DefaultModel
            else
                invalidOp "Select Legate:Llm:DefaultModel in provider/model form when providers are ambiguous."

        let model =
            try
                ModelReference.Parse(raw |> Option.ofObj |> Option.defaultValue "")
            with _ ->
                invalidOp "Select a valid Legate:Llm:DefaultModel in provider/model form."

        let selected = providers |> Array.tryFind (fun p -> p.Id = model.Provider)

        match selected with
        | None -> invalidOp "The selected default model has no registered ILlmProvider."
        | Some p -> p, model

    let validate (provider: IServiceProvider) nodeMode =
        match provider.GetService<DefaultProvenance>() with
        | null -> ()
        | provenance ->
            let store = provider.GetRequiredService<ISessionStore>()
            let journal = provider.GetRequiredService<ISessionEventStore>()
            let workspace = provider.GetRequiredService<IWorkspaceRuntime>()
            let implicitStore = Object.ReferenceEquals(store, provenance.Store)
            let implicitJournal = Object.ReferenceEquals(journal, provenance.Journal)

            if implicitStore <> implicitJournal then
                invalidOp
                    "Replace both ISessionStore and ISessionEventStore together, or keep the adapter's paired defaults."

            if
                nodeMode <> ClusterMode.Local
                && (implicitStore
                    || implicitJournal
                    || Object.ReferenceEquals(workspace, provenance.Workspace))
            then
                invalidOp
                    "Cluster mode requires explicit stores and workspace in every execution binding. Startup validates composition, not shared durability."

/// Configuration-aware ASP.NET Core registration and pipeline extensions.
[<Sealed; AbstractClass; Extension>]
type LegateAspNetCoreExtensions =
    /// Configures trusted tenant selection after configuration binding.
    /// <param name="builder">The existing runtime builder.</param>
    /// <param name="configure">Code overrides, including an optional trusted request resolver.</param>
    /// <returns>The same builder.</returns>
    [<Extension>]
    static member ConfigureAspNetCore(builder: LegateBuilder, configure: Action<LegateAspNetCoreOptions>) =
        ArgumentNullException.ThrowIfNull(builder)
        ArgumentNullException.ThrowIfNull(configure)
        builder.Services.Configure<LegateAspNetCoreOptions>(configure) |> ignore
        builder

    /// Registers the runtime using the Legate configuration section and local defaults.
    /// Session metadata and events are process-local and lost on restart. Process workspace is not a sandbox.
    /// A provider/model remains explicit; cluster mode requires explicit execution providers.
    /// <param name="services">Application services.</param>
    /// <param name="configuration">Application configuration.</param>
    /// <returns>The same services.</returns>
    [<Extension>]
    static member AddLegate(services: IServiceCollection, configuration: IConfiguration) =
        LegateAspNetCoreExtensions.AddLegate(services, configuration, Action<LegateBuilder>(ignore))

    /// Binds configuration first, then applies code overrides on the existing runtime builder.
    /// Registers request-bound SessionClient access; background code uses ISessionClientFactory.
    /// Repeated adapter registration is idempotent and does not rerun configuration callbacks.
    /// <param name="services">Application services.</param>
    /// <param name="configuration">Application configuration.</param>
    /// <param name="configure">Runtime and adapter code overrides.</param>
    /// <returns>The same services.</returns>
    [<Extension>]
    static member AddLegate
        (services: IServiceCollection, configuration: IConfiguration, configure: Action<LegateBuilder>)
        =
        ArgumentNullException.ThrowIfNull(services)
        ArgumentNullException.ThrowIfNull(configuration)
        ArgumentNullException.ThrowIfNull(configure)

        if not (services |> Seq.exists (fun d -> d.ServiceType = typeof<AdapterRegistration>)) then
            let registration = AdapterRegistration(services)
            services.AddSingleton(registration) |> ignore
            services.AddHttpContextAccessor() |> ignore
            services.AddSingleton<DefaultProvenance>() |> ignore
            let section = configuration.GetSection("Legate")

            services.Configure<LegateAspNetCoreOptions>(
                Action<LegateAspNetCoreOptions>(fun target ->
                    let tenant = section.GetSection("Hosting")["Tenant"]

                    match tenant with
                    | null -> ()
                    | value when String.IsNullOrWhiteSpace value -> ()
                    | value -> target.Tenant <- TenantId.Create(value))
            )
            |> ignore

            services.TryAddSingleton<SessionClientOptions>(
                Func<IServiceProvider, SessionClientOptions>(fun provider ->
                    let result = SessionClientOptions()
                    result.Tenant <- provider.GetRequiredService<IOptions<LegateAspNetCoreOptions>>().Value.Tenant

                    result.DefaultModel <-
                        ((Composition.options provider).Llm.DefaultModel
                         |> Option.ofObj
                         |> Option.defaultValue "")

                    result)
            )
            |> ignore

            LegateServiceCollectionExtensions.AddLegate(
                services,
                Action<LegateBuilder>(fun builder ->
                    builder.UseConfiguration(section) |> ignore
                    configure.Invoke(builder)

                    builder.AddExecutionValidation(
                        Action<IServiceProvider, ClusterMode>(fun provider mode ->
                            registration.ValidateClient()
                            Composition.validate provider mode)
                    )
                    |> ignore)
            )
            |> ignore

            services.TryAddSingleton<InMemoryDatabase>() |> ignore

            services.TryAddSingleton<ISessionStore>(
                Func<IServiceProvider, ISessionStore>(fun provider ->
                    Composition.requireLocal provider
                    let store = InMemorySessionStore(provider.GetRequiredService<InMemoryDatabase>())
                    provider.GetRequiredService<DefaultProvenance>().Store <- box store
                    store :> ISessionStore)
            )
            |> ignore

            services.TryAddSingleton<ISessionEventStore>(
                Func<IServiceProvider, ISessionEventStore>(fun provider ->
                    Composition.requireLocal provider

                    let journal =
                        InMemorySessionEventStore(provider.GetRequiredService<InMemoryDatabase>())

                    provider.GetRequiredService<DefaultProvenance>().Journal <- box journal
                    journal :> ISessionEventStore)
            )
            |> ignore

            services.TryAddSingleton<IWorkspaceRuntime>(
                Func<IServiceProvider, IWorkspaceRuntime>(fun provider ->
                    Composition.requireLocal provider
                    let settings = (Composition.options provider).Workspace

                    if settings.Mode <> WorkspaceMode.Process then
                        invalidOp "Register an explicit IWorkspaceRuntime for the selected non-Process workspace mode."

                    let environment = provider.GetRequiredService<IHostEnvironment>()

                    let root =
                        match settings.RootPath with
                        | null -> Path.Combine(environment.ContentRootPath, ".legate", "scratch")
                        | path when Path.IsPathFullyQualified(path) -> path
                        | _ -> invalidOp "Legate:Workspace:RootPath must be absolute."

                    let runtime =
                        ProcessWorkspaceRuntime(
                            ProcessWorkspaceRuntimeOptions(Root = root),
                            environment,
                            provider.GetService<ILogger<ProcessWorkspaceRuntime>>()
                        )

                    provider.GetRequiredService<DefaultProvenance>().Workspace <- box runtime
                    runtime :> IWorkspaceRuntime)
            )
            |> ignore

            services.TryAddSingleton<IChatClient>(
                Func<IServiceProvider, IChatClient>(fun provider ->
                    let selected, model = Composition.selectModel provider

                    try
                        let client = selected.CreateChatClient(model, null)

                        if isNull (box client) then
                            invalidOp "The selected provider returned no IChatClient."

                        client
                    with _ ->
                        invalidOp
                            "The selected provider could not construct IChatClient; check provider-specific settings.")
            )
            |> ignore

            let owned =
                services
                |> Seq.tryPick (fun d ->
                    if
                        d.IsKeyedService
                        && d.ServiceType = typeof<ServiceDescriptor>
                        && Object.Equals(d.ServiceKey, typeof<SessionClient>)
                    then
                        match d.KeyedImplementationInstance with
                        | :? ServiceDescriptor as owned -> Some owned
                        | _ -> None
                    else
                        None)

            let clients =
                services
                |> Seq.filter (fun d -> not d.IsKeyedService && d.ServiceType = typeof<SessionClient>)
                |> Seq.toArray

            match owned with
            | Some descriptor when clients.Length = 1 && Object.ReferenceEquals(clients[0], descriptor) ->
                services.Remove(descriptor) |> ignore
            | _ ->
                invalidOp
                    "ASP.NET Core request integration conflicts with a custom SessionClient registration. Use ISessionClientFactory for background access."

            let scoped =
                ServiceDescriptor.Scoped<SessionClient>(
                    Func<IServiceProvider, SessionClient>(fun provider ->
                        let context = provider.GetRequiredService<IHttpContextAccessor>().HttpContext

                        let context =
                            match context with
                            | null ->
                                invalidOp
                                    "Resolve SessionClient only inside UseLegate middleware; background code uses ISessionClientFactory."
                            | present -> present

                        match context.Features.Get<RequestBinding>() with
                        | null -> invalidOp "UseLegate must run before resolving request SessionClient."
                        | binding -> binding.Client)
                )

            services.Add(scoped)
            registration.ClientDescriptor <- scoped

        services

    /// Installs trusted tenant resolution and immutable request-client binding, not endpoints or authentication.
    /// Place after routing and application authentication/authorization, before session-using endpoints.
    /// Request completion/cancellation never aborts an accepted turn; hosted services own startup and drain.
    /// <param name="app">The application pipeline.</param>
    /// <returns>The same pipeline.</returns>
    [<Extension>]
    static member UseLegate(app: IApplicationBuilder) =
        ArgumentNullException.ThrowIfNull(app)

        match app.ApplicationServices.GetService<AdapterRegistration>() with
        | null -> invalidOp "Register configuration-aware AddLegate before UseLegate."
        | registration -> registration.ValidateClient()

        let key = "Legate.AspNetCore.UseLegate"

        if app.Properties.ContainsKey(key) then
            invalidOp "UseLegate may be installed only once."

        app.Properties[key] <- true

        let settings =
            app.ApplicationServices.GetRequiredService<IOptions<LegateAspNetCoreOptions>>().Value

        let tenant = settings.Tenant
        let resolver = settings.ResolveTenant
        let factory = app.ApplicationServices.GetRequiredService<ISessionClientFactory>()

        let declared =
            System.Collections.Generic.HashSet<TenantId>(
                app.ApplicationServices.GetServices<SessionHostBinding>()
                |> Seq.map (fun b -> b.Tenant)
            )

        if resolver <> null && declared.Count = 0 then
            invalidOp "A trusted request resolver requires explicit declared tenant bindings."

        if tenant <> TenantId.Default && resolver = null && not (declared.Contains tenant) then
            invalidOp "A non-default fixed tenant requires an explicit binding."

        app.Use(
            Func<RequestDelegate, RequestDelegate>(fun next ->
                RequestDelegate(fun context ->
                    task {
                        let selected =
                            match resolver with
                            | null -> tenant
                            | trusted -> trusted.Invoke(context)

                        if resolver <> null && not (declared.Contains selected) then
                            raise (SessionScopeRejectedException(SessionScopeRejectionReason.ScopeUnavailable))

                        let binding = RequestBinding(selected, factory.GetClient(selected))
                        let previous = context.Features.Get<RequestBinding>()
                        context.Features.Set<RequestBinding>(binding)

                        try
                            do! next.Invoke(context)
                        finally
                            context.Features.Set<RequestBinding>(previous)
                    }
                    :> Task))
        )
        |> ignore

        app
