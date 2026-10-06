// SPDX-License-Identifier: Apache-2.0
using System.Net.Http.Json;
using Legate;
using Legate.AspNetCore;
using Legate.Storage.InMemory;
using Legate.Workspace.Process;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Xunit;

namespace Legate.AspNetCore.Tests;

internal sealed class ControlledChat : IChatClient
{
    internal TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    internal TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    internal bool Hold;
    internal bool Ask;
    internal int Disposals;
    internal string Answer = "answer";
    internal string? LastText;
    public async Task<ChatResponse> GetResponseAsync(IEnumerable<ChatMessage> messages, ChatOptions? options = null, CancellationToken cancellationToken = default)
    {
        var history = messages.ToArray();
        LastText = history.LastOrDefault(m => m.Role == ChatRole.User)?.Text;
        Entered.TrySetResult();
        if (Hold) await Release.Task.WaitAsync(cancellationToken);
        if (Ask && !history.Any(m => m.Contents.Any(c => c is FunctionResultContent)))
            return new ChatResponse(new ChatMessage(ChatRole.Assistant, new List<AIContent> { new FunctionCallContent("call", "approved", null) }));
        return new ChatResponse(new ChatMessage(ChatRole.Assistant, Answer));
    }
    public IAsyncEnumerable<ChatResponseUpdate> GetStreamingResponseAsync(IEnumerable<ChatMessage> messages, ChatOptions? options = null, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    public object? GetService(Type serviceType, object? serviceKey = null) => null;
    public void Dispose() => Interlocked.Increment(ref Disposals);
}

internal sealed class Provider(string id, string model, ControlledChat chat) : ILlmProvider
{
    internal ModelReference? CreatedModel;
    internal int Creates;
    public string Id => id;
    public string DefaultModel => model;
    public LlmCapabilities Capabilities => new() { Streaming = false, Reasoning = false, ToolCalling = true };
    public IChatClient CreateChatClient(ModelReference reference, LlmProviderOptions? options)
    {
        CreatedModel = reference;
        Interlocked.Increment(ref Creates);
        return chat;
    }
}

internal sealed class Sentinel : IDisposable
{
    internal bool Disposed;
    public void Dispose() => Disposed = true;
}

internal sealed class CapturedDelay : ILlmDelay
{
    internal System.Collections.Concurrent.ConcurrentQueue<TimeSpan> Calls { get; } = new();
    public Task Delay(TimeSpan duration, CancellationToken cancellationToken)
    {
        Calls.Enqueue(duration);
        return Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
    }
}

internal sealed class Source : IToolSource
{
    public Task<IReadOnlyList<AITool>> GetTools(ToolSourceContext context) =>
        Task.FromResult<IReadOnlyList<AITool>>([AIFunctionFactory.Create(() => "allowed", "approved", "A sample approval.")]);
}
internal sealed class AskPolicy : IPermissionPolicy
{
    public PermissionVerdict Evaluate(PermissionRequest request) => PermissionVerdict.Ask;
}

public sealed class HostingTests
{
    private static readonly AgentId Agent = AgentId.Parse("01ARZ3NDEKTSV4RRFFQ69G5FAV");
    private static WebApplicationBuilder Builder() => WebApplication.CreateBuilder(new WebApplicationOptions { EnvironmentName = "Development" });
    private static void RegisterAgent(LegateBuilder legate, string model = "test/model") => legate.Agents.Add("test", agent =>
    { agent.Id = Agent; agent.Model = ModelReference.Parse(model); return agent; });
    private static WebApplication Create(ControlledChat chat, Action<WebApplicationBuilder>? extra = null, bool middleware = true)
    {
        var builder = Builder();
        builder.WebHost.UseTestServer();
        extra?.Invoke(builder);
        builder.Services.AddLegate(builder.Configuration, b =>
        {
            b.Llm.AddProvider(new Provider("test", "model", chat));
            RegisterAgent(b);
            if (chat.Ask) { b.Tools.AddSource(new Source()); b.Permissions.UsePolicy(new AskPolicy()); }
        });
        var app = builder.Build();
        if (middleware) app.UseLegate();
        Map(app);
        return app;
    }
    private static void Map(WebApplication app)
    {
        app.MapPost("/open", async (SessionClient client) => (await client.OpenSessionAsync(Agent)).Id.ToString());
        app.MapPost("/prompt", async (string id, string text, SessionClient client, HttpContext ctx) =>
        { await client.PromptAsync(SessionId.Parse(id), text, ctx.RequestAborted); return "accepted"; });
        app.MapPost("/ask", async (string id, string text, SessionClient client, HttpContext ctx) =>
            (await client.PromptAndWaitAsync(SessionId.Parse(id), text, ctx.RequestAborted)).AssistantText);
    }

    private static async Task BackgroundPass(WebApplication app, string name)
    {
        var service = app.Services.GetServices<IHostedService>().Single(s => s.GetType().Name == name);
        var method = service.GetType().GetMethod("RunOnceAsync", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Public)!;
        await (Task)method.Invoke(service, [CancellationToken.None])!;
    }

    [Fact]
    public async Task HostedDispatcherReachesPendingWorkWithoutHttpContext()
    {
        var chat = new ControlledChat();
        await using var app = Create(chat); await app.StartAsync();
        var client = app.Services.GetRequiredService<ISessionClientFactory>().GetClient(TenantId.Default);
        var session = await client.OpenSessionAsync(Agent);
        await client.PromptAndWaitAsync(session.Id, "warmup");
        var settled = client.WaitForSettleAsync(session.Id);
        await app.Services.GetRequiredService<ISessionStore>().AppendInboxMessage(TenantId.Default, session.Id,
            new UserMessagePayload(UserMessage.Text("background")), DeliveryMode.Queue, CancellationToken.None);
        await BackgroundPass(app, "DispatcherService");
        Assert.Equal(TurnStatus.Completed, (await settled.WaitAsync(TimeSpan.FromSeconds(15))).Status);
        Assert.Equal("background", chat.LastText);
        Assert.Throws<InvalidOperationException>(() => app.Services.GetRequiredService<SessionClient>());
        await app.StopAsync();
    }

    [Fact]
    public async Task HostedSchedulesFireOnceWithoutRequestScope()
    {
        var clock = new FakeTimeProvider(new DateTimeOffset(2026, 10, 6, 12, 0, 30, TimeSpan.Zero));
        var chat = new ControlledChat();
        var builder = Builder(); builder.WebHost.UseTestServer();
        builder.Services.AddSingleton<TimeProvider>(clock);
        var agents = (IAgentStore)new InMemoryAgentStore(new InMemoryDatabase(clock));
        await agents.UpdateIfUnchanged(TenantId.Default, new Agent
        {
            Id = Agent, Tenant = TenantId.Default, Name = "scheduled", Model = ModelReference.Parse("test/model"),
            SystemPrompt = "", Enabled = true, CreatedAt = clock.GetUtcNow(), UpdatedAt = clock.GetUtcNow(),
            Schedule = new AgentSchedule { Cron = "* * * * *", TimeZone = "UTC", Message = "scheduled", Enabled = true }
        }, 0, CancellationToken.None);
        builder.Services.AddLegate(builder.Configuration, b =>
        {
            b.Llm.AddProvider(new Provider("test", "model", chat));
            b.Agents.UseStore(agents);
        });
        await using var app = builder.Build(); app.UseLegate(); await app.StartAsync();
        await BackgroundPass(app, "ScheduleEvaluatorService");
        await chat.Entered.Task.WaitAsync(TimeSpan.FromSeconds(15));
        await BackgroundPass(app, "ScheduleEvaluatorService");
        var page = await app.Services.GetRequiredService<ISessionStore>().ListSessions(TenantId.Default, null, null, null, null, 100, null, CancellationToken.None);
        Assert.Single(page.Items);
        Assert.Equal("scheduled", chat.LastText);
        await app.StopAsync();
    }

    [Fact]
    public async Task ActualExtractedBuildPageProgressionAndControlsRunOffline()
    {
        var chat = new ControlledChat();
        await using var app = BuildPageFixture.Build(
            ["--environment=Development", "--Legate:Llm:Providers:openai:ApiKey=offline-fixture"],
            builder => { builder.WebHost.UseTestServer(); builder.Services.AddSingleton<IChatClient>(_ => chat); });
        await app.StartAsync();
        using var http = app.GetTestClient();
        var response = await http.PostAsync("/ask", null); response.EnsureSuccessStatusCode();
        Assert.Equal("answer", await response.Content.ReadFromJsonAsync<string>());
        var client = app.Services.GetRequiredService<ISessionClientFactory>().GetClient(TenantId.Default);
        var session = await client.OpenSessionAsync(Agent);
        Assert.Equal(TurnStatus.Completed, (await BuildPageFixture.Wait(client, session)).Status);
        using var token = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        await BuildPageFixture.Read(client, session, token.Token);
        var settled = client.WaitForSettleAsync(session.Id);
        await BuildPageFixture.Controls(client, session, new DefaultHttpContext());
        Assert.Equal(TurnStatus.Completed, (await settled.WaitAsync(token.Token)).Status);
        await app.StopAsync();
    }

    [Fact]
    public async Task LocalDefaultsArePairedAndFactoryFailsBeforeStartupAndAfterStop()
    {
        var chat = new ControlledChat();
        await using var app = Create(chat);
        var factory = app.Services.GetRequiredService<ISessionClientFactory>();
        Assert.Throws<InvalidOperationException>(() => factory.GetClient(TenantId.Default));
        await app.StartAsync();
        var store = app.Services.GetRequiredService<ISessionStore>();
        Assert.True(((ISessionSettlementStore)store).SupportsSettlementJournal(app.Services.GetRequiredService<ISessionEventStore>()));
        Assert.Same(factory.GetClient(TenantId.Default), factory.GetClient(TenantId.Default));
        Assert.Throws<InvalidOperationException>(() => app.Services.GetRequiredService<SessionClient>());
        Assert.Throws<SessionScopeRejectedException>(() => factory.GetClient(TenantId.Create("unknown")));
        await app.StopAsync();
        Assert.Equal(SessionScopeRejectionReason.NodeStopping, Assert.Throws<SessionScopeRejectedException>(() => factory.GetClient(TenantId.Default)).Reason);
    }

    [Fact]
    public async Task ConvenienceQueueIsIndependentOfConfiguredDeliveryAndTextMatchesMessage()
    {
        var chat = new ControlledChat();
        await using var app = Create(chat, builder => builder.Configuration.AddInMemoryCollection(
            new Dictionary<string, string?> { ["Legate:Turns:DefaultDelivery"] = "Inject" }));
        await app.StartAsync();
        Assert.Equal(DeliveryMode.Inject, app.Services.GetRequiredService<IOptions<LegateOptions>>().Value.Turns.DefaultDelivery);
        var client = app.Services.GetRequiredService<ISessionClientFactory>().GetClient(TenantId.Default);
        var session = await client.OpenSessionAsync(Agent);
        foreach (var text in new[] { "plain", "message" })
        {
            var wait = client.WaitForSettleAsync(session.Id);
            var receipt = text == "plain" ? await client.PromptAsync(session.Id, text)
                : await client.PromptAsync(session.Id, UserMessage.Text(text));
            Assert.Equal(OperationKind.Queue, receipt.Kind);
            Assert.Equal(TurnStatus.Completed, (await wait.WaitAsync(TimeSpan.FromSeconds(15))).Status);
            var observations = await Task.WhenAll(
                client.WaitForOperationAsync(receipt, TimeSpan.FromSeconds(15), CancellationToken.None),
                client.WaitForOperationAsync(receipt, TimeSpan.FromSeconds(15), CancellationToken.None));
            Assert.All(observations, observed =>
            {
                Assert.Equal(OperationStatus.Terminal, observed.Status);
                Assert.Equal(receipt.Position, observed.Position);
                Assert.Equal(TurnStatus.Completed, observed.Result!.Status);
            });
            Assert.Equal(text, chat.LastText);
        }
        using var cancelled = new CancellationTokenSource(); cancelled.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => client.PromptAsync(session.Id, "not accepted", cancelled.Token));
        Assert.Empty(await app.Services.GetRequiredService<ISessionStore>().ReadPendingInbox(TenantId.Default, session.Id, CancellationToken.None));
        await app.StopAsync();
    }

    [Fact]
    public async Task StandaloneWaitUsesConfiguredClientBoundAndCancelsOnlyThatWait()
    {
        var delay = new CapturedDelay();
        var bound = TimeSpan.FromSeconds(42);
        await using var app = Create(new ControlledChat(), builder =>
        {
            builder.Services.AddSingleton<ILlmDelay>(delay);
            builder.Services.AddSingleton(new SessionClientOptions { DefaultWaitBound = bound });
        });
        await app.StartAsync();
        var client = app.Services.GetRequiredService<ISessionClientFactory>().GetClient(TenantId.Default);
        using var cancelled = new CancellationTokenSource(); cancelled.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => client.WaitForSettleAsync(SessionId.New(), cancelled.Token));
        Assert.Contains(bound, delay.Calls);
        await app.StopAsync();
    }

    [Fact]
    public async Task AdapterMapsNoAutomaticEndpoints()
    {
        var builder = Builder(); builder.WebHost.UseTestServer();
        builder.Services.AddLegate(builder.Configuration, b => b.Llm.AddProvider(new Provider("test", "model", new ControlledChat())));
        await using var app = builder.Build(); app.UseLegate(); await app.StartAsync();
        using var http = app.GetTestClient();
        Assert.Equal(System.Net.HttpStatusCode.NotFound, (await http.GetAsync("/sessions")).StatusCode);
        await app.StopAsync();
    }

    [Fact]
    public async Task InferredClientAndPrivateOptionsUseSameModelWithoutMutatingHostOptions()
    {
        var options = new SessionClientOptions();
        var chat = new ControlledChat();
        var provider = new Provider("test", "model", chat);
        var builder = Builder(); builder.WebHost.UseTestServer();
        builder.Services.AddSingleton(options);
        builder.Services.AddLegate(builder.Configuration, b => { b.Llm.AddProvider(provider); RegisterAgent(b); });
        await using var app = builder.Build(); app.UseLegate();
        await app.StartAsync();
        Assert.Equal("test/model", provider.CreatedModel!.Value.Value);
        Assert.Equal(1, provider.Creates);
        Assert.Null(options.DefaultModel);
        var client = app.Services.GetRequiredService<ISessionClientFactory>().GetClient(TenantId.Default);
        var session = await client.OpenSessionAsync(Agent);
        options.DefaultModel = "test/changed";
        Assert.Equal("answer", (await client.PromptAndWaitAsync(session.Id, "text")).AssistantText);
        await app.StopAsync();
        Assert.Equal("test/changed", options.DefaultModel);
    }

    [Theory]
    [InlineData(true)] [InlineData(false)]
    public async Task ExplicitChatClientWinsBeforeOrAfterAdapterAndIsDisposedExactlyOnce(bool before)
    {
        var chat = new ControlledChat();
        var builder = Builder(); builder.WebHost.UseTestServer();
        if (before) builder.Services.AddSingleton<IChatClient>(_ => chat);
        var first = new Provider("test", "", chat);
        builder.Services.AddLegate(builder.Configuration, b => { b.Llm.AddProvider(first); b.Llm.AddProvider(new Provider("other", "", chat)); });
        if (!before) builder.Services.AddSingleton<IChatClient>(_ => chat);
        var app = builder.Build(); app.UseLegate();
        await app.StartAsync();
        Assert.Same(chat, app.Services.GetRequiredService<IChatClient>());
        Assert.Equal(0, first.Creates);
        await app.StopAsync(); await app.DisposeAsync();
        Assert.Equal(1, chat.Disposals);
    }

    [Theory]
    [InlineData("test/model", "other/lower", "test/model")]
    [InlineData(null, "test/config", "test/config")]
    public async Task FacadeDefaultWinsEvenWhenLowerPriorityConfigurationDiffers(string? facade, string configured, string expected)
    {
        var builder = Builder(); builder.WebHost.UseTestServer();
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?> { ["Legate:Llm:DefaultModel"] = configured });
        var original = new SessionClientOptions { DefaultModel = facade };
        builder.Services.AddSingleton(original);
        var provider = new Provider("test", "model", new ControlledChat());
        builder.Services.AddLegate(builder.Configuration, b => b.Llm.AddProvider(provider));
        await using var app = builder.Build(); await app.StartAsync();
        Assert.Equal(expected, provider.CreatedModel!.Value.Value);
        Assert.Equal(facade, original.DefaultModel);
        Assert.Equal(configured, app.Services.GetRequiredService<IOptions<LegateOptions>>().Value.Llm.DefaultModel);
        await app.StopAsync();
    }

    [Theory]
    [InlineData("missing")] [InlineData("ambiguous")] [InlineData("unknown")] [InlineData("invalid")]
    public async Task AutoClientSelectionFailsWithActionableSecretFreeErrors(string scenario)
    {
        var builder = Builder(); builder.WebHost.UseTestServer();
        builder.Services.AddLegate(builder.Configuration, b =>
        {
            if (scenario != "missing") b.Llm.AddProvider(new Provider("test", "model", new ControlledChat()));
            if (scenario == "ambiguous") b.Llm.AddProvider(new Provider("other", "model", new ControlledChat()));
        });
        if (scenario is "unknown" or "invalid") builder.Services.AddSingleton(new SessionClientOptions { DefaultModel = scenario == "unknown" ? "absent/model" : "not-qualified" });
        await using var app = builder.Build();
        await Assert.ThrowsAsync<InvalidOperationException>(() => app.StartAsync());
    }

    [Theory]
    [InlineData("session")] [InlineData("journal")]
    public async Task HalfStoreOverrideFailsBeforeAdmission(string half)
    {
        await using var app = Create(new ControlledChat(), b =>
        {
            var database = new InMemoryDatabase();
            if (half == "session") b.Services.AddSingleton<ISessionStore>(new InMemorySessionStore(database));
            else b.Services.AddSingleton<ISessionEventStore>(new InMemorySessionEventStore(database));
        });
        await Assert.ThrowsAsync<InvalidOperationException>(() => app.StartAsync());
        Assert.Throws<InvalidOperationException>(() => app.Services.GetRequiredService<ISessionClientFactory>().GetClient(TenantId.Default));
    }

    [Fact]
    public async Task RequestScopeEndsWhileAcceptedWorkCompletesOnLongLivedGraph()
    {
        var chat = new ControlledChat { Hold = true };
        Sentinel? sentinel = null;
        var builder = Builder(); builder.WebHost.UseTestServer();
        builder.Services.AddScoped<Sentinel>();
        builder.Services.AddLegate(builder.Configuration, b =>
        {
            b.Llm.AddProvider(new Provider("test", "model", chat)); RegisterAgent(b);
            b.ConfigureAspNetCore(o => o.ResolveTenant = ctx =>
            { sentinel = ctx.RequestServices.GetRequiredService<Sentinel>(); return TenantId.Default; });
        });
        builder.Services.AddLegateSessionBinding(new SessionHostBinding(TenantId.Default, root => root));
        await using var app = builder.Build(); app.UseLegate(); Map(app); await app.StartAsync();
        using var http = app.GetTestClient();
        var id = SessionId.Parse(await (await http.PostAsync("/open", null)).Content.ReadAsStringAsync());
        var client = app.Services.GetRequiredService<ISessionClientFactory>().GetClient(TenantId.Default);
        var settled = client.WaitForSettleAsync(id);
        using var operation = new CancellationTokenSource();
        var response = await http.PostAsync($"/prompt?id={id}&text=held", null, operation.Token);
        response.EnsureSuccessStatusCode(); await chat.Entered.Task.WaitAsync(TimeSpan.FromSeconds(15));
        operation.Cancel();
        Assert.True(sentinel!.Disposed);
        chat.Release.TrySetResult();
        Assert.Equal(TurnStatus.Completed, (await settled.WaitAsync(TimeSpan.FromSeconds(15))).Status);
        Assert.False(chat.Disposals > 0);
        Assert.Equal("answer", await (await http.PostAsync($"/ask?id={id}&text=fresh", null)).Content.ReadAsStringAsync());
        await app.StopAsync();
    }

    [Fact]
    public async Task WaitAndStreamCancellationDoNotAbortAcceptedWork()
    {
        var chat = new ControlledChat { Hold = true };
        await using var app = Create(chat); await app.StartAsync();
        var client = app.Services.GetRequiredService<ISessionClientFactory>().GetClient(TenantId.Default);
        var session = await client.OpenSessionAsync(Agent);
        using var token = new CancellationTokenSource();
        var abandoned = client.WaitForSettleAsync(session.Id, token.Token);
        var stream = client.Subscribe(session.Id, token.Token).GetAsyncEnumerator();
        await client.PromptAsync(session.Id, "held");
        await chat.Entered.Task.WaitAsync(TimeSpan.FromSeconds(15));
        token.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => abandoned);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        { for (var i = 0; i < 100 && await stream.MoveNextAsync(); i++) { } });
        await stream.DisposeAsync();
        var settled = client.WaitForSettleAsync(session.Id);
        chat.Release.TrySetResult();
        Assert.Equal(TurnStatus.Completed, (await settled.WaitAsync(TimeSpan.FromSeconds(15))).Status);
        await app.StopAsync();
    }

    [Fact]
    public async Task MissingMiddlewareFailsClosedAndDuplicatePipelineUseFails()
    {
        await using var app = Create(new ControlledChat(), middleware: false);
        await app.StartAsync();
        using var http = app.GetTestClient();
        Assert.Equal(System.Net.HttpStatusCode.InternalServerError, (await http.PostAsync("/open", null)).StatusCode);
        app.UseLegate();
        Assert.Throws<InvalidOperationException>(() => app.UseLegate());
        await app.StopAsync();
        var builder = Builder(); await using var missing = builder.Build();
        Assert.Throws<InvalidOperationException>(() => missing.UseLegate());
    }

    [Fact]
    public async Task ConvenienceCallsForwardDefaultsAndResumePermissionWorkflow()
    {
        var chat = new ControlledChat { Ask = true };
        await using var app = Create(chat); await app.StartAsync();
        var client = app.Services.GetRequiredService<ISessionClientFactory>().GetClient(TenantId.Default);
        var session = await client.OpenSessionAsync(Agent);
        var other = await client.OpenSessionAsync(Agent, CancellationToken.None);
        Assert.NotSame(session.Options, other.Options);
        var settled = client.WaitForSettleAsync(session.Id);
        await client.PromptAsync(session.Id, "please approve");
        using var lifetime = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        await foreach (var evt in client.Subscribe(session.Id).WithCancellation(lifetime.Token))
        {
            if (evt is PermissionRequestedEvent permission)
            { await client.ReplyAsync(session.Id, new PermissionDecision(permission.RequestId, PermissionDecisionKind.AllowOnce)); break; }
        }
        Assert.Equal(TurnStatus.Completed, (await settled.WaitAsync(lifetime.Token)).Status);
        Assert.Equal("please approve", chat.LastText);
        await Assert.ThrowsAsync<ArgumentNullException>(() => client.PromptAsync(session.Id, (string)null!));
        await Assert.ThrowsAsync<ArgumentNullException>(() => client.PromptAndWaitAsync(session.Id, (string)null!));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => client.WaitForSettleAsync(session.Id, TimeSpan.Zero));
        Assert.Throws<ArgumentOutOfRangeException>(() => client.Subscribe(session.Id, -1L).GetAsyncEnumerator());
        await app.StopAsync();
    }

    // Compiled in the real C# consumer project. No calls are executed by this fixture.
    private static ServiceProvider Graph(TenantId tenant, ControlledChat chat, bool defaults = false)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(new SessionClientOptions { Tenant = tenant });
        if (defaults) builder.Services.AddLegate(builder.Configuration, b => b.Llm.AddProvider(new Provider(tenant.Value, "model", chat)));
        else
        {
            builder.Services.AddLegate(b => b.Llm.AddProvider(new Provider(tenant.Value, "model", chat)));
            var database = new InMemoryDatabase();
            builder.Services.AddSingleton<ISessionStore>(new InMemorySessionStore(database));
            builder.Services.AddSingleton<ISessionEventStore>(new InMemorySessionEventStore(database));
            builder.Services.AddSingleton<IChatClient>(_ => chat);
            builder.Services.AddSingleton<IWorkspaceRuntime>(new ProcessWorkspaceRuntime(new ProcessWorkspaceRuntimeOptions { Root = Path.GetTempPath() }, null, null));
        }
        return builder.Services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
    }

    [Fact]
    public async Task AuthorizedConcurrentTenantsUseIndependentGraphsWithoutCrossTenantVisibility()
    {
        var a = TenantId.Create("a"); var b = TenantId.Create("b");
        using var graphA = Graph(a, new ControlledChat { Answer = "A" });
        using var graphB = Graph(b, new ControlledChat { Answer = "B" });
        var builder = Builder(); builder.WebHost.UseTestServer();
        builder.Services.AddLegate(builder.Configuration, legate =>
            legate.ConfigureAspNetCore(o => o.ResolveTenant = context =>
            {
                Assert.NotNull(context.GetEndpoint());
                return context.User.Identity?.IsAuthenticated == true ? TenantId.Create(context.User.FindFirst("tenant")!.Value) : throw new UnauthorizedAccessException();
            }));
        builder.Services.AddLegateSessionBinding(new SessionHostBinding(a, _ => graphA));
        builder.Services.AddLegateSessionBinding(new SessionHostBinding(b, _ => graphB));
        await using var app = builder.Build();
        app.UseRouting();
        // Test-only authentication stand-in. Production authentication validates credentials.
        app.Use(async (context, next) =>
        {
            if (context.Request.Headers.TryGetValue("Test-Authenticated-Tenant", out var value))
                context.User = new System.Security.Claims.ClaimsPrincipal(new System.Security.Claims.ClaimsIdentity(
                    [new System.Security.Claims.Claim("tenant", value.ToString())], "test-auth"));
            await next(context);
        });
        app.UseLegate(); Map(app); await app.StartAsync();
        using var httpA = app.GetTestClient(); httpA.DefaultRequestHeaders.Add("Test-Authenticated-Tenant", "a");
        using var httpB = app.GetTestClient(); httpB.DefaultRequestHeaders.Add("Test-Authenticated-Tenant", "b");
        var ids = await Task.WhenAll(httpA.PostAsync("/open", null), httpB.PostAsync("/open", null));
        var idA = await ids[0].Content.ReadAsStringAsync(); var idB = await ids[1].Content.ReadAsStringAsync();
        var answers = await Task.WhenAll(httpA.PostAsync($"/ask?id={idA}&text=alpha", null), httpB.PostAsync($"/ask?id={idB}&text=beta", null));
        Assert.Equal("A", await answers[0].Content.ReadAsStringAsync());
        Assert.Equal("B", await answers[1].Content.ReadAsStringAsync());
        Assert.Equal(System.Net.HttpStatusCode.InternalServerError, (await httpB.PostAsync($"/prompt?id={idA}&text=denied", null)).StatusCode);
        using var unknown = app.GetTestClient(); unknown.DefaultRequestHeaders.Add("Test-Authenticated-Tenant", "unknown");
        Assert.Equal(System.Net.HttpStatusCode.InternalServerError, (await unknown.PostAsync("/open", null)).StatusCode);
        using var untrusted = app.GetTestClient(); untrusted.DefaultRequestHeaders.Add("Tenant", "a");
        Assert.Equal(System.Net.HttpStatusCode.InternalServerError, (await untrusted.PostAsync("/open", null)).StatusCode);
        Assert.Null(graphA.GetRequiredService<SessionClientOptions>().DefaultModel);
        Assert.Null(graphB.GetRequiredService<SessionClientOptions>().DefaultModel);
        await app.StopAsync();
    }

    [Theory]
    [InlineData(false)] [InlineData(true)]
    public async Task ClusterRejectsImplicitDefaultsOnRootAndBorrowedLocalGraph(bool borrowed)
    {
        var tenant = TenantId.Create("borrowed");
        using var graph = Graph(tenant, new ControlledChat(), defaults: true);
        var builder = Builder(); builder.WebHost.UseTestServer();
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Legate:Cluster:Mode"] = "StaticSeeds", ["Legate:Cluster:SeedNodes:0"] = "akka.tcp://legate@127.0.0.1:4053"
        });
        builder.Services.AddLegate(builder.Configuration, b => b.Llm.AddProvider(new Provider("test", "model", new ControlledChat())));
        if (borrowed) builder.Services.AddLegateSessionBinding(new SessionHostBinding(tenant, _ => graph));
        await using var app = builder.Build();
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => app.StartAsync());
        Assert.Contains("explicit", error.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Throws<InvalidOperationException>(() => app.Services.GetRequiredService<ISessionClientFactory>().GetClient(tenant));
    }

    [Fact]
    public async Task RepeatedRegistrationIsIdempotentAndConfigurationCodePrecedenceRetainsAllSections()
    {
        var builder = Builder(); builder.WebHost.UseTestServer();
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        { ["Legate:Pruning:ReservedBufferTokens"] = "3000", ["Legate:Sessions:MaxSubscribersPerSession"] = "12" });
        var calls = 0;
        builder.Services.AddLegate(builder.Configuration, b =>
        {
            calls++; b.Llm.AddProvider(new Provider("test", "model", new ControlledChat()));
            b.Services.Configure<LegateOptions>(o => o.Sessions.MaxSubscribersPerSession = 14);
        });
        builder.Services.AddLegate(builder.Configuration, _ => throw new InvalidOperationException("Idempotent callback must not rerun."));
        await using var app = builder.Build(); await app.StartAsync();
        Assert.Equal(1, calls);
        var options = app.Services.GetRequiredService<IOptions<LegateOptions>>().Value;
        Assert.Equal(3000, options.Pruning.ReservedBufferTokens); Assert.Equal(14, options.Sessions.MaxSubscribersPerSession);
        await app.StopAsync();
    }

    internal static void CompileMatrix(SessionClient client, AgentId agentId, SessionId sessionId, UserMessage message, Reply reply, CancellationToken cancellationToken)
    {
        _ = client.OpenSessionAsync(agentId); _ = client.OpenSessionAsync(agentId, new SessionOptions());
        _ = client.OpenSessionAsync(agentId, cancellationToken); _ = client.OpenSessionAsync(agentId, (SessionOptions?)null, cancellationToken);
        _ = client.OpenSessionAsync(agentId: agentId, options: default(SessionOptions), cancellationToken: cancellationToken);
        _ = client.PromptAsync(sessionId, message); _ = client.PromptAsync(sessionId, message, cancellationToken);
        _ = client.PromptAsync(sessionId, message, DeliveryMode.Inject); _ = client.PromptAsync(sessionId, message, DeliveryMode.Queue, cancellationToken);
        _ = client.PromptAsync(sessionId, "text"); _ = client.PromptAsync(sessionId, "text", cancellationToken);
        _ = client.PromptAsync(sessionId, "text", DeliveryMode.Interrupt); _ = client.PromptAsync(sessionId, "text", DeliveryMode.Queue, cancellationToken);
        _ = client.PromptAsync(sessionId: sessionId, text: default(string)!, cancellationToken: cancellationToken);
        _ = client.PromptAsync(sessionId, default(UserMessage)!, default(DeliveryMode), default(CancellationToken));
        _ = client.ReplyAsync(sessionId, reply); _ = client.ReplyAsync(sessionId, reply, cancellationToken);
        _ = client.Subscribe(sessionId); _ = client.Subscribe(sessionId, 0L); _ = client.Subscribe(sessionId, cancellationToken); _ = client.Subscribe(sessionId, 0L, cancellationToken);
        _ = client.WaitForSettleAsync(sessionId); _ = client.WaitForSettleAsync(sessionId, TimeSpan.FromMinutes(1));
        _ = client.WaitForSettleAsync(sessionId, cancellationToken); _ = client.WaitForSettleAsync(sessionId, TimeSpan.FromMinutes(1), cancellationToken);
        _ = client.PromptAndWaitAsync(sessionId, message); _ = client.PromptAndWaitAsync(sessionId, message, cancellationToken);
        _ = client.PromptAndWaitAsync(sessionId, "text"); _ = client.PromptAndWaitAsync(sessionId, "text", cancellationToken);
    }
}
