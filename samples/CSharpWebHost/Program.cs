// SPDX-License-Identifier: Apache-2.0
using Legate;
using Legate.AspNetCore;
using Legate.Storage;
using Legate.Coordination;
using Legate.Workspace.Process;
using Microsoft.Extensions.AI;
using System.Text.Json;
using SessionOptions = Legate.SessionOptions;

var builder = WebApplication.CreateBuilder(args);
var agentId = AgentId.Parse("01ARZ3NDEKTSV4RRFFQ69G5FAV");
builder.Services.AddHealthChecks();
builder.Services.AddLegate(builder.Configuration, legate =>
{
    legate.Llm.AddProvider(new SampleProvider(builder.Configuration));
    legate.Tools.AddSource(new SampleTools());
    legate.Permissions.UsePolicy(new SamplePermissions());
    legate.Agents.Add("assistant", agent =>
    {
        agent.Id = agentId;
        agent.Model = ModelReference.Parse("scripted/echo");
        agent.SystemPrompt = "Answer briefly.";
        return agent;
    });
    if (!string.IsNullOrWhiteSpace(builder.Configuration["Legate:Storage:Postgres:ConnectionString"]))
    {
        legate.UsePostgres(builder.Configuration);
        // Deliberately explicit sample execution choice. This is not a sandbox.
        legate.Workspace.UseRuntime(new ProcessWorkspaceRuntime(
            new ProcessWorkspaceRuntimeOptions { Root = Path.Combine(builder.Environment.ContentRootPath, ".legate", "scratch") },
            builder.Environment, null));
    }
    if (!string.IsNullOrWhiteSpace(builder.Configuration["Legate:Llm:DistributedCoordination:ConnectionString"]))
        legate.UseRedisCoordination(builder.Configuration);
});
builder.Services.AddSingleton(new SessionClientOptions
{
    LeaseDuration = TimeSpan.FromSeconds(60), DefaultModel = "scripted/echo",
    ClaimOwner = builder.Configuration["Legate:Cluster:RemotingHostname"] ?? "csharp-web-local"
});

var app = builder.Build();
app.UseLegate();
if (builder.Configuration.GetValue<bool>("Sample:Smoke"))
    app.MapPost("/_sample/stop", (HttpContext context, IHostApplicationLifetime lifetime) =>
    {
        context.Response.OnCompleted(() => { lifetime.StopApplication(); return Task.CompletedTask; });
        return Results.Ok(true);
    });
app.MapGet("/healthz", () => "ok");
app.MapHealthChecks("/ready");
app.MapGet("/_sample/active", () => SampleClient.ActiveCalls);
app.MapGet("/_sample/waiters", () => Volatile.Read(ref SampleSignals.Waiters));
app.MapPost("/sessions", async (OpenRequest request, SessionClient client, HttpContext context) =>
{
    var session = await client.OpenSessionAsync(agentId,
        new SessionOptions { Title = request.Title }, context.RequestAborted);
    return Results.Ok(new { sessionId = session.Id.ToString() });
});
app.MapPost("/sessions/{id}/prompt", async (string id, PromptRequest request, SessionClient client, HttpContext context) =>
    Results.Ok(await client.PromptAsync(SessionId.Parse(id), request.Text, context.RequestAborted)));
app.MapPost("/sessions/{id}/ask", async (string id, PromptRequest request, SessionClient client, HttpContext context) =>
    Results.Ok(await client.PromptAndWaitAsync(SessionId.Parse(id), request.Text, context.RequestAborted)));
app.MapGet("/sessions/{id}/settle", async (string id, SessionClient client, HttpContext context) =>
{
    var wait = client.WaitForSettleAsync(SessionId.Parse(id), context.RequestAborted);
    Interlocked.Increment(ref SampleSignals.Waiters);
    try { return Results.Ok(await wait); }
    finally { Interlocked.Decrement(ref SampleSignals.Waiters); }
});
app.MapPost("/sessions/{id}/reply", async (string id, PermissionReply request, SessionClient client, HttpContext context) =>
    Results.Ok(await client.ReplyAsync(SessionId.Parse(id),
        new PermissionDecision(request.RequestId, PermissionDecisionKind.AllowOnce), context.RequestAborted)));
app.MapGet("/sessions/{id}/journal", async (string id, SessionClient client, HttpContext context) =>
    Results.Ok(await client.ReadEventsAsync(SessionId.Parse(id), 0L, 1000, context.RequestAborted)));
app.MapGet("/sessions/{id}/events", async (string id, SessionClient client, HttpContext context) =>
{
    long.TryParse(context.Request.Headers["Last-Event-ID"], out var cursor);
    context.Response.ContentType = "text/event-stream";
    await foreach (var evt in client.Subscribe(SessionId.Parse(id), cursor, context.RequestAborted))
    {
        await context.Response.WriteAsync($"id: {evt.Sequence}\nevent: {evt.GetType().Name}\ndata: {JsonSerializer.Serialize<SessionEvent>(evt)}\n\n", context.RequestAborted);
        await context.Response.Body.FlushAsync(context.RequestAborted);
    }
});
app.MapGet("/sessions/{id}/abort-target", async (string id, SessionClient client, HttpContext context) =>
    Results.Ok(await client.ReadAbortTargetAsync(SessionId.Parse(id), context.RequestAborted)));
app.MapPost("/sessions/{id}/abort", async (string id, AbortRequest request, SessionClient client, HttpContext context) =>
    Results.Ok(await client.AbortAsync(SessionId.Parse(id), TurnId.Parse(request.ExpectedTurnId),
        StopCause.HostShutdown, request.Reason, context.RequestAborted)));
await app.RunAsync();

internal sealed record OpenRequest(string? Title);
internal sealed record PromptRequest(string Text);
internal sealed record PermissionReply(string RequestId);
internal sealed record AbortRequest(string ExpectedTurnId, string Reason);

internal static class SampleSignals { internal static int Waiters; }
internal sealed class SampleTools : IToolSource
{
    public Task<IReadOnlyList<AITool>> GetTools(ToolSourceContext context) =>
        Task.FromResult<IReadOnlyList<AITool>>([AIFunctionFactory.Create(() => "approval demo allowed", "approval_demo", "Sample permission demonstration.")]);
}
internal sealed class SamplePermissions : IPermissionPolicy
{
    public PermissionVerdict Evaluate(PermissionRequest request) =>
        request.ToolName == "approval_demo" ? PermissionVerdict.Ask : PermissionVerdict.Deny("Not enabled in this sample.");
}

internal sealed class SampleProvider(IConfiguration configuration) : ILlmProvider
{
    public string Id => "scripted";
    public string DefaultModel => "echo";
    public LlmCapabilities Capabilities => new() { Streaming = false, ToolCalling = true, Reasoning = false };
    public IChatClient CreateChatClient(ModelReference model, LlmProviderOptions? options) => new SampleClient(configuration);
}

internal sealed class SampleClient(IConfiguration configuration) : IChatClient
{
    private static int active;
    internal static int ActiveCalls => Volatile.Read(ref active);
    public async Task<ChatResponse> GetResponseAsync(IEnumerable<ChatMessage> messages, ChatOptions? options = null, CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref active);
        try
        {
            // Sample-only hold for a bounded external node-loss harness, not a runtime timing seam.
            var hold = configuration.GetValue<int>("Sample:ReplyDelayMilliseconds");
            if (hold > 0) await Task.Delay(hold, cancellationToken);
            var history = messages.ToArray();
            if (history.LastOrDefault(m => m.Role == ChatRole.User)?.Text == "approve"
                && !history.Any(m => m.Contents.Any(c => c is FunctionResultContent)))
                return new ChatResponse(new ChatMessage(ChatRole.Assistant,
                    new List<AIContent> { new FunctionCallContent("demo", "approval_demo", null) }));
            return new ChatResponse(new ChatMessage(ChatRole.Assistant, "web sample answer"));
        }
        finally { Interlocked.Decrement(ref active); }
    }
    public IAsyncEnumerable<ChatResponseUpdate> GetStreamingResponseAsync(IEnumerable<ChatMessage> messages, ChatOptions? options = null, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    public object? GetService(Type serviceType, object? serviceKey = null) => null;
    public void Dispose() { }
}
