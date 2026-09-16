// SPDX-License-Identifier: Apache-2.0
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Legate;
using Legate.Storage.InMemory;
using Legate.Workspace.Process;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

// CSharpHost: the first C# host for Legate sessions. It builds a container
// with AddLegate over in-memory stores, opens one session through the
// session client facade, prompts it, streams Subscribe events while
// answering the scripted permission request through Reply, waits for the
// settle, and exits 0 only when the permission round-trip completed.
// Scripted transports only; there is no live mode in this sample.
//
// Reply flavor note: the reply below answers a permission suspension
// (PermissionDecision), not an ask_user question. The loop suspends for a
// host question only when its AskUser policy is None, which needs both the
// session override and the configured default to be null
// (SessionClientWiring orElse); but LegateOptions ships AskUser defaulting
// to Fail-closed and startup validation rejects nulling it
// ("AskUser must not be null"), so question suspension is unreachable
// through the validated facade and ask_user runs fail fast instead. The
// permission reply exercises the same ReplyAsync/Subscribe/resume
// machinery the issue asks the sample to prove.

// Scripted transports (no keys, no external services): host-local doubles.
// Scripted support stays in the sample, never a Legate.Testing reference
// (a test-only package no real host can depend on).
internal sealed class ScriptedClient : IChatClient
{
    private readonly Queue<ChatResponse> _answers;

    internal ScriptedClient(IEnumerable<ChatResponse> answers)
    {
        ArgumentNullException.ThrowIfNull(answers);
        _answers = new Queue<ChatResponse>(answers);
    }

    public Task<ChatResponse> GetResponseAsync(
        IEnumerable<ChatMessage> messages,
        ChatOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (_answers.Count == 0)
        {
            throw new InvalidOperationException("The scripted client ran out of answers.");
        }

        return Task.FromResult(_answers.Dequeue());
    }

    public IAsyncEnumerable<ChatResponseUpdate> GetStreamingResponseAsync(
        IEnumerable<ChatMessage> messages,
        ChatOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException(
            "The scripted client is non-streaming: the caller falls back to GetResponseAsync.");
    }

    public object? GetService(Type serviceType, object? serviceKey = null)
    {
        return null;
    }

    public void Dispose()
    {
    }
}

internal sealed class StubScriptedProvider : ILlmProvider
{
    private readonly IChatClient _client;

    internal StubScriptedProvider(IChatClient client)
    {
        ArgumentNullException.ThrowIfNull(client);
        _client = client;
    }

    public string Id => "scripted";

    public string DefaultModel => "scripted";

    public LlmCapabilities Capabilities =>
        new LlmCapabilities { Streaming = false, Reasoning = false, ToolCalling = true };

    public IChatClient CreateChatClient(ModelReference model, LlmProviderOptions? options)
    {
        return _client;
    }
}

internal sealed class StaticSource : IToolSource
{
    private readonly IReadOnlyList<AITool> _tools;

    internal StaticSource(IReadOnlyList<AITool> tools)
    {
        ArgumentNullException.ThrowIfNull(tools);
        _tools = tools;
    }

    public Task<IReadOnlyList<AITool>> GetTools(ToolSourceContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        return Task.FromResult(_tools);
    }
}

// Asks for the scripted gated tool, allows everything else: the turn
// suspends once and the host resumes it through Reply.
internal sealed class AskGatedPolicy : IPermissionPolicy
{
    private readonly string _gated;

    internal AskGatedPolicy(string gated)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(gated);
        _gated = gated;
    }

    public PermissionVerdict Evaluate(PermissionRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        if (string.Equals(request.ToolName, _gated, StringComparison.Ordinal))
        {
            return PermissionVerdict.Ask;
        }

        return PermissionVerdict.Allow;
    }
}

internal sealed class StartPlan
{
    internal string Prompt { get; set; } = "csharp smoke prompt";
    internal double WaitMinutes { get; set; } = 5.0;
}

internal sealed class StreamOutcome
{
    internal bool SawPermissionRequested;
    internal bool SawPermissionResolved;
}

internal static class CSharpHost
{
    private const string GatedToolName = "gated-tool";

    private static string Take(string[] argv, ref int index, string flag)
    {
        index++;

        if (index >= argv.Length)
        {
            throw new ArgumentException($"The {flag} flag needs a value.", flag);
        }

        return argv[index];
    }

    internal static StartPlan ParseArgs(string[] argv)
    {
        var plan = new StartPlan();
        var index = 0;

        while (index < argv.Length)
        {
            switch (argv[index])
            {
                case "--prompt":
                    plan.Prompt = Take(argv, ref index, "--prompt");
                    break;
                case "--wait-minutes":
                    var raw = Take(argv, ref index, "--wait-minutes");

                    if (!double.TryParse(raw, out var minutes) || minutes <= 0.0)
                    {
                        throw new ArgumentException(
                            "The --wait-minutes flag needs a positive number of minutes.",
                            "--wait-minutes");
                    }

                    plan.WaitMinutes = minutes;
                    break;
                case "--scripted":
                    break;
                case "--live":
                    throw new ArgumentException(
                        "CSharpHost runs scripted transports only: --live is not supported here.",
                        "--live");
                case "--help":
                case "-h":
                    throw new ArgumentException(
                        "Usage: CSharpHost [--prompt <text>] [--wait-minutes <n>] [--scripted]",
                        "--help");
                default:
                    throw new ArgumentException($"Unknown flag '{argv[index]}'.", argv[index]);
            }

            index++;
        }

        if (string.IsNullOrWhiteSpace(plan.Prompt))
        {
            throw new ArgumentException("The --prompt flag needs a non-empty prompt.", "--prompt");
        }

        plan.Prompt = plan.Prompt.Trim();
        return plan;
    }

    private static ChatResponse GatedCall()
    {
        var call = new FunctionCallContent(
            "call-1",
            GatedToolName,
            (IDictionary<string, object?>?)null);
        var message = new ChatMessage(
            ChatRole.Assistant,
            (IList<AIContent>)new List<AIContent> { call });
        return new ChatResponse(message);
    }

    private static ChatResponse FinalText()
    {
        return new ChatResponse(new ChatMessage(ChatRole.Assistant, "csharp smoke done"));
    }

    private static AITool GatedTool()
    {
        return AIFunctionFactory.Create(
            (Func<string>)(() => "gated tool ok"),
            GatedToolName,
            "Answers the gated tool call for the scripted smoke run.");
    }

    internal static async Task<int> RunAsync(string[] argv)
    {
        StartPlan plan;
        try
        {
            plan = ParseArgs(argv);
        }
        catch (ArgumentException usage)
        {
            await Console.Error.WriteLineAsync(usage.Message).ConfigureAwait(false);
            return 2;
        }

        try
        {
            var scripted = new ScriptedClient(new[] { GatedCall(), FinalText() });
            var database = new InMemoryDatabase();

            var builder = Host.CreateApplicationBuilder();

            LegateServiceCollectionExtensions.AddLegate(
                builder.Services,
                legate =>
                {
                    legate.Storage.UseSessionStore(new InMemorySessionStore(database));

                    var workspaceOptions = new ProcessWorkspaceRuntimeOptions
                    {
                        Root = Environment.CurrentDirectory,
                    };
                    legate.Workspace.UseRuntime(new ProcessWorkspaceRuntime(workspaceOptions, null, null));

                    legate.Permissions.UsePolicy(new AskGatedPolicy(GatedToolName));
                    legate.Tools.AddSource(new StaticSource(new AITool[] { GatedTool() }));
                });

            builder.Services.AddSingleton<ISessionEventStore>(new InMemorySessionEventStore(database));
            builder.Services.AddSingleton<ILlmProvider>(new StubScriptedProvider(scripted));
            builder.Services.AddSingleton<IChatClient>(scripted);

            using var host = builder.Build();

            // Resolve before starting: the resolve triggers the session
            // router wiring, which must land before the actor system spawns
            // its router.
            var client = host.Services.GetRequiredService<SessionClient>();

            await host.StartAsync(CancellationToken.None).ConfigureAwait(false);

            try
            {
                return await RunSessionAsync(client, plan).ConfigureAwait(false);
            }
            finally
            {
                await host.StopAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }
        catch (Exception error)
        {
            await Console.Error.WriteLineAsync($"csharp: {error.Message}").ConfigureAwait(false);
            return 1;
        }
    }

    private static async Task<int> RunSessionAsync(SessionClient client, StartPlan plan)
    {
        using var lifetime = new CancellationTokenSource();
        var cancellationToken = lifetime.Token;

        try
        {
            var options = new SessionOptions { Title = "csharphost" };
            var session = await client
                .OpenSessionAsync(AgentId.New(), options, cancellationToken)
                .ConfigureAwait(false);
            await Console.Out.WriteLineAsync($"SESSION {session.Id}").ConfigureAwait(false);

            // Queue the settle waiter before the prompt lands: a settle with
            // no waiter only records.
            var bound = TimeSpan.FromMinutes(plan.WaitMinutes);
            var wait = client.WaitForSettleAsync(session.Id, bound, cancellationToken);

            await client
                .PromptAsync(session.Id, UserMessage.Text(plan.Prompt), DeliveryMode.Queue, cancellationToken)
                .ConfigureAwait(false);

            // Stream until the waiter settles: terminal settle events are
            // not journaled, so the settle wait (not the event stream)
            // drives the end of the run.
            using var streamCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var outcome = new StreamOutcome();
            var streaming = StreamAndReplyAsync(client, session.Id, outcome, streamCts.Token);

            var result = await wait.ConfigureAwait(false);

            try
            {
                streamCts.Cancel();
                await streaming.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }

            await Console.Out.WriteLineAsync($"RESULT {result.Status}").ConfigureAwait(false);

            if (!string.IsNullOrEmpty(result.AssistantText))
            {
                await Console.Out.WriteLineAsync($"TEXT {result.AssistantText}").ConfigureAwait(false);
            }

            if (result.Status == TurnStatus.Completed
                && outcome.SawPermissionRequested
                && outcome.SawPermissionResolved)
            {
                return 0;
            }

            await Console.Error
                .WriteLineAsync(
                    $"csharp: expected a completed turn with a resolved permission "
                    + $"(status={result.Status} requested={outcome.SawPermissionRequested} resolved={outcome.SawPermissionResolved}).")
                .ConfigureAwait(false);
            return 1;
        }
        catch (OperationCanceledException)
        {
            await Console.Error.WriteLineAsync("csharp: the run was cancelled.").ConfigureAwait(false);
            return 1;
        }
        catch (LegateException error)
        {
            await Console.Error.WriteLineAsync($"csharp: {error.Message}").ConfigureAwait(false);
            return 1;
        }
    }

    private static async Task StreamAndReplyAsync(
        SessionClient client,
        SessionId sessionId,
        StreamOutcome outcome,
        CancellationToken cancellationToken)
    {
        await foreach (var evt in client.Subscribe(sessionId, 0L, cancellationToken).ConfigureAwait(false))
        {
            if (evt is null)
            {
                continue;
            }

            await Console.Out.WriteLineAsync($"EVENT {evt.GetType().Name}").ConfigureAwait(false);

            if (evt is PermissionRequestedEvent asked)
            {
                await Console.Out
                    .WriteLineAsync($"PERMISSION tool={asked.ToolName} id={asked.RequestId}")
                    .ConfigureAwait(false);
                await client
                    .ReplyAsync(
                        sessionId,
                        new PermissionDecision(asked.RequestId, PermissionDecisionKind.AllowOnce),
                        cancellationToken)
                    .ConfigureAwait(false);
                outcome.SawPermissionRequested = true;
            }
            else if (evt is PermissionResolvedEvent resolved)
            {
                await Console.Out
                    .WriteLineAsync($"RESOLVED id={resolved.RequestId} decision={resolved.Decision}")
                    .ConfigureAwait(false);
                outcome.SawPermissionResolved = true;
            }
        }
    }
}

internal static class Program
{
    private static Task<int> Main(string[] args)
    {
        return CSharpHost.RunAsync(args);
    }
}
