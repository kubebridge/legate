# Getting started: hosted service

## C# ASP.NET Core integration

Reference `Legate.AspNetCore` and your provider package from a .NET 10 Web SDK
project. The adapter supplies a paired **process-local, ephemeral** session/event
database and a Process scratch workspace under `<content-root>/.legate/scratch`.
Process runs tools as the host user without a sandbox and is unsafe for untrusted
agents. Persisted scratch files do not make session metadata durable.

Use configuration-aware registration, then add your application's pipeline and
endpoints. The following progression is the compiled Build-page example. Supply
`Legate:Llm:Providers:openai:ApiKey` through secret configuration, never source.

```csharp
using Legate;
using Legate.AspNetCore;
using Legate.Llm.OpenAI;
using SessionOptions = Legate.SessionOptions;

var builder = WebApplication.CreateBuilder(args);
var agentId = AgentId.Parse("01ARZ3NDEKTSV4RRFFQ69G5FAV");
builder.Services.AddLegate(builder.Configuration, legate =>
{
    legate.Llm.AddOpenAI(builder.Configuration);
    legate.Agents.Add("assistant", agent =>
    {
        agent.Id = agentId;
        agent.Model = ModelReference.Parse("openai/gpt-4o-mini");
        agent.SystemPrompt = "Answer briefly.";
        return agent;
    });
});

var app = builder.Build();
app.UseLegate();
app.MapPost("/ask", async (SessionClient client) =>
{
    var session = await client.OpenSessionAsync(agentId);
    var result = await client.PromptAndWaitAsync(
        session.Id, "Hello, Legate!");
    return Results.Ok(result.AssistantText);
});
await app.RunAsync();
```

`UseLegate` resolves an authorized tenant and installs immutable request-client
binding. It does not create endpoints, authenticate users, start actors, or abort
accepted work at request completion. Hosted services own startup, admission,
shutdown and drain. Minimal hosting supplies routing automatically. In a
conventional pipeline, place `UseRouting`, application authentication and
authorization before `UseLegate`, then session-using endpoints. A resolver needing
endpoint metadata must run after routing.

Omitted cancellation uses `CancellationToken.None`, prompt delivery uses Queue,
open options are fresh interactive defaults, and subscription cursor is exclusive
zero. Standalone `WaitForSettleAsync` uses the client default bound (five minutes)
and must be registered **before** a fast prompt. It is not retrieval of an earlier
result. `PromptAndWaitAsync` registers its waiter first and uses a session timeout
when present; suspended approval workflows need `Subscribe` and `ReplyAsync`.
Pass `HttpContext.RequestAborted` to opt into request cancellation. That token can
prevent admission or abandon the wait/stream, but never implicitly aborts an
accepted turn. Targeted `AbortAsync` remains a separate durable control operation.
For inherently ambiguous bare `null`/`default` literals, use a typed cast or named
`text`, `message`, `options`, or `cancellationToken` argument.

### Tenant and background access

The fixed default is `TenantId.Default`. Non-default fixed tenants and trusted
request resolvers require declared complete `SessionHostBinding` graphs. Use
`ConfigureAspNetCore(o => o.ResolveTenant = ...)` only with application-established
tenant authority. No header, route or claim automatically authorizes a tenant.
Unknown tenants fail with `ScopeUnavailable`; application code owns HTTP mapping.

Scoped `SessionClient` fails outside middleware. Background workers resolve
`ISessionClientFactory` and call `GetClient(authorizedTenant)` after successful
startup. Lookup is read-only, creates no provider/scope/actor, and fails on an
undeclared tenant or stopping node. Runtime dispatcher and schedule workers use
the same initialized contexts, with each graph's frozen settings/stores/clock and
optional agent store, never the request-scoped facade or root execution fallback.
Polling all declared graphs is authoritative; wake hints only reduce latency.

Finish configuration before `StartAsync`. Options are privately copied and frozen
per graph. Default-model priority is client default, Llm default, a valid sole
provider default, then the legacy agent-file fallback. Only adapter-created chat
clients require an unambiguous provider/model choice. An explicit opaque
`IChatClient` is preserved, not inspected or reconstructed; the host is responsible
for configuring attribution if it targets a different model. Original options are
not mutated, including a differing lower-priority Llm default.

### Explicit distributed composition

Register both session and event stores together, plus an explicit execution
runtime, for StaticSeeds/Kubernetes. The adapter rejects its implicit local stores
and workspace in every graph, even a borrowed graph configured Local on a
clustered node. Custom compatible providers are supported. Successful startup
proves dependency wiring and atomic settlement compatibility, **not sharing,
durability, credential validity or network availability**.

This fragment takes an application-chosen `IWorkspaceRuntime`. It is compiled
against the actual packages; the executable deployment is in the web sample.

<!-- compile:deployment -->
```csharp
static void RegisterDistributed(WebApplicationBuilder builder, IWorkspaceRuntime execution)
{
    builder.Services.AddLegate(builder.Configuration, legate =>
    {
        legate.Llm.AddOpenAI(builder.Configuration);
        legate.UsePostgres(builder.Configuration);
        legate.Workspace.UseRuntime(execution);
    });
}
```

Reference `Legate.Storage.Postgres` and import `Legate.Storage` for this fragment.
Deployment configuration supplies cluster seeds/discovery, a genuinely shared
Postgres session/journal database and provider credentials. Each node needs
symmetric trusted bindings, agent identities/models, tools, policies and completion
destinations. S3 is optional unless required by enabled blob/expiry functionality.
Redis is needed only for enabled distributed LLM admission; its startup canary is
retained. Clustering does not turn local storage into shared state or automatically
enable distributed admission. Host-owned borrowed providers are disposed only
after node drain completes; a stop timeout is not quiescence.

Autonomous recovery independently discovers Running/current-turn candidates using
a bounded, finite keyset sweep. It sends existing check-inbox wakes, not takeover
authority. The execution factory's `TryRecoverControlTarget` still rejects live
owners, accepted stops and terminal-pending barriers, preserves the original
real-entry target/position, and fences the policy-correct recovered attempt. A
Running slot is already admitted and is not double-counted against new-work
capacity. Early lease refusal remains retryable on later sweeps.

Run the offline [CSharpWebHost](https://github.com/kubebridge/legate/tree/main/samples/CSharpWebHost) example or its
[shared-Postgres owner-loss harness](https://github.com/kubebridge/legate/tree/main/samples/CSharpWebHost/cluster).
Those assertions describe that sample deployment, not universal provider
capability certification.

## Existing F# web host

The `samples/MinimalHost` sample is the reference hosted host: a Giraffe
web host over the Legate session client facade (`SessionClient` plus
`SessionClientOperations`) exposing session open/prompt/reply/abort
endpoints with a per-session SSE event stream that resumes from
`Last-Event-ID`.

## Run

```bash
dotnet run --project samples/MinimalHost -- --urls http://127.0.0.1:5097
```

The default run is fully offline on scripted transports: no provider keys,
no network. A live provider registers only when its key is present in the
environment (keys come from the environment through configuration binding
only and are never printed or persisted):

```bash
export ANTHROPIC_API_KEY=...
dotnet run --project samples/MinimalHost -- --urls http://127.0.0.1:5097
```

`LEGATE_MINIMALHOST_PROVIDER` picks the live provider (`anthropic`,
`openai`, `google`; default: first registered in that order).
`LEGATE_MODEL` sets `<provider/model>` (default: the provider default).
Quiet the framework logs with `Logging__LogLevel__Default=None`.

## Endpoints

Health probe:

```bash
curl http://127.0.0.1:5097/healthz
# ok
```

Open a session (title optional, runtime default when absent):

```bash
curl -X POST http://127.0.0.1:5097/sessions \
  -H 'Content-Type: application/json' -d '{"title":"demo"}'
# {"sessionId":"01M2NE8QR8JVYRV5B9DETDKHGA","title":"demo","state":"Idle"}
```

Prompt it (`delivery` is `queue`, `inject`, or `interrupt`; default
`queue`):

```bash
SID=<sessionId>
curl -X POST http://127.0.0.1:5097/sessions/$SID/prompt \
  -H 'Content-Type: application/json' -d '{"text":"hello","delivery":"queue"}'
# {"sessionId":"<sessionId>","position":2,"delivery":"Queue"}
```

Reply to a suspended turn (permission decision or question answer). With
nothing pending the reply answers `409`:

```bash
curl -X POST http://127.0.0.1:5097/sessions/$SID/reply \
  -H 'Content-Type: application/json' \
  -d '{"$type":"permissionDecision","requestId":"<request-id>","decision":0}'
curl -X POST http://127.0.0.1:5097/sessions/$SID/reply \
  -H 'Content-Type: application/json' \
  -d '{"$type":"questionAnswer","questionId":"<question-id>","answer":"yes"}'
# 409 {"error":"The session has no pending request for the reply.","type":"ReplyMismatch"}
```

Read the current real-entry target, then request durable abort intent (`cause` is
`explicitAbort` or `hostShutdown`). A null target means no current turn. Acceptance
is not terminal completion; WaitingForInput refuses a new request. Retain the exact
target for retries after an uncertain response, rather than rereading and retargeting.

```bash
curl http://127.0.0.1:5097/sessions/$SID/abort-target
# Capture the returned turnId as TARGET before the request below.
curl -X POST http://127.0.0.1:5097/sessions/$SID/abort \
  -H 'Content-Type: application/json' \
  -d "{\"expectedTurnId\":\"$TARGET\",\"cause\":\"explicitAbort\",\"reason\":\"done exploring\"}"
# A HostAbortReceipt reports the acceptance outcome, not aborted:true.
```

Stream the session's events (replay from the cursor, then live):

```bash
curl -N http://127.0.0.1:5097/sessions/$SID/events
```

Resume from the last seen frame: the frame `id` is the journal sequence,
so `Last-Event-ID` replays everything after it with no duplicates:

```bash
curl -N -H 'Last-Event-ID: 0' http://127.0.0.1:5097/sessions/$SID/events
```

Error mapping is typed: unknown session `404`, closed or mismatched reply
`409`, expired journal `410`, subscriber cap `429`, bad ids and shapes
`400`.

## From sample to service

The sample keeps storage InMemory (sessions live for the process lifetime)
and the workspace root on the process working directory. A production
hosted service swaps in the durable pieces through configuration (see the
[configuration reference](configuration.md)):

- `Legate:Storage:Postgres` (connection string, `legate` schema, table
  prefix) for sessions, inbox, turns, and the event journal.
- `Legate:Storage:S3` for blobs and agent packages.
- `Legate:Workspace:Docker` for sandboxed per-session workspaces.
- `Legate:Cluster` (`StaticSeeds` or `Kubernetes` mode) to scale past one
  node; headless sessions with `AutoClose` plus a registered completion
  destination deliver webhooks for one-shot style work. Register the same
  destination id for the same receiver on every node with
  `LegateBuilder.AddCompletionDestination`; hosts on 0.1.0 prerelease data
  start clean (fresh database), with no mixed-version support.

Cluster formation for the sample's Kubernetes manifests and the k3d
runbook is documented in `samples/MinimalHost/README.md`. Never commit
keys or session transcripts.
