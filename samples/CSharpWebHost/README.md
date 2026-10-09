# CSharpWebHost

Offline ASP.NET Core host with configuration-aware `AddLegate`, meaningful
`UseLegate`, request-scoped session DI and an explicit scripted provider. The
application owns all endpoints; the adapter exposes none automatically.

```sh
dotnet run --project samples/CSharpWebHost -- --urls http://127.0.0.1:5080
```

No keys or model network calls. Agent `01ARZ3NDEKTSV4RRFFQ69G5FAV` is registered
with model `scripted/echo` on every node. Defaults are paired in-memory metadata
and events, plus a Process workspace. Metadata is ephemeral. Process executes as
the host user without a sandbox and is unsafe for untrusted agents.

## HTTP workflow

```sh
curl -X POST http://127.0.0.1:5080/sessions -H 'Content-Type: application/json' -d '{"title":"demo"}'
# Copy sessionId as SID.
curl -X POST http://127.0.0.1:5080/sessions/$SID/ask -H 'Content-Type: application/json' -d '{"text":"hello"}'
curl -N http://127.0.0.1:5080/sessions/$SID/events
```

`/ask` demonstrates `PromptAndWaitAsync`. For approval workflows:

1. Start `GET /sessions/{id}/settle` before prompting. This waits for the next
   settlement, not durable retrieval of a past result.
2. `POST /sessions/{id}/prompt` with `{"text":"approve"}`.
3. Observe PermissionRequested on `/events` and POST `/reply` with its `requestId`.
4. The configured sample policy asks for `approval_demo` and denies other tools.
   A successful reply continues the original turn and resolves the settle waiter.

The SSE stream accepts `Last-Event-ID` as an exclusive cursor. `/journal` returns
a bounded replay page. `/abort-target` and `/abort` demonstrate exact-target control
receipts, not terminal completion. Cancellation passed from RequestAborted affects
that operation; accepted turns continue after the request ends. The sample omits
production authentication and HTTP exception mapping; deploy an application-owned
authorization boundary before UseLegate rather than trusting caller-selected tenants.

## Automated smoke and shutdown

```sh
dotnet build samples/CSharpWebHost/CSharpWebHost.csproj
python samples/CSharpWebHost/smoke.py
```

The smoke owns an isolated process on port 5181, exercises open/ask/SSE/permission
reply/pre-registered settlement, and requests graceful host shutdown. The shutdown
endpoint exists only with the smoke-only `Sample:Smoke=true` switch. It is not
enabled in a normal or cluster run and is not a production administration API.

Background application code uses `ISessionClientFactory.GetClient(tenant)` after
startup, not request-scoped SessionClient. Hosts complete all configuration before
startup. Core snapshots are private and graph-local; original options stay untouched.

See [cluster](cluster/README.md) for an explicitly configured shared Postgres
deployment and actual active-owner loss proof. Generic Host/CLI samples remain
supported independently of this ASP.NET Core package.
