# Getting started: hosted service

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

Abort the running turn (`cause` is `explicitAbort` or `hostShutdown`;
Idle and WaitingForInput acknowledge without effect):

```bash
curl -X POST http://127.0.0.1:5097/sessions/$SID/abort \
  -H 'Content-Type: application/json' \
  -d '{"cause":"explicitAbort","reason":"done exploring"}'
# {"sessionId":"<sessionId>","aborted":true}
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
  node; headless sessions with `AutoClose` plus a completion sink deliver
  webhooks for one-shot style work.

Cluster formation for the sample's Kubernetes manifests and the k3d
runbook is documented in `samples/MinimalHost/README.md`. Never commit
keys or session transcripts.
