# MinimalHost sample

Giraffe web host over the Legate session client facade (`SessionClient`
plus `SessionClientOperations`): session open/prompt/reply/abort endpoints
with a per-session SSE event stream that resumes from `Last-Event-ID`.

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

```bash
export OPENAI_API_KEY=...
LEGATE_MINIMALHOST_PROVIDER=openai LEGATE_MODEL=openai/gpt-4o-mini \
  dotnet run --project samples/MinimalHost -- --urls http://127.0.0.1:5097
```

```bash
export GOOGLE_API_KEY=...
LEGATE_MINIMALHOST_PROVIDER=google \
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

Prompt it (`delivery` is `queue`, `inject`, or `interrupt`; default `queue`):

```bash
SID=<sessionId>
curl -X POST http://127.0.0.1:5097/sessions/$SID/prompt \
  -H 'Content-Type: application/json' -d '{"text":"hello","delivery":"queue"}'
# {"sessionId":"<sessionId>","position":2,"delivery":"Queue"}
```

Reply to a suspended turn (permission decision or question answer).
With nothing pending the reply answers `409`:

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
# : connected
# (frames follow as the journal lands them)
```

Resume from the last seen frame: the frame `id` is the journal sequence,
so `Last-Event-ID` replays everything after it with no duplicates:

```bash
curl -N -H 'Last-Event-ID: 0' http://127.0.0.1:5097/sessions/$SID/events
```

Error mapping is typed: unknown session `404`, closed or mismatched
reply `409`, expired journal `410`, subscriber cap `429`, bad ids and
shapes `400`.

```bash
curl -X POST http://127.0.0.1:5097/sessions/not-a-session/prompt \
  -H 'Content-Type: application/json' -d '{"text":"x"}'
# 400 {"error":"'not-a-session' is not a session id.","type":"BadRequest"}
```

## Scope notes

- Storage is InMemory only (the merged `Legate.Storage.InMemory`
  package): sessions live for the process lifetime. A fresh process
  starts with an empty store.
- The workspace root is the process working directory.
- The stream carries the suspension, compaction, and failure lifecycle
  plus the `: connected` comment on subscribe. A plain scripted turn
  completes with no journal entries of its own, so the stream stays open
  and quiet until the journal lands something. The stream ends on the
  session-closed event; a slow consumer past the bus bounds (512 live
  subscribers per session, 128 buffered events each) is disconnected
  instead of stalling the turn.
- Cluster and Kubernetes manifests are out of scope; live providers never
  run in CI (scripted transports only). Never commit keys or session
  transcripts.
