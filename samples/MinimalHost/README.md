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

## Kubernetes manifests

Sample-only deployment for a clustered MinimalHost on Kubernetes
(`samples/MinimalHost/kubernetes/` plus `samples/MinimalHost/Dockerfile`;
no bootstrap code of their own). The Deployment runs 3 replicas that
form a single Akka cluster through the `Legate.Cluster.Kubernetes`
bootstrap package:

- `namespace.yaml`: the dedicated `legate-demo` namespace.
- `rbac.yaml`: a ServiceAccount plus a namespaced Role/RoleBinding
  granting pod discovery (`get`, `list`, `watch` on `pods`).
  Discovery auto-detects the namespace from the service account:
  `KubernetesOptions.Namespace` stays unset.
- `deployment.yaml`: 3 replicas labelled `app=legate`, the app on
  port `8080`, Akka.Management on the port named `management`
  (`8558`), and the cluster environment below. Liveness probes
  `GET /healthz` on the app port; readiness probes `GET /ready` on
  the app port (filtered to the `legate-cluster` check, so 200 means
  this node is Up with the session role).
- `service.yaml`: a plain ClusterIP Service on the app port for
  humans and curl. Discovery queries the Kubernetes API, not DNS, so
  the Service stays out of the cluster-formation path.

Cluster formation is environment-driven:

| Variable | Value | Meaning |
|---|---|---|
| `Legate__Cluster__Mode` | `Kubernetes` | Bootstrap through Kubernetes discovery |
| `Legate__Cluster__MinimumMembers` | `3` | Each node waits for 3 Up members before serving |
| `Legate__Cluster__JoinTimeout` | `60s` | Bounds the 3-pod bootstrap plus the startup gate (12x the 5s default) |

The code default stays single-node (`Mode=Local`): `HostWiring`
binds the `Legate` configuration section and registers the
Kubernetes bootstrap hook with 3 required contact points, and the
hook stays idle unless `Cluster:Mode` selects Kubernetes.

## k3d runbook

Prerequisites: Docker, k3d, and kubectl. All commands run from the
repo root.

```bash
k3d cluster create legate-demo
docker build -f samples/MinimalHost/Dockerfile -t legate-minimalhost:local .
k3d image import legate-minimalhost:local -c legate-demo
kubectl apply -f samples/MinimalHost/kubernetes/
```

The image tag is `:local` with `IfNotPresent`: rebuild and
re-import the image before re-verifying, or stale node images
will keep running the previous build.

Verify three replicas form one cluster:

```bash
kubectl -n legate-demo rollout status deploy/legate-minimalhost
kubectl -n legate-demo get pods -l app=legate
# 3/3 Ready once every node is Up with the session role

kubectl -n legate-demo logs -l app=legate --prefix | grep "Application started"
# one line per pod: host startup completes only after the
# MinimumMembers=3 quorum is Up, so three lines means one 3-member cluster

kubectl -n legate-demo port-forward deploy/legate-minimalhost 5097:8080
curl http://127.0.0.1:5097/healthz
# ok
```

Per-pod probes (one terminal per pod, pod names from `get pods`):

```bash
kubectl -n legate-demo port-forward pods/legate-minimalhost-<suffix> 8080:8080
curl http://127.0.0.1:8080/healthz
# ok
curl http://127.0.0.1:8080/ready
# 200 once this node is Up with the session role (legate-cluster check)
```

Tear down:

```bash
k3d cluster delete legate-demo
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
- Cluster formation runs through the sample Kubernetes manifests
  above (`Legate.Cluster.Kubernetes` bootstrap, k3d-verified runbook);
  live providers never run in CI (scripted transports only). Never
  commit keys or session transcripts.
