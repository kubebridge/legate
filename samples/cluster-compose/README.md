# Cluster Compose Harness

Sample-only three-node Legate cluster in `StaticSeeds` mode (issue 145).
No `Legate.Cluster.Docker` package: this is a harness plus an `.fsx`
smoke, not a runtime. All commands run from the repo root unless noted.

## The smoke (recommended)

The script owns the lifecycle: it builds and starts the stack, waits for
the quorum, runs the mid-turn kill, checks the survivors, and tears down
on success (on failure the stack stays up for inspection):

```bash
LEGATE_MINIMALHOST_REPLY_DELAY_MS=10000 dotnet fsi samples/cluster-compose/smoke.crash-resume.fsx
```

The reply delay is required, not optional: without it the scripted turn
settles before the kill and the smoke proves nothing. The script fails
fast when the variable is unset. A re-run after a killed node 1 needs a
fresh `docker compose down -v` first (see Bootstrap order).

## Manual start

```bash
LEGATE_MINIMALHOST_REPLY_DELAY_MS=10000 docker compose --project-directory samples/cluster-compose up --build -d
docker compose --project-directory samples/cluster-compose ps
curl http://127.0.0.1:8081/healthz
# ok
curl http://127.0.0.1:8081/ready
# 200 once the 3-member quorum is Up with the session role
```

Tear down:

```bash
docker compose --project-directory samples/cluster-compose down -v
```

## Ports

| Endpoint | Host | Container | Notes |
|---|---|---|---|
| legate-1 app | 8081 | 8080 | `GET /healthz` liveness, `GET /ready` cluster readiness |
| legate-2 app | 8082 | 8080 | same |
| legate-3 app | 8083 | 8080 | same |
| remoting | internal only | 4053 | `Legate:Cluster:RemotingPort`; seeds are `legate-1:4053`, `legate-2:4053`, `legate-3:4053` |
| postgres | internal only | 5432 | `postgres:16-alpine`, db/user `legate` |
| redis | internal only | 6379 | `redis:7-alpine`, StackExchange `redis:6379` |

Each node binds its own compose hostname (`Legate:Cluster:RemotingHostname`
is `legate-1` on legate-1, `legate-2` on legate-2, `legate-3` on legate-3)
so the bound address matches the address peers dial: binding `0.0.0.0`
leaves Akka's inbound address as `0.0.0.0:4053` and the cluster drops
seed messages as non-local. The code default stays loopback for
single-process hosts. Remoting ports are stable and config-declared (one
per node, all 4053 here with distinct hostnames), never ephemeral, so
seed entries can name them.

## Bootstrap order

`docker compose up` starts `legate-1` first and holds `legate-2/3` on its
`/ready` healthcheck. The asymmetry is deliberate: `legate-1` runs with
`MinimumMembers 1` so it forms (and reports ready) alone, while the
followers require the full quorum of 3.

A simultaneous start does not converge here: the StaticSeeds join is
one-shot per node, and a node whose first seed contact fails (peers still
in dotnet startup, which takes tens of seconds and staggers under CPU
contention) joins itself and never retries the seeds, leaving three
permanent singletons. Gating the followers on a listening seed makes
first contact succeed. A runtime-level join retry belongs to the cluster
epics, not this harness; the image installs `curl` for the `/ready`
probes (sample-only).

## Scale to N nodes

Compose service names are the seeds, so scaling means editing
`docker-compose.yaml`, not `docker compose up --scale`:

1. Copy one follower block to `legate-4` with the next app port
   (e.g. `8084:8080`), its own `RemotingHostname: legate-4`, and
   `MinimumMembers: N`.
2. Append `legate-4:4053` to every node's `Legate__Cluster__SeedNodes__*`
   list (add `__3: legate-4:4053`).
3. Bump every follower's `Legate__Cluster__MinimumMembers` to `N`
   (`legate-1` stays the bootstrap at 1).
4. `docker compose --project-directory samples/cluster-compose up --build -d`
   and wait for all N `/ready` probes.

Single-node sanity: `Mode=Local` (the code default) needs no seeds,
ports, or quorum; the harness exists for multi-process remoting,
serialization, and node-loss timing that in-process tests cannot see.

## Inspect cluster state

```bash
docker compose --project-directory samples/cluster-compose ps
docker compose --project-directory samples/cluster-compose logs legate-1 --tail=100
curl http://127.0.0.1:8081/healthz  # ok = process alive
curl http://127.0.0.1:8081/ready    # 200 = Up with the session role, quorum met
curl http://127.0.0.1:8082/ready
curl http://127.0.0.1:8083/ready
```

Open a session on node 1 and stream it from node 2 (cross-node read):

```bash
SID=$(curl -s -X POST http://127.0.0.1:8081/sessions -H 'Content-Type: application/json' -d '{"title":"demo"}' | python -c "import json,sys; print(json.load(sys.stdin)['sessionId'])")
curl -s -X POST http://127.0.0.1:8081/sessions/$SID/prompt -H 'Content-Type: application/json' -d '{"text":"hello","delivery":"queue"}'
curl -N http://127.0.0.1:8082/sessions/$SID/events
```

Storage and coordination:

```bash
docker compose --project-directory samples/cluster-compose exec postgres psql -U legate -c '\dt legate.*'
docker compose --project-directory samples/cluster-compose exec redis redis-cli ping
# PONG
```

## Crash-resume note

`Turns:CrashResume` defaults to `Fail` while the per-session
`SessionOptions.OnCrashResume` defaults to `ResumeAttempt`; the runtime
crash path reads the per-session knob. The smoke accepts either valid
outcome (`TurnFailedEvent` for Fail/FailAttempt, `TurnCompletedEvent`
for RetryTurn/ResumeAttempt) and always requires gap-free,
duplicate-free subscriber sequences plus a single settle.

Honest scope: MinimalHost seeds no agents and opens sessions with a fresh
random agent id, so turns fail fast at agent load before any LLM call
(where the reply delay would hold them). The kill therefore races
dispatch: what the smoke proves is cross-node inbox recovery (a survivor
picks up the pending prompt), exactly-once terminal settlement, a gapless
stream, and keep-majority survival, not an LLM-mid-call resume. A
deterministic mid-call kill needs a seeded smoke agent plus
open-with-agent-id, a sample-contract change left as follow-up.
Split-brain partitions (`docker network disconnect`) stay a stretch goal
and are not covered.

## CI

`.github/workflows/ci.yml` job `cluster-compose` (self-hosted Linux,
Docker available) rebuilds the image, waits for all three `/ready`
probes, runs the smoke, and tears down. It is required for changes
under `src/Legate/Cluster/**` and the wire format.
