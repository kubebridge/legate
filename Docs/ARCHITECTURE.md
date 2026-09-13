# Legate Architecture

Legate is an agent harness: a durable Akka.NET runtime for tool-using AI
agents, packaged as a set of .NET libraries a host registers with
`AddLegate(...)`. This document is the normative description of the model,
the package boundaries, and the rules that keep them apart. Implementation
conventions live in `Docs/agents/code-rules.md`.

## Core model

| Concept | Meaning |
|---|---|
| **Agent** | A definition: model config, system prompt, tool set, skills, sub-agents, permission defaults, package source. |
| **Session** | A conversation with one agent (switchable per turn). Has an id, tenant, title, state, transcript, inbox, and a workspace binding. Lives until closed or expired. |
| **UserMessage** | What the user sent: a list of parts (`Microsoft.Extensions.AI.AIContent`: text, file, image). The only thing that starts a turn. Journaled before it is acted on. |
| **Reply** | The host's answer to something the agent is waiting on: a `PermissionDecision` or a `QuestionAnswer`. Resumes a suspended turn; never starts one. |
| **Turn** | One run of the ReAct loop triggered by a `UserMessage`. Ends when the model stops calling tools, the host aborts, or the budget runs out; suspends while waiting on a reply. The lease and claim unit. |
| **SessionEvent** | Fine-grained, ordered, journaled stream out of a session (`TurnStarted`, `TextDelta`, `ToolCallStarted`, `PermissionRequested`, `TurnCompleted`, ...). Hosts render these. |
| **SessionCell** | The coarse transcript (user, assistant, tool call, tool result, system), derived from events, kept for replay and UI. |

Session state machine: `Idle -> Running -> (Idle | WaitingForInput | Closed)`.

There is no job type. A headless run is a session opened with `AutoClose`,
an `AllowAll` permission policy, an optional completion sink, and optionally
`Outcome = Structured` (which gives the agent `finish`/`fail` tools).

### Client API

| Area | Members |
|---|---|
| Sessions | `OpenSession`, `ResumeSession`, `GetSession`, `ListSessions`, `Close`, `Fork`, `SetAgent` |
| Prompting | `Prompt(sessionId, UserMessage, DeliveryMode)` where `DeliveryMode` is `Queue`, `Inject`, or `Interrupt` |
| Replying | `Reply(sessionId, Reply)` |
| Control | `Abort`, `Compact` |
| Reading | `ReadTranscript`, `ReadEvents(fromSequence)`, `Subscribe(sessionId, fromSequence)` as `IAsyncEnumerable<SessionEvent>` |
| Sugar | `PromptAndWait` extension returning `TurnResult` |

### Turn lifecycle

1. The host prompts. The message is appended to the session inbox in
   `ISessionStore` and the dispatcher wakes the session entity (a sharded
   entity in cluster mode, a local child actor in single-node mode).
2. The session actor claims the turn under a lease, renews it on a heartbeat,
   and fences every side effect with the claim token. On restart it replays
   the journal and resumes or fails the interrupted turn according to policy.
3. It binds the workspace through `IWorkspaceRuntime`, loads the agent
   package (`AGENTS.md`, `.agent/skills/*/SKILL.md`, `.agent/agents/*/AGENT.md`),
   and builds an `IChatClient` wrapped by the LLM coordinator (rate limiting,
   queueing, cooldowns, optional distributed admission) and the model policy.
4. It assembles tools: built-ins (file, exec, skill, task, `ask_user`), custom
   HTTP tools, and every `IToolSource`. Each call passes `IPermissionPolicy`.
5. It runs the ReAct loop, emitting events to the journal and live subscribers,
   folding injected messages in at iteration boundaries.
6. It checkpoints usage, settles the turn, and returns the session to `Idle`
   or `WaitingForInput`; `AutoClose` sessions close and notify the completion
   sink with at-least-once delivery.

### Store contracts

`ISessionStore` is the durable store contract the Postgres, SQLite, and
in-memory implementations implement: session CRUD with tenant-scoped list
paging (`SessionPage`), the session inbox in front of the turn queue
(`AppendInboxMessage`, `ReadPendingInbox`, `MarkInboxConsumed`, with the
`InboxEntry` envelope carrying a `UserMessage` or a `Reply` plus its
`DeliveryMode`), turn claims under a lease, dispatch candidates, and the
capacity count queries. Every method takes the `TenantId` the data belongs
to; isolation across tenants is enforced in the stores, not only in the
host.

- **Atomicity.** Inbox append, `ClaimNextTurn`, `RenewClaim` /
  `ObserveAndRenewClaim`, `CheckpointUsage`, `SettleTurn`, and
  `UpdateSessionState` must be atomic: each lands in one transaction, so a
  reader never observes a half-applied step. `ClaimNextTurn` in particular
  hands a turn to exactly one caller; a loser observes no claimable turn,
  never a double claim.
- **Fencing.** The claim token lives on `TurnClaim` (with its owner, expiry,
  and attempt) and nowhere else; it is opaque: stores mint it, callers carry
  it verbatim, nothing parses it. Every side effect on behalf of a turn
  (checkpoint, settle, abort, and every tool call or journal write the
  runtime performs after claiming) verifies the token at the last moment. A
  correlation id is evidence, not authority. A stale token must never
  produce an effect: `CheckpointUsage`, `SettleTurn`, and `AbortTurn`
  return the lost/rejected outcomes instead of acting.
- **Results, not exceptions, for lease states.** Lease states
  (`TurnLeaseState`: held, renewed, lost, expiring, missing) and settlement
  outcomes (`TurnSettlement`: settled, already settled, rejected stale) are
  result objects the caller branches on; they carry stable `$type`
  discriminators on the wire like `Reply` and `TurnOutcome`. Control-plane
  preconditions (an unknown session, a disallowed state) throw the
  `Exceptions.fs` family.
- **Capacity.** The dispatcher enforces per-agent, per-tenant, and
  per-process limits with three count queries
  (`CountSessionsByAgent`, `CountSessionsByTenant`,
  `CountRunningSessions`) and wakes sessions with pending work through
  `GetDispatchCandidates`, a tenant-scoped bounded batch
  (`DispatchBatch`).

`ISessionEventStore` is the durable store contract for the ordered
per-session event journal, kept separate from `ISessionStore`: the journal
is the system-facing audit trail and the source of `Subscribe`. It appends
`SessionEvent` batches under the turn's claim token (the store assigns
per-session monotonic, gap-free 1-based sequences and stamps them on the
returned events; a stale token rejects the whole batch with zero writes),
replays bounded pages by an exclusive int64 cursor (`EventReplayOutcome`:
page with `NextCursor`, end of stream, unknown session, expired journal),
and leases journal cleanup to a background worker
(`TryClaimCleanup` / `CompleteCleanup` / `DeferCleanup` over an opaque
`EventCleanupClaim` token). Limit values (per-event bytes, per-session
count and bytes, batch size) are host-configured runtime options; a breach
throws `EventLimitExceededException` with structured properties before any
part of the batch lands. Sanitisation is a runtime concern applied before
the append; the store persists what it is given and validates only bounds.

## Package layout

```
Legate.Abstractions         contracts: domain types, store/tool/workspace/policy interfaces
                            deps: FSharp.Core, Microsoft.Extensions.AI.Abstractions (no Akka)
Legate                      runtime: actors, sharding, dispatcher, ReAct loop, built-in tools,
                            local LLM coordinator, hosted services, AddLegate()
Legate.Llm.OpenAI           OpenAI, Anthropic (OpenAI-compatible), Ollama Cloud, any compatible base URL
Legate.Llm.Google           Gemini via Google.GenAI
Legate.Storage.Postgres     ISessionStore, ISessionEventStore, IAgentStore + migrations
Legate.Storage.Sqlite       same stores on one SQLite file (CLI harness)
Legate.Storage.S3           IBlobStore, IAgentPackageStore
Legate.Storage.FileSystem   IBlobStore, IAgentPackageStore on local disk
Legate.Storage.InMemory     all stores in memory (tests, samples)
Legate.Coordination.Redis   IDistributedLlmAdmission
Legate.Workspace.Docker     IWorkspaceRuntime over the docker CLI
Legate.Workspace.HostDirectory  IWorkspaceRuntime bound to a host directory (CLI harness)
Legate.Workspace.Process    IWorkspaceRuntime on a scratch directory, no sandbox
Legate.Cluster.Kubernetes   Akka.Management + Akka.Discovery.KubernetesApi bootstrap
Legate.Mcp                  IToolSource over the ModelContextProtocol SDK
Legate.Testing              fakes, clock/delay/random seams, scripted IChatClient
```

Only `Legate.Abstractions`, `Legate`, and `Legate.Tests` exist today. New
packages are added under `src/<PackageName>/` with a matching test module in
`tests/Legate.Tests` (or their own `tests/<PackageName>.Tests` when they need
external services), registered in `Legate.slnx`, and versioned through
`Directory.Packages.props`.

## Boundaries

- Compile-time references point inward: every package references
  `Legate.Abstractions`; provider and storage packages reference `Legate`
  only when they need runtime helpers; `Legate.Abstractions` references
  nothing else in the solution.
- `Legate` must run with only `Legate.Storage.InMemory` +
  `Legate.Workspace.Process` + one `Legate.Llm.*` package, single node, no
  Redis, no Docker, no Kubernetes. That is the test-suite bar.
- Tenancy: every store call carries a `TenantId`; a single-tenant host uses
  `TenantId.Default`. Isolation across tenants is enforced in the stores, not
  only in the host.
- Host hooks (`ISessionAdmissionPolicy`, `IUsageObserver`, `IModelPolicy`,
  `IToolSource`, `IPermissionPolicy`, `IWorkspaceRuntime`) are the only places
  where a host's business rules enter the runtime. Legate reports tokens; it
  never prices them.

## Observability

- `ILogger<T>` with structured scopes (`SessionId`, `TurnId`, `AgentId`,
  `TenantId`, `Attempt`, `ClaimOwner`).
- `System.Diagnostics.Metrics` through `Meter("Legate")`.
- `ActivitySource("Legate")` around turns, LLM calls, and tool calls.

## Origin

Legate is extracted from the agent execution runtime of BridgeMCP. The
extraction plan (seams, phases, risks) is tracked in the project's planning
notes and on the issue board; this document describes the target, not the
migration.
