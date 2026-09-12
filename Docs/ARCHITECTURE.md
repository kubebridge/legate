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
