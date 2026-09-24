# Configuration reference

Legate binds one `Legate` configuration section onto `LegateOptions`
(`Legate.Abstractions`, `LegateOptions.fs`); standalone packages bind
their own `Legate:...` sections. Environment variables override any key
with the `Legate__Section__Key` form (for example
`Legate__Cluster__Mode=StaticSeeds`).

Conventions:

- Durations accept the `500ms`, `30s`, `15m`, `1h`, `30d` forms (plus
  `TimeSpan` strings such as `00:00:30`) on the options bound through the
  `Legate` root; `Legate:Archive` uses the standard `TimeSpan` forms.
- Flat-choice enums parse case-insensitively by name; unknown values fail
  startup with the offending section path instead of silently becoming
  zero.
- Every options class carries a `Validate()` that returns null when valid;
  the binder throws on the first violation with its section path.
- API keys and signing secrets are never logged or embedded in
  diagnostics.

## `Legate` root (`LegateOptions`)

| Property | Section type | Meaning |
|---|---|---|
| `Sessions` | `SessionsOptions` | Admission, leases, expiry, sub-agents, subscriptions |
| `Dispatcher` | `DispatcherOptions` | Pending-work sweep cadence and batching |
| `Turns` | `TurnsOptions` | Per-turn budgets, prompt delivery, crash resume |
| `Permissions` | `PermissionsOptions` | Default decision, ask timeout, per-tool rules |
| `Llm` | `LlmOptions` | Models, coordination, providers |
| `Workspace` | `WorkspaceSectionOptions` | Workspace mode, root, idle teardown |
| `Completion` | `CompletionOptions` | Headless delivery attempts, retries, retention |
| `AskUser` | `AskUserOptions` | Headless question policy |
| `Cluster` | `ClusterOptions` | Deployment mode, sharding, split-brain timings |
| `Pruning` | `ContextPruningOptions` | Context-window pruning reserve and markers |
| `Schedules` | `ScheduleOptions` | Schedule evaluator sweep |

## `Legate:Sessions` (`SessionsOptions`)

| Key | Default | Meaning |
|---|---|---|
| `Capacity` | `4` | Sessions the dispatcher admits per process |
| `MaxSessionsPerTenant` | `100` | Sessions admitted per tenant |
| `LeaseDuration` | `60s` | How long a claimed turn lease lasts without renewal |
| `LeaseRenewalInterval` | `15s` | Lease heartbeat; must stay below half the lease |
| `Expiry` | unset | Idle time before the sweeper closes a session; unset disables expiry (requires a durable blob store when set) |
| `SubAgents` | (see below) | Task-tool nesting depth and per-run timeout |
| `AutoTitle` | `false` | Title untitled sessions from their first prompt |
| `AutoTitleModel` | null | Title model in `provider/model` form; falls back to `Legate:Llm:Compaction`, then the session/default model |
| `MaxSubscribersPerSession` | `512` | Live subscribers per session before new ones reject |
| `PerSubscriberBufferSize` | `128` | Buffered events per cross-node subscriber before it is treated as slow and disconnected |
| `SubscriptionReplayCacheSize` | `256` | Recent journaled events kept for resuming subscribers |
| `SubscriptionMaxEventPayloadBytes` | `1048576` | Largest single event streamed to a remote subscriber |

### `Legate:Sessions:SubAgents` (`SubAgentsOptions`)

| Key | Default | Meaning |
|---|---|---|
| `MaxDepth` | `1` | Task-tool nesting depth; deeper calls return a tool error |
| `Timeout` | `10m` | Per nested-run timeout, further bounded by the parent's remaining budget |

## `Legate:Dispatcher` (`DispatcherOptions`)

| Key | Default | Meaning |
|---|---|---|
| `PollInterval` | `5s` | How often the dispatcher sweeps for sessions with pending work |
| `MaxBatchSize` | `50` | Candidates drained per sweep at most |
| `MaxSessionsPerAgent` | `4` | Sessions of one agent admitted at once |

## `Legate:Turns` (`TurnsOptions`)

| Key | Default | Meaning |
|---|---|---|
| `DefaultMaxIterations` | `50` | Model iterations per turn when the session names no budget |
| `DefaultTimeout` | `30m` | Wall-clock budget per turn when the session names none |
| `DefaultDelivery` | `Queue` | `Queue`, `Inject`, or `Interrupt` for prompts to a busy session |
| `CrashResume` | `Fail` | `Fail` settles the interrupted turn as failed; `RetryTurn` re-runs it |

## `Legate:Permissions` (`PermissionsOptions`)

| Key | Default | Meaning |
|---|---|---|
| `DefaultDecision` | `Deny` | Decision when no rule matches (`AllowOnce`, `AllowForSession`, `Deny`) |
| `AskTimeout` | `5m` | How long the runtime waits for a host answer before the turn fails |
| `Rules` | empty | Ordered per-tool rules: `ToolPattern` (supports `mcp:<server>:*` shapes) plus `Decision` |

## `Legate:Llm` (`LlmOptions`)

| Key | Default | Meaning |
|---|---|---|
| `DefaultModel` | null | Default model in `provider/model` form |
| `Compaction` | null | Compaction model in `provider/model` form (`Legate:Llm:Compaction`); null compacts through the session's model |
| `CompactionKeepMessages` | `10` | Recent history messages kept after the summary |
| `Coordination` | (see `LlmCoordinationOptions`) | Rate, concurrency, and retry knobs |
| `Providers` | empty | Per-provider settings keyed by provider id |
| `DistributedCoordination` | `false` | Admit work through the distributed admission seam |

### `Legate:Llm:Providers:<id>` (`LlmProviderOptions` and per-provider subtypes)

| Key | Default | Meaning |
|---|---|---|
| `ApiKey` | null | Provider API key (or supply keys via `IApiKeyProvider`); never logged |
| `Model` (Google) | `gemini-3.8-flash` | Default model the Gemini provider serves (`GoogleLlmOptions`) |
| `Timeout` (Google) | `100s` | Per-call timeout the Gemini SDK HTTP client enforces |

Provider sections converge on `Legate:Llm:Providers:<Name>`; the OpenAI
package additionally binds named presets from
`Legate:Llm:Providers:<preset-id>`.

### `Legate:Llm:DistributedCoordination` (`DistributedCoordinationOptions`)

| Key | Default | Meaning |
|---|---|---|
| `Mode` | `Disabled` | `Disabled` coordinates locally; `Redis` admits through one shared Redis instance |
| `ConnectionString` | empty | Redis connection (for example `127.0.0.1:6379`); required for `Redis`, never logged |
| `KeyPrefix` | `legate:llm` | Isolates one deployment's admission keys |
| `StartupRequired` | `true` | Startup proves Redis before reporting ready |
| `FailClosed` | `true` | Reject new work when Redis is unavailable; `false` admits locally under bounded emergency limits |

## `Legate:Workspace` (`WorkspaceSectionOptions`)

| Key | Default | Meaning |
|---|---|---|
| `Mode` | `Process` | `Process` (scratch directory, no sandbox), `HostDirectory`, or `Docker` |
| `RootPath` | null | Where host-directory workspaces live; null when the mode needs no root |
| `IdleTeardownAfter` | `10m` | Idle time before the runtime tears down the execution vehicle |

## `Legate:Completion` (`CompletionOptions`)

| Key | Default | Meaning |
|---|---|---|
| `MaxDeliveryAttempts` | `3` | Delivery attempts per headless completion |
| `RetryDelay` | `30s` | Wait between delivery attempts |
| `DeliveredRetention` | `7d` | How long delivered outbox rows are retained for idempotency |
| `RedriveInterval` | `30s` | How often the re-drive service polls the outbox |
| `ClaimLeaseDuration` | `60s` | Re-drive delivery lease before another owner may claim the row |

## `Legate:AskUser` (`AskUserOptions`)

| Key | Default | Meaning |
|---|---|---|
| `Mode` | `Fail` | `Fail` fails the turn when no host can answer; `AnswerWith` continues with `CannedAnswer` |
| `CannedAnswer` | null | Verbatim answer for `AnswerWith`; must be non-empty in that mode |

## `Legate:Cluster` (`ClusterOptions`)

| Key | Default | Meaning |
|---|---|---|
| `Mode` | `Local` | `Local`, `StaticSeeds`, or `Kubernetes` |
| `RemotingPort` | `0` | Remoting TCP port (`0` is ephemeral); compose declares one stable port per node |
| `RemotingHostname` | `127.0.0.1` | Remoting bind hostname (`0.0.0.0` inside containers) |
| `SeedNodes` | empty | `host:port` seeds for `StaticSeeds`; required non-empty in that mode, ignored for `Kubernetes` |
| `Roles` | empty | This node's cluster roles |
| `SessionRole` | `session` | Role sharding hosts session entities on |
| `ShardCount` | `128` | Session-region shards (part of the version stamp) |
| `ShardHashVersion` | `1` | Shard hash version mixed into the version stamp |
| `ShutdownGraceSeconds` | `30s` | Graceful-shutdown wait (coordinated shutdown on host stop) |
| `MaxWirePayloadBytes` | `1048576` | Largest wire payload accepted, capping every manifest on top of its per-case bound |
| `StableAfter` | `20s` | Split-brain resolver stabilization wait (`akka.cluster.split-brain-resolver.stable-after`) |
| `DownRemovalMargin` | `off` (`Zero`) | `Zero` emits `off`; positive emits the duration (`akka.cluster.down-removal-margin`) |
| `DownAllWhenUnstable` | `on` (null) | Null emits `on`; `Zero` emits `off`; positive emits the duration |
| `JoinTimeout` | `5s` | Seed-node join wait (`akka.cluster.seed-node-timeout`); also bounds the startup quorum wait |
| `HostExitDeadline` | `60s` | Legate-level total bound on the cluster hosted service's stop; never rendered into HOCON |
| `MinimumMembers` | `1` | Up members awaited before `StartAsync` completes |

## `Legate:Pruning` (`ContextPruningOptions`)

| Key | Default | Meaning |
|---|---|---|
| `ReservedBufferTokens` | `10000` | Tokens held back beyond the model's reserved output |
| `KeepLastAssistantTurns` | `2` | Recent assistant turns pruning never touches |
| `PrunedMarker` | `[legate-pruned-tool-result]` | Replacement text written into pruned tool-result cells |

## `Legate:Schedules` (`ScheduleOptions`)

| Key | Default | Meaning |
|---|---|---|
| `PollInterval` | `60s` | How often the evaluator sweeps agents with enabled schedules |

## `Legate:Artifacts` (`ArtifactOptions`)

| Key | Default | Meaning |
|---|---|---|
| `MaxEncodedBytes` | `10485760` (10 MiB) | Largest encoded artifact payload |
| `MaxDecodedBytes` | `134217728` (128 MiB) | Largest decoded image (width x height x 4 RGBA bytes) |
| `MaxWidth` / `MaxHeight` | `8192` | Largest image dimension in pixels |
| `MaxPixels` | `16777216` | Largest image pixel count |
| `PreviewThresholdBytes` | `1048576` (1 MiB) | Encoded size above which images gain a JPEG preview |
| `PreviewMaxDimension` | `1024` | Long edge of a generated JPEG preview |
| `AllowedImageMediaTypes` | png, jpeg, gif, webp | Accepted image media types |
| `AllowedVideoMediaTypes` | mp4, quicktime, webm, matroska | Accepted video media types |
| `PresignedExpiry` | `7d` | Presigned artifact URL lifetime |
| `ReservationReclaimAfter` | `5m` | Stale quota-reservation reclamation bound |

## `Legate:Archive` (`JournalArchiveOptions`)

| Key | Default | Meaning |
|---|---|---|
| `ArchiveDirectory` | null | Where archived journals land (`<tenant>_<session>/events.jsonl`); null/empty disables the worker |
| `RetentionDelay` | `7d` | Wait after session close before archival |
| `PollInterval` | `1h` | Time between archive passes |
| `LeaseDuration` | `10m` | Journal-cleanup lease per pass |
| `VerifyBackoff` | `1m` | Base backoff after an archive verification failure (plus jitter) |

Durations here use the standard `TimeSpan` forms; the `Legate`-root
shorthand forms do not apply.

## `Legate:Tools:Mcp` (`McpOptions`, `Legate.Mcp`)

| Key | Default | Meaning |
|---|---|---|
| `Servers` | empty | MCP servers in order; each names exactly one transport: stdio (`Command` plus optional `Arguments`/`EnvironmentVariables`) or streamable HTTP (`Url` plus optional `Headers`) |
| `CollisionPolicy` | `Fail` | `Fail` degrades to zero tools on duplicate sanitized names; `Suffix` renames with `_2`, `_3` |
| `Servers:<name>:Overrides` | none | `DisabledTools` hides server tools; `DescriptionOverrides` rewrites descriptions (unknown tool names are ignored) |

Servers can also load from a Claude-style `mcp.json` via
`ToolsBuilder.AddMcpServersFromConfig(path)` with `${VAR}` expansion from
the process environment.

## Storage

| Section | Options class | Key properties |
|---|---|---|
| `Legate:Storage:Postgres` | `PostgresOptions` | `ConnectionString` (required); `Schema` (`legate`); `TablePrefix` (empty); `RunMigrations` (`true`); journal caps `MaxEventBytes`, `MaxEventsPerSession`, `MaxJournalBytesPerSession`, `MaxAppendBatchSize` (`0` is unbounded) |
| (code API) | `SqliteStorageOptions` | Configured via `UseSqlite(path)`, not a section: `Path` (required), `BusyTimeout` (`5s`), `TablePrefix` (empty), `RunMigrations` (`true`) |
| `Legate:Storage:S3` | `S3StorageOptions` | `ServiceUrl` (empty is the AWS default chain); `Region` (`us-east-1`); `Bucket` (required); `AccessKeyId`/`SecretAccessKey` (both set or both empty; never logged); `ForcePathStyle` (`false`); `PresignExpiry` (`15m`) |

The shared relational migrations (`Legate.Storage.Migrations`,
`MigrationOptions`) own the `legate` schema with a configurable table
prefix; versions are UTC `yyyymmddHHMM` (baseline `202609161200`) and grow
additively.

## `Legate:Workspace:Docker` (`DockerWorkspaceOptions`)

| Key | Default | Meaning |
|---|---|---|
| `Root` | required | Absolute host directory sessions bind under |
| `Image` | required | Image every session container starts from |
| `Network` | null | Docker network (null keeps the daemon default) |
| `CpuLimit` | unset | `--cpus` limit when set |
| `MemoryLimit` | null | `--memory` limit when set |
| `User` | null | `--user` when set; null runs as the image default |
| `DefaultExecTimeout` | unset | Exec timeout when the caller passes none; unset is unbounded |

Per-command environment rides on `IWorkspace.Exec`, never on these
options, and is never logged.
