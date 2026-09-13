# Review Notes: Legate

Detail file for `AGENTS.md § Review Notes`. Load this before reviewing any PR,
branch, or diff. Code-style detail lives in `Docs/agents/code-rules.md`;
architecture in `Docs/ARCHITECTURE.md`.

## Public API and packaging

- Any change to a public type in `Legate.Abstractions` or `Legate` is an API
  change: check it against the C#-friendly rules (no `option`/`list`/DU/`Async`
  on the boundary), confirm XML docs, and call it out in the PR notes.
- A new package must follow the layout in `Docs/ARCHITECTURE.md`, reference
  versions only through `Directory.Packages.props`, and stay `IsPackable`
  unless it is a test or sample.
- `Legate` must still build and pass its tests with no Redis, Docker, Postgres,
  or Kubernetes available. Reject a test that assumes any of them.
- No BridgeMCP-specific vocabulary (org, wallet, connector, marketplace,
  Virtual MCP, job) in Legate code, docs, or config keys. The host adapts;
  Legate does not know the host.
- No "Job" type and no one-shot verb (`RunOnce`, `Execute`) on the client:
  headless runs are sessions with options.

## Correctness risk areas (highest priority)

- **Fencing.** Every external side effect performed on behalf of a turn (store
  writes, tool calls, webhooks, workspace mutations) must check the turn's
  claim token at the last moment. A correlation id is evidence, not authority.
  Any claim of stale-attempt suppression needs a takeover race test proving
  zero effects from the loser.
- **Time, delay, and randomness seams.** Runtime code (`src/Legate`) never
  calls `Task.Delay`, `DateTime.UtcNow`, `Stopwatch`, or
  `Environment.TickCount` directly; it goes through the injected
  `TimeProvider` (the clock), `ILlmDelay` (waits), and `ILlmRandom` (jitter)
  seams. Reject a diff that reaches for the direct calls, including wrapper
  helpers that dodge the seams. The system-backed defaults live in
  `src/Legate/Seams.fs` and are registered by `AddLegate`.
- **Leases.** Renewal, expiry, and crash recovery must be exercised through
  the real timer bounds and the deterministic clock seams (`TimeProvider`,
  `ILlmDelay`, `ILlmRandom`) under virtual time, not by sleeping in tests.
- **Cancellation.** Inventory every awaited call inside a turn; each receives
  the turn token or is explicitly bounded. Caller-side `WaitAsync(token)` does
  not prove the provider request was cancelled.
- **Journal and replay.** Event append must be ordered and idempotent per
  sequence number; replay must rebuild the same conversation. A schema change
  to `SessionEvent` needs a versioned serializer and a replay test on old data.
- **Wire compatibility.** Anything crossing an Akka node boundary is versioned
  explicitly; renaming a message type without a manifest migration is a
  rolling-upgrade break.
- **Permissions.** A tool call reaches execution only after `IPermissionPolicy`
  returned `Allow`; `Ask` must suspend the turn, keep the lease alive, and
  resume exactly once on the matching `Reply`.
- **Path confinement.** `HostDirectory` workspaces reject any path that
  resolves outside the root, including through symlinks and `..`.
- **Secrets.** Provider keys, tokens, and tool arguments never appear in logs,
  events, or exceptions. Session transcripts may contain tool output; treat
  them as sensitive when writing docs and samples.
- **Tenancy.** Every store method takes a `TenantId` and every query filters by
  it; a host bug must not be able to read another tenant's sessions through
  Legate.

## F# specifics

- Incomplete matches are build errors here (`TreatWarningsAsErrors` with
  FS0025 on). A total match with no wildcard is the property to assert when a
  DU gains a case; reject a wildcard added just to silence the error.
- Struct types with `CustomEquality` must implement a same-named
  `Equals(T)` member; an `Equals(obj)` override that calls `this.Equals x`
  without it recurses forever (observed: a hung test host).
- New files are registered in the `.fsproj` compile list in dependency order.

## GitHub Actions

- Never interpolate `${{ ... }}` into `run:` blocks; bind under `env:` and use
  quoted shell variables. Validate externally influenced values (tags, branch
  names, titles) against a strict pattern before use.
- Workflows call `build.fsx` targets rather than restating paths or flags.
- `permissions:` is explicit and minimal on every workflow.
