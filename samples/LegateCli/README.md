# LegateCli sample

Interactive CLI harness over the Legate session client facade (`SessionClient`
plus `SessionClientOperations`): a REPL that prompts sessions, streams the
journaled event lifecycle to the terminal, approves permission requests and
answers questions inline, and supports `/compact`, `/abort`, `/agent`,
`/new`, `/sessions`, `/resume`, and `--resume`.

## Run

```bash
dotnet run --project samples/LegateCli -- --scripted
```

`--scripted` runs fully offline on scripted transports: no provider keys, no
network. It exercises the whole loop (prompt, permission approval, MCP
fixture tool call, `/compact`, `/abort`, `/agent`, `/new`, `/resume`,
second prompt) and is the CI smoke path.

Real providers read keys from the environment through configuration
binding only; keys are never printed or persisted:

```bash
export ANTHROPIC_API_KEY=...
dotnet run --project samples/LegateCli -- --provider anthropic
```

```bash
export OPENAI_API_KEY=...
dotnet run --project samples/LegateCli -- --provider openai --model openai/gpt-4o-mini
```

```bash
export GOOGLE_API_KEY=...
dotnet run --project samples/LegateCli -- --provider google
```

Flags: `--provider <anthropic|openai|google>` (default `anthropic`),
`--model <provider/model>` (default: the provider default),
`--mcp <path>` (attach MCP servers from a Claude-style `mcp.json`),
`--resume <session-id>` (attach an in-process session at startup),
`--scripted` (offline scripted transports), `--wait-minutes <n>`
(settle wait bound, default 5).

Quiet the framework logs with `Logging__LogLevel__Default=None`; the event
stream and results own stdout either way.

## MCP attach (`mcp.json`)

`--mcp samples/LegateCli/mcp.json` attaches the scripted fixture server
(`samples/McpFixture`, one `echo` tool served over stdio). Point
`LEGATE_MCP_FIXTURE_DLL` at the built fixture first:

```bash
dotnet build samples/McpFixture/McpFixture.fsproj
export LEGATE_MCP_FIXTURE_DLL=$PWD/samples/McpFixture/bin/Debug/net10.0/McpFixture.dll
dotnet run --project samples/LegateCli -- --scripted --mcp samples/LegateCli/mcp.json
```

`${VAR}` placeholders in `mcp.json` expand from the process environment.
Any Claude-style file with `command`/`args` stdio entries (or `url`
streamable-HTTP entries) attaches the same way.

## Scope notes

- Storage is InMemory only (the merged `Legate.Storage.InMemory`
  package; no new storage package): `--resume` and `/resume` work within
  the process lifetime. A fresh process starts with an empty store, so
  `--resume <id>` from an earlier run reports `RESUME-FAILED` and opens a
  new session. File-backed resume arrives with the Wave 3 SQLite package.
- The workspace root is the process working directory (per-session
  scratch binds under it) until #113 (HostDirectory) lands.
- `/agent <name>` is stubbed: agent switching arrives with Wave 3
  (#122-124), which also owns Fork/SetAgent/ListSessions. The facade and
  the CLI surface exactly the ARCHITECTURE.md Client API table minus those
  ops, plus the `WaitForSettleAsync` sugar an interactive host needs
  beside Subscribe plus Reply.
- Tool calls come from MCP servers only in this sample: every call asks
  for console approval (allow once, allow for session, deny). Built-in
  file/exec tools over the workspace arrive in a later sample pass.
- The journaled event stream carries the suspension lifecycle
  (permission/question asked and resolved), compaction, and failure
  events. Per-token deltas are not journaled on the suspendable path, so
  the REPL renders lifecycle events plus the settled result text rather
  than a token stream.
- The on-demand compact arms the one-shot force flag while a turn runs
  (`COMPACT deferred`); the pass fires once the suspendable loop honors
  force-aware turn hooks (follow-up).
