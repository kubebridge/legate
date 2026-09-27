# Getting started: CLI harness

The `samples/LegateCli` sample is the reference CLI host: an interactive
REPL over the Legate session client facade (`SessionClient` plus
`SessionClientOperations`). It prompts sessions, streams the journaled
event lifecycle to the terminal, approves permission requests and answers
questions inline, and supports `/compact`, `/abort`, `/agent`, `/new`,
`/sessions`, `/resume`, and `--resume`.

## Run offline (no keys, no network)

```bash
dotnet run --project samples/LegateCli -- --scripted
```

`--scripted` runs fully offline on scripted transports and is the CI smoke
path. It exercises the whole loop: prompt, permission approval, an MCP
fixture tool call, `/compact`, `/abort`, `/agent`, `/new`, `/resume`, and a
second prompt.

## Run against a live provider

Keys come from the environment through configuration binding only; they are
never printed or persisted:

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

## Flags

| Flag | Meaning | Default |
|---|---|---|
| `--provider <anthropic\|openai\|google>` | Which live provider to register | `anthropic` |
| `--model <provider/model>` | Model reference in `provider/model` form | The provider default |
| `--mcp <path>` | Attach MCP servers from a Claude-style `mcp.json` | None |
| `--resume <session-id>` | Attach an in-process session at startup | None |
| `--scripted` | Offline scripted transports | Off |
| `--wait-minutes <n>` | Settle wait bound | `5` |

Quiet the framework logs with `Logging__LogLevel__Default=None`; the event
stream and results own stdout either way.

## Attach an MCP server (`mcp.json`)

```bash
dotnet build samples/McpFixture/McpFixture.fsproj
export LEGATE_MCP_FIXTURE_DLL=$PWD/samples/McpFixture/bin/Debug/net10.0/McpFixture.dll
dotnet run --project samples/LegateCli -- --scripted --mcp samples/LegateCli/mcp.json
```

`${VAR}` placeholders in `mcp.json` expand from the process environment.
Any Claude-style file with `command`/`args` stdio entries (or `url`
streamable-HTTP entries) attaches the same way.

## Scope notes

- Storage is InMemory only: `--resume` and `/resume` work within the
  process lifetime. A fresh process starts with an empty store, so
  `--resume <id>` from an earlier run reports `RESUME-FAILED` and opens a
  new session. File-backed resume arrives with the SQLite package.
- The workspace root is the process working directory (per-session scratch
  binds under it).
- `/agent <name>` is stubbed until agent switching lands; the CLI surface
  follows the client API table in the [architecture](architecture-overview.md)
  (`/sessions` renders the `ListSessionsAsync` page with id, title, and
  state), plus the `WaitForSettleAsync` sugar an interactive host needs.
- Tool calls come from MCP servers only in this sample: every call asks for
  console approval (allow once, allow for session, deny).
- The journaled event stream carries the suspension lifecycle
  (permission/question asked and resolved), compaction, and failure events.
  Per-token deltas are not journaled on the suspendable path, so the REPL
  renders lifecycle events plus the settled result text rather than a token
  stream.
