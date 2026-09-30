# Dot

Dot is the single-process Legate coding-agent sample: a REPL over durable
SQLite sessions that drives an agent against your working directory.

## Sessions

Sessions live in one SQLite file, so transcripts survive process exit for
`/sessions` (newest-first list) and `/resume` (attach by id or open
index, including across processes). The file resolves to
`%APPDATA%/dot/dot.db` on Windows and `$XDG_CONFIG_HOME/dot/dot.db` (else
`~/.config/dot/dot.db`) on Unix; set `DOT_DB_PATH` to override it. Only
one dot process may open the file: a second process exits with a locked
error naming the path.

REPL commands: `/new [title]`, `/sessions`, `/resume <id-or-index>`,
`/abort`, `/compact`, `/quit`. Unknown slash commands reprint the usage.

## Workspace and tools

Dot binds the working directory through the host-directory workspace
runtime and offers the seven coding built-ins over it: `read_file`,
`write_file`, `list_files`, `edit_file`, `glob`, `grep`, and `exec`.
`edit_file`, `glob`, and `grep` ride the public Legate factories;
`read_file`, `write_file`, `list_files`, and `exec` are dot-local
functions over the public workspace primitives mirroring the runtime
tool contracts (see `CodingTools.fs`).

## Approval policy and sandboxing

The default policy allows every tool call with zero approval friction, as
pi does: the model acts, and sandboxing is the user's job. **Run dot in a
container** for anything you would not run by hand; `exec` runs host-shell
commands as your user with your environment.

`--ask` opts into per-call approval for runs outside a sandbox. Each tool
call suspends the turn with an inline prompt: allow once, allow for the
session, or deny. A deny runs nothing and the turn continues coherently;
an allow-for-session grant lasts for that session only.

The workspace root fence stays on in every policy mode: paths that escape
the working directory are rejected, and writes under `input/` are refused.

## MCP servers

Pass `--mcp <path>` to attach extra tools from a Claude-style `mcp.json`
file (the `LegateCli` precedent: stdio and streamable-HTTP servers). A
missing path fails startup naming the path. Attached servers stop with
the process.

## Modes

Dot runs on scripted transports (`--scripted`, the default): no keys, no
network, a canned model for exercising the loop, the slash commands, the
permissions, and the tools. Live-provider wiring belongs to a later
change; there are no live branches here.
