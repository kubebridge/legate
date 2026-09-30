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
`/model [provider[/model]]`, `/abort`, `/compact`, `/quit`. Unknown slash
commands reprint the usage.

## Providers and models

`anthropic`, `openai`, and `google` register only when `ANTHROPIC_API_KEY`,
`OPENAI_API_KEY`, or `GOOGLE_API_KEY` is set (keys flow from the
environment through `Legate:Llm:Providers:<id>:ApiKey` binding only, never
printed or persisted; `Legate__Llm__Providers__<id>__ApiKey` already set
wins). Anthropic rides the OpenAI-compatible preset under id `anthropic`.
With exactly one key set dot just works; with several, `--provider` picks,
else the default order `anthropic, openai, google` wins; `--model
<provider/model>` overrides the model (`--provider`/`--model` apply to
live mode; `--scripted` pins the scripted transport).

`/model` lists the registered providers with their defaults (marking the
current one) and `/model <provider[/model]>` switches mid-session through
`SetAgentAsync` against a model-carrying agent row: the transcript and
workspace binding survive the switch. Unknown providers and unparsable
references fail naming the known ids; a missing key names its env var.

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

Dot runs on scripted transports when no provider key is set (or under
`--scripted`): no keys, no network, a canned model for exercising the
loop, the slash commands, the permissions, and the tools. With a provider
key set dot runs live on the selected provider and model (see above).
