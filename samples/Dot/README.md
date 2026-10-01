# Dot

Dot is the single-process Legate coding-agent sample: a REPL over durable
SQLite sessions that drives an agent against your working directory, plus
one-shot print (`-p`) and JSON event-stream (`--mode json`) modes that make
it scriptable like `pi -p` and `pi --mode json`.

## Install and run

Prerequisites: the .NET 10 SDK pinned in `global.json` (`10.0.100`,
`rollForward: latestFeature`). No database server, no Docker, no keys for
the scripted path.

Plain `dotnet run` from the repo root (in-repo build, no install step):

```bash
dotnet run --project samples/Dot/Dot.fsproj -- --scripted -p "hello"
dotnet run --project samples/Dot/Dot.fsproj -- --help
```

Build first through the repo chain when iterating with the suite:

```bash
dotnet fsi build.fsx -- -t Build
dotnet run --project samples/Dot/Dot.fsproj --no-build -- --scripted
```

Dot is a sample host, never a packed library: `Dot.fsproj` sets
`IsPackable=false` and it ships no NuGet package. It is registered in
`Legate.slnx` under `/samples/` with an explicit `<Compile Include>` order
and SPDX headers, inside the Fantomas `samples/` scope.

## Sessions

Sessions live in one SQLite file, so transcripts survive process exit for
`/sessions` (newest-first list), `--sessions`/`--list` (same list without
the REPL), and `/resume` / `--resume` (attach by id or, in the REPL, open
index, including across processes). The file resolves to
`%APPDATA%/dot/dot.db` on Windows and `$XDG_CONFIG_HOME/dot/dot.db` (else
`~/.config/dot/dot.db`) on Unix; set `DOT_DB_PATH` to override it (the
smoke runs use a temp file per invocation). Only one dot process may open
the file: a second process exits with a locked error naming the path.

REPL commands: `/new [title]`, `/sessions`, `/resume <id-or-index>`,
`/model [provider[/model]]`, `/steer <text>`, `/follow <text>`, `/abort`,
`/compact`, `/tree`, `/fork <sequence>`, `/clone`, `/session`,
`/export <file>`, `/<template>`, `/quit`. Unknown slash commands reprint
the usage; a `/<name>` that matches a prompt template expands it instead
(see below).

CLI flags: `--provider <id>`, `--model <provider/model>`,
`--resume <session-id>`, `--sessions`/`--list`, `-p`/`--print <query>`,
`--mode text|json`, `--scripted`, `--wait-minutes <n>`, `--ask`,
`--mcp <path>`, `--print-config`, `--help`/`-h`. Missing values fail naming the flag;
unknown flags fail naming the flag; `--mode` accepts only `text`/`json`;
`-p` needs a non-empty query; `--wait-minutes` needs a positive number.

## One-shot print

`dot -p "<query>"` (or `--print`) runs one Headless-shaped turn with the
Dot agent instead of the REPL: it opens (or `--resume` attaches) one
SQLite-backed session, runs the single prompt through `PromptAndWaitAsync`
under `--wait-minutes`, prints only the final answer to stdout, and exits
with the Headless map (0 completed, 2 aborted, 1 failed, 3 anything else).
`SESSION`/`RESULT` diagnostics go to stderr so stdout stays pipe-clean:

```bash
dotnet run --project samples/Dot/Dot.fsproj -- --scripted -p "smoke"
```

`--sessions` still lists when combined; otherwise `-p` takes the one-shot
path and the REPL never starts. `--resume <session-id>` attaches instead
of opening (unknown ids fail instead of silently opening); `--provider`,
`--model`, `--wait-minutes`, `--ask`, and `--mcp` apply as in the REPL.
Usage errors exit 2 naming the flag; runtime failures exit 1.

## JSON event stream

`dot --mode json -p "<query>"` streams session events as JSONL on stdout
(one `$type`-polymorphic event per line, the `DotExport` options, so the
pipe and `/export` share one wire format) for scripting:

```bash
dotnet run --project samples/Dot/Dot.fsproj -- --scripted --mode json -p "smoke" | python -c "import sys,json; [json.loads(l) for l in sys.stdin if l.strip()]"
```

Every human diagnostic (`SESSION`, `RESULT`, `TEXT`, errors) goes to
stderr in this mode; stdout carries nothing but JSONL. The stream replays
`Subscribe`-from-cursor (the pre-turn cursor), so a `--resume` rerun
streams only the new turn's events. Without `-p`, `--mode` is ignored and
the REPL runs in text. Quiet the framework logs for pure pipes with
`Logging__LogLevel__Default=None` (the smoke tests already do).

## Steering, follow-ups, and branches

While a turn runs the loop stays foreground with one turn in flight, so
the next line steers it: `/steer <text>` interrupts (`Interrupt`) and
starts the new turn, settling the pre-empted turn visibly as `Aborted`;
`/follow <text>` folds in (`Inject`) at the next iteration boundary
without interrupting; plain input queues (`Queue`) behind the running
turn. The settle waiter is queued before each prompt lands, so the
pre-empted turn still prints its `RESULT Aborted` line, never silently.
`/abort` and `/compact` work mid-turn and while idle, including the
deferred-compact path for running turns.

`/tree` lists the journal positions to branch from (sequence plus
event-type detail) and `/fork <sequence>` opens a new session carrying
that prefix through `ForkAsync`: the fork is registered and made current
while the source row and journal stay untouched. Beyond-tail cursors clamp
to the full journal and a cursor below the first sequence forks an empty
transcript. `/clone` duplicates the active branch (a tail fork) when that
falls out for free. No `Alt+Enter` key handling; console commands are the
interface.

## Context files

At session open dot resolves `AGENTS.md` in every directory from the
filesystem root down to the working directory (root first) plus the
nearest `SYSTEM.md` upward from the working directory, and passes them as
the session's host instruction files. Dot's default prompt is empty
(agents carry an empty system prompt), so a present `SYSTEM.md` occupies
the lead host-file slot as the project prompt and every `AGENTS.md`
appends after it; absent files simply contribute nothing. The runtime
re-reads every listed file on every turn, so an edit between turns steers
the next turn. No per-turn injection extensions and no custom compaction
models; auto-compaction rides the Legate defaults.

## Skills

Dot uploads a sample `review` skill (`.agent/skills/review/SKILL.md`,
read-only built-ins only) into each ensured agent's package and serves the
`skill` tool through its own tool source, so the model loads the skill on
demand with progressive disclosure. Unknown names return the runtime's
available-skills error listing the packaged names. Successful loads log
the skill name through the host logger. Skill loads are
log-only on the host: dot has no journal seam, so no `SkillLoadedEvent`
is journaled from dot (content, discovery block, companion staging, and
diagnosis all work; a runtime journal seam is future work).

## Prompt templates

`/<name>` expands `<working-directory>/.agent/templates/<name>.md`
verbatim as the next prompt: idle starts a turn, a running turn queues
behind it like plain input. There is no parameter-substitution syntax;
the file text becomes the user message unchanged. Unknown names error
listing the available template names (known commands win over templates).
One sample ships under `samples/Dot/Templates/commit.md`; copy it into
`.agent/templates/` to use it.

## Session info

`/session` reports message counts (accepted prompts plus folded
follow-ups), completed turns, and the token sums: `SESSION <id>
messages=<n> turns=<n> input-tokens=<n> output-tokens=<n>`. The counts page
the journal once; the tokens add this process's settled-turn usage on top
of the journal's `UsageEvent` sums (the runtime journals no usage on this
path, so without settled turns the sums read zero). Legate reports tokens
but never prices them, so `/session` shows tokens only. Per-call tool
counts are not reported: the runtime journals no per-call tool events for
these turns.

## Transcript export

`/export <file>` pages the journal to the file under the working
directory: JSONL (one `$type`-polymorphic event per line) by default, or
escaped static HTML for an `.html` target. Paths resolve under the
working directory with escape refused, mirroring the coding-tools fence;
overwrite mirrors `write_file`. There is no `/share` gist upload.

## Providers and models

`anthropic`, `openai`, `google`, and `ollamacloud` register only when
`ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, `GOOGLE_API_KEY`, or `OLLAMA_API_KEY`
is set (keys flow from the
environment through `Legate:Llm:Providers:<id>:ApiKey` binding only, never
printed or persisted; `Legate__Llm__Providers__<id>__ApiKey` already set
wins). Anthropic rides the OpenAI-compatible preset under id `anthropic`;
Ollama Cloud rides the OpenAI-compatible preset under id `ollamacloud`
(endpoint `https://ollama.com/v1`, default model `llama3.1`).
With exactly one key set dot just works; with several, `--provider` picks,
else the default order `anthropic, openai, google, ollamacloud` wins; `--model
<provider/model>` overrides the model (`--provider`/`--model` apply to
live mode; `--scripted` pins the scripted transport).

`/model` lists the registered providers with their defaults (marking the
current one) and `/model <provider[/model]>` switches mid-session through
`SetAgentAsync` against a model-carrying agent row: the transcript and
workspace binding survive the switch. Unknown providers and unparsable
references fail naming the known ids; a missing key names its env var.

## Configuration files

Dot reads two optional YAML scopes that share one schema: the user scope
(`%APPDATA%/dot/appsettings.yaml` on Windows,
`$XDG_CONFIG_HOME/dot/appsettings.yaml` else `~/.config/dot/appsettings.yaml`
on Unix, or `DOT_CONFIG_HOME/dot/appsettings.yaml` when set) and the
project scope (`./.dot/appsettings.yaml` under the working directory).
Each scope has an optional gitignored sibling with the same schema that
overrides its committed counterpart: `appsettings.local.yaml` next to
each `appsettings.yaml`. Missing files are never errors; malformed YAML
fails startup naming the path.

Effective precedence, strongest first: CLI flags, environment variables,
project `appsettings.local.yaml`, project `appsettings.yaml`, user
`appsettings.local.yaml`, user `appsettings.yaml`, built-in safe defaults
(no provider pick, no model override, the working directory as workspace
root, the per-user `dot.db`, allow-all tools).

Schema (both scopes, same keys):

```yaml
Dot:
  Provider: anthropic        # default provider; --provider wins
  Model: anthropic/claude-opus-4-6  # default model; --model wins
  WorkspaceRoot: /path/to/work      # default: the working directory
  DbPath: /path/to/dot.db           # default: the per-user dot.db; DOT_DB_PATH wins
  Ask: false                         # default permission policy; --ask forces true
  Providers:
    anthropic:
      ApiKey: sk-ant-...            # or ${ANTHROPIC_API_KEY}
    ollamacloud:
      ApiKey: ollama-...            # or ${OLLAMA_API_KEY}
Legate:
  Llm:
    Providers:
      openai:
        ApiKey: sk-...              # same binding live registration reads
      ollamacloud:
        ApiKey: ollama-...          # endpoint https://ollama.com/v1, default llama3.1
```

The `Legate` subtree flows unaltered into the runtime's `UseConfiguration`
section, so every Legate option rides the files too. Nested maps flatten
to `:`-joined keys and lists index by position. `${NAME}` placeholders in
any string value expand from the process environment (`${VAR:-default}`,
`$VAR`, and `~/tilde` never expand); an unset or empty variable fails
startup naming the config key and the variable, and values never appear in
error text.

`--print-config` prints the effective `Dot` plus `Legate` configuration,
one sorted `key=value` line per entry on stdout, and exits 0 without
opening the database or the network. Keys carrying `ApiKey`, `Secret`,
`Token`, or `Password` print as `***`:

```bash
dotnet run --project samples/Dot/Dot.fsproj -- --print-config
```

Secrets posture: API keys may live inline in either YAML file or come
from the environment (`ANTHROPIC_API_KEY`, `OPENAI_API_KEY`,
`GOOGLE_API_KEY`, `OLLAMA_API_KEY`, or the `Legate__Llm__Providers__<id>__ApiKey` /
`Dot__*` bindings). Inline files are convenient and work offline; the
environment keeps secrets out of the filesystem. Either way, set file
permissions you trust and never paste the dump (even redacted) next to
real keys. The repo gitignores `appsettings.local.yaml`; add the same
pattern to your own project's `.gitignore` so local overrides are never
committed.

## Environment keys

| Key | Effect |
|-----|--------|
| `DOT_DB_PATH` | Overrides the SQLite file (per-run temp files in smoke). |
| `DOT_CONFIG_HOME` | Overrides the user config base dir (`<dir>/dot/appsettings.yaml`). |
| `DOT_WORKSPACE_ROOT` | Overrides the workspace root (else `Dot:WorkspaceRoot`, else cwd). |
| `Dot__<Section>__<Key>` | Sets any `Dot:` file value from the environment (env beats files). |
| `ANTHROPIC_API_KEY` | Enables the `anthropic` provider (OpenAI-compatible preset). |
| `OPENAI_API_KEY` | Enables the `openai` provider. |
| `GOOGLE_API_KEY` | Enables the `google` provider. |
| `OLLAMA_API_KEY` | Enables the `ollamacloud` provider (`https://ollama.com/v1`, default `llama3.1`). |
| `Legate__Llm__Providers__<id>__ApiKey` | Direct binding already-set wins over the plain key. |
| `XDG_CONFIG_HOME` | Unix config base when `DOT_DB_PATH` is unset. |
| `Logging__LogLevel__Default` | Set `None` for pure `--mode json` pipes. |

Keys flow from the environment through configuration binding only, are
never printed or persisted, and never appear in logs, issues, or PRs.

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
an allow-for-session grant lasts for that session only. `-p` one-shots do
not answer approvals inline: a suspended turn exits 1 naming the request
(answer in the REPL or rerun without `--ask`).

The workspace root fence stays on in every policy mode: paths that escape
the working directory are rejected, and writes under `input/` are refused.

## MCP servers

Pass `--mcp <path>` to attach extra tools from a Claude-style `mcp.json`
file (the `LegateCli` precedent: stdio and streamable-HTTP servers). A
missing path fails startup naming the path. Attached servers stop with
the process.

## Pi-to-Legate map

| pi | dot (Legate) |
|----|---------------|
| `pi -p "query"` print mode | `dot -p "query"` / `--print`: same Headless shape (`PromptAndWaitAsync`, exit map 0/2/1/3) with the Dot agent and workspace attached. |
| `pi --mode json` event stream | `dot --mode json -p "query"`: `Subscribe`-from-cursor JSONL on stdout (`$type` contract), diagnostics on stderr. |
| pi coding tools over cwd | Dot seven coding built-ins over the host-directory workspace (`CodingTools.fs`). |
| pi allow-all default | Dot allow-all default; `--ask` mirrors per-call approval. |
| pi container sandboxing | Same expectation: run unsandboxed dot in a container; the root fence is not a sandbox. |
| pi sessions/transcripts | SQLite-backed Legate sessions (`--sessions`, `--resume`, `/tree`, `/fork`). |
| pi skills/templates | Dot `review` skill package plus `/.agent/templates/<name>.md` expansion. |
| pi provider/model flags | `--provider`, `--model`, `/model` over the Legate LLM layer. |

## Deliberate cuts (each adoptable by a later issue)

- No RPC/SDK modes: dot is a single-process CLI sample; a daemon or
  client SDK would need a wire protocol and auth story first.
- No themes: output is stable plain text/JSONL for scripting; styling
  would break the pipe contract.
- No extension system: tools come from the seven coding built-ins, the
  sample skill, templates, and `--mcp`; a plugin loader is future work.
- No `/share` gist upload: export stays local (`/export` file); sharing
  needs an auth and redaction story first.
- No per-turn injection extensions and no custom compaction models:
  context files plus Legate defaults cover the sample; custom hooks would
  widen the host surface.
- No `Alt+Enter` key handling: console commands are the interface, so
  piped stdin steers deterministically.
- No pricing: Legate reports tokens only, so `/session` shows tokens and
  never prices.
- No per-call tool counts in `/session`: the runtime journals no per-call
  tool events for facade-driven turns, so the summary reports messages,
  turns, and tokens only.
- No `SkillLoadedEvent` journal seam from dot: skill loads are host-log
  only; a runtime journal seam is future work.
- No parameter substitution in templates: file text becomes the message
  unchanged, keeping expansion predictable.

## Modes

Dot runs on scripted transports when no provider key is set (or under
`--scripted`): no keys, no network, a canned model for exercising the
loop, the slash commands, the permissions, the tools, `-p`, and
`--mode json`. With a provider key set dot runs live on the selected
provider and model (see above).
