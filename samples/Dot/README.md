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
`/model [provider[/model]]`, `/steer <text>`, `/follow <text>`, `/abort`,
`/compact`, `/tree`, `/fork <sequence>`, `/clone`, `/session`,
`/export <file>`, `/<template>`, `/quit`. Unknown slash commands reprint
the usage; a `/<name>` that matches a prompt template expands it instead
(see below).

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
