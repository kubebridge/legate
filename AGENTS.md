# AGENTS.md - Legate

This file satisfies the metaskills harness contract (`AGENTS.template.md`).
The shared user-level skills (`issue-raise`, `issue-refine`, `issue-plan`,
`issue-implement`, `dev-cycle`, `code-review`, `burndown`, `backlog-refine`,
`qa`, `setup`, `weekly-review`) resolve every project fact from the sections
below at runtime. Single-workflow detail lives under `Docs/agents/`; a skill
acting on a section that names a detail file must read that file first.

Harness conventions this project accommodates: issue bodies are the What,
the single `<!-- plan-ledger -->` comment is the How; worktrees live in
`.worktrees/` and GitHub body staging files are `.tmp-*` (both gitignored);
`weekly-review` writes to `Docs/status/`; batch state lives only on the board
and in issue comments.

## Project Overview

Legate is an open-source (Apache 2.0) **agent harness for .NET**: a durable
Akka.NET runtime for tool-using AI agents, written in F# with a C#-friendly
public surface. A host registers it with `AddLegate(...)`, configures LLM
providers, tools (MCP servers, `AIFunction`s, built-ins), and storage, and
gets **sessions** it can prompt at any time, steer mid-turn, abort, resume
after a crash, and replay. Two hosts are first-class: a CLI harness (single
process, no database server, tools on the user's working directory, approval
prompts) and a hosted multi-tenant service (clustered, Postgres and S3,
sandboxed workspaces, headless sessions with webhooks).

Phase: pre-release scaffolding. The runtime is being extracted from BridgeMCP's
agent engine and generalised from a job model to a session model; nothing is
published to NuGet yet. `Docs/ARCHITECTURE.md` is the normative target.

## Code Layout & Tech Stack

- **Language/runtime**: F# on .NET 10 (`global.json` pins SDK `10.0.100`,
  `rollForward: latestFeature`). `Directory.Build.props` sets
  `TreatWarningsAsErrors=true` (incomplete pattern matches are build
  errors), `Nullable=enable`, deterministic builds, and NuGet metadata.
  Package versions are centralised in `Directory.Packages.props`; projects
  reference packages without a `Version` attribute.
- **Solution**: `Legate.slnx` at the repo root.
- **`src/Legate.Abstractions/`**: contracts (domain types, store, tool,
  workspace, and policy interfaces). Depends only on `FSharp.Core` and
  `Microsoft.Extensions.AI.Abstractions`; never on Akka.
- **`src/Legate/`**: the runtime (actors, dispatcher, ReAct loop, built-in
  tools, LLM coordinator, hosted services, `AddLegate`). References
  `Legate.Abstractions`. Grants `InternalsVisibleTo` to `Legate.Tests` only.
- **`tests/Legate.Tests/`**: xUnit + FsUnit.xUnit suite covering both
  projects. One `<Name>Tests.fs` module per source module.
- **Future packages** (`Legate.Llm.*`, `Legate.Storage.*`,
  `Legate.Workspace.*`, `Legate.Cluster.Kubernetes`, `Legate.Mcp`,
  `Legate.Testing`) go under `src/<PackageName>/` and are added to
  `Legate.slnx`; `samples/` holds F# host samples and is already in the
  Fantomas scope. See `Docs/ARCHITECTURE.md` § Package layout.
- **Enforced architecture**: references point inward (everything ->
  `Legate.Abstractions`; Abstractions references nothing in the solution).
  `Legate` must build and test with no Redis, Docker, Postgres, or
  Kubernetes. Public API rules (Task not Async, no `option`/`list`/DU on the
  boundary, XML docs on every public member) are in
  `Docs/agents/code-rules.md`.
- **New-file registration**: F# compile order is explicit. Every new source
  file must be added to the owning `.fsproj` `<Compile Include>` list in
  dependency order, and every new source file starts with
  `// SPDX-License-Identifier: Apache-2.0`.
- **Build script**: `build.fsx` (FAKE 5) with helpers in `.build/Helpers.fs`.
  The Fantomas scope list (`fantomasPaths`) lives there once; CI calls the
  targets rather than restating paths.
- **Migrations**: none yet. When `Legate.Storage.Postgres` lands, its
  FluentMigrator migrations own the `legate` schema with a configurable
  table prefix; list existing migrations before numbering a new one.

## Build & Validation

All commands run from the repo root. `dotnet tool restore` is run by the
targets that need it.

```bash
dotnet fsi build.fsx -- -t Restore      # tool restore + dotnet restore
dotnet fsi build.fsx -- -t Build        # Clean -> Restore -> Build (Debug)
dotnet fsi build.fsx -- -t Test         # Build -> dotnet test Legate.slnx (all test projects)
dotnet fsi build.fsx -- -t Format       # Fantomas 7.0.0 over src, tests, samples, build.fsx (writes)
dotnet fsi build.fsx -- -t CheckFormat  # same scope, non-mutating; exit 99 on drift (CI gate)
dotnet fsi build.fsx -- -t Pack         # Release NuGet packages into ./artifacts
dotnet build Legate.slnx                # plain build without the FAKE chain
dotnet test Legate.slnx                 # plain test run
```

- **Minimum pre-PR gate**: `dotnet fsi build.fsx -- -t Test` and
  `dotnet fsi build.fsx -- -t CheckFormat`, both exit 0. Cite the per-project
  `Passed!` summary line and the exit code; never claim green from the
  absence of the word "Failed".
- **Format F#**: only through the `Format`/`CheckFormat` targets. Do not
  invoke `fantomas` directly; the pinned version is in
  `.config/dotnet-tools.json`.
- **Public API changes**: any change to a public type in
  `src/Legate.Abstractions` or `src/Legate` must be called out in the PR
  Notes and checked against `Docs/agents/code-rules.md` § Public surface.
- **Hung test host**: a test run that prints `Starting: Legate.Tests` and then
  never finishes is almost always a stack overflow in the code under test
  (observed with a recursive `Equals`). Kill `testhost`, do not raise the
  timeout.
- **DB tripwire files**: none (no database layer yet). When
  `src/Legate.Storage.Postgres/**` or `src/Legate.Storage.Sqlite/**` exist,
  changes there require the relevant store tests to run against a real
  provider (SQLite locally; Postgres via Testcontainers in CI) before opening
  or merging a PR.
- **Commit message convention**: single line, lowercase, imperative mood,
  under 72 characters (`add session store contract`). Every commit carries a
  `Signed-off-by` trailer (`git commit -s`); CI rejects PRs without it (DCO).

## Project Board

- Board: https://github.com/orgs/kubebridge/projects/1
  (`orgs/kubebridge/projects/1`); owner `kubebridge`, project number `1`.
- Status options in lifecycle order:
  `Backlog -> Ready -> In progress -> In review -> Done`
  - `Backlog`: unrefined (route through `issue-refine`)
  - `Ready`: refined, queued (set by `issue-refine`)
  - `In progress`: plan ledger posted; this is the claim (set by `issue-plan`)
  - `In review`: PR open and linked (set by `issue-implement`)
  - `Done`: PR with `Closes #<n>` merged
- **New-issue status**: `Backlog`.
- **Done automation**: off. GitHub's built-in "Item closed" workflow is not
  enabled on this board, so `code-review` must move the closed issue to
  `Done` after merging. (Enable the workflow in the board's Workflows tab to
  change this; then update this line.)
- Look up field and option IDs live with `gh project field-list 1 --owner kubebridge`
  and `gh project item-list 1 --owner kubebridge`; never hardcode them. If a
  scope error appears, run `gh auth refresh --hostname github.com -s project`.

## Repositories

- App repo: `kubebridge/legate` (this repo; org is `kubebridge`, singular).
  Private until the first public release.
- Deployment repo: none (library; releases are NuGet packages).
- Issue template: `.github/ISSUE_TEMPLATE/issue.md` (sections `## Summary`,
  `## Context`, `## Expected outcome`, `## Notes`).
- PR template: `.github/pull_request_template.md`; the last body line is
  `Closes #<n>`.

## Environments

`none`. Legate is a library; there is no running instance to QA against.
`qa` and `burndown` closing sweeps are satisfied by the test suite and, once
they exist, the `samples/` hosts run locally (`dotnet run --project samples/<Name>`).
Default QA target: the test suite.

Releases: pushing a tag `vX.Y.Z[-suffix]` runs `.github/workflows/release.yml`,
which tests, packs with that version, and publishes to GitHub Packages
(`https://nuget.pkg.github.com/kubebridge/index.json`). Switch the source
and API key to nuget.org for the first public release. The release workflow
has not yet been exercised by a real tag.

## Branch Map

| Branch | Role | CI workflow |
|--------|------|-------------|
| `main` | working branch; PR base; protected by CI | `.github/workflows/ci.yml` (push to `main`, pull requests, manual) |
| `v*` tags | releases | `.github/workflows/release.yml` |

- `ci.yml` is the gate for pull requests and `main`: `Restore`,
  `CheckFormat`, `Test`, and `Pack` via `build.fsx` on Ubuntu and Windows,
  plus a `dco` job that fails a PR when any commit lacks `Signed-off-by`.
- Merging to `main` deploys nothing. Reviewer auto-merge is safe whenever CI
  is green; releases are an explicit tag push.
- Never implement or commit directly on `main` or the user's active working
  branch; `dev-cycle` branches are `issue-<number>-<short-slug>` based on
  `origin/main`. Squash-merge PRs and delete the branch afterwards.

## Agent Login

`none`. There is no authenticated instance. Samples that call LLM providers
read API keys from environment variables (`ANTHROPIC_API_KEY`,
`OPENAI_API_KEY`, `GOOGLE_API_KEY`) or the `Legate:Llm:Providers:*:ApiKey`
configuration; never commit keys, never print them in logs, issues, or PRs,
and never paste session transcripts that may contain tool output into issues.

## Review Notes

Full details: `Docs/agents/review-notes.md`, load before reviewing any PR,
branch, or diff. Code-style detail: `Docs/agents/code-rules.md`; boundaries:
`Docs/ARCHITECTURE.md`. Most critical:

- Public API surface: no `option`/`list`/DU/`Async` on public signatures; XML
  docs on every public member; API changes named in the PR.
- No BridgeMCP vocabulary (org, wallet, connector, marketplace, Virtual MCP,
  job) and no one-shot verbs (`RunOnce`) in Legate; headless runs are sessions
  with options.
- Fencing: every side effect on behalf of a turn checks the claim token at the
  last moment; a correlation id is evidence, not authority. Takeover races need
  a test proving zero effects from the loser.
- `Legate` must still build and test with no external services.
- Incomplete matches are build errors; reject a wildcard added only to silence
  one.
- GitHub Actions: never interpolate `${{ ... }}` into `run:` blocks; bind
  under `env:` and validate externally influenced values.

## Plan Comment Convention

The `issue-plan`, `dev-cycle`, and `burndown` skills track execution state in
a single GitHub issue comment, the plan ledger, whose first body line is
`<!-- plan-ledger -->`. One ledger per issue, edited in place. Find and update
it by marker with the REST listing (the GraphQL node id from `gh issue view`
is rejected by the REST PATCH):

```powershell
gh api repos/kubebridge/legate/issues/<number>/comments --jq '.[] | select(.body | startswith("<!-- plan-ledger -->")) | .id'
gh api -X PATCH repos/kubebridge/legate/issues/comments/<comment-id> -F body=@.tmp-plan-<number>.md
```

Write the body to a `.tmp-*` file as UTF-8 **without BOM** (a BOM breaks the
`startswith` marker check), then remove the file.

## Issue Claim Protocol

The board `Status` field doubles as the claim lock. `issue-plan` claims only
from `Ready` (`Ready -> In progress`, then re-read to confirm it took); never
steal an issue that is `In progress`, `In review`, or `Done`; `Backlog` items
go through `issue-refine` first. `issue-implement` proceeds only when the
issue is `In progress` with a plan ledger.
