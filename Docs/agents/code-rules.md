# Code Rules: Legate (F#)

Implementation rules complementing `Docs/ARCHITECTURE.md`. Load both before
writing or reviewing code.

## Structure

- Prefer top-level `module Legate.<Area>.<Name>` over `namespace` + nested
  module. Use `namespace Legate.<Area>` only when a file declares several
  public types (contracts in `Legate.Abstractions` usually do).
- Compile order is explicit in each project's `.fsproj` `<Compile Include>`
  list. Every new file is registered there, in dependency order; the build
  fails on an unregistered file only if something references it, so check the
  list in review.
- Fantomas 7.0.0 is pinned in `.config/dotnet-tools.json`. Run
  `dotnet fsi build.fsx -- -t Format` before committing and
  `dotnet fsi build.fsx -- -t CheckFormat` to check without writing. The
  scope list (`fantomasPaths` in `build.fsx`) is the only place paths live; CI
  calls the same target. Do not invoke `fantomas` directly.
- Section separators inside long files: `// ──────────────────` comment blocks.
- Every source file starts with `// SPDX-License-Identifier: Apache-2.0`.

## Public surface (C#-friendly)

Legate is F# end to end and is consumed by C# hosts without a wrapper package,
so the public boundary follows these rules; internal code stays idiomatic F#.

- Public methods return `Task`, `Task<T>`, or `IAsyncEnumerable<T>`, never
  `Async<'T>`. Use `task { }` internally.
- No F# `option`, `list`, `Map`, `Set`, or discriminated unions on public
  signatures. Use nullable references (`Nullable` is enabled), `IReadOnlyList<T>`,
  `IReadOnlyDictionary<K,V>`, enums for flat choices, and sealed class
  hierarchies for tagged shapes. Map at the boundary.
- Records hosts construct carry `[<CLIMutable>]` or explicit constructors so
  object initialisers and `System.Text.Json` work.
- Options bound through `IOptions<T>` are plain classes with mutable
  properties and defaults.
- Interfaces meant for hosts to implement stay small and use only BCL types.
- Everything outside the contract is `internal`; `InternalsVisibleTo` is
  granted to `Legate.Tests` and `Legate.Testing` only.
- Every public type and member has an XML doc comment (the build emits
  documentation files and warnings are errors).
- Do not expose `FSharp.Core` types from `Legate.Abstractions`.

## Types & Naming

- Domain types: F# records (internal) or sealed classes (public).
- Identifiers are value types wrapping a string or ULID (`TenantId`,
  `SessionId`, `TurnId`); they implement `IEquatable<T>` explicitly and via a
  same-named member so `Equals(obj)` never recurses.
- Interfaces are I-prefixed (`ISessionStore`, `IToolSource`,
  `IPermissionPolicy`).
- Never inline fully-qualified namespaces in type signatures; `open` the
  namespace or add a type alias.

## Error handling

- Expected failures are `Result<'T, LegateError>` internally, using
  `result { }` and `taskResult { }` from FsToolkit.ErrorHandling once it is
  added; never raw strings as errors.
- Public APIs surface expected failures as typed exceptions derived from
  `LegateException`, or as a result object when the host must branch on the
  outcome (`TurnResult`). Do not leak `Result` on the public surface.
- The throw family lives in `src/Legate.Abstractions/Exceptions.fs`:
  `SessionNotFoundException`, `InvalidSessionStateException`,
  `AdmissionRejectedException`, `ProviderException`, `DeadlineExceededException`,
  `WorkspaceException`, `ToolException`, `AgentNotFoundException`,
  `AgentDisabledException`, and `InvalidBlobKeyException`. Each carries
  structured context (ids, provider id, HTTP status, retry-after, offending blob
  key) on properties; hosts never
  parse exception messages, and messages never embed secrets or tool
  arguments.
- Throw-versus-result rule for client methods: control-plane precondition
  failures throw. Unknown session ids throw `SessionNotFoundException`
  (`OpenSession` with an unknown agent, `ResumeSession`, `GetSession`,
  `SetAgent` on a missing session), a session in a disallowed state throws
  `InvalidSessionStateException` (prompting, replying, aborting, compacting,
  or rebinding a closed session; forking allows closed sources), admission
  rejection throws `AdmissionRejectedException`, provider failures outside a
  turn outcome throw `ProviderException`, bounded operations that outrun their
  deadline throw `DeadlineExceededException`, workspace binding or teardown
  failures throw `WorkspaceException`, and tool infrastructure failures (an
  MCP server that will not start, a tool source that fails to load) throw
  `ToolException`. Anything after a turn is accepted is a turn outcome
  returned through `TurnResult` (`Prompt`, `Reply`), whether the turn
  completes, aborts, suspends, exhausts budget, or fails inside the turn.
  `PromptAndWait`'s own wait deadline throws `DeadlineExceededException` even
  though the turn keeps running.
- Raw `task { }` without `Result` only for genuine fire-and-forget or
  fallback work.
- Guard clauses over nested `match`: `Result.requireSome`, `Result.requireNone`,
  `if cond then return! Error ...`.

## Akka.NET actors

- Use the `Akka.FSharp` API (`spawn`, `actorOf`, `actor { }`), never the C#
  `UntypedActor`/`Props.Create` surface, and never expose Akka types on the
  public API.
- Recursive `actor { let! msg = mailbox.Receive(); match msg ...; return! loop () }`.
- Messages are F# DUs in the owning area's `Messages.fs`.
- Never block the actor thread: `Async.StartAsTask |> ignore` or pipe results
  back as messages. `Ask` sparingly and always with a timeout.
- Every side effect that can outlive a turn is fenced by the turn's claim
  token; an in-memory flag is not a fence.
- Actor logic must be identical in single-node and clustered mode; only the
  router in front differs.

## Tests

- xUnit + FsUnit.xUnit in `tests/Legate.Tests`. One test module per source
  module, `<Name>Tests.fs`.
- Deterministic: the clock is BCL `TimeProvider` (use `FakeTimeProvider` from
  `Microsoft.Extensions.TimeProvider.Testing`), and waits and jitter go
  through the injected `ILlmDelay` and `ILlmRandom` seams. The dedicated
  fakes (`FakeLlmDelay`, `FakeLlmRandom`) come from `Legate.Testing` once
  that package exists; until then tests define small local fakes. Never
  sleep to exercise a timeout, backoff, or cooldown.
- Tests that touch a store run against the in-memory implementation and, for
  relational stores, SQLite; never assume Postgres or Docker on the machine.

## Other conventions

- Imports: System first, third-party next, project modules last.
- JSON: `System.Text.Json`, camelCase.
- Configuration is bound from the `Legate` section; env vars override with
  `Legate__Section__Key`.
- DI: stores and policies `AddSingleton` unless they hold per-request state;
  the actor system is a singleton hosted service.
- No `localhost` in host-side config examples; use `127.0.0.1`.
