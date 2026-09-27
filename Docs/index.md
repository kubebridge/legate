# Legate

Legate is an open-source (Apache-2.0) **agent harness for .NET**: a durable
Akka.NET runtime for tool-using AI agents, written in F# with a C#-friendly
public surface.

A host registers Legate with `AddLegate(...)`, configures LLM providers,
tools (MCP servers, `AIFunction`s, built-ins), and storage, and gets
**sessions** it can prompt at any time, steer mid-turn, abort, resume after
a crash, and replay.

Two hosts are first-class:

- The **CLI harness** — a single process, no database server, tools on the
  user's working directory, approval prompts. Start with
  [Getting started: CLI harness](getting-started-cli.md).
- The **hosted multi-tenant service** — clustered, Postgres and S3,
  sandboxed workspaces, headless sessions with webhooks. Start with
  [Getting started: hosted service](getting-started-hosted.md).

Then read the [configuration reference](configuration.md), the
[architecture](architecture-overview.md), and the [API reference](api/index.md).
