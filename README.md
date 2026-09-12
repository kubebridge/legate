# Legate

> A durable Akka.NET runtime for tool-using AI agents.

Legate is an **agent harness** for .NET. You register it in a host (ASP.NET
Core, a generic host, or a CLI), point it at one or more LLM providers, hand it
tools (MCP servers, `AIFunction`s, built-ins), and it runs **sessions**:
long-lived conversations with an agent that you can prompt at any time, steer
while it is working, abort, resume after a crash, and replay.

The mental model is the backend of an agentic CLI such as OpenCode, Codex, or
Claude Code: a session, one primary verb (the user sends a message), a stream
of events back, and the ability to answer, steer, or abort while the agent
works. Two hosts are first-class:

- **A CLI harness**: a single process, no database server, tools acting on the
  user's working directory, tool calls that can require approval.
- **A hosted service**: many tenants, clustered nodes, Postgres and S3,
  sandboxed workspaces, headless sessions with webhooks.

Sessions are durable, lease-protected, and cluster-sharded when you want that,
and plain in-process when you do not. Every infrastructure concern (session
store, event journal, blob storage, workspace runtime, LLM admission,
permissions) sits behind an interface with a default implementation.

## Status

Pre-release. The public API described in `Docs/ARCHITECTURE.md` is being
built; nothing is published to NuGet yet.

## Building

Requires the .NET 10 SDK.

```
dotnet fsi build.fsx -- -t Build        # restore + build
dotnet fsi build.fsx -- -t Test         # build + run all tests
dotnet fsi build.fsx -- -t Format       # format F# with the pinned Fantomas
dotnet fsi build.fsx -- -t CheckFormat  # non-mutating format gate (CI)
dotnet fsi build.fsx -- -t Pack         # Release NuGet packages into ./artifacts
```

## Contributing

See `CONTRIBUTING.md`. Contributions are accepted under the DCO; sign your
commits with `git commit -s`.

## License

Apache License 2.0. See `LICENSE` and `NOTICE`.
