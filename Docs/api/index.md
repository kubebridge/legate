# API reference

The API reference is generated from the XML documentation emitted by the
Release build (every public member carries XML docs; the build treats
documentation warnings as errors). The pages below cover the packable
`src` assemblies:

- `Legate.Abstractions` — contracts: domain types, store, tool,
  workspace, and policy interfaces. Depends on nothing else in the
  solution.
- `Legate` — the runtime: actors, dispatcher, ReAct loop, built-in tools,
  LLM coordinator, hosted services, `AddLegate`.
- `Legate.Mcp` — `IToolSource` over the ModelContextProtocol SDK
  (`Legate:Tools:Mcp`).
- `Legate.Llm.OpenAI`, `Legate.Llm.Google` — provider packages.
- `Legate.Storage.*` — InMemory, FileSystem, Postgres, Sqlite, S3,
  Packages stores plus the shared Migrations assembly.
- `Legate.Workspace.*` — Process, HostDirectory, Docker runtimes.
- `Legate.Coordination.Redis` — distributed LLM admission.
- `Legate.Cluster.Kubernetes` — Akka.Management plus Kubernetes discovery
  bootstrap.

See the [configuration reference](../configuration.md) for the options class
behind each section path, and the [architecture](../architecture-overview.md) for the
package boundaries.
