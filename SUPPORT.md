# Support

How to get help with Legate, and what to expect.

Legate is a pre-1.0 open-source library (Apache 2.0). There is **no running
instance, no hosted service, and no SLA** — support is best-effort by the
maintainers and the community.

## Where to ask

- **Bugs and feature requests:** open an issue on the
  [issue tracker](https://github.com/kubebridge/legate/issues). Please check
  for an existing issue first and include a minimal reproduction where
  possible.
- **Questions and discussion:** open an issue with your question; there is no
  separate forum or chat channel yet.
- **Security vulnerabilities:** do **not** open a public issue. See
  `SECURITY.md` for the private reporting channel.

## Scope

Maintainers support:

- the documented public API of the latest state of `main` and, once they
  exist, the latest published NuGet release and its supported predecessors;
- build, test, and sample-host problems reproducible with the instructions in
  `AGENTS.md` and the sample READMEs.

Out of scope:

- hosting, operating, or monitoring your deployment of Legate;
- API keys, billing, or quota for LLM providers (see your provider's support);
- custom forks or unpublished patches (reproduce on unmodified `main` first).

## Supported versions

Before the first stable release, only the latest commit on `main` and the
latest pre-release package (if any) are supported. Once stable versions are
published, the support window will be documented here. See `SECURITY.md` for
the security-fix policy.

## Contributing

Bug reports with reproductions and pull requests are the most effective way
to get an issue fixed. See `CONTRIBUTING.md` (including the Individual CLA in
`CLA.md`) before contributing.
