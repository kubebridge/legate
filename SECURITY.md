# Security Policy

## Reporting a vulnerability

**Do not open a public issue for a suspected vulnerability.** Please report
it privately through
[GitHub's security advisory form](https://github.com/kubebridge/legate/security/advisories/new)
for this repository, consistent with the guidance in `CONTRIBUTING.md`.

Include in your report:

- a description of the vulnerability and its potential impact;
- steps to reproduce or a proof of concept;
- the commit or version affected, and your environment (OS, .NET SDK).

You can expect an acknowledgement within a reasonable time and updates as the
report is triaged. Please give the maintainers a chance to address the issue
before any public disclosure, and coordinate the disclosure timeline with
them.

## Scope

Legate is a library, not a running service: there is no hosted instance to
probe. In-scope reports cover the Legate packages themselves — for example,
authentication/authorization bypasses in session admission, sandbox or path
escapes in workspace runtimes, secret leakage into logs or events, and
dependency vulnerabilities with a reachable path in Legate's code.

Out of scope: LLM provider APIs and billing, vulnerabilities in forks or
third-party hosts built on Legate, and social-engineering or physical attacks.

## Supported versions

Before the first stable release, security fixes land on `main` (and the
latest pre-release package, if any); earlier commits are not patched. Once
stable versions are published, the patch window will be documented here.

## Disclosure

Once a fix is available, the maintainers publish a GitHub security advisory
describing the issue, the affected range, and the fixed version, and credit
the reporter unless they prefer to remain anonymous.
