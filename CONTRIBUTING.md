# Contributing to Legate

Thanks for considering a contribution. This document covers the mechanics;
the design and conventions live in `Docs/ARCHITECTURE.md` and
`Docs/agents/code-rules.md`.

## Contributor License Agreement

Contributions are accepted under the Individual Contributor License Agreement
(`CLA.md`) rather than a sign-off trailer. The CLA grants KubeBridge an
Apache-2.0-consistent copyright and patent license for your contributions so
Legate can stay open-source under the Apache License 2.0.

Accept it once by posting the following statement on your first pull request:

```
I have read the Legate Individual Contributor License Agreement (`CLA.md`)
and I agree to its terms for this and all future contributions I make to
Legate.
```

Maintainers verify the statement before merging; it then covers all of your
future contributions unless you withdraw in writing. No `Signed-off-by`
trailer is required on commits.

## Workflow

1. Open or pick an issue. Issues describe the *what* (summary, context,
   expected outcome); the implementation approach is agreed in a plan comment
   on the issue before code is written.
2. Branch from `main`. Never commit directly on `main`.
3. Build and validate locally (see `AGENTS.md` § Build & Validation):

   ```
   dotnet fsi build.fsx -- -t Test
   dotnet fsi build.fsx -- -t CheckFormat
   ```

4. Open a pull request against `main`. The last line of the PR body must be
   `Closes #<issue>`.
5. Pull requests are squash-merged once CI is green and review passes.

## Coding rules in brief

- F# throughout, formatted with the pinned Fantomas (`dotnet fsi build.fsx -- -t Format`).
- Warnings are errors, including incomplete pattern matches.
- The public surface stays C#-friendly: `Task`, `IReadOnlyList<T>`, nullable
  references, enums and sealed class hierarchies; no `option`, `list`, or
  discriminated unions on public signatures.
- New source files must be added to the owning `.fsproj` compile list in
  dependency order.
- Every public type gets an XML doc comment.

## Security

Please report vulnerabilities privately through GitHub's security advisory
form for this repository rather than a public issue.
