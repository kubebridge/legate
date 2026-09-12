# Contributing to Legate

Thanks for considering a contribution. This document covers the mechanics;
the design and conventions live in `Docs/ARCHITECTURE.md` and
`Docs/agents/code-rules.md`.

## Developer Certificate of Origin

Contributions are accepted under the [Developer Certificate of Origin](https://developercertificate.org/)
(DCO) rather than a CLA. Every commit must carry a `Signed-off-by` trailer
that matches the commit author:

```
git commit -s -m "add session store contract"
```

By signing off you certify that you wrote the change or otherwise have the
right to submit it under the Apache 2.0 license. CI rejects pull requests
whose commits are missing the trailer.

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
