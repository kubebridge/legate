# Contributing to Legate

Thanks for considering a contribution. This document covers the mechanics;
the design and conventions live in `Docs/ARCHITECTURE.md` and
`Docs/agents/code-rules.md`.

## Contributor License Agreement

**The revised agreement in `CLA.md` is a draft and is not open for signing.**
The Steward is KUBEBRIDGE TECHNOLOGIES INC., incorporated in British Columbia,
Canada. Legal review must be completed before a final version is published
and activated. This draft does not cancel earlier agreements or convert earlier
signatures into acceptance of revised terms.
No `Signed-off-by` trailer is required on commits.

The proposed policy preserves contributor ownership and documents copyright
and patent permissions for Apache-2.0 distribution. It grants no additional
relicensing permission beyond Apache-2.0. Read the actual final agreement
before accepting it; this summary is not a substitute for its terms.

### Signing after activation

Use the configured CLA Assistant link on your pull request only after checking
that it displays the approved **Legate Individual Contributor License
Agreement**, the correct legal Steward, and the final version. If the page
instead names SAP as the recipient of the contribution rights, or otherwise
differs from the approved agreement, stop and notify the maintainers on the
pull request. The service provider's branding is distinct from the agreement's
named parties.

As a maintainer-verified fallback, post this statement with both placeholders
replaced by the published final version and its immutable reference:

```text
I have read the Legate Individual Contributor License Agreement, version
<final-version>, at <immutable-agreement-URL>, and agree to its terms for
the Contribution in this pull request and my subsequent Contributions
covered by that agreement.
```

Maintainers record the contributor's identity, acceptance date, pull request,
agreement version, and immutable text reference. A substantive revision
requires fresh acceptance. Withdrawal from coverage of future Contributions
does not revoke licenses already granted.

### Maintainer activation and CLA Assistant configuration

1. Confirm the Steward identification in `CLA.md`, obtain legal review, and
   replace the draft designation with an approved final version.
2. Publish the approved text at an immutable Git commit URL. Put the same
   agreement text in a maintainer-controlled GitHub Gist for the hosted
   CLA Assistant service, and record its Gist revision alongside the repository
   version. Adding `CLA.md` to the repository alone does not configure the
   hosted service.
3. In CLA Assistant, inspect the effective repository and organization
   configuration for `kubebridge/legate` and link the approved Gist. Verify
   the document shown through the actual pull-request signing link, including
   its parties and version, before asking anyone to accept it.
4. Preserve existing acceptance records and the exact documents they refer to.
   Do not import signatures against a different agreement as acceptance of the
   new one. Obtain fresh acceptance and verify the resulting pull-request check;
   do not assume changing a Gist automatically resolves every existing PR.
5. For signatures against an unintended agreement, retain the relevant text,
   version, and PR reference, and contact the service operator to clarify or
   correct the record. Changing configuration or deleting a record does not
   itself rescind a legal agreement.
6. Once the approved agreement and signing flow are verified, update this draft
   status notice and the README to describe the active policy.

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
