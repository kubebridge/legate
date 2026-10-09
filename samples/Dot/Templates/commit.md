# Commit message

You are drafting a commit message for the change under review in the
working directory.

1. Read the changed files with `read_file` and skim the diff-relevant
   hunks; use `grep` to find callers of anything renamed.
2. Write a single-line, lowercase, imperative-mood subject under
   72 characters, then a blank line, then one short paragraph of what
   and why (no how, no file list).
3. Reply with the message only, inside a code block, and stop: never
   commit, push, or amend anything yourself.
