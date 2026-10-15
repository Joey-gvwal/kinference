# AGENTS.md

## Code comments and documentation

Comments in this repo are kept short on purpose. Target an overall Rust comment
ratio under 15%, with no module above ~25%.

### Write a comment only when it says something the code can't

Keep (and keep tight) comments that explain *why* something non-obvious is
done:

- Invariants and contracts a caller must respect.
- Concurrency and locking rules, lifecycle and leak bounds.
- Protocol quirks and wire formats
- Performance-sensitive choices and workarounds for upstream bugs.
- Units and ranges when the type doesn't make them clear.

When in doubt, leave the comment out.

### Don't write

- **History.** No "originally", "pre-refactor", "used to", "we changed X to
  Y", "now that...", bug stories or references to removed code. That goes in
  commit messages and PR descriptions.
- **Restatements.** No comment that repeats what the next line or the
  signature already says.
- **Essays.** No multi-paragraph docs arguing for a design on small types or
  functions. Don't explain why something lives in one module instead of
  another.
- **Cross-reference lists to keep in sync** ("used in three places, keep
  these in sync..."). Name the callers in one sentence if it matters.
- **Notes about the current task or change** ("added for the new X",
  "fix for the issue above", "TODO from review").

### Style

- Module docs (`//!`): one or two sentences of purpose, plus real invariants.
- Item docs (`///`): a brief contract. One line is usually enough; fields
  usually need nothing.
- Inline (`//`): only for the non-obvious why.
- Test docs: state what the test checks in one line, not why it was written.
- Plain sentences. No ad hoc bold/caps emphasis.

### Keep comments correct

When changing code, update or delete the comments that describe it in the
same change: renamed or removed items, moved logic, changed defaults. Never
leave a doc comment attached to the wrong item, and re-read edited comments
so they still parse as sentences.

## Commits and pull requests

PRs are merged with a merge commit, not squashed, so each commit lands on
`main` as written and the PR description becomes the merge commit message.

### Commits

- One logical change per commit: the feature or fix, its tests, docs and
  unrelated small fixes go in separate commits when they make sense on their
  own.
- Every commit builds and passes tests, so `git bisect` works.
- The message says what the commit changes and why, for that commit only.
  Subject line in the imperative, under ~72 characters; wrap the body at 72.
- Address review feedback with `git commit --fixup <sha>` and fold it in with
  `git rebase -i --autosquash` before merging. No "address review" or "fix
  typo" commits on the final branch.

### PR descriptions

The description is the merge commit message, so write it for `git log`
readers, not just reviewers:

- Title: a short summary of the whole change.
- Body: the overall why and what, and anything that spans commits. Don't
  repeat each commit's message.
- No checklists, review chatter or links that only make sense on GitHub.
