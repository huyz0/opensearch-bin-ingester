# Git

**Family:** Process
**Read when:** Before committing, when unsure whether a change is one commit or several, or before amending anything already pushed.

1. **The commit subject starts with the backlog task ID**, then a summary in the
   imperative. → `scripts/check-commit-msg.sh`
2. **One task, one commit, tree green.** Split anything that cannot meet that.
3. **Never commit a tree you know is broken**, including "I will fix it in the
   next commit". No gate.
4. **The commit body says what changed and why**, and records any `minor` review
   finding that was accepted rather than fixed.
5. **When the change touches the object-store request path, the body states
   requests-per-MiB before and after.** No gate; `cost.md` explains why.
6. **Never write a `Reviewed-by:` trailer.** ⚠️ It was never a rule here — an
   agent invented it and repeated it on nine commits, where it read
   `Reviewed-by: reviewer, test-reviewer` every time. No script reads it, no
   standard required it, and until `review-roles.sh` landed it could only ever
   restate what `check-reviewed.sh` already enforced unconditionally. A field
   that never varies carries no information.
   ⚠️ **And it is self-asserted, so it can be false.** One commit was drafted
   carrying the full trailer while the production reviewer had returned
   `changes-requested` and never recorded a verdict at all — a message
   laundering an unreviewed commit as reviewed, which is the defect
   non-negotiable 4 exists to forbid. The gate is the record; a hand-typed copy
   of it adds nothing but a surface to lie on.
   ⚠️ **A SKIPPED review is different and must be stated**, because there the
   commit body is the only trace: name the roles that did not run and why.
7. **Never push unless asked.**
8. **Never amend or rebase anything already pushed.**
9. **Never widen scope silently.** Doing more than the task asked breaks the
   one-task-one-commit property as much as doing less.
10. **Generated index regions are committed with the change that moved them.**
   → `scripts/build-index.sh --check`
