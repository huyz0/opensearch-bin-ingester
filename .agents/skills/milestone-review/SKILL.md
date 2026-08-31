---
name: milestone-review
description: Review a milestone's commits as a whole, turn what is found into backlog tasks, and re-plan. Use when a milestone reaches its completion condition or a checkpoint, and the commits need reading together rather than one at a time.
---

# Milestone review

Per-commit review sees one diff at a time and cannot see what only shows up
across a milestone: a concept that drifted, a duplication that accumulated, an
abstraction that stopped fitting, a budget that eroded a little per commit.

## Inputs

- `git log --oneline <milestone-start>..HEAD` and the full diff
- The milestone's `SPEC.md` and its completion condition in the roadmap
- Every `minor` finding recorded in a commit body during the milestone
- The cost meter's trend across the milestone

## What to look for

**Did it deliver what was specified?**
- Every acceptance criterion in the spec, observed — not assumed.
- Anything in scope that quietly did not happen.
- Anything delivered that was never in scope.

**Erosion the per-commit gate cannot see:**
- Requests-per-MiB trending upward commit by commit, each increment defensible.
- A concept implemented two ways in two commits.
- An abstraction introduced for one caller and never given a second.
- Tests that grew to assert implementation as the implementation settled.
- A research finding the milestone silently contradicted.

**Harvest the minors.** Every `minor` recorded in a commit body during the
milestone gets read here and either becomes a backlog row or is explicitly
dropped. ⚠️ This is the only place they are collected; without it they exist
only in commit bodies nobody greps.

## Output

1. Findings, each with a concrete failure or erosion scenario.
2. **Backlog rows** for what should be fixed — in the next milestone, not
   retrofitted into this one.
3. A re-plan: does the roadmap's next milestone still start in the right place?
4. Update the research corpus if the milestone taught something the corpus
   should have said.
