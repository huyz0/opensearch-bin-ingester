---
name: milestone
description: Use when told to work through a milestone, when a session should run until its completion condition is met, or when a milestone needs reviewing as a whole.
---

# Milestone

Autonomous mode. The loop runs until the milestone's completion condition in
`docs/internal/product/roadmap.md` is met, or until something genuinely needs a
human.

## The loop

```
until completion condition met:
    next-task            choose the top unblocked task, confirm it is ready
    tdd                  implement it test-first
    run the gates        every one that exists; report honestly which ran
    review               BOTH agents: `reviewer` on production, `test-reviewer` on tests
    resolve findings     fix, or argue in a staged baseline entry
    commit               one task, one commit, subject starts with the task ID
    update the backlog   in the same commit
```

## Bounds — what keeps the loop from thrashing

⚠️ **An unbounded loop is how autonomous work becomes slop.** Three hard limits:

| Bound | Value | On breach |
|---|---|---|
| Review rounds per task | **2** (review.md rule two-round-cap) | Stop and report. Round one finds, round two verifies; only a blocking finding may extend it |
| Red→green attempts per task | **3** | Stop and report. Three failures means the task or the spec is wrong, not the code |
| Tasks between checkpoints | **5** | Emit a progress report — tasks done, gates run, cost delta — and continue. **Do not wait for a reply**; this is a report, not a question |

The checkpoint is what makes a long run auditable without making it interactive.

## Finishing — enumerate the evidence, do not assert completion

⚠️ **A milestone is not done because the loop stopped.** Before declaring
completion:

1. Write `VERIFIED.md` beside the milestone's `SPEC.md`, with **one line per
   acceptance criterion**, naming the test or command that demonstrated it.
2. Run `scripts/check-milestone-verified.sh <milestone-dir>`. It fails on any
   criterion with no evidence line, or whose line names no test or command.
3. ⚠️ For anything not actually observed, write **`NOT-RUN`** or **`OBSERVED-NOT`**
   and say so in the report. **Never write an evidence line for something you did
   not run** — that is non-negotiable 4, and it is the rule every other one rests
   on.
4. Run the **milestone review** below over the whole diff.

The script cannot check that the evidence is *true*. It forces **enumeration**,
which is what catches the quiet omission of criterion 6.

## Stop and ask when

- The top three tasks are all blocked — that is a planning problem.
- A task depends on a 🔴 open question
  ([`docs/research/50-open-questions.md`](../../../docs/research/50-open-questions.md) Q1–Q5).
- A decision is needed that is expensive to reverse and no ADR covers it.
- The spec turns out to be wrong in a way that changes a requirement.
- A gate fails for a reason you would have to weaken the gate to fix.
  ⚠️ **Never move a threshold in the weakening direction to make a check pass.**

Otherwise **do not ask between tasks.** Asking after every commit is the failure
mode this skill exists to prevent.

## Report at the end

- Tasks completed, with IDs, one line each
- Gates that ran, and **gates that did not and why** — ⚠️ never imply a gate
  passed when it did not run. ⚠️ **`check-coverage.sh` EXISTS BUT IS NOT WIRED**
  into `.pre-commit-config.yaml` (M0.13), so a milestone that does not invoke it
  by hand ran without a coverage floor, and the report must say so every time
- Cost budget before and after, if the milestone touched the request path
- What is now blocked, and what the next milestone should start with

## Honesty rules that matter more here than anywhere

Autonomous mode has no human reading each diff, so the reporting *is* the
observability.

- **Never claim a test passes without having run it.** No script can check this.
- **Never widen scope silently.** Doing more than the task asked breaks the
  one-task-one-commit property just as much as doing less.
- **Never commit a tree you know is broken**, including "I will fix it in the
  next commit".

## Milestone review — reading the commits together

⚠️ Merged in from the separate `milestone-review` skill: it was only ever
invoked from step 4 above, so a session doing milestone work loaded two bodies
and every session paid for two layer-1 descriptions to reach one procedure.

Per-commit review sees one diff at a time and cannot see what only shows up
across a milestone: a concept that drifted, a duplication that accumulated, an
abstraction that stopped fitting, a budget that eroded a little per commit.

### Inputs

- `git log --oneline <milestone-start>..HEAD` and the full diff
- The milestone's `SPEC.md` and its completion condition in the roadmap
- Every `minor` finding recorded in a commit body during the milestone
- The cost meter's trend across the milestone

### What to look for

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

### Output

1. Findings, each with a concrete failure or erosion scenario.
2. **Backlog rows** for what should be fixed — in the next milestone, not
   retrofitted into this one.
3. A re-plan: does the roadmap's next milestone still start in the right place?
4. Update the research corpus if the milestone taught something the corpus
   should have said.
