---
name: review
description: Review a staged change as an independent agent that did not write it. Use before every commit. Defines what the reviewer is given, what it is deliberately denied, and what to look for that the deterministic gates cannot see.
---

# Review

**Two passes, both mandatory, run by ONE agent that did not write the change:**

| Pass | Reviews | Its single question | Verdict recorded as |
|---|---|---|---|
| 1 | the production diff | *what is wrong with this change?* | `reviewer` |
| 2 | the tests | *would this test fail if the code were wrong?* | `test-reviewer` |

⚠️ **The test pass is not optional and not a sub-heading of the production pass.**
Test weakness is invisible to every gate — coverage counts executed lines, not
constrained ones — so it is a pass whose only job is to find a mutation that
survives, asked after the production pass and recorded on its own.

⚠️ **One agent, not two, since M0.114.** Two agents each re-read the same diff,
packet and standards, which was the largest single cost of a round. What the
separation actually buys — two questions, two findings lists, two verdicts bound
to the same hash — is kept. What it loses is real and stated in review.md rules
1c and 1d: an opinion formed in pass 1 travels into pass 2, and — the larger
loss — there is no longer a second independent read of the PRODUCTION diff.
This repository has the receipts for that one: M5.48's fifth round was the first
test-reviewer pass on the task and found two majors four production rounds had
missed. `tools/xreview/` therefore stays two agents.

⚠️ **This skill is run by agents that did not author the change.** A
self-review inherits every blind spot that produced the defect — the same
misreading of the task, the same assumption about what a method guarantees, plus
a commitment bias toward work already done.

## Running it

```
scripts/review.sh context --task <ID>                 the packet: task, standards, gates, diff
scripts/review.sh record --file v.json --task <ID> --role <role>   store one verdict
scripts/check-reviewed.sh                             the gate
```

⚠️ **`review.sh` does not spawn the reviewer**, because no script can do that in
a way that works across tools. It owns the hash, the packet, the schema, and the
artifact; the agent owns the judgement.

## What the reviewer receives

- The task, verbatim from the backlog, with its acceptance criteria
- The staged diff
- The relevant standards, selected from the staged paths by
  `scripts/which-standards.sh` — **not chosen by the author**
- The list of deterministic gates that already passed

## What the reviewer must NOT receive

- The author's transcript, plan, or reasoning
- The author's description of what the change does
- Any justification the author produced

⚠️ **This exclusion is the point.** An author's rationale is persuasive by
construction — it was generated to make the change look correct — and a reviewer
given it grades the rationale instead of the code. Reconstruct intent from the
task and the diff alone, because *task says X, diff does Y* is precisely the
defect self-review cannot see.

## Do not re-check what the gates already checked

Formatting, compilation, test pass/fail, file size, and link integrity are
already decided by scripts. Attention spent there is attention not spent on what
only a reader can catch.

## What to look for

**Conformance:**
- Does the change do what the task specified — no more, and no less?
- Was scope silently widened? Something out of scope is a backlog task.
- Do the acceptance criteria actually hold, and is each one checkable?

**Cost — the project-specific lens, and the one most worth the attention:**
- Does this add an object-store request per *record*, per *shard*, or per
  *partition*? Any of those is a defect — request rates may scale with segments
  and with nodes, never with records, shards, partitions, or indices
  (rules R1, R5, R6, R10).
- Does it introduce a `LIST` anywhere but recovery or GC (R2)?
- Does it make an idle path issue any request at all (R3)?
- Does it split one coalesced read into many (R4)?

**Tests:**
- Do they assert **behaviour**, or the implementation back at itself?
- Would any of them fail if the logic were subtly wrong? If not, they are
  coverage theatre.
- Is there a test for the failure path, not only the happy one?

**Design:**
- Is this the simplest thing that works, or invented structure?
- Does it duplicate something that already exists?
- Does it contradict a research finding or an ADR without saying so?

**Correctness of claims:**
- Are the comments true?
- Does the error message let an operator act?
- Does a claimed invariant (I1–I4) actually hold under the failure it names?

**Memory and streaming:**
- Is anything materialised whole that should be streamed (constraint C8)?
- Is every queue and buffer pool bounded?

## What the test pass looks for

Its full brief is in the reviewer agent's own definition. ⚠️ No claim is made
here about how much of the old separate brief survived the merge — round 2
falsified the first such claim, and a completeness claim about a list that moves
is the defect this repository has paid for repeatedly. Read the brief. The core
of it:

- **Mentally mutate the production code** — flip a boundary, negate a condition,
  return a constant, drop a side effect, make a method a no-op. **If a test still
  passes, name that mutation.** That finding outweighs every style comment.
- Does every acceptance criterion have a test that would fail without the change?
- Is there a failure-path test, not only a happy path?
- Was an existing assertion **weakened** in the same commit as a production change?
  That is the "production changed to satisfy the test" inversion — blocking unless
  justified in the commit body.
- A test needing a socket, clock or object store where T0 would do is a **design**
  finding about production layering, not a test finding.

⚠️ **A weak-test finding without a surviving mutation named is an opinion.**

## Output

Structured findings, each with `file:line`, a severity, and **a concrete failure
scenario**. ⚠️ A finding that cannot say how it fails is a style opinion, and
style is the formatter's job.

Return the verdict as JSON to `scripts/review.sh record --file <path> --task
<ID>`, which validates it and writes `.harness/review/<staged-diff-sha256>.json`.
The gate recomputes that hash, so amending one byte after review invalidates it
— which is what makes the review a fact rather than a claim.

⚠️ An empty findings list is a valid and expected outcome; invented findings are
worse than none.

## Resolution

Each blocking finding is **fixed** (the diff changes, the hash changes, review
re-runs) or **argued** (a staged entry in `baselines/review.txt` naming the
finding's id and why it is not a defect). ⚠️ The entry must be **staged** — an
unstaged one suppresses a finding while leaving no trace of it.

On a `pass` verdict a `minor` finding is recorded in the commit body and the
commit lands. ⚠️ **Opening a round for a minor is forbidden** (review.md rule
11): measured across four M5 tasks, every round past the second was opened for a
finding the gate does not block on, and most of those rounds found a new defect
in the fix. ⚠️ **And there is no fix-it-before-the-verdict-is-recorded
escape**, which an earlier draft of this paragraph offered: neither counter can
see it — `review_rounds.py` counts distinct hashes among RECORDED verdicts, and
xreview counts verdict files — so review, fix, re-review, never record leaves
the count where it was. A minor goes to the commit body, or to a backlog row
when it names work.

⚠️ **Three rounds is the cap** (review.md rule 12): round one finds, round two
fixes and finds in the fix, round three verifies. A fourth means the commit is
too big.

⚠️ A growing argued-list is itself a signal: somebody is being systematically
overruled, and one side is systematically wrong.
