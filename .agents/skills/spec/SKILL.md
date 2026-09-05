---
name: spec
description: Use when starting a milestone, when a task lacks acceptance criteria, or when what to build is clearer than how it will be checked.
---

# Spec

Written before implementation. Covers one milestone or one coherent piece.
Lives in `docs/internal/product/milestones/<milestone>/SPEC.md`.

## Contents

| Section | Must answer |
|---|---|
| **Requirements** | Which FR/NFR IDs does this serve? |
| **Scope** | What is being built — and explicitly, what is not |
| **Design** | The approach, and the alternatives rejected with reasons |
| **Cost impact** | Which request-rate rule (R1–R11) does this touch, and what is the budget? |
| **Acceptance criteria** | How will we know it works? |
| **Test plan** | Which tier, which suites, what must fail before the code exists |
| **Risks** | What could make this wrong, and what would reveal it |
| **Tasks** | The decomposition, each one commit's worth |

⚠️ **Cost impact is a required section in this project**, not a nicety. The
product exists to convert a network bill into an API bill
([`docs/research/00-problem/02-cost-model.md`](../../../docs/research/00-problem/02-cost-model.md)),
so a spec that does not say what it does to requests-per-MiB is incomplete. If
the answer is "nothing", say that.

## Acceptance criteria carry the weight

Each must be checkable by something other than an opinion — a test that passes,
a gate that goes green, a number inside a bound.

- ❌ "Segments are written efficiently"
- ✅ "A 100 MiB/s workload across 3 pods issues fewer than 0.3 PUT per MiB, asserted by `CountingBinStore`"
- ❌ "The consumer is cheap when idle"
- ✅ "1,600 idle shards issue **zero** object-store requests over 5 minutes"
- ❌ "Failover works"
- ✅ "Killing the sequencer mid-commit preserves invariants I1–I4 across 1,000 simulation seeds"

⚠️ If you cannot write a checkable criterion, you do not yet understand the
task well enough to implement it. That is the useful signal, not an obstacle.

## The test plan is written with the spec, not after it

⚠️ **A milestone spec without a test plan is not ready to implement.** The plan
names, per task:

- **Tier** for each behaviour (T0–T4) — and if anything above T0 is proposed,
  why the seam cannot be injected instead.
- **The test that must fail first**, by name. This is the list that
  `scripts/tdd-red.sh` will be run against, so it has to be concrete enough to
  write before the production code exists.
- **The mutation each test is meant to catch.** If you cannot say what wrong
  behaviour a test would detect, the test is coverage, not verification
  ([testing.md](../../../docs/internal/standards/testing.md) rules 8–10).
- **Coverage and mutation expectations**, and any exclusion with its reason.
- **Which suites this milestone extends**: store conformance, the commit-protocol
  simulation, the memory-flatness soak, the cost assertions.

## Decomposition

- **Only the current milestone**, in detail. Tasks written three milestones
  ahead are wrong by the time they are reached — not because the plan was bad,
  but because the intervening work changes what the right task is.
- **One task equals one commit** — one coherent change leaving the tree green.
- **Stable IDs**, never reused. Format `M<n>.<k>`.
- Each task cites the requirement it serves and lands a row in
  `docs/internal/product/backlog.md`.

## Read the research corpus first

⚠️ Most design questions in this project are **already answered with numbers**
in `docs/research/`. Use the [`research`](../research/SKILL.md) skill before
designing anything. A spec that re-derives a conclusion the corpus already
reached — or worse, contradicts one without saying so — is the most common
failure mode here.

## Get the spec reviewed before writing code

⚠️ **A wrong spec produces correct code solving the wrong problem, and every
downstream gate passes.** Spec review is the cheapest gate available and the
only one that catches this.

Review against the mission, the architecture, and the corpus: does it serve the
requirements it cites, does it contradict a recorded decision or a research
finding, are its criteria checkable.

## If it turns out to be wrong

Normal, and expected. **Stop and amend the spec.** Do not silently implement
something adjacent — that produces work passing every gate while serving no
requirement. If the correction changes an approach or a requirement, it is a
decision: write an [`adr`](../adr/SKILL.md).
