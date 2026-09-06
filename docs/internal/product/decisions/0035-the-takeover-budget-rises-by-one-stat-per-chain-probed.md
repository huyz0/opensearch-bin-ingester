# 0035. The takeover budget rises by one stat per chain probed

Status: accepted
Date: 2026-09-06
Requirements: FR-10, FR-11, NFR-3
Research: docs/research/30-design-space/03-metadata-and-cas.md

## Context

M4.9 bounds recovery by asking each chain, before walking past it, whether it
has a checkpoint. On a chain that HAS one the walk stops there and the whole
crossing becomes constant. On a chain that has none the probe answers "no",
costs one `stat`, and buys nothing.

`LocalSequencerFailoverTest`'s fixture is uncheckpointed by construction — it
uses the 4-arg `LocalSequencer.start`, whose shipped policy takes a thousand
deltas or sixty seconds, and commits far fewer — so every probe in it misses.
Its assertions are exact totals, so they move.

cost.md R18: moving a budget is an ADR with the number, the rejected
alternative, and what the new budget buys. This is that ADR. ⚠️ It is also the
promissory note M4.6e wrote: that commit relaxed these same four assertions and
recorded M4.9 as the reason it would be acceptable.

## Decision

**The uncheckpointed takeover budget rises by one `stat` per takeover plus one
per ancestor walked.** Measured, on the fixture that carries it:

| assertion | was | now |
|---|---|---|
| 12 entries, 1 prior term, total | 28 | 30 |
| 40 entries, 1 prior term, total | 56 | 58 |
| 4 entries, 1 prior term, total | 20 | 22 |
| 4 entries, 4 prior terms, total | 47 | 52 |

**GETs are unchanged** — 18 at 12 entries and 46 at 40. The probe is a `stat`,
and the entry-slope stays exactly 1.

**The SHAPE that test exists to pin is unchanged**: cost still grows per
ancestor and per entry on an uncheckpointed ancestry. Only the constant moved.

## What the new budget buys

The crossing on a CHECKPOINTED ancestry stops being transitive. Measured on the
same harness, before and after:

| prior terms | before | after |
|---|---|---|
| 1 | 29 | 17 |
| 16 | 284 | 17 |

GETs fall from 18 to 5 at one term and from 243 to 5 at sixteen; LISTs from
3/18 to 3/3; stats from 4/19 to 5/5.

⚠️ **THE BEFORE COLUMN WAS WRONG IN A FIRST DRAFT**, which quoted 26/…/281 with
"LISTs and stats each growing 3/4/6/10/18". Those numbers came from M4.9's
backlog row and describe an older tree; re-measured on `15da3fb` the series is
29/46/80/148/284, and the stat series is 4/5/7/11/19 — the quoted one was the
LIST series written twice, which is the signature of transcription rather than
measurement. The row itself records an earlier draft making this exact mistake
under the same word MEASURED. The correction makes the bound look BETTER, which
is why it needed checking rather than accepting.

That is the difference between O(all commit-log entries ever written) per
failover and a constant. Production checkpoints by default, so the budget that
rose is the one no deployment pays.

## Alternatives considered

**Skip the own-epoch probe.** Saves exactly one `stat` per takeover, which is
half of the rise at one prior term. Rejected: `CommitLog.recover()` on a chain
that has its OWN checkpoint goes back to unbounded. ⚠️ TWO DRAFTS OF THIS
REJECTION WERE WRONG ABOUT ITS EVIDENCE. The first said the variant "rests on an
assumption about callers" — `ownEpoch` is an internal field, so it does not. The
second cited
`recoveringAChainThatCheckpointedItsOWNProgressKeepsItsNextSequence` as what
fails, and measured, that test still PASSES under the variant, because a full
replay derives the same `nextSequence`. ⚠️ THE THIRD DRAFT THEN ENUMERATED WHICH
TESTS DO FAIL, which is the sentence form that went stale six times in this
milestone — an inventory of assertions elsewhere, falsified by the next commit
that moves one, and this ADR's own commit moved two. What is durable is the
property, not the census: the variant is caught by the bounded path's exact
request counts, and it is those counts being EXACT rather than comparisons that
catches it.

**Probe once, for the immediate predecessor only.** Rejected: an ancestry whose
newest term is too short to checkpoint but whose older terms are not would then
walk past checkpoints that exist, which is the case the bound is for.

**Leave the thresholds and exclude the probe from the count.** Rejected
outright — that is relaxing a budget by not measuring it, and cost.md R9 counts
requests through a decorator precisely so no request kind can be argued out of
the total. Twice already a mutation has hidden in the request kind nobody
listed.
