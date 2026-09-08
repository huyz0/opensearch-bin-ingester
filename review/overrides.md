<!-- SPDX-License-Identifier: Apache-2.0 -->
# Review round overrides

A task appears here only when a review went past the cap in
`tools/xreview/xreview.py` and a person decided that was right. One line each:

```
<taskid> - <why this is genuinely one change> - approved-by: <name>
```

⚠️ **This file is the whole mechanism, and its value is that it is a diff.** The
harness this replaced offered `REVIEW_ROUND_BUDGET` and a `rounds:<task>` key in
`baselines/review.txt`; the archive holds 25 such escapes, 21 of them from M4,
and every one of them says "argued rather than split". The cap never once caused
a split, because nothing about taking the escape was visible to anyone but the
agent taking it. A line here is read by whoever reads the commit.

⚠️ **STANDING AUTHORITY, FOR M5 ONLY, GRANTED 2026-09-08.** Huy Nguyen
authorised these lines to be signed on his behalf for the remaining M5 tasks,
on three conditions, ALL of which must hold and each of which is stated in the
line itself: rounds one and two each found REAL defects, the remaining fix is
small, and round three changes NO production logic. A round three that touches
production logic goes back to him. ⚠️ This is a narrowing of the mechanism, not
a widening: the conditions are checkable against the recorded verdicts under
`review/verdicts/<task>/`, which are in the tree, so a reader can tell whether a
signature was earned. It expires with M5.

M5.6a - rounds 1 and 2 each found real defects, at DIFFERENT seams: round 1 the unpinned negative arm (nothing asserted a CLOSE refusal is not a FencedException), round 2 CommitLog's SECOND fence site at :404 and the PendingCommit unwrap a BatchingSequencer caller depends on. Round 3 changes no production logic at all - two test pins and one corrected javadoc sentence - so splitting would divide the pins from the type they pin - approved-by: Huy Nguyen

M5.6d - rounds one and two each found REAL defects at different seams: round one that `close()` was not terminal (an election in flight could install a term nothing would ever release, and a commit after close could elect a new one) and that `close()` had zero coverage; round two that `retire()`'s compare-and-set was pinned only while nothing was held, so a plain null check survived and would throw away a LIVE term. Round three changes NO production logic - two test pins, on the standing M5 authority above - approved-by: Huy Nguyen

M5.6 - FOUR rounds, and ⚠️ THIS ONE IS OUTSIDE THE STANDING M5 AUTHORITY AND WAS APPROVED SEPARATELY, because its third condition -- "round three changes NO production logic" -- is FALSE here: round three found a major that required a production fix. Stated so a reader is not misled by the standing header above. Rounds one and two reviewed a much larger class and were answered by SPLITTING it, which is what the cap asks for: `FencedException` became M5.6a (9c1a9b7) and the term-holding object became `Leadership` in M5.6d (e297534), each landing with its own defects found. Round three then reviewed the remainder and found that the constructor ACQUIRED THE LEASE before validating its other arguments -- one null argument would leave a term held and renewed by a pod whose only reference the failed constructor had just discarded, so no pod could lead and none could forward, for the life of the JVM -- plus a discarded fence and a false closure claim on the backlog row. Round four verifies those three fixes and two new tests. - approved-by: Huy Nguyen

