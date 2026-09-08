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

M5.6a - rounds 1 and 2 each found real defects, at DIFFERENT seams: round 1 the unpinned negative arm (nothing asserted a CLOSE refusal is not a FencedException), round 2 CommitLog's SECOND fence site at :404 and the PendingCommit unwrap a BatchingSequencer caller depends on. Round 3 changes no production logic at all - two test pins and one corrected javadoc sentence - so splitting would divide the pins from the type they pin - approved-by: Huy Nguyen
