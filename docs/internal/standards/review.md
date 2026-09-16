# Review

**Family:** Process
**Read when:** Writing a review prompt, deciding whether a finding blocks a commit, or wondering why the reviewer was not given the author's reasoning.

1. **Every commit is reviewed by an agent that did not write it, in TWO passes
   with two recorded verdicts** — `reviewer` for production, `test-reviewer` for
   the tests. Both verdicts are mandatory.
   → `scripts/check-reviewed.sh`
1a. **The test review is a separate PASS with a separate question:** *would this
   test fail if the code were wrong?* Test weakness is invisible to coverage,
   which counts executed lines rather than constrained ones, so it needs a pass
   whose only job is to find a surviving mutation, asked after the production
   pass and answered on its own.
1b. **A weak-test finding must name the mutation that would survive.** Without
   one it is a style opinion.
1c. ⚠️ **One agent runs both passes since M0.114.** They were two agents,
   and what that bought was independence between the passes; what it cost was
   reading the same diff, the same packet and the same standards twice, which was
   the largest single cost of a round. The two artifacts and the two questions
   are what carry the property — a test weakness answered inside a production
   review is a sub-heading nobody reads — and those are kept. ⚠️ **What is
   genuinely lost is stated rather than waved at:** one agent forming an opinion
   in pass 1 carries it into pass 2, so a test that merely confirms the
   production reading is now likelier. The counter-pressure is that pass 2 must
   name a surviving mutation it actually ran, which an opinion cannot produce —
   and that counter-pressure is rung 7, enforced by nothing.
1d. ⚠️ **The merge also costs a second independent read of the PRODUCTION diff**,
   which is a different loss, and it is recorded because this repository has the
   receipts: `review/overrides.md` holds M5.48's fifth round as the FIRST
   `test-reviewer` pass on that task, finding two majors four production rounds
   had missed, and M5.65's two roles finding one unpinned claim independently.
   ⚠️ **`tools/xreview/` stays two agents** for exactly that reason — merging
   both paths would remove the second production read everywhere at once, and the
   external path is where a disputed verdict is settled.
2. **The reviewer is given the task, the diff, the selected standards, and the
   list of gates that passed — and nothing else.** No transcript, no plan, no
   author rationale.
3. **Standards are selected from the staged paths**, by
   `scripts/which-standards.sh`, not chosen by the author.
3a. **Both verdicts are required.** `check-reviewed.sh` refuses a commit missing
   either the `reviewer` or the `test-reviewer` artifact — whether one agent
   produced both or two did. A production-only review is not a review, because
   test weakness is invisible to every other gate.
4. **The verdict is bound to the staged diff by hash.** Amending one byte after
   review invalidates it. That is what makes the review a fact rather than a claim.
5. ⚠️ **The hash proves the verdict matches the diff. It does not prove the
   reviewer was not the author** — that is bought by the harness, and saying so
   is the same discipline as non-negotiable 3.
6. **Every finding names a concrete failure scenario.** A finding that cannot say
   how it fails is a style opinion, and style is the formatter's job.
7. **The reviewer does not re-check what a gate already checked.**
8. **An empty findings list is a valid outcome.** Invented findings are worse
   than none.
9. **A blocking finding is fixed or argued**, and an argued entry in
   `baselines/review.txt` must be **staged** — an unstaged one suppresses a
   finding while leaving no trace.
10. **On `changes-requested`, `major` findings block too.** Otherwise a reviewer
    asks for changes, the commit lands anyway, and the findings live only in a
    gitignored directory.
11. **On `pass`, a `minor` is recorded in the commit body and the commit lands.**
    ⚠️ **Spending a round on a `minor` is forbidden, not merely discouraged**
    (M0.114). Measured across M5.56, M5.65, M5.69 and M5.60: every round past the
    second was opened for a finding this gate does not block on, and the majority
    of those rounds found a NEW defect in the fix — a stale count, a false
    attestation, a claim about the entry's own history. A minor outlives the
    commit in one of two places and neither is another round: the commit body, or
    a backlog row when it names work. ⚠️ **And there is no "fix it before the verdict is
    recorded" escape**, which an earlier draft of this rule offered: neither
    counter can see it — `scripts/review_rounds.py` counts distinct hashes among
    RECORDED verdicts, and xreview counts written verdict files — so review, fix,
    re-review, never record would leave the count where it was. That is the
    "nothing counted" failure this gate exists to end, re-introduced by rule. It
    would also rebind the verdict to bytes the reviewer never read, with the
    reviewer as their author.
12. **Three rounds is the cap** (M0.114, raised from two). Round one finds,
    round two fixes and finds in the fix, round three verifies.
    ⚠️ **A blocking finding in round three does not buy a round four — it means
    the commit is too big.** Split it and review the pieces.
    ⚠️ **Raising the cap is not weakening the gate**, which non-negotiable 2
    would forbid: the gate never blocked on a `minor`, and every round it did
    block was spent anyway under a signed override — M5.56 reached EIGHTEEN. The
    third round is the shape those overrides record over and over, bought once
    instead of signed per task. ⚠️ **It is a cap, not a budget:** rule 11
    forbids spending a round on a minor, so a task that needs all three has found
    two rounds of blocking or major defects and is a task worth splitting next
    time. Measured on M-1.1:
    a 129-file commit (a research corpus, 24 scripts, 12 standards and 21 ADRs
    under one task ID) took **five rounds and ~2.5 hours of review** to converge,
    and every extra round found defects in the previous round's *fix* rather than
    in the original change. One task, one commit (non-negotiable 1) is what keeps
    review affordable, not just tidy.
12a. **What the reviewer is asked to look at is generated, not improvised.**
    → `scripts/review-lenses.sh` selects lenses from the changed paths, the way
    `which-standards.sh` selects standards. A hand-written brief per commit is
    how a reviewer once spent its whole budget fuzzing 20,295 files of an
    unrelated upstream checkout.
12b. ⚠️ **Never edit the tree while a review is running.** The verdict is bound to
    the staged bytes by hash, so an edit throws the review away. Three were lost
    that way in one session. Batch the changes, then review once.
13. **Minors are harvested at the milestone boundary**, not left in commit
    bodies nobody greps.
