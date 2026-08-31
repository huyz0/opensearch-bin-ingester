# Review

**Family:** Process
**Read when:** Writing a review prompt, deciding whether a finding blocks a commit, or wondering why the reviewer was not given the author's reasoning.

1. **Every commit is reviewed by two agents that did not write it** — `reviewer`
   for production, `test-reviewer` for the tests. Both are mandatory.
   → `scripts/check-reviewed.sh`
1a. **The test review is a separate pass with a separate question:** *would this
   test fail if the code were wrong?* Test weakness is invisible to coverage,
   which counts executed lines rather than constrained ones, so it needs an agent
   whose only job is to find a surviving mutation.
1b. **A weak-test finding must name the mutation that would survive.** Without
   one it is a style opinion.
2. **The reviewer is given the task, the diff, the selected standards, and the
   list of gates that passed — and nothing else.** No transcript, no plan, no
   author rationale.
3. **Standards are selected from the staged paths**, by
   `scripts/which-standards.sh`, not chosen by the author.
3a. **Both verdicts are required.** `check-reviewed.sh` refuses a commit missing
   either the `reviewer` or the `test-reviewer` artifact. A production-only review
   is not a review, because test weakness is invisible to every other gate.
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
    Fixing it is permitted and usually wrong: the next round's surface is the
    prose the fix just added.
12. **Two rounds is the cap.** Round one finds, round two verifies.
    ⚠️ **A blocking finding in round two does not buy a round three — it means
    the commit is too big.** Split it and review the pieces. Measured on M-1.1:
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
