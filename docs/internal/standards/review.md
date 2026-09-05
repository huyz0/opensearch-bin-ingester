# Review

⚠️ **Cite these by NAME, never by number.** The numbering is for reading order and
shifts when a rule is added — this file already carries 1a, 1b and 3a because
inserting one would have renumbered every citation in the tree. Each rule's name
is the slug in backticks after its statement, and `scripts/check-rule-citations.sh`
refuses a numeric citation and an unresolvable name alike.

**Family:** Process
**Read when:** Writing a review prompt, deciding whether a finding blocks a commit, or wondering why the reviewer was not given the author's reasoning.

1. **Every commit is reviewed by two agents that did not write it** (`two-reviewers`) — `reviewer`
   for production, `test-reviewer` for the tests. Both are mandatory.
   → `scripts/check-reviewed.sh`
1a. **The test review is a separate pass with a separate question:** (`test-review-separate`) *would this
   test fail if the code were wrong?* Test weakness is invisible to coverage,
   which counts executed lines rather than constrained ones, so it needs an agent
   whose only job is to find a surviving mutation.
1b. **A weak-test finding must name the mutation that would survive.** (`name-the-mutation`) Without
   one it is a style opinion.
2. **The reviewer is given the task, the diff, the selected standards, and the
   list of gates that passed — and nothing else.** (`reasoning-denied`) No transcript, no plan, no
   author rationale.
3. **Standards are selected from the staged paths** (`standards-from-paths`), by
   `scripts/which-standards.sh`, not chosen by the author.
3a. **Both verdicts are required.** (`both-verdicts`) `check-reviewed.sh` refuses a commit missing
   either the `reviewer` or the `test-reviewer` artifact. A production-only review
   is not a review, because test weakness is invisible to every other gate.
4. **The verdict is bound to the staged diff by hash.** (`hash-bound`) Amending one byte after
   review invalidates it. That is what makes the review a fact rather than a claim.
5. ⚠️ **The hash proves the verdict matches the diff. It does not prove the
   reviewer was not the author** (`hash-proves-what`) — that is bought by the harness, and saying so
   is the same discipline as non-negotiable 3.
6. **Every finding names a concrete failure scenario.** (`failure-scenario`) A finding that cannot say
   how it fails is a style opinion, and style is the formatter's job.
7. **The reviewer does not re-check what a gate already checked.** (`no-regating`)
8. **An empty findings list is a valid outcome.** (`empty-is-valid`) Invented findings are worse
   than none.
9. **A blocking finding is fixed or argued** (`fixed-or-argued`), and an argued entry in
   `baselines/review.txt` must be **staged** — an unstaged one suppresses a
   finding while leaving no trace.
10. **On `changes-requested`, `major` findings block too.** (`major-blocks`) Otherwise a reviewer
    asks for changes, the commit lands anyway, and the findings live only in a
    gitignored directory.
11. **On `pass`, a `minor` is recorded in the commit body and the commit lands.** (`minors-land`)
    Fixing it is permitted and usually wrong: the next round's surface is the
    prose the fix just added.
12. **Two rounds is the cap.** (`two-round-cap`) Round one finds, round two verifies.
    ⚠️ **And the cap is ARGUABLE, keyed `rounds:<task>` in a staged
    `baselines/review.txt`** — because the alternative is worse. Before that
    existed, an author whose commit genuinely could not be split had no route but
    `SKIP=check-reviewed`, and a SKIP disables the gate's *substantive* checks
    too: both roles present, and no unresolved blocking or major finding. So a
    missing escape did not make the cap bite harder, it made the gate ABSENT
    exactly when the cap bit. Measured on M4 with `git log --grep=SKIP=check-reviewed`:
    **at least nine** commits landed that way — the grep finds commits that
    DISCLOSED a skip, not commits that took one, and M4.12 bypassed a 12-round cap
    naming neither the skip nor the gate — M4.0, M4.1, M4.3, M4.3b, M4.4b, M4.6c,
    M4.6e, M4.19 and M4.20 — each naming the round cap as the only remaining
    refusal and recording the skip in its body because the body was the only trace
    available. ⚠️ **The first draft of this rule said "three"**, counted from
    the commits that numbered themselves rather than from the log — and only two
    of the nine carry an ordinal at all (M4.19 "the second time", M4.20 "the
    third"), so even that count was inferred rather than read. The number is load-bearing twice — as
    the evidence for the escape and as the baseline for the warning below — so it
    is measured here rather than remembered.
    An argued exception keeps every required role and no-unresolved-majors
    enforced and puts the reason in the tree. → `ReviewRoundCapTest`
    ⚠️ **What the gate actually requires, all three of which it enforces and only
    one of which an earlier draft of this rule stated:** the line is keyed
    `rounds:<task>` as its FIRST field; it carries a reason after the key; and it
    is **ADDED by this commit** in a **staged `baselines/review.txt`** — an
    argument sitting in the already-committed file does not carry a later round,
    and a `rounds:` line in some other staged file does not count at all. One
    added line then covers every further round of the same commit — ⚠️ except
    that "ADDED" means "git renders it as an added line", so an edit that
    re-renders an already-committed argument as added re-arms it. Measured, not
    yet fixed: M4.31.
    ⚠️ **Stated here even though this is NOT where the agent that needs it will
    look, and a draft of this rule claimed otherwise.** It said the requirements
    belong here "because the milestone loop reads this file" — it does not.
    `milestone/SKILL.md`'s bounds table mentions this standard only in a
    parenthetical, and its breach action is "Stop and report", so an agent at the
    cap stops without learning the escape exists. M4.33 is the row that fixes the
    skills and its premise is the correct one; this paragraph is the reference,
    not the delivery mechanism. ⚠️ The draft's version was worse than wrong: it
    would have let whoever picks up M4.33 read this rule and close the row as
    already covered.
    ⚠️ **A growing list of `rounds:` entries is a signal, not a workflow.** Rule
    12 exists because it was violated repeatedly in one session when nothing
    counted; an escape used routinely is that failure with paperwork.
    ⚠️ **How often, measured rather than remembered.** Grouping distinct
    `diff_sha256` by `task` across `.harness/review`: **at least 22 tasks** have
    gone over the cap, M4.12 reaching 12 rounds and M0.13 eleven.
    ⚠️ **NO DENOMINATOR HERE, and three drafts of this sentence are why.** The
    first quoted a total hash count; the second "22 of 78"; the third "22 of 79"
    and dated it. Every one was stale before the round that checked it finished
    — measured across this task's own reviews, the same denominator read 78, 79,
    80 and 81 within hours, because it counts reviewed TASKS and tasks keep being
    reviewed. Dating it is not enough at day granularity. The numerator is a
    floor and behaves like one, so the floor is what is quoted.
    ⚠️ **NOT a second reason 22 is a floor, and a draft of this paragraph got the
    mechanism wrong twice.** 90 of the files in `.harness/review` carry no `task`,
    but 89 are `record --file` INPUTS — hand- or agent-written before being fed
    to `review.sh record`, not written by it — in a different name shape that
    `rounds_for`'s `<64hex>.<role>.json` glob never reads at all. Of the files
    that shape actually matches, exactly one lacks a task — and its same-hash
    sibling names one, which `rounds_for` counts by task across every verdict for
    the hash. Measured: **zero rounds count against nothing.** M4.29 is a real
    gap (a verdict recorded with no `--task` at all, no sibling to rescue it, is
    a reachable future state) but it is not evidence that today's 22 undercounts.
    ⚠️ How badly the rule was broken the FIRST time, the tree contradicts itself
    about: `check-reviewed.sh`, backlog M0.47 (which calls it this repository's
    own record) and backlog M4.21 all say **8** of 12 tasks; `review_rounds.py`
    says 7, and said so before any of this. Neither is reconstructible, so the
    split is recorded rather than settled.
    ⚠️ **Three drafts of the commit that wrote this paragraph got that wrong, each
    inside the sentence disclosing the previous error.** The first quietly changed
    an 8 to a 7 — a measured number moved in the direction that weakens the case
    for the cap the same commit was loosening. The second restored one of the two
    sites and announced that "the higher figure is kept", which was false: it had
    been relocated, not reverted. The third is this one. The lesson is not about
    arithmetic — a correction is a claim about the diff, and it needs measuring
    against the diff exactly like any other claim. So does the draft that said
    "21 of 78 reviewed hashes", taken from a reviewer's prose rather than
    counted, wrong in both the number and the unit.
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
    the staged bytes by hash, so an edit throws the review away. It has happened
    repeatedly in single sessions — no running total is kept here. A draft carried
    one, backlog M0.21 carries another ("five times in one session"), the two
    disagreed, and a count that must be incremented by hand is a count that goes
    stale. One place claiming a number is enough. Batch the changes, then review once.
    ⚠️ **AND THE EDITOR NEED NOT BE THE AUTHOR.** One was lost when a
    SECOND agent session working in the same checkout staged its own task over
    this one's index mid-round; the reviewer found `git diff --cached` empty,
    correctly declined to bind its verdict to the empty diff, and the round was
    gone. Sessions sharing a worktree share one index AND one HEAD: two blobs of
    the same file built from different bases cannot both land, and the loser is
    reverted with no conflict marker to warn anyone.
    ⚠️ **NO REMEDY IS PRESCRIBED HERE, because the draft that prescribed one was
    wrong.** It offered a private `GIT_INDEX_FILE`, which does stop two sessions
    staging over each other but CANNOT carry the review step: `review.sh` and
    `check-reviewed.sh` both hash `git diff --cached`, so a reviewer not
    inheriting that index reviews different bytes. The commit that wrote the
    advice was its own counter-example. Sequencing the commits by hand is what
    actually worked. M0.21 owns the real fix.
13. **Minors are harvested at the milestone boundary** (`minors-at-milestone`), not left in commit
    bodies nobody greps.
14. **A change confined to the harness machinery needs NO reviewer verdict.** (`harness-exempt`)
    → `check-reviewed.sh`, `HarnessExemptionTest`
    ⚠️ **This LOOSENS the gate, deliberately, and the owner decided it after the
    cost was measured.** Reviewing the harness with the harness compounds: a
    verdict is bound to the exact staged bytes, so *any* edit voids it and every
    correction costs two more agent runs. Two harness tasks in one session took
    **ten** and **five** rounds. On the ten-round one, rounds 7–10 found nothing
    in the code at all — every finding was a false sentence in the backlog prose
    *describing the review*, which the author then corrected, which voided the
    verdicts, which bought another round. The author was generating the defect
    surface the next round consumed.
    ⚠️ **What replaces the reviewer is not nothing.** `check-tdd` still demands a
    red record for every new test, `check-test-integrity` still refuses a
    weakened assertion, and the suite must be green. For gate code these are the
    *stronger* signal anyway: a gate is verified by mutating it and watching a
    test fail, which is what the reviewers were doing by hand.
    ⚠️ **What it costs, stated rather than hidden.** The reviews on harness code
    are what caught two changes in one session that left a gate WEAKER than
    before it was touched — a `git mv` that walked past `check-test-integrity`,
    and a rename-following rule that opened a two-commit bypass of
    non-negotiable 3. No test then existing would have caught either. The bet is
    that the tests those rounds produced now do; if a harness weakening ships
    unnoticed, this rule is the first suspect.
    ⚠️ **Scope is COMPUTED from the staged paths, never claimed**, and fails
    closed twice: every path must be inside the allowlist, AND the diff must
    touch `scripts/` or `buildSrc/`. One product file, one build file, and the
    whole diff takes the ordinary path — so the exemption cannot be bought by
    bundling. A docs-only commit is NOT exempt; it is already reduced to one role
    by `review-roles.sh`, and exempting it would exempt most commits in the
    project. **Say plainly in the commit body that no reviewer saw the change.**
15. **One worktree per session.** (`worktree-per-session`) ⚠️ Rule 12's tail records what sharing one
    costs and declines to prescribe a remedy, because the draft that did
    prescribed a private `GIT_INDEX_FILE` — which stops two sessions staging
    over each other and then **cannot carry the commit**: `pre-commit` clears
    that variable, so every hook evaluates the SHARED index instead. Measured:
    the staged diff hashed `927b8a9a…` outside the hook and `63e3cb9d…` inside
    it, `check-reviewed` refused verdicts it had just been given, and a stale
    blob the shared index still referenced produced `fatal: unable to read
    15d0fc41…` four separate times in one session.
    ⚠️ **The remedy is a separate worktree**, `git worktree add`, or the agent
    tool's `isolation: "worktree"`. Each session then has its own working tree
    AND its own index against the same history, so nothing above can happen:
    no staged-file collisions, no parking another session's files around every
    commit, no `pre-commit` stashing work that belongs to someone else.
    → `scripts/session-worktree.sh`
    ⚠️ **And commit small.** One session in this repository ended with a
    complete, fourteen-round, fully-reviewed change existing nowhere but its
    working directory. A worktree isolates; only committing preserves.

## Reporting to a person

⚠️ Moved out of AGENTS.md by M0.71, which keeps the rule in layer 0 and
links here for what it cost to learn it.

- Never report in bare task IDs. `M0.20` names nothing a reader can hold:
  say **`M0.20 (check-cross-refs.sh — every M<n> and R<n> resolves)`**.
  A status line built out of IDs — "M0.20, M0.28 and M0.9 are queued" — forces
  the reader to open `backlog.md` to learn what is being discussed, and reads as
  progress without being checkable. The name is a few words saying what the task
  *is*. ⚠️ **No script can enforce this**, because it governs what is said rather
  than what is committed; it holds only as long as it is followed.
  ⚠️ And the ID must RESOLVE. The first draft of this bullet taught the rule
  using `M0.37`, an ID with no backlog row — invented in conversation, repeated
  for a whole session, and never written down. An unresolvable ID is the same
  defect one step worse: it names nothing AND there is nothing to look up.
  `M0.20` is the gate that would catch it in the tree; nothing catches it in
  speech.
