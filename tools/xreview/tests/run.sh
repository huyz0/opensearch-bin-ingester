#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# The external reviewer's own test suite.
#
# ⚠️ THIS IS THE BOOTSTRAP. `xreview` exists because the review loop it replaces
# could not be trusted to report on itself: on M0.56's final round both
# reviewers closed with "verdict recorded at this hash" and neither had run the
# recorder. So xreview is not vouched for by an agent. Every case below is a
# predicate over files and exit codes -- rung 3 of the gate-design ladder -- and
# each one is written to FAIL against a harness that does not have the property.
#
# ⚠️ NO CASE HERE INVOKES A MODEL. A test that shells out to `claude -p` would
# be measuring the model's mood, not the runner's contract. The reviewer is
# stubbed through XREVIEW_AGENT_CMD, so what is under test is the part that can
# be wrong deterministically: what goes into the packet, what is refused, what
# is written, and what the gate does with it.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
XREVIEW="$HERE/../xreview.py"
GATE="$HERE/../check-xreviewed.sh"

PASS=0
FAIL=0
CASE=""

case_start() { CASE="$1"; printf '\033[1m%s\033[0m\n' "  $1"; }
good() { PASS=$((PASS + 1)); printf '    \033[32mok\033[0m   %s\n' "$*"; }
bad() { FAIL=$((FAIL + 1)); printf '    \033[31mFAIL\033[0m %s\n' "$*"; }

expect_exit() { # expect_exit <wanted> <got> <what>
  if [ "$2" = "$1" ]; then good "$3 (exit $2)"; else bad "$3: wanted exit $1, got $2"; fi
}

expect_contains() { # expect_contains <haystack-file> <needle> <what>
  if grep -qF -- "$2" "$1"; then good "$3"; else
    bad "$3: output did not contain '$2'"
    sed 's/^/        | /' "$1" | head -20
  fi
}

expect_absent() { # expect_absent <path> <what>
  if [ -e "$1" ]; then bad "$2: $1 exists"; else good "$2"; fi
}

expect_present() { # expect_present <path> <what>
  if [ -e "$1" ]; then good "$2"; else bad "$2: $1 does not exist"; fi
}

# ---------------------------------------------------------------------------
# A scratch repository per case.
#
# ⚠️ REAL GIT, NOT A MOCK. Everything xreview reads -- the staged diff, its
# hash, whether a file is tracked, whether an overrides line is in the INDEX
# rather than the worktree -- is a git question, and the defects being guarded
# against here (M4.26's untracked baseline; M4.31's added-versus-committed
# line) are all defects in how a gate asked git. A stub git would have
# reproduced the bug rather than caught it.
new_repo() {
  local d
  d="$(mktemp -d)"
  git -C "$d" init -q
  git -C "$d" config user.email t@example.com
  git -C "$d" config user.name Test
  mkdir -p "$d/docs/internal/product"
  cat > "$d/docs/internal/product/backlog.md" <<'ROW'
# Backlog

| ID | Task | Serves | State |
|---|---|---|---|
| H1.1 | A task that exists, so the packet has a row to carry | FR-11 | todo |
ROW
  printf 'seed\n' > "$d/seed.txt"
  git -C "$d" add -A
  git -C "$d" commit -qm "base"
  printf '%s' "$d"
}

# ⚠️ D AND OUT MOVE TOGETHER. A first draft set OUT once, from the first case's
# scratch directory, and every later case then redirected into a path whose
# parent had already been removed. The redirect failed, the assertion could not
# read the file -- and `expect_exit` still reported `ok`, because the gate had
# exited non-zero for the wrong reason. Six cases passed while measuring
# nothing. That is the same vacuous-pass shape this suite exists to refuse, and
# it appeared in the suite itself on the first run.
fresh() {
  D="$(new_repo)"
  OUT="$D/out.txt"
}

# ⚠️ THE STUB LIVES OUTSIDE THE REPOSITORY UNDER TEST, and finding out why is
# worth the comment. A first draft wrote it into the scratch repo, so the
# `git add -A` that stages the verdicts staged the stub too -- which moved the
# staged bytes, which correctly invalidated the verdicts, which failed four
# cases. The gate was right and the scaffolding was wrong: a new file in the
# commit IS a change to what was reviewed. Keeping the stub out of the tree
# keeps the cases measuring their own property instead of this one.
#
# ⚠️ AND ITS NAME COMES FROM `mktemp`, NOT A COUNTER. `stub_agent` is called in
# a command substitution, which is a SUBSHELL, so a `STUB_N=$((STUB_N + 1))`
# inside it never reaches the parent: every call computed the same name and
# each stub overwrote the last. R6 asked for a blocking agent and then a clean
# one, got the clean one twice, and reported that a blocking finding does not
# block -- a false FAIL that looked exactly like a real defect in the gate. The
# gate was correct throughout.
STUBS="$(mktemp -d)"
stub_agent() { # stub_agent <unused-dir> <json-it-returns>
  local s
  s="$(mktemp "$STUBS/agent-XXXXXX.sh")"
  cat > "$s" <<STUB
#!/usr/bin/env bash
cat <<'JSON'
$2
JSON
STUB
  chmod +x "$s"
  printf '%s' "$s"
}

CLEAN_VERDICT='{"verdict":"pass","findings":[]}'
FINDING_VERDICT='{"verdict":"changes-requested","findings":[{"id":"R1","severity":"blocking","file":"a.txt:1","summary":"s","failure_scenario":"f"}]}'

# ---------------------------------------------------------------------------
# ⚠️ PREFLIGHT, AND IT IS NOT CEREMONY. `python3 missing.py` exits 2, and so
# does xreview's own refusal -- so on the first red run R3 and N2 reported
# `ok ... (exit 2)` against a tree where xreview.py did not exist at all. Nine
# cases "passed" against nothing. An exit code alone is too coarse a signal to
# distinguish a refusal from an absence, which is the `ok nothing staged`
# failure one level down, inside the suite meant to catch it. Two answers, both
# applied: refuse to run at all when the implementation is absent, and assert
# on the MESSAGE wherever a refusal is expected.
for required in "$XREVIEW" "$GATE"; do
  if [ ! -f "$required" ]; then
    printf '\033[31mFAIL\033[0m %s does not exist -- refusing to run.\n' "$required"
    printf '       An exit code alone cannot tell a refusal from an absence,\n'
    printf '       so a suite run against a missing implementation would report\n'
    printf '       passes it has not earned.\n'
    exit 1
  fi
done

# ===========================================================================
printf '\n\033[1mxreview\033[0m\n\n'

# --- R1: an oversized packet is REFUSED, never truncated --------------------
#
# M4.34: the old packet was cut at ~37 KB by the agent tool, and `review.sh`
# had no length check anywhere in the packet path -- so it could not know, and
# the reviewer judged a prefix while believing it had the whole diff. It then
# filed a major against code it had never been shown. A size cap is not the
# fix on its own; REFUSING is, because a truncated packet is indistinguishable
# from a complete one.
case_start "R1 an over-cap packet is refused, not truncated"
fresh
python3 -c "
import random, string
random.seed(1)
print('\n'.join(''.join(random.choices(string.ascii_letters, k=80)) for _ in range(2000)))
" > "$D/big.txt"
git -C "$D" add big.txt
(cd "$D" && python3 "$XREVIEW" packet --task H1.1) > "$OUT" 2>&1
expect_exit 2 "$?" "refuses to emit an over-cap packet"
expect_contains "$OUT" "split this commit" "names the remedy"
# The refusal must not be silently satisfiable by reading a prefix.
if [ "$(wc -c < "$OUT")" -lt 4000 ]; then
  good "prints a refusal, not a truncated packet"
else
  bad "printed $(wc -c < "$OUT") bytes -- that looks like a packet, not a refusal"
fi
rm -rf "$D"

# --- R2: a verdict whose hash does not match the staged diff is rejected -----
#
# rule `hash-bound`: the verdict is bound to the staged bytes. A verdict for
# some other diff is not a review of this one.
case_start "R2 a stale diff_sha256 is rejected"
fresh
printf 'change\n' >> "$D/seed.txt"
git -C "$D" add -A
AGENT="$(stub_agent "$D" "$CLEAN_VERDICT")"
# ⚠️ BOTH ROLES, or this case cannot isolate what it is about. A first draft
# recorded only `reviewer`, moved the diff, and asserted exit 1 -- which the
# gate would have returned anyway for the missing `test-reviewer`. The case
# would have passed against a harness with no hash binding at all.
(cd "$D" && XREVIEW_AGENT_CMD="$AGENT" python3 "$XREVIEW" review --task H1.1 --role reviewer) >/dev/null 2>&1
(cd "$D" && XREVIEW_AGENT_CMD="$AGENT" python3 "$XREVIEW" review --task H1.1 --role test-reviewer) >/dev/null 2>&1
git -C "$D" add -A
RC=0; (cd "$D" && bash "$GATE") >/dev/null 2>&1 || RC=$?
expect_exit 0 "$RC" "both verdicts satisfy the hash they were taken against"
printf 'more\n' >> "$D/seed.txt"
git -C "$D" add -A
RC=0; (cd "$D" && bash "$GATE") > "$OUT" 2>&1 || RC=$?
expect_exit 1 "$RC" "and stop satisfying it the moment the diff moves"
expect_contains "$OUT" "no verdict" "says the verdicts do not bind these bytes"
rm -rf "$D"

# --- R3: a verdict with no task is rejected ---------------------------------
#
# M4.29: `review.sh record` never validated --task. Recording both roles under
# `--task M9.7` with four prior rounds on disk gave "ok ... (round 1 of 2)",
# exit 0 -- a complete bypass of the round cap leaving no trace. Omitting the
# flag entirely wrote "task": "" and reported "round 0 of 2" over two present
# verdicts, a count the counter's own definition makes impossible.
case_start "R3 a verdict with no task is rejected"
fresh
printf 'change\n' >> "$D/seed.txt"
git -C "$D" add -A
AGENT="$(stub_agent "$D" "$CLEAN_VERDICT")"
(cd "$D" && XREVIEW_AGENT_CMD="$AGENT" python3 "$XREVIEW" review --role reviewer) > "$OUT" 2>&1
expect_exit 2 "$?" "review without --task is refused"
expect_contains "$OUT" "--task" "names the missing flag"
(cd "$D" && XREVIEW_AGENT_CMD="$AGENT" python3 "$XREVIEW" review --task '' --role reviewer) > "$OUT" 2>&1
expect_exit 2 "$?" "an empty --task is refused"
expect_contains "$OUT" "--task" "names the flag"
(cd "$D" && XREVIEW_AGENT_CMD="$AGENT" python3 "$XREVIEW" review --task NOPE.9 --role reviewer) > "$OUT" 2>&1
expect_exit 2 "$?" "a --task with no backlog row is refused"
expect_contains "$OUT" "no row" "says why"
rm -rf "$D"

# --- R4: the third round fails, and the escape is a signed line -------------
#
# 25 `rounds:` escapes sit in the archived baselines/review.txt, 21 of them M4,
# and every one says "argued rather than split" -- the cap never once caused a
# split. So the cap is real here: no env var, no self-service key. The only
# way past is a line a person signs, which is a diff someone reads.
case_start "R4 round three fails without a signed override"
fresh
AGENT="$(stub_agent "$D" "$CLEAN_VERDICT")"
for i in 1 2 3; do
  printf 'round %s\n' "$i" >> "$D/seed.txt"
  git -C "$D" add -A
  (cd "$D" && XREVIEW_AGENT_CMD="$AGENT" python3 "$XREVIEW" review --task H1.1 --role reviewer) >/dev/null 2>&1
  (cd "$D" && XREVIEW_AGENT_CMD="$AGENT" python3 "$XREVIEW" review --task H1.1 --role test-reviewer) >/dev/null 2>&1
  git -C "$D" add -A
  RC=0
  (cd "$D" && bash "$GATE") > "$OUT" 2>&1 || RC=$?
  if [ "$i" -le 2 ]; then
    expect_exit 0 "$RC" "round $i passes"
  else
    expect_exit 1 "$RC" "round $i fails at the cap"
    expect_contains "$OUT" "approved-by" "names the override as the only route"
  fi
done
# Now the signed override, STAGED.
mkdir -p "$D/review"
printf 'H1.1 - genuinely one change - approved-by: a-person\n' > "$D/review/overrides.md"
git -C "$D" add review/overrides.md
RC=0
(cd "$D" && bash "$GATE") > "$OUT" 2>&1 || RC=$?
expect_exit 0 "$RC" "a staged, signed override lifts the cap"
rm -rf "$D"

# --- R5: an UNTRACKED overrides file silences nothing ------------------------
#
# M4.26 measured the shape this guards: the old finding-argue path read the
# WORKTREE, so a wholly untracked baselines/review.txt silenced blocking and
# major findings from both roles while the gate printed ok. The fix is to read
# the index -- `git show :` -- never the file on disk.
case_start "R5 an untracked overrides file silences nothing"
fresh
AGENT="$(stub_agent "$D" "$CLEAN_VERDICT")"
for i in 1 2 3; do
  printf 'round %s\n' "$i" >> "$D/seed.txt"
  git -C "$D" add -A
  (cd "$D" && XREVIEW_AGENT_CMD="$AGENT" python3 "$XREVIEW" review --task H1.1 --role reviewer) >/dev/null 2>&1
  (cd "$D" && XREVIEW_AGENT_CMD="$AGENT" python3 "$XREVIEW" review --task H1.1 --role test-reviewer) >/dev/null 2>&1
done
git -C "$D" add -A
mkdir -p "$D/review"
printf 'H1.1 - untracked, so it is not part of the commit - approved-by: nobody\n' \
  > "$D/review/overrides.md"   # deliberately NOT staged
RC=0
(cd "$D" && bash "$GATE") > "$OUT" 2>&1 || RC=$?
expect_exit 1 "$RC" "the cap still bites"
rm -rf "$D"

# --- R6: a blocking finding blocks -------------------------------------------
case_start "R6 an unresolved blocking finding blocks the commit"
fresh
printf 'change\n' >> "$D/seed.txt"
git -C "$D" add -A
BAD_AGENT="$(stub_agent "$D" "$FINDING_VERDICT")"
GOOD_AGENT="$(stub_agent "$D" "$CLEAN_VERDICT")"
(cd "$D" && XREVIEW_AGENT_CMD="$BAD_AGENT" python3 "$XREVIEW" review --task H1.1 --role reviewer) >/dev/null 2>&1
(cd "$D" && XREVIEW_AGENT_CMD="$GOOD_AGENT" python3 "$XREVIEW" review --task H1.1 --role test-reviewer) >/dev/null 2>&1
git -C "$D" add -A
RC=0
(cd "$D" && bash "$GATE") > "$OUT" 2>&1 || RC=$?
expect_exit 1 "$RC" "a blocking finding refuses the commit"
expect_contains "$OUT" "blocking" "names the severity"
rm -rf "$D"

# --- R6b: a finding FIXED in the next round stops blocking --------------------
#
# rule `fixed-or-argued`, and the direction a cap makes easy to get wrong. The
# findings loop filters to verdicts taken against the CURRENT staged bytes;
# without that filter a blocking finding from round one keeps blocking after
# it has been fixed, and the only way forward is an override for a defect that
# no longer exists. Measured: deleting the filter leaves every other case in
# this suite green, which is why this one exists.
case_start "R6b a blocking finding fixed in the next round no longer blocks"
fresh
printf 'the defect\n' >> "$D/seed.txt"
git -C "$D" add -A
BAD="$(stub_agent "$D" "$FINDING_VERDICT")"
GOOD="$(stub_agent "$D" "$CLEAN_VERDICT")"
(cd "$D" && XREVIEW_AGENT_CMD="$BAD" python3 "$XREVIEW" review --task H1.1 --role reviewer) >/dev/null 2>&1
(cd "$D" && XREVIEW_AGENT_CMD="$GOOD" python3 "$XREVIEW" review --task H1.1 --role test-reviewer) >/dev/null 2>&1
git -C "$D" add -A
RC=0; (cd "$D" && bash "$GATE") >/dev/null 2>&1 || RC=$?
expect_exit 1 "$RC" "round one blocks"
# Round two: the author fixes it, and both roles now pass on the NEW bytes.
printf 'the fix\n' >> "$D/seed.txt"
git -C "$D" add -A
(cd "$D" && XREVIEW_AGENT_CMD="$GOOD" python3 "$XREVIEW" review --task H1.1 --role reviewer) >/dev/null 2>&1
(cd "$D" && XREVIEW_AGENT_CMD="$GOOD" python3 "$XREVIEW" review --task H1.1 --role test-reviewer) >/dev/null 2>&1
git -C "$D" add -A
RC=0; (cd "$D" && bash "$GATE") > "$OUT" 2>&1 || RC=$?
expect_exit 0 "$RC" "round two passes -- the old finding does not follow the fix"
rm -rf "$D"

# --- R7: BOTH roles are required ---------------------------------------------
case_start "R7 one role is not a review"
fresh
printf 'change\n' >> "$D/seed.txt"
git -C "$D" add -A
AGENT="$(stub_agent "$D" "$CLEAN_VERDICT")"
(cd "$D" && XREVIEW_AGENT_CMD="$AGENT" python3 "$XREVIEW" review --task H1.1 --role reviewer) >/dev/null 2>&1
git -C "$D" add -A
RC=0
(cd "$D" && bash "$GATE") > "$OUT" 2>&1 || RC=$?
expect_exit 1 "$RC" "the missing test-reviewer refuses the commit"
expect_contains "$OUT" "test-reviewer" "names the role that is missing"
rm -rf "$D"

# --- R8: NOTHING STAGED IS A FAILURE, not a green line -----------------------
#
# ⚠️ THE FAILURE SHAPE THIS WHOLE EXERCISE IS ABOUT. `check-reviewed.sh:11`
# prints "ok nothing staged" and exits 0, so `pre-commit run --all-files` in CI
# reports it green having verified nothing -- build.md says so in as many
# words: "It does not fail, which is worse."
case_start "R8 nothing staged is a failure, not a vacuous pass"
fresh
RC=0
(cd "$D" && bash "$GATE") > "$OUT" 2>&1 || RC=$?
expect_exit 1 "$RC" "an empty staged diff cannot pass"
# ⚠️ THE MESSAGE, NOT ONLY THE EXIT CODE. Measured: replacing this gate's
# empty-diff guard with `if False:` left R8 green, because the run then failed
# one step later for a DIFFERENT reason -- no verdict binds the empty hash. The
# case passed while the property it names was gone. Asserting the wording is
# what makes the guard falsifiable.
expect_contains "$OUT" "nothing staged" "fails FOR being empty, not incidentally"
expect_contains "$OUT" "has verified" "says what a green line would have meant"
if grep -qE '\bok\b.*nothing staged' "$OUT"; then
  bad "printed a green 'nothing staged' line"
else
  good "does not print a green line"
fi
rm -rf "$D"

# --- R9: no harness exemption -------------------------------------------------
#
# review.md rule `harness-exempt` waives the reviewer entirely for a change
# confined to scripts/, buildSrc/, .agents/, .claude/, docs/ or baselines/ --
# which is every harness fix. That is why the harness was never reviewed while
# it was being built. xreview has no such waiver, and this case is what keeps
# one from being added back quietly.
case_start "R9 a harness-only change still needs both verdicts"
fresh
mkdir -p "$D/scripts"
printf '#!/bin/sh\necho hi\n' > "$D/scripts/a-gate.sh"
git -C "$D" add -A
RC=0
(cd "$D" && bash "$GATE") > "$OUT" 2>&1 || RC=$?
expect_exit 1 "$RC" "a scripts/-only diff is not exempt"
rm -rf "$D"

# --- N1: the negative control -------------------------------------------------
#
# rule `empty-is-valid`. A suite that only proves refusals is satisfied by a
# gate that refuses everything, which is the mirror of the vacuous pass.
case_start "N1 a clean diff with two passing verdicts is allowed through"
fresh
printf 'change\n' >> "$D/seed.txt"
git -C "$D" add -A
AGENT="$(stub_agent "$D" "$CLEAN_VERDICT")"
(cd "$D" && XREVIEW_AGENT_CMD="$AGENT" python3 "$XREVIEW" review --task H1.1 --role reviewer) >/dev/null 2>&1
(cd "$D" && XREVIEW_AGENT_CMD="$AGENT" python3 "$XREVIEW" review --task H1.1 --role test-reviewer) >/dev/null 2>&1
git -C "$D" add -A
RC=0
(cd "$D" && bash "$GATE") > "$OUT" 2>&1 || RC=$?
expect_exit 0 "$RC" "two clean verdicts pass"
rm -rf "$D"

# --- N2: reported IS recorded -------------------------------------------------
#
# M0.64, and the reason the runner writes the verdict rather than asking the
# reviewer to. On M0.56's final round BOTH reviewers closed their reports with
# "verdict recorded at this hash" and NEITHER had run `review.sh record`; no
# file existed for either role. Being told not to write explains not running
# the command; it explains nothing about asserting it had been run. Here the
# agent CANNOT write a verdict -- it only speaks -- so "reported" and
# "recorded" are one act and the gap cannot open.
case_start "N2 the verdict is written by the runner, from what the agent returned"
fresh
printf 'change\n' >> "$D/seed.txt"
git -C "$D" add -A
# An agent that LIES: it claims to have recorded a pass, and returns nothing.
cat > "$STUBS/liar.sh" <<'LIAR'
#!/usr/bin/env bash
echo "I have reviewed the diff and recorded a pass verdict at this hash."
LIAR
chmod +x "$STUBS/liar.sh"
(cd "$D" && XREVIEW_AGENT_CMD="$STUBS/liar.sh" python3 "$XREVIEW" review --task H1.1 --role reviewer) > "$OUT" 2>&1
expect_exit 2 "$?" "a reply that is prose, not a verdict, is refused"
# ⚠️ THE SPECIFIC WORDING, for the same reason as R8: `validate()` also
# refuses a non-dict, so a bare "verdict" match stayed green with the
# only-prose branch deleted. Measured.
expect_contains "$OUT" "only prose" "names prose as what came back"
expect_contains "$OUT" "a claim is not a verdict" "says why nothing was written"
expect_absent "$D/review/verdicts/H1.1" "no verdict was written for a claim"
rm -rf "$D"

# --- V1: a verdict that contradicts itself is refused, and nothing is written -
#
# Four guards, each measured surviving before this case existed. They share one
# shape: a verdict the runner could happily store but which asserts nothing.
case_start "V1 a malformed or self-contradicting verdict is refused"
fresh
printf 'change\n' >> "$D/seed.txt"
git -C "$D" add -A

try_verdict() { # try_verdict <json> <expected-message-fragment> <what>
  local a; a="$(stub_agent "$D" "$1")"
  (cd "$D" && XREVIEW_AGENT_CMD="$a" python3 "$XREVIEW" review --task H1.1 --role reviewer) > "$OUT" 2>&1
  expect_exit 2 "$?" "$3"
  expect_contains "$OUT" "$2" "$3: says why"
  expect_absent "$D/review/verdicts/H1.1/1.reviewer.json" "$3: nothing written"
}

# rule `failure-scenario`: a finding without one is a style opinion, and a
# reviewer permitted to file style opinions files those instead of defects.
try_verdict \
  '{"verdict":"changes-requested","findings":[{"id":"R1","severity":"major","file":"a:1","summary":"s"}]}' \
  "failure_scenario" "a finding with no failure scenario"

# ⚠️ A PASS CARRYING A BLOCKER IS TWO ANSWERS AT ONCE, and storing it leaves the
# gate to guess which one the reviewer meant.
try_verdict \
  '{"verdict":"pass","findings":[{"id":"R1","severity":"blocking","file":"a:1","summary":"s","failure_scenario":"f"}]}' \
  "cannot carry" "a pass carrying a blocking finding"

# One id, one defect. Two findings under one id cannot both be fixed or argued.
try_verdict \
  '{"verdict":"changes-requested","findings":[{"id":"R1","severity":"major","file":"a:1","summary":"s","failure_scenario":"f"},{"id":"R1","severity":"major","file":"b:2","summary":"t","failure_scenario":"g"}]}' \
  "duplicate finding id" "two findings sharing one id"

try_verdict \
  '{"verdict":"looks-fine","findings":[]}' \
  "verdict must be one of" "a verdict outside the vocabulary"

# ⚠️ AN UNKNOWN SEVERITY SILENTLY DOWNGRADES. `verify` blocks on
# `blocking`/`major`; a finding labelled anything else is stored and then
# passes straight through, so an unchecked vocabulary turns a real defect into
# a green commit. Measured: without this case, deleting the severity check
# leaves the whole suite green.
try_verdict \
  '{"verdict":"changes-requested","findings":[{"id":"R1","severity":"cosmetic","file":"a:1","summary":"s","failure_scenario":"f"}]}' \
  "severity must be one of" "a finding with a severity outside the vocabulary"
rm -rf "$D"

# --- V2: an override without a reason is not a signature ----------------------
#
# ⚠️ M4.26 measured the shape: the older argue key required NO REASON at all --
# "a file containing only `reviewer:R1` and `test-reviewer:R1`, with no
# justification at all, silences two blocking findings and the gate prints ok".
# A name with no reason is that defect wearing a signature.
case_start "V2 an override with no reason does not lift the cap"
fresh
AGENT="$(stub_agent "$D" "$CLEAN_VERDICT")"
for i in 1 2 3; do
  printf 'round %s\n' "$i" >> "$D/seed.txt"
  git -C "$D" add -A
  (cd "$D" && XREVIEW_AGENT_CMD="$AGENT" python3 "$XREVIEW" review --task H1.1 --role reviewer) >/dev/null 2>&1
  (cd "$D" && XREVIEW_AGENT_CMD="$AGENT" python3 "$XREVIEW" review --task H1.1 --role test-reviewer) >/dev/null 2>&1
done
git -C "$D" add -A
mkdir -p "$D/review"
printf 'H1.1 approved-by: a-person\n' > "$D/review/overrides.md"
git -C "$D" add review/overrides.md
RC=0; (cd "$D" && bash "$GATE") > "$OUT" 2>&1 || RC=$?
expect_exit 1 "$RC" "a name with no reason is refused"
# ⚠️ AND PUNCTUATION IS NOT A REASON. The line format puts a dash between the
# task and the reason, so `H1.1 - approved-by: x` satisfies the regex's
# "something is here" and says nothing at all. Measured: without this, the
# guard that strips dashes and spaces before judging the reason survives
# deletion with the suite green.
printf 'H1.1 - approved-by: a-person\n' > "$D/review/overrides.md"
git -C "$D" add review/overrides.md
RC=0; (cd "$D" && bash "$GATE") > "$OUT" 2>&1 || RC=$?
expect_exit 1 "$RC" "a dash is not a reason"
printf 'H1.1 - the seal protocol is one change and splitting it hides the race - approved-by: a-person\n' \
  > "$D/review/overrides.md"
git -C "$D" add review/overrides.md
RC=0; (cd "$D" && bash "$GATE") > "$OUT" 2>&1 || RC=$?
expect_exit 0 "$RC" "a reason and a name together lift it"
rm -rf "$D"

# --- N4: an outage is not a finding about the reviewer ---------------------
#
# ⚠️ MEASURED, on the second round of the first real review this tool ran. The
# reviewer's OAuth session had expired, `claude` printed 73 bytes and exited
# non-zero, and the runner reported "the reviewer returned no verdict object --
# only prose". That accuses the reviewer of exactly the dishonesty this tool
# exists to make impossible, when in fact nothing had run at all. The two
# failures must never share a message: one means the agent answered badly, the
# other means the harness is broken.
case_start "N4 a reviewer that never ran is reported as an outage, not as prose"
fresh
printf 'change\n' >> "$D/seed.txt"
git -C "$D" add -A
cat > "$STUBS/dead.sh" <<'DEAD'
#!/usr/bin/env bash
echo "Failed to authenticate: OAuth session expired and could not be refreshed"
exit 1
DEAD
chmod +x "$STUBS/dead.sh"
(cd "$D" && XREVIEW_AGENT_CMD="$STUBS/dead.sh" python3 "$XREVIEW" review --task H1.1 --role reviewer) > "$OUT" 2>&1
expect_exit 2 "$?" "refuses"
expect_contains "$OUT" "never ran" "says the reviewer never ran"
expect_contains "$OUT" "NOT a finding about the reviewer" "does not blame the reviewer"
expect_absent "$D/review/verdicts/H1.1/1.reviewer.json" "records nothing"
# And the prose path must still be distinguishable from it.
cat > "$STUBS/chatty.sh" <<'CHAT'
#!/usr/bin/env bash
echo "I reviewed it and recorded a pass."
CHAT
chmod +x "$STUBS/chatty.sh"
(cd "$D" && XREVIEW_AGENT_CMD="$STUBS/chatty.sh" python3 "$XREVIEW" review --task H1.1 --role reviewer) > "$OUT" 2>&1
expect_contains "$OUT" "only prose" "a reviewer that DID run and said nothing useful reads differently"
rm -rf "$D"

# --- N5: the `record` fallback, and the mark that says it was used ----------
#
# ⚠️ `record` IS THE WEAKER PATH. `review` invokes the reviewer and writes what
# came back, so the author never handles the reply; `record` takes a reply the
# caller captured, which restores one link of exactly the chain `review`
# removes. It exists because a subprocess reviewer is not always possible --
# measured here, the CLI's access token is refreshed by the host session, so a
# spawned `claude -p` cannot authenticate at all. Since it cannot carry the
# same guarantee, every verdict it writes is marked, permanently.
case_start "N5 record writes a verdict, refuses prose, and marks itself"
fresh
printf 'change\n' >> "$D/seed.txt"
git -C "$D" add -A
printf 'I reviewed it and it is fine.\n' > "$D/reply.txt"
(cd "$D" && python3 "$XREVIEW" record --task H1.1 --role reviewer --file reply.txt) > "$OUT" 2>&1
expect_exit 2 "$?" "prose is refused on this path too"
# ⚠️ THE MESSAGE, because `validate()` also refuses a non-dict, so a bare
# exit-2 assertion stayed green with this path's own guard deleted. Measured.
expect_contains "$OUT" "Nothing recorded" "refused by THIS path's guard, not incidentally"
expect_absent "$D/review/verdicts/H1.1/1.reviewer.json" "nothing written for prose"
printf '%s\n' "$CLEAN_VERDICT" > "$D/reply.txt"
(cd "$D" && python3 "$XREVIEW" record --task H1.1 --role reviewer --file reply.txt) > "$OUT" 2>&1
expect_exit 0 "$?" "a real verdict is written"
expect_present "$D/review/verdicts/H1.1/1.reviewer.json" "the verdict exists"
expect_contains "$OUT" "not \`review\`" "warns that the weaker path was used"
if grep -q '"via": "record"' "$D/review/verdicts/H1.1/1.reviewer.json"; then
  good "the verdict records HOW it was taken, so a reader can tell the paths apart"
else
  bad "the verdict does not mark itself as recorded rather than reviewed"
fi
# ⚠️ And it validates identically: the weaker path must not be the lax one.
printf '{"verdict":"pass","findings":[{"id":"R1","severity":"blocking","file":"a:1","summary":"s","failure_scenario":"f"}]}\n' > "$D/reply.txt"
(cd "$D" && python3 "$XREVIEW" record --task H1.1 --role test-reviewer --file reply.txt) > "$OUT" 2>&1
expect_exit 2 "$?" "a self-contradicting verdict is refused here too"
rm -rf "$D"

# --- N3: the packet carries the diff and the task, and NOT a gate-pass list ---
#
# The old packet's "GATES THAT ALREADY PASSED (do not re-check these)" header
# listed check-diff-size.sh as PASSED when it had structurally been unable to
# run -- invoked with no commit-message file, it warns and exits 0. Combined
# with rule `no-regating`, the reviewer was instructed not to look at diff size
# on the strength of a gate that never ran.
case_start "N3 the packet carries the diff and the row, and no gate-pass list"
fresh
printf 'a distinctive line the reviewer must see\n' >> "$D/seed.txt"
git -C "$D" add -A
(cd "$D" && python3 "$XREVIEW" packet --task H1.1) > "$OUT" 2>&1
expect_exit 0 "$?" "emits a packet"
expect_contains "$OUT" "a distinctive line the reviewer must see" "carries the diff"
expect_contains "$OUT" "A task that exists" "carries the backlog row"
if grep -qi 'already passed\|do not re-check' "$OUT"; then
  bad "carries a gate-pass list"
else
  good "carries no gate-pass list"
fi
rm -rf "$D"

# ===========================================================================
printf '\n'
if [ "$FAIL" -ne 0 ]; then
  printf '\033[31m%s passed, %s FAILED\033[0m\n' "$PASS" "$FAIL"
  exit 1
fi
rm -rf "$STUBS"
printf '\033[32m%s passed, 0 failed\033[0m\n' "$PASS"
