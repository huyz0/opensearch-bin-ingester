#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Non-negotiable 5: every commit is reviewed by two agents that did not write it,
# and the verdict is bound to the staged bytes by hash.
source "$(dirname "$0")/lib.sh"
cd "$ROOT"
hdr "check-reviewed"
if [ -n "${CHECK_RANGE:-}" ]; then
  SHA=$(git diff "$CHECK_RANGE" HEAD | sha256sum | cut -d' ' -f1)
else
  git diff --cached --quiet && { ok "nothing staged"; finish; }
  SHA=$(git diff --cached | sha256sum | cut -d' ' -f1)
fi
# ⚠️ WHICH roles is DERIVED from the staged paths, never declared by the author.
# This once read `for ROLE in reviewer test-reviewer` unconditionally, so a
# documentation-only commit summoned a test-reviewer that had no test to
# evaluate. Its only moves were a vacuous verdict or no commit -- and downstream
# "pass over a diff with nothing to test" and "pass over a diff nobody read" are
# THE SAME BITS. A test-reviewer once declined to file one on exactly that
# ground, and was right.
#
# ⚠️ This LOOSENS the gate, so it is computed rather than claimed: the author
# cannot assert "docs-only". Falls back to BOTH roles if the deriver fails, so a
# broken script cannot silently buy a one-role commit.
REQUIRED_ROLES=$(./scripts/review-roles.sh 2>/dev/null | grep -v '^#' || true)
[ -n "$REQUIRED_ROLES" ] || REQUIRED_ROLES=$'reviewer\ntest-reviewer'
for ROLE in $REQUIRED_ROLES; do
  [ -f ".harness/review/$SHA.$ROLE.json" ] || {
    fail "no '$ROLE' verdict for the staged bytes ($SHA)"
    echo "         scripts/review.sh context --task <ID>"
    echo "         then have the $ROLE agent return: scripts/review.sh record --file v.json --task <ID> --role $ROLE"
  }
done
[ "$FAILED" -eq 0 ] || finish

# ⚠️ review.md rule 12's cap, as a predicate rather than a sentence. It was
# violated on 8 of 12 tasks in one session -- one reaching ELEVEN rounds, where
# rounds 9-11 fixed `minor` findings this gate has never blocked on -- because
# NOTHING COUNTED. A rule only the author can uphold differs per run and dies
# with the session.
# ⚠️ 8, AND DRAFTS OF THIS COMMIT MADE IT 7 IN TWO PLACES -- here and in backlog.md's
# M4.21 row. That is the wrong direction for the number justifying the cap this same
# commit loosens: non-negotiable 2's subject, even though a comment is not a threshold.
# ⚠️ AND THE FIRST FIX ONLY MOVED IT. Round 6 restored this line, left the row's copy at
# 7, and wrote here that "the higher figure is kept" -- false, because it had been
# relocated rather than reverted. A disclosure that is wrong about the diff it sits in is
# worse than a silent flip: it spends the reader's trust to conceal the thing it claims
# to reveal. Round 7 measured it; both sites now say 8.
# ⚠️ WHAT THE TREE SAYS: 8 here, 8 in backlog M0.47 (which calls it this repository's own
# record) and 8 in the M4.21 row; 7 in `review_rounds.py`, which said 7 before this
# change and is not this commit's to reinterpret. Neither is reconstructible, so the
# split is recorded in review.md rule 12 rather than settled.
# ⚠️ THE EXIT STATUS IS CHECKED, not swallowed. `|| echo 0` treated every failure
# as "no rounds yet" -- including the refusal `review_rounds.py` raises when two
# verdicts for one hash disagree about the task, which turned a deliberate stop
# into a silent pass at round 0. A gate that cannot tell "nothing recorded" from
# "the records contradict each other" is the shape this whole file exists against.
#
# ⚠️ AND STDERR STAYS OUT OF THE VALUE. The first fix for the swallowed status
# wrote `ROUNDS=$(... 2>&1)`, which routed any interpreter noise -- a pyenv shim, a
# DeprecationWarning, PYTHONWARNINGS -- INTO the operand the arithmetic then reads.
# Measured: one stderr line with exit 0 and the gate prints "[: integer expected",
# SKIPS the cap block entirely, and exits 0. A cap that fails OPEN on an
# environmental trigger. So stderr goes to a file for the message only, and the
# value is then required to be digits -- anything else refuses rather than passes.
ERRS=$(mktemp)
# ⚠️ `|| { RC=$?; ...; }` RATHER THAN `if ! ...; then`, so the status is the
# SCRIPT'S. Under `if !` the `!` has already inverted the pipeline by the time the
# branch runs, so `$?` there is 0 -- a message reading "failed with status 0".
ROUNDS=$(python3 scripts/review_rounds.py "$SHA" 2>"$ERRS") || {
  RC=$?
  # ⚠️ A REFUSAL THAT NAMES SOMETHING. `SystemExit(3)` with nothing on stderr
  # printed a bare "FAIL" and exited 1 -- fail-closed, but naming no reason, which
  # is the `ok nothing staged` complaint in AGENTS.md wearing the other sign.
  WHY=$(cat "$ERRS")
  [ -n "$WHY" ] || WHY="review_rounds.py exited $RC and printed nothing"
  fail "$WHY"
  rm -f "$ERRS"
  finish
}
rm -f "$ERRS"
case "$ROUNDS" in
  ''|*[!0-9]*)
    fail "review_rounds.py printed a non-numeric round count: '$ROUNDS'"
    finish
    ;;
esac
TASK=$(python3 scripts/review_rounds.py "$SHA" --task 2>/dev/null || true)
# ⚠️ VERDICTS WITH NO TASK ARE A BROKEN RECORD, not a fresh first round. `record`
# does not require `--task`, so omitting it writes `"task": ""` and the count came
# back 0 -- "round 0 of 2" over two present verdicts, which the counter's own
# definition (a round IS a reviewed hash) makes impossible. ⚠️ THE FULL FIX IS
# ELSEWHERE and is a real choice -- require `--task`, validate it against
# backlog.md, or bind it in check-commit-msg.sh where the subject's id and the
# staged hash are both in scope -- so M4.29 owns it; this refuses the case that is
# visible from here.
if [ -z "$TASK" ] && ls ".harness/review/$SHA."*.json >/dev/null 2>&1; then
  fail "verdicts exist for the staged bytes but name no task -- recorded without --task"
  finish
fi
# ⚠️ THE CAP IS ARGUABLE NOW, and it was not before: this block used to print
# "SPLIT it, or argue the finding with a staged baselines/review.txt entry" and
# then return BEFORE the loop below that reads that file, so the second remedy
# was never true for the round count -- the baseline could only ever silence an
# individual blocking or major finding.
#
# ⚠️ WHY MAKING IT TRUE IS STRICTER THAN LEAVING IT FALSE, which is the whole
# argument: an author whose commit genuinely cannot be split had no route left
# but SKIP=check-reviewed, and a SKIP disables this gate's SUBSTANTIVE checks
# too -- both roles present, and no unresolved blocking or major finding. So the
# missing escape did not make the cap bite harder, it made the gate ABSENT
# exactly when the cap bit. AT LEAST NINE commits in M4 landed that way, found with
# `git log --grep=SKIP=check-reviewed`; seven predate M4.19 and M4.20, this session's own two skips.
# ⚠️ AT LEAST, because that grep finds commits that DISCLOSED a skip, not commits that
# took one. M4.12 bypassed a 12-round cap with neither `SKIP` nor `check-reviewed` in its
# message, so it is not among the nine, and the log cannot bound the real number from
# above. That is an argument FOR the escape: a skip can leave no trace, an argued
# exception is a staged line reviewed with the commit it excuses. An argued
# exception keeps both-roles and no-unresolved-majors enforced, and leaves the
# argument in the tree where it is greppable and reviewed like any other change.
#
# ⚠️ THE KEY IS `rounds:<task>`, not a bare word: an entry keyed on nothing would
# cover every future task silently, which is the defect this file's own header
# records for bare finding ids.
if [ "${ROUNDS:-0}" -gt 2 ]; then
  ARGUED=0
  # ⚠️ THE ADDED LINES OF THE DIFF, not "was the file touched". Two bypasses,
  # both measured by round-3 review, both closed by asking the right question:
  #   - `grep -qx 'baselines/review.txt'` is a BRE, so the `.` was a wildcard and
  #     a file named `baselines/review_txt` waived the cap. That is the same defect
  #     fixed for `$TASK` a few lines down and left sitting in the FILENAME.
  #   - a `rounds:` line already in HEAD plus ANY staged edit to that file waived
  #     the cap, because the line appears in the diff as CONTEXT. The escape is
  #     supposed to be reviewed with the commit it excuses, and a context line is
  #     reviewed with a different one.
  # ⚠️ So the argument must be ADDED here: `+rounds:<task>  <reason>`.
  if [ -n "$TASK" ]; then
    # ⚠️ awk WITH AN EXACT FIELD MATCH, not a grep pattern. `$TASK` goes into a
    # BRE unescaped otherwise, and every task id here contains a `.` -- measured
    # against task M9.1, both `rounds:M9x1` and `rounds:M9-1` lifted the cap, and
    # `review.sh record` does not validate `--task`, so `--task '.*'` would let any
    # `rounds:` line argue anything.
    # ⚠️ A NON-SPACE REMAINDER REQUIRES A REASON -- NOT `NF > 1`, which was tried
    # and is broken: a trailing tab splits into a second, EMPTY field, so
    # `rounds:M9.1<TAB>` argued the cap while saying why it could not be split
    # precisely nowhere. The key is stripped and what is left must contain a
    # non-space character.
    if git diff --cached -- baselines/review.txt \
        | awk -v k="rounds:$TASK" '/^\+/ && !/^\+\+\+/ {
                                     line = substr($0, 2)
                                     if (line ~ /^[[:space:]]*#/) next
                                     split(line, f, /[[:space:]]+/)
                                     rest = line
                                     sub(/^[^[:space:]]+[[:space:]]*/, "", rest)
                                     # ⚠️ A NON-SPACE REASON, not just a field. A
                                     # trailing tab split into a second, EMPTY
                                     # field, so `rounds:M9.1<TAB>` argued the cap
                                     # again -- caught by the very test written for
                                     # that case one round earlier.
                                     if (f[1] == k && rest ~ /[^[:space:]]/) found = 1
                                   }
                                   END { exit found ? 0 : 1 }'; then
      ARGUED=1
    fi
  fi
  if [ "$ARGUED" -eq 0 ]; then
    fail "review round $ROUNDS exceeds review.md rule 12's cap of 2"
    echo "         Round one finds, round two verifies. A third means the commit is"
    echo "         too big: SPLIT it, or argue it with a STAGED baselines/review.txt"
    echo "         line keyed 'rounds:$TASK  <why it cannot be split>'."
    echo "         ⚠️ The line must be ADDED by this commit -- an argument sitting"
    echo "         in the already-committed file does not carry a later round."
    echo "         One added line then covers every further round of THIS commit:"
    echo "         you argued that these bytes cannot be split, and that does not"
    echo "         stop being true at round 6. Landing it normally ends its"
    echo "         effect -- but see M4.31: a later edit that makes git re-render"
    echo "         the committed line as added can re-arm it, which is measured"
    echo "         and not yet fixed."
    echo "         ⚠️ Staging that line MOVES the staged hash, so both verdicts must"
    echo "         be re-recorded against the new one -- the next run will ask for"
    echo "         them before it looks at the cap again."
    echo "         ⚠️ Only 'blocking' and 'major' findings block a commit -- a 'pass'"
    echo "         carrying 'minor' findings lands, with them recorded in the"
    echo "         commit body (review.md rule 11)."
    finish
  fi
  warn "review round $ROUNDS exceeds the cap of 2, ARGUED as 'rounds:$TASK' in baselines/review.txt"
fi

for ROLE in $REQUIRED_ROLES; do
V=".harness/review/$SHA.$ROLE.json"
# Checked for EVERY verdict, not only changes-requested: review.md rules 9-10
# say a blocking or major finding is fixed or argued, and a reviewer returning
# 'pass' while carrying one is exactly the case that used to slip through.
unresolved=$(python3 - "$V" "$ROLE" <<'PY'
import json, os, sys
v = json.load(open(sys.argv[1])); role = sys.argv[2]
argued = set()
p = 'baselines/review.txt'
if os.path.exists(p):
    for line in open(p):
        line = line.strip()
        if line and not line.startswith('#'):
            # Keyed <role>:<id>. Keyed on the bare id, an argument staged for
            # the production reviewer's R3 also silenced the test reviewer's R3.
            argued.add(line.split()[0])
bad = [f for f in v.get('findings', [])
       if f.get('severity') in ('blocking', 'major')
       and ('%s:%s' % (role, f.get('id'))) not in argued]
print(len(bad))
PY
)
[ "$unresolved" = "0" ] || fail "$ROLE: $unresolved blocking/major finding(s) neither fixed nor argued as '$ROLE:<id>' in baselines/review.txt"
done
# ⚠️ NAMES the roles. "reviewed by both" printed over a one-role run is the same
# false line this gate exists to prevent.
[ "$FAILED" -eq 0 ] && ok "staged bytes reviewed by $(echo $REQUIRED_ROLES | tr '\n' ' ')(round $ROUNDS of 2) ($SHA)"
finish
