#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Non-negotiable 5: every commit is reviewed in two passes by an agent that did
# not write it, and each verdict is bound to the staged bytes by hash.
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
#
# ⚠️ THE CAP IS THREE SINCE M0.114, RAISED FROM TWO, and raising it is not the
# weakening non-negotiable 2 forbids: this gate has never blocked on a `minor`,
# and every round it did block was a round someone spent anyway, past the cap,
# under a signed override. M5.56 reached EIGHTEEN. What the third round buys is
# the shape the overrides record over and over -- round one finds, round two
# fixes and finds in the fix, round three verifies the fix -- without a
# signature per task. ⚠️ AND IT IS A CAP ON ROUNDS, NOT A BUDGET TO SPEND: rule
# 11 forbids spending one on a `minor`, so a task that needs three has found two
# rounds of blocking or major defects and is a task worth splitting next time.
ROUNDS=$(python3 scripts/review_rounds.py "$SHA" 2>/dev/null || echo 0)
if [ "${ROUNDS:-0}" -gt 3 ]; then
  fail "review round $ROUNDS exceeds review.md rule 12's cap of 3"
  echo "         Round one finds, round two fixes, round three verifies. A fourth"
  echo "         means the commit is too big: SPLIT it, or argue the finding with"
  echo "         a staged baselines/review.txt entry. ⚠️ Only 'blocking' and"
  echo "         'major' block a commit -- a 'pass' carrying 'minor' findings"
  echo "         lands, with them recorded in the commit body (review.md rule 11),"
  echo "         and spending a round on one is forbidden rather than merely"
  echo "         discouraged."
  finish
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
[ "$FAILED" -eq 0 ] && ok "staged bytes reviewed by $(echo $REQUIRED_ROLES | tr '\n' ' ')(round $ROUNDS of 3) ($SHA)"
finish
