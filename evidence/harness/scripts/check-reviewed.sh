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
# non-negotiable 5: BOTH reviewers. Test weakness is invisible to every other gate,
# so a production-only review is not a review.
for ROLE in reviewer test-reviewer; do
  [ -f ".harness/review/$SHA.$ROLE.json" ] || {
    fail "no '$ROLE' verdict for the staged bytes ($SHA)"
    echo "         scripts/review.sh context --task <ID>"
    echo "         then have the $ROLE agent return: scripts/review.sh record --file v.json --task <ID> --role $ROLE"
  }
done
[ "$FAILED" -eq 0 ] || finish
for ROLE in reviewer test-reviewer; do
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
[ "$FAILED" -eq 0 ] && ok "staged bytes reviewed by both reviewer and test-reviewer ($SHA)"
finish
