#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Which reviewer roles a change actually needs — DERIVED from the staged paths,
# never asserted by the author.
#
# ⚠️ Why this exists. check-reviewed.sh demanded BOTH roles for every commit,
# including documentation-only ones. A test-reviewer handed a diff with no test
# and no code has nothing to evaluate, so the only ways forward were a vacuous
# verdict or no commit — and downstream, "pass over a diff with nothing to test"
# and "pass over a diff nobody read" are THE SAME BITS. A test reviewer declined
# on exactly that ground rather than file one, and it was right.
#
# The rule, and it is deliberately blunt:
#   reviewer       always. Someone independent reads every change.
#   test-reviewer  only when the diff touches something a test could constrain —
#                  test sources, production code, or executable enforcement
#                  (gate scripts, build logic, CI workflows, the hook config).
#
# Docs-only changes get one reviewer. Anything executable gets two.
#
# ⚠️ This LOOSENS a gate, so it is derived rather than declared: the author
# cannot claim "docs-only", the script computes it from the paths. Non-negotiable
# 2 forbids weakening a gate to make a check pass; deriving the requirement from
# the diff is not a weakening, and printing the reason keeps a one-role run from
# ever being mistaken for a two-role one.
source "$(dirname "$0")/lib.sh"
cd "$ROOT"

if [ -n "${CHECK_RANGE:-}" ]; then
  PATHS=$(git diff --name-only --diff-filter=ACMRD "$CHECK_RANGE" HEAD 2>/dev/null)
else
  PATHS=$(git diff --cached --name-only --diff-filter=ACMRD 2>/dev/null)
fi

# ⚠️ ACMRD, with D. A commit that DELETES a test is exactly the case
# non-negotiable 2 names, and it must still summon a test reviewer.
testable=0
TESTABLE_PATHS=""
while IFS= read -r p; do
  [ -n "$p" ] || continue
  case "$p" in
    */src/test/*|*/src/main/*|*Test.java|*.java|*.kt) ;;
    scripts/*.sh|scripts/*.py|*.gradle.kts|.pre-commit-config.yaml) ;;
    .github/workflows/*) ;;
    *) continue ;;
  esac
  # ⚠️ NO `break`. It used to stop at the first testable path, which was enough
  # to answer "both roles?" but not "is every one of them prose?" -- and a diff
  # whose first file was a comment-only .java would have hidden a changed script
  # behind it. Measured as a one-role routing for a diff that edited a shell
  # script, before this loop was made to see the whole diff.
  testable=1
  TESTABLE_PATHS="$TESTABLE_PATHS$p
"
done <<EOF
$PATHS
EOF

# ⚠️ CONTENT, AFTER PATH (M0.80). A `.java` diff whose every edit is a comment
# gives a test-reviewer nothing to mutate -- the same bind docs-only diffs were
# in, one level down. `diff_shape.py` answers `prose` only when EVERY changed
# Java file is identical to its committed version once comments are dropped, and
# `executable` for everything it cannot decide, so a broken or missing script
# keeps both roles rather than buying a one-role commit.
SHAPE=$(printf '%s' "$TESTABLE_PATHS" | python3 scripts/diff_shape.py 2>/dev/null || echo executable)
if [ "$testable" -eq 1 ] && [ "$SHAPE" = "prose" ]; then
  echo "reviewer"
  echo "# reviewer only: every Java edit in this diff is a comment" >&2
elif [ "$testable" -eq 1 ]; then
  echo "reviewer"
  echo "test-reviewer"
  echo "# both: the diff touches code, tests, or executable enforcement" >&2
else
  echo "reviewer"
  echo "# reviewer only: no test-eligible file in this diff" >&2
fi
