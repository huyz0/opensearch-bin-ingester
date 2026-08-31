#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# testing.md rule 2: record that new tests were RUN and OBSERVED TO FAIL, before
# the production code exists.
#
#   scripts/tdd-red.sh <fully.qualified.TestClass#method>...
#
# It runs the named tests, reads the JUnit XML the run produced, and records
# ONLY the ids that actually appear there as failed.
#
# It deliberately does NOT treat "gradle exited non-zero" as red. A compile
# error, an unresolvable selector, a missing task and a daemon crash all exit
# non-zero, and every one of them would otherwise mint a red record for a test
# that never ran. "The build failed" and "this test failed" are different facts.
source "$(dirname "$0")/lib.sh"
cd "$ROOT"
hdr "tdd-red"
[ $# -gt 0 ] || { echo "usage: tdd-red.sh <fully.qualified.TestClass#method>..." >&2; exit 2; }

OUT=.harness/tdd; mkdir -p "$OUT"
if [ ! -x ./gradlew ]; then
  fail "no ./gradlew -- run this from the repository root"
  finish
fi

# Which Gradle task runs each id depends on the source set its file lives in.
# check-tdd.sh demands a red record for every tier, so a runner that only knows
# `test` would deadlock every T3 and T4 test in a milestone's plan: no way to
# record one honestly, and the forgeable red.json waiting right there.
PLAN="$OUT/plan.txt"
python3 scripts/tdd_scan.py plan "$@" > "$PLAN" || { cat "$PLAN"; FAILED=1; finish; }

# One Gradle invocation PER SELECTOR. A parameterized invocation appears in the
# JUnit XML as `[1] arg, arg` with no method name, so results can only be
# attributed to an id if that id was the only thing selected.
RC=0
while read -r task sel; do
  [ -n "$task" ] || continue
  ident="${sel%.*}#${sel##*.}"
  find . -path '*/build/test-results/*' -name 'TEST-*.xml' -delete 2>/dev/null || true
  case "$task" in
    buildSrc:*) GRADLE=(./gradlew -p buildSrc "${task#buildSrc:}") ;;
    *)          GRADLE=(./gradlew "$task") ;;
  esac
  echo "         ${GRADLE[*]} --tests $sel"
  "${GRADLE[@]}" --tests "$sel" >> "$OUT/red.log" 2>&1
  python3 scripts/tdd_scan.py record-one "$OUT" "$ident" || RC=1
done < "$PLAN"
[ "$RC" -eq 0 ] || FAILED=1
finish
