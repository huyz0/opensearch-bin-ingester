#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# M8.41: an override entry's round count and finding ids are diffed against the
# verdict store. review/overrides.md records that a hard rule was exceeded, and
# was the one such artefact with no gate; M8.4's entry misstated its own record
# three times in three rounds.
#
# ⚠️ DELTA ONLY, AND ONLY THE ENTRIES THE STAGED DIFF ADDS OR CHANGES. The
# verdicts are read from both the local .harness store and committed
# review/verdicts/, so an entry with no record in either is reported UNJUDGED,
# never passed in silence.
# ⚠️ A FRESH CI CHECKOUT HAS NO STAGED DIFF, so this delta check validates no
# entries there. That is distinct from check-reviewed's need for local verdicts;
# committed verdicts can be used here when a changed entry is actually selected.
source "$(dirname "$0")/lib.sh"
cd "$ROOT"
hdr "check-override"

FILE="review/overrides.md"
# ⚠️ THE NEW-SIDE LINE NUMBERS THE STAGED DIFF TOUCHES, from its hunk headers,
# so an edit to ANY line of an entry judges that entry -- not only an edit to
# the line that names the task. A pure deletion marks the line it happened at.
lines=$(git diff --cached -U0 -- "$FILE" 2>/dev/null \
  | sed -nE 's/^@@ -[0-9,]+ \+([0-9]+)(,([0-9]+))? @@.*/\1 \3/p' \
  | while read -r start count; do
      count=${count:-1}
      if [ "$count" -eq 0 ]; then echo "$start"; echo "$((start + 1))"; fi
      i=0; while [ "$i" -lt "$count" ]; do echo "$((start + i))"; i=$((i + 1)); done
    done | sort -un | paste -sd, -)
if [ -z "$lines" ]; then
  ok "no override entry added or changed"
  finish
fi

# ⚠️ BOTH STORES: the committed review/verdicts/<task>/ holds rounds the local
# one does not, and judging against one alone refuses true entries.
out=$(python3 scripts/override_check.py check "$FILE" .harness/review:review/verdicts "$lines")
status=$?
[ -n "$out" ] && printf '%s\n' "$out" | sed 's/^/         /'
if [ "$status" -ne 0 ]; then
  fail "an override entry misstates the verdicts recorded for its task"
  echo "         Correct the entry from .harness/review/ and review/verdicts/, the record it exists to be."
  finish
fi
ok "the changed override entries are consistent with the recorded verdicts"
finish
