#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# M8.25, M8 criterion 16: every entry of M8's unwired set is WIRED by its own
# predicate, or OWNED by a backlog row that is not done.
#
# ⚠️ THE LIST IS THE TABLE IN M8's SPEC § "The unwired set", and this script
# carries no copy of it: a copy in a file no spec reviewer opens is where this
# set went stale three times inside M8.0's own review. The grammar of its third
# column is in wired_scan.py.
#
# ⚠️ ALWAYS FULL, never delta: whether a mechanism is wired is a fact about
# the whole tree, and a change that deletes the one construction touches only
# the file it deleted it from.
source "$(dirname "$0")/lib.sh"
cd "$ROOT"
hdr "check-wired"

SPEC="docs/internal/product/milestones/M8/SPEC.md"
out=$(workspace_files '*.java' | python3 scripts/wired_scan.py "$SPEC" docs/internal/product/backlog.md)
status=$?
[ -n "$out" ] && printf '%s\n' "$out" | sed 's/^/         /'
if [ "$status" -ne 0 ]; then
  fail "an entry of the unwired set is neither wired nor owned by an open row"
  echo "         Wire it, name an open backlog row in the table's fourth column, or"
  echo "         fix a cell the scan cannot read -- in $SPEC, the only copy."
  finish
fi
ok "every entry of the unwired set is wired or owned (full tree)"
finish
