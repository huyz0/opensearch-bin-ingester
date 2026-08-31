#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# build.md: a suite that exceeds its budget fails. A slow suite stops being run,
# and a suite that is not run is not a gate.
#   check-suite-time.sh <layer> <seconds>   -- record and assert
source "$(dirname "$0")/lib.sh"
cd "$ROOT"
LAYER="${1:-}"; SECS="${2:-}"
[ -n "$LAYER" ] && [ -n "$SECS" ] || { echo "usage: check-suite-time.sh <L0|L1|L2|L3> <seconds>" >&2; exit 2; }
hdr "check-suite-time $LAYER"
case "$LAYER" in
  L0) BUDGET=90  ;; L1) BUDGET=300 ;; L2) BUDGET=600 ;; L3) BUDGET=900 ;;
  *)  echo "unknown layer $LAYER" >&2; exit 2 ;;
esac
mkdir -p .harness/timing
printf '%s %s\n' "$(date -u +%FT%TZ)" "$SECS" >> ".harness/timing/$LAYER.log"
if [ "${SECS%.*}" -gt "$BUDGET" ]; then
  fail "$LAYER took ${SECS}s, budget ${BUDGET}s"
  echo "         Split it, move a test to a slower layer, or argue the budget up in build.md."
  echo "         Do NOT silently accept it: the suites people stop running are the slow ones."
  finish
fi
ok "$LAYER ${SECS}s within ${BUDGET}s"
finish
