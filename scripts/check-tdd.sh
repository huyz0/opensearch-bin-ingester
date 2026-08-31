#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# testing.md rule 2: every NEW test in the staged diff was observed to fail first.
#
# It compares the whole set of test methods before and after, rather than
# parsing diff hunks. Hunk parsing cannot see an annotation and a signature on
# one line, cannot see @ParameterizedTest, and mis-attributes a method to the
# annotation above it -- three ways a new test slips in unredded.
source "$(dirname "$0")/lib.sh"
cd "$ROOT"
hdr "check-tdd"
if [ -z "${CHECK_RANGE:-}" ]; then
  git diff --cached --quiet && { ok "nothing staged"; finish; }
fi
python3 scripts/tdd_scan.py check "${CHECK_RANGE:-}" || FAILED=1
finish
