#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# M5.56: every test method a `src/main` javadoc names is a test method that
# exists, and is one a reader can run.
#
# ⚠️ MEASURED, which is why this exists. M5.34's argument for citing a fixture
# by name rather than tallying one is that A NAME IS CHECKABLE -- and nothing
# checked it. Three of that row's four names were split across `{@code}` spans
# and grepped nowhere until review caught them by hand; a fourth instance in
# `SubscriptionHub` was found by the prototype of this gate, unprompted, in the
# same run. A citation that resolves for no reader is worse than the count it
# replaced, because it reads as evidence.
#
# ⚠️ ALWAYS FULL SCOPE, and it offers no delta mode. A citation goes dangling
# when a TEST is renamed or deleted, and that change touches no `src/main` file
# at all -- so the file carrying the broken citation is never in the diff that
# broke it, and a delta scope is structurally blind to exactly the change this
# exists to catch. check-adr-refs.sh never leaves full scope for the same
# reason.
#
# ⚠️ NO EXEMPTION PATH, DELIBERATELY: no allow-list, no baseline, no inline
# marker. The gate reads every tracked-or-staged `src/main` file INCLUDING any
# that documents this gate. An escape hatch here would be an escape hatch from
# the one rule the gate has.
#
# ⚠️ WHAT IT COVERS IS THE FOUR SHAPES LISTED IN `javadoc_cites.py`, NOT "every
# citation". An earlier draft of this header said a tree may not quote a
# citation it cannot resolve, and review falsified it with a live instance the
# gate walked past -- a bare class name, which that draft read only when a
# method followed it. The blind spot that remains is stated there rather than
# here, so there is one copy of it.
source "$(dirname "$0")/lib.sh"
cd "$ROOT"
hdr "check-javadoc-cites"

FILES=$(workspace_files '*.java')
if [ -z "$FILES" ]; then
  ok "no java files to read"
  finish
fi

# ⚠️ THE FILE LIST COMES FROM `workspace_files` AND IS PIPED IN, never walked
# inside python. That is check-gate-scope.sh's rule and it is the reason a
# reference clone under .tmp/ or a sibling checkout cannot reach this gate.
OUT=$(printf '%s\n' "$FILES" | python3 scripts/javadoc_cites.py)
RC=$?

SUMMARY=$(printf '%s\n' "$OUT" | grep '^__SUMMARY__ ' | tail -1)
# ⚠️ A MISSING SUMMARY IS A FAILURE, NOT A PASS. python3 exiting 127 gives an
# empty OUT and an exit status this script would otherwise read as "nothing
# dangling" -- the green-line-over-nothing shape non-negotiable 4 is about.
# check-fault-store-records.sh records the same trap being hit.
if [ -z "$SUMMARY" ]; then
  fail "the predicate did not run -- python3 produced no summary (exit $RC)"
  printf '%s\n' "$OUT" | head -5 | sed 's/^/           /'
  finish
fi
set -- $SUMMARY
CITED=$2; BAD=$3; CLASSES=$4; METHODS=$5

printf '%s\n' "$OUT" | grep -v '^__SUMMARY__ '

if [ "$BAD" -lt 0 ]; then
  fail "a test file could not be parsed, so no citation can be resolved against it"
  finish
fi
if [ "$BAD" -gt 0 ]; then
  fail "$BAD of $CITED citation(s) name no test that exists, or are split across lines"
  finish
fi

ok "$CITED citation(s) all resolve, against $METHODS test method(s) in $CLASSES class(es) (scope: full)"
finish
