#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# testing.md rule 9: 80% of mutants killed on CHANGED production code (M0.14).
#
# ⚠️ WHY THIS EXISTS RATHER THAN AN AGENT BEING ASKED TO CHECK IT: mutation
# survival is a predicate over the tree, so gate-design puts it at rung 3. It was
# rung 6 for four milestones, and it cost review rounds -- M4.10c spent two on
# mutants PIT generates by default (a guard deleted from a compact constructor,
# a method call replaced by a constant).
#
# ⚠️ WHAT IT CANNOT SEE, and this is the reason the test reviewer does not go
# away: PIT generates SYNTACTIC mutants. The findings that cost the most rounds
# in M4.10b-d were SEMANTIC -- "hoist the attribution to once per delta", "take
# the incarnation from requests.get(0)", "observe(requests, 0L)". No mutator
# generates those, because they require knowing what the design means.
source "$(dirname "$0")/lib.sh"
cd "$ROOT"
hdr "check-mutants"
[ -x ./gradlew ] || { warn "no build yet -- mutation unenforced"; finish; }

FLOOR="${MUTANT_FLOOR:-80}"
TARGETS=".harness/mutants-targets.txt"
mkdir -p .harness
: > "$TARGETS"

# Changed PRODUCTION sources only. A test's own mutants say nothing about
# whether the test constrains anything.
MODULES=""
while IFS= read -r f; do
  [ -n "$f" ] || continue
  case "$f" in
    */src/main/java/*) ;;
    *) continue ;;
  esac
  mod="${f%%/*}"
  fqcn="${f#*/src/main/java/}"
  fqcn="${fqcn%.java}"
  fqcn="${fqcn//\//.}"
  printf '%s\n' "$fqcn" >> "$TARGETS"
  case " $MODULES " in *" $mod "*) ;; *) MODULES="$MODULES $mod" ;; esac
done <<EOF
$(scoped_files '*.java')
EOF

if [ ! -s "$TARGETS" ]; then
  # ⚠️ NOT `ok`. Nothing mutable changed is a legitimate outcome, but it is not
  # evidence that anything was measured, and a gate that says ok here trains the
  # eye to read ok as "the tests constrain the code".
  warn "no production class changed -- mutation NOT MEASURED$(scope_note)"
  finish
fi

REPORTS=""
for mod in $MODULES; do
  # ⚠️ ONE INVOCATION PER MODULE, scoped by -PmutantTargets to the classes this
  # change touched. Unscoped, PIT mutates the whole module and the run costs
  # minutes instead of seconds -- measured, 132 mutants over three classes in
  # 14.6s against a module-wide run an order of magnitude larger.
  csv="$(paste -sd, < "$TARGETS")"
  rm -f "$mod/build/reports/pitest/mutations.xml"
  ./gradlew ":$mod:pitest" "-PmutantTargets=$csv" -q > .harness/mutants-$mod.log 2>&1 || true
  REPORTS="$REPORTS $mod/build/reports/pitest/mutations.xml"
done

FOUND=""
for r in $REPORTS; do [ -f "$r" ] && FOUND="$FOUND $r"; done
if [ -z "$FOUND" ]; then
  # ⚠️ THE SHAPE THAT MAKES A THRESHOLD GATE GREEN FOREVER. A build that failed
  # writes no XML; treating that as "nothing to judge" reports success having
  # measured nothing. The log is named so the cause is one command away.
  fail "no mutation report was produced -- see .harness/mutants-*.log"
  finish
fi

OUT="$(python3 scripts/mutants.py "$FLOOR" "$TARGETS" $FOUND)"
RC=$?
printf '%s\n' "$OUT" | sed '$d' | grep -v '^$' || true
LAST="$(printf '%s\n' "$OUT" | tail -1)"
case "$RC" in
  0) ok "${LAST#PASS }$(scope_note)" ;;
  2) warn "${LAST#NOT-MEASURED }" ;;
  *) fail "${LAST#FAIL } -- kill them, or argue each in baselines/mutants.txt" ;;
esac
finish
