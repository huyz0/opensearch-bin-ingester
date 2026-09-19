#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# M0.14 / M9 criterion 1: 80% of the mutants on the CHANGED LINES killed.
#
# Drives jzap's `mutationTestDiff` (ADR-0045, M0.113) for every module whose
# src/main Java this change touches, then scores the reports with
# scripts/mutants.py. Coverage is a floor -- a line can execute without being
# constrained -- and this is the number that says whether the tests constrain
# anything (testing.md rule 8).
#
# ---------------------------------------------------------------------------
# ⚠️ IT IS A `manual`-STAGE HOOK, NOT A BLOCKING PRE-COMMIT ONE, AND THAT IS A
# DECISION ABOUT COST RATHER THAN ABOUT IMPORTANCE.
#
# MEASURED on this rig before choosing: `./gradlew :format:mutationTestDiff`
# over a two-line change took 42 s wall, of which 15 s was the coverage pass
# over :format's 390 tests -- and it is the SMALLEST module. build.md's L0
# budget is 90 s for the WHOLE pre-commit set, which the text gates already
# spend most of. A change touching two modules would blow it on its own.
#
# So it is wired in .pre-commit-config.yaml with `stages: [manual]`: declared
# in the one file that defines this project's gates, never fired by an ordinary
# `git commit`, and run by name --
#
#     pre-commit run --hook-stage manual check-mutants
#     scripts/check-mutants.sh
#
# ⚠️ WHAT ACTUALLY INVOKES IT TODAY IS ONE LINE IN ONE SKILL:
# .agents/skills/tdd/SKILL.md § "Before you call it done" names this script at
# the point a commit is offered for review. That is rung 7 of the gate-design
# ladder -- an instruction in a prompt, the weakest enforcement there is -- and
# it is named here rather than implied, because a manual-stage hook that no
# procedure names is a gate nobody runs.
#
# ⚠️ CI DOES NOT RUN IT YET. The L1 job that would (with CHECK_RANGE naming the
# push or pull-request base, read below as JZAP_FROM) is M0.27, an OPEN backlog
# row. The CHECK_RANGE path here exists so that row is a wiring change rather
# than a rewrite; until it lands, this gate is local-only, like check-tdd and
# check-reviewed.
#
# Wiring a 40-second-per-module gate into every commit is how a gate gets
# switched off, and a gate nobody can afford to run enforces nothing. Saying so
# is worth more than a green line in a table.
#
# ---------------------------------------------------------------------------
# ⚠️ ALWAYS DIFF-SCOPED. `GATE_SCOPE=full` does not widen it, and this is the
# one gate where that is not a weakness: the rule IS "80% on the changed
# lines" (tdd/SKILL.md), not on the tree. A whole-tree pass is a different
# task (`./gradlew mutationTestAll`) with a different meaning and an unbounded
# cost, and reporting it here would answer a question nobody asked. The mode
# line says `diff` rather than `delta` so it is never read as the cheap half of
# a gate that also has a full half.
#
# ⚠️ WHAT IT CANNOT SEE. jzap's default base is `HEAD..-Local-` -- staged AND
# unstaged work -- so a run here scores the working tree, which is a superset
# of what `git commit` would record. In CI, CHECK_RANGE sets JZAP_FROM so the
# base is the push or PR base instead. It sees nothing about a commit made with
# --no-verify, and nothing about modules whose Java did not change.
#
# ---------------------------------------------------------------------------
# baselines/mutants.txt -- recorded survivors.
#
#   <mutant key>   <reason: what unblocks this entry>
#
# One line per mutant, keyed exactly as jzap keys it:
#
#   binjava.format.RunCommit::lastOffset()J::33::MATH#0   equivalent mutant on a
#   defensive bound; the branch has no observable behaviour. M8.62
#
# An entry EXCLUDES that mutant from both halves of the score. Three properties
# keep it from becoming a place survivors go to die:
#
#   1. A BARE KEY IS REFUSED. The reason is required and names what unblocks
#      the entry -- the test that would kill the mutant, or the backlog row
#      that writes it (testing.md rule 7).
#   2. AN ENTRY WHOSE MUTANT IS NOW KILLED FAILS THE GATE, even at 100%. Left
#      in place it would swallow the next survivor to land on that key without
#      anyone deciding to swallow it.
#   3. THE KEY CARRIES THE LINE NUMBER AND THE MUTATOR ORDINAL, so an edit to
#      the method surfaces its survivors again rather than inheriting the old
#      excuse.
#
# ⚠️ Non-negotiable 2: the 80% floor and this file both move in one direction
# only. Baselining a survivor instead of killing it is a decision a reviewer
# reads in the diff, which is the point of it being a committed file.
source "$(dirname "$0")/lib.sh"
cd "$ROOT"
hdr "check-mutants"

[ -x ./gradlew ] || { warn "no build yet -- mutation testing unenforced"; finish; }

# Production Java only. A test-only or docs-only change has nothing to mutate,
# and must pay NO Gradle startup -- the same rule check-module.sh follows.
CHANGED=$(changed_files '*/src/main/*.java')
if [ -z "$CHANGED" ]; then
  ok "no src/main Java in this change; nothing to mutate (diff-scoped)"
  finish
fi

# Modules, git-derived. ⚠️ The module list comes from tracked build files, not
# from the changed paths alone: a stray `foo/src/main/java` outside any Gradle
# module would otherwise mint a `:foo:mutationTestDiff` that does not exist and
# fail the gate for a reason that is not about tests.
#
# ⚠️ buildSrc is excluded BY NAME and with a reason: it is an included build,
# `./gradlew :buildSrc:...` from the root does not address it, and the
# conventions plugin that applies jzap is the thing buildSrc BUILDS -- so it
# has no mutationTestDiff task to run. Its own suite is the one
# check-harness-tests.sh runs.
MODULES=$(git ls-files '*/build.gradle.kts' 2>/dev/null \
          | awk -F/ 'NF==2 && $1!="buildSrc" {print $1}' | sort -u)
TARGETS=$(printf '%s\n' "$CHANGED" | awk -F/ '{print $1}' | sort -u \
          | grep -Fx -f <(printf '%s\n' "$MODULES") || true)
if [ -z "$TARGETS" ]; then
  ok "no Gradle module's src/main Java changed; nothing to mutate (diff-scoped)"
  finish
fi

TASKS=$(printf '%s\n' "$TARGETS" | sed 's|^|:|; s|$|:mutationTestDiff|')
echo "         diff-scoped: $(printf '%s ' $TASKS)"

mkdir -p .harness
# CI has no `-Local-` to diff against; CHECK_RANGE names the push or PR base
# and jzap reads it from JZAP_FROM. ⚠️ Without this, a CI run would diff
# HEAD..-Local- in a fresh checkout -- an EMPTY diff -- and print NOT-MEASURED
# on every push while looking green.
[ -n "${CHECK_RANGE:-}" ] && export JZAP_FROM="$CHECK_RANGE"

# ⚠️ THE GATE GENERATES THE REPORT IT READS, AND DELETES THE OLD ONE FIRST.
# Generating it is not enough on its own: `mutationTestDiff`'s scope depends on
# git state that is not a declared task input (jzap says so in the task's own
# up-to-date comment), so Gradle may report UP-TO-DATE and leave the PREVIOUS
# diff's report on disk -- a gate scoring one change against another change's
# mutants. Deleting it makes that state unrepresentable (gate-design rung 1):
# after the run the report is either this run's or absent, and absent is a
# failure four lines further down.
for _m in $TARGETS; do rm -f "$_m/build/reports/jzap-diff/jzap-result.json"; done

if ! ./gradlew $TASKS --console=plain -q > .harness/mutants.log 2>&1; then
  fail "mutationTestDiff did not complete -- mutation NOT measured"
  tail -5 .harness/mutants.log | sed 's/^/           /'
  finish
fi

python3 scripts/mutants.py $TARGETS || FAILED=1
finish
