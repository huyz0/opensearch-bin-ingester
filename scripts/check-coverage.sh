#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# testing.md rule 6: 95% line / 90% branch, per module, from JaCoCo's XML.
#
# ⚠️ A module with compiled classes and no report is a FAILURE, not a skip, and
# "no module has classes yet" is reported as NOT-MEASURED rather than ok. The
# characteristic failure of a threshold gate is not a wrong threshold -- it is
# reporting success while measuring nothing.
source "$(dirname "$0")/lib.sh"
cd "$ROOT"
hdr "check-coverage"
[ -x ./gradlew ] || { warn "no build yet -- coverage unenforced"; finish; }
# ⚠️ The gate GENERATES the report it reads. Two defects made this necessary,
# both found by review running the gate rather than reading it:
#
#   - nothing wired `jacocoTestReport` into the build. `./gradlew check` runs
#     `test` and stops, so the report existed only if someone had run an
#     undocumented manual step
#   - so the gate checked that a report EXISTED, never that it described the
#     classes on disk now. Reproduced: compile a second, entirely untested class
#     without regenerating, and the gate still reported ok 100.0%. A single
#     historical run could leave a module permanently green
#
# check-harness-tests.sh already sets the precedent of a gate invoking Gradle
# rather than trusting build output it did not produce.
# ⚠️ NOT delta-scoped, deliberately, and this is a deferred optimisation rather
# than an oversight. A delta path was written and REMOVED under review.md rule
# 12: every test here runs at GATE_SCOPE=full, so the branch never executed under
# test and reverting its `--diff-filter=ACMRD` (which is what makes a deleted
# test visible) left the whole suite green. An untested fast path on a gate is
# how a gate stops measuring without anyone noticing. M0.39 restores it WITH
# tests for both directions. Until then this gate always measures, which costs a
# Gradle run and is the safe direction to be wrong in.
mkdir -p .harness
# ⚠️ `test` AND `jacocoTestReport`, not the report alone.
#
# The first attempt ran only `jacocoTestReport` and relied on
# `mustRunAfter(tasks.withType<Test>())` in the conventions plugin. That was
# wrong: mustRunAfter only ORDERS tasks already in the graph -- it never puts
# `test` INTO it. So the report was regenerated from whatever build/jacoco/
# test.exec happened to be on disk. Review reproduced the consequence: weaken an
# existing test to a no-op WITHOUT re-running `test`, and Gradle prints
# `:jacocoTestReport UP-TO-DATE` while coverage still reads 100%.
#
# That earlier fix only appeared to work because the case used to demonstrate it
# was a brand-new class, whose bytecode has no prior exec data. JaCoCo's
# staleness check is keyed to main-class bytecode, not to test sources, so
# weakening or deleting a TEST leaves the report silently unchanged -- the same
# "reports success while measuring nothing" defect, one layer down.
if ! ./gradlew test jacocoTestReport --console=plain -q > .harness/coverage-report.log 2>&1; then
  fail "could not generate the JaCoCo reports -- coverage NOT measured"
  tail -5 .harness/coverage-report.log | sed 's/^/           /'
  finish
fi
python3 scripts/coverage.py || FAILED=1
finish
