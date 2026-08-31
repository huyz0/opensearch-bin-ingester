#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# The harness has its own tests, and they must actually run.
#
# `buildSrc` holds the Java-test parser both TDD gates depend on and the licence
# task. ⚠️ Gradle 8+ treats buildSrc as an included build and runs only what is
# needed to produce the plugin jar, so `./gradlew build` does NOT run its tests --
# verified: a deliberately wrong expectation left the root build green. A test
# suite nothing runs is not a gate.
#
# Scoped by GATE_SCOPE so the JVM cost is paid only when it can matter: the
# parser and the licence task are what these tests constrain.
source "$(dirname "$0")/lib.sh"
cd "$ROOT"
hdr "check-harness-tests"

[ -x ./gradlew ] || { warn "no build yet -- harness tests unenforced"; finish; }

if [ "$GATE_SCOPE" != "full" ]; then
  # ⚠️ settings.gradle.kts is here because three suites take it as their
  # SUBJECT -- RuleListTest, ModulePlanTest and ModuleListDerivationTest all
  # assert against the module list derived from it. Without it, renaming a
  # module ran no test at all and printed "no harness change".
  relevant=$(changed_files 'scripts/*.py' 'scripts/*.sh' 'buildSrc/*' 'settings.gradle.kts' | head -1)
  if [ -z "$relevant" ]; then
    ok "no harness change; suite not run$(scope_note)"
    finish
  fi
fi

# ⚠️ mkdir BEFORE the redirect. .harness/ is gitignored, so a fresh checkout
# does not have it, and bash evaluates the redirect before running the command:
# Gradle never started, and this gate reported "harness tests failed -- see
# .harness/harness-tests.log" about a log that did not exist. That is the exact
# inversion of the "reported 0 tests -- it did not run" antidote five lines
# below. check-module.sh already does this; this script runs FIRST and did not.
mkdir -p .harness
if ./gradlew -p buildSrc test --console=plain -q > .harness/harness-tests.log 2>&1; then
  n=$(python3 - <<'PY'
import glob, xml.etree.ElementTree as ET
print(sum(int(ET.parse(x).getroot().get('tests', 0))
          for x in glob.glob('buildSrc/build/test-results/test/TEST-*.xml')))
PY
)
  [ "${n:-0}" -gt 0 ] || { fail "the harness suite reported 0 tests -- it did not run"; finish; }
  ok "$n harness test(s) pass"
else
  fail "harness tests failed -- see .harness/harness-tests.log"
  grep -E "expected|actual|FAILED|AssertionError" .harness/harness-tests.log | head -6 | sed 's/^/           /'
fi
finish
