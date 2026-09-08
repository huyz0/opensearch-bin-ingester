#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# build.md: the configured memory limits must SUM below the per-session ceiling,
# and every container must declare one.
#
# WSL2 does not kill the offending process, it kills whatever it likes, so a
# runaway has to die as a JVM OutOfMemoryError or a Docker OOM-kill. That only
# works if the limits are actually set AND actually add up -- the previous
# version grepped for the presence of `-Xmx` without reading its value, so
# `-Xmx12g` passed while the script printed "ceiling 6144 MiB".
source "$(dirname "$0")/lib.sh"
cd "$ROOT"
hdr "check-test-budget"

if [ ! -f gradle.properties ]; then
  warn "no gradle.properties yet -- budget unenforced"
  finish
fi
python3 scripts/test_budget.py || FAILED=1
finish
