#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# build.md: every source file carries an SPDX header. Retrofitting headers to a
# grown tree is tedious; from the first commit it is free.
source "$(dirname "$0")/lib.sh"
cd "$ROOT"
hdr "check-license-headers"
SPDX="SPDX-License-Identifier: Apache-2.0"
missing=0; checked=0
while IFS= read -r f; do
  [ -f "$f" ] || continue
  checked=$((checked+1))
  head -8 "$f" | grep -qF "$SPDX" || { fail "$f: no '$SPDX' in the first 8 lines"; missing=$((missing+1)); }
# Shell and Python are source too. check-file-size.sh already treats *.sh as
# source; leaving them out here meant the 19 scripts that ARE the enforcement
# harness were the only files in the tree exempt from the licence rule.
done < <(scoped_files '*.java' '*.gradle' '*.gradle.kts' '*.kt' '*.sh' '*.py')
[ "$missing" -eq 0 ] && ok "$checked source file(s) carry the SPDX header$(scope_note)"
finish
