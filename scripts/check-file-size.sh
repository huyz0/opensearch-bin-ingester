#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# code-structure.md: a source file is at most 500 lines.
source "$(dirname "$0")/lib.sh"
cd "$ROOT"
hdr "check-file-size"
LIMIT=500
count=0
while IFS= read -r f; do
  [ -f "$f" ] || continue
  count=$((count+1))
  n=$(wc -l < "$f")
  [ "$n" -le "$LIMIT" ] || fail "$f: $n lines (limit $LIMIT) -- split it, do not raise the limit"
done < <(scoped_files '*.java' '*.sh' '*.py' '*.gradle.kts' '*.kt')
[ "$FAILED" -eq 0 ] && ok "$count source file(s) within $LIMIT lines$(scope_note)"
finish
