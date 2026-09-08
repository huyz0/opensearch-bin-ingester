#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# code-structure.md rule 1: a source file is at most 700 lines.
source "$(dirname "$0")/lib.sh"
cd "$ROOT"
hdr "check-file-size"
LIMIT=700
count=0
while IFS= read -r f; do
  [ -f "$f" ] || continue
  count=$((count+1))
  n=$(wc -l < "$f")
  [ "$n" -le "$LIMIT" ] || fail "$f: $n lines (limit $LIMIT) -- split it; raising the cap so one file fits is what non-negotiable 2 forbids"
done < <(scoped_files '*.java' '*.sh' '*.py' '*.gradle.kts' '*.kt')
[ "$FAILED" -eq 0 ] && ok "$count source file(s) within $LIMIT lines$(scope_note)"
finish
