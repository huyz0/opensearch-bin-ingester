#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Every relative markdown link resolves. Cheap, and the corpus is mostly links.
source "$(dirname "$0")/lib.sh"
cd "$ROOT"
hdr "check-links"

# ⚠️ Delta mode is only sound while link TARGETS are stable. If this change
# deletes or renames a file, a link in some file it did not touch may now be
# broken, and only a full pass finds it -- so the gate escalates itself rather
# than reporting a pass it cannot support.
ESCALATED=""
if [ "$GATE_SCOPE" != "full" ]; then
  moved=$( { if [ -n "${CHECK_RANGE:-}" ]; then
               git diff --name-only --diff-filter=DR "$CHECK_RANGE" HEAD
             else
               git diff --cached --name-only --diff-filter=DR
             fi; } 2>/dev/null | head -1)
  [ -n "$moved" ] && ESCALATED="a file was deleted or renamed"
fi
LINK_FILES() {
  if [ "$GATE_SCOPE" = "full" ] || [ -n "$ESCALATED" ]; then
    workspace_files '*.md'
  else
    scoped_files '*.md'
  fi
}
n=0
while IFS= read -r f; do
  d="$(dirname "$f")"
  while IFS= read -r l; do
    [ -z "$l" ] && continue
    case "$l" in http*|mailto*|'#'*) continue ;; esac
    t="${l%%#*}"
    [ -z "$t" ] && continue
    n=$((n+1))
    [ -e "$d/$t" ] || fail "$f -> $t"
  done < <(grep -o '](\([^)]*\.md\)[^)]*)' "$f" 2>/dev/null | sed 's/](\(.*\))/\1/')
done < <(LINK_FILES)
[ "$FAILED" -eq 0 ] && ok "$n relative markdown links resolve$([ -n "$ESCALATED" ] && printf ' (full tree: %s)' "$ESCALATED" || scope_note)"
finish
