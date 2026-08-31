#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# skills/README.md rules 2, 3, 5: skills are vendor-neutral and adapters are thin.
source "$(dirname "$0")/lib.sh"
cd "$ROOT"
hdr "check-portability"

for d in .agents/skills/*/; do
  name="$(basename "$d")"
  f="$d/SKILL.md"
  [ -f "$f" ] || { fail "$name: no SKILL.md"; continue; }
  head -1 "$f" | grep -q '^---$' || fail "$name: SKILL.md does not begin with YAML front matter"
  fname=$(awk -F'name: ' '/^name: /{print $2; exit}' "$f")
  [ "$fname" = "$name" ] || fail "$name: frontmatter name is '$fname', must equal the directory name"
  grep -q '^description: ' "$f" || fail "$name: no description in frontmatter"
  desc=$(awk -F'description: ' '/^description: /{print $2; exit}' "$f")
  # A trigger, not a title. "Use when/before/after/at/whenever ..." all qualify.
  printf '%s' "$desc" | grep -qiE 'use (when|before|after|at|whenever|during)' \
    || fail "$name: description has no 'Use <when|before|after|at|whenever> <trigger>' clause -- layer 1 is the whole mechanism"
  # rule 3: no vendor syntax
  grep -nE '^@[A-Za-z._/]' "$f" >/dev/null 2>&1 && fail "$name: SKILL.md contains a vendor @import"
  # rule 1: adapter must exist
  [ -f ".claude/commands/$name.md" ] || fail "$name: no .claude/commands/$name.md adapter"
done

grep -nE '^@[A-Za-z._/]' AGENTS.md >/dev/null 2>&1 && fail "AGENTS.md contains a vendor @import; that belongs in CLAUDE.md"

for c in .claude/commands/*.md; do
  n="$(basename "$c" .md)"
  [ -d ".agents/skills/$n" ] || fail "$c: adapter for a skill that does not exist"
  grep -q ".agents/skills/$n/SKILL.md" "$c" || fail "$c: does not point at its skill"
  lines=$(wc -l < "$c")
  [ "$lines" -le 20 ] || fail "$c: $lines lines -- an adapter is a pointer, not a procedure"
done

[ "$FAILED" -eq 0 ] && ok "skills are portable and adapters are thin"
finish
