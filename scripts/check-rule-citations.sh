#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# A rule cited by NUMBER names nothing, and goes stale silently when a rule is
# inserted above it.
#
# ⚠️ "review.md rule 12" appears in scripts, skills, tests and backlog rows, and
# resolving it means loading a 14.8 KB standard to learn which rule that is. It is
# the defect AGENTS.md already forbids one level out -- never report to a person
# in bare task IDs -- applied to rules instead of tasks. pstore, the harness this
# one was distilled from, made it a non-negotiable after the same thing bit it:
# "the numbering is for reading order and shifts when a rule is added -- a
# numbered cross-reference in another file goes stale silently. It already did
# once."
#
# ⚠️ THE NUMBERING IS ALREADY UNDER STRAIN HERE: review.md carries rules 1a, 1b
# and 3a, which exist because inserting a rule would have renumbered every
# citation in the tree. A name has no such cost.
#
# Two predicates, and the second is what makes the first worth having:
#   * a numeric citation is refused;
#   * a NAMED citation must resolve to a rule that exists, or a name is just a
#     number with better spelling.
#
# ⚠️ THE ARCHIVE IS NOT JUDGED. backlog-done.md and backlog-notes.md hold rows
# verbatim as they were written. Rewriting them to satisfy a citation style would
# make this gate an instruction to edit the record, and nothing reads them as
# instructions.
source "$(dirname "$0")/lib.sh"
cd "$ROOT"
hdr "check-rule-citations"

STANDARD=docs/internal/standards/review.md
[ -f "$STANDARD" ] || { ok "no $STANDARD"; finish; }

# The rule names, derived from the standard itself -- rung 2, so adding a rule
# needs no edit here.
# ⚠️ Anchored on the CLOSING `**` of the statement, not on the line the rule
# opens. Several rules wrap, so their slug lands on the second or third line and a
# line-anchored pattern silently misses them -- which would make two real names
# unresolvable and refuse every correct citation of them.
NAMES=$(grep -oE '\*\* \(`[a-z0-9-]+`\)' "$STANDARD" | grep -oE '`[a-z0-9-]+`' | tr -d '`')
[ -n "$NAMES" ] || { fail "$STANDARD names no rules -- expected \`slug\` after each bold statement"; finish; }

FILES=$(workspace_files '*.md' '*.sh' '*.py' '*.java' '*.yaml' '*.yml' \
        | grep -v '^docs/internal/product/backlog-done\.md$' \
        | grep -v '^docs/internal/product/backlog-notes\.md$' \
        | grep -v '^scripts/check-rule-citations\.sh$' \
        | grep -v '^buildSrc/src/test/java/binjava/RuleCitationTest\.java$')
# ⚠️ THE GATE AND ITS TEST ARE BOTH EXEMPT, and the reason is the same for each:
# a check that refuses a string cannot be written, or exercised, without
# containing that string. The cost is stated rather than hidden -- a genuine
# numeric citation in either file is invisible to this gate, and the two files
# are the ones most likely to be read by someone who already knows the rule.

n=0
for f in $FILES; do
  while IFS= read -r hit; do
    [ -n "$hit" ] || continue
    n=$((n + 1))
    fail "$f: '$hit' -- cite the rule by NAME. Resolving a number means loading the standard."
  done < <(grep -oE 'review\.md rules? [0-9]+[a-z]?(-[0-9]+)?' "$f" || true)
  while IFS= read -r name; do
    [ -n "$name" ] || continue
    if ! printf '%s\n' "$NAMES" | grep -qx "$name"; then
      n=$((n + 1))
      fail "$f: 'review.md rule $name' resolves to no rule in $STANDARD"
    fi
  done < <(grep -oE 'review\.md rules? [a-z][a-z0-9-]+' "$f" | sed -E 's/.*rules? //' || true)
done

[ "$n" -eq 0 ] && ok "every review.md citation names a rule that exists$(scope_note)"
finish
