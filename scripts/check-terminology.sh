#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# glossary.md: one name per concept. Flags deprecated vocabulary in prose and code.
source "$(dirname "$0")/lib.sh"
cd "$ROOT"
hdr "check-terminology"

# term|replacement  -- matched case-insensitively on word boundaries
BAD=(
  "service pod|ingester node"
  "service node|ingester node"
  "the service|the ingester"
  "client library|consumer library"
  "service-side|ingester-side"
  "ingester pod|ingester node"
  "collector|one of the six roles"
)
# NOT banned outright: "agent" and "broker". glossary.md deprecates them as names
# for OUR components, but docs/research/10-prior-art/ describes Kafka, WarpStream
# and AutoMQ, where "broker" is those systems' own correct term -- and "agent"
# names this repository's harness. A gate that cannot tell the two apart would be
# turned off, and a gate that is off enforces nothing.
FILES=$(scoped_files '*.md' '*.java')
# ⚠️ An EMPTY operand list turns `grep -r` into a recursive search of the working
# directory. Staging only a script once made this gate fail the commit over a
# sibling checkout under .tmp/ and its own review packets under .harness/ --
# a gate reading files the project does not own. Guard, never assume non-empty.
if [ -z "$FILES" ]; then
  ok "no markdown or Java in scope$(scope_note)"
  finish
fi

for pair in "${BAD[@]}"; do
  term="${pair%%|*}"; repl="${pair##*|}"
  # skip the glossary itself (it must name the deprecated terms) and ADR filenames
  hits=$(grep -rniE "\b${term}\b" $FILES 2>/dev/null \
         | grep -v '^docs/internal/standards/glossary.md:' \
         | grep -v 'scripts/check-terminology.sh' \
         | grep -v '0004-the-service-serves-reads.md' || true)
  if [ -n "$hits" ]; then
    n=$(printf '%s\n' "$hits" | wc -l | tr -d ' ')
    fail "\"$term\" ($n) -- use \"$repl\" (glossary.md)"
    printf '%s\n' "$hits" | head -3 | sed 's/^/           /'
    [ "$n" -gt 3 ] && echo "           ... and $((n-3)) more"
  fi
done
[ "$FAILED" -eq 0 ] && ok "vocabulary matches glossary.md$(scope_note)"
finish
