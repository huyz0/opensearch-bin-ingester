#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# observability.md rule 1: the metric label allow-list is closed.
# A high-cardinality label is a standards violation with a number attached:
# `index` costs 200,000 series, `tenant` costs 100 million.
source "$(dirname "$0")/lib.sh"
cd "$ROOT"
hdr "check-metric-cardinality"
FILES=$(scoped_files '*.java')
if [ -z "$FILES" ]; then
  ok "no Java sources yet (build lands in M0.4)"
  finish
fi
# metric/span label registration sites naming a forbidden dimension
BAD='"(index|indexName|partition|stream|streamId|tenant|tenantId|routing|objectKey|segmentKey|offset|docId|_id)"'
hits=$(grep -rnE "(tag|label|attribute|withTag|addTag|put)\s*\(\s*$BAD" $FILES 2>/dev/null || true)
if [ -n "$hits" ]; then
  n=$(printf '%s\n' "$hits" | wc -l | tr -d ' ')
  fail "$n high-cardinality metric/span label(s) -- observability.md rule 1"
  printf '%s\n' "$hits" | head -5 | sed 's/^/           /'
  echo "         Count it in memory and expose it via top-K logs or /admin/cost instead."
  finish
fi
ok "no forbidden metric labels$(scope_note)"
finish
