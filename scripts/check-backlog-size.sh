#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# backlog.md is read WHOLE at the start of every session, so its size is a
# per-session tax and nothing was collecting it.
#
# ⚠️ MEASURED: 18,638 bytes on 2026-08-31, 315,383 on 2026-09-05 -- monotonic,
# seventeen-fold, five days. 71,149 of those bytes were preamble before the first
# heading; 84,501 were the rows of M1, M2 and M3, all complete; the longest single
# row was 18,845. The file's own first line has said "current milestone only"
# throughout, which is rung 7 of the gate-design ladder -- an instruction in prose
# -- for a rule that is a predicate over the file. This is that predicate.
#
# Three bounds, and what each one is FOR:
#   * A done row belongs in the archive. It is the growth that needs no author.
#   * A row is a SUMMARY. The essay goes to backlog-notes.md, which a session
#     opens for the ONE task it picks rather than for all of them.
#   * The preamble is bounded on its own, because the row rules cannot see it.
#
# ⚠️ THE ARCHIVE IS NOT JUDGED, deliberately. Applying the row cap to rows that
# already shipped would turn a size gate into an instruction to rewrite the
# record, and nothing loads the archive, so its size costs nothing.
source "$(dirname "$0")/lib.sh"
cd "$ROOT"
hdr "check-backlog-size"

BACKLOG=docs/internal/product/backlog.md
: "${BACKLOG_ROW_CAP:=600}"
: "${BACKLOG_PREAMBLE_CAP:=4000}"

[ -f "$BACKLOG" ] || { ok "no $BACKLOG"; finish; }

python3 - "$BACKLOG" "$BACKLOG_ROW_CAP" "$BACKLOG_PREAMBLE_CAP" <<'PY'
import re, sys

path, row_cap, pre_cap = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
lines = open(path).read().split('\n')

RED, GREEN, RESET = '\033[31m', '\033[32m', '\033[0m'
failed = 0


def fail(msg):
    global failed
    print('  %sFAIL%s %s' % (RED, RESET, msg))
    failed = 1


# A row is a table line whose first cell is a task id. The separator line and
# the header are not rows, and neither is a table elsewhere in the file.
ROW = re.compile(r'^\|\s*(M-?\d+\.\d+[a-z]?)\s*\|')

first_row = None
for n, line in enumerate(lines):
    m = ROW.match(line)
    if m is None:
        continue
    if first_row is None:
        first_row = n
    ident = m.group(1)
    # ⚠️ The STATE column, which is the last cell -- not "the word done appears
    # somewhere in the line". A row whose text discusses a done task is not itself
    # done, and that distinction is the difference between a gate and a grep.
    cells = [c.strip() for c in line.strip().strip('|').split('|')]
    state = cells[-1] if cells else ''
    if 'done' in state.lower():
        fail('%s:%d %s is %s -- move it to backlog-done.md, which nothing loads'
             % (path, n + 1, ident, state))
    if len(line) > row_cap:
        fail('%s:%d %s row is %d bytes, cap %d -- a row is a SUMMARY; put the '
             'rest under ### %s in backlog-notes.md and link it'
             % (path, n + 1, ident, len(line), row_cap, ident))

preamble = len('\n'.join(lines[:first_row if first_row is not None else len(lines)]))
if preamble > pre_cap:
    fail('%s preamble is %d bytes before the first task row, cap %d -- this is the '
         'part that grows with nobody adding a task' % (path, preamble, pre_cap))

if not failed:
    print('  %sok%s   %s: %d bytes, preamble %d, every row open and under %d'
          % (GREEN, RESET, path, sum(len(x) + 1 for x in lines), preamble, row_cap))
sys.exit(1 if failed else 0)
PY
[ $? -eq 0 ] || FAILED=1
finish
