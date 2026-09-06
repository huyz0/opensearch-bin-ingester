#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# A row ADDED to the archive is a summary, not a narrative of its own review.
#
# ⚠️ BacklogBoundsTest ARGUES THE ARCHIVE MUST NOT BE JUDGED -- "applying the row
# cap to it would mean rewriting rows that already shipped ... and the archive is
# not loaded by anything, so its size costs nothing". Both halves are true of
# EXISTING rows, and this gate leaves them alone: it judges only rows whose ID is
# NEW. What it collects is a DIFFERENT cost from session load: a row added in a
# commit is part of that commit's REVIEW SURFACE, and every clause in it is a
# claim a reviewer then checks.
#
# ⚠️ EXEMPTION IS BY ID, NOT BY BYTES, and the difference is the whole point. 25
# of the 121 rows already in the BASE are over this cap; with a byte-equality
# exemption, ANY later edit to one of them -- a glossary rename, and
# check-terminology.sh scans every *.md full-tree in CI -- would fail with
# "summarise it", which is a size gate turned into an instruction to destroy the
# record. An ID already in the base is exempt however its text changes.
#
# ⚠️ IT RUNS IN CI AND MUST NOT PASS VACUOUSLY THERE. The hook is `always_run`,
# and CI does not skip it; in a fresh checkout `git diff --cached` is EMPTY, so a
# staged-only reading prints its SUCCESS line over an empty set and exits 0 while
# HEAD holds an 18,845-character row. CHECK_RANGE is what CI supplies for exactly this, and
# ci.yml resolves it FAIL-CLOSED rather than defaulting to "check nothing". An
# earlier draft dropped this branch, having confused CHECK_RANGE with GATE_SCOPE:
# GATE_SCOPE=full has no meaning here, because there is no whole-tree reading of
# "what this change adds", but a RANGE is precisely that reading.
#
# ⚠️ NO ENV OVERRIDE OF THE CAP. `REVIEW_ROUND_BUDGET` is the cautionary case: its
# escape hatch is taken on 13 tasks in M4 alone, each time to keep doing the
# thing the budget exists to stop.
source "$(dirname "$0")/lib.sh"
cd "$ROOT"
hdr "check-archive-row-size"
ARCHIVE=docs/internal/product/backlog-done.md
CAP=4000

python3 - "$ARCHIVE" "$CAP" <<'PY'
import os, re, subprocess, sys

archive, cap = sys.argv[1], int(sys.argv[2])
rng = os.environ.get('CHECK_RANGE', '').strip()

# ⚠️ check-session-load.sh's RECOGNISER, WIDENED. A literal '| M' prefix let a row
# through on two spaces, on no space and on a leading space -- while reporting
# success over an empty set, which reads as "nothing to judge" rather than "I
# could not parse it". ⚠️ AN EARLIER DRAFT PUT A FABRICATED QUOTE HERE, and the
# grep that was meant to remove it missed this one because the phrase WRAPPED --
# the exact blindness that withdrew the second gate, reproduced in the comment
# describing it. Beyond the sibling's `^\|`: an indented row, a **bold** or `backticked`
# id, a multi-part id (M0.7.1), and GFM's optional outer pipe -- and a bolded id
# is NOT a shape that can be dismissed as uncitable, because check-commit-msg.sh
# accepts the subject `M9.9` against a row headed `| **M9.9** |`.
ROW = re.compile(r'^\s*\|?\s*[*`_]{0,2}\s*(M-?\d+(?:\.\d+)+[a-z0-9]*)\s*[*`_]{0,2}\s*\|')

def run(*args):
    out = subprocess.run(args, capture_output=True, text=True)
    return out.stdout if out.returncode == 0 else None

if rng:
    mode = "CI range %s..HEAD" % rng[:12]
    diff = run('git', 'diff', '-U0', rng, 'HEAD', '--', archive)
    base = rng
else:
    mode = "staged diff"
    diff = run('git', 'diff', '--cached', '-U0', '--', archive)
    base = 'HEAD'

if diff is None:
    print("  \033[31mFAIL\033[0m could not read the %s for %s -- refusing rather than "
          "reporting a clean tree" % (mode, archive))
    sys.exit(1)

before = run('git', 'show', '%s:%s' % (base, archive))
if before is None:
    print("  \033[31mFAIL\033[0m could not read %s at %s -- refusing rather than "
          "reporting a clean tree" % (archive, base))
    sys.exit(1)
known = {m.group(1) for m in (ROW.match(l) for l in before.splitlines()) if m}

judged = failed = exempt = 0
for line in diff.splitlines():
    if not line.startswith('+') or line.startswith('+++'):
        continue
    body = line[1:]
    m = ROW.match(body)
    if not m:
        continue
    if m.group(1) in known:
        exempt += 1
        continue
    judged += 1
    n = len(body)
    if n > cap:
        print("  \033[31mFAIL\033[0m %s: new row %s is %d characters, cap %d -- summarise it "
              "and put the essay in backlog-notes.md" % (archive, m.group(1), n, cap))
        failed = 1

if not failed:
    print("  \033[32mok\033[0m   %d new archive row(s), each within %d characters; "
          "%d already in the base and exempt (%s)" % (judged, cap, exempt, mode))
sys.exit(failed)
PY
[ $? -eq 0 ] || FAILED=1
finish
