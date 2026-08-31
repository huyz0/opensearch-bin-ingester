#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# A milestone is not done because the loop stopped. Every acceptance criterion in
# SPEC.md must have a matching evidence line in VERIFIED.md naming what demonstrated it.
#
#   scripts/check-milestone-verified.sh <milestone-dir>
#
# It cannot verify the evidence is TRUE -- that is non-negotiable 4, which no script
# can check. It forces ENUMERATION, which catches the quiet omission.
source "$(dirname "$0")/lib.sh"
cd "$ROOT"
DIR="${1:-}"
[ -n "$DIR" ] && [ -f "$DIR/SPEC.md" ] || { echo "usage: check-milestone-verified.sh <dir with SPEC.md>" >&2; exit 2; }
hdr "check-milestone-verified $(basename "$DIR")"
python3 - "$DIR" <<'PY'
import os, re, sys
d = sys.argv[1]
spec = open(os.path.join(d, 'SPEC.md')).read()
m = re.search(r'^## Acceptance criteria\s*(.*?)(?=^## )', spec, re.S | re.M)
if not m:
    print("  \033[31mFAIL\033[0m SPEC.md has no '## Acceptance criteria' section"); sys.exit(1)
# `-?` because M1's first acceptance criterion is numbered -1. The gate
# whose entire purpose is forcing enumeration must not silently drop one.
crit = re.findall(r'^\s*(-?\d+)\.\s+(.+)$', m.group(1), re.M)
if not crit:
    print("  \033[31mFAIL\033[0m no numbered acceptance criteria found"); sys.exit(1)
vp = os.path.join(d, 'VERIFIED.md')
if not os.path.exists(vp):
    print("  \033[31mFAIL\033[0m %s missing. %d criteria need evidence:" % (vp, len(crit)))
    for n, t in crit: print("           %s. %s" % (n, t[:80]))
    print("         Each line must name the test or command that demonstrated it,")
    print("         or say OBSERVED-NOT / NOT-RUN. Do not write one you did not run.")
    sys.exit(1)
ver = open(vp).read()
missing, unproven = [], []
for n, t in crit:
    row = re.search(r'^\s*%s\.\s+(.+)$' % re.escape(n), ver, re.M)
    if not row: missing.append((n, t))
    elif not re.search(r'(#|\.sh|\bgradlew\b|Test\b|NOT-RUN|OBSERVED-NOT)', row.group(1)):
        unproven.append((n, row.group(1)))
for n, t in missing: print("  \033[31mFAIL\033[0m criterion %s has no evidence line: %s" % (n, t[:70]))
for n, r in unproven: print("  \033[31mFAIL\033[0m criterion %s names no test or command: %s" % (n, r[:70]))
if missing or unproven: sys.exit(1)
notrun = len(re.findall(r'(NOT-RUN|OBSERVED-NOT)', ver))
print("  \033[32mok\033[0m   %d criteria, all with evidence%s"
      % (len(crit), (" (%d explicitly NOT run -- report them)" % notrun) if notrun else ""))
PY
[ $? -eq 0 ] || FAILED=1   # a heredoc exit status is not inherited by `finish`
finish
