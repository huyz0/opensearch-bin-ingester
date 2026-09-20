#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Print only the open rows for the current milestone. The full backlog is a
# historical record; loading it into every autonomous session makes completed
# milestones look actionable and grows the session context without bound.
source "$(dirname "$0")/lib.sh"
cd "$ROOT"

python3 - <<'PY'
import re
from pathlib import Path

lines = Path("docs/internal/product/backlog.md").read_text().splitlines()
current = next((i for i, line in enumerate(lines)
                if line.startswith("**Current milestone:")), None)
if current is None:
    raise SystemExit("current milestone declaration is missing")

milestone = re.search(r"\bM\d+\b", lines[current])
if milestone is None:
    raise SystemExit("current milestone declaration has no milestone ID")
milestone_id = milestone.group(0)

print("# Current milestone backlog")
print(lines[current])
print()
print("| ID | Task | Serves | State |")
print("|---|---|---|---|")
found = False
for line in lines[current + 1:]:
    if re.match(r"^\*\*M\d+", line):
        break
    match = re.match(r"^\|\s*(M-?\d+\.\d+[a-z]?)\s*\|", line)
    if not match:
        continue
    if not match.group(1).startswith(milestone_id + "."):
        raise SystemExit(
            f"backlog row {match.group(1)} is outside current milestone {milestone_id}"
        )
    cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
    if cells and "done" not in cells[-1].lower():
        print(line)
        found = True
if not found:
    print("(no open rows)")
PY
