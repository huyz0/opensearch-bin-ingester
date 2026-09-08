#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# The gate: the staged bytes were reviewed by two agents that did not write
# them, and the round cap is real.
#
# ⚠️ IT DOES NOT SOURCE `scripts/lib.sh`. Not because that file is bad, but
# because two of its behaviours are the ones under repair and inheriting them
# here would reintroduce them: `--diff-filter=ACMR` drops deletions from every
# delta-scoped file list, and the `ok ... nothing staged` idiom it exists to
# print is the vacuous pass this gate refuses. Output is formatted the same way
# so a run reads consistently; nothing else is shared.
#
# ⚠️ NO `harness-exempt`. review.md waives the reviewer entirely for a change
# confined to scripts/, buildSrc/, .agents/, .claude/, docs/ or baselines/ --
# which is every harness change there has ever been. That waiver is why the
# review harness was rebuilt repeatedly without a reviewer ever reading it.
# There is no equivalent here, and `tests/run.sh` case R9 is what keeps one
# from being added back quietly.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
printf '\033[1m%s\033[0m\n' "check-xreviewed"

if ! command -v python3 >/dev/null 2>&1; then
  # Fail, never skip. A missing interpreter means this gate did not run, and a
  # gate that did not run has not passed.
  printf '  \033[31mFAIL\033[0m python3 is not available -- this gate did not run.\n'
  exit 1
fi

exec python3 "$HERE/xreview.py" verify "$@"
