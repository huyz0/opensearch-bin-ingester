#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# review.md rule 12's remedy, applied BEFORE the rounds are spent (M0.81).
#
# ⚠️ WHY A CAP AT ALL. This repository's three largest commits to module source
# trees are also the ones that consumed the most review rounds: M4.7 at 2,039
# added lines, M4.10c at 1,423 over six rounds, M4.8b2 at 1,385. Rule 12 already
# says the remedy for exceeding the round budget is to SPLIT; by then two agents
# have read the thing several times each.
#
# ⚠️ THE CORRELATION IS NOT CLEAN, and this comment says so rather than
# implying a law: M0.76 blew its round budget at 243 added lines, on a diff
# whose problem was prose, not size. Size is one driver. That is why the cap
# sits where only the clearly-large land above it, and why the escape is an
# ARGUMENT rather than a refusal -- a big commit that has to be big says why,
# in the diff, once.
#
# ⚠️ COMMIT-MSG STAGE, because the argument is keyed by task id and the id
# lives in the subject -- the same place check-commit-msg and
# check-test-integrity read it from.
source "$(dirname "$0")/lib.sh"
cd "$ROOT"
hdr "check-diff-size"

CAP="${DIFF_SIZE_CAP:-1000}"
MSG_FILE="${1:-}"
[ -n "$MSG_FILE" ] && [ -f "$MSG_FILE" ] || { warn "no commit message file -- size unenforced"; finish; }
SUBJECT="$(head -1 "$MSG_FILE")"
TASK="$(printf '%s' "$SUBJECT" | grep -oE '^M-?[0-9]+\.[0-9]+[a-z0-9]*' || true)"

# ⚠️ SOURCE ONLY. A backlog row or an ADR is read, not mutated, and counting
# prose would fire the gate hardest on the commits that are cheapest to review.
ADDED=$(git diff --cached --numstat -- '*/src/*' 2>/dev/null | awk '{s+=$1} END {print s+0}')

if [ "$ADDED" -le "$CAP" ]; then
  ok "$ADDED added line(s) of source, within the cap of $CAP"
  finish
fi

# ⚠️ STAGED, and matched against the ADDED lines of the diff -- an unstaged
# argument suppresses the gate while leaving no trace in what anyone reviews.
# M4.26 records that exact hole for the sibling key in this same file.
ARG=""
if [ -n "$TASK" ]; then
  ARG=$(git diff --cached -U0 -- baselines/review.txt 2>/dev/null \
        | grep -E "^\+size:$TASK([[:space:]]|$)" || true)
fi

# ⚠️ NOT `NF > 1`. M4.26 measured that on the sibling key: a trailing tab splits
# into a second, EMPTY field, so `size:M9.1<TAB>` argued the cap while
# justifying nothing. Strip the key, then require a non-space remainder.
REASON=""
if [ -n "$ARG" ]; then
  REASON=$(printf '%s' "$ARG" | sed -E "s/^\+size:$TASK[[:space:]]*//")
fi

if [ -n "$REASON" ] && printf '%s' "$REASON" | grep -q '[^[:space:]]'; then
  ok "$ADDED added line(s) of source, over the cap of $CAP -- argued for $TASK"
  finish
fi

fail "$ADDED added line(s) of source, over the cap of $CAP"
echo "         review.md rule 12: SPLIT it. Two reviewers read every round of"
echo "         this, and the largest commits here have cost the most rounds."
echo "         If it genuinely cannot be split -- one wire format, which"
echo "         non-negotiable 8 forbids splitting -- stage a line in"
echo "         baselines/review.txt saying so:"
echo "             size:${TASK:-<TASK>}  <why this cannot be split>"
finish
