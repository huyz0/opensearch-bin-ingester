#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Non-negotiable 1: the commit subject names a real backlog task.
source "$(dirname "$0")/lib.sh"
cd "$ROOT"
MSG_FILE="${1:-}"
[ -n "$MSG_FILE" ] && [ -f "$MSG_FILE" ] || { echo "usage: check-commit-msg.sh <file>"; exit 2; }
subject=$(head -1 "$MSG_FILE")
case "$subject" in "Merge "*|"Revert "*|"fixup!"*) exit 0 ;; esac
hdr "check-commit-msg"
id=$(printf '%s' "$subject" | grep -oE '^M-?[0-9]+\.[0-9]+' || true)
if [ -z "$id" ]; then
  fail "subject does not start with a task ID (expected e.g. 'M0.3 <summary>'): $subject"
  finish
fi
# ⚠️ THE BACKLOG AND ITS ARCHIVE. Done rows move out of backlog.md so a session
# does not load them (check-backlog-size.sh), and a completed task's ID must stay
# nameable -- otherwise every follow-up, revert and fix-up commit that cites the
# task it amends is refused. The glob is the whole list: backlog.md and
# backlog-done.md today, and any later split without a second edit here.
BACKLOG=$(ls docs/internal/product/backlog*.md 2>/dev/null)
if [ -z "$BACKLOG" ]; then
  fail "no docs/internal/product/backlog*.md, so the ID cannot be verified"
  finish
fi
# shellcheck disable=SC2086
grep -qE "(^|[^0-9A-Za-z.])${id}([^0-9]|$)" $BACKLOG \
  || fail "task $id is in none of: $(echo $BACKLOG | tr '\n' ' ')"
[ "$FAILED" -eq 0 ] && ok "subject names backlog task $id"
finish
