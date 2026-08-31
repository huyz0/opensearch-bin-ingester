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
BACKLOG=docs/internal/product/backlog.md
if [ ! -f "$BACKLOG" ]; then
  fail "$BACKLOG does not exist, so the ID cannot be verified"
  finish
fi
grep -qE "(^|[^0-9A-Za-z.])${id}([^0-9]|$)" "$BACKLOG" || fail "task $id is not in $BACKLOG"
[ "$FAILED" -eq 0 ] && ok "subject names backlog task $id"
finish
