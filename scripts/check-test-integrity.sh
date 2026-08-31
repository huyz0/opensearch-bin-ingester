#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# testing.md rules 4-5: production changes to satisfy the test, never the reverse.
#
#   check-test-integrity.sh [<commit-msg-file>]
#
# It runs at the commit-msg stage because the escape hatch testing.md names is
# "justify it in the commit body", and the commit body does not exist at the
# pre-commit stage. The previous version grepped the staged *markdown* for
# /remove.*test/i instead, so the documented remedy did not work and an
# unrelated documentation line did.
source "$(dirname "$0")/lib.sh"
cd "$ROOT"
hdr "check-test-integrity"
MSG_FILE="${1:-}"
if [ -z "${CHECK_RANGE:-}" ]; then
  git diff --cached --quiet && { ok "nothing staged"; finish; }
fi
python3 scripts/test_integrity.py "${MSG_FILE}" "${CHECK_RANGE:-}" || FAILED=1
finish
