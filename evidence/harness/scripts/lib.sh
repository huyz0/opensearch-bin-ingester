#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Shared helpers. Sourced, never executed.
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FAILED=0
fail() { printf '  \033[31mFAIL\033[0m %s\n' "$*"; FAILED=1; }
warn() { printf '  \033[33mWARN\033[0m %s\n' "$*"; }
ok()   { printf '  \033[32mok\033[0m   %s\n' "$*"; }
hdr()  { printf '\033[1m%s\033[0m\n' "$*"; }
finish() { if [ "$FAILED" -ne 0 ]; then exit 1; fi; exit 0; }

# ---------------------------------------------------------------------------
# workspace_files -- the ONE definition of "files this project owns".
#
#   workspace_files '*.md' '*.java'
#
# Derived from git, never from a filesystem walk. That is the whole point: a
# reference clone under .tmp/, a sibling checkout at ../OpenSearch, a Gradle
# build/ directory and anything else outside version control are unreachable
# BY CONSTRUCTION rather than by each gate remembering to exclude them.
#
# ⚠️ This replaced five different hand-maintained `find . -not -path ...` lists
# that had drifted apart -- one excluded .tmp/ but not build/, another the
# reverse -- and a python glob that needed its own .tmp/ special case. Every new
# gate was a fresh chance to forget one. scripts/check-gate-scope.sh now fails
# any gate that walks the filesystem directly, so this cannot drift again.
#
# Scope is tracked + staged, which is exactly what a pre-commit gate should
# judge: an unstaged file is not part of the commit under test. It also fixes
# the initial-commit case, where `git ls-files` alone returns nothing.
workspace_files() {
  {
    git ls-files -- "$@" 2>/dev/null
    git diff --cached --name-only --diff-filter=ACMR -- "$@" 2>/dev/null
  } | sort -u | while IFS= read -r f; do
    [ -n "$f" ] && [ -f "$f" ] && printf '%s\n' "$f"
  done
}

# ---------------------------------------------------------------------------
# Fast mode. GATE_SCOPE=delta (the default) judges only what this change
# touches; GATE_SCOPE=full judges the whole tree.
#
# Pre-commit runs delta, because a gate people wait for is a gate people
# disable. CI runs full, because a delta gate never re-examines a file that
# stopped being valid for a reason outside its own diff.
#
# ⚠️ Every gate PRINTS its mode. A `delta` pass is a weaker statement than a
# `full` pass and must not read like one -- non-negotiable 4.
: "${GATE_SCOPE:=delta}"

# Files this change touches: staged, or the CI range when CHECK_RANGE is set.
changed_files() {
  if [ -n "${CHECK_RANGE:-}" ]; then
    git diff --name-only --diff-filter=ACMR "$CHECK_RANGE" HEAD -- "$@" 2>/dev/null
  else
    git diff --cached --name-only --diff-filter=ACMR -- "$@" 2>/dev/null
  fi | while IFS= read -r f; do [ -n "$f" ] && [ -f "$f" ] && printf '%s\n' "$f"; done
}

# What a per-file gate should examine, given the mode.
scoped_files() {
  case "$GATE_SCOPE" in
    full) workspace_files "$@" ;;
    *)    changed_files "$@" ;;
  esac
}

# Suffix for the success line, so the mode is never implicit.
scope_note() {
  case "$GATE_SCOPE" in
    # "full" means every TRACKED or staged file. Before the first commit most
    # of the tree is neither, and saying "full tree" would overstate it.
    full) printf ' (full tree: %s tracked/staged)' "$(git ls-files | wc -l | tr -d ' ')" ;;
    *)    printf '%s' " (changed files only -- CI runs GATE_SCOPE=full)" ;;
  esac
}
# A review is in flight against a staged hash; editing the index voids it.
# Written because a prose rule (review.md 12b) was broken five times in one
# session. A predicate belongs in a script -- non-negotiable 9.
mark_review_inflight() { mkdir -p .harness/review; git diff --cached | sha256sum | cut -d' ' -f1 > .harness/review/.inflight; }
warn_if_index_moved() {
  [ -f .harness/review/.inflight ] || return 0
  local was now; was=$(cat .harness/review/.inflight); now=$(git diff --cached | sha256sum | cut -d' ' -f1)
  [ "$was" = "$now" ] && return 0
  printf '  \033[33mWARN\033[0m the index moved while a review was in flight\n'
  printf '         reviewed %s\n         staged   %s\n' "${was:0:12}" "${now:0:12}"
  printf '         That verdict cannot bind. review.md rule 12b.\n'
}
