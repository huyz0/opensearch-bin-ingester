#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# review.md rule 15: one worktree per session, so two agent sessions never share
# an index or a HEAD.
#
#   session-worktree.sh <name>     create it if absent, print its path
#
# ⚠️ THE PROBLEM THIS SOLVES IS NOT THE ONE `review-tree.sh` SOLVES, and the two
# are easy to confuse because both hand out a directory. `review-tree.sh`
# materialises the INDEX for a reviewer, and a worktree is the wrong tool there
# -- it checks out a COMMIT and so carries none of the change under review.
# THIS script is for a SESSION doing its own work: it wants a real working tree
# with its own index and its own branch, and a commit is exactly the right base.
#
# ⚠️ WHAT SHARING COSTS, measured rather than asserted. Two sessions in one
# checkout share one index and one HEAD: staged files collide, a reviewer finds
# `git diff --cached` empty because the other session staged over it, and
# `pre-commit` stashes work belonging to someone else. The obvious workaround --
# a private `GIT_INDEX_FILE` -- stops the staging collisions and then CANNOT
# carry the commit, because pre-commit clears that variable and every hook
# evaluates the shared index instead: one session measured its staged diff
# hashing `927b8a9a...` outside the hook and `63e3cb9d...` inside it, with
# `check-reviewed` refusing verdicts it had just been handed.
#
# ⚠️ OUTSIDE THE REPOSITORY, for the same reason `review-tree.sh` is: anything
# that WALKS the tree rather than asking git -- `check-terminology.sh` and
# `check-metric-cardinality.sh` both `grep -r` -- would otherwise read a second
# copy of every source file as if it were the real one.
#
# ⚠️ NOTHING HERE REMOVES ANYTHING. Retiring a session is a person's decision,
# because the tree may hold uncommitted work -- one session in this repository
# ended with a complete, fourteen-round change existing nowhere else:
#     git worktree remove ../binjava-sessions/<name>
source "$(dirname "$0")/lib.sh"
cd "$ROOT"

NAME="${1:-}"
# ⚠️ AN ALLOWLIST, load-bearing rather than tidy: NAME lands in a filesystem
# path AND in a branch name. `review-tree.sh` learned this the expensive way --
# an unvalidated role there was demonstrated deleting a file outside the tree.
case "$NAME" in
  '' | *[!a-z0-9-]* | -* )
    echo "usage: session-worktree.sh <name>   (lowercase, digits, dashes)" >&2
    exit 2 ;;
esac

# ⚠️ PHYSICAL, both sides, before anything is created. A logical path walks
# straight past a containment check when either the repository or the
# destination is reached through a symlink -- measured twice on `review-tree.sh`,
# once on each side of the same comparison.
ROOT_P=$(cd "$ROOT" && pwd -P) || { echo "session-worktree: cannot resolve $ROOT" >&2; exit 1; }
PARENT=$(cd "$ROOT_P/.." && pwd -P) || {
  echo "session-worktree: cannot resolve the parent of $ROOT_P" >&2; exit 1
}
BASE="$PARENT/binjava-sessions"
DEST="$BASE/$NAME"

if [ -e "$DEST" ]; then
  # ⚠️ RESOLVED AND CHECKED BEFORE IT IS HANDED BACK: an existing destination
  # may be a symlink pointing into the repository, and appending one component
  # to a physical parent does not make that component physical.
  DEST_P=$(cd "$DEST" && pwd -P) && [ -n "$DEST_P" ] || {
    echo "session-worktree: $DEST exists but is not a usable directory" >&2; exit 1
  }
  case "$DEST_P/" in
    "$ROOT_P"/*)
      echo "session-worktree: $DEST resolves inside $ROOT_P; refusing" >&2; exit 1 ;;
  esac
  # ⚠️ REUSED, not recreated. An earlier draft of `review-tree.sh` cleared its
  # destination first and was measured destroying a live agent's work; the whole
  # point of a session worktree is that it PERSISTS across invocations.
  printf '%s\n' "$DEST_P"
  exit 0
fi

mkdir -p "$BASE" || { echo "session-worktree: cannot create $BASE" >&2; exit 1; }
# ⚠️ A BRANCH PER SESSION, so two sessions cannot advance the same ref. Without
# it both worktrees would refuse to check out the same branch anyway, and the
# error would send someone back to sharing one checkout.
BRANCH="session/$NAME"
if git show-ref --verify --quiet "refs/heads/$BRANCH"; then
  git worktree add "$DEST" "$BRANCH" >&2 || {
    echo "session-worktree: could not attach $DEST to $BRANCH" >&2; exit 1
  }
else
  git worktree add -b "$BRANCH" "$DEST" HEAD >&2 || {
    echo "session-worktree: could not create $DEST" >&2; exit 1
  }
fi

DEST_P=$(cd "$DEST" && pwd -P) && [ -n "$DEST_P" ] || {
  echo "session-worktree: created $DEST but cannot resolve it" >&2; exit 1
}
printf '%s\n' "$DEST_P"
