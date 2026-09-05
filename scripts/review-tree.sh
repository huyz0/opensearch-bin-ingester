#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# M0.57: one isolated copy of the STAGED tree per reviewer, so the two can run
# CONCURRENTLY without corrupting each other's evidence.
#
#   review-tree.sh <role>        materialise, and print the path
#
# ⚠️ THE REVIEWERS VERIFY BY MUTATING -- apply a change to production code, run
# the suite, see which test fails, restore. That is not incidental: nearly every
# major finding in M4 was found that way, and a read-only reviewer reasoning
# over a diff would have found almost none of them. Run in ONE tree they
# overwrite each other's mutation halfway through the other's build, and both
# `./gradlew test` into the same `build/`, clobbering the `test-results` XML
# that `tdd_scan` and every pass/fail count read.
#
# ⚠️ `git checkout-index`, NOT `git worktree add`. A worktree checks out the
# COMMIT and carries no index state, so each reviewer would review a tree
# WITHOUT the diff under review -- which is worse than no isolation, because it
# looks like a review and is not. `checkout-index` materialises exactly the
# bytes `check-reviewed` binds the verdict to by hash.
#
# ⚠️ A FRESH DIRECTORY PER INVOCATION, never a reused one. An earlier draft
# keyed the path on repository+role and `rm -rf`'d it first; round-1 review
# measured its own tree being removed and re-created underneath it mid-review by
# an unrelated invocation for the same role, which is the M4.3 evidence
# corruption this script exists to prevent, re-created one level up and with an
# `rm -rf` instead of an overwrite. `mktemp -d` cannot collide, so there is
# nothing to clear and no `rm -rf` in this script at all.
#
# ⚠️ NOT RECLAIMED, deliberately, AND THAT COSTS DISK. Trees share one parent so
# they can be removed in one deliberate act by a person who knows no reviewer is
# running; nothing here removes them, and no claim is made that the platform
# does. A draft of this comment said the temp reaper would take them, which
# round-3 review measured as false on the documented platform: /tmp is tmpfs
# here and the parent had already reached 433M across 192 directories, at ~9M
# per tree before any build and ~14M after the buildSrc suite. An age-based
# sweep would put the `rm -rf` back, and this script's whole history is that the
# `rm -rf` was the dangerous part -- so the honest trade is stated rather than
# hidden behind a reaper that is not running:
#     rm -rf "${TMPDIR:-/tmp}/binjava-review-trees"   # when no review is active
source "$(dirname "$0")/lib.sh"
cd "$ROOT"

ROLE="${1:-}"

# ⚠️ `--gradle-home` PRINTS THE SHARED DEPENDENCY CACHE AND EXITS, materialising
# no tree. WHAT IS SHARED AND WHAT IS NOT is the whole of this: a per-tree
# `build/` is the isolation that matters -- two reviewers writing one clobber the
# test-results XML every pass/fail count reads -- while GRADLE_USER_HOME holds the
# wrapper distribution, the dependency cache and the build cache, none of which is
# evidence. Re-fetching all of it per tree was pure cold-start cost on a round
# already priced at 7-13 minutes, paid twice a round, forever.
#
# ⚠️ It sits UNDER THE SAME BASE as the trees, so it inherits every containment
# guard below rather than repeating them, and so a person clearing the base in one
# deliberate act clears the cache with it.
# ⚠️ AN ALLOWLIST, and it is load-bearing rather than tidy: ROLE lands in a
# filesystem path, and round-1 test review demonstrated `review-tree.sh
# '../../../victim'` deleting a file outside the tree back when this script
# removed its destination. There is no `rm -rf` here any more, but a traversing
# role would still materialise a repository somewhere nobody asked for.
case "$ROLE" in
  reviewer|test-reviewer|--gradle-home) ;;
  *) echo "usage: review-tree.sh <reviewer|test-reviewer|--gradle-home>" >&2; exit 2 ;;
esac

# ⚠️ RESOLVED AND REFUSED BEFORE ANYTHING IS CREATED. An earlier draft checked
# containment after `mkdir -p`, so refusing an in-repo TMPDIR still left the
# directories it had just made inside the working tree -- refusing an input
# after acting on it is not refusing it.
# ⚠️ `pwd -P`, not `pwd`: the logical path walks straight past this check when
# TMPDIR is a symlink into the repository, which round-2 review measured
# landing a 566-file copy and a nested `.git` physically inside it.
TMPROOT=$(cd "${TMPDIR:-/tmp}" 2>/dev/null && pwd -P) || {
  echo "review-tree: TMPDIR=${TMPDIR:-/tmp} is not a directory" >&2; exit 1
}
# ⚠️ BOTH SIDES PHYSICAL. `lib.sh` sets ROOT from a plain `pwd`, so comparing a
# resolved TMPROOT against it walks straight past this guard whenever the
# REPOSITORY is reached through a symlink -- the same logical-versus-physical
# defect as the one this check already carries on the other side, measured
# landing a full copy and a nested `.git` physically inside the working tree.
ROOT_P=$(cd "$ROOT" && pwd -P) || { echo "review-tree: cannot resolve $ROOT" >&2; exit 1; }
case "$TMPROOT/" in
  "$ROOT_P"/*) echo "review-tree: TMPDIR resolves inside $ROOT_P; set it elsewhere" >&2; exit 1 ;;
esac

BASE="$TMPROOT/binjava-review-trees"
# ⚠️ A PHYSICAL TMPROOT DOES NOT MAKE BASE PHYSICAL. One fixed component is
# appended to it, and `mkdir -p` and `mktemp -d` both follow that component when
# it is a symlink -- so round-4 review reproduced a pre-existing symlink at
# exactly this predictable name, in a world-writable directory, landing the
# whole tree AND a nested `.git` physically inside the working tree while
# printing an outside-looking path. A recursive delete of test-results XML then
# reaches a live reviewer's evidence.
# ⚠️ AND RESOLVED BEFORE ANYTHING IS CREATED UNDER IT. A first fix checked the
# tree AFTER `mktemp -d` had made it, which refuses the input having already
# acted on it -- the directory was created inside the repository and then
# rejected, the same defect this script's TMPDIR guard was moved above `mkdir`
# to fix in an earlier round. Its own test caught it.
if [ -e "$BASE" ]; then
  BASE_P=$(cd "$BASE" 2>/dev/null && pwd -P) && [ -n "$BASE_P" ] || {
    echo "review-tree: $BASE exists but is not a usable directory" >&2; exit 1
  }
  case "$BASE_P/" in
    "$ROOT_P"/*)
      echo "review-tree: $BASE resolves inside $ROOT_P; set TMPDIR elsewhere" >&2; exit 1 ;;
  esac
else
  mkdir -p "$BASE" || { echo "review-tree: cannot create $BASE" >&2; exit 1; }
  # Freshly created directly under an already-physical TMPROOT, so it is one.
  BASE_P=$BASE
fi
# The shared cache, created under the already-guarded BASE_P. Answered here and
# not in review.sh so the path exists in exactly one place: a literal duplicated
# across two scripts is a fork waiting to drift.
GRADLE_HOME="$BASE_P/gradle-home"
mkdir -p "$GRADLE_HOME" || {
  echo "review-tree: cannot create $GRADLE_HOME" >&2; exit 1
}
if [ "$ROLE" = "--gradle-home" ]; then
  printf '%s\n' "$GRADLE_HOME"
  exit 0
fi

# ⚠️ WHAT THE TREES COST, REPORTED RATHER THAN RECLAIMED. Nothing here removes
# anything, deliberately: this script's history is that the `rm -rf` was the
# dangerous part -- an earlier draft was measured destroying a live reviewer's
# tree mid-review -- and an age-based sweep would put it back. So the number is
# printed instead, and removing them stays one deliberate act by a person who
# knows no reviewer is running:
#     rm -rf "$BASE_P"
n=$(find "$BASE_P" -maxdepth 1 -type d -name '*-reviewer.*' -o -maxdepth 1 -type d -name 'reviewer.*' 2>/dev/null | wc -l)
if [ "${n:-0}" -gt 40 ]; then
  echo "review-tree: $n trees under $BASE_P ($(du -sh "$BASE_P" 2>/dev/null | cut -f1))." >&2
  echo "review-tree: nothing here reclaims them. When no review is running:" >&2
  echo "review-tree:     rm -rf \"$BASE_P\"" >&2
fi

# `mktemp -d` creates a real directory, never a symlink, so appending its name
# to a physical parent keeps the result physical: no further resolution is
# needed here, and an unfalsifiable one would only look like a guard.
DEST=$(mktemp -d "$BASE_P/$ROLE.XXXXXXXX") || {
  echo "review-tree: cannot create a tree under $BASE_P" >&2; exit 1
}

# ⚠️ CHECKED. `lib.sh` sets `set -uo pipefail`, NOT `set -e`, so an unchecked
# failure here still printed the path and exited 0 -- and `TREE=$(...) && cd
# "$TREE"` then succeeded into a tree that is not the staged bytes, which a
# reviewer cannot tell from the real thing. Round-1 review reproduced it with an
# index naming an unreachable blob: two of three files materialised, exit 0.
# ⚠️ Honours GIT_INDEX_FILE, which is what makes this compose with a private
# index: a session reviewing its own staged bytes in a shared checkout gets ITS
# index materialised, not whatever another session left in .git/index.
if ! git checkout-index -a --prefix="$DEST/"; then
  echo "review-tree: could not materialise the index into $DEST" >&2
  exit 1
fi

# ⚠️ A REAL REPOSITORY, because every per-file gate resolves its input through
# `lib.sh`'s `workspace_files`/`scoped_files`, which ask git. Without this the
# gates find nothing and PASS -- "0 source file(s) within 500 lines" -- so a
# reviewer verifying a gate change the intended way, by mutating the input the
# gate must reject, gets green for every mutation. Round-1 review measured that,
# plus 13 `FreshCheckoutTest` failures from `fatal: not a git repository`.
#
# ⚠️ AN EMPTY BASE COMMIT AND THEN `git add -A`, WITHOUT COMMITTING -- and the
# difference is the whole point. An earlier draft COMMITTED the tree, which
# leaves index == HEAD, so `git diff --cached` is empty and every gate in its
# DEFAULT delta mode examines nothing and passes vacuously. Round-3 review
# measured `check-file-size` printing this script's own quoted failure string,
# `ok 0 source file(s) within 500 lines`, in a tree holding a deliberately
# 863-line file -- the exact vacuous pass the paragraph above says this block
# prevents, re-created by the block itself. `check-tdd` and `check-reviewed`
# have no full mode at all and printed `ok nothing staged`. Staging without
# committing lists every file as added, so a delta gate sees the whole tree;
# the empty commit is there so `HEAD` still resolves for the gates that read it.
# ⚠️ `env -u` for exactly the reason `GitEnv` exists on the Java side: these
# commands must build the NEW repository's index, not write into whatever
# private index the caller had exported.
if ! sh -c '
      # NOTE: no apostrophes in here -- this is a single-quoted sh -c body.
      # Strip EVERY GIT_* variable, not the three obvious ones: this commit own
      # ReviewTreeTest argues by name that an explicit list is insufficient,
      # since GIT_OBJECT_DIRECTORY alone redirects the new repository objects
      # into the caller store and leaves a .git with no objects/ -- R2 silent
      # green verbatim. A three-key list here, while the test next door says
      # three keys are not enough, was one claim contradicted inside one commit.
      for v in $(env | sed -n "s/^\\(GIT_[A-Za-z0-9_]*\\)=.*/\\1/p"); do unset "$v"; done
      cd "$1" || exit 1
      git init -q . || exit 1
      git config user.email review@tree || exit 1
      git config user.name "review tree" || exit 1
      git commit -q --allow-empty -m "empty base, so HEAD resolves" || exit 1
      git add -A || exit 1
    ' _ "$DEST"; then
  echo "review-tree: materialised $DEST but could not make it a repository" >&2
  exit 1
fi

printf '%s\n' "$DEST"
