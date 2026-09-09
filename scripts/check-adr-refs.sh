#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# AGENTS.md's Never list: the ID must RESOLVE. It says so of `M<n>` and `R<n>`;
# a decision-record citation is the same defect and was outside every gate,
# because check-links.sh judges relative markdown LINKS and a citation written
# into a javadoc is not one.
#
# ⚠️ MEASURED, which is why this exists. M4 was delivered onto `main` from the
# `archive/m4` branch and its ELEVEN decision records were not: the code that
# arrived cited nine of them 136 times with nothing behind any of them, the
# most-cited being the record that justifies the idempotency key three
# milestones are built on.
#
# ⚠️ ALWAYS FULL SCOPE, and it offers no delta mode. A citation goes dangling
# when a record is RENAMED or DELETED, and the file carrying that citation is
# not in the diff that broke it -- so a delta scope is structurally blind to
# exactly the change this exists to catch. check-links.sh escalates to full for
# the same reason; this one never leaves it. Measured at 27 ms over 37
# citations, so the whole-tree read costs nothing worth saving.
#
# ⚠️ NO EXEMPTION PATH, DELIBERATELY: no allow-list, no baseline, no inline
# marker. A tree may therefore not QUOTE a citation it cannot resolve, this
# script included -- which is why nothing here writes one out in the citation
# form, and why a document about a missing record must name it some other way.
# An escape hatch here would be an escape hatch from the one rule the gate has.
source "$(dirname "$0")/lib.sh"
cd "$ROOT"
hdr "check-adr-refs"

DECISIONS="docs/internal/product/decisions"

# ⚠️ EVERY TRACKED-OR-STAGED FILE, with no extension list. A list is a promise
# that goes stale: the first version named seven suffixes while the gate's own
# success line claimed "the tree", and review measured citations in `.json`
# review verdicts and `.txt` baselines that it silently walked past. `-I` skips
# binaries, which is the only reason an extension list looked necessary.
FILES=$(workspace_files)
if [ -z "$FILES" ]; then
  ok "no files to read"
  finish
fi

# ⚠️ ONE TO FOUR DIGITS, not exactly four. A hand-typed short form -- the
# prefix with two digits rather than the zero-padded four -- is the invented-ID
# shape AGENTS.md's Never bullet is about, and a gate that only sees the
# well-formed spelling misses precisely the citations nobody checked. ⚠️ AND
# THIS CANNOT MATCH A RECORD'S OWN HEADING: those read `# 0027. One refresh
# attempt...`, with no prefix before the number.
# ⚠️ NOTHING IN THIS FILE WRITES A CITATION OUT, and that is forced rather than
# stylistic: the gate reads every tracked file INCLUDING ITSELF, so an example
# spelt in the citation form would be a citation, and one naming a record that
# does not exist would make the gate refuse the tree for its own comment.
cited=$(printf '%s\n' "$FILES" | tr '\n' '\0' \
  | xargs -0 grep -IhoE 'ADR-[0-9]{1,4}' 2>/dev/null | sort -u)
if [ -z "$cited" ]; then
  ok "no decision record is cited anywhere"
  finish
fi

# ⚠️ RESOLVED AGAINST GIT, NOT AGAINST THE FILESYSTEM. Citations come from
# tracked-or-staged files, so the records must too: review MEASURED a record
# sitting on disk UNTRACKED satisfying a `compgen -G` probe, because pre-commit
# stashes unstaged changes to TRACKED files only. The gate went green on a
# commit whose citation resolved for nobody else -- the defect this task exists
# to make unrepresentable, one notch smaller.
# ⚠️ SPACE-SEPARATED ON ONE LINE, because the membership test below is a `case`
# glob over `" $have "`: newline-separated, the separators around a number are
# not spaces and every citation reads as dangling -- measured, 38 of 38.
have=$(workspace_files "$DECISIONS/*.md" | sed "s|^$DECISIONS/||" | cut -d- -f1 \
  | sort -u | tr '\n' ' ')

dangling=""
for ref in $cited; do
  # ⚠️ ZERO-PADDED TO FOUR, so a two-digit citation and its zero-padded twin
  # resolve to one record rather than to two different answers about the same
  # decision. (Spelt out rather than shown, for the reason above.)
  n=$(printf '%04d' "$((10#${ref#ADR-}))")
  case " $have " in
    *" $n "*) ;;
    *) dangling="$dangling $ref" ;;
  esac
done

checked=$(printf '%s\n' $cited | wc -l | tr -d ' ')
if [ -n "$dangling" ]; then
  n=$(printf '%s\n' $dangling | wc -l | tr -d ' ')
  fail "$n of $checked citation(s) resolve to no decision record:$dangling"
  for ref in $dangling; do
    # ⚠️ WHERE, not just WHICH. A number alone sends the reader grepping for the
    # thing the gate has already found.
    printf '%s\n' "$FILES" | tr '\n' '\0' | xargs -0 grep -IlE "$ref\\b" 2>/dev/null \
      | head -3 | sed "s|^|           $ref cited in |"
  done
  echo "         Either the record was never delivered onto this branch -- check"
  echo "         other branches for $DECISIONS/ -- or the citation names a"
  echo "         decision nobody made, which is worse: it reads as settled."
  finish
fi

ok "$checked citation(s) all resolve, against $(printf '%s\n' $have | wc -w | tr -d ' ') records (scope: full)"
finish
