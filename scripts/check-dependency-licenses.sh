#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# build.md rule 2: no GPL/AGPL/SSPL/EUPL/CDDL-1.0/BUSL dependency.
#
# ⚠️ This is the FAST half. The authoritative gate is `./gradlew dependencyLicenses`,
# which additionally verifies a pinned SHA-1 per jar against the jars actually
# resolved, and runs as part of `check`. This script checks only what can be
# checked from the tree in milliseconds, so a pre-commit hook does not need a JVM.
#
# The mechanism is OpenSearch's: a licences directory committed to the tree, with
# a pinned sha and a licence text per dependency. It replaced a task that scraped
# POM <licenses> over the network, which emitted prose where the deny-list
# expected SPDX ids, never followed <parent>, and could not be cached.
source "$(dirname "$0")/lib.sh"
cd "$ROOT"
hdr "check-dependency-licenses"

DIR=licenses
MANIFEST="$DIR/SPDX.txt"
# Exact SPDX identifiers, never substrings: "GPL-2.0" as a substring also matches
# LGPL-2.0, which is a different licence and one this project has not ruled out.
DENIED="GPL-1.0-only GPL-1.0-or-later GPL-2.0-only GPL-2.0-or-later GPL-3.0-only
GPL-3.0-or-later GPL-2.0 GPL-3.0 AGPL-1.0-only AGPL-1.0-or-later AGPL-3.0-only
AGPL-3.0-or-later AGPL-3.0 SSPL-1.0 EUPL-1.0 EUPL-1.1 EUPL-1.2 CDDL-1.0 BUSL-1.1"

if [ ! -f "$MANIFEST" ]; then
  if [ -f settings.gradle.kts ]; then
    fail "$MANIFEST is missing, but a Gradle build exists"
    echo "         Every dependency needs a declared SPDX id and a committed licence."
    echo "         Run: ./gradlew updateShas && ./gradlew dependencyLicenses"
  else
    warn "no build yet -- dependency licences unenforced"
  fi
  finish
fi

n=0
while read -r prefix spdx _; do
  case "$prefix" in ''|\#*) continue ;; esac
  n=$((n+1))
  [ -f "$DIR/$prefix-LICENSE.txt" ] || fail "$MANIFEST names '$prefix' with no $DIR/$prefix-LICENSE.txt"
  for d in $DENIED; do
    [ "$spdx" = "$d" ] && {
      fail "'$prefix' is $spdx, which this project cannot ship (build.md rule 2)"
      echo "         Replace the dependency, or record an exception with the reviewer's"
      echo "         name in baselines/licenses.txt."
    }
  done
done < "$MANIFEST"

# A licence text with no declared identifier is unreviewed, not clean.
for f in "$DIR"/*-LICENSE.txt; do
  [ -e "$f" ] || continue
  p=$(basename "$f"); p=${p%-LICENSE.txt}
  grep -qE "^[[:space:]]*$p[[:space:]]" "$MANIFEST" \
    || fail "$f has no SPDX identifier in $MANIFEST -- add: $p <SPDX-ID>"
done

[ "$FAILED" -eq 0 ] && ok "$n declared licence(s), none denied (authoritative check: ./gradlew dependencyLicenses)"
finish
