#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Map staged paths to the standards a reviewer should be given.
# Selected from the paths, deliberately NOT chosen by the author.
source "$(dirname "$0")/lib.sh"
cd "$ROOT"
paths="${*:-$(git diff --cached --name-only)}"
S=docs/internal/standards
out=""
add() { case "$out" in *"$1"*) ;; *) out="$out$1"$'\n' ;; esac; }
add "$S/git.md"; add "$S/sdd.md"
case "$paths" in *.java*) add "$S/java-style.md"; add "$S/code-structure.md"; add "$S/testing.md" ;; esac
# cost.md reaches every module that can decide how many object-store requests
# happen -- which is most of them. Routing it only to *store*/*segment* paths
# handed the reviewer of `ingest/`, `client/` or `plugin/` java-style.md and
# testing.md and never the standard that IS the architecture, on exactly the
# modules where "one GET per object per node" turns into "one per shard".
case "$paths" in \
  *store*|*Store*|*s3*|*S3*|*segment*|*Segment*|*ingest*|*Ingest*|*client*|*Client*|\
  *plugin*|*Plugin*|*consumer*|*Consumer*|*reader*|*Reader*|*writer*|*Writer*|\
  *fetch*|*Fetch*|*cache*|*Cache*|*poll*|*Poll*|*format*|*Format*|*flush*|*Flush*|\
  *http*|*Http*|*bulk*|*Bulk*)
    add "$S/cost.md"; add "$S/performance.md" ;;
esac
case "$paths" in *Test*|*test*|*bench*|*Bench*) add "$S/testing.md"; add "$S/performance.md" ;; esac
case "$paths" in *lease*|*Lease*|*commit*|*Commit*|*sequencer*|*Sequencer*) add "$S/cost.md"; add "$S/security.md" ;; esac
case "$paths" in *plugin*|*Plugin*|*subscri*|*Subscri*|*grant*|*Grant*) add "$S/security.md" ;; esac
printf '%s' "$out" | sed '/^$/d'
