#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Select the review lenses a change actually warrants, from its paths.
#
#   review-lenses.sh [paths...]     (default: the staged diff)
#
# ⚠️ This routes ATTENTION, never enforcement. Every deterministic gate runs on
# every commit regardless -- all ten cost 412 ms together, so skipping one buys
# nothing and risks a gate that enforces nothing. What is expensive is agent
# review: 7-13 minutes a round. This file decides what those minutes are spent
# on, so a brief is generated from the diff rather than improvised per commit.
#
# The default was a hand-written 15-point brief per review, which is how a
# reviewer ended up fuzzing 20,295 files of an unrelated upstream checkout.
source "$(dirname "$0")/lib.sh"
cd "$ROOT"

paths="${*:-$(git diff --cached --name-only)}"
[ -n "$paths" ] || { echo "(nothing staged)"; exit 0; }

emit() { printf '%s\n' "$1"; }
seen=""
lens() {
  case "$seen" in *"[$1]"*) return ;; esac
  seen="$seen[$1]"
  emit ""
  emit "### Lens: $1"
  emit "$2"
}

case "$paths" in
  *scripts/*|*buildSrc/*|*.pre-commit-config.yaml*|*.gradle.kts*)
    lens "a gate that reports success while checking nothing" \
"This change touches enforcement. Every defect of this class found in this
repository so far looked green: a deny-list that could never match its own input,
two gates that printed FAIL and exited 0, a budget that summed a key the runtime
does not read, a parser blinded by one char literal.
- Construct the input each check must REJECT and confirm it rejects it.
- Confirm the check is wired (a task nothing depends on is a preference).
- Confirm it cannot pass vacuously: empty input set, absent report, missing file.
- Check the exit status actually propagates." ;;
esac

cost_code=$(printf '%s\n' $paths \
  | grep -E '^(binstore-spi|binstore-backends|format|ingest|client|plugin|sequencer)/src/.*\.(java|kt)$' \
  | wc -l | tr -d ' ')
if [ "$cost_code" != "0" ]; then
    lens "object-store request rate" \
"cost.md: request rate may scale with segments, AZs and nodes -- NEVER with
records, shards, partitions, indices or documents. This is the architecture in
one line, and it is invisible to every other gate.
- Every get/put/list/stat added or moved inside a loop is a finding until proven otherwise.
- Does an idle consumer issue any request at all?"
fi

case "$paths" in
  *src/test/*|*src/integrationTest/*|*src/clusterTest/*|*Test.java*|*testFixtures*)
    lens "would this test fail if the code were wrong" \
"Mentally mutate the production code each test covers: flip a boundary, negate a
condition, return a constant, drop a side effect. If the test still passes, name
the surviving mutation -- that finding is worth more than every style comment." ;;
esac

case "$paths" in
  *licenses/*|*gradle/libs.versions.toml*|*DependencyLicenses*)
    lens "supply chain" \
"A dependency enters the build here.
- Can an artifact reach the build WITHOUT passing the gate (literal coordinate,
  compileOnly, platform/BOM, a transitive of a transitive)?
- Is the licence claim machine-checkable, or is it prose someone must read?" ;;
esac

case "$paths" in
  *docs/internal/product/decisions/*|*wire-format*|*format/src/main*)
    lens "expensive to reverse" \
"A wire format or a recorded decision changes here. Does every reader, writer,
fake and golden file change with it, in this commit?" ;;
esac

# Docs-only changes get the cheap lens and nothing else.
code=$(printf '%s\n' $paths | grep -cE '\.(java|kt|kts|sh|py)$|\.pre-commit-config' || true)
if [ "$code" = "0" ]; then
  lens "documentation consistency" \
"No code changed. Do NOT review for code defects.
- Does any claim here contradict a standard, an ADR, or the generated indexes?
- Is anything stated in the present tense that does not exist yet? That is the
  non-negotiable-4 failure, and it is the most common defect in a docs commit."
fi

emit ""
emit "### Out of scope for this review"
emit "- Anything the packet lists as already passed -- a script owns it."
emit "- Any path outside this repository. \`../\` and \`.tmp/\` are not ours."
emit "- Lenses not listed above: the change does not touch them."
