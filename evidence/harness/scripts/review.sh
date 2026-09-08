#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Build the review packet, and record a verdict bound to the staged diff by hash.
#   review.sh context --task <ID>
#   review.sh record  --file <verdict.json> --task <ID>
# It does NOT spawn the reviewer: no script can do that across tools. It owns the
# hash, the packet and the artifact; the agent owns the judgement.
source "$(dirname "$0")/lib.sh"
cd "$ROOT"
CMD="${1:-}"; shift || true
TASK=""; FILE=""; ROLE=""
while [ $# -gt 0 ]; do
  case "$1" in
    --task) TASK="$2"; shift 2 ;;
    --file) FILE="$2"; shift 2 ;;
    --role) ROLE="$2"; shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done
DIFF_SHA=$(git diff --cached | sha256sum | cut -d' ' -f1)
OUT=".harness/review"; mkdir -p "$OUT"

case "$CMD" in
  context)
    [ -n "$TASK" ] || { echo "--task required" >&2; exit 2; }
    echo "=== TASK $TASK ==="
    grep -F "$TASK" docs/internal/product/backlog.md 2>/dev/null || echo "(not found in backlog)"
    echo
    echo "=== STANDARDS THE REVIEWER MUST READ (selected from paths, not by the author) ==="
    ./scripts/which-standards.sh
    echo
    echo "=== WHAT TO LOOK AT (selected from paths, not improvised per commit) ==="
    ./scripts/review-lenses.sh
    echo
    echo "=== ROUNDS ==="
    echo "review.md rule 12: TWO rounds. Round one finds, round two verifies."
    echo "If round two still carries a blocking finding, the commit is too big --"
    echo "the author splits it. Do not expect a round three."
    echo
    # Run them. The previous version printed a hardcoded four-item list under
    # this heading without invoking anything -- which is precisely the claim
    # non-negotiable 4 exists to forbid, made by the script that serves the
    # reviewer.
    echo "=== GATES THAT ALREADY PASSED (do not re-check these) ==="
    gate_failed=0
    for g in $(grep -oE 'scripts/check-[a-z-]+\.sh' .pre-commit-config.yaml | sort -u) \
             "scripts/build-index.sh --check"; do
      case "$g" in
        */check-commit-msg.sh|*/check-test-integrity.sh) continue ;;  # need the message file
        # check-reviewed is the gate this packet exists to satisfy. Running it
        # here would fail by construction, every time.
        */check-reviewed.sh) continue ;;
      esac
      if out=$(eval "$g" 2>&1); then
        echo "  PASSED  $(basename "$g")"
      else
        echo "  FAILED  $(basename "$g")"
        echo "$out" | sed 's/^/          /'
        gate_failed=1
      fi
    done
    if [ "$gate_failed" -ne 0 ]; then
      echo
      echo "!!! A deterministic gate is failing. Fix it before spending a review:"
      echo "!!! the reviewer's attention is the scarce thing, and a script already"
      echo "!!! knows the answer to whatever it would find."
      exit 1
    fi
    echo
    # Deletions and renames first, because they are what a mis-staged index looks
    # like. Twice in one session an index was staged wrong -- once leaving 24
    # stale files in, once staging 44 deletions that would have reverted the
    # previous commit -- and neither was visible in a diff read top to bottom.
    mark_review_inflight
    echo "=== STAGED FILES BY STATUS ==="
    git diff --cached --name-status | awk '{print $1}' | sort | uniq -c \
      | awk '{printf "  %-4s %s\n", $2, $1}'
    dels=$(git diff --cached --name-status | grep -c '^D' || true)
    if [ "${dels:-0}" -gt 0 ]; then
      echo "  ⚠️  $dels DELETION(S) -- confirm each is intended by this task:"
      git diff --cached --name-status | grep '^D' | head -20 | sed 's/^/      /'
    fi
    echo
    echo "=== STAGED DIFF (sha256 $DIFF_SHA) ==="
    git diff --cached
    ;;
  record)
    warn_if_index_moved
    [ -n "$FILE" ] && [ -f "$FILE" ] || { echo "--file <verdict.json> required" >&2; exit 2; }
    case "$ROLE" in reviewer|test-reviewer) ;; *) echo "--role reviewer|test-reviewer required" >&2; exit 2 ;; esac
    command -v python3 >/dev/null || { echo "python3 required" >&2; exit 2; }
    python3 - "$FILE" "$DIFF_SHA" "$TASK" "$OUT" "$ROLE" <<'PY'
import json, sys, os
path, sha, task, out, role = sys.argv[1:6]
v = json.load(open(path))
errs = []
if v.get('diff_sha256') != sha:
    errs.append('diff_sha256 is stale: verdict says %r, staged bytes hash to %r'
                % (v.get('diff_sha256'), sha))
if v.get('verdict') not in ('pass', 'changes-requested'):
    errs.append("verdict must be 'pass' or 'changes-requested'")
for i, f in enumerate(v.get('findings', [])):
    if not f.get('failure_scenario'):
        errs.append('finding %d has no failure_scenario -- that makes it a style opinion' % i)
    if f.get('severity') not in ('blocking', 'major', 'minor'):
        errs.append('finding %d: severity must be blocking|major|minor' % i)
# review.md rules 9-10: a blocking or major finding is fixed or argued. A
# 'pass' carrying either was accepted here and then never checked against
# baselines/review.txt by check-reviewed.sh, so the finding vanished into a
# gitignored directory.
heavy = sorted({f.get('severity') for f in v.get('findings', [])
                if f.get('severity') in ('blocking', 'major')})
if v.get('verdict') == 'pass' and heavy:
    errs.append("a 'pass' cannot coexist with a %s finding -- review.md rules 9-10"
                % '/'.join(heavy))
if errs:
    for e in errs:
        print('  \033[31mFAIL\033[0m ' + e)
    sys.exit(1)
v['task'] = task; v['role'] = role
json.dump(v, open(os.path.join(out, '%s.%s.json' % (sha, role)), 'w'), indent=2)
print('  \033[32mok\033[0m   %s verdict recorded for %s (%s)' % (role, sha[:12], v['verdict']))
PY
    ;;
  *) echo "usage: review.sh context|record ..." >&2; exit 2 ;;
esac
