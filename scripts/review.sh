#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Build the review packet, and record a verdict bound to the staged diff by hash.
#   review.sh context --task <ID>
#   review.sh record  --file <verdict.json> --task <ID> --role <reviewer|test-reviewer>
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
if ! STAGED_TREE=$(git write-tree 2>/dev/null); then
  echo "!!! Could not create a tree for the staged bytes; refusing to build a packet." >&2
  exit 2
fi
OUT=".harness/review"; mkdir -p "$OUT"

# Pre-commit entries may be routed through the portable launcher.  Keep the
# command intact for execution, but normalize the wrapped script for validation
# and reporting below.
gate_script_path() {
  local command="$1"
  case "$command" in
    python\ scripts/run-gate.py\ scripts/*.sh*)
      printf '%s\n' "$command" | sed -n 's#^python scripts/run-gate.py \(scripts/[^ ]*\.sh\).*#\1#p'
      ;;
    "./gradlew.bat gates") printf '%s\n' "gradle-gates" ;;
    "./gradlew.bat checkTdd") printf '%s\n' "scripts/check-tdd.sh" ;;
    "./gradlew.bat checkReviewed") printf '%s\n' "scripts/check-reviewed.sh" ;;
    *) printf '%s\n' "${command%% *}" ;;
  esac
}

case "$CMD" in
  context)
    [ -n "$TASK" ] || { echo "--task required" >&2; exit 2; }
    # Count completed verdicts before emitting any packet content or running any
    # gate. The reviewer's scarce work must not be paid for before the cap can
    # refuse the next round.
    if ! ROUND_N=$(python3 scripts/review_rounds.py --for-task "$TASK" 2>/dev/null); then
      echo "!!! Could not determine completed review rounds for $TASK; refusing to open a packet." >&2
      exit 2
    fi
    case "$ROUND_N" in
      ''|*[!0-9]*)
        echo "!!! Round counter returned a non-negative integer: $ROUND_N; refusing to open a packet." >&2
        exit 2
        ;;
    esac
    ROUND=$((ROUND_N + 1))
    BUDGET="${REVIEW_ROUND_BUDGET:-3}"
    case "$BUDGET" in
      ''|*[!0-9]*)
        echo "!!! REVIEW_ROUND_BUDGET must be a non-negative integer: $BUDGET" >&2
        exit 2
        ;;
    esac
    if [ "$ROUND" -gt "$BUDGET" ]; then
      echo "!!! Round $ROUND for $TASK exceeds the budget of $BUDGET."
      echo "!!! Rule 12's remedy is SPLIT, not another round. If it genuinely"
      echo "!!! cannot be split, use an explicit REVIEW_ROUND_BUDGET override."
      echo "!!! Deliberate override: REVIEW_ROUND_BUDGET=$ROUND $0 context --task $TASK"
      exit 1
    fi
    echo "=== TASK $TASK ==="
    grep -F "$TASK" docs/internal/product/backlog.md 2>/dev/null || echo "(not found in backlog)"
    echo
    echo "=== STANDARDS THE REVIEWER MUST READ (selected from paths, not by the author) ==="
    ./scripts/which-standards.sh
    echo
    echo "=== WHAT TO LOOK AT (selected from paths, not improvised per commit) ==="
    ./scripts/review-lenses.sh
    echo
    echo "=== ROUNDS AND WHAT ACTUALLY BLOCKS ==="
    # ⚠️ PRINTED, not left to memory. Rule 11 -- a `minor` on a `pass` lands --
    # already existed as prose in review/SKILL.md and was violated eight times
    # on one task, because it is read at the START of a task while the verdict
    # arrives many turns later saying only "minor". A rule an agent has to
    # RECALL at the deciding moment is the weakest rung on non-negotiable 9's
    # ladder; this is the same rule as a line the reader cannot miss.
    echo "This is round $ROUND of $BUDGET for $TASK."
    echo "review.md rule 12: round one finds, round two fixes and finds in the"
    echo "fix, round three verifies. A blocking finding in round THREE means"
    echo "the commit is TOO BIG -- it is split, not reviewed a fourth time."
    echo
    echo "⚠️ ONLY 'blocking' and 'major' block a commit (check-reviewed.sh)."
    echo "A 'pass' carrying 'minor' findings LANDS: they are recorded in the"
    echo "commit body or become a backlog row. Rule 11 -- opening a round for a"
    echo "minor is FORBIDDEN, not merely discouraged, and there is no fix-it-"
    echo "before-recording escape either: neither counter can see one. Measured"
    echo "here: one task reached EIGHTEEN rounds, and across four M5 tasks every"
    echo "round past the second was opened for a finding that never blocked."
    echo
    echo "So: report severity honestly, and do not hunt for minors to justify"
    echo "the round. An empty findings list is a valid and expected outcome."
    echo
    # ⚠️ A verify round gets the DELTA, not the whole diff again. Returns
    # non-zero on round one, when there is no prior round to delta against.
    if python3 scripts/review_delta.py "$TASK" "$DIFF_SHA" 2>/dev/null; then
      echo "=== FILES TOUCHED SINCE THE LAST REVIEWED ROUND ==="
      git diff --cached --name-status
      echo
    fi
    # Run them. The previous version printed a hardcoded four-item list under
    # this heading without invoking anything -- which is precisely the claim
    # non-negotiable 4 exists to forbid, made by the script that serves the
    # reviewer.
    echo "=== GATES THAT ALREADY PASSED (do not re-check these) ==="
    # A review packet must not pay for a manual-stage gate. The mutation gate is
    # deliberately manual because it costs tens of seconds per module; treating
    # every entry in the config as a pre-commit gate made each reviewer packet
    # pay that cost again. Parse hook blocks so this stays correct if another
    # manual gate is added.
    gate_scripts=$(awk '
      function emit() { if (entry != "" && precommit) print entry }
      function reset() { entry=""; manual=0; precommit=1; in_stages=0 }
      BEGIN { reset() }
      /- id:/ { emit(); reset(); next }
      /entry:/ {
        entry=$0
        sub(/^[^:]*:[[:space:]]*/, "", entry)
        next
      }
      /stages:/ {
        manual=($0 ~ /manual/)
        precommit=($0 ~ /pre-commit/)
        in_stages=1
        next
      }
      in_stages && /^[[:space:]]+-[[:space:]]/ {
        if ($0 ~ /manual/) manual=1
        if ($0 ~ /pre-commit/) precommit=1
        next
      }
      in_stages && !/^[[:space:]]/ { in_stages=0 }
      END { emit() }
    ' .pre-commit-config.yaml \
      | sed -e 's/^"//' -e 's/"$//' -e "s/^'//" -e "s/'$//" \
      | sort -u)
    if printf '%s\n' "$gate_scripts" | grep -Eq 'scripts/build-index\.sh --check$'; then
      gate_commands="$gate_scripts"
    else
      gate_commands="$gate_scripts"$'\n'"scripts/build-index.sh --check"
    fi
    GATE_INPUTS="$STAGED_TREE GATE_SCOPE=${GATE_SCOPE:-delta} CHECK_RANGE=${CHECK_RANGE:-}"
    EXPECTED_CACHE=""
    while IFS= read -r g; do
      [ -n "$g" ] || continue
      gate_path="$(gate_script_path "$g")"
      case "$gate_path" in
        */check-commit-msg.sh|*/check-test-integrity.sh|*/check-reviewed.sh) continue ;;
      esac
      case "$gate_path" in
        scripts/check-*.sh|scripts/build-index.sh|gradle-gates) ;;
        *)
          echo "!!! Unsupported pre-commit entry: $g" >&2
          exit 2
          ;;
      esac
      EXPECTED_CACHE="$EXPECTED_CACHE  PASSED  $(basename "$gate_path")"$'\n'
    done <<< "$gate_commands"
    EXPECTED_CACHE="${EXPECTED_CACHE%$'\n'}"
    while IFS= read -r -d '' input; do
      input_sha=$(sha256sum "$input" | cut -d' ' -f1) || { echo "!!! Could not hash worktree gate input: $input" >&2; exit 2; }
      input_mode=$(stat -c '%a' "$input") || { echo "!!! Could not stat worktree gate input: $input" >&2; exit 2; }
      GATE_INPUTS="$GATE_INPUTS worktree:$input:$input_mode:$input_sha"
    done < <(git ls-files -co --exclude-standard -z | sort -z)
    while IFS= read -r -d '' input; do
      [ -f "$input" ] || continue
      input_sha=$(sha256sum "$input" | cut -d' ' -f1) || { echo "!!! Could not hash ignored source gate input: $input" >&2; exit 2; }
      input_mode=$(stat -c '%a' "$input") || { echo "!!! Could not stat ignored source gate input: $input" >&2; exit 2; }
      GATE_INPUTS="$GATE_INPUTS ignored-source:$input:$input_mode:$input_sha"
    done < <(git ls-files --others --ignored --exclude-standard -z -- 'src/*.kt' 'src/*.java' 'src/*.kts' '*/src/*.kt' '*/src/*.java' '*/src/*.kts' | sort -z)
    for input in .pre-commit-config.yaml; do
      [ -f "$input" ] || { echo "!!! Gate input is missing: $input" >&2; exit 2; }
      input_sha=$(sha256sum "$input" | cut -d' ' -f1) || { echo "!!! Could not hash gate input: $input" >&2; exit 2; }
      input_mode=$(stat -c '%a' "$input") || { echo "!!! Could not stat gate input: $input" >&2; exit 2; }
      GATE_INPUTS="$GATE_INPUTS $input:$input_mode:$input_sha"
    done
    while IFS= read -r input; do
      [ -n "$input" ] || continue
      input_sha=$(sha256sum "$input" | cut -d' ' -f1) || { echo "!!! Could not hash gate input: $input" >&2; exit 2; }
      GATE_INPUTS="$GATE_INPUTS $input:$input_sha"
    done < <(find scripts -type f -print | sort)
    if [ -d .harness/tdd ]; then
      while IFS= read -r input; do
        [ -n "$input" ] || continue
        input_sha=$(sha256sum "$input" | cut -d' ' -f1) || { echo "!!! Could not hash TDD evidence: $input" >&2; exit 2; }
        input_mode=$(stat -c '%a' "$input") || { echo "!!! Could not stat TDD evidence: $input" >&2; exit 2; }
        GATE_INPUTS="$GATE_INPUTS tdd:$input:$input_mode:$input_sha"
      done < <(find .harness/tdd -type f -print | sort)
    fi
    if [ -d .harness/review ]; then
      while IFS= read -r input; do
        [ -n "$input" ] || continue
        input_sha=$(sha256sum "$input" | cut -d' ' -f1) || { echo "!!! Could not hash review evidence: $input" >&2; exit 2; }
        input_mode=$(stat -c '%a' "$input") || { echo "!!! Could not stat review evidence: $input" >&2; exit 2; }
        GATE_INPUTS="$GATE_INPUTS review:$input:$input_mode:$input_sha"
      done < <(find .harness/review -type f ! -name '*.gates' ! -name '*.lock' ! -name '*.tmp.*' -print | sort)
    fi
    GATE_KEY=$(printf '%s' "$GATE_INPUTS" | sha256sum | cut -d' ' -f1)
    GATE_CACHE="$OUT/$STAGED_TREE.$GATE_KEY.gates"
    GATE_LOCK="$OUT/$STAGED_TREE.gates.lock"
    GATE_TMP=""
    GATE_LOCK_DIR=""
    LOCK_TIMEOUT="${REVIEW_GATE_LOCK_TIMEOUT:-120}"
    case "$LOCK_TIMEOUT" in ''|*[!0-9]*) echo "!!! REVIEW_GATE_LOCK_TIMEOUT must be a non-negative integer: $LOCK_TIMEOUT" >&2; exit 2;; esac
    if command -v flock >/dev/null 2>&1; then
      exec 9>"$GATE_LOCK"
      if ! flock -w "$LOCK_TIMEOUT" 9; then echo "!!! Timed out waiting for the gate run for staged tree $STAGED_TREE." >&2; exit 1; fi
      trap 'if [ -n "$GATE_TMP" ]; then rm -f "$GATE_TMP"; fi; flock -u 9; exec 9>&-' EXIT
    else
      # Git for Windows does not ship util-linux's flock.  mkdir is atomic on
      # the local filesystems this harness supports, so use it as the fallback.
      GATE_LOCK_DIR="${GATE_LOCK}.d"
      lock_elapsed=0
      while ! mkdir "$GATE_LOCK_DIR" 2>/dev/null; do
        if [ "$lock_elapsed" -ge "$LOCK_TIMEOUT" ]; then
          echo "!!! Timed out waiting for the gate run for staged tree $STAGED_TREE." >&2
          exit 1
        fi
        sleep 1
        lock_elapsed=$((lock_elapsed + 1))
      done
      trap 'if [ -n "$GATE_TMP" ]; then rm -f "$GATE_TMP"; fi; if [ -n "$GATE_LOCK_DIR" ]; then rmdir "$GATE_LOCK_DIR" 2>/dev/null || true; fi' EXIT
    fi
    if [ -s "$GATE_CACHE" ]; then
      CACHED_GATES=$(cat "$GATE_CACHE" 2>/dev/null) || { echo "!!! Could not read the gate cache; refusing to use stale evidence." >&2; exit 1; }
      [ "$CACHED_GATES" = "$EXPECTED_CACHE" ] || { echo "!!! Gate cache is incomplete or stale; refusing to use it." >&2; exit 1; }
      printf '%s\n' "$CACHED_GATES"
      echo "  (cached: run once against these exact staged bytes, sha $DIFF_SHA)"
      gate_failed=0
    else
      gate_failed=0
      gate_lines=""
      while IFS= read -r g; do
        [ -n "$g" ] || continue
        gate_path="$(gate_script_path "$g")"
        case "$gate_path" in scripts/check-*.sh|scripts/build-index.sh|gradle-gates) ;; *) echo "!!! Unsupported pre-commit entry: $g" >&2; exit 2;; esac
        case "$gate_path" in */check-commit-msg.sh|*/check-test-integrity.sh|*/check-reviewed.sh) continue;; esac
        case "$g" in
          "./gradlew.bat gates") gate_command="./gradlew gates" ;;
          "./gradlew.bat checkTdd") gate_command="bash scripts/check-tdd.sh" ;;
          *) gate_command="$g" ;;
        esac
        case "$g" in
          python\ scripts/run-gate.py\ *) gate_command="${PYTHON3:-python3}${g#python}" ;;
          scripts/*.sh*) gate_command="bash $g" ;;
        esac
        if out=$(eval "$gate_command" 2>&1); then
          gate_lines="$gate_lines  PASSED  $(basename "$gate_path")"$'\n'
        else
          echo "  FAILED  $(basename "$gate_path")"
          echo "$out" | sed 's/^/          /'
          gate_failed=1
        fi
      done <<< "$gate_commands"
      if [ "$gate_failed" -ne 0 ]; then
        echo; echo "!!! A deterministic gate is failing. Fix it before spending a review:"; echo "!!! the reviewer's attention is the scarce thing, and a script already"; echo "!!! knows the answer to whatever it would find."; exit 1
      fi
      GATE_TMP=$(mktemp "$GATE_CACHE.tmp.XXXXXX")
      if ! printf '%s' "$gate_lines" > "$GATE_TMP"; then echo "!!! Could not write the gate cache; refusing to use partial evidence." >&2; exit 1; fi
      if ! mv -f "$GATE_TMP" "$GATE_CACHE"; then echo "!!! Could not publish the gate cache; refusing to use partial evidence." >&2; exit 1; fi
      GATE_TMP=""
      printf '%s' "$gate_lines"
    fi
    echo
    # Deletions and renames first, because they are what a mis-staged index looks
    # like. Twice in one session an index was staged wrong -- once leaving 24
    # stale files in, once staging 44 deletions that would have reverted the
    # previous commit -- and neither was visible in a diff read top to bottom.
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
