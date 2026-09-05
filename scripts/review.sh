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
    # ⚠️ M0.57. Verifying by MUTATION is what finds the majors here, so the
    # tree to mutate is issued rather than improvised: reviewers were already
    # rsync'ing scratch copies into their own scratchpads, differently each
    # time, and two of them doing that in ONE tree is how M4.3's evidence got
    # corrupted. Printed for both roles because the packet serves both.
    echo "=== YOUR OWN TREE TO MUTATE IN (do not mutate the repository) ==="
    echo "The two reviewers run CONCURRENTLY and both verify by mutating."
    echo "Materialise your own copy of the STAGED bytes, and do every"
    echo "mutation, build and test run inside it:"
    echo
    echo "    TREE=\$(./scripts/review-tree.sh <your-role>) && cd \"\$TREE\""
    echo
    echo "It is a copy of the INDEX, not of HEAD and not of the worktree, so"
    echo "it holds exactly the bytes this verdict is bound to. It has its own"
    echo "build/ -- sharing one would overwrite the test-results XML every"
    echo "pass/fail count reads."
    echo
    echo "⚠️ RUN THE BUILD WITH THE INDEX UNSET, ALWAYS:"
    echo
    echo "    env -u GIT_INDEX_FILE ./gradlew -p buildSrc test"
    echo
    echo "GIT_INDEX_FILE is INHERITED, and harness tests shell out to git"
    echo "inside their own temp repositories -- so a suite run that inherits"
    echo "it writes THEIR fixture paths into YOUR index. Measured twice on"
    echo "this very task: a private index went from hundreds of entries to a"
    echo "handful, with the suite reporting BUILD SUCCESSFUL, and the"
    echo "reviewed bytes were only recoverable because a tree had been"
    echo "materialised beforehand."
    echo
    echo "⚠️ The tree IS a git repository, because every per-file gate resolves"
    echo "its input through git and would otherwise pass having examined"
    echo "nothing. Its shape matters to you: an EMPTY base commit, and every"
    echo "file STAGED on top of it. Gates default to delta mode, whose input is"
    echo "\`git diff --cached\`, so staging is what makes them see the tree;"
    echo "committing it would leave that delta empty and every gate would go"
    echo "green having examined nothing."
    echo
    echo "⚠️ SO RESTORE A FILE WITH \`git checkout -- <path>\`, never with"
    echo "\`git checkout HEAD -- <path>\` (HEAD is the EMPTY commit and knows no"
    echo "paths), and NEVER with \`git reset --hard\`, which resolves to that"
    echo "empty commit and would delete every file in your tree mid-review."
    echo "It carries no HISTORY and no remote. Record your verdict, and read"
    echo "\`.harness/\`, back in the REPOSITORY."
    echo
    echo "=== ROUNDS AND WHAT ACTUALLY BLOCKS ==="
    # ⚠️ PRINTED, not left to memory. Rule 11 -- a `minor` on a `pass` lands --
    # already existed as prose in review/SKILL.md and was violated eight times
    # on one task, because it is read at the START of a task while the verdict
    # arrives many turns later saying only "minor". A rule an agent has to
    # RECALL at the deciding moment is the weakest rung on non-negotiable 9's
    # ladder; this is the same rule as a line the reader cannot miss.
    # ⚠️ COUNTED BY TASK, NOT BY HASH. The previous form asked
    # `review_rounds.py "$DIFF_SHA"`, which resolves the task from a verdict FOR
    # THAT HASH -- and no verdict for it exists yet, because generating this
    # packet is how one gets made. So it printed 0 and this line said "round 1 of
    # 2" on EVERY round, for the life of the gate. Measured: a round-3 packet
    # said round 1 with six verdicts for the task already on disk. The number
    # that decides whether rule 12 is about to bite was wrong in the one document
    # the reviewer reads, so no reviewer ever applied the split remedy, and two
    # tasks in one session reached ten and five rounds.
    ROUND_N=$(python3 scripts/review_rounds.py --for-task "$TASK" 2>/dev/null || echo 0)
    ROUND=$((ROUND_N + 1))
    # ⚠️ A BUDGET THAT INTERRUPTS THE BEHAVIOUR, not just the outcome. The cap
    # refuses the COMMIT; nothing refused the next ROUND, so an author could
    # keep spending pairs of agents indefinitely and each round felt justified
    # because it found something. It always finds something: reviewers verify by
    # mutation, and the marginal round is never empty. What decays is SEVERITY --
    # on the ten-round task, rounds 7-10 found only false sentences in the prose
    # describing the review, each correction voiding the verdicts and buying the
    # next round. This makes continuing a decision rather than a default.
    if [ "$ROUND" -gt "${REVIEW_ROUND_BUDGET:-4}" ]; then
      echo "!!! Round $ROUND for $TASK exceeds the budget of ${REVIEW_ROUND_BUDGET:-4}."
      echo "!!!"
      echo "!!! Rule 12's remedy is SPLIT, not another round. If it genuinely"
      echo "!!! cannot be split, argue the cap with a staged 'rounds:$TASK' line"
      echo "!!! in baselines/review.txt -- and if the last rounds have been"
      echo "!!! finding prose rather than defects, the honest move is to DELETE"
      echo "!!! the prose that keeps being wrong rather than correct it again."
      echo "!!!"
      echo "!!! Deliberate override: REVIEW_ROUND_BUDGET=$ROUND $0 context --task $TASK"
      exit 1
    fi
    echo "This is round $ROUND of 2 for $TASK."
    echo "review.md rule 12: round one finds, round two verifies. A blocking"
    echo "finding in round two means the commit is TOO BIG -- it is split, not"
    echo "reviewed a third time."
    echo
    echo "⚠️ ONLY 'blocking' and 'major' block a commit (check-reviewed.sh)."
    echo "A 'pass' carrying 'minor' findings LANDS: they are recorded in the"
    echo "commit body or become a backlog row. Rule 11 -- fixing a minor is"
    echo "'permitted and usually wrong, because the new round's surface is the"
    echo "prose the fix just added'. Measured here: one task reached ELEVEN"
    echo "rounds that way, rounds 9-11 fixing minors that never blocked."
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
