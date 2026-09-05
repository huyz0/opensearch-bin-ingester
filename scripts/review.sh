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
# ⚠️ The staged TREE, not just its diff hash. A verify round is supposed to be
# shown only what changed since the last reviewed round, and a hash cannot
# reconstruct a tree -- so the tree object is recorded with the verdict and the
# next round diffs against it. `write-tree` writes objects git already holds for
# every staged blob, so this costs nothing and changes no ref.
STAGED_TREE=$(git write-tree 2>/dev/null || true)
OUT=".harness/review"; mkdir -p "$OUT"

case "$CMD" in
  context)
    [ -n "$TASK" ] || { echo "--task required" >&2; exit 2; }
    echo "=== TASK $TASK ==="
    # ⚠️ ANCHORED TO THE ID COLUMN. `grep -F "$TASK"` substring-matched, so the
    # packet for M4.7 carried M4.7a and M4.7b too -- 11,620 bytes for a row of
    # about 1,000, and three sets of acceptance criteria with nothing saying
    # which one binds the diff. The dots are escaped because M4.7 must not match
    # M4x7.
    TASK_RE=$(printf '%s' "$TASK" | sed 's/\./\\./g')
    # ⚠️ The archive too: a follow-up commit names a task whose row has already
    # moved out of backlog.md, and a packet that cannot show the row leaves the
    # reviewer reconstructing intent from the diff alone.
    grep -hE "^\| *${TASK_RE} *\|" docs/internal/product/backlog*.md 2>/dev/null \
      || echo "(no row whose ID column is exactly $TASK)"
    # The row is a summary; the essay, if there is one, lives here.
    if [ -f docs/internal/product/backlog-notes.md ]; then
      awk -v id="$TASK" '
        $0 == "### " id { p = 1; print; next }
        /^### / { p = 0 }
        p { print }' docs/internal/product/backlog-notes.md
    fi
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
    # ⚠️ WHAT IS PRINTED HERE IS THE INSTRUCTION; WHY IT EXISTS IS A COMMENT.
    # The packet is read by an agent, twice a round, and every byte of it is
    # paid for at that rate -- so the reasoning that makes a rule credible to a
    # maintainer lives where maintainers read, and only the part that changes
    # what the reviewer DOES is echoed. Measured: this block alone was ~4 KB of
    # war story per packet.
    #
    #   * The tree is issued rather than improvised because reviewers were
    #     rsync'ing scratch copies differently each time, and two of them doing
    #     that in ONE tree is how M4.3's evidence got corrupted.
    #   * It is a copy of the INDEX, not of HEAD and not of the worktree, so it
    #     holds exactly the bytes the verdict is bound to. Its own build/ --
    #     sharing one overwrites the test-results XML every pass/fail count reads.
    #   * GIT_INDEX_FILE is INHERITED, and harness tests shell out to git inside
    #     their own temp repositories, so a suite run that inherits it writes
    #     THEIR fixture paths into YOUR index. Measured twice: a private index
    #     went from hundreds of entries to a handful with the suite reporting
    #     BUILD SUCCESSFUL.
    #   * The tree IS a git repository, with an EMPTY base commit and every file
    #     STAGED on top of it, because every per-file gate resolves its input
    #     through git and gates default to delta mode -- committing it would
    #     leave `git diff --cached` empty and every gate would go green having
    #     examined nothing.
    echo "=== YOUR OWN TREE TO MUTATE IN (do not mutate the repository) ==="
    echo "Both reviewers run CONCURRENTLY and both verify by mutating. Take your"
    echo "own copy of the STAGED bytes and do every mutation, build and test in it:"
    echo
    echo "    TREE=\$(./scripts/review-tree.sh <your-role>) && cd \"\$TREE\""
    echo "    env -u GIT_INDEX_FILE ./gradlew -p buildSrc test"
    echo
    echo "⚠️ ALWAYS unset GIT_INDEX_FILE for the build: it is inherited, and a"
    echo "   suite run that inherits it rewrites the index you are reviewing."
    echo "⚠️ Restore a file with \`git checkout -- <path>\`. NEVER"
    echo "   \`git checkout HEAD -- <path>\` (HEAD is an EMPTY commit) and NEVER"
    echo "   \`git reset --hard\`, which would delete every file in your tree."
    echo "⚠️ Record your verdict, and read \`.harness/\`, back in the REPOSITORY."
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
    echo
    echo "Round one FINDS, round two VERIFIES. A blocking finding in round two"
    echo "means the commit is TOO BIG -- it is split, not reviewed a third time."
    echo
    echo "⚠️ ONLY 'blocking' and 'major' block a commit. A 'pass' carrying"
    echo "'minor' findings LANDS -- minors go in the commit body or become a"
    echo "backlog row. Fixing a minor is permitted and USUALLY WRONG: the next"
    echo "round's surface is the prose the fix just added. One task reached"
    echo "ELEVEN rounds that way, rounds 9-11 fixing minors that never blocked."
    echo
    echo "So report severity honestly, and do not hunt for minors to justify the"
    echo "round. An empty findings list is a valid and expected outcome."
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
    # ⚠️ CACHED BY THE STAGED HASH, which makes it a cache and not a skip: a
    # gate's answer is a function of the bytes it judged, and BOTH roles build a
    # packet for the SAME hash. Every round therefore paid for two identical
    # runs of the whole suite, one of them carrying Gradle through
    # check-module.sh. Only a PASS is cached -- a failure exits below, and the
    # fix that follows changes the hash anyway.
    GATE_CACHE="$OUT/$DIFF_SHA.gates"
    echo "=== GATES THAT ALREADY PASSED (do not re-check these) ==="
    if [ -s "$GATE_CACHE" ]; then
      cat "$GATE_CACHE"
      echo "  (cached: run once against these exact staged bytes, sha $DIFF_SHA)"
    else
      gate_failed=0
      gate_lines=""
      for g in $(grep -oE 'scripts/check-[a-z-]+\.sh' .pre-commit-config.yaml | sort -u) \
               "scripts/build-index.sh --check"; do
        case "$g" in
          */check-commit-msg.sh|*/check-test-integrity.sh) continue ;;  # need the message file
          # check-reviewed is the gate this packet exists to satisfy. Running it
          # here would fail by construction, every time.
          */check-reviewed.sh) continue ;;
        esac
        if out=$(eval "$g" 2>&1); then
          gate_lines="$gate_lines  PASSED  $(basename "$g")"$'\n'
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
      printf '%s' "$gate_lines" > "$GATE_CACHE"
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
    # ⚠️ A VERIFY ROUND GETS THE DELTA. review_delta.py has promised this in its
    # own docstring since it was written -- "the delta since the last reviewed
    # hash" -- while this script printed `git diff --cached` in full anyway, under
    # a heading that said FILES TOUCHED SINCE THE LAST REVIEWED ROUND above a
    # command that lists every staged file. Round two of M0.31 re-read ~200 lines
    # to confirm three fixes; round five re-read them again.
    #
    # ⚠️ AND THE WHOLE DIFF IS STILL NAMED, because this is a REDUCTION in what a
    # reviewer sees. Less review is never the failure-safe default: with no prior
    # tree recorded, or on round one, the whole diff is what is printed.
    PRIOR_TREE=""
    if [ -n "$STAGED_TREE" ]; then
      PRIOR_TREE=$(python3 scripts/review_delta.py --prior-tree "$TASK" "$DIFF_SHA" 2>/dev/null || true)
    fi
    if [ -n "$PRIOR_TREE" ]; then
      echo "=== WHAT CHANGED SINCE THE LAST REVIEWED ROUND (staged sha256 $DIFF_SHA) ==="
      git diff "$PRIOR_TREE" "$STAGED_TREE"
      echo
      echo "=== THE WHOLE DIFF IS STILL AVAILABLE ==="
      echo "Everything above is what changed since the round that already carries"
      echo "a verdict. When the delta does not stand on its own, read all of it:"
      echo
      echo "    git diff --cached"
    else
      echo "=== STAGED DIFF (sha256 $DIFF_SHA) ==="
      git diff --cached
    fi
    ;;
  record)
    [ -n "$FILE" ] && [ -f "$FILE" ] || { echo "--file <verdict.json> required" >&2; exit 2; }
    case "$ROLE" in reviewer|test-reviewer) ;; *) echo "--role reviewer|test-reviewer required" >&2; exit 2 ;; esac
    command -v python3 >/dev/null || { echo "python3 required" >&2; exit 2; }
    python3 - "$FILE" "$DIFF_SHA" "$TASK" "$OUT" "$ROLE" "$STAGED_TREE" <<'PY'
import json, sys, os
path, sha, task, out, role, tree = sys.argv[1:7]
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
# ⚠️ The tree the verdict is bound to, so the NEXT round can be shown only what
# changed since this one. Absent (empty) rather than guessed when git could not
# write one -- review_delta.py then reports no prior tree and the caller falls
# back to the whole diff.
if tree:
    v['staged_tree'] = tree
json.dump(v, open(os.path.join(out, '%s.%s.json' % (sha, role)), 'w'), indent=2)
print('  \033[32mok\033[0m   %s verdict recorded for %s (%s)' % (role, sha[:12], v['verdict']))
PY
    ;;
  *) echo "usage: review.sh context|record ..." >&2; exit 2 ;;
esac
