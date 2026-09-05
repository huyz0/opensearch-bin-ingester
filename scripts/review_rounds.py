# SPDX-License-Identifier: Apache-2.0
"""How many review rounds a task has had.

⚠️ review.md rule 12 -- "two rounds is the cap" -- was violated on 7 of 12 tasks
in one session (five rounds on one, four on two others) because NOTHING COUNTED.
A prose rule only a reviewer can uphold differs per run and dies with the
session; this makes it a predicate over the verdict files.

A round is a DISTINCT staged hash reviewed for the same task.

⚠️ THIS DOES NOT EXCLUDE A BASELINE-ONLY DIFF, and an earlier version of this
docstring claimed it did. `rounds_for` counts every distinct `diff_sha256` it
finds for the task, full stop. So staging an argument DOES move the hash and DOES
cost a round -- which is survivable because the argument is consulted before the
count is REFUSED (not before it is compared: `check-reviewed.sh` compares first and
consults the argument inside that branch), but it is not what the old wording promised. Said plainly
because a docstring describing a filter that is not there is worse than no
docstring: it tells the next reader not to look.
"""
import glob
import json
import sys


def rounds_for(sha, review_dir='.harness/review'):
    # ⚠️ EVERY VERDICT FOR THIS HASH, NOT THE FIRST ONE FOUND. Breaking on the
    # first glob hit made the task -- and therefore the round count -- resolve by
    # DIRECTORY ORDER when two roles disagreed about it. Measured: reviewer
    # recorded under a task with 3 rounds, test-reviewer under one with 1, and the
    # gate read the second and never fired the cap. Since `review.sh record` does
    # not validate `--task`, that is reachable by a typo as easily as by intent.
    #
    # ⚠️ DISAGREEMENT IS REFUSED, not resolved. Picking a winner would be choosing
    # which reviewer to believe about what they reviewed; the honest answer is that
    # the artifacts contradict each other and a human has to look.
    tasks = set()
    for f in sorted(glob.glob('%s/%s.*.json' % (review_dir, sha))):
        try:
            t = json.load(open(f)).get('task')
        except (OSError, ValueError):
            continue
        if t:
            tasks.add(t)
    if len(tasks) > 1:
        raise SystemExit(
            'verdicts for %s disagree about the task: %s -- one of them reviewed '
            'something else, or was recorded with the wrong --task' % (sha, sorted(tasks)))
    task = next(iter(tasks), None)
    if not task:
        return 0, None
    seen = set()
    for f in glob.glob('%s/*.json' % review_dir):
        try:
            d = json.load(open(f))
        except (OSError, ValueError):
            continue
        if d.get('task') == task and d.get('diff_sha256'):
            seen.add(d['diff_sha256'])
    return len(seen), task


def rounds_of_task(task, review_dir='.harness/review'):
    """Rounds recorded for a NAMED task, without needing a hash to resolve it.

    ⚠️ THIS EXISTS BECAUSE THE PACKET SAID "round 1 of 2" ON EVERY ROUND.
    `rounds_for` resolves the task from a verdict FOR THE HASH IT IS GIVEN, and
    `review.sh context` runs before any verdict for the new hash exists -- so it
    resolved nothing, printed 0, and told every reviewer it was round one. The
    one number that decides whether rule 12 is about to bite was wrong in the
    document the reviewer reads, on every round, for the life of the gate.
    Measured: a round-3 packet said `This is round 1 of 2` with six verdicts for
    that task already on disk, and reviewers therefore never applied rule 12's
    split remedy -- two tasks reached ten and five rounds that way.
    ⚠️ The gate is unaffected and stays on `rounds_for`: at COMMIT time the
    staged hash does carry verdicts, so the task resolves and the count is
    right. The defect was only ever in the packet.
    """
    seen = set()
    for f in glob.glob('%s/*.json' % review_dir):
        try:
            d = json.load(open(f))
        except (OSError, ValueError):
            continue
        if d.get('task') == task and d.get('diff_sha256'):
            seen.add(d['diff_sha256'])
    return len(seen)


if __name__ == '__main__':
    # ⚠️ `--task` prints the TASK rather than the count, so the gate can key an
    # argued round-cap exception on it. Kept as a flag on the same script because
    # the task is already resolved here, from the verdict files, and a second
    # resolver would be a second thing to drift.
    # ⚠️ `--for-task <ID>` counts by NAME instead, for callers that know the task
    # and whose hash is not yet reviewed -- which is every packet.
    if '--for-task' in sys.argv:
        i = sys.argv.index('--for-task')
        rest = sys.argv[i + 2:]
        print(rounds_of_task(sys.argv[i + 1], rest[0] if rest else '.harness/review'))
        raise SystemExit(0)
    args = [a for a in sys.argv[1:] if a != '--task']
    n, task = rounds_for(args[0], args[1] if len(args) > 1 else '.harness/review')
    if '--task' in sys.argv:
        print(task or '')
    else:
        print(n)
