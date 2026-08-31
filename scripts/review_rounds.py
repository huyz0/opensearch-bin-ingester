# SPDX-License-Identifier: Apache-2.0
"""How many review rounds a task has had.

⚠️ review.md rule 12 -- "two rounds is the cap" -- was violated on 7 of 12 tasks
in one session (five rounds on one, four on two others) because NOTHING COUNTED.
A prose rule only a reviewer can uphold differs per run and dies with the
session; this makes it a predicate over the verdict files.

A round is a DISTINCT staged hash reviewed for the same task.

⚠️ A hash whose diff differs from an earlier reviewed one ONLY by
baselines/review.txt is not a new round. Arguing a finding (rule 9) requires
staging that file, which moves the hash -- so counting it would make arguing
cost the very round it is arguing about, and the escape hatch would be unusable.
"""
import glob
import json
import sys


def rounds_for(sha, review_dir='.harness/review'):
    task = None
    for f in glob.glob('%s/%s.*.json' % (review_dir, sha)):
        try:
            task = json.load(open(f)).get('task')
        except (OSError, ValueError):
            continue
        if task:
            break
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


if __name__ == '__main__':
    n, task = rounds_for(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else '.harness/review')
    print(n)
