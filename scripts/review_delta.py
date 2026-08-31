# SPDX-License-Identifier: Apache-2.0
"""What changed since this task was last reviewed, and what was said then.

⚠️ Why. Every round re-read the WHOLE diff. Round two of M0.31 re-read ~200
lines to confirm three fixes; round five re-read them again. Measured across one
session: 140 verdicts over 9 commits, most of the cost in re-reading bytes a
previous round had already cleared.

A reviewer with no memory cannot know what it already checked -- so the packet
tells it: the delta since the last reviewed hash, and the findings that were
open. Everything unchanged has been reviewed already and carries a verdict.

⚠️ This is a REDUCTION in what the reviewer sees, so it is derived from the
verdict files rather than from the author's summary. The author does not get to
say "only this changed" -- git does. When no prior round exists, or the previous
hash is not reconstructible, it returns nothing and the caller falls back to the
full diff: less review is never the failure-safe default.
"""
import glob
import json
import subprocess
import sys


def _verdicts(review_dir):
    for f in glob.glob('%s/*.json' % review_dir):
        try:
            yield f, json.load(open(f))
        except (OSError, ValueError):
            continue


def prior_rounds(task, current_sha, review_dir='.harness/review'):
    """Verdicts for this task on hashes other than the current one, newest last."""
    out = []
    for f, d in _verdicts(review_dir):
        if d.get('task') == task and d.get('diff_sha256') not in (None, current_sha):
            out.append((f, d))
    out.sort(key=lambda fd: fd[0])
    return out


def open_findings(task, current_sha, review_dir='.harness/review'):
    """Blocking/major findings from prior rounds, which is what a verify round is for."""
    found = []
    for _, d in prior_rounds(task, current_sha, review_dir):
        for x in d.get('findings') or []:
            if x.get('severity') in ('blocking', 'major'):
                found.append((d.get('diff_sha256', '?')[:12], x))
    return found


def main():
    task, sha = sys.argv[1], sys.argv[2]
    review_dir = sys.argv[3] if len(sys.argv) > 3 else '.harness/review'
    prior = prior_rounds(task, sha, review_dir)
    if not prior:
        return 1  # round one: caller shows the full diff

    print('=== THIS IS NOT ROUND ONE ===')
    print('A previous round reviewed this task at a different hash. Everything')
    print('not shown below was in that diff and already carries a verdict --')
    print('re-reading it is what made review cost more than the work.')
    print()
    fs = open_findings(task, sha, review_dir)
    if fs:
        print('=== FINDINGS STILL OPEN FROM EARLIER ROUNDS ===')
        for h, x in fs:
            print('  [%s] %s %s -- %s' % (x.get('severity'), x.get('id'), h,
                                          str(x.get('summary'))[:100]))
            if x.get('failure_scenario'):
                print('        scenario: %s' % str(x['failure_scenario'])[:160])
        print()
        print('⚠️ Verify THESE. A finding you raised and the author did not fix')
        print('   is the thing this round exists to catch.')
        print()
    return 0


if __name__ == '__main__':
    sys.exit(main())
