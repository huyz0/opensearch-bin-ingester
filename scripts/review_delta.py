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


def prior_tree(task, current_sha, review_dir='.harness/review'):
    """The staged git TREE the last reviewed round was bound to, or None.

    ⚠️ MTIME, not filename. `prior_rounds` sorts by path, which is hash order and
    therefore arbitrary -- fine for gathering open findings, wrong for "the last
    round", because the tree of an OLDER round would then decide what a verify
    round is shown.

    ⚠️ Returns None rather than guessing when no round recorded a tree. The
    caller falls back to the whole diff: less review is never the failure-safe
    default.
    """
    import os
    best = None
    for f, d in prior_rounds(task, current_sha, review_dir):
        if not d.get('staged_tree'):
            continue
        try:
            mtime = os.path.getmtime(f)
        except OSError:
            continue
        if best is None or mtime > best[0]:
            best = (mtime, d['staged_tree'])
    return best[1] if best else None


def main():
    argv = sys.argv[1:]
    want_tree = '--prior-tree' in argv
    if want_tree:
        argv.remove('--prior-tree')
    task, sha = argv[0], argv[1]
    review_dir = argv[2] if len(argv) > 2 else '.harness/review'
    if want_tree:
        t = prior_tree(task, sha, review_dir)
        if not t:
            return 1
        print(t)
        return 0
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
