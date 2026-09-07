# SPDX-License-Identifier: Apache-2.0
"""Is this diff PROSE ONLY, or does it change something executable? (M0.80)

⚠️ THIS LOOSENS A GATE. `review-roles.sh` routes by path, so a `.java` diff
always summons both reviewers; this lets a diff whose every Java edit is a
comment summon one. So every branch that cannot be decided returns
`executable` -- an unreadable blob, a lexer that throws, an added or deleted
file, a path this does not understand. The only way to reach `prose` is for
every changed source file to be byte-identical to its committed version once
comments are dropped.

⚠️ THE COMPARISON IS `hashable`, the same lexer `check-tdd` binds its red
records with. It drops comments, collapses whitespace BETWEEN tokens, and keeps
literal interiors verbatim -- that last part is what stops an assertion's
expected value being rewritten to whatever the code happens to produce, which
is the weakening non-negotiable 2 names.
"""
import subprocess
import sys

sys.path.insert(0, __file__.rsplit("/", 1)[0])
from java_tests import hashable, UnparseableJava  # noqa: E402

# Only these can be judged by the Java lexer. Everything else executable stays
# executable: a shell comment is prose too, but proving it needs a shell lexer
# this does not have, and guessing would loosen the gate on a hunch.
JUDGEABLE = (".java",)


def _git(*args):
    return subprocess.run(["git", *args], capture_output=True, text=True, check=False)


def status_of(path):
    """The staged status letter for one path, or None if git will not say."""
    out = _git("diff", "--cached", "--name-status", "--diff-filter=ACMRD", "--", path)
    if out.returncode != 0:
        return None
    for line in out.stdout.splitlines():
        parts = line.split("\t")
        if len(parts) >= 2:
            return parts[0][:1]
    return None


def shape(paths):
    """`prose` only if EVERY path given is a comment-only Java edit.

    ⚠️ THE CALLER SUPPLIES THE PATHS, and that is the point: `review-roles.sh`
    already decides which paths are test-eligible, so re-deriving that predicate
    here would be a second copy to drift. Anything it hands over that is not
    Java is executable by definition -- a shell comment is prose too, but
    proving it needs a lexer this does not have.

    ⚠️ IT READS THE INDEX, NOT `CHECK_RANGE`, so under CI's range mode every
    status comes back None and the answer is `executable`. That is the safe
    direction and it costs nothing: `check-reviewed` cannot run in CI at all.
    """
    saw_judgeable = False
    for path in paths:
        if not path.endswith(JUDGEABLE):
            return "executable"
        saw_judgeable = True
        status = status_of(path)
        if status is None:
            return "executable"
        # ⚠️ Added and deleted files have no committed counterpart to be
        # prose-identical to. A DELETED test is the case non-negotiable 2 names.
        if status in ("A", "D", "R"):
            return "executable"
        old = _git("show", f"HEAD:{path}")
        if old.returncode != 0:
            return "executable"
        try:
            with open(path, encoding="utf-8") as f:
                new_text = f.read()
        except OSError:
            return "executable"
        try:
            if hashable(old.stdout) != hashable(new_text):
                return "executable"
        except (UnparseableJava, Exception):  # noqa: BLE001 -- fail closed, always
            return "executable"
    return "prose" if saw_judgeable else "none"


if __name__ == "__main__":
    print(shape([p.strip() for p in sys.stdin.read().splitlines() if p.strip()]))
