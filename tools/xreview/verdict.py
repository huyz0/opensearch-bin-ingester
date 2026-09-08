#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The verdict: its vocabulary, its validation, its store, and the override.

Split out of `xreview.py` when that file crossed the 500-line cap in
code-structure.md rule 1. The seam is real rather than arbitrary: everything
here answers "is this a verdict, and what does the tree already hold", and
nothing here builds a packet or invokes an agent.
"""
import json
import re
import subprocess
from pathlib import Path

from common import die

ROLES = ("reviewer", "test-reviewer")
SEVERITIES = ("blocking", "major", "minor")
VERDICTS = ("pass", "changes-requested")
ROUND_CAP = 2

VERDICT_DIR = Path("review/verdicts")
OVERRIDES = "review/overrides.md"


def extract_json(text):
    """The first complete JSON object in the reply.

    ⚠️ A REPLY THAT IS PROSE IS NOT A VERDICT. M0.64: on M0.56's final round
    BOTH reviewers closed with "verdict recorded at this hash" and NEITHER had
    run the recorder -- no file existed for either role, and nothing but
    `check-reviewed` noticed. They were wrong identically, so their agreement
    carried no information. Here the agent cannot write a verdict at all; it
    can only speak, and the runner writes what it actually said. "Reported" and
    "recorded" become one act, so the gap cannot open.
    """
    start = text.find("{")
    while start != -1:
        depth, in_str, esc = 0, False, False
        for i in range(start, len(text)):
            c = text[i]
            if in_str:
                if esc:
                    esc = False
                elif c == "\\":
                    esc = True
                elif c == '"':
                    in_str = False
                continue
            if c == '"':
                in_str = True
            elif c == "{":
                depth += 1
            elif c == "}":
                depth -= 1
                if depth == 0:
                    try:
                        return json.loads(text[start:i + 1])
                    except json.JSONDecodeError:
                        break
        start = text.find("{", start + 1)
    return None


def validate(v, task, role, sha):
    if not isinstance(v, dict):
        die(2, "the reply carried no verdict object.")
    if v.get("verdict") not in VERDICTS:
        die(2, f"verdict must be one of {VERDICTS}, not {v.get('verdict')!r}.")
    findings = v.get("findings")
    if not isinstance(findings, list):
        die(2, "verdict has no findings list (an empty list is valid).")
    seen = set()
    for f in findings:
        if not isinstance(f, dict):
            die(2, "a finding is not an object.")
        for key in ("id", "severity", "file", "summary", "failure_scenario"):
            if not str(f.get(key, "")).strip():
                # rule `failure-scenario`: without one it is a style opinion.
                die(2, f"finding {f.get('id', '?')!r} has no {key}.")
        if f["severity"] not in SEVERITIES:
            die(2, f"severity must be one of {SEVERITIES}, not {f['severity']!r}.")
        if f["id"] in seen:
            die(2, f"duplicate finding id {f['id']!r} -- an id must address one finding.")
        seen.add(f["id"])
    blocking = [f for f in findings if f["severity"] in ("blocking", "major")]
    if v["verdict"] == "pass" and blocking:
        die(2, f"a pass cannot carry {len(blocking)} blocking/major finding(s).")
    return {
        "task": task, "role": role, "diff_sha256": sha,
        "verdict": v["verdict"], "findings": findings,
    }


def rounds_for(task):
    """Distinct staged hashes ever reviewed for this task.

    ⚠️ COUNTED FROM THE TREE, not from a gitignored directory. The old store
    lived under `.harness/`, which `.gitignore` excludes -- which is exactly
    why `check-reviewed` cannot run in CI, and why on `--all-files` it printed
    `ok nothing staged` and passed vacuously. build.md says it plainly: "It
    does not fail, which is worse."
    """
    d = VERDICT_DIR / task
    if not d.is_dir():
        return []
    hashes = []
    for f in sorted(d.glob("*.json")):
        try:
            h = json.loads(f.read_text()).get("diff_sha256")
        except (json.JSONDecodeError, OSError):
            die(1, f"{f}: unreadable verdict -- refusing to count rounds around it.")
        if h and h not in hashes:
            hashes.append(h)
    return hashes


def overrides_in_index():
    """Override lines as the INDEX holds them, never as the worktree does.

    ⚠️ M4.26 measured the worktree version: a wholly UNTRACKED
    `baselines/review.txt` silenced blocking and major findings from both roles
    while the gate printed `ok`. ⚠️ AND NOT "does git render this line as
    added" either -- M4.31 measured that question wrong in both directions:
    moving an already-committed line re-arms the escape, and a staged DELETION
    of the line arms it too, so tidying a stale argument away granted the
    thing being tidied.
    """
    r = subprocess.run(["git", "show", f":{OVERRIDES}"], capture_output=True, text=True)
    return r.stdout.splitlines() if r.returncode == 0 else []


def override_for(task):
    # A task, a reason, and a name. All three, or it is not a signature.
    want = re.compile(r"^\s*" + re.escape(task) + r"\s+(.*\S.*)approved-by:\s*(\S.*)$")
    for line in overrides_in_index():
        m = want.match(line)
        if m and m.group(1).strip(" -\t"):
            return m.group(2).strip()
    return None
