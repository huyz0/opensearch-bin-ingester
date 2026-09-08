#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""The external reviewer: packet, invocation, and verdict.

⚠️ THIS SHARES NO CODE WITH `scripts/review*.sh`, AND THAT IS THE POINT.
`review.md` rule `harness-exempt` waives the reviewer entirely for a change
confined to `scripts/`, `buildSrc/`, `.agents/`, `.claude/`, `docs/` or
`baselines/`. Every harness fix therefore landed unreviewed, including the
fixes to the review harness itself. A reviewer living inside that waiver cannot
review the waiver, so this one lives in `tools/` and has no exemption at all.

Three commands:

    xreview.py packet --task <ID>
    xreview.py review --task <ID> --role <reviewer|test-reviewer>
    xreview.py verify [--task <ID>]

What each defect below cost is recorded where it is defended against, because
a guard whose reason is not written down is the guard someone deletes.
"""
import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import die, git
from verdict import (OVERRIDES, ROLES, ROUND_CAP, SEVERITIES, VERDICT_DIR, VERDICTS,
                     extract_json, override_for, rounds_for, validate)


# ⚠️ REFUSE, DO NOT TRUNCATE. M4.34: the old packet was cut at ~37 KB by the
# agent tool, and `review.sh` had no length check anywhere in its packet path,
# so it could not know the cut had happened. The reviewer judged a prefix
# believing it had the whole diff and filed a major against code it was never
# shown. A truncated packet is indistinguishable from a complete one, which is
# the `ok nothing staged` shape: the guarantee quietly becomes a guarantee
# about a prefix. So the cap refuses rather than trims, and there is no
# override -- an override would restore exactly the silence being removed.
PACKET_CAP = 32768


# ⚠️ THE HASH EXCLUDES `review/`, AND WITHOUT THIS NOTHING WORKS. M0.32
# measured the trap on M0.31: staging one baseline line moved the staged hash,
# and `check-reviewed` went from one bound verdict to none -- so arguing a
# finding always cost another round, and rule `two-round-cap` forbade that
# round. Verdicts are tracked here (they must be, or CI cannot see them), so
# recording one would move the hash it was just bound to. Excluding the
# reviewer's own bookkeeping from the bytes under review is the remedy M0.32
# names, and it is what makes a tracked verdict store possible at all.
EXCLUDE = ":(exclude)review/"


def staged_diff():
    return git("diff", "--cached", "--", ".", EXCLUDE)


def staged_hash():
    return hashlib.sha256(staged_diff().encode()).hexdigest()


def backlog_row(task):
    """The one row whose ID column is exactly this task.

    ⚠️ ANCHORED, NOT `grep -F`. M0.69 measured the substring version: M4.7's
    packet carried M4.7a's and M4.7b's rows too -- 9,262 bytes for a row of
    438, and three sets of acceptance criteria with nothing saying which one
    bound the diff under review.
    """
    pattern = re.compile(r"^\|\s*`?" + re.escape(task) + r"`?\s*\|")
    for path in sorted(Path("docs/internal/product").glob("backlog*.md")):
        for line in path.read_text(errors="replace").splitlines():
            if pattern.match(line):
                return line
    return None


def require_task(task):
    if not task:
        die(2, "--task is required, and must name a real backlog row.")
    row = backlog_row(task)
    if row is None:
        # ⚠️ M4.29: `review.sh record` never validated --task at all. Recording
        # both roles under `--task M9.7` with four prior rounds on disk gave
        # "ok ... (round 1 of 2)", exit 0 -- a complete bypass of the round cap
        # that left no trace. Omitting the flag wrote `"task": ""` and reported
        # "round 0 of 2" over two present verdicts, a count the counter's own
        # definition makes impossible.
        die(2, f"--task {task}: no row whose ID column is exactly that, "
               f"in any docs/internal/product/backlog*.md")
    return row


def build_packet(task):
    row = require_task(task)
    diff = staged_diff()
    if not diff.strip():
        die(2, "nothing staged -- there is no change to review.")
    checklist = (Path(__file__).parent / "CHECKLIST.md").read_text()
    packet = (
        f"=== TASK {task} ===\n{row}\n\n"
        f"=== THE COMPLETE STAGED DIFF ===\n{diff}\n"
        f"=== WHAT YOU ARE BEING ASKED ===\n{checklist}\n"
    )
    # ⚠️ NO GATE-PASS LIST. The old packet carried a "GATES THAT ALREADY PASSED
    # (do not re-check these)" header, and it listed `check-diff-size.sh` as
    # PASSED when that gate had been structurally unable to run -- invoked with
    # no commit-message file it warns and exits 0. Combined with rule
    # `no-regating`, the reviewer was told not to look at diff size on the
    # strength of a gate that never ran. Nothing here tells a reviewer what not
    # to examine.
    if len(packet.encode()) > PACKET_CAP:
        die(2, f"split this commit: the packet is {len(packet.encode())} bytes "
               f"against a cap of {PACKET_CAP}. It is NOT truncated to fit, "
               f"because a reviewer cannot see what it was not sent.")
    return packet


def agent_command(role):
    """How the reviewer is invoked.

    ⚠️ THE MODEL IS PINNED. Neither `.claude/agents/reviewer.md` nor
    `test-reviewer.md` carried a `model:` field, so what did the reviewing
    drifted with whatever happened to be configured that session, across 760
    recorded verdicts.

    ⚠️ HOME IS REDIRECTED, AND THAT IS A NARROWING, NOT A SANDBOX. M0.61:
    reviewer isolation stopped at the filesystem the reviewer WROTE to, and
    nothing prevented a read of `~/.claude/projects/<project>/<session>.jsonl`
    -- the author's transcript, which rule `reasoning-denied` refuses on
    purpose. The rule was rung 7, an instruction, and the row records that a
    good-faith reviewer and a good-faith coordinator could not agree afterwards
    about whether it had been broken.

    What redirecting HOME actually buys: `~` and every default path now resolve
    inside a directory holding no transcript but the reviewer's own. What it
    does NOT buy, stated plainly rather than assumed: the filesystem is not
    confined, so an ABSOLUTE path to the real home still resolves. This moves
    the rule from rung 7 to somewhere short of rung 1 -- the natural way to
    reach the transcript is gone, the deliberate way is not. Closing it fully
    needs a container or a seccomp'd runner, and until that exists this
    docstring is the honest statement of the gap rather than a claim of
    isolation the code does not deliver.
    """
    override = os.environ.get("XREVIEW_AGENT_CMD")
    if override:
        return [override], None
    brief = (Path(__file__).parent / "agents" / f"{role}.md").read_text()
    model = os.environ.get("XREVIEW_MODEL", "claude-opus-5")
    return (
        ["claude", "-p", "--model", model, "--append-system-prompt", brief],
        scratch_home(),
    )


def scratch_home():
    """A HOME with the credentials and nothing else -- above all, no transcripts.

    ⚠️ A BARE EMPTY HOME DOES NOT WORK, and finding that out is the point of
    this function. The first version simply pointed HOME at an empty directory;
    the reviewer then could not authenticate at all ("Not logged in") and
    returned prose instead of a verdict. Isolation that stops the tool running
    is not isolation, it is an outage that looks like a refusal.

    So the scratch HOME is built by copying in exactly two things -- the
    credential file and a minimal trust record -- and NOT `~/.claude/projects/`,
    which is where `<session>.jsonl` lives. The reviewer can therefore reach the
    API and cannot reach the author's transcript, which is what rule
    `reasoning-denied` asks for and what M0.61 records as being, until now,
    only an instruction: "a rule that a good-faith reviewer and a good-faith
    coordinator can disagree about having broken is not enforced."
    """
    home = Path(".git/xreview-home").resolve()
    (home / ".claude").mkdir(parents=True, exist_ok=True)
    real = Path.home()
    # ⚠️ SYMLINKED, NOT COPIED, and the difference is a whole failed review. A
    # copy is a SNAPSHOT: the OAuth session behind it expires, the refresh
    # writes into the scratch home where nothing will ever read it again, and
    # the next run dies with "OAuth session expired and could not be refreshed"
    # -- measured, on the second round of the first real review this tool ran.
    # A symlink lets a refresh write through to the real file.
    creds = real / ".claude" / ".credentials.json"
    link = home / ".claude" / ".credentials.json"
    if creds.is_file():
        if link.is_symlink() or link.exists():
            link.unlink()
        link.symlink_to(creds)
    # Trust this one directory, so the run is not blocked on an interactive
    # dialog. Written fresh rather than copied: `~/.claude.json` carries the
    # user's whole project history, which is exactly what must not travel.
    (home / ".claude.json").write_text(json.dumps(
        {"projects": {str(Path.cwd()): {"hasTrustDialogAccepted": True}}}))
    return str(home)


def cmd_packet(args):
    sys.stdout.write(build_packet(args.task))
    return 0


def cmd_record(args):
    """Write a verdict from a reply the caller captured, instead of invoking.

    ⚠️ THIS IS THE WEAKER PATH AND MUST BE LABELLED AS ONE. `review` invokes
    the reviewer itself and writes what came back, so the author never handles
    the reply and cannot shape it -- that is what closes M0.64, where both
    reviewers claimed a verdict neither had recorded. `record` cannot promise
    that: the reply passes through the caller's hands, so it restores exactly
    one link of the chain that `review` removes.

    It exists because invoking a reviewer as a SUBPROCESS is not always
    possible. Measured here: the `claude` CLI's access token had expired and
    the refresh is performed by the host session, so a spawned `claude -p`
    fails with "OAuth session expired and could not be refreshed" no matter
    which HOME it is given. Where a session can run an agent but cannot spawn
    one, this is the route, and the verdict it writes records `via: record` so
    a reader can tell the two apart forever.

    ⚠️ Prefer `review`. If `record` appears in a verdict, the chain was longer
    by one hop than it should have been.
    """
    if args.role not in ROLES:
        die(2, f"--role must be one of {ROLES}.")
    require_task(args.task)
    if not staged_diff().strip():
        die(2, "nothing staged -- there is no change to have reviewed.")
    text = sys.stdin.read() if args.file == "-" else Path(args.file).read_text()
    v = extract_json(text)
    if v is None:
        die(2, "that reply carries no verdict object. Nothing recorded.")
    sha = staged_hash()
    record = validate(v, args.task, args.role, sha)
    record["via"] = "record"
    out = VERDICT_DIR / args.task
    out.mkdir(parents=True, exist_ok=True)
    rounds = rounds_for(args.task)
    n = rounds.index(sha) + 1 if sha in rounds else len(rounds) + 1
    (out / f"{n}.{args.role}.json").write_text(json.dumps(record, indent=2) + "\n")
    print(f"  \033[33mWARN\033[0m recorded via `record`, not `review` -- the reply "
          f"passed through the caller. Prefer `review` where a subprocess can run.")
    print(f"  \033[32mok\033[0m   {args.role} verdict {record['verdict']} recorded "
          f"for {args.task} round {n} ({len(record['findings'])} finding(s))")
    return 0


def cmd_review(args):
    if args.role not in ROLES:
        die(2, f"--role must be one of {ROLES}.")
    packet = build_packet(args.task)
    sha = staged_hash()
    cmd, home = agent_command(args.role)
    env = dict(os.environ)
    if home:
        env["HOME"] = home
    r = subprocess.run(cmd, input=packet, capture_output=True, text=True, env=env)

    # ⚠️ DID THE REVIEWER SPEAK, OR DID THE TOOL FAIL TO RUN? These are
    # different failures and must not share a message. Measured: an expired
    # OAuth session produced a 73-byte reply, and the runner reported "the
    # reviewer returned no verdict object -- only prose" -- accusing the
    # reviewer of exactly the dishonesty this tool exists to make impossible,
    # when nothing had run at all. A harness that misattributes its own outage
    # to the agent is worse than one that simply stops.
    combined = (r.stdout or "") + (r.stderr or "")
    broken = ("Failed to authenticate", "OAuth session expired",
              "Not logged in", "Please run /login")
    if r.returncode != 0 or any(t in combined for t in broken):
        raw = Path(".git/xreview-last-reply.txt")
        raw.write_text(combined)
        die(2, f"the reviewer never ran (exit {r.returncode}). This is NOT a "
               f"finding about the reviewer -- nothing was reviewed. Reply "
               f"saved to {raw}:\n       "
               + (combined.strip().splitlines() or ["(no output)"])[0])

    v = extract_json(r.stdout)
    if v is None:
        # The reviewer ran and answered, but did not answer with a verdict.
        raw = Path(".git/xreview-last-reply.txt")
        raw.write_text(r.stdout)
        die(2, f"the reviewer returned no verdict object -- only prose. "
               f"Nothing was recorded, because a claim is not a verdict. "
               f"Its reply is in {raw}.")
    record = validate(v, args.task, args.role, sha)
    out = VERDICT_DIR / args.task
    out.mkdir(parents=True, exist_ok=True)
    n = len(rounds_for(args.task)) or 1
    if sha not in rounds_for(args.task):
        n = len(rounds_for(args.task)) + 1
    else:
        n = rounds_for(args.task).index(sha) + 1
    (out / f"{n}.{args.role}.json").write_text(json.dumps(record, indent=2) + "\n")
    print(f"  \033[32mok\033[0m   {args.role} verdict {record['verdict']} recorded "
          f"for {args.task} round {n} ({len(record['findings'])} finding(s))")
    return 0


def cmd_verify(args):
    sha = staged_hash()
    if not staged_diff().strip():
        # ⚠️ NOT `ok nothing staged`. That line, in `check-reviewed.sh:11`, is
        # why the gate passes vacuously under `pre-commit run --all-files` in
        # CI. A gate with nothing in scope has verified nothing and must say so
        # by failing, not by printing green.
        die(1, "nothing staged -- a gate with nothing in scope has verified "
               "nothing, and must not report success.")
    task = args.task
    if not task:
        found = {json.loads(f.read_text()).get("task")
                 for f in VERDICT_DIR.rglob("*.json")
                 if json.loads(f.read_text()).get("diff_sha256") == sha} \
            if VERDICT_DIR.is_dir() else set()
        found.discard(None)
        if len(found) != 1:
            die(1, f"no verdict binds these staged bytes ({sha[:12]}). "
                   f"Run: tools/xreview/xreview.py review --task <ID> --role <role>")
        task = found.pop()
    require_task(task)

    missing = [r for r in ROLES
               if not (VERDICT_DIR / task).is_dir()
               or not any(json.loads(f.read_text()).get("diff_sha256") == sha
                          for f in (VERDICT_DIR / task).glob(f"*.{r}.json"))]
    if missing:
        die(1, f"no verdict for these staged bytes from: {', '.join(missing)}. "
               f"⚠️ There is no harness exemption -- a change to scripts/, "
               f".agents/ or the standards needs both verdicts like any other.")

    unresolved = []
    for role in ROLES:
        for f in (VERDICT_DIR / task).glob(f"*.{role}.json"):
            v = json.loads(f.read_text())
            if v.get("diff_sha256") != sha:
                continue
            unresolved += [f"{role}:{x['id']} ({x['severity']})"
                           for x in v["findings"] if x["severity"] in ("blocking", "major")]
    if unresolved:
        die(1, "unresolved blocking/major finding(s): " + ", ".join(unresolved))

    rounds = rounds_for(task)
    n = rounds.index(sha) + 1 if sha in rounds else len(rounds) + 1
    if n > ROUND_CAP:
        who = override_for(task)
        if not who:
            # ⚠️ NO ENV VAR, AND NO SELF-SERVICE KEY. `review.sh` refused at
            # >${REVIEW_ROUND_BUDGET:-4} and printed its own override as the
            # remedy; the archived `baselines/review.txt` holds 25 `rounds:`
            # escapes, 21 of them M4, and EVERY ONE says "argued rather than
            # split". The cap never once caused a split. What is left is a line
            # a person signs, which is a diff somebody reads -- rung 4, not
            # rung 7.
            die(1, f"round {n} for {task}, and the cap is {ROUND_CAP}. "
                   f"Split the commit, or stage a line in {OVERRIDES} of the form:\n"
                   f"       {task} - <why this is genuinely one change> - approved-by: <name>")
        print(f"  \033[33mWARN\033[0m round {n} for {task}, past the cap of "
              f"{ROUND_CAP}, on {who}'s signature")

    print(f"  \033[32mok\033[0m   {task}: both roles reviewed these bytes "
          f"({sha[:12]}, round {n} of {ROUND_CAP})")
    return 0


def main():
    p = argparse.ArgumentParser(prog="xreview.py")
    sub = p.add_subparsers(dest="cmd", required=True)
    for name in ("packet", "review", "verify", "record"):
        s = sub.add_parser(name)
        s.add_argument("--task", default="")
        if name in ("review", "record"):
            s.add_argument("--role", default="")
        if name == "record":
            s.add_argument("--file", default="-")
    args = p.parse_args()
    if args.cmd in ("packet", "review", "record"):
        require_task(args.task)
    return {"packet": cmd_packet, "review": cmd_review,
            "verify": cmd_verify, "record": cmd_record}[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
