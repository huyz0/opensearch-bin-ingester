#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Check that the nightly measurement workflow runs its named L2S soak job."""
import argparse
import sys
from pathlib import Path


def job_block(lines: list[str], name: str) -> list[str] | None:
    start = next((i for i, line in enumerate(lines) if line == f"  {name}:"), None)
    if start is None:
        return None
    end = next((i for i in range(start + 1, len(lines))
                if lines[i].startswith("  ") and not lines[i].startswith("    ")), len(lines))
    return lines[start:end]


def failures(workflow: str) -> list[str]:
    lines = workflow.splitlines()
    problems = []
    if "  schedule:" not in lines or "    - cron: '17 3 * * *'" not in lines:
        problems.append("workflow must retain the nightly 03:17 UTC schedule")
    if "  workflow_dispatch:" not in lines:
        problems.append("workflow must support manual dispatch")
    soak = job_block(lines, "soak")
    if soak is None:
        problems.append("workflow must define the soak job")
    else:
        if not any("L2S" in line for line in soak):
            problems.append("soak job must be named as the L2S layer")
        if any(line.lstrip().startswith("if:") for line in soak):
            problems.append("soak job and steps must not be conditionally disabled")
        if "    timeout-minutes: 10" not in soak:
            problems.append("soak job must retain the ten-minute L2S budget")
        if "        run: ./gradlew soakTest --no-daemon" not in soak:
            problems.append("soak job must execute ./gradlew soakTest --no-daemon")
        if any(line.strip().lower().startswith("continue-on-error:") for line in soak):
            problems.append("soak job must not ignore a failed soakTest step")
    return problems


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workflow", type=Path, required=True)
    args = parser.parse_args()
    try:
        problems = failures(args.workflow.read_text(encoding="utf-8"))
    except OSError as error:
        print(f"cannot read measurement workflow {args.workflow}: {error}", file=sys.stderr)
        return 2
    if problems:
        for problem in problems:
            print(problem, file=sys.stderr)
        return 1
    print("measurement workflow includes the scheduled L2S soak job")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
