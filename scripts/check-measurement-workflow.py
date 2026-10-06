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


BUILD_CACHE_KEY = "key: gradle-build-${{ runner.os }}-${{ github.sha }}"
DEPS_SAVE_KEY = "key: ${{ steps.gradle-deps.outputs.cache-primary-key }}"
DEPS_COMPILE = "run: ./gradlew testClasses integrationTestClasses --no-daemon"


def ci_cache_failures(lines: list[str]) -> list[str]:
    """CI's cache steps (M13.63; M13.61, M13.62): what fits L1 in its cap."""
    deps = job_block(lines, "gradle-deps")
    if deps is None:
        return ["CI workflow must define the gradle-deps job that fills L1's caches"]
    problems = []
    # ⚠️ A STEP's FIRST KEY TOO (its review round 1, P1): `- run: ...`.
    steps = [line.strip().removeprefix("- ") for line in deps]
    restore = [line for line in steps if line.startswith("key: gradle-deps-")]
    if len(restore) != 1:
        problems.append("gradle-deps must restore its dependencies under one gradle-deps key")
    # ⚠️ NEVER A FRESH hashFiles (M13.61): after the build it hashes the
    # generated *.gradle.kts too, and saves under a key no restore asks for.
    if DEPS_SAVE_KEY not in steps:
        problems.append("gradle-deps must save its dependencies under the restore's "
                        "cache-primary-key, never a fresh hashFiles")
    # ⚠️ COMPILE ONLY (M13.62): a task that runs tests would put results in
    # the build cache L1 restores -- a pass nobody ran.
    if [line for line in steps if line.startswith("run:")] != [DEPS_COMPILE]:
        problems.append("gradle-deps must compile only: " + DEPS_COMPILE)
    if BUILD_CACHE_KEY not in steps:
        problems.append("gradle-deps must save the build cache under "
                        "gradle-build-<os>-<sha>")
    l1 = job_block(lines, "l1")
    if l1 is not None:
        l1_steps = [line.strip().removeprefix("- ") for line in l1]
        if restore and restore[0] not in l1_steps:
            problems.append("L1 must restore the dependencies under gradle-deps' key")
        if BUILD_CACHE_KEY not in l1_steps:
            problems.append("L1 must restore the build cache under gradle-build-<os>-<sha>")
    return problems


def ci_failures(ci: str) -> list[str]:
    lines = ci.splitlines()
    problems = ci_cache_failures(lines)
    l1 = job_block(lines, "l1")
    if l1 is None:
        return problems + ["CI workflow must define the L1 job"]
    fast_requirements = (
        "name: Cost assertions (fast subset)",
        "M9_8_RATE: '40'",
        "M9_8_POINT_DURATION: PT1M",
        "./gradlew :server:integrationTest",
        "WriteRequestRateIT",
        "ReadRequestRateIT",
        "CrossAzBytesIT",
        "python scripts/check-cost-test-results.py --results server/build/test-results/integrationTest --profile fast",
    )
    for requirement in fast_requirements:
        if not any(requirement in line for line in l1):
            problems.append(f"L1 job must retain the fast cost subset requirement: {requirement}")
    if any(line.lstrip().startswith("continue-on-error:") for line in l1):
        problems.append("L1 cost tests must fail the per-push job on regression")
    if any(line.lstrip().startswith("if:") for line in l1):
        problems.append("L1 cost job and its steps must not be conditionally skipped")
    return problems


def cost_job_failures(workflow: str) -> list[str]:
    lines = workflow.splitlines()
    cost = job_block(lines, "cost")
    if cost is None:
        return ["measurement workflow must define the full cost job"]
    problems = []
    requirements = (
        "L4 — full cost points",
        "    timeout-minutes: 90",
        "WriteRequestRateIT",
        "LowRateWriteBudgetIT",
        "ReadRequestRateIT",
        "CrossAzBytesIT",
        "--profile full",
    )
    for requirement in requirements:
        if not any(requirement in line for line in cost):
            problems.append(f"full cost job must retain {requirement}")
    if not any(line.strip() == "./gradlew :server:integrationTest -Pm9.fullMeasurement=true"
               for line in cost):
        problems.append("full cost job must opt into the extended integration-test timeout")
    if any(line.lstrip().startswith("continue-on-error:") for line in cost):
        problems.append("full cost assertions must gate the measurement workflow")
    if any(line.startswith("    if:") for line in cost):
        problems.append("full cost job must not be conditionally skipped")
    if any(line.lstrip().startswith("if:") and line.strip() != "if: always()"
           for line in cost):
        problems.append("full cost assertions must not be conditionally skipped")

    latency = job_block(lines, "latency-trends")
    if latency is None:
        problems.append("measurement workflow must define the latency-trends job")
    else:
        for name in ("VisibilityLatencyIT", "PartitionVisibilityIT", "MacroHarnessIT",
                     "CompressionBenchmark", "KillNodeMidBacklogIT"):
            if not any(name in line for line in latency):
                problems.append(f"latency-trends job must retain criterion run {name}")
        trend_steps = [i for i, line in enumerate(latency) if "continue-on-error: true" in line]
        if len(trend_steps) < 4:
            problems.append("latency trend runs must remain non-gating")
    return problems


SHUTDOWN_CLASSES = ("ConfigExitCodeIT", "ShutdownDrainIT")

# ⚠️ THE REFUSAL's CONTENT, NOT ITS NAME (M13.60 review P2): a renamed or
# emptied step is what lets a skipped shutdown case read as a pass.
SHUTDOWN_REFUSAL = (
    "for c in " + " ".join(SHUTDOWN_CLASSES) + "; do",
    'test -f "$f" || { echo "::error::$c did not run"; exit 1; }',
    "grep -q 'skipped=\"0\"' \"$f\" || { echo \"::error::$c skipped a case\"; exit 1; }",
)


def shutdown_failures(workflow: str) -> list[str]:
    """The only run of the shutdown hook and the SIGTERM drain (M13.56, M13.60)."""
    lines = workflow.splitlines()
    shutdown = job_block(lines, "shutdown")
    if shutdown is None:
        return ["measurement workflow must define the shutdown job"]
    problems = []
    # ⚠️ EXACT LINES (its review P1): a substring takes `timeout-minutes: 100`.
    if "    timeout-minutes: 10" not in shutdown:
        problems.append("shutdown job must retain its ten-minute L2 budget")
    stripped = [line.strip() for line in shutdown]
    for name in SHUTDOWN_CLASSES:
        if f"--tests '*server.{name}'" not in stripped:
            problems.append(f"shutdown job must run {name}")
    if any(each not in stripped for each in SHUTDOWN_REFUSAL):
        problems.append("shutdown job must refuse a case that did not run or was skipped")
    # ⚠️ A STEP's FIRST KEY TOO (its review round 2, P4): `- if: false`.
    keys = [line.lstrip().removeprefix("- ") for line in shutdown]
    if any(key.startswith("continue-on-error:") for key in keys):
        problems.append("shutdown job must fail the measurement workflow on regression")
    if any(key.startswith("if:") for key in keys):
        problems.append("shutdown job and its steps must not be conditionally skipped")
    return problems


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workflow", type=Path, required=True)
    parser.add_argument("--ci", type=Path)
    args = parser.parse_args()
    try:
        workflow = args.workflow.read_text(encoding="utf-8")
        problems = failures(workflow) + shutdown_failures(workflow)
    except OSError as error:
        print(f"cannot read measurement workflow {args.workflow}: {error}", file=sys.stderr)
        return 2
    if args.ci is not None:
        try:
            ci = args.ci.read_text(encoding="utf-8")
        except OSError as error:
            print(f"cannot read CI workflow {args.ci}: {error}", file=sys.stderr)
            return 2
        problems.extend(ci_failures(ci))
        try:
            problems.extend(cost_job_failures(args.workflow.read_text(encoding="utf-8")))
        except OSError as error:
            print(f"cannot read measurement workflow {args.workflow}: {error}", file=sys.stderr)
            return 2
    if problems:
        for problem in problems:
            print(problem, file=sys.stderr)
        return 1
    print("measurement workflow includes the scheduled L2S soak job")
    print("measurement workflow includes the blocking L2 SIGTERM shutdown job")
    if args.ci is not None:
        print("measurement workflow includes full cost points and non-gating latency trends")
        print("CI workflow includes the blocking fast L1 cost subset")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
