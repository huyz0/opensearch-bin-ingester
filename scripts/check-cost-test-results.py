#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Fail cost CI when its required RustFS tests are absent or skipped."""
import argparse
import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path


FAST = {
    ("WriteRequestRateIT", "sizeTriggeredFleetStaysBelowTheWriteRequestBudgetAtThreeRates"): 1,
    ("ReadRequestRateIT", "storeGetsStayFlatAcrossConsumerNodesAndShards"): 1,
    ("CrossAzBytesIT", "crossAzDeliveryIsCountedAndStaysBelowTheProducerByteBudget"): 1,
}
FULL = FAST | {
    ("LowRateWriteBudgetIT", "lowRateWritesStayWithinTwoPutsPerIntervalAtBothCeilings"): 1,
}
RATE_POINT = re.compile(r"mibPerSecond=([0-9]+(?:\.[0-9]+)?)")


def check(results: Path, profile: str) -> list[str]:
    expected = dict(FAST if profile == "fast" else FULL)
    if profile == "full":
        key = ("WriteRequestRateIT", "sizeTriggeredFleetStaysBelowTheWriteRequestBudgetAtThreeRates")
        expected[key] = 3
    found: dict[tuple[str, str], list[ET.Element]] = {key: [] for key in expected}
    reports = sorted(results.glob("TEST-*.xml"))
    if not reports:
        required = ", ".join(f"{name}.{method}" for name, method in expected)
        return [f"no integration test reports found under {results}; required: {required}"]
    problems = []
    for report in reports:
        try:
            root = ET.parse(report).getroot()
        except (ET.ParseError, OSError) as error:
            return [f"cannot read cost test report {report}: {error}"]
        for case in root.iter("testcase"):
            class_name = case.attrib.get("classname", "").rsplit(".", 1)[-1]
            method = case.attrib.get("name", "")
            for key in found:
                if class_name == key[0] and method.startswith(key[1]):
                    found[key].append(case)
    for (class_name, method), cases in found.items():
        if len(cases) != expected[(class_name, method)]:
            problems.append(f"{class_name}.{method}: expected {expected[(class_name, method)]} executed point(s), found {len(cases)}")
        if any(case.find("skipped") is not None for case in cases):
            problems.append(f"{class_name}.{method}: required cost test was skipped")
        if any(case.find("failure") is not None or case.find("error") is not None for case in cases):
            problems.append(f"{class_name}.{method}: required cost test failed")
    write_key = ("WriteRequestRateIT", "sizeTriggeredFleetStaysBelowTheWriteRequestBudgetAtThreeRates")
    required_rates = {"40.0"} if profile == "fast" else {"40.0", "80.0", "160.0"}
    found_rates = {match.group(1) for case in found[write_key]
                   if (match := RATE_POINT.search(case.attrib.get("name", ""))) is not None}
    if found_rates != required_rates:
        problems.append(f"required M9.8 rate points {sorted(required_rates)}, found {sorted(found_rates)}")
    return problems


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", type=Path, required=True)
    parser.add_argument("--profile", choices=("fast", "full"), required=True)
    args = parser.parse_args()
    problems = check(args.results, args.profile)
    if problems:
        for problem in problems:
            print(problem, file=sys.stderr)
        return 1
    print(f"{args.profile} cost tests executed without skips")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
