#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Fail L1 when Gradle produced no executed product unit tests."""
import argparse
import sys
import xml.etree.ElementTree as ET
from pathlib import Path


def count_product_tests(root: Path) -> int:
    results = sorted(root.glob("*/build/test-results/test/TEST-*.xml"))
    total = 0
    for result in results:
        try:
            suite = ET.parse(result).getroot()
            tests = int(suite.attrib.get("tests", "0"))
            skipped = int(suite.attrib.get("skipped", "0"))
            if tests < 0 or skipped < 0 or skipped > tests:
                raise ValueError(f"invalid tests/skipped counts: {tests}/{skipped}")
            total += tests - skipped
        except (ET.ParseError, OSError, ValueError) as error:
            raise ValueError(f"cannot count tests in {result}: {error}") from error
    return total


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd(),
                        help="Gradle repository root (default: current directory)")
    args = parser.parse_args()
    root = args.root.resolve()
    if not root.is_dir():
        print(f"L1 result root is not a directory: {root}", file=sys.stderr)
        return 2
    try:
        count = count_product_tests(root)
    except ValueError as error:
        print(str(error), file=sys.stderr)
        return 2
    if count == 0:
        print("L1 reported zero product unit tests; it did not run")
        return 1
    print(f"L1 executed {count} product unit test(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
