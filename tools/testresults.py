#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Read JUnit XML with an XML parser, because a regex over it lies.

⚠️ WRITTEN AFTER A REGEX-BASED READER REPORTED A FAILING TEST AS `ok`.
`<testcase .../>` self-closes when a test passes and wraps a `<failure>` when
it does not, so a non-greedy scan for `</testcase>|/>` ends in the wrong place
and the failure is never seen. Several decisions in this session were taken on
that reader's output before Gradle's own console contradicted it.
"""
import glob
import sys
import xml.etree.ElementTree as ET


def main(patterns):
    total = failed = skipped = 0
    files = 0
    lines = []
    for pattern in patterns:
        for path in sorted(glob.glob(pattern, recursive=True)):
            files += 1
            try:
                root = ET.parse(path).getroot()
            except ET.ParseError as bad:
                # ⚠️ A TRUNCATED RESULTS FILE IS NOT AN EMPTY ONE. A crashed
                # runner leaves half a document, and parsing it as "no tests"
                # would report a green suite from a JVM that died.
                print(f"  FAIL {path} is not readable XML: {bad}")
                return 1
            for case in root.iter("testcase"):
                total += 1
                bad = case.find("failure") if case.find("failure") is not None \
                    else case.find("error")
                if case.find("skipped") is not None:
                    skipped += 1
                if bad is not None:
                    failed += 1
                    name = f'{case.get("classname", "").split(".")[-1]}.{case.get("name")}'
                    lines.append(f"  FAIL {name}\n       "
                                 + (bad.get("message") or "")[:300].replace("\n", " "))
    for line in lines:
        print(line)
    # ⚠️ NO INPUT IS A FAILURE, NEVER A PASS. Run before the task, from the
    # wrong directory, after a `clean`, or against a module path that no longer
    # exists, and a reader that exits 0 here certifies a green suite from zero
    # evidence -- which is the `ok nothing staged` shape, in the one tool whose
    # entire purpose is to stop a green reading being believed.
    if files == 0:
        print("  FAIL no results files matched " + " ".join(patterns)
              + " -- refusing to report a pass over nothing")
        return 1
    if total == 0:
        print(f"  FAIL {files} results file(s) but 0 tests -- the suite did not run")
        return 1
    print(f"  {total} tests, {failed} failures, {skipped} skipped, from {files} file(s)")
    return 1 if failed else 0


if __name__ == "__main__":
    args = sys.argv[1:] or ["*/build/test-results/*/TEST-*.xml"]
    sys.exit(main(args))
