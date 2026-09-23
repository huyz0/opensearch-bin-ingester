# SPDX-License-Identifier: Apache-2.0
"""Regression tests for source-path routing in the TDD evidence scanner."""

import importlib.util
import pathlib
import subprocess
import sys
import unittest


MODULE_PATH = pathlib.Path(__file__).with_name("tdd_scan.py")
SPEC = importlib.util.spec_from_file_location("tdd_scan", MODULE_PATH)
tdd_scan = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(tdd_scan)


class TddScanPathTest(unittest.TestCase):
    def test_source_digest_uses_canonical_git_line_endings(self):
        self.assertEqual(
            tdd_scan.source_digest(b"package example;\r\nclass Probe {}\r\n"),
            tdd_scan.source_digest(b"package example;\nclass Probe {}\n"),
        )

    def test_windows_buildsrc_test_path_routes_to_buildsrc_test(self):
        source = (
            r"buildSrc\src\test\java\io\github\huyz0\os\biningester\GateTest.java"
        )
        self.assertEqual(tdd_scan.task_for(source), "buildSrc:test")

    def test_slash_separated_buildsrc_test_path_still_routes(self):
        source = "buildSrc/src/test/java/io/github/huyz0/os/biningester/GateTest.java"
        self.assertEqual(tdd_scan.task_for(source), "buildSrc:test")

    def test_crlf_plan_selector_loses_only_its_trailing_carriage_return(self):
        self.assertEqual(
            tdd_scan.normalize_selector(
                "io.github.huyz0.os.biningester.GateTest.example\r"
            ),
            "io.github.huyz0.os.biningester.GateTest.example",
        )
        result = subprocess.run(
            [
                sys.executable,
                str(MODULE_PATH),
                "normalize-selector",
                "io.github.huyz0.os.biningester.GateTest.example\r",
            ],
            check=True,
            capture_output=True,
        )
        self.assertEqual(
            result.stdout, b"io.github.huyz0.os.biningester.GateTest.example"
        )

if __name__ == "__main__":
    unittest.main()
