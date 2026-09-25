# SPDX-License-Identifier: Apache-2.0
"""Regression tests for source-path routing in the TDD evidence scanner."""

import importlib.util
import pathlib
import subprocess
import sys
import tempfile
import unittest


MODULE_PATH = pathlib.Path(__file__).with_name("tdd_scan.py")
SCRATCH = MODULE_PATH.parents[1] / "build/tmp/tdd-scan-unit"
SCRATCH.mkdir(parents=True, exist_ok=True)
SPEC = importlib.util.spec_from_file_location("tdd_scan", MODULE_PATH)
tdd_scan = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(tdd_scan)


class TddScanPathTest(unittest.TestCase):
    def test_clean_reports_removes_prior_junit_results(self):
        with tempfile.TemporaryDirectory(dir=SCRATCH) as scratch:
            result_dir = pathlib.Path(scratch) / "server/build/test-results/test"
            result_dir.mkdir(parents=True, exist_ok=True)
            stale_failure = result_dir / "TEST-example.ProbeTest.xml"
            stale_failure.write_text(
                "<testsuite><testcase classname='example.ProbeTest'><failure/></testcase></testsuite>"
            )
            other_report = result_dir / "TEST-other.ProbeTest.xml"
            other_report.write_text("<testsuite><testcase/></testsuite>")

            run = subprocess.run(
                [sys.executable, str(MODULE_PATH), "clean-reports", scratch],
                capture_output=True,
                text=True,
            )

            self.assertEqual(run.returncode, 0, run.stdout + run.stderr)
            self.assertFalse(stale_failure.exists())
            self.assertFalse(other_report.exists())

    def test_clean_reports_fails_when_a_matching_report_cannot_be_removed(self):
        with tempfile.TemporaryDirectory(dir=SCRATCH) as scratch:
            report_dir = pathlib.Path(scratch) / "server/build/test-results/test/TEST-locked.xml"
            report_dir.mkdir(parents=True, exist_ok=True)

            run = subprocess.run(
                [sys.executable, str(MODULE_PATH), "clean-reports", scratch],
                capture_output=True,
                text=True,
            )

            self.assertNotEqual(run.returncode, 0, run.stdout + run.stderr)
            self.assertIn("TEST-locked.xml", run.stdout + run.stderr)
            self.assertTrue(report_dir.is_dir())

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
