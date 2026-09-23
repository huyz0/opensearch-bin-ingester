# SPDX-License-Identifier: Apache-2.0
"""Exercise the real TDD runner against an isolated fake Gradle executable."""

import json
import hashlib
import os
import pathlib
import ntpath
import shutil
import subprocess
import sys
import unittest
import uuid


ROOT = pathlib.Path(__file__).resolve().parent.parent
SCRATCH = ROOT / "build/tmp/tdd-red-integration"
SCRATCH.mkdir(parents=True, exist_ok=True)


def bash_executable():
    if os.name == "nt":
        candidate = pathlib.Path(os.environ.get("ProgramFiles", "C:/Program Files"))
        candidate = candidate / "Git/bin/bash.exe"
        return str(candidate) if candidate.is_file() else None
    return shutil.which("bash")


def bash_path(path):
    value = str(path)
    if os.name != "nt":
        return value
    drive, tail = ntpath.splitdrive(value)
    return "/" + drive[0].lower() + tail.replace("\\", "/")


class TddRedIntegrationTest(unittest.TestCase):
    def run_runner(self, fake_result):
        bash = bash_executable()
        if bash is None:
            self.skipTest("Bash is required to exercise tdd-red.sh")
        fixture = SCRATCH / uuid.uuid4().hex
        scripts = fixture / "scripts"
        scripts.mkdir(parents=True)
        for name in ("tdd-red.sh", "lib.sh", "tdd_scan.py", "java_tests.py"):
            shutil.copy2(ROOT / "scripts" / name, scripts / name)

        test_source = fixture / "buildSrc/src/test/java/example/ProbeTest.java"
        test_source.parent.mkdir(parents=True)
        test_source.write_text(
            "package example; import org.junit.jupiter.api.Test;\n"
            "class ProbeTest { @Test void fails() {} }\n"
        )
        (fixture / ".harness/tdd").mkdir(parents=True)

        gradle = fixture / "gradlew"
        gradle.write_text(
            "#!/usr/bin/env bash\n"
            "printf '%s\\n' \"$*\" >> gradle-args.txt\n"
            "mkdir -p buildSrc/build/test-results/test\n"
            "if [ \"$FAKE_TEST_RESULT\" = failed ]; then\n"
            "  printf '%s' \"<testsuite tests='1' failures='1'><testcase "
            "classname='example.ProbeTest' name='fails()'><failure/></testcase>"
            "</testsuite>\" > buildSrc/build/test-results/test/TEST-example.ProbeTest.xml\n"
            "  exit 1\n"
            "fi\n"
            "printf '%s' \"<testsuite tests='1' failures='0'><testcase "
            "classname='example.ProbeTest' name='fails()'/></testsuite>\" "
            "> buildSrc/build/test-results/test/TEST-example.ProbeTest.xml\n"
            "exit 0\n"
        )
        gradle.chmod(0o755)

        python3 = bash_path(sys.executable)
        python_launcher = fixture / "python runtime/python3"
        python_launcher.parent.mkdir(parents=True)
        python_launcher.write_text(
            "#!/usr/bin/env bash\nexec " + json.dumps(python3) + " \"$@\"\n"
        )
        python_launcher.chmod(0o755)
        env = os.environ.copy()
        env["PYTHON3"] = bash_path(python_launcher)
        env["FAKE_TEST_RESULT"] = fake_result
        run = subprocess.run(
            [bash, "scripts/tdd-red.sh", "example.ProbeTest#fails"],
            cwd=fixture,
            env=env,
            capture_output=True,
            text=True,
        )
        return fixture, test_source, run

    def test_crlf_plan_runs_the_scoped_test_and_records_its_red_bound_to_source(self):
        fixture, test_source, run = self.run_runner("failed")
        self.assertEqual(run.returncode, 0, run.stdout + run.stderr)
        args = (fixture / "gradle-args.txt").read_text()
        self.assertIn("-p buildSrc test", args)
        self.assertIn("--tests example.ProbeTest.fails", args)
        self.assertNotIn("\r", args)
        red = json.loads((fixture / ".harness/tdd/red.json").read_text())
        record = red["red"]["example.ProbeTest#fails"]
        self.assertEqual(record["sha256"], hashlib.sha256(test_source.read_bytes()).hexdigest())

    def test_passing_test_does_not_create_red_evidence(self):
        fixture, _, run = self.run_runner("passed")
        self.assertEqual(run.returncode, 1, run.stdout + run.stderr)
        self.assertFalse((fixture / ".harness/tdd/red.json").exists())


if __name__ == "__main__":
    unittest.main()
