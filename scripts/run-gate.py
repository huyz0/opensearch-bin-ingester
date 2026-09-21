#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Run a repository shell gate through the available portable shell adapter."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def windows_path_to_bash(path: Path) -> str:
    text = str(path)
    if len(text) >= 2 and text[1] == ":":
        return "/" + text[0].lower() + text[2:].replace("\\", "/")
    return text.replace("\\", "/")


def bash_executable() -> str:
    if os.name == "nt":
        for variable in ("ProgramFiles", "PROGRAMFILES", "ProgramW6432"):
            base = os.environ.get(variable)
            if base:
                candidate = Path(base) / "Git" / "bin" / "bash.exe"
                if candidate.exists():
                    return str(candidate)
        x86 = os.environ.get("ProgramFiles(x86)")
        if x86:
            candidate = Path(x86) / "Git" / "bin" / "bash.exe"
            if candidate.exists():
                return str(candidate)
    found = shutil.which("bash")
    if found:
        return found
    raise SystemExit("bash is required to run repository gates (install Git for Windows)")


def python_executable() -> Path | None:
    candidates = []
    for variable in ("LocalAppData", "LOCALAPPDATA"):
        base = os.environ.get(variable)
        if base:
            candidates.append(Path(base) / "Programs" / "Python" / "Python313" / "python.exe")
    profile = os.environ.get("USERPROFILE") or os.environ.get("HOME")
    if profile:
        candidates.append(Path(profile) / "AppData" / "Local" / "Programs" / "Python" / "Python313" / "python.exe")
    candidates.append(Path(sys.executable))
    return next((candidate for candidate in candidates if candidate.exists()), None)


def java_home() -> Path | None:
    configured = os.environ.get("JAVA_HOME")
    if configured:
        candidate = Path(configured)
        if (candidate / "bin" / ("java.exe" if os.name == "nt" else "java")).exists():
            return candidate
    if os.name == "nt":
        for variable in ("ProgramFiles", "PROGRAMFILES", "ProgramW6432"):
            base = os.environ.get(variable)
            if not base:
                continue
            for pattern in ("Microsoft/jdk-*", "Java/jdk-*"):
                matches = sorted((Path(base) / pattern.split("/")[0]).glob(pattern.split("/")[1]))
                for candidate in reversed(matches):
                    if (candidate / "bin" / "java.exe").exists():
                        return candidate
    return None


def main() -> int:
    if len(sys.argv) < 2:
        raise SystemExit("usage: run-gate.py scripts/check-example.sh [arguments]")
    script = (ROOT / sys.argv[1]).resolve()
    if ROOT not in script.parents or script.suffix != ".sh" or not script.exists():
        raise SystemExit(f"gate is not a repository shell script: {sys.argv[1]}")

    bash = bash_executable()
    command = [bash, "--noprofile", "--norc", "-c"]
    python = python_executable()
    prefix = ""
    if python and os.name == "nt":
        unix_python = windows_path_to_bash(python)
        prefix = f'python3() {{ "{unix_python}" "$@"; }}; export -f python3; '
    command.extend([prefix + 'exec bash "$1" "${@:2}"', "--", windows_path_to_bash(script)])
    command.extend(sys.argv[2:])
    environment = os.environ.copy()
    if os.name == "nt":
        # Git Bash otherwise makes Python inherit the Windows ANSI code page;
        # repository documents are UTF-8 and several gates read them directly.
        environment["PYTHONUTF8"] = "1"
        # Child processes in the Gradle harness invoke `git` from Git Bash.
        # Selecting bash.exe alone does not put its sibling executables on the
        # inherited Windows PATH.
        git_bin = Path(bash).parent
        if git_bin.name.lower() == "bin":
            environment["PATH"] = str(git_bin) + os.pathsep + environment.get("PATH", "")
        jdk = java_home()
        if jdk:
            environment["JAVA_HOME"] = str(jdk)
            environment["PATH"] = str(jdk / "bin") + os.pathsep + environment.get("PATH", "")
    return subprocess.run(command, cwd=ROOT, env=environment).returncode


if __name__ == "__main__":
    raise SystemExit(main())
