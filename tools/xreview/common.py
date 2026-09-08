#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Two helpers every other module needs, and nothing else.

⚠️ `die` PRINTS TO STDERR AND EXITS. It is the only way this tool reports a
refusal, so that a refusal can never be mistaken for output: a caller reading
stdout gets a packet or a verdict, never an excuse.
"""
import subprocess
import sys


def die(code, msg):
    print(f"  \033[31mFAIL\033[0m {msg}", file=sys.stderr)
    sys.exit(code)


def git(*args, check=True):
    r = subprocess.run(["git", *args], capture_output=True, text=True)
    if check and r.returncode != 0:
        die(2, f"git {' '.join(args)}: {r.stderr.strip()}")
    return r.stdout
