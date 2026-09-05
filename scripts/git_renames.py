# SPDX-License-Identifier: Apache-2.0
"""Where a staged path CAME FROM, when git scored it a rename (M0.53).

⚠️ `git diff --name-only` emits only the DESTINATION of a rename, and both
harness scanners read `<ref>:<destination>` to learn what a file looked like
before. For a renamed file that blob does not exist, so:

  * `test_integrity.py` got `None`, took its "new test file: nothing to weaken"
    branch, and never saw the SOURCE path at all -- so the removal side went
    unexamined too. The input that first proved it -- a test moved with six
    assertions deleted and one `isEqualTo(3)` -> `isNotNull()` -- was scored
    R095 by git, and the gate printed `ok no test weakened, disabled or
    removed`, exit 0.
  * `tdd_scan.py` got the empty string, so every moved test read as NEW and
    demanded a fresh red record for tests that had simply moved.

⚠️ FOLLOWING THE RENAME IS NOT ENOUGH BY ITSELF. Module and top-level package
are the same thing here, so a MODULE move changes every test id -- and handing
the scanner the source blob unchanged made a pure move read as a wholesale
REMOVAL, which is a false accusation whose documented workaround (`Test-removed:`,
matched over the whole commit body) would silence a genuine deletion in the same
commit. `java_tests.rekey` re-qualifies the before-ids into the destination's
package; see its own note.

Both halves go blind on the SAME input, which is why this lives in one place
rather than being fixed twice: the two scanners already share `java_tests` for
exactly that reason.
"""
import subprocess


def parse_name_status(text):
    """{destination: source} from `--name-status -M` output.

    ⚠️ A PURE TRANSFORM over TEXT, separated from the git call so the parsing
    rules are testable directly. Round-1 test review measured that dropping the
    `R` guard, and swapping source with destination, both survived a suite that
    could only reach this code through git -- because the inputs that
    distinguish them (a `C` copy line, an unmerged `U` line) cannot be produced
    on demand from a fixture repository.
    """
    renamed = {}
    for line in text.splitlines():
        # R<score>\t<source>\t<destination>. `C` is a COPY: the source still
        # exists, so it is not a rename and must not be followed.
        parts = line.split('\t')
        if len(parts) == 3 and parts[0].startswith('R'):
            renamed[parts[2]] = parts[1]
    return renamed


def rename_map(base=''):
    """{destination: source} for every path git scored as a rename.

    ⚠️ `-M` is explicit rather than relying on `diff.renames`, which is
    configuration and therefore not the same on every machine -- a gate whose
    blindness depends on a developer's git config is worse than one that is
    reliably blind.
    """
    args = ['diff', '--name-status', '-M']
    args += [base, 'HEAD'] if base else ['--cached']
    r = subprocess.run(['git'] + args, capture_output=True, text=True)
    if r.returncode != 0:
        return {}
    return parse_name_status(r.stdout)


if __name__ == '__main__':
    # ⚠️ A SEAM for testing the parser as the pure transform it is, over text
    # no fixture repository can be made to emit -- the same seam `tdd_scan.py`
    # exposes for `task-for`.
    import sys
    for dest, src in sorted(parse_name_status(sys.stdin.read()).items()):
        print('%s\t%s' % (dest, src))
