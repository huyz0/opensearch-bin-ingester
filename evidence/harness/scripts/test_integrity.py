# SPDX-License-Identifier: Apache-2.0
"""Detect a test weakened in the same change as the production code it covers.

Two earlier versions of this gate were green when they should have been red:

* the first compared the COUNT of added and removed assertion lines, which is
  blind to a rewrite in place -- one line out, one line in;
* the second scored assertion strength but summed it per FILE, so a commit that
  added one test while gutting another netted to zero.

So the scoring is per test METHOD, and a disabled test counts as a removed one.

⚠️ What it still cannot see: a rewrite that keeps the same strength class --
`isEqualTo(expected)` becoming `isEqualTo(actualComputedTheSameWay)`. No
lexical rule catches that. It is the test reviewer's job, which is why
non-negotiable 5 buys a second pair of eyes rather than a third script.
"""
import os, re, subprocess, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from java_tests import scan_checked, UnparseableJava   # one parser, shared with check-tdd

RED, GREEN, YEL = '\033[31m', '\033[32m', '\033[33m'
OFF = '\033[0m'

# Strength is "how much of the output space does this rule out". An assertion
# that pins an exact value rules out nearly everything; a null check rules out
# one value. The ordering matters more than the exact numbers.
STRENGTH = [
    (3, r'\b(assertEquals|assertArrayEquals|assertIterableEquals|assertLinesMatch)\s*\('),
    (3, r'\.(isEqualTo|isEqualByComparingTo|containsExactly|containsExactlyInAnyOrder|'
        r'isSameAs|hasSize|isCloseTo|hasToString|containsOnly|isDeepEqualTo)\s*\('),
    (2, r'\b(assertTrue|assertFalse|assertThrows|assertThrowsExactly|assertSame|'
        r'assertNotEquals)\s*\('),
    (2, r'\.(isTrue|isFalse|contains|containsEntry|startsWith|endsWith|isGreaterThan|'
        r'isLessThan|isBetween|isInstanceOf|hasMessage|hasMessageContaining|matches|'
        r'isNotEqualTo|allSatisfy|anySatisfy)\s*\(?'),
    (1, r'\b(assertNotNull|assertNull|assertDoesNotThrow|assertNotSame)\s*\('),
    (1, r'\.(isNotNull|isNull|isNotEmpty|isEmpty|doesNotThrowAnyException|isPresent|'
        r'isNotBlank)\s*\(?'),
    # cost.md's Enforcement section names three assertions to write first and
    # two of them are `.isZero()`. Absent from this table they scored 0, so the
    # project's flagship cost assertions could not be seen to weaken at all.
    (3, r'\.(isZero|isOne|isNotZero|isPositive|isNegative|isNotPositive|isNotNegative)\s*\(?'),
    (3, r'\b(assertZero)\s*\('),
]
TEST_PATH = re.compile(r'/src/(?:test|integrationTest|clusterTest)/java/')
MAIN_PATH = re.compile(r'/src/main/java/')


def strength(text):
    return sum(w * len(re.findall(p, text)) for w, p in STRENGTH)


def test_methods(src, path='<source>'):
    """{'pkg.Class#method': (strength, disabled)} -- keyed by the QUALIFIED id.

    Keyed by bare method name, two same-named methods in different @Nested
    classes collapsed into one entry and gutting the strong one was invisible.
    """
    return {k: (strength(v['body']), v['disabled'])
            for k, v in scan_checked(src, path).items()}


def git(*a):
    r = subprocess.run(['git'] + list(a), capture_output=True, text=True)
    return r.stdout if r.returncode == 0 else None


def main(msg_file, base):
    if base:
        names = git('diff', '--name-only', base, 'HEAD') or ''
        after_ref, before_ref = 'HEAD:', base + ':'
    else:
        names = git('diff', '--cached', '--name-only') or ''
        after_ref, before_ref = ':', 'HEAD:'
    paths = names.split()

    if not any(p.endswith('.java') and MAIN_PATH.search(p) for p in paths):
        print('  %sok%s   no production change in this commit' % (GREEN, OFF))
        return 0

    weakened, removed, disabled = [], [], []
    for p in paths:
        if not (p.endswith('.java') and TEST_PATH.search(p)):
            continue
        before = git('show', before_ref + p)
        if before is None:
            continue                       # new test file: nothing to weaken
        after = git('show', after_ref + p)
        try:
            b = test_methods(before, p)
            a = test_methods(after, p) if after is not None else {}
        except UnparseableJava as e:
            print('  %sFAIL%s %s' % (RED, OFF, e))
            print('         Refusing to certify a file the parser cannot read.')
            return 1
        for name, (b_str, b_dis) in b.items():
            if name not in a:
                removed.append('%s  %s' % (p, name))
            else:
                a_str, a_dis = a[name]
                # Per METHOD, not per file: summing per file let an added test
                # cancel out a gutted one.
                if a_str < b_str:
                    weakened.append(('%s  %s' % (p, name), b_str, a_str))
                if a_dis and not b_dis:
                    disabled.append('%s  %s' % (p, name))

    if not (weakened or removed or disabled):
        print('  %sok%s   no test weakened, disabled or removed alongside the production change'
              % (GREEN, OFF))
        return 0

    # testing.md rule 5: the justification belongs in the commit body, and it
    # must name what it is justifying.
    body = open(msg_file).read() if msg_file else ''
    def has(tag):
        return re.search(r'^%s:\s*\S' % tag, body, re.M | re.I)

    bad = False
    for name, b, a in weakened:
        if has('Test-weakened'):
            print('  %sWARN%s %s: assertion strength %d -> %d, justified in the commit body'
                  % (YEL, OFF, name, b, a))
        else:
            print('  %sFAIL%s %s: assertion strength fell %d -> %d' % (RED, OFF, name, b, a))
            bad = True
    for name in removed + disabled:
        verb = 'disabled' if name in disabled else 'removed'
        if has('Test-removed'):
            print('  %sWARN%s %s %s, justified in the commit body' % (YEL, OFF, name, verb))
        else:
            print('  %sFAIL%s %s %s alongside a production change' % (RED, OFF, name, verb))
            bad = True
    if bad:
        print('         This is the "production changed to satisfy the test" inversion')
        print('         (testing.md rule 4). Either split the change, or add a trailer to')
        print('         the commit body naming what you did and why:')
        print('             Test-weakened: <which assertion, and why the new one is right>')
        print('             Test-removed:  <which test, and why it is no longer needed>')
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else '',
                  sys.argv[2] if len(sys.argv) > 2 else ''))
