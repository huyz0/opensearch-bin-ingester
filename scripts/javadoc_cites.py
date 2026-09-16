# SPDX-License-Identifier: Apache-2.0
"""Resolve the test fixtures that `src/main` javadoc cites.

M5.34's argument for naming a fixture rather than counting one is that A NAME
IS CHECKABLE. It was not checked by anything: three of that row's four names
were split across `{@code}` spans and grepped nowhere, and review caught them
by hand. Non-negotiable 9 puts a predicate over files at rung 3, so this is a
script rather than a review lens.

⚠️ PER LINE, AND THE LINES ARE NEVER JOINED. That is the whole mechanism rather
than an implementation detail. Both instances that motivated this were names
broken across a line break -- one inside a single `{@code}` span, one across
two -- which leaves the first half still parseable as a citation and naming a
method that does not exist, while the full name greps nowhere from `src/main`.
Joining a javadoc's lines back together would reconstruct a name no reader's
`grep` can find and report the tree clean, which is the silence this exists to
end.

⚠️ FOUR SHAPES ARE REFUSED, AND THE LIST IS THE COVERAGE CLAIM. An earlier
draft of this file claimed instead that "a tree may not QUOTE a citation it
cannot resolve", and review falsified it with a live instance the gate walked
past. What is refused:

  1. `Class.method` / `Class#method` naming no directly-`@Test`-annotated
     method.
  2. A bare `Class` INSIDE an inline tag span naming no test source file.
  3. A `*Test`/`*IT` name inside an inline tag span left OPEN at end of line --
     the citation continues onto the next line, so it greps nowhere whole.
  4. A name inside a closed span that ends AT its separator, `{@code FooTest.}`
     -- the break landed on the dot rather than inside the identifier.

⚠️ AND WHAT IS NOT. First: a break landing inside the CLASS name rather than
after it -- `{@code FooT` / `est.aCase}` -- leaves no `*Test`/`*IT` token on
either line, so no rule sees it. Review measured that shape reporting `ok 0
citation(s) all resolve`. Closing it would mean matching on prefixes of class
names, whose false-positive surface is every capitalised word in every javadoc.

⚠️ Second: a bare class name in ordinary prose, outside any inline tag. ⚠️ THE
REASON GIVEN HERE WAS FALSE ONCE AND IS STATED AS MEASURED NOW: an earlier
draft said checking prose would swamp the gate in BIT, HIT, JIT and UNSPLIT,
quoting the pattern from BEFORE the lowercase letter was required. Run against
the SHIPPED pattern there are 0 false positives, so the cost of checking prose
is small; it is simply not checked yet, and saying otherwise made this file
argue with itself.

⚠️ NO COUNT AND NO RECIPE FOR ONE IS WRITTEN HERE. Three drafts produced three
answers, and the third was a PROCEDURE rather than a number -- which review
then ran and got the same wrong answer from, because the subtraction it
prescribes cannot remove the shape the next paragraph lists. The count is not
load-bearing; nothing here needs it.

⚠️ Third, and unnamed until review found it: a bare class name inside a span
OPENED ON THE PREVIOUS LINE. `SPAN` needs both braces on one line and
`open_tail` returns the text after the opener, which on the continuation line
is empty, so neither reads it. Live at `DefaultIngest.java`'s citation of
`CommitRetryTripleTest`.

⚠️ Fourth: a class name whose prefix before the suffix is ALL CAPS --
`WALTest`, `IOTest`, `S3IT`, `V2Test` -- excluded by the same lowercase
requirement that keeps `LIMIT` and `COMMIT` out. That one is a SILENT MISS
rather than a false refusal: a dangling `WALTest` citation would pass. No such
class exists in the tree today, and the trade was made knowing which direction
it errs in.

⚠️ THE CLASS PATTERN IS SCOPED TO `*Test` AND `*IT` throughout, for the same
reason: otherwise the surface is every dotted phrase in every javadoc in the
tree -- `Duration.ofSeconds`, `Math.max`, `this.field`. ⚠️ IT COSTS THE
`*Conformance` SUITES, AND AN EARLIER DRAFT CLAIMED NO SUCH CLASS EXISTED:
`BinStoreConformance`, `MultipartConformance`, `ConditionalWriteConformance`
and `PresignConformance` all carry directly-`@Test`-annotated methods, and
`BinStore.java` and `GrantIssuer.java` each cite one today -- unchecked, so a
rename leaves both dangling silently. M5.78 owns it.

⚠️ A METHOD TARGET MUST BE DIRECTLY `@Test`-ANNOTATED, not merely present, and
a CLASS target need only be a test source file. The asymmetry is deliberate.
`java_tests.py` is the single parser for the method question -- the same one
`check-tdd` and `check-test-integrity` use -- and it sees only a DIRECT
`@Test`. Resolving a class through it would refuse every `*IT` in
`plugin/src/clusterTest`: those six classes extend OpenSearch's test base
classes and name their cases by the JUnit3 convention, `void testX()`, with no
annotation at all -- `grep -c '@Test'` is 0 in every one of them. Measured,
`DeleteAndVersionIT` dangles against the parsed index and resolves against the
file names. ⚠️ M0.17 DOES NOT COVER THIS AND AN EARLIER DRAFT SAID IT DID: that
row is about a COMPOSED annotation, and implementing it leaves these six
yielding zero methods exactly as they do now.
"""
import re
import sys

import java_tests

# Both javadoc spellings. `Class#method` is the `@link` form and is the same
# defect exactly; excluding it would leave live citations unchecked for no
# reason but the task row's choice of example.
# ⚠️ A LOWERCASE LETTER IS REQUIRED, which is what keeps this off ordinary
# ALL-CAPS constants. Review MEASURED the previous pattern refusing
# `{@code LIMIT}`, `{@code MAX_UNIT}` and `{@code WAIT_LIMIT}` as dangling
# citations -- a FALSE REFUSAL with no remedy but `SKIP=`, which is the
# direction of error a gate must not have. Measured again after: 0 of the
# tree's 177 `*Test`/`*IT` class names are lost, and BIT, HIT, JIT, COMMIT,
# EXPLICIT, IMPLICIT, INHERIT and UNSPLIT all stop matching.
CITE = re.compile(
    r'\b([A-Z][A-Za-z0-9_$]*[a-z][A-Za-z0-9_$]*(?:Test|IT))([.#])([a-z_$][A-Za-z0-9_$]*)')
NAME = re.compile(r'\b([A-Z][A-Za-z0-9_$]*[a-z][A-Za-z0-9_$]*(?:Test|IT))\b')
SPAN = re.compile(r'\{@(?:code|link|linkplain|literal)\s+([^{}]*)\}')
TAG = re.compile(r'\{@(?:code|link|linkplain|literal)\b|[{}]')


def is_main(path):
    return '/src/main/' in '/' + path


def is_test_source(path):
    """A java file in a source set that is not `main`.

    ⚠️ THE `java` SEGMENT IS LOAD-BEARING. `buildSrc/src/test/resources/`
    holds deliberately unparseable fixtures for `java_tests.py`'s own refusal
    cases; they are resources with a `.java` suffix, not tests, and indexing
    them would make this gate refuse the tree for another gate's fixture.
    """
    parts = path.split('/')
    for i, p in enumerate(parts):
        if p == 'src' and i + 2 < len(parts):
            return parts[i + 1] != 'main' and parts[i + 2] == 'java'
    return False


def index(paths):
    """simple class name -> the directly-@Test-annotated methods it declares.

    ⚠️ KEYED ON THE SIMPLE NAME, so two test classes sharing one in different
    modules MERGE their method sets -- `ingest`'s and `sequencer`'s
    `CommitLogTest` are one entry here, and a citation of either resolves
    against both. That is a real weakening and it is stated rather than hidden:
    a javadoc citation carries no package, so distinguishing them would mean
    demanding a spelling no citation in this tree uses.

    A nested id `pkg.Outer$Inner#m` is filed under BOTH simple names, because a
    javadoc cites whichever one a reader would type.
    """
    by_class = {}
    for path in paths:
        with open(path, encoding='utf-8') as f:
            src = f.read()
        for ident in java_tests.test_ids(src, path):
            qualified, method = ident.split('#', 1)
            for simple in qualified.split('.')[-1].split('$'):
                by_class.setdefault(simple, set()).add(method)
    return by_class


def open_tail(line):
    """The text of an inline tag span left unterminated at end of line."""
    depth = 0
    start = None
    for m in TAG.finditer(line):
        token = m.group(0)
        if token == '}':
            depth = max(0, depth - 1)
            if depth == 0:
                start = None
        else:
            if depth == 0 and token.startswith('{@'):
                start = m.end()
            depth += 1
    return line[start:] if depth > 0 and start is not None else None


def scan_line(line, by_class, classes):
    """(how many citations this line carries, what is wrong with them).

    ⚠️ ONE TRAVERSAL RETURNS BOTH. They were counted separately and disagreed:
    a split citation was collected as a finding but counted as no citation, so
    the gate printed `1 of 0 citation(s)` -- a line that reads as a bug in the
    gate rather than a defect in the tree.
    """
    bad = []
    cited = 0
    # ⚠️ THE SEPARATOR IS ECHOED AS WRITTEN, `.` or `#`. A gate that normalises
    # it prints a string the reader cannot grep for, which is the same defect
    # one level down from the one this gate exists to catch.
    for cls, sep, method in CITE.findall(line):
        cited += 1
        if method not in by_class.get(cls, ()):
            bad.append(('names no directly-@Test-annotated method', cls + sep + method))
    for span in SPAN.findall(line):
        for m in NAME.finditer(span):
            cls = m.group(1)
            after = span[m.end():m.end() + 1]
            if after in ('.', '#'):
                rest = span[m.end() + 1:m.end() + 2]
                if not (rest.isalpha() or rest in ('_', '$')):
                    cited += 1
                    bad.append(('ends at its separator, so the name is cut short',
                                cls + after))
                continue
            cited += 1
            if cls not in classes:
                bad.append(('names no test source file', cls))
    tail = open_tail(line)
    if tail:
        for m in NAME.finditer(tail):
            cited += 1
            bad.append(('is SPLIT across lines -- write the whole citation on one',
                        m.group(1)))
    return cited, bad




def main():
    paths = [p for p in (l.strip() for l in sys.stdin) if p]
    main_paths = sorted(p for p in paths if is_main(p))
    test_paths = sorted(p for p in paths if is_test_source(p))
    classes = {p.split('/')[-1][:-5] for p in test_paths}

    # ⚠️ A PARSER THAT LOST ITS PLACE MUST REFUSE, NEVER SKIP. `scan_checked`
    # raises when its brace walk and its annotation count disagree; swallowing
    # that would drop every test in the file from the index and report every
    # citation of it as dangling -- an avalanche of false failures blamed on the
    # citations. The honest answer is to say the file could not be read.
    try:
        by_class = index(test_paths)
    except java_tests.UnparseableJava as e:
        print('           %s' % e)
        print('__SUMMARY__ 0 -1 0 0')
        return 1

    cited = 0
    bad = []
    for path in main_paths:
        with open(path, encoding='utf-8') as f:
            for n, line in enumerate(f, 1):
                n_cited, problems = scan_line(line, by_class, classes)
                cited += n_cited
                for why, text in problems:
                    bad.append((path, n, text, why))

    for path, n, text, why in bad:
        print('           %s:%d: %s -- %s' % (path, n, text, why))
    # ⚠️ THE REASON STRINGS OF RULES 3 AND 4 ARE A CONTROL SIGNAL HERE, not
    # only prose: this line greps `SPLIT` and `separator` out of them to decide
    # whether the line-break advice applies. Rewording either without changing
    # this condition silently drops the advice from the path it exists for.
    if any('SPLIT' in why or 'separator' in why for _, _, _, why in bad):
        print('           A name broken across a line break leaves the first half')
        print('           parseable and dangling while the whole name greps nowhere.')
        print('           Put each citation on ONE line.')
    methods = sum(len(v) for v in by_class.values())
    print('__SUMMARY__ %d %d %d %d' % (cited, len(bad), len(by_class), methods))
    return 1 if bad else 0


if __name__ == '__main__':
    sys.exit(main())
