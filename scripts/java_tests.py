# SPDX-License-Identifier: Apache-2.0
"""Locate test methods in Java source.

One parser, imported by both `check-tdd` and `check-test-integrity`. They had a
copy each, and review found them disagreeing three separate times -- about
`@Nested` ids, about `@ValueSource`, about which brace opened a method body. Two
gates that must agree on what a test is cannot be kept in agreement by care.

⚠️ This is a lexical parser, not a Java front end. It normalises away the
constructs that break brace counting -- text blocks, char literals, string
literals, comments and annotation arguments -- and then walks braces. It does
not resolve types and does not need to.

⚠️ **Known blind spot: composed annotations.** JUnit 5 lets a project declare
`@Test public @interface ClusterTest {}` and then annotate tests with
`@ClusterTest` alone. This parser recognises the five JUnit annotations by name,
so such a test is invisible to check-tdd and check-test-integrity -- it does not
refuse, it simply does not see it. Resolving that needs cross-file knowledge of
every `@interface` in the tree. Recorded as a backlog task rather than guessed
at, because a gate with an undocumented blind spot is worse than one with a
documented one.
"""
import re

# Matched against an annotation's LAST dotted segment, so the fully-qualified
# `@org.junit.jupiter.api.Test` -- plain Java that JUnit 5 runs -- is seen.
TEST_ANN = re.compile(r'(?:Test|ParameterizedTest|RepeatedTest|TestFactory|TestTemplate)$')
# An annotation name may be qualified: @Test, @api.Test, @org.junit...Test.
ANN_NAME = r'@\s*[A-Za-z_$][\w$]*(?:\s*\.\s*[A-Za-z_$][\w$]*)*'
ANN_BLOCK = re.compile(r'(?:' + ANN_NAME + r'\s*)+')
NOT_A_METHOD = {'if', 'for', 'while', 'switch', 'catch', 'return', 'new',
                'synchronized', 'do', 'else', 'try', 'assert'}
PKG = re.compile(r'^\s*package\s+([\w.]+)\s*;', re.M)

TOKEN = re.compile(r"""
    (?P<decl>\b(?:class|interface|enum|record)\s+(?P<name>[A-Za-z_$][\w$]*))
  | (?P<ann>@\s*[A-Za-z_$][\w$]*(?:\s*\.\s*[A-Za-z_$][\w$]*)*)
  | (?P<open>\{)
  | (?P<close>\})
  | (?P<semi>;)
  | (?P<call>\b(?P<method>[A-Za-z_$][\w$]*)\s*\()
""", re.X)


# One alternation, scanned left to right. `re.sub` takes the leftmost match and
# resumes after it, so a `//` inside a string is consumed BY the string and a
# quote inside a comment is consumed BY the comment. Order within the group only
# breaks ties at the same start position, which is why the text block precedes
# the string -- at `"""` both could match, and the string would take the empty
# `""` and leave a stray quote behind.
NOISE = re.compile(r"""
    (?P<textblock>\"{3}(?:[^"\\]|\\.|"(?!""))*\"{3})
  | (?P<block>/\*.*?\*/)
  | (?P<line>//[^\n]*)
  | (?P<char>'(?:\\.|[^'\\\n])*')
  | (?P<string>"(?:\\.|[^"\\\n])*")
""", re.X | re.S)

_NOISE_SUB = {'textblock': '""', 'block': ' ', 'line': '', 'char': "' '", 'string': '""'}


# JLS 3.3: a unicode escape is translated BEFORE lexing, and only when preceded
# by an even number of backslashes -- so \u0022 really does terminate a string,
# while \\u0022 is a literal backslash-u. Doing what the language does is both
# more faithful and cheaper than refusing: an earlier version refused on any
# such escape and rejected two real OpenSearch test files, and a gate that
# rejects real files gets switched off.
UNICODE_ESC = re.compile(r'(?<!\\)((?:\\\\)*)\\u+([0-9a-fA-F]{4})')


def decode_unicode_escapes(src):
    return UNICODE_ESC.sub(lambda m: m.group(1) + chr(int(m.group(2), 16)), src)


def normalise(src):
    """Remove everything that can carry a brace without meaning one.

    ⚠️ This MUST stay a single pass. It was three sequential re.sub calls, and
    `assertThat(url).isEqualTo("http://localhost:9000")` -- a line this
    project's first store test will contain -- had its line truncated by the
    comment pass, which orphaned the opening quote, which then paired with a
    later quote and deleted every test method in between. Both gates then
    reported success on a change they should have rejected.
    """
    src = NOISE.sub(lambda m: _NOISE_SUB[m.lastgroup], decode_unicode_escapes(src))
    # ⚠️ NOT constrained by any fixture -- one of several here; see the note on
    # the `@interface` branch below. The mutation audit says why: the
    # `ann` token already consumes `@Name`, so the `(` that follows has no
    # identifier before it and cannot be read as a method call. The phantom this
    # loop was written to prevent is therefore unreachable through `scan`. Kept
    # as belt-and-braces for annotation arguments that survive `NOISE` with
    # unbalanced braces; deleting it breaks no test, which is recorded rather
    # than hidden.
    #
    # Annotation arguments hold braces and parentheses of their own:
    # `@ValueSource(ints = {1, 2, 3})` was read as a method body, and
    # `@ExtendWith(Foo.class)` as a class declaration. Innermost first,
    # repeatedly, so nested annotations collapse too. Safe as a separate pass:
    # by here there are no string or comment delimiters left to confuse it.
    prev = None
    while prev != src:
        prev = src
        src = re.sub(r'(@[A-Za-z_$][\w$]*)\s*\([^()]*\)', r'\1', src)
    return src


def scan(src):
    """{'pkg.Outer$Inner#method': {'disabled': bool, 'body': str}}."""
    return _scan(src)[0]


def _scan(src):
    """(found, detected) -- `detected` counts test methods BEFORE ids collapse.

    Overloads share one qualified id, so `len(found)` under-counts them. The
    cross-check needs the pre-collapse number or it refuses legal Java.
    """
    src = normalise(src)
    m = PKG.search(src)
    pkg = m.group(1) + '.' if m else ''

    found = {}
    classes = []           # [name, depth_opened, disabled]
    depth = 0
    pending_class = None   # (name, disabled)
    pending_method = None  # (name, disabled)
    armed = False          # a test annotation is waiting for its signature
    in_annotation_type = False  # inside an `@interface` body
    disabled_here = False  # @Disabled seen in the current annotation block
    open_method = None     # (id, body_start, depth)
    ann_type_depth = None  # brace depth of an @interface body, if open
    detected = 0           # test methods seen, counting overloads separately

    for t in TOKEN.finditer(src):
        kind = t.lastgroup if t.lastgroup in ('name', 'method') else None
        if t.group('decl'):
            pending_class = (t.group('name'), disabled_here)
        elif t.group('ann'):
            last = re.sub(r'\s+', '', t.group('ann'))[1:].split('.')[-1]
            # ⚠️ UNCONSTRAINED by the suite. Removing this branch, or the
            # counter's `interface` skip, or both, leaves every fixture green:
            # the `ann` token consumes `@interface` before `decl` can see it, so
            # the annotation type never becomes a class and its members never
            # reach the class stack.
            #
            # Kept rather than deleted because its absence is unvalidated, and a
            # fixture written to pass only against the current implementation
            # would be worse than an honest gap. ⚠️ This is a gap, not a proof:
            # several other branches here are equally unconstrained (NOT_A_METHOD,
            # the `classes` guard, UNICODE_ESC's even-backslash rule). Closing
            # them is NOT tracked: backlog M0.17 covers the composed-annotation
            # case only, and widening a task from a code comment is how a gap
            # gets marked done without being done.
            if last == 'interface':
                # `@interface Foo` declares an annotation type, not a test.
                # JUnit 5 composed annotations (@ClusterTest and friends) carry
                # @Test on the declaration; treating that as a test method both
                # invented an id and, via the cross-check, hard-blocked the gate.
                armed, disabled_here, pending_class = False, False, None
                in_annotation_type = True
            elif last == 'Disabled':
                disabled_here = True
            elif TEST_ANN.match(last):
                armed = True
        elif t.group('call'):
            name = t.group('method')
            if armed and classes and name not in NOT_A_METHOD:
                pending_method = (name, disabled_here or any(c[2] for c in classes))
                armed = False
        elif t.group('open'):
            depth += 1
            if in_annotation_type:
                ann_type_depth = depth
                in_annotation_type = False
                armed = False
                disabled_here = False
                continue
            if pending_method:
                ident = '%s%s#%s' % (pkg, '$'.join(c[0] for c in classes), pending_method[0])
                open_method = (ident, t.start(), depth, pending_method[1])
                pending_method = None
                detected += 1
            elif pending_class:
                classes.append([pending_class[0], depth, pending_class[1]])
                pending_class = None
            disabled_here = False
        elif t.group('close'):
            if ann_type_depth is not None and ann_type_depth == depth:
                ann_type_depth = None
                depth -= 1
                disabled_here = False
                continue
            if open_method and open_method[2] == depth:
                ident, start, _, dis = open_method
                found[ident] = {'disabled': dis, 'body': src[start:t.end()]}
                open_method = None
            if classes and classes[-1][1] == depth:
                classes.pop()
            depth -= 1
            disabled_here = False
        elif t.group('semi'):
            disabled_here = False
            pending_class = None
    return found, detected


class UnparseableJava(Exception):
    """The parser does not trust its own answer for this file."""


def scan_checked(src, path='<source>'):
    """`scan`, with its result cross-checked against independent signals.

    ⚠️ This exists because the failure that matters is not a wrong answer, it is
    a wrong answer that looks like a clean one. Every parser bug review found --
    a `'}'` char literal, `@ValueSource(ints = {…})`, a string containing `//`,
    a comment containing `\"\"\"` -- ended with a gate printing `ok` on a change
    it should have rejected. A gate that cannot parse must say so.
    """
    norm = normalise(src)
    found, detected = _scan(src)

    # 1. Every construct must have consumed its own delimiters.
    if norm.count('{') != norm.count('}'):
        raise UnparseableJava('%s: braces do not balance after normalisation '
                              '(%d open, %d close)'
                              % (path, norm.count('{'), norm.count('}')))
    if norm.count('"') % 2:
        raise UnparseableJava('%s: an unpaired quote survives normalisation' % path)

    # 2. The brace walk and a plain count of annotation blocks must agree.
    #    ⚠️ These are NOT independent -- both resolve an annotation to its simple
    #    name through TEST_ANN, deliberately, because when they used separate
    #    spellings the counter could not see `@org.junit.jupiter.api.Test` and
    #    the two agreed on a self-consistent zero. Sharing one vocabulary is what
    #    makes them disagree when the BRACE WALK loses its place, which is the
    #    failure this check exists for. It does not detect an annotation neither
    #    of them knows -- see the composed-annotation note above.
    # Count annotation BLOCKS containing a test annotation, not annotations:
    # `@Test @Disabled void x()` is two annotations and one test method.
    annotated = 0
    for m in ANN_BLOCK.finditer(norm):
        names = [re.sub(r'\s+', '', a)[1:].split('.')[-1]
                 for a in re.findall(ANN_NAME, m.group(0))]
        if not any(TEST_ANN.match(n) for n in names):
            continue
        # An `@interface` declaration carries the test annotation but declares
        # no test method. Counting it made every JUnit 5 composed annotation --
        # the idiom the clusterTest source set will use -- refuse forever.
        if 'interface' in names:
            continue
        if re.match(r'\s*(?:public|protected|private|abstract|static|final|\s)*@\s*interface\b',
                    norm[m.end():m.end() + 120]):
            continue
        annotated += 1
    if annotated != detected:
        raise UnparseableJava(
            '%s: found %d test method(s) but %d test annotation(s). The parser '
            'lost track of the file, and a gate that guesses here reports '
            'success on tests it never saw.' % (path, detected, annotated))
    return found


def test_ids(src, path='<source>'):
    return set(scan_checked(src, path))
