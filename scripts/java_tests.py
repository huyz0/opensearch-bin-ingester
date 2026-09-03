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


def _blank(text, keep):
    """Same length, same newlines, nothing but `keep` and whitespace."""
    return ''.join(c if c == '\n' else (c if c in keep else ' ') for c in text)


def hashable(text):
    """What a red record is hashed over: code, with literals kept verbatim.

    ⚠️ ONE LEX PASS doing two things that must not be done separately.
    Comments are DROPPED, and whitespace between tokens is collapsed -- but only
    outside literals, whose interiors are appended exactly as written.

    ⚠️ Each half fixes a defect a review demonstrated. Hashing raw made a
    sentence added to a class javadoc invalidate every record in the file, which
    is the churn this task exists to remove. Collapsing whitespace GLOBALLY made
    a shared `EXPECTED = "hello  world"` rewritable to `"hello world"` -- the
    value the test asserts against -- with the gate printing `ok ... unchanged
    since`. Doing both in one pass is what keeps them from trading off.

    ⚠️ Blanking comments length-preservingly, which was tried, is not enough: it
    makes a comment's CONTENT irrelevant and leaves its LENGTH in the hash, so
    adding or deleting one still invalidates.

    ⚠️ Applied AFTER slicing, which is what makes removal safe -- offsets are
    needed only to cut a declaration out, and nothing indexes into the result.
    Callers pass whole declarations or whole remainder pieces, so the re-lex
    cannot begin inside a literal.
    """
    src = decode_unicode_escapes(text)
    # ⚠️ Literals come OUT first, behind placeholders, so the collapse that
    # follows cannot reach inside one. Comments become a single SPACE rather
    # than nothing, and the reason is token fusion, not spacing: the collapse
    # runs afterwards, so `a /*c*/ b` gives `a b` either way -- but `int/*c*/x`
    # gives `int x` with the space and `intx` without it, which is a different
    # program. An earlier note here gave the spacing reason, which is measurably
    # not what happens.
    lits, out, at = [], [], 0
    for m in NOISE.finditer(src):
        out.append(src[at:m.start()])
        if m.lastgroup in ('string', 'char', 'textblock'):
            out.append('\x00%d\x00' % len(lits))
            lits.append(m.group(0))
        else:
            out.append(' ')
        at = m.end()
    out.append(src[at:])
    collapsed = re.sub(r'\s+', ' ', ''.join(out)).strip()
    return re.sub(r'\x00(\d+)\x00', lambda m: lits[int(m.group(1))], collapsed)


def normalise_lp(src):
    """`normalise`, but LENGTH-PRESERVING, so offsets index the RAW source.

    ⚠️ THE POINT IS THE OFFSETS, not the parsing. `normalise` shortens: it
    decodes unicode escapes, replaces a string with `""` and strips annotation
    arguments. Spans taken from it therefore cannot slice the original, and a
    binding key computed from normalised text is blind to exactly what
    `normalise` threw away — every string literal, char literal, text block and
    annotation argument in a test. M0.56's first attempt did that, and review
    proved a whole NDJSON text block and 11 of 12 `@CsvSource` rows could be
    rewritten after a red run with the key unchanged.

    ⚠️ Same removals, same effect on the tokeniser: what remains carries no
    quote, no comment delimiter and no annotation-argument brace. Only the
    lengths differ, and `scan` is indifferent to length.
    """
    out = UNICODE_ESC.sub(
        lambda m: (m.group(1) + chr(int(m.group(2), 16))).ljust(len(m.group(0))), src)
    out = NOISE.sub(lambda m: _blank(m.group(0), '"\''), out)
    # ⚠️ Innermost-first, repeatedly, exactly as `normalise` does -- but the
    # annotation NAME is kept in place and only its argument list is blanked, so
    # the offsets of everything after it do not move.
    prev = None
    while prev != out:
        prev = out
        # ⚠️ `\s*` is INSIDE group 2. Outside it, `re.sub` dropped the space in
        # `@SuppressWarnings ("unchecked")` -- legal Java, no formatter here
        # forbids it -- and the length guard then refused a file `normalise`
        # reads perfectly. Two normalisations disagreeing about whether a real
        # file is parseable is how a gate that rejects real files gets switched
        # off.
        out = re.sub(r'(@[A-Za-z_$][\w$]*)(\s*\([^()]*\))',
                     lambda m: m.group(1) + _blank(m.group(2), ''), out)
    if len(out) != len(src):
        # ⚠️ Names the construct, not two integers: an operator cannot act on
        # `161 -> 160`. This fails CLOSED, which is right -- a span that does
        # not slice what it claims to would bind a record to the wrong bytes.
        for i, (a, b) in enumerate(zip(src, out)):
            if a != b and b != ' ' and b != '\n':
                break
        raise UnparseableJava(
            'normalise_lp changed the length (%d -> %d); first divergence near: %r'
            % (len(src), len(out), src[max(0, i - 40):i + 40]))
    return out


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


def scan_raw_spans(src):
    """{id: (start, end)} as offsets into the RAW `src`.

    ⚠️ Separate entry point rather than a change to `scan`, because `scan`'s
    `body` feeds `check-test-integrity`'s `strength()` scoring and must keep
    the shortened normalisation it has always had. This one exists only so a
    red record can be bound to raw bytes.
    """
    return {k: v['span'] for k, v in _scan(src, pre=normalise_lp(src))[0].items()}


def scan(src):
    """{'pkg.Outer$Inner#method': {'disabled': bool, 'body': str, 'span': (int, int)}}.

    ⚠️ `span` covers the DECLARATION -- from the arming test annotation through
    the closing brace -- where `body` covers only the braces. A red record binds
    to the declaration plus the file's non-test remainder (M0.56), so a test
    must own its annotations and signature: otherwise adding a test would append
    its signature to the shared remainder and invalidate every other record in
    the file, which is the churn that binding was changed to remove.

    ⚠️ These offsets index THIS function's normalised output, which is shorter
    than the source. A red record needs offsets into the raw file, and takes
    them from `scan_raw_spans`, which parses `normalise_lp` instead. Do not
    reach for `span` here to slice an original -- an earlier draft of M0.56 did
    exactly that, and review proved the resulting key blind to every string
    literal, char literal, text block and annotation argument in a test.
    """
    return _scan(src)[0]


def _scan(src, pre=None):
    """(found, detected) -- `detected` counts test methods BEFORE ids collapse.

    Overloads share one qualified id, so `len(found)` under-counts them. The
    cross-check needs the pre-collapse number or it refuses legal Java.
    """
    src = normalise(src) if pre is None else pre
    m = PKG.search(src)
    pkg = m.group(1) + '.' if m else ''

    found = {}
    classes = []           # [name, depth_opened, disabled]
    depth = 0
    pending_class = None   # (name, disabled)
    pending_method = None  # (name, disabled, decl_start)
    armed = False          # a test annotation is waiting for its signature
    arm_start = 0          # where the annotation RUN began -- the declaration start
    run_start = None       # first annotation of a contiguous run, if one is open
    in_annotation_type = False  # inside an `@interface` body
    disabled_here = False  # @Disabled seen in the current annotation block
    open_method = None     # (id, body_start, depth, disabled, decl_start)
    ann_type_depth = None  # brace depth of an @interface body, if open
    detected = 0           # test methods seen, counting overloads separately

    for t in TOKEN.finditer(src):
        kind = t.lastgroup if t.lastgroup in ('name', 'method') else None
        if t.group('decl'):
            pending_class = (t.group('name'), disabled_here)
        elif t.group('ann'):
            # ⚠️ The RUN's start, not the arming annotation's. `@Disabled` or
            # `@Tag` written ABOVE `@Test` would otherwise fall outside the
            # declaration and into the shared remainder -- so adding one test
            # would invalidate every record in the file, which is the churn
            # M0.56 exists to remove, returning through the one annotation
            # ordering the design did not cover.
            if run_start is None:
                run_start = t.start()
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
                # ⚠️ `run_start` too, not just `armed`. A run dangling across an
                # `@interface` declaration made the NEXT test's span start at
                # the annotation above the annotation TYPE -- swallowing the
                # whole declaration out of the shared remainder, so editing a
                # composed annotation left every other test's record valid.
                armed, disabled_here, pending_class = False, False, None
                run_start = None
                in_annotation_type = True
            elif last == 'Disabled':
                disabled_here = True
            elif TEST_ANN.match(last):
                armed = True
                arm_start = run_start
        elif t.group('call'):
            name = t.group('method')
            if armed and classes and name not in NOT_A_METHOD:
                pending_method = (name, disabled_here or any(c[2] for c in classes),
                                  arm_start)
                armed = False
            run_start = None
        elif t.group('open'):
            depth += 1
            if in_annotation_type:
                ann_type_depth = depth
                in_annotation_type = False
                armed = False
                disabled_here = False
                run_start = None
                continue
            if pending_method:
                ident = '%s%s#%s' % (pkg, '$'.join(c[0] for c in classes), pending_method[0])
                open_method = (ident, t.start(), depth, pending_method[1],
                               pending_method[2])
                pending_method = None
                detected += 1
            elif pending_class:
                classes.append([pending_class[0], depth, pending_class[1]])
                pending_class = None
            disabled_here = False
            run_start = None
        elif t.group('close'):
            if ann_type_depth is not None and ann_type_depth == depth:
                ann_type_depth = None
                depth -= 1
                disabled_here = False
                continue
            if open_method and open_method[2] == depth:
                ident, start, _, dis, decl = open_method
                found[ident] = {'disabled': dis, 'body': src[start:t.end()],
                                'span': (decl, t.end())}
                open_method = None
            if classes and classes[-1][1] == depth:
                classes.pop()
            depth -= 1
            disabled_here = False
            run_start = None
        elif t.group('semi'):
            disabled_here = False
            pending_class = None
            run_start = None
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
