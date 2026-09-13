# SPDX-License-Identifier: Apache-2.0
"""Read Java paths on stdin; print the count, then one line per violation.

⚠️ WHAT THIS CANNOT SEE, STATED RATHER THAN LEFT TO BE DISCOVERED. It is a
DENY-LIST over source text, so it catches the constructs named below and
nothing else. Reflection reaches any of them; a helper in a module this scan
exempts reaches them for a caller it does not; a dependency's API can open a
socket without naming one here. A rung-1 mechanism -- a module system that
makes `java.nio.file` unreachable from business logic -- would not need a list
at all, and does not exist for the JDK's own packages. So this raises the cost
of reaching past a seam; it does not make it impossible, and AGENTS.md says so
beside the rule.

⚠️ COMMENTS, STRING LITERALS, CHAR LITERALS AND TEXT BLOCKS ARE BLANKED FIRST,
by a single left-to-right pass rather than by a sequence of regexes. The
sequence was wrong and review MEASURED two ways past it: `log("see
https://x/y")` let the line-comment pattern match inside the string and blank
the real `Files.readAllBytes` after it, and `c == '"'` mis-paired the string
pattern and swallowed a whole filesystem read. One pass never looks at a
delimiter already inside something else, which is what removed BOTH.

⚠️ IT IS STILL A HAND-WRITTEN LEXER AND NOT A PARSER, so "that class of bug is
impossible now" would be the wrong claim -- an earlier draft of this header made
it, and the very next round MEASURED an escaped `\"""` inside a text block
blanking the rest of the file. Each of the four branches honours escapes and
stops where Java says the construct stops; that is the whole of the guarantee.
"""
import io
import re
import sys

# ⚠️ EACH ENTRY SOURCES A REAL INSTANCE; none merely NAMES a type. `Clock` is
# absent on purpose -- taking one as a parameter is the shape
# code-structure.md rule 4 requires, and banning the word would red the whole
# sequencer module.
#
# ⚠️ WHITESPACE IS ALLOWED AROUND EVERY `.` AND `(`, because the scan runs over
# the whole file rather than line by line: review MEASURED `Instant\n    .now()`
# passing a per-line scan.
def _shape(literal):
    """A literal call shape as a regex.

    Whitespace-tolerant around every separator, because the scan runs over the
    whole file rather than line by line -- review MEASURED `Instant\\n    .now()`
    passing a per-line scan. Anchored with `\\b` so an identifier ENDING in the
    needle (`dataFiles.size()`) is not a match. And `.` also matches `::`,
    because `Instant::now` sources the same clock as `Instant.now()` and review
    MEASURED the method-reference form scanning clean.
    """
    if literal.startswith('new '):
        # The paren is REQUIRED here and nowhere else: without it `new File(`
        # would also match `new FileInputStream(`, reporting the wrong name.
        # A package qualifier is skipped, so `new java.io.File(` is the same
        # construct as `new File(` with an import -- which is the form a file
        # reaching past a seam ONCE tends to use.
        qualifier = r'(?:[A-Za-z_$][\w$]*\s*\.\s*)*'
        return re.compile(r'\bnew\s+' + qualifier + re.escape(literal[4:-1]) + r'\s*\(')
    if '.' not in literal:
        # ⚠️ A BARE CALL, which a STATIC IMPORT produces: review MEASURED
        # `import static java.lang.System.*;` then `currentTimeMillis()`
        # reaching the real clock while `System.currentTimeMillis(` saw
        # nothing. No receiver to anchor, so the name alone must carry it.
        name = re.escape(literal[:-1])
        return re.compile(r'(?:\b' + name + r'\s*\(|::\s*' + name + r'\b)')
    if literal.endswith('()'):
        # ⚠️ THE RECEIVER MUST LOOK LIKE A TYPE. `Instant.now()` is a static
        # factory and every JDK one is called on a capitalised name, while
        # `window.now()` is a record accessor reaching nothing -- review
        # MEASURED the unanchored form refusing a file that touched no clock.
        # ⚠️ THE NO-ARGUMENT CALL *AND* THE METHOD REFERENCE, which has no
        # parens to be empty. Review MEASURED `Instant::now` passing when the
        # arity was pinned by literal `()` alone -- the two forms source the
        # same clock and `LocalDate.now(clock)` must still pass.
        owner, _, member = literal.partition('.')
        member = re.escape(member[:-2])
        head = (r'\b' + re.escape(owner)) if owner else r'\b[A-Z][\w$]*'
        return re.compile(head + r'(?:\s*\.\s*' + member + r'\s*\(\s*\)'
                          + r'|\s*::\s*' + member + r'\b)')
    owner, _, member = literal.partition('.')
    # ⚠️ THE TRAILING `\b` GOES ON A WHOLE METHOD NAME AND NOT ON A PREFIX.
    # `Clock.system` is a prefix standing for `systemUTC`, `systemDefaultZone`
    # and `system`; anchoring it would demand a word boundary after `system`
    # and match none of the three. A literal ending in `(` is a whole name, and
    # there the boundary is what stops `now` matching inside a longer one.
    whole = member.endswith('(')
    member = member[:-1] if whole else member
    head = (r'\b' + re.escape(owner)) if owner else ''
    tail = re.escape(member) + (r'\b' if whole and member else '')
    return re.compile(head + r'\s*(?:\.|::)\s*' + tail)


CLOCK = 'the real clock'
SOCKET = 'a socket'
DISK = 'the filesystem'
# ⚠️ A SUBPROCESS IS ITS OWN KIND. Review MEASURED `Runtime.getRuntime(`
# reported as reaching the filesystem while `availableProcessors()` -- which
# reaches nothing -- was refused by it. The reach is `.exec(`, and what it
# reaches is a process.
PROCESS = 'a subprocess'

# ⚠️ THE I/O FAMILIES ARE BANNED BY PACKAGE, NOT BY CLASS NAME, and that is the
# whole difference between this list and the three that preceded it. Naming
# classes made every round a sibling hunt: review found `AsynchronousFileChannel`
# beside the listed `FileChannel`, then `SSLSocketFactory` beside the listed
# `SocketFactory` and `MulticastSocket` beside `DatagramSocket`. Banning the
# PACKAGE ends that: no sibling can be NAMED inside a listed one.
#
# ⚠️ IT DOES NOT END THE ENUMERATION, and an earlier draft of this very
# comment claimed it did. A reach can live in a package that is not listed:
# `java.util.jar.JarFile` EXTENDS the banned `java.util.zip.ZipFile`, and
# `javax.sql.DataSource` hands back a `java.sql` connection a `var` never
# names. Both were MEASURED passing, one round apart. This list grows when a
# reader finds a package a reach lives in; what it no longer does is grow by
# one SIBLING at a time.
#
# ⚠️ MEASURED ON THIS TREE, AND NOT THE WAY AN EARLIER DRAFT PUT IT. Fifty-
# three non-exempt `src/main` files DO name `java.io.` or `java.util.zip.` --
# they pass because `ALLOWED` carves out the byte and checksum types, not
# because the packages are unused. What is true is that the rule as a whole
# matches exactly ONE file in the tree, `LocalFsBinStore`, and that file is the
# adapter. A reader who believed the earlier sentence would delete `ALLOWED`
# and red fifty-three files.
PACKAGES = [
    ('java.nio.file', DISK),
    ('java.nio.channels', DISK),
    ('java.io', DISK),
    ('java.util.zip', DISK),
    ('java.net', SOCKET),
    ('javax.net', SOCKET),
    # ⚠️ ADDED AFTER REVIEW REACHED THE FILESYSTEM THROUGH BOTH.
    # `java.util.logging.FileHandler` opens a file and `java.sql.DriverManager`
    # opens a connection, and neither was covered by the stated blind spots.
    ('java.util.logging', DISK),
    ('java.sql', SOCKET),
    # ⚠️ A SUBCLASS ESCAPES BY LIVING SOMEWHERE ELSE. `java.util.jar.JarFile`
    # EXTENDS `java.util.zip.ZipFile` from a package that was not listed, and
    # review MEASURED it opening a file and passing. Banning a package closes
    # naming a sibling INSIDE it; it does not close subclassing OUT of it.
    ('java.util.jar', DISK),
    ('java.util.prefs', DISK),
    # ⚠️ `javax.sql.DataSource` HANDS BACK A CONNECTION A `var` NEVER NAMES,
    # so the `java.sql` ban never fires -- measured. `javax.naming` is the
    # lookup that finds the DataSource.
    ('javax.sql', SOCKET),
    ('javax.naming', SOCKET),
]

# ⚠️ `java.time` IS NOT ON THAT LIST, on purpose: `Clock`, `Instant` and
# `Duration` are value types this project passes across seams all day, so the
# package cannot be forbidden and the clock has to be named by construct. One
# suffix shape closes the factory family -- `.now()` catches `Instant`,
# `LocalTime`, `Year`, `YearMonth` and anything `java.time` adds later, which
# naming five of the nine types did not.
#
# ⚠️ SO THE TWO HALVES OF THIS GATE ARE NOT EQUALLY COMPLETE, AND SAYING SO IS
# THE POINT. The I/O half is banned BY PACKAGE, so no sibling can be named
# INSIDE a listed package -- which is what ended rounds 1-3's sibling hunt.
# ⚠️ IT DOES NOT CLOSE SUBCLASSING *OUT* OF ONE, and an earlier draft claimed
# it did: review MEASURED `java.util.jar.JarFile`, which EXTENDS the banned
# `java.util.zip.ZipFile` from a package that was not listed, opening a file
# and passing. `java.util.jar` and `java.util.prefs` are listed now, and the
# honest rule is that each PACKAGE a reach can live in must be named.
# `java.io` and `java.util.zip` are bannable only because `ALLOWED` names the
# byte and checksum types that live in them. MEASURED: this tree's non-exempt
# `src/main` uses exactly eight `java.io` types and `CRC32C`, and all nine are
# listed; the remaining names are their obvious siblings, listed ahead of need
# so the first use of `Flushable` is not a spurious refusal. Getting this half
# wrong is what let `java.io.PrintStream` walk past a list that named
# `PrintWriter`.
#
# ⚠️ THE CLOCK AND SUBPROCESS HALF IS A HAND-NAMED LIST inside `java.time`,
# `java.util` and `java.lang`, packages that cannot be banned because `Clock`,
# `Instant`, `List`, `Map` and `Runtime` live there. It is exactly as complete
# as the enumeration below and no more -- review found `new GregorianCalendar()`
# beside the listed `new Date(`, then `now(ZoneOffset.UTC)` beside the listed
# no-argument `now()`, each after a previous round had gone looking. Expect
# more: this half raises the cost and does not close the family.
BANNED = [
    # ⚠️ THE NO-ARGUMENT FORM ONLY. `LocalDate.now(clock)` is the JDK's
    # prescribed way to READ an injected clock and must pass -- review
    # MEASURED the argument-blind version refusing it, which is a gate
    # refusing the shape rule 4 requires.
    ('.now()', CLOCK),
    # ⚠️ THE ZONE OVERLOADS SOURCE THE REAL CLOCK TOO. `LocalDateTime.now(
    # ZoneOffset.UTC)` reads the system clock in a named zone -- review
    # MEASURED it passing when only the no-argument form was listed. These two
    # and the no-argument form are the JDK's `now` overloads that do NOT take a
    # `Clock`; the one that does is the shape a seam is FOR.
    ('.now(...)', CLOCK),
    ('currentTimeMillis(', CLOCK),
    ('nanoTime(', CLOCK),
    ('Clock.system', CLOCK),
    ('Clock.tick', CLOCK),
    ('new Date(', CLOCK),
    ('new GregorianCalendar(', CLOCK),
    ('Calendar.getInstance(', CLOCK),
    ('new ProcessBuilder(', PROCESS),
    ('.exec(', PROCESS),
    ('.getResourceAsStream(', DISK),
    ('getSystemResourceAsStream(', DISK),
    ('.toURL(', SOCKET),
    # ⚠️ A URL CAN ARRIVE WITHOUT ITS PACKAGE BEING NAMED --
    # `var u = getClass().getResource("/x")` imports nothing -- so the two
    # opening calls stay as a backstop BEHIND the `java.net` package ban.
    # Review measured them being replaced by a narrower `.toURL(`, which let
    # exactly that shape through; non-negotiable 2 is why they came back.
    ('.openStream(', SOCKET),
    ('.openConnection(', SOCKET),
]

# ⚠️ VALUE TYPES INSIDE A BANNED PACKAGE, NAMED AND SHORT. `java.net.URI` is the
# natural type for an endpoint or an object key, `URLEncoder`/`URLDecoder` are
# pure string functions and `FileNotFoundException` is a type a caller catches --
# none of them reaches anything. Same argument that keeps `java.time` off the
# package list, and review measured the package ban refusing all four.
# ⚠️ THE OPENING CALLS ARE STILL BANNED, so allowing `URI` opens no route:
# `.toURL(`, `.openStream(`, `.openConnection(` and `.getResourceAsStream(` are
# entries in their own right.
# ⚠️ THE CARVE-OUT IS WHAT MAKES `java.io` AND `java.util.zip` BANNABLE AT ALL.
# Both packages hold the pure byte and checksum types this project moves
# segments with -- MEASURED: exactly the eight `java.io` types below and
# `CRC32C` appear in non-exempt `src/main`, and nothing else. Listing them is
# what lets the PACKAGE be forbidden, which is what stops `PrintStream`
# appearing beside `PrintWriter` the way review found it.
#
# ⚠️ MATCHED AS A PREFIX, not as a whole name, so `java.net.URISyntaxException`
# is admitted by `java.net.URI` and `java.io.IOException` covers nothing else.
# That is deliberate -- an exception type reaches nothing -- and it is said here
# rather than described as a whole-name match, which an earlier draft claimed.
#
ALLOWED = [
    'java.net.URI',
    'java.net.URLEncoder',
    'java.net.URLDecoder',
    'java.io.IOException',
    'java.io.UncheckedIOException',
    'java.io.FileNotFoundException',
    'java.io.InputStream',
    'java.io.OutputStream',
    'java.io.ByteArrayInputStream',
    'java.io.ByteArrayOutputStream',
    'java.io.FilterInputStream',
    'java.io.Closeable',
    'java.io.Flushable',
    'java.io.DataInputStream',
    'java.io.DataOutputStream',
    'java.io.Serializable',
    'java.util.zip.CRC32',
    'java.util.zip.CRC32C',
    'java.util.zip.Adler32',
    'java.util.zip.Checksum',
]

QUALIFIED = re.compile(r'[A-Za-z_$][\w$]*(?:\s*\.\s*[A-Za-z_$][\w$]*)*')


def _allowed_at(code, start):
    """Whether the qualified name beginning at `start` is an allowed value type.

    ⚠️ `java.net.URI` must be allowed while `java.net.URL` is not, and they
    share a prefix -- so the decision is made on the WHOLE dotted name at the
    match rather than on the package that matched.
    """
    m = QUALIFIED.match(code, start)
    if not m:
        return False
    return any(re.sub(r'\s+', '', m.group(0)).startswith(a) for a in ALLOWED)


def _package(name):
    """A qualified prefix as a regex, matching the import AND any qualified use.

    ⚠️ A PREFIX, DELIBERATELY AND IN BOTH DIRECTIONS. `java.io.File` is meant to
    take `FileInputStream`, `FileOutputStream`, `FileReader` and `FileWriter`
    with it, which is the whole point of banning a family rather than its
    members. The cost is that a hypothetical `java.network` would also match;
    no such JDK package exists, and the alternative -- a word boundary -- would
    break the `File*` family this exists to close.
    """
    return re.compile(r'\b' + re.escape(name).replace(r'\.', r'\s*\.\s*'))


# ⚠️ TWO ENTRIES THE LITERAL BUILDER CANNOT EXPRESS, written as regexes and
# kept in the same list so the coverage case still sees them. `now(ZoneId...)`
# takes a package qualifier the builder has no arm for.
EXPLICIT = {
    # ⚠️ EVERY `now(...)` EXCEPT ONE NAMING A CLOCK. Review MEASURED the
    # previous form -- which looked for a literal `Zone` token -- matching the
    # SAMPLE's spelling rather than the overload: `LocalDate.now(zone)` with a
    # variable passed straight through. Text cannot tell a `ZoneId` variable
    # from a `Clock` variable, so the rule is the project's own naming: an
    # argument whose text mentions `clock` is the injected seam, anything else
    # sources the real one.
    #
    # ⚠️ THAT IS A NAMING CONVENTION, NOT A TYPE CHECK, and it is the weakest
    # entry here. `now(c)` for an injected clock is refused and `now(clockwise)`
    # would be allowed. The alternative -- banning every `now(` -- refuses
    # `LocalDate.now(clock)`, which is the JDK's own way to READ the seam and
    # which review has already flagged once as the false refusal most likely to
    # get a gate edited rather than obeyed.
    '.now(...)': r'\b[A-Z][\w$]*\s*\.\s*now\s*\(\s*(?![^()]*[Cc][Ll][Oo][Cc][Kk])[^()]',
}


def _compile(lit):
    return re.compile(EXPLICIT[lit]) if lit in EXPLICIT else _shape(lit)


SHAPES = ([(_package(name), name, what) for name, what in PACKAGES]
          + [(_compile(lit), lit, what) for lit, what in BANNED])
# ⚠️ `ALLOWED` IS PART OF THE ENUMERATION, not a footnote to it. Review
# MEASURED that adding `java.io.File` to it let a real delete through with no
# case redding and the printed count unchanged -- a carve-out widened in
# silence, which is non-negotiable 2's direction.
BAN_ENTRIES = [name for name, _ in PACKAGES] + [lit for lit, _ in BANNED]
ALL_ENTRIES = BAN_ENTRIES + ['allow:' + a for a in ALLOWED]
PACKAGE_NAMES = {name for name, _ in PACKAGES}


def _blank(text):
    """The same text with every non-newline character replaced by a space."""
    return re.sub(r'[^\n]', ' ', text)


def code_only(src):
    """The source with comments and literals blanked, offsets preserved.

    One left-to-right pass. Offsets are preserved rather than the text
    shortened, so a match's position still maps to its real line.
    """
    out = []
    i, n = 0, len(src)
    while i < n:
        if src.startswith('//', i):
            j = src.find('\n', i)
            j = n if j < 0 else j
        elif src.startswith('/*', i):
            j = src.find('*/', i + 2)
            j = n if j < 0 else j + 2
        elif src.startswith('"""', i):
            # ⚠️ ESCAPES ARE HONOURED HERE TOO, and review MEASURED what their
            # absence cost: a `\\"""` inside a text block made `find` land on
            # the escaped delimiter, the REAL closing delimiter then read as a
            # new opener, and everything to end of file was blanked -- a
            # `Files.readString` after it reported clean.
            j = i + 3
            while j < n and not src.startswith('"""', j):
                j += 2 if src[j] == '\\' else 1
            j = min(j + 3, n) if j < n else n
        elif src[i] in '"\'':
            quote, j = src[i], i + 1
            # ⚠️ A NEWLINE ENDS IT. Java forbids a raw newline in a string or
            # char literal, so stopping here keeps ONE unterminated literal in
            # malformed source from blanking the rest of the file.
            while j < n and src[j] != quote and src[j] != '\n':
                j += 2 if src[j] == '\\' else 1
            j = min(j + 1, n) if j < n and src[j] == quote else j
        else:
            out.append(src[i])
            i += 1
            continue
        out.append(_blank(src[i:j]))
        i = j
    return ''.join(out)


def main():
    paths = [p.strip() for p in sys.stdin if p.strip()]
    violations = []
    for path in paths:
        try:
            src = io.open(path, encoding='utf-8').read()
        except OSError as e:
            violations.append('%s: unreadable (%s)' % (path, e))
            continue
        code = code_only(src)
        for shape, literal, what in SHAPES:
            for m in shape.finditer(code):
                if literal in PACKAGE_NAMES and _allowed_at(code, m.start()):
                    continue
                line = code.count('\n', 0, m.start()) + 1
                violations.append('%s:%d: %s reaches %s directly'
                                  % (path, line, literal, what))
    violations.sort()
    print(len(paths))
    for v in violations:
        print(v)
    return 0


if __name__ == '__main__':
    sys.exit(main())
