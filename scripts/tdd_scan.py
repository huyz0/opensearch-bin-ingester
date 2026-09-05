# SPDX-License-Identifier: Apache-2.0
"""Find the test methods in a Java source, and check new ones have a red record.

Kept out of check-tdd.sh because the parsing is the whole substance of the gate,
and a heredoc is a bad place for the only thing standing between the project and
tests written after the code.

    tdd_scan.py ids   <file.java>       -- print every test id in a file
    tdd_scan.py check [<base-ref>]      -- gate the staged diff (or a CI range)
"""
import hashlib, json, os, re, subprocess, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

RED, GREEN = '\033[31m', '\033[32m'
OFF = '\033[0m'

# JUnit 5 has five test-bearing annotations. The old gate matched the literal
# string "@Test", so @ParameterizedTest and @RepeatedTest were invisible.
from java_tests import test_ids, scan_raw_spans, hashable, UnparseableJava, package_of, rekey  # one parser, shared with check-test-integrity
from git_renames import rename_map  # one rename map, shared with check-test-integrity


def key_parts(src, ident):
    """The two halves a red record is built from, or None if `ident` is absent.

    ⚠️ ONE FUNCTION FOR BOTH the key and the `key-parts` seam. An earlier
    version had the seam RECOMPUTE the halves, which made the test assert that
    the seam was right and nothing about what `binding_key` used -- so dropping
    `hashable` from the declaration call site survived the whole suite. A seam
    that duplicates the logic it exposes is not a seam.

    ⚠️ The declaration is one half; the remainder -- the file with every test
    declaration cut out -- is the other. They are independent paths, and a
    mutation that transforms one differently is invisible to any comparison of
    whole keys.
    """
    spans = scan_raw_spans(src)
    if ident not in spans:
        return None
    ordered = sorted(spans.values())
    remainder, at = [], 0
    for lo, hi in ordered:
        remainder.append(src[at:lo])
        at = hi
    remainder.append(src[at:])
    # ⚠️ EMPTY PIECES DROPPED, or the separator count leaks the number of tests:
    # cutting one more span adds one more (empty) piece, so appending a test
    # would stale every record -- the churn this replaces, surviving as a
    # delimiter.
    joined = '\n'.join(p for p in (hashable(p) for p in remainder) if p)
    lo, hi = spans[ident]
    return hashable(src[lo:hi]), joined


def binding_key(src, ident):
    """What a red record for `ident` is bound to, or None if it is not there.

    ⚠️ THE TEST'S OWN DECLARATION PLUS THE FILE'S NON-TEST REMAINDER (M0.56).
    Not the whole file, which invalidated every record in it whenever any one
    test was added or edited. Not the body alone either, which would be cheaper
    and would be a real weakening: a shared helper, field, import or fixture in
    the same file is part of what the test was observed doing.

    ⚠️ Both halves go through `hashable`, which drops comments, collapses
    whitespace between tokens and keeps every literal verbatim. See `key_parts`.
    """
    parts = key_parts(src, ident)
    if parts is None:
        return None
    h = hashlib.sha256()
    h.update(parts[0].encode())
    h.update(b'\x00')
    h.update(parts[1].encode())
    return h.hexdigest()


def git(*a):
    r = subprocess.run(['git'] + list(a), capture_output=True, text=True)
    return r.stdout if r.returncode == 0 else None


# A test source is anything under a source set named for a test tier. The old
# gate matched only *Test.java / *Tests.java, a convention no standard states,
# so a class named anything else was silently exempt.
TEST_PATH = re.compile(r'/src/(?:test|integrationTest|clusterTest)/java/')


def check(base):
    if base:
        names = git('diff', '--name-only', base, 'HEAD') or ''
        after_ref, before_ref = 'HEAD:', base + ':'
    else:
        names = git('diff', '--cached', '--name-only') or ''
        after_ref, before_ref = ':', 'HEAD:'

    # ⚠️ ONE PREDICATE, asked of every path AND of every rename source below.
    reads = lambda q: q.endswith('.java') and TEST_PATH.search(q)
    paths = [p for p in names.split() if reads(p)]
    # ⚠️ M0.53. Without the rename map a MOVED test reads as new, because
    # `<before_ref>:<destination>` is absent -- so a file moved between source
    # sets demanded a fresh red record for every test in it.
    renamed = rename_map(base)
    # ⚠️ NO `sources` FILTER HERE, and the asymmetry with `test_integrity` is
    # deliberate rather than an omission: when `diff.renames` is off and the
    # source is listed too, the loop below already drops it, because a rename's
    # source does not exist at `after_ref` and `after is None` continues.
    # `test_integrity` has no such early return, so it needs the filter and
    # this does not. Round-2 review measured the copy here as unfalsifiable.
    new_ids, blob_sha = set(), {}
    for p in paths:
        after = git('show', after_ref + p)
        if after is None:
            continue                                   # deleted
        # ⚠️ ONLY A SOURCE THIS GATE WOULD ITSELF HAVE SCANNED may supply the
        # `before`. Following a rename from anywhere subtracts that file's ids
        # from `fresh`, so an unguarded `origin` is a two-commit, green-tree
        # bypass of non-negotiable 3: add `docs/drafts/FooTest.java`, which no
        # gate reads, then `git mv` it into the test tier. Measured against
        # HEAD, which refuses it -- so the guard is what keeps this change from
        # being WEAKER than the gate it fixes. `integrationTest -> test` still
        # follows, because that source IS one this gate scans.
        origin = renamed.get(p, p)
        if not reads(origin):
            origin = p
        before = git('show', before_ref + origin) or ''   # absent => new file
        try:
            was = test_ids(before, p)
            if origin != p:
                # ⚠️ A module move changes the package, so the before-ids must
                # be re-qualified or every moved test reads as new and is told
                # to earn a red record it already has.
                was = {rekey(i, package_of(before), package_of(after)) for i in was}
            fresh = test_ids(after, p) - was
        except UnparseableJava as e:
            print('  %sFAIL%s %s' % (RED, OFF, e))
            print('         Refusing to certify a file the parser cannot read.')
            return 1
        for i in fresh:
            new_ids.add(i)
            blob_sha[i] = binding_key(after, i)
            if blob_sha[i] is None:
                # ⚠️ Both SIDES, not just the writer. `stale` below compares
                # `rec[i].get('sha256') == blob_sha[i]`, and None == None
                # passes -- so a null here is the vacuous acceptance the
                # comment there says is refused.
                print('  %sFAIL%s %s is staged but invisible to the parser'
                      % (RED, OFF, i))
                return 1

    if not new_ids:
        print('  %sok%s   no new tests in this diff' % (GREEN, OFF))
        return 0

    rec_path = '.harness/tdd/red.json'
    if not os.path.exists(rec_path):
        print('  %sFAIL%s %d new test(s) staged but no red record at %s'
              % (RED, OFF, len(new_ids), rec_path))
        for i in sorted(new_ids):
            print('           ' + i)
        print('         run: scripts/tdd-red.sh <ids...>   BEFORE the production code')
        return 1

    rec = json.load(open(rec_path)).get('red', {})
    missing = [i for i in sorted(new_ids) if i not in rec]
    # Each record is bound to the bytes it was observed in. Editing the test
    # after watching it fail means the thing observed failing is not the thing
    # being committed.
    # Every record must be a dict carrying the sha256 of the file the test was
    # observed failing in. A record of any other shape -- an int, a string, a
    # dict with sha256 null -- is a hand-written one, and is refused rather than
    # tolerated: the tolerance WAS the bypass.
    stale = [i for i in sorted(new_ids)
             if i in rec and not (isinstance(rec[i], dict)
                                  and rec[i].get('sha256') == blob_sha[i])]

    if missing:
        print('  %sFAIL%s these new tests were never observed to fail:' % (RED, OFF))
        for i in missing:
            print('           ' + i)
        print('         A test written after the code, or never run red, constrains nothing.')
    if stale:
        print('  %sFAIL%s these tests changed after their red run:' % (RED, OFF))
        for i in stale:
            print('           ' + i)
        print('         Re-run scripts/tdd-red.sh -- what was observed failing is not')
        print('         what is being committed.')
    if missing or stale:
        return 1
    print('  %sok%s   %d new test(s), all observed failing first, unchanged since'
          % (GREEN, OFF, len(new_ids)))
    return 0


SOURCE_SET_TASK = {'test': 'test', 'integrationTest': 'integrationTest',
                   'clusterTest': 'clusterTest'}


def source_of(fq_id):
    """The file a test id lives in, or None.

    Outer$Inner lives in Outer.java: the nested part is not a path component.
    Getting this wrong made the sha256 binding a no-op for every @Nested test.
    """
    import glob as _glob
    fq_class = fq_id.split('#')[0].split('$')[0]
    rel = fq_class.replace('.', '/') + '.java'
    for cand in _glob.glob('*/src/*/java/' + rel):
        return cand
    return None


def task_for(src):
    """The Gradle task that runs a test source, or None. PURE -- no filesystem.

    ⚠️ Extracted from plan() because the rule is a string transform reachable
    only through source_of(), which globs the live working tree. That made the
    rule untestable without first committing a .java file into a module that had
    none -- and the first attempt at a test did exactly that badly: it named a
    class that was not in the tree, so plan() took its `bad` path and the test
    asserted against an ERROR message. Both the pre-fix and post-fix versions
    printed it, so the red record proved nothing and reverting the fix survived.
    Testing.md rule 1, non-negotiable 7: if it needs I/O to test, it is in the
    wrong layer.
    """
    m = re.search(r'/src/([^/]+)/java/', src)
    task = SOURCE_SET_TASK.get(m.group(1) if m else '', None)
    if task is None:
        return None
    # buildSrc is a separate build: `./gradlew test` never descends into it, so
    # a red record for a harness test would otherwise be unobtainable.
    if src.startswith('buildSrc/'):
        return 'buildSrc:' + task
    # ⚠️ SCOPED TO THE OWNING MODULE. This emitted the bare root task, and
    # `./gradlew test --tests <one class>` runs `test` in all eight modules at
    # org.gradle.parallel=true.
    #
    # ⚠️ A module with NO test source is NO-SOURCE and is skipped, not failed --
    # an earlier version of this comment said the seven siblings "fail on no
    # tests found", and review disproved it by running the command. What does
    # fail is a module that HAS test sources and no match, and whether the
    # owning module wrote its JUnit XML before the build died is then a
    # SCHEDULING RACE. So the defect is unreliability, not impossibility:
    # tdd-red.sh recorded 2 of 7 M1.0 ids, then 7 of 7 once scoped. Scoping
    # turns the race into a certainty, and matters more with every module that
    # gains tests. Invisible until now because buildSrc, the only place with
    # tests, is special-cased just above.
    return ':%s:%s' % (src.split('/', 1)[0], task)


def plan(ids):
    """`<gradle-task> <gradle-selector>` per id, on stdout."""
    out, bad = [], []
    for i in ids:
        src = source_of(i)
        task = task_for(src) if src is not None else None
        if task is None:
            bad.append(i)
            continue
        # Gradle's selector syntax is Class.method; ours is Class#method.
        out.append('%s %s' % (task, i.replace('#', '.')))
    if bad:
        print('  %sFAIL%s cannot locate a test source for:' % (RED, OFF))
        for i in bad:
            print('           ' + i)
        print('         Use fully-qualified ids: binjava.format.SegmentTest#roundTrips')
        return 1
    print('\n'.join(out))
    return 0


def record_one(out_dir, ident):
    """Record ONE id, from the results of a run that selected only that id.

    ⚠️ A @ParameterizedTest invocation is written to the JUnit XML as
    `name="[1] arg, arg"` -- the method name is not there at all. So an id can
    never be recovered from the XML by name, and matching by name alone made
    every parameterized test impossible to red, while check-tdd still demanded a
    record for one. The runner therefore invokes Gradle once per selector, and
    every testcase in the results belongs to that selector by construction.
    """
    import glob as _glob, json as _j, time as _t
    import xml.etree.ElementTree as ET

    cls = ident.split('#')[0]
    seen = failed = 0
    for xml in _glob.glob('*/build/test-results/*/TEST-*.xml'):
        try:
            root = ET.parse(xml).getroot()
        except ET.ParseError:
            continue
        for case in root.iter('testcase'):
            if case.get('classname', '') != cls:
                continue
            seen += 1
            if case.find('failure') is not None or case.find('error') is not None:
                failed += 1

    if seen == 0:
        print('  %sFAIL%s %s did not run -- no testcase for %s in the results.'
              % (RED, OFF, ident, cls))
        print('         An unmatched selector is not a red test. Check the id.')
        return 1
    if failed == 0:
        print('  %sFAIL%s %s PASSED (%d case(s)). A test that passes before the'
              % (RED, OFF, ident, seen))
        print('         production code exists is not testing the change.')
        return 1

    src = source_of(ident)
    if src is None:
        print('  %sFAIL%s no source for %s -- a record not bound to the bytes'
              % (RED, OFF, ident))
        print('         observed failing is not evidence.')
        return 1
    key = binding_key(open(src).read(), ident)
    if key is None:
        # ⚠️ REFUSED, not stored. `check`'s own comment says a record with a
        # null sha256 is refused because "the tolerance WAS the bypass" -- and
        # its staleness test compares `rec[i].get('sha256') == blob_sha[i]`,
        # where None == None passes. Writing a null here would hand that
        # comparison the vacuous pass it was written to prevent. Reachable via
        # the M0.17 composed-annotation blind spot, where the parser does not
        # see the test at all.
        print('  %sFAIL%s %s is not visible to the parser in %s'
              % (RED, OFF, ident, src))
        print('         A record not bound to bytes is not evidence (M0.17).')
        return 1
    path = os.path.join(out_dir, 'red.json')
    rec = _j.load(open(path)) if os.path.exists(path) else {"red": {}}
    rec["red"][ident] = {"at": int(_t.time()), "source": src,
                         "sha256": key}
    os.makedirs(out_dir, exist_ok=True)
    _j.dump(rec, open(path, 'w'), indent=2, sort_keys=True)
    print('  %sok%s   %s observed failing (%d of %d case(s))'
          % (GREEN, OFF, ident, failed, seen))
    return 0


if __name__ == '__main__':
    cmd = sys.argv[1] if len(sys.argv) > 1 else 'check'
    if cmd == 'scan':
        # `<id> DISABLED|enabled` per line -- the disabled flag is what
        # check-test-integrity uses to see a neutered test, so it needs a way to
        # be asserted from outside.
        from java_tests import scan_checked
        for k, v in sorted(scan_checked(open(sys.argv[2]).read(), sys.argv[2]).items()):
            # The body length is printed because check-test-integrity's
            # strength() scores the BODY: without an observable seam, replacing
            # it with '' blinds that gate while every parser test stays green.
            print('%s %s body=%d'
                  % (k, 'DISABLED' if v['disabled'] else 'enabled', len(v['body'])))
        sys.exit(0)
    if cmd == 'key':
        # ⚠️ A seam for testing the binding as the pure transform it is, over
        # source TEXT. The rule it encodes -- what makes an observation stale --
        # is otherwise reachable only through a staged git diff.
        k = binding_key(open(sys.argv[2]).read(), sys.argv[3])
        if k is None:
            print('no such test: ' + sys.argv[3])
            sys.exit(1)
        print(k)
        sys.exit(0)
    if cmd == 'key-parts':
        # ⚠️ THE HALVES AS BYTES, base64'd, not as digests of them.
        #
        # Printing digests made this a CONSISTENCY seam: a test could only ask
        # production whether it agreed with itself, and such a test is blind to
        # any transform applied UNIFORMLY to both sides -- which is exactly the
        # defect class five review rounds kept re-finding. Blanking text blocks
        # in `binding_key` after `key_parts` returned, or hashing the raw
        # declaration there, both survived a whole suite.
        #
        # ⚠️ With the halves as bytes a test can compute the key ITSELF --
        # sha256(decl + NUL + remainder) -- and compare. That turns "does
        # production agree with production" into an independent check, and it
        # is the only way anything between `key_parts` and `hexdigest()` is
        # constrained at all.
        parts = key_parts(open(sys.argv[2]).read(), sys.argv[3])
        if parts is None:
            print('no such test: ' + sys.argv[3])
            sys.exit(1)
        import base64
        print('DECL %s' % base64.b64encode(parts[0].encode()).decode())
        print('REM %s' % base64.b64encode(parts[1].encode()).decode())
        sys.exit(0)
    if cmd == 'hashable':
        # ⚠️ A seam for asserting the transform ITSELF. Four review rounds each
        # found one more of its behaviours unconstrained, and the cause was
        # structural: every claim about it was made by comparing two 64-hex
        # digests, and a digest says only SAME or DIFFERENT, never WHY. So each
        # behaviour cost a fixture file and the set was always one short.
        print(hashable(open(sys.argv[2]).read()), end='')
        sys.exit(0)
    if cmd == 'spans':
        # ⚠️ A seam for asserting that the two normalisations AGREE. The whole
        # binding rests on `normalise_lp` seeing exactly the tests `normalise`
        # does; where they disagree a span slices the wrong bytes and a record
        # is bound to something other than the test -- a false green, not churn.
        src = open(sys.argv[2]).read()
        for k, (lo, hi) in sorted(scan_raw_spans(src).items()):
            print('%s %d %d %s' % (k, lo, hi, 'OK' if src[lo] == '@' and src[hi - 1] == '}'
                                   else 'MALFORMED'))
        sys.exit(0)
    if cmd == 'ids':
        for i in sorted(test_ids(open(sys.argv[2]).read())):
            print(i)
        sys.exit(0)
    if cmd == 'task-for':
        # ⚠️ A seam for testing task_for as the pure transform it is, over path
        # STRINGS -- including paths no on-disk tree can express. Prints the
        # task, or 'none' when the source set is not a test one.
        for src in sys.argv[2:]:
            print(task_for(src) or 'none')
        sys.exit(0)
    if cmd == 'plan':
        sys.exit(plan(sys.argv[2:]))
    if cmd == 'record-one':
        sys.exit(record_one(sys.argv[2], sys.argv[3]))
    sys.exit(check(sys.argv[2] if len(sys.argv) > 2 else ''))
