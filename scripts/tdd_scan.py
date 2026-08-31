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
from java_tests import test_ids, UnparseableJava   # one parser, shared with check-test-integrity


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

    paths = [p for p in names.split() if p.endswith('.java') and TEST_PATH.search(p)]
    new_ids, blob_sha = set(), {}
    for p in paths:
        after = git('show', after_ref + p)
        if after is None:
            continue                                   # deleted
        before = git('show', before_ref + p) or ''     # absent => new file
        try:
            fresh = test_ids(after, p) - test_ids(before, p)
        except UnparseableJava as e:
            print('  %sFAIL%s %s' % (RED, OFF, e))
            print('         Refusing to certify a file the parser cannot read.')
            return 1
        for i in fresh:
            new_ids.add(i)
            blob_sha[i] = hashlib.sha256(after.encode()).hexdigest()

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


def plan(ids):
    """`<gradle-task> <gradle-selector>` per id, on stdout."""
    out, bad = [], []
    for i in ids:
        src = source_of(i)
        if src is None:
            bad.append(i)
            continue
        m = re.search(r'/src/([^/]+)/java/', src)
        task = SOURCE_SET_TASK.get(m.group(1) if m else '', None)
        # buildSrc is a separate build: `./gradlew test` never descends into it,
        # so a red record for a harness test would otherwise be unobtainable.
        if task and src.startswith('buildSrc/'):
            task = 'buildSrc:' + task
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
    import glob as _glob, hashlib as _h, json as _j, time as _t
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
    path = os.path.join(out_dir, 'red.json')
    rec = _j.load(open(path)) if os.path.exists(path) else {"red": {}}
    rec["red"][ident] = {"at": int(_t.time()), "source": src,
                         "sha256": _h.sha256(open(src, 'rb').read()).hexdigest()}
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
    if cmd == 'ids':
        for i in sorted(test_ids(open(sys.argv[2]).read())):
            print(i)
        sys.exit(0)
    if cmd == 'plan':
        sys.exit(plan(sys.argv[2:]))
    if cmd == 'record-one':
        sys.exit(record_one(sys.argv[2], sys.argv[3]))
    sys.exit(check(sys.argv[2] if len(sys.argv) > 2 else ''))
