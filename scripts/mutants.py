# SPDX-License-Identifier: Apache-2.0
"""tdd/SKILL.md, M9 criterion 1: 80% of the mutants on the changed lines killed.

Reads jzap's native JSON reporter -- `<module>/build/reports/jzap-diff/jzap-result.json`
-- for each module named on the command line, and scores them together. The
format is jzap 0.1.1's, read off a real `./gradlew :format:mutationTestDiff`
run rather than guessed:

    {"engine": "schemata", "scope": "...", "testsDiscovered": 390,
     "mutationScore": 100.0, "testStrength": 100.0,
     "scoredMutants": 3, "unscoredMutants": 0, "coveredMutants": 3,
     "detectedMutants": 3, "failingBaselineTests": [],
     "mutants": [{"key": "io.github.huyz0.os.biningester.format.RunCommit::lastOffset()J::33::MATH#0",
                  "class": ..., "method": ..., "line": 33, "mutator": "MATH",
                  "ordinal": 0, "sourceFile": ..., "description": ...,
                  "status": "KILLED", "killingTest": ..., "coveringTests": 1,
                  "testsRun": 1}],
     "timings": {...}}

⚠️ The SCORE IS RECOMPUTED from `mutants`, never taken from `mutationScore`.
The report's own number knows nothing about `baselines/mutants.txt`, and two
modules' reports cannot be averaged by taking the mean of two percentages.
jzap's own arithmetic is reproduced exactly (`AnalysisResult.mutationScore`):
detected = KILLED + TIMED_OUT, scored = everything except NON_VIABLE and
RUN_ERROR, score = 100 * detected / scored.

⚠️ Written against the lesson of this repository's own review history: the
characteristic failure of a threshold gate is not a wrong threshold, it is
reporting success while measuring nothing. So:

* a module whose production sources changed and that produced no report is a
  FAILURE, not a skip;
* a report that cannot be parsed is a FAILURE, never an empty one;
* "the changed lines carry no mutable code" is reported NOT-MEASURED, never ok;
* every survivor is NAMED by key, because a survivor nobody can name is a
  survivor nobody can kill.
"""
import json
import os
import re
import subprocess
import sys

RED, GREEN, YEL = '\033[31m', '\033[32m', '\033[33m'
OFF = '\033[0m'
FLOOR = 80.0
BASELINE = 'baselines/mutants.txt'

DETECTED = {'KILLED', 'TIMED_OUT'}
UNSCORABLE = {'NON_VIABLE', 'RUN_ERROR'}
REPORT = '%s/build/reports/jzap-diff/jzap-result.json'


def baselined():
    """({mutant key: reason}, [(lineno, key)]) -- named AND justified.

    ⚠️ A bare key is REFUSED, not accepted with an empty reason -- the same
    arity rule baselines/coverage.txt carries, for the same reason: an
    exemption that names nothing can never be shown to be finished, so it is a
    permanent exemption wearing a ratchet's clothes (testing.md rule 7).
    """
    out, malformed = {}, []
    if os.path.exists(BASELINE):
        with open(BASELINE) as f:
            for n, line in enumerate(f, 1):
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                parts = line.split(None, 1)
                if len(parts) < 2:
                    malformed.append((n, parts[0]))
                    continue
                out[parts[0]] = parts[1]
    return out, malformed


def read(path):
    """The mutant list, or None if the report cannot be read.

    ⚠️ None and [] are DIFFERENT FACTS and the caller must keep them apart. A
    truncated file -- a killed build, a full disk -- scored as "no mutants"
    would turn a broken measurement into a passing gate.
    """
    try:
        with open(path) as f:
            doc = json.load(f)
    except (ValueError, OSError):
        return None
    mutants = doc.get('mutants')
    return mutants if isinstance(mutants, list) else None


def diff_hunks():
    """{source path: [(old start, old count, new count)]} for this change.

    The same scope `changed_files` uses: CHECK_RANGE..HEAD when set, else the
    index. None if git cannot answer -- the caller then keeps refusing.
    """
    rng = os.environ.get('CHECK_RANGE')
    # ⚠️ THE FORMAT IS PINNED, not left to the user's git config:
    # diff.mnemonicPrefix prints `+++ i/`, diff.noprefix prints no prefix, and
    # color or an external diff change every line -- each of which would find
    # no path and quietly bring the false refusal back.
    cmd = ['git', 'diff', '-U0', '--no-renames', '--no-color', '--no-ext-diff',
           '--src-prefix=a/', '--dst-prefix=b/']
    cmd += [rng, 'HEAD'] if rng else ['--cached']
    try:
        out = subprocess.run(cmd + ['--', '*/src/main/*.java'], capture_output=True,
                             text=True, check=True).stdout
    except (OSError, subprocess.CalledProcessError):
        return None
    hunks, path = {}, None
    for line in out.splitlines():
        if line.startswith('+++ '):
            path = line[6:] if line.startswith('+++ b/') else None
            if path is not None:
                hunks.setdefault(path, [])
        elif line.startswith('@@ ') and path is not None:
            m = re.match(r'@@ -(\d+)(?:,(\d+))? \+\d+(?:,(\d+))? @@', line)
            if m:
                hunks[path].append((int(m.group(1)),
                                    int(m.group(2) if m.group(2) is not None else 1),
                                    int(m.group(3) if m.group(3) is not None else 1)))
    return hunks


def line_moved(key):
    """Whether this change touched or moved the baselined key's line.

    ⚠️ True whenever it cannot be shown otherwise -- no diff, no source file for
    the class, a key that does not parse -- so a doubt keeps the refusal.
    """
    try:
        method_key, line, _ = key.rsplit('::', 2)
        cls, line = method_key.split('::', 1)[0], int(line)
    except ValueError:
        return True
    hunks = diff_hunks()
    if hunks is None:
        return True
    source = '/src/main/java/' + cls.split('$', 1)[0].replace('.', '/') + '.java'
    files = [f for f in hunks if f.endswith(source)]
    if len(files) != 1:
        return True
    shift = 0
    for start, removed, added in hunks[files[0]]:
        if removed and start <= line < start + removed:
            return True                           # the line itself changed
        if (removed and start + removed - 1 < line) or (not removed and start < line):
            shift += added - removed              # lines added or removed above it
    return shift != 0


def main(modules):
    skips, malformed = baselined()
    for n, key in malformed:
        print('  %sFAIL%s %s:%d  "%s" is baselined with no reason'
              % (RED, OFF, BASELINE, n, key))
    if malformed:
        print('         An entry says WHAT UNBLOCKS IT -- the test that would kill')
        print('         the mutant, or the backlog row that writes it. A baseline')
        print('         that can swallow a survivor silently defeats the gate.')
        return 1

    failed = False
    mutants = []
    for m in modules:
        # ⚠️ A filesystem read, and it must be: build output is untracked by
        # definition, so git cannot answer it. Bounded to a module directory
        # the caller derived from git, so it cannot wander into a sibling
        # checkout -- and it is an exists()/open(), never a walk.
        path = REPORT % m
        if not os.path.exists(path):
            print('  %sFAIL%s %-20s changed, but there is no jzap report at' % (RED, OFF, m))
            print('         %s' % path)
            print('         Run ./gradlew :%s:mutationTestDiff. A gate that skips an' % m)
            print('         unmeasured module reports success for exactly the code')
            print('         whose tests were never shown to constrain anything.')
            failed = True
            continue
        got = read(path)
        if got is None:
            print('  %sFAIL%s %-20s report is unreadable (truncated or malformed JSON):'
                  % (RED, OFF, m))
            print('         %s' % path)
            print('         A report that cannot be parsed is a measurement that did')
            print('         not happen, not a diff with nothing to mutate.')
            failed = True
            continue
        mutants.extend(got)
    if failed:
        return 1

    scored, detected, survivors, excused = 0, 0, [], []
    stale, unused = [], []
    # ⚠️ Keyed by `class::method`, which is what makes an UNUSED entry visible
    # on a DIFF-scoped run. A baseline key carries a line number and a mutator
    # ordinal, so editing the method moves every key in it -- and the old entry
    # would then match nothing and be silently kept, which is a standing excuse
    # for whatever mutant lands on that key next. Seeing the method but not the
    # key is exactly that state.
    #
    # ⚠️ BUT A MUTATED METHOD IS NOT PROOF THE KEY MOVED (M10.7, harvested
    # from 5f7e242's review). A diff-scoped run mutates only the CHANGED lines, so an edit to
    # another line of the method yields mutants in it without ever examining
    # the baselined line -- and refusing that entry refused a correct excuse.
    # The diff decides: the entry is UNUSED only when its line was itself
    # changed, or moved because lines were added or removed above it.
    methods = {mu.get('key', '?').rsplit('::', 2)[0] for mu in mutants}
    present = {mu.get('key', '?') for mu in mutants}
    for key, reason in skips.items():
        if key not in present and key.rsplit('::', 2)[0] in methods \
                and line_moved(key):
            unused.append(key)
    for mu in mutants:
        key, status = mu.get('key', '?'), mu.get('status', '?')
        if key in skips:
            if status in DETECTED:
                stale.append(key)
            else:
                excused.append((key, skips[key]))
            continue                              # outside BOTH halves
        if status in UNSCORABLE:
            continue                              # jzap's MutantStatus.isScored()
        scored += 1
        if status in DETECTED:
            detected += 1
        else:
            survivors.append((key, status, mu.get('description', '')))

    for key, reason in excused:
        print('  %sWARN%s BASELINED %s' % (YEL, OFF, key))
        print('         %s' % reason)

    # ⚠️ The ratchet's teeth, and the half that makes the baseline safe. An
    # entry whose mutant is dead describes nothing, and left in place it
    # swallows the NEXT survivor to land on that key without anyone deciding
    # to swallow it. So a dead entry FAILS even at a perfect score.
    for key in sorted(stale):
        print('  %sFAIL%s STALE %s is killed now' % (RED, OFF, key))
        print('         Remove its line from %s. A recorded survivor that is no' % BASELINE)
        print('         longer surviving is a hole waiting for the next one.')
    # ⚠️ The other way an entry stops describing anything: its method was
    # mutated this run and produced no mutant with that key, because the method
    # was edited and every line and ordinal in it moved. The entry survives as
    # a standing excuse for a key nobody chose to excuse.
    for key in sorted(unused):
        print('  %sFAIL%s UNUSED %s no longer exists' % (RED, OFF, key))
        print('         %s was mutated this run and produced no such mutant.'
              % key.rsplit('::', 2)[0])
        print('         Re-record the key jzap emits now, or remove the line from')
        print('         %s. An excuse for a mutant that is gone is an' % BASELINE)
        print('         excuse waiting for whatever lands on that key next.')
    if stale or unused:
        return 1

    if scored == 0:
        # Never print ok here. "Nothing to measure" and "everything passed" are
        # different facts, and jzap scores an empty mutant list as 0.0% -- which
        # would refuse a javadoc edit, an added import or a field rename until
        # somebody switched the gate off.
        #
        # ⚠️ This is the gate's stated blind spot: a change that touches
        # production Java but no mutable line is unmeasured, and so is one where
        # every mutant is baselined or unscorable. The word says so.
        print('  %sNOT-MEASURED%s the changed lines produced no scorable mutant'
              % (YEL, OFF))
        print('         (no mutable code on them, or every mutant baselined or')
        print('         unscorable) -- mutation unenforced for this change.')
        return 0

    score = 100.0 * detected / scored
    bad = score < FLOOR
    # ⚠️ EVERY SURVIVOR IS NAMED, WHATEVER THE SCORE. Naming them only below
    # the floor makes 85% with two survivors a silent pass: the two changes no
    # test noticed are the whole output of this gate, and a percentage that
    # clears a threshold is not a substitute for them. The floor decides the
    # EXIT STATUS; the list is the finding either way.
    for key, status, desc in survivors:
        print('  %s%s%s %-11s %s'
              % (RED + 'FAIL' if bad else YEL + 'WARN', '', OFF, status, key))
        print('         %s' % desc)
    print('  %s%s%s %d of %d scorable mutant(s) killed: %.1f%% (floor %.0f)'
          % (RED + 'FAIL' if bad else GREEN + 'ok  ', '', OFF,
             detected, scored, score, FLOOR))
    if not bad:
        if survivors:
            print('         ⚠️ The %d line(s) above are changes to the code that NO'
                  % len(survivors))
            print('         TEST NOTICED. The score clears the floor; they are still')
            print('         the finding, and non-negotiable 2 forbids moving the floor')
            print('         to keep clearing it.')
        return 0
    print('         Each line above is a change to the code that NO TEST NOTICED.')
    print('         Write the test that kills it. If it cannot be killed -- an')
    print('         equivalent mutant -- record the key in %s' % BASELINE)
    print('         with the reason, and non-negotiable 2 forbids moving this floor.')
    return 1


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
