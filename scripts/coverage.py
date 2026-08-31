# SPDX-License-Identifier: Apache-2.0
"""testing.md rule 6: 95% line / 90% branch, per module, from JaCoCo's XML.

⚠️ Written against the lesson of this repository's own review history: the
characteristic failure of a threshold gate is not a wrong threshold, it is
reporting success while measuring nothing. So:

* a module with classes but no report is a FAILURE, not a skip;
* "no modules had any classes" is reported as NOT-MEASURED, never as ok;
* the exit status is returned, not discarded by a shell `finish`.
"""
import glob, os, re, subprocess, sys
import xml.etree.ElementTree as ET

RED, GREEN, YEL = '\033[31m', '\033[32m', '\033[33m'
OFF = '\033[0m'
LINE_FLOOR, BRANCH_FLOOR = 95.0, 90.0
BASELINE = 'baselines/coverage.txt'


def excluded():
    """({module: reason}, [(lineno, module)]) -- named AND justified (rule 7).

    ⚠️ A bare module name is REFUSED, not accepted with an empty reason. This
    gate printed "each entry names what unblocks it" while `format` alone
    parsed fine and rendered as "EXCLUDED:" with nothing after the colon -- a
    property asserted in output and never checked, which is the exact defect
    the gate exists to refuse. Stating it and enforcing it are different acts.
    """
    out, malformed = {}, []
    if os.path.exists(BASELINE):
        for n, line in enumerate(open(BASELINE), 1):
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            parts = line.split(None, 1)
            # ⚠️ Arity alone. An `or not parts[1].strip()` clause stood here
            # and was UNREACHABLE: `line` is stripped above, and split(None, 1)
            # on a stripped non-empty line always leaves parts[1] starting with
            # a non-whitespace character. It survived every mutation because it
            # could not fire, and the only test that would cover it is one
            # fabricating a state the code cannot reach.
            if len(parts) < 2:
                malformed.append((n, parts[0]))
                continue
            out[parts[0]] = parts[1]
    return out, malformed


def counters(xml_path):
    """Report-level totals, or None if the report cannot be read.

    ⚠️ A malformed or empty report is a FAILURE WITH A REASON, never a
    traceback and never a skip. JaCoCo 0.8.12 emitted a zero-byte XML for Java
    25 bytecode and this function died with ElementTree.ParseError -- the gate
    crashed instead of reporting, which is indistinguishable to a caller from
    the gate being broken. No current version emits that, but a gate must
    handle bad input rather than rely on the tool having stopped producing it.
    """
    try:
        root = ET.parse(xml_path).getroot()
    except ET.ParseError as e:
        return None
    except OSError as e:
        return None
    got = {}
    for c in root.findall('counter'):          # report-level totals only
        missed, covered = int(c.get('missed')), int(c.get('covered'))
        total = missed + covered
        got[c.get('type')] = (covered, total)
    return got


def pct(covered, total):
    return 100.0 if total == 0 else 100.0 * covered / total


def main():
    skips, malformed = excluded()
    for n, name in malformed:
        print('  %sFAIL%s %s:%d  "%s" is excluded with no reason'
              % (RED, OFF, BASELINE, n, name))
        print('         testing.md rule 7: an exclusion names what unblocks it,')
        print('         so the entry can be removed when that happens. A bare')
        print('         name is a permanent exemption wearing a ratchet\'s clothes.')
    if malformed:
        return 1
    # ⚠️ GIT-DERIVED, not a filesystem walk. `glob.glob('*/build.gradle.kts')`
    # matches any directory on disk, so a reference clone parked at the repo
    # root became a "module" -- the defect that once blocked every commit and
    # which check-gate-scope.sh exists to catch. It caught this file.
    out = subprocess.run(['git', 'ls-files', '*/build.gradle.kts'],
                         capture_output=True, text=True, check=True).stdout
    # ⚠️ No buildSrc special case. One stood here and was DEAD: has_classes
    # globs build/classes/java/main, buildSrc's main sources are Kotlin, and
    # `:compileJava NO-SOURCE` means that directory never exists -- so the loop
    # already skips it. Worse than dead, it defended the wrong thing:
    # buildSrc/build.gradle.kts says buildSrc "gets tests like anything else",
    # so exempting it would un-measure real gate logic exactly when it appears.
    #
    # ⚠️ buildSrc is in this LIST like any other module. It is NOT REGENERATED
    # like one, and that gap is M0.44, not a claim to make here. buildSrc is an
    # implicit included build, so the `./gradlew test jacocoTestReport` this
    # gate runs from the root never reaches it, and buildSrc/build.gradle.kts
    # applies kotlin-dsl but never jacoco. The day buildSrc gains src/main/java
    # the gate fails with a remedy line naming a command that cannot satisfy
    # it. It fails CLOSED, which is why it ships -- but it fails wrong.
    modules = sorted({os.path.dirname(p) for p in out.split()
                      if p.count('/') == 1})
    measured, failed, missing, skipped = 0, False, [], []

    for m in modules:
        # ⚠️ This one IS a filesystem read, and must be: build output is
        # untracked by definition, so git cannot answer it. It is bounded to a
        # module directory that came from git above, so it cannot wander into a
        # sibling checkout. Declared in check-gate-scope.sh's ALLOWED with this
        # reason rather than left for someone to rediscover.
        has_classes = bool(glob.glob('%s/build/classes/java/main/**/*.class' % m, recursive=True))
        report = '%s/build/reports/jacoco/test/jacocoTestReport.xml' % m
        if not has_classes:
            continue                            # nothing to cover yet
        if not os.path.exists(report):
            # Classes exist and no report does. That is a missing measurement,
            # and a missing measurement is a failure -- not a quiet skip.
            missing.append(m)
            failed = True
            continue
        c = counters(report)
        if c == {}:
            # ⚠️ Parsed, but carries no <counter> at all. pct()'s (0,0) default
            # would score that 100% -- success reported over a measurement that
            # did not happen, which is the one thing this gate exists to refuse.
            print('  \033[31mFAIL\033[0m %-20s report has no counters: %s' % (m, report))
            print('         A report that measured nothing is not full coverage.')
            failed = True
            continue
        if c is None:
            print('  \033[31mFAIL\033[0m %-20s report is unreadable (empty or malformed XML): %s'
                  % (m, report))
            print('         A report that cannot be parsed is a measurement that did not happen.')
            failed = True
            continue
        # ⚠️ LINE and BRANCH are NOT symmetric, and defaulting both to (0, 0)
        # made the gate's headline number lie. An absent BRANCH counter is a
        # FACT about the code -- JaCoCo omits it for a class with no branches --
        # so 100% is true. An absent LINE counter is a fact about the
        # MEASUREMENT: every class has lines, so its absence means they were not
        # measured, which `-g:none` or `options.debug = false` produces. Scored
        # as (0, 0) that printed `ok  line 100.0% (floor 95)` for a report whose
        # instruction counter read 10%. `c == {}` above catches only the case
        # where EVERY counter is missing, never a partially populated report.
        if 'LINE' not in c:
            print('  \033[31mFAIL\033[0m %-20s report has no LINE counter: %s' % (m, report))
            print('         Every class has lines, so an absent LINE counter is an')
            print('         unmeasured module, not a fully covered one. Check for')
            print('         -g:none or options.debug = false.')
            failed = True
            continue
        line_cov = pct(*c['LINE'])
        br_cov = pct(*c.get('BRANCH', (0, 0)))
        # ⚠️ AFTER the exclusion check, never before. Counting an excluded
        # module as measured makes the final line state a success that did not
        # happen -- one baselined module at 10%/10% printed
        # "ok 1 module(s) meet 95% line / 90% branch" while zero met either.
        if m in skips:
            skipped.append(m)
            print('  %sWARN%s %-20s line %5.1f%% branch %5.1f%%  EXCLUDED: %s'
                  % (YEL, OFF, m, line_cov, br_cov, skips[m]))
            continue
        measured += 1
        bad = line_cov < LINE_FLOOR or br_cov < BRANCH_FLOOR
        print('  %s%s%s %-20s line %5.1f%% (floor %.0f)  branch %5.1f%% (floor %.0f)'
              % (RED + 'FAIL' if bad else GREEN + 'ok  ', '', OFF, m,
                 line_cov, LINE_FLOOR, br_cov, BRANCH_FLOOR))
        if bad:
            failed = True

    for m in missing:
        print('  %sFAIL%s %s has compiled classes but no JaCoCo report at' % (RED, OFF, m))
        print('         %s/build/reports/jacoco/test/jacocoTestReport.xml' % m)
        print('         Run ./gradlew jacocoTestReport. A gate that skips an unmeasured')
        print('         module reports success for the code most likely to be untested.')

    if failed:
        print('         ⚠️ Line coverage is a floor, not a measure of test quality')
        print('         (testing.md rule 8). Raising it by executing lines without')
        print('         constraining them makes the number worse, not better.')
        return 1
    if measured == 0:
        # Never print ok here. "Nothing to measure" and "everything passed" are
        # different facts and the gate must not conflate them.
        #
        # ⚠️ And "nothing built yet" is a THIRD fact, distinct from "everything
        # that exists is baselined away". Both reach here with measured == 0;
        # only the first is benign. Reporting the second as "no module has
        # compiled classes yet" would describe an empty repository while every
        # module sat excluded at 10% -- which is how a baseline file stops being
        # a ratchet and becomes a place coverage goes to die.
        if skipped:
            print('  %sNOT-MEASURED%s every module with classes is excluded (%s)'
                  % (YEL, OFF, ', '.join(sorted(skipped))))
            print('         baselines/coverage.txt is a ratchet, not a parking space')
            print('         (testing.md rule 7): each entry names what unblocks it.')
            return 0
        print('  %sNOT-MEASURED%s no module has compiled classes yet -- coverage unenforced'
              % (YEL, OFF))
        return 0
    print('  %sok%s   %d module(s) meet %.0f%% line / %.0f%% branch'
          % (GREEN, OFF, measured, LINE_FLOOR, BRANCH_FLOOR))
    return 0


if __name__ == '__main__':
    sys.exit(main())
