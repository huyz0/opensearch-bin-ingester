# SPDX-License-Identifier: Apache-2.0
"""Name the harness tests that failed, from the JUnit XML the suite wrote.

⚠️ THE LOG CANNOT ANSWER THIS. `check-harness-tests.sh` runs the suite under
`--console=plain -q`, so its log holds "BUILD FAILED" and no test names, and the
grep that used to read it matched exactly that -- printing a line that named
nothing. On CI, where `.harness/` is never uploaded, that made a failing gate
undiagnosable: two runs were spent not knowing which test broke.
"""
import glob
import xml.etree.ElementTree as ET

CAP = 12
seen = 0
for path in sorted(glob.glob('buildSrc/build/test-results/test/TEST-*.xml')):
    try:
        root = ET.parse(path).getroot()
    except (ET.ParseError, OSError):
        continue
    for tc in root.iter('testcase'):
        bad = list(tc.iter('failure')) + list(tc.iter('error'))
        if not bad:
            continue
        seen += 1
        if seen > CAP:
            continue
        # ⚠️ THE MESSAGE AND THE FIRST LINES OF THE TEXT, and generously.
        # A 120-character cap was tried and it truncated `rm: cannot remove
        # '/tmp/fresh...'` exactly before the errno -- the one word that says
        # whether the cause is a permission, a busy file or a race. A reporter
        # that cuts off the reason has moved the problem rather than solved it.
        msg = (bad[0].get('message') or '').replace('\n', ' ')[:600]
        print('%s.%s' % (tc.get('classname', '?').rsplit('.', 1)[-1], tc.get('name', '?')))
        print('    %s' % msg)
        body = (bad[0].text or '').strip().splitlines()
        for line in body[:4]:
            print('      | %s' % line.strip()[:220])
if seen > CAP:
    print('... and %d more' % (seen - CAP))
if seen == 0:
    # ⚠️ NOT SILENCE. A suite that failed before writing any XML -- a compile
    # error, an OOM -- has no failing testcase to name, and saying so is the
    # difference between "nothing failed" and "I cannot tell you what did".
    print('no failing testcase in the XML -- the suite did not get that far;')
    print('see .harness/harness-tests.log')
