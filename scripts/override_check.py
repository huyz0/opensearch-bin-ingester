#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""An override entry's round count and finding ids, diffed against the verdicts.

review/overrides.md is the one artefact recording that a hard rule was
exceeded, and the only one with no gate (M8.41).

   override_check.py check <overrides.md> <store>[:<store>...] [<line>,<line>...]

A store is a directory of verdict files, read along with its per-task
subdirectories: the local .harness/review/ and the committed review/verdicts/
<task>/ together are the record. ⚠️ READING ONE ALONE REFUSES TRUE ENTRIES: many
older tasks' rounds live only in the committed store.

An ENTRY is a blank-line-separated block whose first line opens with a task id
and then " - " or " (" -- the file's own format. A block's later lines are the
entry's too, and a line that merely starts with an id inside a paragraph is
not an entry. With line numbers given, only the entries containing one of them
are judged; with none, every entry is. Each entry is judged ON ITS OWN, since a
task can have several:
  - a round count it STATES ("FOUR ROUNDS") may not exceed the rounds recorded:
    distinct diff hashes reviewed for the task, review_rounds.py's definition.
    ⚠️ AN UPPER BOUND ONLY: a diff re-staged and re-reviewed is a second hash
    in one round, so hashes can outnumber rounds and equality would refuse
    true entries.
  - every finding id it cites, P<n> or T<n>, is a finding recorded for the
    task, in ANY of its rounds: which round an entry says a finding came from
    is prose this does not parse. ⚠️ P AND T ONLY, because R<n> is also a cost
    rule in cost.md. T0-T4 are also testing.md's tiers: one the task has no
    finding for is refused with a message saying so, and one it DOES have a
    finding for passes as that finding.
It does NOT parse a verdict claim ("all three passed"), a per-round finding
count ("four, then three, then two") or "both majors" -- which is every one of
M8.4's three mistakes. What it catches is an id or a round that never existed.
A task with no verdict here is UNJUDGED, reported and not failed: the store is
gitignored and local, so a fresh checkout has none (the reason this cannot run
in CI, as check-reviewed cannot).
"""
import glob
import json
import os
import re
import sys

WORDS = {'ONE': 1, 'TWO': 2, 'THREE': 3, 'FOUR': 4, 'FIVE': 5, 'SIX': 6,
         'SEVEN': 7, 'EIGHT': 8, 'NINE': 9, 'TEN': 10}
ENTRY_START = re.compile(r'^(M\d+\.\w+)\s+[-(]')
COUNT = re.compile(r'\b(' + '|'.join(WORDS) + r'|\d+) ROUNDS\b', re.IGNORECASE)
FINDING = re.compile(r'\b([PT]\d{1,3})\b')
TIER = re.compile(r'^T[0-4]$')


def entries(text):
    """Every entry as (task, first line, last line, text), lines 1-based."""
    found = []
    block = []
    start = 0

    def close():
        if block:
            m = ENTRY_START.match(block[0])
            if m:
                found.append((m.group(1), start, start + len(block) - 1, '\n'.join(block)))

    for number, line in enumerate(text.splitlines(), start=1):
        if line.strip():
            if not block:
                start = number
            block.append(line)
        else:
            close()
            block = []
    close()
    return found


def recorded(stores, task):
    """(rounds, finding ids) the stores hold for task, or None if they hold none."""
    hashes = set()
    ids = set()
    paths = []
    for store in stores:
        paths += glob.glob(os.path.join(store, '*.json'))
        paths += glob.glob(os.path.join(store, task, '*.json'))
    for path in paths:
        try:
            with open(path) as f:
                v = json.load(f)
        except (OSError, ValueError):
            continue
        if v.get('task') != task:
            continue
        if v.get('diff_sha256'):
            hashes.add(v['diff_sha256'])
        for finding in v.get('findings') or []:
            if isinstance(finding, dict) and finding.get('id'):
                # a reviewer sometimes prefixes the task: `M8.0-P1` is P1
                ids.add(str(finding['id']).rsplit('-', 1)[-1])
    if not hashes:
        return None
    return len(hashes), ids


def claimed_rounds(entry):
    m = COUNT.search(entry)
    if not m:
        return None
    word = m.group(1).upper()
    return WORDS.get(word) or int(word)


def judge(entry, rounds, ids):
    """Every mismatch between one entry and its recorded verdicts."""
    problems = []
    claimed = claimed_rounds(entry)
    if claimed is not None and claimed > rounds:
        problems.append('states %d rounds; %d recorded' % (claimed, rounds))
    missing = sorted({i for i in FINDING.findall(entry) if i not in ids},
                     key=lambda i: (i[0], int(i[1:])))
    if missing:
        problem = 'cites finding(s) no verdict records: ' + ', '.join(missing)
        if any(TIER.match(i) for i in missing):
            problem += (' (T0-T4 are also testing.md tiers; a tier is written '
                        '"tier N" in an entry, so it is not read as a finding)')
        problems.append(problem)
    return problems


def main(argv):
    if len(argv) not in (4, 5) or argv[1] != 'check':
        print('usage: override_check.py check <overrides.md> <store>[:<store>...] '
              '[<line>,...]', file=sys.stderr)
        return 2
    with open(argv[2]) as f:
        all_entries = entries(f.read())
    lines = {int(n) for n in argv[4].split(',') if n} if len(argv) == 5 else None
    failed = 0
    for task, first, last, text in all_entries:
        if lines is not None and not any(first <= n <= last for n in lines):
            continue
        held = recorded(argv[3].split(':'), task)
        if held is None:
            print('UNJUDGED %s (line %d): no verdict for it in %s' % (task, first, argv[3]))
            continue
        for problem in judge(text, *held):
            print('MISMATCH %s (line %d): %s' % (task, first, problem))
            failed += 1
    return 1 if failed else 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
