#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Regenerate the generated index regions in AGENTS.md and .agents/skills/README.md.
#   build-index.sh          rewrite them
#   build-index.sh --check  fail if they are not current
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
MODE="${1:-write}"
python3 - "$MODE" <<'PY'
import os, re, sys, glob
mode = sys.argv[1]
changed = []
broken = []

def frontmatter(path, key):
    with open(path, encoding='utf-8') as f:
        txt = f.read()
    m = re.search(r'^%s:\s*(.+)$' % key, txt, re.M)
    return m.group(1).strip() if m else ''

def trigger(desc):
    """The 'Use when ...' clause -- layer 1 is what gets a skill loaded."""
    m = re.search(r'\bUse (when|before|after|at|whenever|during)\b(.*)', desc, re.S)
    if not m:
        return desc.split('.')[0].strip()
    t = (m.group(1) + m.group(2)).strip().rstrip('.')
    t = re.sub(r'\s+', ' ', t)
    return t[0].upper() + t[1:]

skills = []
for d in sorted(glob.glob('.agents/skills/*/')):
    name = os.path.basename(d.rstrip('/\\'))
    f = os.path.join(d, 'SKILL.md')
    if not os.path.exists(f):
        continue
    skills.append((name, trigger(frontmatter(f, 'description'))))

FAMILY_ORDER = {'Process': 0, 'Quality': 1, 'Delivery': 2, 'Code': 3}
stds = []
for f in sorted(glob.glob('docs/internal/standards/*.md')):
    txt = open(f).read()
    fam = (re.search(r'^\*\*Family:\*\*\s*(.+)$', txt, re.M) or [None, 'Code'])[1].strip()
    rw = re.search(r'^\*\*Read when:\*\*\s*(.+)$', txt, re.M)
    stds.append((fam, os.path.basename(f), f, rw.group(1).strip() if rw else ''))
stds.sort(key=lambda r: (FAMILY_ORDER.get(r[0], 9), r[1]))

def table_skills(prefix):
    rows = ['| Skill | Use when |', '|---|---|']
    rows += ['| [`%s`](%s%s/SKILL.md) | %s |' % (n, prefix, n, t) for n, t in skills]
    return '\n'.join(rows)

def table_standards():
    rows = ['| Family | Standard | Read when |', '|---|---|---|']
    rows += ['| %s | [%s](%s) | %s |' % (fam, base, path.replace('\\', '/'), rw)
             for fam, base, path, rw in stds]
    return '\n'.join(rows)

def table_gates():
    """Generate the enforcement table from registered Gradle tasks and hooks.

    The hook manifest names only launch points; `gates` owns additional tasks
    through its Gradle dependency graph. Read their descriptions from the task
    registrations and add those transitive gates explicitly so the index does
    not regress to treating a Gradle build as a list of shell scripts.
    """
    with open('.pre-commit-config.yaml', encoding='utf-8') as f:
        cfg = f.read()
    with open('build.gradle.kts', encoding='utf-8') as f:
        gradle = f.read()
    hooks = {}
    for block in re.split(r'\n      - id: ', cfg)[1:]:
        entry = re.search(r'^\s*entry:\s*\./gradlew\.bat\s+(\S+)', block, re.M)
        stage = re.search(r'^\s*stages:\s*\[(.*)\]', block, re.M)
        if entry:
            hooks[entry.group(1)] = stage.group(1).strip() if stage else 'pre-commit'

    descriptions = {}
    registration = re.compile(
        r'tasks\.register(?:<[^>]+>)?\("([A-Za-z0-9]+)"\)\s*\{')
    for match in registration.finditer(gradle):
        next_registration = registration.search(gradle, match.end())
        end = next_registration.start() if next_registration else len(gradle)
        body = gradle[match.end():end]
        description = re.search(r'^\s*description\s*=\s*"([^"]+)"', body, re.M)
        if description:
            descriptions[match.group(1)] = description.group(1)

    stages = {
        'gates': 'pre-commit',
        'checkCostLatencyCurve': 'pre-commit',
        'checkHarnessTests': 'pre-commit',
        'checkWired': 'pre-commit',
        'checkOverride': 'pre-commit',
        'checkReviewed': hooks.get('checkReviewed', 'pre-commit'),
        'checkTdd': hooks.get('checkTdd', 'pre-commit'),
        'checkTestIntegrity': hooks.get('checkTestIntegrity', 'commit-msg'),
        'checkCommitMessage': hooks.get('checkCommitMessage', 'commit-msg'),
        'checkMilestoneVerified': 'manual',
        'checkCoverage': 'manual',
        'checkSuiteTime': 'manual',
        'checkMutants': hooks.get('checkMutants', 'manual'),
        'dependencyLicenses': 'build/check',
        'check': 'build/check',
    }
    descriptions['check'] = 'Runs the complete Gradle/JDK gate set and dependency licence gate'
    rows = ['| Gradle task | Stage | Enforces |', '|---|---|---|']
    for task, stage in stages.items():
        description = descriptions.get(task)
        if not description:
            broken.append('build.gradle.kts has no task description for ' + task)
            continue
        rows.append('| `./gradlew %s` | %s | %s |' % (task, stage, description))
    return '\n'.join(rows)


def splice(path, key, body):
    # A missing file or a missing marker used to return silently, which made
    # "the region is absent" indistinguishable from "the region is current".
    if not os.path.exists(path):
        broken.append('%s does not exist' % path)
        return
    with open(path, encoding='utf-8') as f:
        txt = f.read()
    start, end = '<!-- index:%s:start -->' % key, '<!-- index:%s:end -->' % key
    if start not in txt or end not in txt:
        broken.append('%s has no index:%s region' % (path, key))
        return
    # Use a callable replacement: generated paths may contain backslashes on
    # Windows, and a string replacement makes re.sub interpret them as escape
    # sequences (for example, ``\g`` as a group reference).
    replacement = lambda _match: start + '\n' + body + '\n' + end
    new = re.sub(re.escape(start) + r'.*?' + re.escape(end),
                 replacement, txt, flags=re.S)
    if new != txt:
        changed.append('%s (%s)' % (path, key))
        if mode != '--check':
            with open(path, 'w', encoding='utf-8', newline='') as f:
                f.write(new)

splice('AGENTS.md', 'skills', table_skills('.agents/skills/'))
splice('AGENTS.md', 'standards', table_standards())
splice('AGENTS.md', 'gates', table_gates())
splice('.agents/skills/README.md', 'skills', table_skills(''))

# ⚠️ The prose below the generated table is hand-maintained and CAN contradict
# it: the commit adding check-module.sh regenerated the table to say it exists
# while the paragraph four lines down still listed it as "not yet existing",
# and --check passed because that line is outside the markers. A script that
# exists must not be named as absent.
if os.path.exists('AGENTS.md'):
    with open('AGENTS.md', encoding='utf-8') as f:
        txt = f.read()
    # ⚠️ Anchored on a heading, so it FAILS when the anchor is gone rather than
    # silently passing. Both reviewers defeated the first version by rewording
    # the heading or inserting a blank line -- which would have shipped the very
    # contradiction this check exists to catch, undetected. A guard that can be
    # switched off by editing prose is not a guard.
    m = re.search(r'Not yet existing(?:.|\n)*?(?=\n## |\Z)', txt)
    if not m:
        broken.append('AGENTS.md has no "Not yet existing" paragraph -- this check '
                      'is anchored on it. Restore the heading, or update '
                      'build-index.sh to anchor on whatever replaced it.')
    else:
        for f in sorted(glob.glob('scripts/check-*.sh')):
            name = os.path.basename(f)
            if '`%s`' % name in m.group(0):
                broken.append('AGENTS.md lists %s as "not yet existing", but it is in scripts/' % name)

if broken:
    print('  \033[31mFAIL\033[0m generated index region(s) missing: ' + ', '.join(broken))
    sys.exit(1)
if mode == '--check':
    if changed:
        print('  \033[31mFAIL\033[0m generated index is stale: ' + ', '.join(changed))
        print('         run scripts/build-index.sh')
        sys.exit(1)
    print('  \033[32mok\033[0m   generated index regions are current')
else:
    print('  \033[32mok\033[0m   regenerated ' + (', '.join(changed) if changed else 'nothing (already current)'))
PY
