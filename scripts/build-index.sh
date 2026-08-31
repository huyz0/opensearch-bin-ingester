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
    with open(path) as f:
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
    name = os.path.basename(d.rstrip('/'))
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
    rows += ['| %s | [%s](%s) | %s |' % (fam, base, path, rw) for fam, base, path, rw in stds]
    return '\n'.join(rows)

def table_gates():
    """The Gates table, read from .pre-commit-config.yaml and scripts/.

    Hand-maintained, this table was wrong in three directions at once: it named
    a hook that was not wired, omitted four that were, and never mentioned a
    script that existed. A section that calls itself "the honest answer to what
    actually runs" is the last place a stale list belongs, so it is generated.
    """
    cfg = open('.pre-commit-config.yaml').read()
    hooks = []
    for block in re.split(r'\n      - id: ', cfg)[1:]:
        hid = block.split('\n', 1)[0].strip()
        ent = re.search(r'^\s*entry:\s*(.+)$', block, re.M)
        nam = re.search(r'^\s*name:\s*"(.*)"\s*$', block, re.M)
        stg = re.search(r'^\s*stages:\s*\[(.*)\]', block, re.M)
        hooks.append((hid,
                      ent.group(1).strip() if ent else '?',
                      nam.group(1) if nam else '',
                      stg.group(1).strip() if stg else 'pre-commit'))
    rows = ['| Script | Stage | Enforces |', '|---|---|---|']
    wired = set()
    for _, entry, name, stage in hooks:
        script = entry.split()[0]
        wired.add(os.path.basename(script))
        shown = entry if ' ' in entry else os.path.basename(entry)
        rows.append('| `%s` | %s | %s |' % (shown, stage, name))
    present = {os.path.basename(f) for f in glob.glob('scripts/check-*.sh')}
    unwired = sorted(present - wired)
    out = '\n'.join(rows)
    if unwired:
        # ⚠️ No reason is asserted here. An earlier version claimed each unwired
        # script "needs an argument a whole-tree hook cannot supply", which is
        # false for check-module.sh -- it runs with zero arguments on the delta
        # path. A generator cannot know why a script is unwired, so it states
        # the fact and leaves the reason to the script.
        out += ('\n\nPresent in `scripts/` but **not** wired into '
                '`.pre-commit-config.yaml` — invoke by hand, from a skill, or from CI: '
                + ', '.join('`%s`' % u for u in unwired) + '.')
    return out


def splice(path, key, body):
    # A missing file or a missing marker used to return silently, which made
    # "the region is absent" indistinguishable from "the region is current".
    if not os.path.exists(path):
        broken.append('%s does not exist' % path)
        return
    txt = open(path).read()
    start, end = '<!-- index:%s:start -->' % key, '<!-- index:%s:end -->' % key
    if start not in txt or end not in txt:
        broken.append('%s has no index:%s region' % (path, key))
        return
    new = re.sub(re.escape(start) + r'.*?' + re.escape(end),
                 start + '\n' + body + '\n' + end, txt, flags=re.S)
    if new != txt:
        changed.append('%s (%s)' % (path, key))
        if mode != '--check':
            open(path, 'w').write(new)

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
    txt = open('AGENTS.md').read()
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
