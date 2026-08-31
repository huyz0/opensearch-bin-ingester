# SPDX-License-Identifier: Apache-2.0
"""Sum every configured memory limit and compare it against the session ceiling.

Reads the real numbers out of the real files: gradle.properties for the daemons,
the java-conventions plugin for the test JVM, and every docker-compose service
for the containers.
"""
import os, re, subprocess, sys


def workspace_files(*patterns):
    """Tracked + staged, git-derived -- the python half of lib.sh's helper.

    Not a filesystem walk: a docker-compose.yml inside a reference clone under
    .tmp/ is not ours to check, and a gate whose answer depends on what else
    happens to be on the disk is not a gate.
    """
    out = set()
    for args in (['git', 'ls-files', '--'], ['git', 'diff', '--cached',
                                             '--name-only', '--diff-filter=ACMR', '--']):
        r = subprocess.run(args + list(patterns), capture_output=True, text=True)
        if r.returncode == 0:
            out.update(p for p in r.stdout.split() if os.path.isfile(p))
    return sorted(out)

RED, GREEN, YEL = '\033[31m', '\033[32m', '\033[33m'
OFF = '\033[0m'

# build.md: 8 GiB is the per-session share of a 32 GiB WSL2 box running several
# sessions. 6 GiB is the ceiling; the rest is page cache and headroom.
CEILING_MIB = 6144
CONVENTIONS = 'buildSrc/src/main/kotlin/binjava.java-conventions.gradle.kts'


def mib(text):
    # Decimals are legal in compose (`mem_limit: 1.5g`) and in -Xmx only as
    # whole units, but accepting both here costs nothing and refusing to parse
    # is safer than silently reading 1.5g as 1.
    m = re.match(r'^(\d+(?:\.\d+)?)\s*([gGmMkK]?[bB]?)$', text.strip())
    if not m:
        return None
    n, unit = float(m.group(1)), m.group(2).lower().rstrip('b')
    scale = {'g': 1024.0, 'm': 1.0, 'k': 1.0 / 1024.0, '': 1.0 / (1024.0 * 1024.0)}
    if unit not in scale:
        return None
    return int(round(n * scale[unit]))


def main():
    props = open('gradle.properties').read()
    parts, failed = [], False

    def need(pattern, label, src, text):
        nonlocal failed
        m = re.search(pattern, text, re.M)
        if not m:
            print('  %sFAIL%s %s declares no %s (build.md)' % (RED, OFF, src, label))
            failed = True
            return 0
        v = mib(m.group(1))
        if v is None:
            print('  %sFAIL%s %s: cannot read %s from %r' % (RED, OFF, src, label, m.group(1)))
            failed = True
            return 0
        parts.append((label, v))
        return v

    need(r'^org\.gradle\.jvmargs=.*?-Xmx(\S+?)\b', 'Gradle daemon heap', 'gradle.properties', props)


    m = re.search(r'^org\.gradle\.workers\.max=(\d+)', props, re.M)
    if not m:
        print('  %sFAIL%s gradle.properties sets no org.gradle.workers.max (build.md)' % (RED, OFF))
        print('         Without it Gradle sizes the pool from the core count, and the')
        print('         per-worker limits below stop bounding anything.')
        failed = True
        workers = 1
    else:
        workers = int(m.group(1))

    conv_files = [p for p in workspace_files('buildSrc/src/main/kotlin/*.gradle.kts')
                  if 'maxHeapSize' in open(p).read()] or (
                  [CONVENTIONS] if os.path.exists(CONVENTIONS) else [])
    if conv_files:
        conv = '\n'.join(open(p).read() for p in conv_files)
        # The compile daemon's heap is read from the conventions plugin, where
        # the runtime actually enforces it. It used to be read from
        # gradle.properties, whose `org.gradle.java.compile-daemon.jvmargs` key
        # Gradle does not read at all -- so this gate counted 512 MiB toward the
        # budget for a daemon that was in fact unbounded.
        cd = re.search(r'forkOptions\.memoryMaximumSize\s*=\s*"([^"]+)"', conv)
        if not cd:
            print('  %sFAIL%s %s sets no compile-daemon forkOptions.memoryMaximumSize (build.md)'
                  % (RED, OFF, CONVENTIONS))
            failed = True
        else:
            per_cd = mib(cd.group(1)) or 0
            parts.append(('compile daemons (%d x %d MiB)' % (workers, per_cd),
                          per_cd * workers))

        t = re.search(r'maxHeapSize\s*=\s*"([^"]+)"', conv)
        if not t:
            print('  %sFAIL%s %s sets no test-JVM maxHeapSize (build.md)' % (RED, OFF, CONVENTIONS))
            failed = True
        else:
            # Every worker can hold a test JVM at once, so the budget is the
            # product, not the single value.
            per = mib(t.group(1))
            if per is None:
                print('  %sFAIL%s cannot read test-JVM maxHeapSize %r'
                      % (RED, OFF, t.group(1)))
                failed = True
            else:
                parts.append(('test JVMs (%d x %d MiB)' % (workers, per), per * workers))
        if 'timeout.set(' not in conv:
            print('  %sFAIL%s %s sets no test timeout -- a hung test holds its memory'
                  % (RED, OFF, CONVENTIONS))
            failed = True
    elif os.path.exists('gradlew'):
        # The build exists, so an unfound limit is a missing limit, not a
        # not-yet. Warning here is how a term silently drops out of the sum.
        print('  %sFAIL%s no test-JVM maxHeapSize found in buildSrc/ (build.md)' % (RED, OFF))
        failed = True
    else:
        print('  %sWARN%s no build yet -- test JVM limit unenforced' % (YEL, OFF))

    # Every compose service declares a limit, checked within its own block. The
    # previous regex was unanchored, so a service with no limit was satisfied by
    # one belonging to a different service later in the file.
    for f in workspace_files('docker-compose*.y*ml', '*/docker-compose*.y*ml'):
        lines = open(f).read().splitlines()
        # Only blocks nested under a top-level `services:` key are services.
        # Reading every two-space key as one meant `volumes:` and `networks:`
        # were asked to declare a memory limit.
        blocks, name, buf, in_services, svc_indent = [], None, [], False, None
        for line in lines:
            if re.match(r'^[A-Za-z0-9_-]+:', line):
                if name:
                    blocks.append((name, buf))
                    name, buf = None, []
                in_services = line.startswith('services:')
                svc_indent = None
                continue
            if not in_services:
                continue
            m = re.match(r'^(\s+)([A-Za-z0-9_-]+):\s*(?:#.*)?$', line)
            if m and svc_indent is None:
                svc_indent = len(m.group(1))     # whatever this file indents by
            if m and len(m.group(1)) != svc_indent:
                m = None                          # a nested key, not a service
            if m:
                if name:
                    blocks.append((name, buf))
                name, buf = m.group(2), []
            elif name is not None:
                buf.append(line)
        if name:
            blocks.append((name, buf))
        for svc, body in blocks:
            text = '\n'.join(body)
            lim = re.search(r'^\s*(?:mem_limit|memory):\s*[\'"]?'
                            r'(\d+(?:\.\d+)?\s*[gGmMkK]?[bB]?)', text, re.M)
            if lim and mib(lim.group(1)) is None:
                print('  %sFAIL%s %s: service %r has an unparseable memory limit %r'
                      % (RED, OFF, f, svc, lim.group(1)))
                failed = True
                continue
            if not lim:
                print("  %sFAIL%s %s: service '%s' declares no memory limit" % (RED, OFF, f, svc))
                print('         A container without one can take the session down with it.')
                failed = True
            else:
                parts.append(('%s/%s' % (os.path.basename(f), svc), mib(lim.group(1)) or 0)) 

    total = sum(v for _, v in parts)
    for label, v in parts:
        print('         %-34s %5d MiB' % (label, v))
    if total > CEILING_MIB:
        print('  %sFAIL%s configured limits sum to %d MiB, ceiling %d MiB (build.md)'
              % (RED, OFF, total, CEILING_MIB))
        print('         Lower a limit or reduce org.gradle.workers.max. Do NOT raise the')
        print('         ceiling: it is the number that keeps WSL2 from killing the session.')
        return 1
    if failed:
        return 1
    print('  %sok%s   limits sum to %d MiB, within the %d MiB ceiling'
          % (GREEN, OFF, total, CEILING_MIB))
    return 0


if __name__ == '__main__':
    sys.exit(main())
