#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Each entry of M8's unwired set is WIRED, or OWNED by an open row (M8.25).

    workspace_files '*.java' | wired_scan.py <SPEC.md> <backlog.md>

Reads the table under `## The unwired set` in the spec -- the ONLY copy of
the list; this file carries none -- and judges each row:

    WIRED    every predicate in its third column holds over src/main code
    OWNED    not wired, and its fourth column names a backlog row not `done`
    UNWIRED  neither: exit 1

A predicate is a backticked span in the third column, one of:

    `new T`             `new T(` or `new T<` outside the file declaring T, or a
                        call `T.m(` from another file when T's file builds T
    `new-impl I`        `new C(` for some src/main class C implementing I,
                        outside C's own file
    `call pkg.T.m`      a call through a receiver declared as exactly pkg.T, or pkg.T.m/pkg.T::m
    `implements I`      a src/main type implements I (or X.I)
    `main`              `public static void main(`
    `returns T`         a method whose return type is exactly T
    `constant N in M`   N is declared in src/main and read in module M's src/main

Every predicate in a cell must hold. A cell with no span this grammar reads is
the SPEC's defect and fails -- never a silent WIRED and never a silent OWNED.

⚠️ CODE ONLY, src/main ONLY. Comments and literals are blanked first
(io_seam_scan.code_only): "referenced" was MEASURED green on the real tree for
five mechanisms whose every hit was a javadoc saying they were NOT wired.
"""
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from io_seam_scan import code_only  # noqa: E402

SPAN = re.compile(r'`([^`]+)`')
ROW_ID = re.compile(r'\bM\d+\.\d+[a-z]?\b')


def main_sources(paths):
    """(path, module, code) for every .java path under a src/main tree.

    ⚠️ THE PATHS COME ON STDIN, from `workspace_files` -- tracked and staged,
    git-derived -- never from a walk of the filesystem, which would judge
    build output and a sibling checkout (check-gate-scope).
    """
    found = []
    for path in paths:
        parts = path.replace(os.sep, '/').split('/')
        if not path.endswith('.java') or 'src' not in parts:
            continue
        i = parts.index('src')
        if i + 1 >= len(parts) or parts[i + 1] != 'main':
            continue
        module = parts[i - 1] if i > 0 else ''
        with open(path, encoding='utf-8') as f:
            found.append((path, module, code_only(f.read())))
    return found


def declares_type(code, name):
    return re.search(r'\b(class|interface|record|enum)\s+' + re.escape(name) + r'\b', code)


def package_name(code):
    match = re.search(r'(?m)^\s*package\s+([\w.]+)\s*;', code)
    return match.group(1) if match else ''


def declared_types(sources):
    result = set()
    for _, _, code in sources:
        package = package_name(code)
        for name in re.findall(r'\b(?:class|interface|record|enum)\s+([A-Za-z_$][\w$]*)', code):
            result.add(f'{package}.{name}' if package else name)
    return result


def qualified_type(name, code, known_types):
    if '.' in name:
        return name
    imported = next((value for value in re.findall(r'\bimport\s+([\w.]+)\s*;', code)
                     if value.rsplit('.', 1)[-1] == name), None)
    if imported:
        return imported
    package = package_name(code)
    same_package = f'{package}.{name}' if package else name
    if same_package in known_types:
        return same_package
    wildcard_imports = set(re.findall(r'\bimport\s+([\w.]+)\.\*\s*;', code))
    candidates = {value for value in known_types
                  if value.rsplit('.', 1)[-1] == name
                  and value.rpartition('.')[0] in wildcard_imports}
    if len(candidates) == 1:
        return next(iter(candidates))
    if len(candidates) > 1:
        return f'<ambiguous>.{name}'
    return same_package


def method_scope(code, offset):
    controls = {'if', 'for', 'while', 'switch', 'catch', 'synchronized', 'try', 'when'}
    scope = re.compile(r'\b([A-Za-z_$][\w$]*)\s*\([^{};]*\)\s*(?:throws\s+[^{}]+)?\{')
    matches = []
    for match in scope.finditer(code):
        if match.group(1) in controls:
            continue
        opening = match.end() - 1
        depth = 0
        closing = None
        for index in range(opening, len(code)):
            if code[index] == '{':
                depth += 1
            elif code[index] == '}':
                depth -= 1
                if depth == 0:
                    closing = index
                    break
        if closing is not None and opening < offset < closing:
            matches.append((opening + 1, closing))
    return min(matches, key=lambda span: span[1] - span[0]) if matches else None


def constructs(sources, name):
    pat = re.compile(r'\bnew\s+' + re.escape(name) + r'\s*[(<]')
    if any(pat.search(code) and not declares_type(code, name) for _, _, code in sources):
        return True
    # ⚠️ A STATIC FACTORY CALLED FROM ANOTHER FILE IS A CONSTRUCTION -- the
    # plugin builds `NodeSubscriptions` through `NodeSubscriptions.fetching`
    # -- but only when T's own file does construct T: a static call into a
    # class that never builds itself is not one.
    if not any(declares_type(code, name) and pat.search(code) for _, _, code in sources):
        return False
    call = re.compile(r'\b' + re.escape(name) + r'\s*\.\s*\w+\s*\(')
    return any(not declares_type(code, name) and any(
        not re.search(r'\bnew\s*$', code[:m.start()]) for m in call.finditer(code))
        for _, _, code in sources)


def implementors(sources, iface):
    pat = re.compile(r'\b(?:class|record|enum)\s+(\w+)[^{;]*?\bimplements\b[^{]*?'
                     r'(?:\.|\b)' + re.escape(iface) + r'\b')
    return {m.group(1) for _, _, code in sources for m in pat.finditer(code)}


def judge(kind, arg, sources):
    if kind == 'new':
        return constructs(sources, arg)
    if kind == 'new-impl':
        return any(constructs(sources, c) for c in implementors(sources, arg))
    if kind == 'call':
        target = re.fullmatch(r'((?:[A-Za-z_$][\w$]*\.)*[A-Za-z_$][\w$]*)\.([A-Za-z_$][\w$]*)', arg)
        if not target:
            return False
        type_name, method = target.groups()
        known_types = declared_types(sources)
        simple_type = type_name.rsplit('.', 1)[-1]
        target_types = {qualified_type(simple_type, code, known_types) for _, _, code in sources
                        if declares_type(code, simple_type)}
        target_type = type_name if '.' in type_name else next(iter(target_types), None)
        if target_type is None or target_type not in target_types:
            return False
        static_call = re.compile(r'\b((?:[A-Za-z_$][\w$]*\.)*[A-Za-z_$][\w$]*)\s*(?:\.|::)\s*'
                                 + re.escape(method) + r'\s*\(')
        method_call = re.compile(r'\b(\w+)\s*(?:\.|::)\s*' + re.escape(method) + r'\s*\(')
        for _, _, code in sources:
            if declares_type(code, simple_type) and qualified_type(simple_type, code, known_types) == target_type:
                continue
            declarations = [
                (match.start(), match.group(1), match.group(2), method_scope(code, match.start()))
                for match in re.finditer(
                    r'\b((?:[A-Za-z_$][\w$]*\.)*[A-Za-z_$][\w$]*)\s+(\w+)\b(?!\s*\()', code)]
            static_target_call = any(qualified_type(match.group(1), code, known_types) == target_type
                                     for match in static_call.finditer(code))
            def receiver_matches(call):
                call_scope = method_scope(code, call.start())
                visible = [declaration for declaration in declarations
                           if declaration[2] == call.group(1)
                           and (declaration[3] is None or declaration[3] == call_scope)]
                return bool(visible) and all(
                    qualified_type(declaration[1], code, known_types) == target_type
                    for declaration in visible)
            if static_target_call or any(receiver_matches(match) for match in method_call.finditer(code)):
                return True
        return False
    if kind == 'implements':
        return bool(implementors(sources, arg))
    if kind == 'main':
        return any(re.search(r'\bpublic\s+static\s+void\s+main\s*\(', code)
                   for _, _, code in sources)
    if kind == 'returns':
        pat = re.compile(r'(?:^|[\s.])' + re.escape(arg).replace(r'\ ', r'\s*')
                         + r'\s+\w+\s*\(', re.M)
        return any(pat.search(code) for _, _, code in sources)
    if kind == 'constant':
        m = re.fullmatch(r'(\w+)\s+in\s+([\w-]+)', arg)
        if not m:
            raise ValueError('constant needs `NAME in MODULE`')
        name, module = m.groups()
        decl = re.compile(r'\b' + name + r'\s*=(?!=)')
        use = re.compile(r'\b' + name + r'\b')
        declared = any(decl.search(code) for _, _, code in sources)
        read = any(mod == module and use.search(code) and not decl.search(code)
                   for _, mod, code in sources)
        return declared and read
    raise ValueError('unknown kind')


def predicates(cell):
    """[(kind, arg)] from a cell, or None if a span is not in the grammar."""
    out = []
    for span in SPAN.findall(cell):
        span = span.strip()
        if span == 'main':
            out.append(('main', ''))
            continue
        kind, _, arg = span.partition(' ')
        if kind not in ('new', 'new-impl', 'call', 'implements', 'returns', 'constant') \
                or not arg.strip():
            return None
        if kind == 'call' and not re.fullmatch(r'[A-Za-z_$][\w$]*(?:\.[A-Za-z_$][\w$]*){2,}', arg.strip()):
            return None
        out.append((kind, arg.strip()))
    return out or None


def table(spec):
    rows, inside = [], False
    with open(spec, encoding='utf-8') as f:
        for line in f:
            if line.startswith('## '):
                inside = line.strip() == '## The unwired set'
                continue
            if inside and line.startswith('| M'):
                cells = [c.strip() for c in line.strip().strip('|').split('|')]
                rows.append(cells)
    return rows


def open_rows(backlog):
    state = {}
    with open(backlog, encoding='utf-8') as f:
        for line in f:
            if line.startswith('| M'):
                cells = [c.strip() for c in line.strip().strip('|').split('|')]
                state[cells[0]] = cells[-1]
    return {k for k, v in state.items() if not v.startswith('done')}, set(state)


def main():
    spec, backlog = sys.argv[1:3]
    sources = main_sources([p.strip() for p in sys.stdin if p.strip()])
    open_ids, known = open_rows(backlog)
    rows = table(spec)
    if not rows:
        print('no `## The unwired set` table found in ' + spec)
        return 1
    bad = 0
    for cells in rows:
        entry = cells[0]
        if len(cells) < 4:
            print(f'{entry}: UNREADABLE -- the table needs an "Else owned by" column')
            bad += 1
            continue
        preds = predicates(cells[2])
        if preds is None:
            print(f'{entry}: UNREADABLE -- no predicate this scan reads in "{cells[2]}"')
            bad += 1
            continue
        try:
            missing = [f'`{k} {a}`'.replace(' `', '`') for k, a in preds
                       if not judge(k, a, sources)]
        except ValueError as e:
            print(f'{entry}: UNREADABLE -- {e}')
            bad += 1
            continue
        if not missing:
            print(f'{entry}: WIRED')
            continue
        owners = [o for o in ROW_ID.findall(cells[3]) if o in open_ids]
        if owners:
            print(f'{entry}: OWNED by {", ".join(owners)} (still missing {", ".join(missing)})')
            continue
        named = ROW_ID.findall(cells[3])
        why = ('its owner ' + ', '.join(named) + (' is done' if all(n in known for n in named)
                                                  else ' has no backlog row')) if named \
            else 'no open row owns it'
        print(f'{entry}: UNWIRED -- missing {", ".join(missing)}; {why}')
        bad += 1
    return 1 if bad else 0


if __name__ == '__main__':
    sys.exit(main())
