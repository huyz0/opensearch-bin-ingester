# SPDX-License-Identifier: Apache-2.0
"""Judge a PIT report against the changed classes (testing.md rule 9, M0.14).

⚠️ SCOPED TO THE DIFF, NOT TO THE MODULE. PIT is asked to mutate only the
changed classes, but a report on disk may be older or wider than this run --
a previous invocation's file, or a module-wide run someone did by hand -- so
the scoping is applied AGAIN here, over the report's own contents. A gate that
trusted the invocation would measure whatever the last run left behind.
"""
import sys
import xml.etree.ElementTree as ET

KILLED = ("KILLED", "TIMED_OUT")


def score(xml_paths, targets):
    """(killed, total, survivors) over mutants whose class is in `targets`."""
    killed = total = 0
    survivors = []
    for path in xml_paths:
        try:
            root = ET.parse(path).getroot()
        except (ET.ParseError, OSError) as broken:
            # ⚠️ FAIL CLOSED. An unreadable report is not an empty one.
            raise SystemExit(f"unreadable mutation report {path}: {broken}")
        for m in root.iter("mutation"):
            cls = (m.findtext("mutatedClass") or "").strip()
            if cls not in targets:
                continue
            total += 1
            if (m.get("status") or "").strip() in KILLED:
                killed += 1
            else:
                survivors.append((
                    cls,
                    (m.findtext("lineNumber") or "?").strip(),
                    (m.findtext("mutator") or "?").rsplit(".", 1)[-1],
                    (m.get("status") or "?").strip(),
                ))
    return killed, total, survivors


def main(argv):
    if len(argv) < 3:
        raise SystemExit("usage: mutants.py <floor-pct> <targets-file> <report.xml>...")
    floor = float(argv[1])
    with open(argv[2], encoding="utf-8") as f:
        targets = {line.strip() for line in f if line.strip()}
    killed, total, survivors = score(argv[3:], targets)

    if total == 0:
        # ⚠️ NOT SUCCESS. The caller decides whether "no mutants" is legitimate
        # (nothing mutable changed) or a failure (PIT ran and produced nothing);
        # this exit code says only that nothing was measured.
        print("NOT-MEASURED no mutants were generated for the changed classes")
        return 2

    pct = 100.0 * killed / total
    for cls, line, mutator, status in survivors:
        print(f"       {status} {cls.rsplit('.', 1)[-1]}:{line} {mutator}")
    verdict = "PASS" if pct >= floor else "FAIL"
    print(f"{verdict} {killed}/{total} mutant(s) killed on changed code, "
          f"{pct:.1f}% (floor {floor:.0f}%)")
    return 0 if pct >= floor else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
