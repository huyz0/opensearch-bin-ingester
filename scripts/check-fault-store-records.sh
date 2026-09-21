#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# non-negotiable 9 / M5.49: every verb of the fault-injecting store is METERED.
#
# `FaultInjectingStore` counts the store calls a release path makes, and a verb
# added without its `record(...)` call is invisible to that meter -- silently,
# until some release path reaches it and reds a floor with no defect present.
# M5.29 records the history: `capabilities`, then `close`, then `presign`, the
# last arriving fifteen hours after the row that predicted it.
#
# ⚠️ A TEXT PREDICATE OVER ONE FILE, WHICH IS RUNG 3, and an earlier draft of
# M5.49 argued for a reflective T0 case instead on the grounds that "invoke the
# method, watch the counter" is a predicate over BEHAVIOUR that no grep can
# express. Review answered that `every @Override contains a record(...)` IS a
# grep over one file, that it would have caught `presign`, and that it is
# cheaper and more reliable than reflection. This is `check-terminology.sh`'s
# shape.
#
# ⚠️ WHAT THIS GATE CANNOT SEE, and why it does not replace the cases: WHERE in
# the verb the call sits. Counting a REFUSED or AMBIGUOUS call means recording
# above the injected throw and above `refuseIfPartitioned`, and position is
# pinned by named cases rather than by this script --
# `theLEADERSOWNPartitionIsSEENButBlamedOnNOBODYElse` for `stat`,
# `aNONPARTITIONFaultInTheWindowIsNotCountedAsARefusal` for `list`, and
# `putIfMatch` at two positions in the one verb
# (`aConditionalWriteREFUSEDByAPartitionIsStillCOUNTED` above
# `refuseIfPartitioned`, `theStoresOWNAmbiguousWriteIsCOUNTEDAndStillNotCredited`
# above the injected throws). ⚠️ NAMED RATHER THAN COUNTED: M5.49's row carried a
# COUNT of those cases through two review rounds and it was wrong both times,
# because a case was added beside it in the same round.
#
# ⚠️ AND IT DOES NOT COVER `calls++` ITSELF. Review measured that deleted from
# `record` and restored inside `refuseIfPartitioned`: 494 tests green with every
# `record(...)` present and correctly placed, while the no-arg total silently
# reverts and the per-verb ledger stays right. One assertion in
# `theVerbsThatSKIPThePartitionCheckAreStillCOUNTED` kills that. The gate covers
# PRESENCE, the cases cover POSITION and the TOTAL, and neither substitutes for
# the other.
source "$(dirname "$0")/lib.sh"
cd "$ROOT"
hdr "check-fault-store-records"

# ⚠️ The file is an argument so the harness test can point this at fixtures.
# Without that the only way to test the gate is to break the real store, which
# is how a gate comes to be tested solely by the thing it guards.
TARGET="${1:-sequencer/src/test/java/io/github/huyz0/os/biningester/sequencer/FaultInjectingStore.java}"

if [ ! -f "$TARGET" ]; then
  fail "$TARGET does not exist -- the fault-injecting store moved and this gate did not"
  echo "         A gate that cannot find its subject passes vacuously, which is worse"
  echo "         than failing: it reports ok having examined nothing."
  finish
fi

RAW=$(python3 - "$TARGET" <<'PY'
import sys

src = open(sys.argv[1], encoding='utf-8').read().splitlines()


def first_statement_after(start):
    """The first real statement of the body whose signature starts at `start`.

    ⚠️ BODY-BOUNDED, NOT A LINE COUNT. An earlier version of this gate took an
    eight-line window after the `@Override`, and review MEASURED the
    consequence: for five of twelve verbs the window reached PAST the end of a
    short method into the NEXT one's `record(...)`, so deleting the call left
    the gate green -- `getRange`, `put`, `multipart`, `delete` and
    `capabilities`, the last of which is one of the three verbs M5.29's history
    names. A line count is aimed at the wrong unit.
    """
    depth = 0
    opened = False
    for i in range(start, len(src)):
        line = src[i]
        for ch in line:
            if ch == '{':
                depth += 1
                opened = True
            elif ch == '}':
                depth -= 1
        if not opened:
            continue
        # The body is open; the first meaningful line at or after here is the
        # first statement. Skip the signature's own line only if the brace is
        # the last thing on it.
        for j in range(i + 1, len(src)):
            t = src[j].strip()
            if not t or t.startswith('//') or t.startswith('/*') or t.startswith('*'):
                continue
            return j, t
        return None, None
    return None, None


def is_comment(line):
    """⚠️ `@Override` IN PROSE IS NOT A VERB. The first fixture written for this
    gate said "an @Override with no record(...) call" in its own javadoc, and the
    gate reported the javadoc line as an unmetered verb -- a false refusal that
    would have made the real failure message unreadable beside it.
    """
    t = line.strip()
    return t.startswith('//') or t.startswith('*') or t.startswith('/*')


missing = []
examined = 0
for i, line in enumerate(src):
    if '@Override' not in line or is_comment(line):
        continue
    examined += 1
    j, stmt = first_statement_after(i)
    sig = next((s.strip() for s in src[i:i + 3] if 'public' in s), line.strip())
    if stmt is None:
        missing.append('%s:%d  %s  -- no body found' % (sys.argv[1], i + 1, sig[:70]))
    elif not stmt.startswith('record('):
        # ⚠️ FIRST STATEMENT, NOT "SOMEWHERE IN THE BODY". The message, the hook
        # name and the backlog cell all say FIRST, and review measured the gap:
        # an early `return` above the call meters one branch only, and a
        # commented-out `record(...)` is a substring match. Neither is caught by
        # presence alone, and both are what a verb author reading "first
        # statement" would believe was enforced.
        missing.append('%s:%d  %s  -- first statement is: %s'
                       % (sys.argv[1], i + 1, sig[:60], stmt[:44]))

# ⚠️ THE COUNT IS WHAT THE PREDICATE EXAMINED. An earlier version printed
# `grep -c '@Override'` instead, and review measured three SCOPE mutations
# surviving every fixture because of it: examine only the first `@Override`,
# skip anything past line 100, or simply double the number. None changes a
# verdict; all three shrink what was looked at while the ok line keeps claiming
# twelve. A gate must report the number it computed.
print('EXAMINED %d' % examined)
print('\n'.join(missing))
PY
) || {
  # ⚠️ A CRASHING PREDICATE MUST NOT READ AS "NOTHING MISSING". Command
  # substitution swallows the exit status, so without this an exception inside
  # the heredoc above leaves $MISSING empty and the script falls through to
  # `ok`. Review MEASURED it: with python3 exiting 127, the gate printed
  # `ok 12 verb(s) metered` over a store with `record("stat")` deleted -- the
  # exact state it exists to refuse. This is the vacuous pass this gate was
  # partly written to avoid, one level down inside the gate itself.
  fail "the predicate did not run -- python3 exited non-zero"
  echo "         A gate whose check crashed has examined nothing, and must say so"
  echo "         rather than report ok."
  finish
}

EXAMINED=$(printf '%s\n' "$RAW" | head -1 | awk '{print $2}')
MISSING=$(printf '%s\n' "$RAW" | tail -n +2 | sed '/^$/d')

if [ -n "$MISSING" ]; then
  fail "a verb of the fault-injecting store is not metered:"
  printf '%s\n' "$MISSING" | sed 's/^/           /'
  echo "         Every @Override must call record(\"<verb>\") as its first statement."
  echo "         An unmetered verb is invisible to the release-path floors until one"
  echo "         reaches it and reds with no defect present (M5.29: capabilities, close, presign)."
  finish
fi

# ⚠️ ZERO VERBS IS A FAILURE, NOT AN `ok 0`. `FaultInjectingStore.java` is at
# exactly 700 lines, on `check-file-size.sh`'s ceiling, so its next change is a
# SPLIT -- and a split moving the verbs to a sibling would leave this gate green
# over nothing forever. That is `check-reviewed`'s documented empty-index
# failure mode, and a gate added to close one blind spot must not open another.
if [ "${EXAMINED:-0}" -eq 0 ]; then
  fail "$TARGET declares no @Override at all -- this gate has lost its subject"
  echo "         Either the verbs moved to another file, in which case point this"
  echo "         gate at it, or the store stopped overriding anything, in which case"
  echo "         the release-path floors are measuring something else now."
  finish
fi
ok "$EXAMINED verb(s) metered in $TARGET"
finish
