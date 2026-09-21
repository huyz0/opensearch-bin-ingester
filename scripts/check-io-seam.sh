#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Non-negotiable 7 / code-structure.md rule 3: business logic touches no socket,
# clock or object store directly. It takes a seam.
#
# ⚠️ THE I/O PACKAGES ARE BANNED OUTRIGHT; THE CLOCK AND THE SUBPROCESS ARE
# NAMED BY CONSTRUCT. `io_seam_scan.py`'s `PACKAGES` is the list -- NO COUNT AND NO ENUMERATION APPEARS HERE, because this header
# carried both and went stale the round after the list grew, for the ninth
# recurrence of one defect class on this task. Banning a package means no
# sibling can be NAMED inside a listed one, which is what ended rounds 1-3's
# sibling hunt.
#
# ⚠️ IT DOES NOT CLOSE SUBCLASSING OUT OF A LISTED PACKAGE, nor a type handed
# back without being named: `java.util.jar.JarFile` EXTENDS the banned
# `java.util.zip.ZipFile`, and `javax.sql.DataSource` returns a `java.sql`
# connection a `var` never spells. Both were MEASURED passing, one round apart.
# Each PACKAGE a reach can live in must be named, and the list grows when a
# reader finds one.
#
# ⚠️ `java.io` AND `java.util.zip` ARE BANNABLE ONLY BECAUSE OF THE CARVE-OUT.
# `InputStream`, `ByteArrayOutputStream`, `IOException` and `CRC32C` live in
# them and are how every segment in this project moves; `io_seam_scan.py`'s
# `ALLOWED` names them, and each one is pinned by a case that must PASS.
#
# ⚠️ `java.time`, `java.util` AND `java.lang` CANNOT BE BANNED, because
# `Clock`, `Instant`, `List`, `Map` and `Runtime` live there -- and taking a
# `Clock` parameter is the shape code-structure.md rule 4 REQUIRES. So the
# clock and the subprocess are a HAND-NAMED list, exactly as complete as its
# enumeration and no more.
#
# ⚠️ ONE MODULE AND TWO FILES ARE EXEMPT, ALL THREE NAMED RATHER THAN DERIVED.
#
# The two files are the composition root's edge: `server/.../Main.java` reaches
# for the real clock and `server/.../ConfigFile.java` reads the settings file.
# That is what a composition root IS -- the I/O and the clock become real in
# exactly ONE place -- and it is the reason eight milestones of tests can move
# time at all.
#
# ⚠️ FILES AND NOT THE WHOLE `server` MODULE, which is what M8's SPEC design
# section proposed ("the exemption list grows by exactly one module"). Naming
# two files is STRICTLY NARROWER: `Assembly`, `StoreFactory`, `FrontDoor`,
# `IngesterNode`, `ServerConfig`, `ServerProperties` and `StoreConfig` stay
# under this gate, and they are where the graph is actually built. A module-wide
# exemption would have let any of them open a socket or read a clock, which is
# precisely the drift a composition root is supposed to make visible.
#
# ⚠️ AND IT IS AN EXACT PATH MATCH, anchored at both ends. A prefix match on
# `server/` would exempt the module after all; a bare basename would exempt any
# `Main.java` a future module grows.
#
# ⚠️ ONE MODULE IS EXEMPT, NAMED RATHER THAN DERIVED FROM `implements`. The
# obvious derivation -- a file implementing one of the five seams may do I/O --
# is wrong, and MEASURED: ELEVEN `src/main` files match `implements
# .*(BinStore|Sequencer|SubscriptionTransport|Membership|Clock)` against this
# module's TWO, so that carve-out is five times wider and takes
# `LocalSequencer`, `FleetSequencer` and `BatchingSequencer` with it -- the
# files holding the commit protocol. `binstore-backends` is the object-store
# adapter, the one module whose JOB is the I/O everything else takes as a seam.
#
# ⚠️ COMMENTS, STRING LITERALS, CHAR LITERALS AND TEXT BLOCKS ARE STRIPPED
# BEFORE MATCHING. This tree's comments quote code constantly, so a raw grep
# would red on a javadoc saying "never call this", and the pressure would be to
# weaken the gate rather than the comment.
#
# ⚠️ WHAT IT REPORTS IS THAT A FILE NAMED NONE OF THE LISTED ENTRIES -- not
# that the file takes a seam for everything. `io_seam_scan.py` states the rest
# of what this cannot see.
source "$(dirname "$0")/lib.sh"
cd "$ROOT"
[ "${1:-}" = "--full" ] && GATE_SCOPE=full
hdr "check-io-seam"

# ⚠️ `|| true` BECAUSE lib.sh SETS pipefail AND `grep -v` EXITS 1 ON NO MATCH.
# Without it a commit touching no `src/main` file at all -- which is most of
# them, and was this gate's own commit -- died as "the scanner failed" while the
# scanner had run perfectly and found nothing to read.
FOUND=$(scoped_files '*/src/main/java/io/github/huyz0/os/biningester/*.java' \
  | { grep -v '^binstore-backends/' || true; } \
  | { grep -vxF -e 'server/src/main/java/io/github/huyz0/os/biningester/server/Main.java' \
                -e 'server/src/main/java/io/github/huyz0/os/biningester/server/ConfigFile.java' || true; } \
  | python3 scripts/io_seam_scan.py)
RC=$?
[ "$RC" -eq 0 ] || { fail "the scanner failed"; echo "$FOUND"; finish; }

COUNT=$(printf '%s' "$FOUND" | head -1)
VIOLATIONS=$(printf '%s' "$FOUND" | tail -n +2)
if [ -n "$VIOLATIONS" ]; then
  while IFS= read -r line; do [ -n "$line" ] && fail "$line"; done <<< "$VIOLATIONS"
  echo "         Business logic takes a seam. The five are BinStore, Clock,"
  echo "         Sequencer, SubscriptionTransport and Membership"
  echo "         (code-structure.md rule 4). If this file IS an adapter, it"
  echo "         belongs in binstore-backends -- which is a module move and an"
  echo "         argument, not a line added to this gate's exempt list."
  finish
fi
BANNED_COUNT=$(python3 -c 'import sys; sys.path.insert(0, "scripts"); import io_seam_scan; print(len(io_seam_scan.BAN_ENTRIES))')
ok "$COUNT business-logic file(s) name none of the $BANNED_COUNT banned constructs$(scope_note)"
finish
