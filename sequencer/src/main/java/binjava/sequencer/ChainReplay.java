// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import binjava.binstore.BinStore;
import binjava.binstore.ListPage;
import binjava.binstore.ObjectStat;
import binjava.format.ChainEntry;
import binjava.format.Checkpoint;
import binjava.format.CommitDelta;
import binjava.format.Continue;
import binjava.format.RunCommit;
import binjava.format.RunKey;
import binjava.format.Seal;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * READING a chain, including across the boundary into whatever it continues from.
 *
 * <p>⚠️ Split out of {@link CommitLog} when M4.6e's crossing pushed that file
 * past the 500-line limit. code-structure.md rule 1: split it, do not raise the
 * limit. The seam is real rather than convenient — reading a chain and WRITING
 * to one are different jobs, and only the writer needs a lease, a redrive budget
 * or a barrier to respect.
 *
 * <p>⚠️ ADDRESSING COMES FROM {@link LogKeys}, never re-derived here. The key
 * grammar zero-pads epoch and sequence to 16 hex digits so lexicographic order
 * is numeric order (ADR-0022 left key order as the only ordering a reader has),
 * and a second copy of that formatting would be a wire-format duplication
 * waiting to drift.
 *
 * <p>⚠️ ONE SITE STILL GOES THROUGH {@link CommitLog} — {@link #firstEntry}
 * builds one to address slot 0 — and it is a LEFTOVER of the split, not a
 * guard. An earlier draft claimed the constructor's {@code epoch < 0} check was
 * why it stayed; that was measured FALSE. Both callers already establish
 * {@code chainEpoch >= 1} before reaching it, so the throw is unreachable here
 * and a bare {@link LogKeys} leaves the suite green.
 */
final class ChainReplay {

    /**
     * One hop of the ancestry: which chain, where to start, and how far in.
     *
     * <p>⚠️ {@code from} IS WHAT M4.9 ADDED. It is the first sequence NOT
     * already covered by a checkpoint, so the LIST starts after the entries
     * below it and they are never GET at all — bounding the LIST, the GETs and
     * the stats together, which is the point: the measurement this replaces had
     * all three growing per ancestor.
     */
    private record Hop(long epoch, long from, long upTo) {
    }

    /** What a replay learned: offsets, this chain's next free slot, its seal. */
    record Result(Map<RunKey, Long> offsets, long nextSequence, Seal seal) {
    }

    private final BinStore store;
    private final String prefix;
    private final long ownEpoch;
    private final Map<RunKey, Long> offsets = new HashMap<>();
    private long nextSequence;
    private Seal seal;

    private ChainReplay(BinStore store, String prefix, long ownEpoch) {
        this.store = store;
        this.prefix = prefix;
        this.ownEpoch = ownEpoch;
    }

    /** Full recovery of {@code epoch}, crossing into whatever it continues from. */
    static Result replay(BinStore store, String prefix, long epoch) throws IOException {
        ChainReplay r = new ChainReplay(store, prefix, epoch);
        r.replayAncestry(epoch, Long.MAX_VALUE);
        return new Result(r.offsets, r.nextSequence, r.seal);
    }

    /**
     * The offsets a chain INHERITS from the slot a CONTINUE names.
     *
     * <p>⚠️ Needed because a NEW leader recovers its chain BEFORE opening it —
     * the chain is empty at that moment, so there is no CONTINUE to follow, and
     * without this it would start every stream again at 0 despite all the
     * crossing machinery existing.
     */
    static Map<RunKey, Long> inherited(BinStore store, String prefix, long prevEpoch,
            long prevSeq) throws IOException {
        ChainReplay r = new ChainReplay(store, prefix, Long.MIN_VALUE);
        if (prevEpoch >= 1) {
            r.replayAncestry(prevEpoch, prevSeq);
        }
        return r.offsets;
    }

    /**
     * Replays {@code epoch} and every chain it continues from, OLDEST FIRST.
     *
     * <p>⚠️ ITERATIVE, not recursive, and that is a correctness property rather
     * than a style choice. Depth here is bounded by the EPOCH NUMBER — every
     * restart, rolling deploy, TTL expiry and failed {@code start} burns one,
     * and nothing ever compacts them — so a long-lived cluster legitimately has
     * a very deep ancestry. Recursion made that a {@code StackOverflowError}
     * unwinding through a public API on the startup path.
     *
     * <p>⚠️ TERMINATION comes from the epoch STRICTLY DECREASING, which is
     * checked below, not from a depth cap. An earlier draft capped depth at
     * 1000 and called anything deeper corrupt; that was FALSE and measured so —
     * at 1002 legitimate terms every node's {@code start} threw, the lease was
     * released, the next node minted E+1 and hit the same wall, so the cluster
     * could never elect a sequencer again while being told its correct history
     * was corrupt bytes.
     */
    private void replayAncestry(long epoch, long upTo) throws IOException {
        List<Hop> hops = new ArrayList<>();
        // ⚠️ THE OWN CHAIN IS ALWAYS REPLAYED, before any question of ancestry.
        // Gating this on `epoch >= 1` made an epoch-0 log — the reserved
        // unleased chain, which every `CommitLog(store, prefix)` uses — recover
        // NOTHING, because 0 fails the ancestry test that has no business
        // applying to the chain itself.
        // ⚠️ THE FIRST HOP IS CHECKED TOO, and omitting it made the bound do
        // nothing on the path that matters. `LocalSequencer.start` calls
        // `recover()` BEFORE `open()`, so the new chain is empty and that walk
        // terminates immediately; the crossing that actually reads the ancestry
        // is `inherited()`, which STARTS at the predecessor. Checking only the
        // epochs reached by following a CONTINUE skipped exactly that one.
        if (crossFromCheckpoint(hops, epoch, upTo)) {
            applyChain(hops.get(0));
            return;
        }
        hops.add(new Hop(epoch, 0, upTo));
        long chainEpoch = epoch;
        long limit = upTo;
        boolean atOrigin = true;
        while (chainEpoch >= 1) {
            ChainEntry first = firstEntry(chainEpoch);
            if (first instanceof Continue opening) {
                // ⚠️ THE ONLY TERMINATION ARGUMENT THERE IS. Nothing else stops
                // a CONTINUE naming its own epoch, or a higher one: measured, a
                // two-object cycle recursed until the stack went. A chain may
                // only ever continue from a chain that was sealed BEFORE it, and
                // epochs advance by exactly one, so a non-decreasing link is
                // corrupt bytes — the claim the old depth cap made wrongly, made
                // here where it is actually true.
                if (opening.prevEpoch() >= chainEpoch) {
                    throw new IOException("chain at epoch " + chainEpoch
                            + " continues from epoch " + opening.prevEpoch()
                            + ", which is not LOWER; a chain may only continue from"
                            + " one sealed before it, so this is corrupt");
                }
                limit = opening.prevSeq();
                chainEpoch = opening.prevEpoch();
                // ⚠️ A CHECKPOINT TERMINATES THE WALK, and that is the whole of
                // M4.9's bound. Its offsets are CUMULATIVE — the leader that
                // wrote it had already recovered everything IT inherited — so
                // reaching one means nothing older need be read. Without this
                // the crossing is transitive and a takeover costs O(all
                // commit-log entries ever written), which is the shape
                // M4.6e's row records. Measured on THIS harness at 1/2/4/8/16
                // prior terms: 29/46/80/148/284 requests.
                if (chainEpoch >= 1 && crossFromCheckpoint(hops, chainEpoch, limit)) {
                    atOrigin = false;
                    break;
                }
                // ⚠️ EPOCH 0 IS NOT FOLLOWED. It is the RESERVED unleased chain
                // (M4.4b), which a first leader names truthfully to say it had
                // no predecessor. Replaying it would read the unleased chain
                // into a leased one's history — the fork M4.6d removes — so the
                // hop is not added and the loop below ends.
                if (chainEpoch >= 1) {
                    hops.add(new Hop(chainEpoch, 0, limit));
                }
                atOrigin = false;
            } else if (neverOpened(first, atOrigin)) {
                // ⚠️ SEALED BEFORE IT WAS EVER OPENED. A leader that acquires an
                // epoch and dies before writing its CONTINUE leaves exactly this:
                // a chain with a seal at slot 0 and no link. Measured, and
                // reachable through THIS commit's own error path — `start`
                // releases the lease when the opening PUT fails — where it
                // silently discarded every offset assigned before it.
                // ⚠️ The link has to be reconstructed by arithmetic because the
                // CONTINUE that would have carried it was never written. That is
                // sound for exactly the reason the CONTINUE normally exists to
                // avoid needing: epochs advance by exactly one per acquisition.
                limit = Long.MAX_VALUE;
                chainEpoch = chainEpoch - 1;
                atOrigin = false;
                // ⚠️ THIS BRANCH IS PROBED TOO. Without it, the first genuinely
                // opened chain reached across a run of BURNED epochs is replayed
                // in full with its checkpoint sitting unread — unreachable today
                // only because `LocalSequencer.start` resolves burned epochs
                // through `firstInheritableAncestor` first, which is an ordering
                // invariant held in ANOTHER CLASS. `neverOpened`'s own javadoc
                // records that caveat rather than assuming it; so does this.
                if (chainEpoch >= 1 && crossFromCheckpoint(hops, chainEpoch, limit)) {
                    break;
                }
                if (chainEpoch >= 1) {
                    hops.add(new Hop(chainEpoch, 0, limit));
                }
            } else {
                break;
            }
        }
        // Oldest first, which reads as the history did. ⚠️ NOT load-bearing,
        // and said so because an earlier comment claimed it was: `fold` uses
        // `Math::max`, and `nextSequence` and `seal` are both gated on
        // `ownEpoch`, so reversing this order is an EQUIVALENT mutant. The
        // rewind protection is the max, not the ordering.
        for (int i = hops.size() - 1; i >= 0; i--) {
            applyChain(hops.get(i));
        }
    }

    /**
     * Seeds this replay from {@code chainEpoch}'s newest checkpoint, if it has
     * one that is SAFE to use, and reports whether the walk can stop there.
     *
     * <p>⚠️ SAFE MEANS AT OR BEFORE THE BARRIER. A checkpoint at sequence S
     * reflects the chain through S-1, so it may only be used when
     * {@code S - 1 <= upTo}. A leader that was fenced goes on writing for a
     * while, and its checkpoints can cover entries PAST the slot its successor's
     * CONTINUE named — applying one of those would fold a discarded suffix into
     * the history, which is I3. When the newest is too new there is no older one
     * to fall back to (the pointer holds one), so the walk continues unbounded
     * and correct.
     *
     * <p>⚠️ THE OFFSETS ARE MERGED, not assigned, for the reason {@link #fold}
     * gives: a crossing merges an older chain's offsets into a newer one's and
     * taking the later value unconditionally would rewind the stream (I2).
     */
    private boolean crossFromCheckpoint(List<Hop> hops, long chainEpoch, long upTo)
            throws IOException {
        java.util.Optional<Checkpoint> found = CheckpointCursor.newest(store, prefix, chainEpoch);
        if (found.isEmpty()) {
            return false;
        }
        Checkpoint checkpoint = found.get();
        if (checkpoint.sequence() - 1 > upTo) {
            return false;
        }
        checkpoint.streams().forEach(
                (key, at) -> offsets.merge(key, at.nextOffset(), Math::max));
        if (chainEpoch == ownEpoch) {
            // ⚠️ `nextSequence` IS DERIVED FROM THE ENTRIES READ, so skipping
            // the ones a checkpoint covers would leave it short — at zero if the
            // checkpoint covers the whole chain — and the next commit would
            // re-race slots that are already taken. The checkpoint's own
            // sequence IS the writer's `nextSequence` at the moment it was
            // written, so it restores exactly what the skipped entries would.
            nextSequence = Math.max(nextSequence, checkpoint.sequence());
        }
        hops.add(new Hop(chainEpoch, checkpoint.sequence(), upTo));
        return true;
    }

    /**
     * Whether this chain was SEALED OR ABANDONED before it was ever opened.
     *
     * <p>⚠️ TWO SHAPES, ONE BRANCH, and an earlier fix handled only the first.
     * A leader that acquires an epoch and dies before writing its CONTINUE
     * leaves either a {@code Seal} at slot 0 — if a successor got as far as
     * fencing it — or NOTHING AT ALL, if the successor died before that too.
     * Neither carries a link, so the same arithmetic recovers both.
     *
     * <p>⚠️ Measured: two consecutive takeovers each failing at or before their
     * first PUT — a store outage spanning a failover, which is the most
     * correlated failure this path meets — left an empty chain mid-ancestry,
     * broke the walk, and reassigned offset 0. That is I2.
     *
     * <p>⚠️ NOT AT THE ORIGIN, where empty means something else entirely: a new
     * leader recovers its own chain BEFORE opening it, so empty is the normal
     * state at that moment and {@code open} does the inheriting explicitly.
     * Crossing here too would read the predecessor twice on every takeover.
     *
     * <p>⚠️ THAT ARGUMENT COVERS ONE OF THE TWO CALLERS. For {@link #replay} the
     * origin is this node's OWN chain, where empty is normal. For
     * {@link #inherited} the origin is the PREDECESSOR, where empty is not
     * normal at all — and an empty one there yields offset 0 silently, with no
     * exception and no failing test.
     *
     * <p>It is unreachable today only because {@code LocalSequencer.start}
     * always seals the predecessor before opening, and every route through
     * {@code seal} leaves at least a {@code Seal} at slot 0. That is an
     * ordering invariant held in ANOTHER CLASS, so it is recorded here rather
     * than assumed: any future path that opens without sealing first — M4.6d's
     * rerouting or M5's forwarding are the candidates — gets silent
     * reassignment from 0.
     */
    private static boolean neverOpened(ChainEntry first, boolean atOrigin) {
        if (first == null) {
            return !atOrigin;
        }
        return first instanceof Seal sealed && sealed.sequence() == 0;
    }

    /**
     * A chain's SLOT 0, or null if the chain is empty.
     *
     * <p>⚠️ ADDRESSED DIRECTLY, never LISTed. A chain's CONTINUE is always at
     * slot 0 — that is what {@code open} means — so the ancestry is discoverable
     * with a stat and a get. A LIST here would add one request per chain to
     * every recovery, and two existing tests pin the LIST count precisely
     * because paging is where this reader's cost lives.
     */
    private ChainEntry firstEntry(long chainEpoch) throws IOException {
        return firstEntry(store, prefix, chainEpoch);
    }

    private static ChainEntry firstEntry(BinStore store, String prefix, long chainEpoch)
            throws IOException {
        String slotZero = new CommitLog(store, prefix, chainEpoch).keyFor(0);
        if (store.stat(slotZero).isEmpty()) {
            return null;
        }
        return read(store, slotZero);
    }

    /**
     * The nearest ancestor of {@code prevEpoch}, walking backward, that was
     * genuinely opened — carries a real {@link Continue} at slot 0, not a
     * chain burned before ever writing one. ADR-0029: this is the ancestor a
     * taking-over leader must SEAL before crossing into it, because it is the
     * one whose offsets {@link #inherited} will eventually read.
     *
     * <p>⚠️ NOT {@link #replayAncestry}. That walk follows every
     * {@code CONTINUE} transitively, because a full replay needs the whole
     * ancestry. This one stops at the FIRST real {@code CONTINUE} it meets —
     * ADR-0029's decision only needs the one hop back, because sealing it is
     * what stops its writer, and the existing transitive walk in
     * {@link #inherited} already reads everything beyond it once this method's
     * caller has sealed it and named it directly in the new chain's own
     * {@code CONTINUE}.
     *
     * <p>⚠️ SHARES {@link #neverOpened}'S ORIGIN CAVEAT. {@code prevEpoch}
     * itself is treated as the origin, so a {@code prevEpoch} that is
     * completely empty (no {@code CONTINUE}, no {@code Seal} — a store outage
     * spanning the acquisition that minted it, before anything was ever
     * written) is not walked past here, same as {@link #inherited}. That gap
     * is M4.23's, not this method's: closing it changes {@link #neverOpened}
     * for both callers at once, and this method is deliberately built on the
     * same primitive as {@link #inherited} rather than a divergent one.
     *
     * @return {@code prevEpoch} or a lower epoch, or {@code 0} when nothing
     *     needs sealing — mirrors the {@code prevEpoch >= 1} guard callers
     *     already use for "no predecessor to seal"
     */
    static long firstInheritableAncestor(BinStore store, String prefix, long prevEpoch)
            throws IOException {
        long chainEpoch = prevEpoch;
        boolean atOrigin = true;
        while (chainEpoch >= 1 && neverOpened(firstEntry(store, prefix, chainEpoch), atOrigin)) {
            chainEpoch--;
            atOrigin = false;
        }
        return chainEpoch;
    }


    private void applyChain(Hop hop) throws IOException {
        LogKeys keys = new LogKeys(prefix, hop.epoch());
        String logPrefix = keys.logPrefix();
        // ⚠️ START THE LIST AFTER THE LAST COVERED ENTRY, rather than filtering
        // after reading. `startAfter` is what keeps the LIST itself bounded —
        // the measurement this replaces had LISTs growing per ancestor too, and
        // skipping entries in the loop would leave that class untouched.
        String startAfter = hop.from() > 0 ? keys.keyFor(hop.from() - 1) : null;
        while (true) {
            ListPage page = store.list(logPrefix, startAfter, 1000);
            for (ObjectStat stat : page.objects()) {
                // ⚠️ NOT OURS, so not READ — the GET is skipped, not issued and
                // its failure swallowed. Swallowing would be I3 and would also
                // make recovery's cost grow with the checkpoints beside the
                // chain, which is the dimension its test pins.
                // ⚠️ `continue`, never `return`: a key that sorts between two
                // entries would otherwise TRUNCATE the chain and silently drop
                // every offset past it (I2).
                if (!keys.isEntryKey(stat.key())) {
                    continue;
                }
                ChainEntry entry = read(store, stat.key());
                // ⚠️ STOP AT THE SLOT THE CONTINUE NAMED, not at the chain's end.
                // Anything past it was written by a leader that had already been
                // fenced, so it is a discarded suffix and must not enter the
                // history (I3).
                if (entry.sequence() > hop.upTo()) {
                    return;
                }
                applyOffsets(entry);
                // ⚠️ OFFSETS CROSS, SEQUENCE NUMBERS DO NOT. `nextSequence` is
                // where THIS chain's next commit goes, so taking it from a
                // predecessor would send the successor's first delta past its
                // own empty slots and break the consecutiveness the seal
                // protocol depends on.
                if (hop.epoch() == ownEpoch) {
                    nextSequence = Math.max(nextSequence, entry.sequence() + 1);
                }
                if (entry instanceof Seal found) {
                    // ⚠️ ONLY THIS CHAIN'S SEAL BINDS THE WRITER. A seal in a
                    // PREDECESSOR is the boundary a crossing reader stops at,
                    // not a fence on the successor — a leader whose own chain is
                    // sealed is fenced, one whose ANCESTOR is sealed is normal.
                    if (hop.epoch() == ownEpoch) {
                        seal = found;
                    }
                    return;
                }
            }
            if (page.nextStartAfter().isEmpty()) {
                return;
            }
            startAfter = page.nextStartAfter().get();
        }
    }

    private void applyOffsets(ChainEntry entry) {
        fold(entry, offsets);
    }

    /**
     * ⚠️ THE ONE PLACE a delta becomes an offset. It lived here AND in
     * {@code CommitLog.apply} until review pointed out that the split's stated
     * principle — do not re-derive wire semantics in two places — had been
     * applied to the key grammar and not to this.
     *
     * <p>{@code Math::max} rather than assignment, because entries arrive in
     * ascending sequence within a chain but a CROSSING merges an older chain's
     * offsets into a newer one's; taking the later value unconditionally would
     * rewind the stream, which is I2.
     */
    static void fold(ChainEntry entry, Map<RunKey, Long> into) {
        if (entry instanceof CommitDelta delta) {
            // ⚠️ `allRuns`, deliberately: an offset is a STREAM fact, not a
            // segment fact, so this is the one reader that genuinely does not
            // care which object holds the records. Everything that DELIVERS
            // records pairs each run with its own segment instead — see
            // SubscriptionHub and ADR-0032.
            for (RunCommit run : delta.allRuns()) {
                into.merge(run.key(), run.lastOffset() + 1, Math::max);
            }
        }
    }

    /**
     * ⚠️ PACKAGE-PRIVATE, not private, because {@link ChainEnd} reads the one
     * entry it needs through the same decoder. Two copies of "GET it and
     * decode it" is how a reader stops agreeing with its sibling about what a
     * chain entry is.
     */
    static ChainEntry read(BinStore store, String key) throws IOException {
        try (InputStream in = store.get(key)) {
            return ChainEntry.decode(in.readAllBytes());
        }
    }
}
