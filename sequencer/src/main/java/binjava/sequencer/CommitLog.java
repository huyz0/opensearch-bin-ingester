// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import binjava.binstore.BinStore;
import binjava.binstore.Body;
import binjava.binstore.ListPage;
import binjava.binstore.ObjectStat;
import binjava.format.ChainEntry;
import binjava.format.CommitDelta;
import binjava.format.Continue;
import binjava.format.Seal;
import binjava.format.RunCommit;
import binjava.format.RunKey;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The commit log v0 (M1.10): a chain of sequentially numbered deltas, each
 * written with {@code putIfAbsent}.
 *
 * <p>⚠️ THE STORE IS THE ONLY COORDINATOR (ADR-0002). Two writers may both try
 * slot N; the store decides, the loser re-reads and retries at N+1. Invariant I1
 * — no seq is ever written twice — holds with no lock, no lease and no consensus
 * round, and that is the entire reason this system can be cheap.
 *
 * <p>⚠️ OFFSETS ARE ASSIGNED HERE, NOT IN THE SEGMENT (ADR-0001). The segment is
 * already durable when this runs, so an abandoned PUT leaves no hole: it is
 * simply never committed and is garbage-collected later. Pre-assigning offsets
 * before the PUT would create holes that stall every consumer of the partition.
 */
public final class CommitLog {

    private final BinStore store;
    private final String prefix;
    private final long epoch;
    private final Map<RunKey, Long> nextOffsets = new HashMap<>();
    private long nextSequence;
    /**
     * The SEAL this chain ended at, if recovery reached one.
     *
     * <p>⚠️ NOT reset by {@link #recover()}, unlike the two fields above, and
     * deliberately: a chain is sealed permanently, so this only ever goes from
     * null to set. A reset would be a line no test could ever falsify.
     */
    private Seal sealedAt;

    /**
     * The chain for one term of leadership.
     *
     * <p>⚠️ THE EPOCH IS IN THE PATH, and that is what makes fencing not depend
     * on catching a write in time (ADR-0002). A fenced leader's in-flight PUT
     * lands under its OWN epoch, where readers of the new epoch never look — so
     * it does not have to be stopped before it writes, only before anyone
     * believes it.
     *
     * <p>⚠️ THE EPOCH IS ALWAYS PASSED, never defaulted. M4.6f deleted a two-arg
     * constructor that supplied 0, because the chain it opened is a FORK: once a
     * leader writes at epoch 1, a reader of that epoch never lists epoch 0's
     * prefix, so records committed there are acked and never become visible, and
     * the leader re-issues offsets that chain already assigned. Nor can such a
     * chain be SEALED — the seal depends on a losing writer treating its loss as
     * proof it is fenced, and {@code commit} does the opposite by construction,
     * folding the winner's offsets in and retrying at the next sequence forever.
     * There is no leader there to fence. M4.6d had already removed the last
     * production caller; what remained was a default the next wiring commit
     * could reach by accident, which is rung 1 of gate-design left undone.
     *
     * <p>⚠️ EPOCH 0 IS STILL A LEGAL ARGUMENT, and reserved for "no lease".
     * {@code LeaseManager} starts its FIRST term at 1 so no leased chain can
     * collide with the historical one (M4.4b) — that reservation is what keeps
     * I3 true for the first term, since a first leader also at epoch 0 would
     * write a byte-identical chain and "readers of the new epoch never look
     * there" would be void. {@code CONTINUE} likewise uses {@code prevEpoch = 0}
     * for "no predecessor". What M4.6f removed is reaching epoch 0 by OMISSION,
     * not the value itself.
     */
    public CommitLog(BinStore store, String prefix, long epoch) {
        this.store = Objects.requireNonNull(store, "store");
        this.prefix = Objects.requireNonNull(prefix, "prefix");
        if (epoch < 0) {
            // ⚠️ Epochs count up from 0. A negative one could only come from a
            // corrupt lease, and it would order BEFORE every real epoch.
            throw new IllegalArgumentException("epoch is never negative: " + epoch);
        }
        this.epoch = epoch;
    }

    /**
     * ⚠️ Slot 0 because ADR-0007 fixes S = 1 — but the slot stays IN THE KEY so
     * raising it is configuration, not a key-grammar change. M1 wrote slot 0 /
     * epoch 0 for exactly this reason, and M4.4 is that promise being kept:
     * the dimension was always there, and this only fills it in.
     *
     * <p>⚠️ The epoch is zero-padded to 16 hex digits, like the sequence beside
     * it, so LEXICOGRAPHIC ORDER IS NUMERIC ORDER. That matters because
     * ADR-0022 removed {@code lastModifiedMillis}, leaving key order as the
     * only ordering a reader has; unpadded, epoch 10 would sort before epoch 9.
     */
    String keyFor(long sequence) {
        // ⚠️ Locale.ROOT: an object key is a wire value and must not depend on
        // the process's locale.
        return String.format(java.util.Locale.ROOT, "%s%016x.delta", logPrefix(), sequence);
    }

    String logPrefix() {
        return String.format(java.util.Locale.ROOT, "%s/ctl/log/0/%016x/", prefix, epoch);
    }

    /** Which term of leadership this chain belongs to. */
    public long epoch() {
        return epoch;
    }

    /**
     * Rebuilds the offset state by replaying the log.
     *
     * <p>⚠️ A LIST, and R2 permits it because this is RECOVERY. It runs once at
     * startup, never on a hot path.
     */
    public void recover() throws IOException {
        ChainReplay.Result r = ChainReplay.replay(store, prefix, epoch);
        nextOffsets.clear();
        nextOffsets.putAll(r.offsets());
        nextSequence = r.nextSequence();
        if (r.seal() != null) {
            sealedAt = r.seal();
        }
    }

    /**
     * Finds where this chain ENDS without reading what it holds.
     *
     * <p>⚠️ FOR A WRITER THAT WILL ONLY SEAL. A taking-over leader needs the
     * predecessor's last slot and nothing else; {@link #recover} would spend one
     * GET per delta of that whole term and then drop every offset it built.
     * Measured on a 51-entry chain: 52 GETs, all discarded — a failover cost
     * growing without bound in the PREVIOUS term's length, on the path whose
     * entire purpose is restoring sequencing quickly.
     *
     * <p>⚠️ The LIST alone gives the answer because {@link #keyFor} zero-pads to
     * 16 hex digits precisely so lexicographic order IS numeric order, so the
     * last key is the highest sequence. Only that one entry is read, to learn
     * whether the chain is already sealed.
     *
     * <p>⚠️ THIS LOG MUST NOT THEN COMMIT. It deliberately leaves {@code
     * nextOffsets} empty, so a commit through it would reassign offsets from 0
     * — which is I2. It is a boundary probe, not a recovery.
     */
    public void recoverChainEnd() throws IOException {
        ChainReplay.Result r = ChainReplay.chainEnd(store, prefix, epoch);
        nextSequence = r.nextSequence();
        if (r.seal() != null) {
            sealedAt = r.seal();
        }
    }

    private ChainEntry readEntry(String key) throws IOException {
        try (InputStream in = store.get(key)) {
            return ChainEntry.decode(in.readAllBytes());
        }
    }

    /**
     * Folds one chain entry into this reader's state.
     *
     * <p>⚠️ EXHAUSTIVE BY COMPILATION (M4.5). {@link ChainEntry} is sealed, so
     * a fourth shape cannot be added without every switch like this one failing
     * to compile — which is the point of the sealed interface rather than a
     * kind byte and a default branch that silently ignores what it does not
     * know.
     *
     * <p>⚠️ A {@code SEAL} and a {@code CONTINUE} carry no runs, so they move
     * no offsets — but they DO consume a sequence number, deliberately, and
     * {@code nextSequence} must advance past them or the next commit would race
     * for a slot that is already taken and spin.
     */
    private void apply(ChainEntry entry) {
        switch (entry) {
            case CommitDelta delta -> ChainReplay.fold(delta, nextOffsets);
            case Seal ignored -> { }
            case Continue ignored -> { }
        }
        nextSequence = Math.max(nextSequence, entry.sequence() + 1);
    }

    /** Merges in whatever this chain inherits from the slot its CONTINUE names. */
    private void crossFrom(long prevEpoch, long prevSeq) throws IOException {
        ChainReplay.inherited(store, prefix, prevEpoch, prevSeq)
                .forEach((key, next) -> nextOffsets.merge(key, next, Math::max));
    }

    /** The offset the next record of this stream will get. */
    public long nextOffset(RunKey key) {
        return nextOffsets.getOrDefault(key, 0L);
    }

    public long nextSequence() {
        return nextSequence;
    }

    /**
     * Commits one segment, assigning offsets to each of its runs.
     *
     * <p>⚠️ RETRIES ON A LOST RACE rather than failing. Losing slot N is normal:
     * another writer got there first, so this one re-reads what landed, folds it
     * into its own offset state and tries N+1. Treating a lost race as an error
     * would turn ordinary contention into an outage.
     *
     * @return the delta as committed, with the offsets that were actually assigned
     */
    public CommitDelta commit(String segmentKey, Map<RunKey, Integer> recordCounts)
            throws IOException {
        if (recordCounts.isEmpty()) {
            throw new IllegalArgumentException("a commit with no runs commits nothing");
        }
        if (sealedAt != null) {
            // ⚠️ The half `commit`'s lost-race branch below cannot reach. That
            // branch needs a race to lose; this writer recovered a sealed chain
            // and would win an empty slot beyond the barrier without ever
            // racing anyone.
            throw fenced(sealedAt);
        }
        while (true) {
            List<RunCommit> runs = new ArrayList<>(recordCounts.size());
            // ⚠️ Sorted, so a delta's runs are in the same order the segment's
            // directory uses and a replay is deterministic.
            recordCounts.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(e -> runs.add(
                            new RunCommit(e.getKey(), e.getValue(), nextOffset(e.getKey()))));
            CommitDelta delta = new CommitDelta(nextSequence, segmentKey, runs);
            Optional<binjava.binstore.Version> written = store.putIfAbsent(keyFor(nextSequence),
                    new Body(delta.encode().length, () -> new ByteArrayInputStream(delta.encode())));
            if (written.isPresent()) {
                apply(delta);
                return delta;
            }
            // ⚠️ Somebody else took this slot. Read what they wrote, fold their
            // offsets in, and try the next one -- which is why offsets must be
            // recomputed inside the loop rather than before it.
            ChainEntry taken = readEntry(keyFor(nextSequence));
            if (taken instanceof Seal seal) {
                // ⚠️ FAIL CLOSED. Losing a slot to a SEAL is not ordinary
                // contention -- it is proof this writer is fenced. Folding the
                // seal in and retrying at the next slot would write PAST the
                // barrier and acknowledge it, which is invariant I5 ("no
                // acknowledged commit exists beyond a SEAL in its own chain"),
                // and recovery would then apply a discarded suffix, which is
                // I3.
                // ⚠️ Teaching this reader to UNDERSTAND a seal is what made
                // that reachable: before M4.5 an unknown version threw here, so
                // the writer failed closed by accident. A read side that ships
                // first must be no less safe than the one it replaces.
                // ⚠️ Stopping is only the FENCED branch. A new leader holding a
                // valid lease must redrive to N+2 instead of stopping, or it
                // surrenders sequencing cluster-wide -- that split needs the
                // lease, so it is M4.6's. This refusal is the safe half, and
                // the half that must not wait.
                throw fenced(seal);
            }
            apply(taken);
        }
    }

    /**
     * Opens this chain with a CONTINUE at slot 0, naming the chain it follows.
     *
     * <p>⚠️ IDEMPOTENT ON PURPOSE. A leader that crashed between acquiring its
     * lease and opening its chain restarts at the SAME epoch only if it also
     * re-won the same term, but a caller may legitimately re-run this after a
     * partial start; losing slot 0 to an identical CONTINUE is success, not
     * contention. Losing it to anything else means this epoch's chain was opened
     * by somebody else, which cannot happen while the lease is held and is
     * therefore reported rather than absorbed.
     *
     * <p>⚠️ {@code prevEpoch == 0} is not a placeholder. Epoch 0 is RESERVED for
     * the unleased chain (M4.4b), so a first leader naming it says truthfully
     * that there was no predecessor chain — which M4.5 chose over an explicit
     * absent-marker, because a marker can be forgotten and an epoch cannot.
     */
    public Continue open(long prevEpoch, long prevSeq) throws IOException {
        Continue opening = new Continue(0, prevEpoch, prevSeq);
        byte[] bytes = opening.encode();
        Optional<binjava.binstore.Version> written = store.putIfAbsent(keyFor(0),
                new Body(bytes.length, () -> new ByteArrayInputStream(bytes)));
        if (written.isPresent()) {
            apply(opening);
            crossFrom(prevEpoch, prevSeq);
            return opening;
        }
        ChainEntry taken = readEntry(keyFor(0));
        if (opening.equals(taken)) {
            apply(opening);
            crossFrom(prevEpoch, prevSeq);
            return opening;
        }
        throw new IOException("chain at epoch " + epoch + " was opened by something else: "
                + taken + "; this node holds the lease and must not share the chain");
    }

    /**
     * Closes this chain with a SEAL, naming the epoch it continues at.
     *
     * <p>⚠️ THE OTHER LOSING BRANCH, and it is the OPPOSITE of {@link #commit}'s.
     * A fenced old leader that loses a slot stops, because anything it writes is
     * in a discarded suffix (I3). A NEW leader holding a valid lease REDRIVES:
     * it is racing the old leader's still-in-flight commits, and stopping would
     * surrender sequencing cluster-wide while holding the lease — a
     * self-inflicted outage. The caller is the one that knows which it is,
     * because the lease lives there.
     *
     * <p>⚠️ CONSECUTIVENESS IS LOAD-BEARING. A fenced leader must claim {@code
     * N+1} before {@code N+2}, so it necessarily collides with this seal. The
     * protocol never asks whether a slot is EMPTY — it claims it, and lets the
     * store adjudicate. A timestamp- or randomly-keyed log would not have that
     * property.
     *
     * @param maxRedrives the caller's budget, and it is NOT the old leader's
     *     in-flight depth. A fenced leader does not learn it is fenced until a
     *     write loses or its renew fails, so it keeps ORIGINATING commits for up
     *     to one renew interval; a budget sized to in-flight depth gives up
     *     early and causes the outage this branch prevents. The bound belongs to
     *     the caller because it derives from the renew interval, which this
     *     class does not know. Termination does not rest on it — the old leader
     *     stops on its own first lost race — so it is a safety net against a
     *     store that never lets this writer win, never the argument.
     */
    public Seal seal(long continuedAt, int maxRedrives) throws IOException {
        if (sealedAt != null) {
            // ⚠️ THE SAME GUARD `commit` HAS, and it was missing here — the
            // M4.6a hole one method over. A fresh CommitLog that recovers an
            // already-sealed chain proposes at sealSeq+1, WINS uncontended, and
            // writes a SECOND seal past the barrier. Measured: "first seal
            // seq=1, second seal seq=2". The adopt branch below states the
            // principle this contradicted — the chain is closed, which is what
            // was asked for.
            // ⚠️ Two consequences, both downstream: the CONTINUE would name a
            // slot past the real boundary, which is exactly what M4.6e's
            // crossing reader follows; and `open`'s advertised idempotence
            // after a partial start breaks, because the retry's CONTINUE would
            // not equal the one this node wrote.
            return sealedAt;
        }
        for (int redrives = 0; ; redrives++) {
            Seal proposed = new Seal(nextSequence, continuedAt);
            byte[] bytes = proposed.encode();
            Optional<binjava.binstore.Version> written = store.putIfAbsent(keyFor(nextSequence),
                    new Body(bytes.length, () -> new ByteArrayInputStream(bytes)));
            if (written.isPresent()) {
                apply(proposed);
                // ⚠️ Binds the writer to its OWN seal. Otherwise the node that
                // closed the chain is the one node still able to write past the
                // barrier it just installed, which is I5 by the shortest route
                // there is.
                sealedAt = proposed;
                return proposed;
            }
            ChainEntry taken = readEntry(keyFor(nextSequence));
            if (taken instanceof Seal existing) {
                // ⚠️ NOT a failure. The chain is closed, which is what was
                // asked for; failing here would abandon a takeover that had
                // already succeeded. Adopt THEIR seal, including their
                // continuation epoch — ours was only a proposal.
                apply(existing);
                sealedAt = existing;
                return existing;
            }
            if (redrives >= maxRedrives) {
                throw new IOException("seal did not converge at seq " + nextSequence
                        + " within " + maxRedrives + " redrive(s); a writer is still"
                        + " originating commits on a chain this node is trying to close");
            }
            // ⚠️ RE-READ AND APPLY, never blind-retry: the offsets that commit
            // assigned are now part of this chain's history, and a seal written
            // without them would leave a reader's state disagreeing with the log.
            apply(taken);
        }
    }

    /**
     * ⚠️ Stopping is only the FENCED branch. A new leader holding a valid lease
     * must redrive to N+2 rather than stop, or it surrenders sequencing
     * cluster-wide; that split needs the lease and is not this task's.
     */
    private static IOException fenced(Seal seal) {
        return new IOException("chain sealed at seq " + seal.sequence()
                + ", continued at epoch " + seal.continuedAt()
                + "; this writer is fenced and must stop");
    }
}
