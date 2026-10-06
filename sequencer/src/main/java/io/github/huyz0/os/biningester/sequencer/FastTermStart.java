// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.ObjectStat;
import io.github.huyz0.os.biningester.binstore.Version;
import io.github.huyz0.os.biningester.format.Roster;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;

/**
 * A new leader's walk, fences, roster and {@code LATEST} (ADR-0081 §5 steps
 * 2-3, §1, §3; M13.26b): every term has a roster, fast index or not, and a
 * leader runs this before it assigns any fast offset and before it admits any
 * default-path commit.
 *
 * <p>⚠️ IT READS {@code LATEST} AND WALKS BACK to the first closed roster or
 * the first term; every roster it passes that is not closed is UNCLOSED and is
 * fenced by this term with a {@code putIfMatch}, retried on a re-read when a
 * deposed leader wrote it meanwhile. ⚠️ A NEWER TERM ANYWHERE DEPOSES THIS ONE:
 * {@code LATEST}, a walked roster's epoch or its {@code fencedBy} above this
 * epoch, and this leader writes nothing more -- {@code LATEST} and every
 * {@code fencedBy} are only ever raised.
 *
 * <p>⚠️ {@code LATEST} IS WRITTEN BY {@code putIfMatch} against the version the
 * walk read (never blindly: a paused deposed leader would hide the newer term).
 * A refused write is re-read: this epoch means an earlier attempt landed; a
 * newer one deposes; an older one is a deposed leader's late write, and the
 * walk is made again from it -- fencing what it finds, rewriting this term's
 * predecessor and {@code notBefore}, and judging the graceful exemption over
 * every term found by every walk.
 *
 * <p>⚠️ NOT YET THE TAKEOVER: collecting, deciding, committing and closing the
 * unclosed terms is M13.33; this returns them, fenced. The one exception is a
 * term that recorded no fast index, which has no stream to decide:
 * {@link EmptyTermCloser} closes it right after this start (M13.27g), and
 * {@link FastTermOpening} runs the two together for every elected term.
 */
public final class FastTermStart {

    /** Bounds the retries of one write against a store that keeps refusing it. */
    static final int MAX_ATTEMPTS = 16;

    private final BinStore store;
    private final String prefix;

    public FastTermStart(BinStore store, String prefix) {
        this.store = Objects.requireNonNull(store, "store");
        this.prefix = Objects.requireNonNull(prefix, "prefix");
    }

    /** How a start ended. */
    public sealed interface Outcome permits Deposed, Started {
    }

    /** A newer term exists: this leader writes and assigns nothing more. */
    public record Deposed(long newer) implements Outcome {
    }

    /**
     * This term's roster and {@code LATEST} are written.
     *
     * @param own this term's roster as stored
     * @param unclosed every earlier unclosed term, newest first, as fenced
     * @param notBefore the wall-clock instant (ms) before which this leader
     *     assigns and decides nothing, 0 for none
     * @param handedOver §3's graceful exemption: the replaced lease's holder
     *     and every unclosed term's leader departed, so the lease's own waits
     *     do not apply -- {@code notBefore} still does
     */
    public record Started(Roster own, List<Roster> unclosed, long notBefore, boolean handedOver)
            implements Outcome {
        public Started {
            unclosed = List.copyOf(unclosed);
        }

        /** Whether this leader may assign or decide at {@code wallNow}. */
        public boolean mayAssign(long wallNow, boolean leaseWaitServed) {
            return wallNow >= notBefore && (handedOver || leaseWaitServed);
        }
    }

    private record Versioned(Roster roster, Version version) {
    }

    private record Latest(long epoch, Version version) {
    }

    /** Raised inside the walk when a newer term is found. */
    private static final class DeposedSignal extends Exception {
        private final long newer;

        DeposedSignal(long newer) {
            super(null, null, false, false);
            this.newer = newer;
        }
    }

    /**
     * Starts term {@code epoch} for {@code self}.
     *
     * @param walQuorums every fast index's {@code wal_quorum} now: the term
     *     record's first element
     * @param leaseNotBefore the lease's part of {@code notBefore} (the replaced
     *     lease's expiry plus the margin, {@link FastLeaseFence}), or
     *     {@link Long#MIN_VALUE} for none
     * @param replacedHolderUid the replaced lease's holder pod UID, empty when
     *     no lease was replaced or it named none
     * @throws IOException the store failed, a roster is missing or unreadable,
     *     or a write kept being refused -- nothing is decided, and the caller
     *     runs this again
     */
    public Outcome start(long epoch, Roster.Incarnation self, Map<UUID, Integer> walQuorums,
            long leaseNotBefore, Optional<String> replacedHolderUid) throws IOException {
        if (epoch < 1) {
            throw new IllegalArgumentException("a term's epoch is a lease's, at least 1: " + epoch);
        }
        Objects.requireNonNull(self, "self");
        Objects.requireNonNull(replacedHolderUid, "replacedHolderUid");
        long leasePart = Math.max(0, leaseNotBefore);
        long inherited = 0;
        boolean handedOver = replacedHolderUid.isPresent() && !replacedHolderUid.get().isEmpty();
        boolean holderFoundDeparted = false;
        try {
            for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
                Latest latest = readLatest();
                if (latest.epoch() > epoch) {
                    return new Deposed(latest.epoch());
                }
                List<Roster> walked = new ArrayList<>();
                List<Versioned> unclosed = new ArrayList<>();
                long newest = walk(latest.epoch(), epoch, self, walked, unclosed);
                List<Roster> fenced = new ArrayList<>();
                for (Versioned v : unclosed) {
                    fenced.add(fence(v, epoch));
                }
                // ⚠️ OVER EVERY WALK SO FAR (ADR-0081 §3; M13.22a review round
                // 3, P1): a deposed leader's late LATEST names a term whose
                // leader did not depart, and a second walk must not forget it.
                for (Roster r : fenced) {
                    inherited = Math.max(inherited, r.notBefore());
                    handedOver &= r.leaderDeparted();
                }
                for (Roster r : walked) {
                    if (replacedHolderUid.isPresent()
                            && r.leader().podUid().equals(replacedHolderUid.get())
                            && r.leaderDeparted()) {
                        holderFoundDeparted = true;
                    }
                }
                boolean exempt = handedOver && holderFoundDeparted;
                long notBefore = exempt ? inherited : Math.max(inherited, leasePart);
                Roster own = writeOwn(epoch, newest, self, walQuorums, notBefore);
                if (latest.epoch() == epoch || writeLatest(latest, epoch)) {
                    return new Started(own, fenced, own.notBefore(), exempt);
                }
            }
        } catch (DeposedSignal deposed) {
            return new Deposed(deposed.newer);
        }
        throw new IOException("LATEST kept moving under term " + epoch + " for "
                + MAX_ATTEMPTS + " attempts");
    }

    /**
     * Walks from {@code from} to the first closed roster or the first term,
     * filling {@code walked} (every roster read but this term's) and
     * {@code unclosed}; returns the newest epoch found other than this term's,
     * or -1.
     */
    private long walk(long from, long epoch, Roster.Incarnation self, List<Roster> walked,
            List<Versioned> unclosed) throws IOException, DeposedSignal {
        long newest = -1;
        long e = from;
        while (e != -1) {
            Versioned v = readRoster(e).orElseThrow(() -> new IOException(
                    "LATEST or a predecessor names a missing roster"));
            Roster r = v.roster();
            if (r.fencedBy() > epoch) {
                throw new DeposedSignal(r.fencedBy());
            }
            if (e == epoch) {
                // ⚠️ THIS TERM'S OWN, from an earlier attempt whose LATEST
                // write landed: walk on from its predecessor.
                if (!r.leader().equals(self)) {
                    throw new IOException("roster " + e + " names another leader");
                }
            } else {
                if (newest == -1) {
                    newest = e;
                }
                walked.add(r);
                if (r.closed()) {
                    break;
                }
                unclosed.add(v);
            }
            e = r.predecessor();
        }
        return newest;
    }

    private Roster fence(Versioned found, long epoch) throws IOException, DeposedSignal {
        Versioned v = found;
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
            Roster r = v.roster();
            if (r.fencedBy() > epoch) {
                throw new DeposedSignal(r.fencedBy());
            }
            if (r.fencedBy() == epoch) {
                return r;
            }
            Roster fenced = r.fencedBy(epoch);
            if (store.putIfMatch(Roster.key(prefix, r.epoch()), Body.ofBytes(fenced.encode()),
                    v.version()).isPresent()) {
                return fenced;
            }
            // ⚠️ RE-READ AND RECOMPUTE FROM THE VERSION FENCED (M13.22b review
            // round 2, P4): the deposed leader wrote its roster after the walk
            // read it -- a join, a decision -- and the fence carries what it
            // wrote.
            v = readRoster(r.epoch()).orElseThrow(() -> new IOException(
                    "roster " + r.epoch() + " vanished while it was fenced"));
        }
        throw new IOException("roster " + found.roster().epoch() + " kept changing while fenced");
    }

    private Roster writeOwn(long epoch, long predecessor, Roster.Incarnation self,
            Map<UUID, Integer> walQuorums, long notBefore) throws IOException, DeposedSignal {
        Roster fresh = new Roster(epoch, predecessor, self,
                List.of(new Roster.Member(self, Roster.State.ROSTERED)),
                List.of(new Roster.TermRecord(0, new TreeMap<>(walQuorums))), List.of(),
                notBefore, 0, false);
        String key = Roster.key(prefix, epoch);
        if (store.putIfAbsent(key, Body.ofBytes(fresh.encode())).isPresent()) {
            return fresh;
        }
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
            Versioned existing = readRoster(epoch).orElseThrow(() -> new IOException(
                    "roster " + epoch + " was refused as present and read as absent"));
            if (!existing.roster().leader().equals(self)) {
                throw new IOException("roster " + epoch + " names another leader");
            }
            // ⚠️ ITS OWN ROSTER FENCED IS DEPOSITION TOO (M13.26b review round
            // 1, P1): a newer leader fenced this term between the walk's read
            // and this one, and carrying its fencedBy on would start a term
            // already fenced.
            if (existing.roster().fencedBy() > epoch) {
                throw new DeposedSignal(existing.roster().fencedBy());
            }
            Roster wanted = existing.roster().after(predecessor, notBefore);
            if (wanted.equals(existing.roster())) {
                return wanted;
            }
            if (store.putIfMatch(key, Body.ofBytes(wanted.encode()), existing.version())
                    .isPresent()) {
                return wanted;
            }
        }
        throw new IOException("roster " + epoch + " kept changing under its own leader");
    }

    /** Writes {@code LATEST = epoch}; false when another write moved it first. */
    private boolean writeLatest(Latest latest, long epoch) throws IOException {
        Body body = Body.ofBytes(Roster.encodeLatest(epoch));
        String key = Roster.latestKey(prefix);
        return (latest.version() == null
                ? store.putIfAbsent(key, body)
                : store.putIfMatch(key, body, latest.version())).isPresent();
    }

    private Latest readLatest() throws IOException {
        String key = Roster.latestKey(prefix);
        Optional<ObjectStat> stat = store.stat(key);
        if (stat.isEmpty()) {
            return new Latest(-1, null);
        }
        try (InputStream in = store.get(key)) {
            return new Latest(Roster.decodeLatest(in.readAllBytes()), stat.get().version());
        }
    }

    private Optional<Versioned> readRoster(long epoch) throws IOException {
        String key = Roster.key(prefix, epoch);
        Optional<ObjectStat> stat = store.stat(key);
        if (stat.isEmpty()) {
            return Optional.empty();
        }
        Roster r;
        try (InputStream in = store.get(key)) {
            r = Roster.decode(in.readAllBytes());
        }
        if (r.epoch() != epoch) {
            throw new IOException("roster " + key + " holds epoch " + r.epoch());
        }
        return Optional.of(new Versioned(r, stat.get().version()));
    }
}
