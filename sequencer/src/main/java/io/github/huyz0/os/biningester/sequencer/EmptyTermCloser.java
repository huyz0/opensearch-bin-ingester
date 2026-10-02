// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.ObjectStat;
import io.github.huyz0.os.biningester.binstore.Version;
import io.github.huyz0.os.biningester.format.Roster;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Closes, oldest first, every earlier term a new leader fenced that recorded
 * no fast index (ADR-0081 §5 step 7, its vacuous case; M13.27g).
 *
 * <p>⚠️ WHY A TERM START CLOSES ANYTHING BEFORE THE TAKEOVER EXISTS: the walk
 * stops at the first closed roster, and nothing else closes one until M13.33.
 * Every fleet runs the term start, fast index or not (ADR-0081, Context), so without
 * this each new leader would walk and fence EVERY term the cluster ever had --
 * requests growing with its history, never bounded by the pods or the terms in
 * flight (non-negotiable 6).
 *
 * <p>⚠️ VACUOUS, NOT A SHORTCUT: a term is closed once every one of its streams
 * is decided and committed and every earlier term is closed (§5 step 7). A
 * leader assigns a fast offset only under a {@code wal_quorum} its term record
 * holds (§4) and decides only a stream with entries or a term-record value
 * (§5 step 5), so a roster whose every term-record element is empty and whose
 * decisions are empty has no stream at all. Judged on the roster AFTER this
 * term fenced it: the fence fails its leader's every later write.
 *
 * <p>⚠️ OLDEST FIRST, STOPPING AT THE FIRST IT CANNOT CLOSE (invariant a): a
 * term that recorded a fast index stays open for M13.33, and so does every term
 * after it, however empty.
 */
public final class EmptyTermCloser {

    private final BinStore store;
    private final String prefix;

    public EmptyTermCloser(BinStore store, String prefix) {
        this.store = Objects.requireNonNull(store, "store");
        this.prefix = Objects.requireNonNull(prefix, "prefix");
    }

    /**
     * Closes what it can of {@code unclosed}, as {@link FastTermStart.Started}
     * returns it: every earlier unclosed term, newest first, fenced by
     * {@code epoch}.
     *
     * @return how many terms it closed
     * @throws IOException the store failed, or a roster is missing or kept
     *     changing -- whatever was closed stays closed, and the next term
     *     start closes the rest
     */
    public int close(long epoch, List<Roster> unclosed) throws IOException {
        int closed = 0;
        for (int i = unclosed.size() - 1; i >= 0; i--) {
            if (!closeOne(epoch, unclosed.get(i).epoch())) {
                break;
            }
            closed++;
        }
        return closed;
    }

    /** Whether term {@code term} is closed when this returns. */
    private boolean closeOne(long epoch, long term) throws IOException {
        String key = Roster.key(prefix, term);
        for (int attempt = 0; attempt < FastTermStart.MAX_ATTEMPTS; attempt++) {
            Optional<ObjectStat> stat = store.stat(key);
            if (stat.isEmpty()) {
                throw new IOException("roster " + term + " vanished after its fence");
            }
            Roster r;
            try (InputStream in = store.get(key)) {
                r = Roster.decode(in.readAllBytes());
            }
            if (r.closed()) {
                return true;
            }
            // ⚠️ ONLY WHILE THIS TERM'S FENCE HOLDS IT: a newer term fenced it
            // since, and closing is that term's to judge.
            if (r.fencedBy() != epoch || !recordedNothing(r)) {
                return false;
            }
            Version version = stat.get().version();
            if (store.putIfMatch(key, Body.ofBytes(r.asClosed().encode()), version).isPresent()) {
                return true;
            }
        }
        throw new IOException("roster " + term + " kept changing while it was closed");
    }

    private static boolean recordedNothing(Roster r) {
        return r.decisions().isEmpty()
                && r.termRecord().stream().allMatch(t -> t.walQuorum().isEmpty());
    }
}
