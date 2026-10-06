// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.format.Roster;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * What every elected term runs before it serves (ADR-0081 §5 steps 2-3;
 * M13.27d): the walk, the fences, its roster and {@code LATEST}, then the
 * earlier terms that recorded no fast index closed.
 *
 * <p>⚠️ BEFORE ANY DEFAULT-PATH COMMIT (§5, round 3, R3-2): the caller hands
 * the term to nobody until this returns, so a commit admitted before the walk
 * cannot take an offset of an exposed entry the walk has not found yet.
 *
 * <p>⚠️ A TERM THAT CANNOT START IS GIVEN BACK, as
 * {@link LocalSequencer#start} gives back one whose chain cannot be read: a
 * newer term deposes it (its lease is gone too, which its next renewal would
 * find), and a store failure leaves it holding a lease it cannot use -- either
 * way a held term would leave the cluster without a sequencer for a TTL.
 */
public final class FastTermOpening {

    /** Closes what it can of a start's unclosed terms: {@link EmptyTermCloser#close}. */
    @FunctionalInterface
    public interface TermCloser {
        int close(long epoch, List<Roster> unclosed) throws IOException;
    }

    /**
     * A started term, after its start closed what it could (M13.27j).
     *
     * @param closedThrough the highest closed epoch, 0 for none: every JOINED
     *     carries it (ADR-0081 §5 step 7)
     * @param stillOpen the earlier terms still open, newest first: their
     *     decisions can supersede a group a pod reports
     */
    public record Opened(FastTermStart.Started started, long closedThrough,
            List<Roster> stillOpen) {
        public Opened {
            Objects.requireNonNull(started, "started");
            stillOpen = List.copyOf(stillOpen);
        }
    }

    private final FastTermStart start;
    private final TermCloser closer;
    private final FastLeaseFence fence;
    private final Roster.Incarnation self;
    private final Supplier<Map<UUID, Integer>> walQuorums;

    /**
     * @param walQuorums every fast index's {@code wal_quorum} this pod's catalog
     *     holds, read at each start: the term record's first element
     */
    public FastTermOpening(FastTermStart start, TermCloser closer, FastLeaseFence fence,
            Roster.Incarnation self, Supplier<Map<UUID, Integer>> walQuorums) {
        this.start = Objects.requireNonNull(start, "start");
        this.closer = Objects.requireNonNull(closer, "closer");
        this.fence = Objects.requireNonNull(fence, "fence");
        this.self = Objects.requireNonNull(self, "self");
        this.walQuorums = Objects.requireNonNull(walQuorums, "walQuorums");
    }

    /**
     * Starts term {@code epoch}, just won by {@code term}.
     *
     * @return the started term; empty when a newer term deposed it, and
     *     {@code term} is then closed
     * @throws IOException the start failed; {@code term} is closed
     */
    public Optional<Opened> open(long epoch, Closeable term) throws IOException {
        Objects.requireNonNull(term, "term");
        FastTermStart.Outcome outcome;
        try {
            outcome = start.start(epoch, self, Map.copyOf(walQuorums.get()),
                    fence.notBeforeWallMillis(), fence.replacedHolderUid());
        } catch (IOException | RuntimeException failed) {
            giveBack(term, failed);
            throw failed;
        }
        if (!(outcome instanceof FastTermStart.Started started)) {
            term.close();
            return Optional.empty();
        }
        // ⚠️ A FAILED CLOSE STARTS THE TERM ANYWAY: closing is bookkeeping the
        // next term start retries, and nothing this term does depends on it.
        // ⚠️ UNCHECKED TOO (M13.27d review round 1, P2): the election above
        // catches only IOException, so an escape here would leave the lease
        // held and renewed by a term nobody holds.
        int closed = 0;
        try {
            closed = closer.close(epoch, started.unclosed());
        } catch (EmptyTermCloser.Stopped partly) {
            // ⚠️ WHAT IT CLOSED BEFORE FAILING STAYS CLOSED, and counts toward
            // closedThrough (M13.27j review round 1, P2).
            closed = partly.closed();
            System.getLogger(FastTermOpening.class.getName()).log(System.Logger.Level.WARNING,
                    "term " + epoch + " closed " + closed + " earlier term(s) of "
                            + started.unclosed().size() + "; the next start retries", partly);
        } catch (IOException | RuntimeException notNow) {
            System.getLogger(FastTermOpening.class.getName()).log(System.Logger.Level.WARNING,
                    "term " + epoch + " closed no earlier term; the next start retries", notNow);
        }
        return Optional.of(opened(started, closed));
    }

    /**
     * ⚠️ THE CLOSED ONES ARE A PREFIX, OLDEST FIRST (invariant a): the walk
     * stopped at the first closed term, and the closer closed the oldest
     * {@code closed} of the unclosed ones.
     */
    private static Opened opened(FastTermStart.Started started, int closed) {
        List<Roster> unclosed = started.unclosed();
        int open = unclosed.size() - closed;
        long closedThrough;
        if (closed > 0) {
            closedThrough = unclosed.get(open).epoch();
        } else if (unclosed.isEmpty()) {
            closedThrough = Math.max(0, started.own().predecessor());
        } else {
            closedThrough = Math.max(0, unclosed.get(unclosed.size() - 1).predecessor());
        }
        return new Opened(started, closedThrough, unclosed.subList(0, open));
    }

    private static void giveBack(Closeable term, Exception failed) {
        try {
            term.close();
        } catch (IOException | RuntimeException alsoFailed) {
            failed.addSuppressed(alsoFailed);
        }
    }
}
