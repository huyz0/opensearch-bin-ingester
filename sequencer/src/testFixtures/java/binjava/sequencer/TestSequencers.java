// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import binjava.binstore.BinStore;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;

/**
 * Builds a REAL {@link LocalSequencer} over a store, for tests that need one and
 * cannot use {@link FakeSequencer}.
 *
 * <p>⚠️ WHY A FAKE IS NOT ALWAYS ENOUGH, stated NARROWLY because the first draft
 * of this comment got it wrong and round-1 review measured it: a fake writes
 * nothing to the store, so any assertion on a REQUEST COUNT reads one less per
 * flush with it in place. The sites that actually care are
 * {@code DefaultIngestTest}'s request deltas — five {@code isEqualTo(4)}
 * assertions over {@code CountingBinStore} — and
 * {@code IngestShutdownTest}'s {@code isEqualTo(5)}.
 *
 * <p>⚠️ IT IS <b>NOT</b> {@code IntervalPutRateTest}, and that claim stood in
 * three places here before it was checked. That test constructs no
 * {@code DefaultIngest}, no {@code Sequencer} and no {@code CommitLog} — it
 * drives {@code Accumulator} and {@code SegmentPublisher} directly — and its own
 * javadoc says so in capitals: "ONLY SegmentPublisher, no CommitLog … a
 * commit-log append is a SEPARATE request the cost table does not fold into that
 * figure". Nor do the {@code http} tests count requests at all: the two
 * {@code MemoryFlat*} tests measure heap and files on disk, and
 * {@code BulkThroughTheRealStackTest} wraps a {@code CountingBinStore} without
 * ever reading {@code counts()}. For those, and for the three plugin ITs, the
 * reason they take a real sequencer is simply that {@code DefaultIngest}
 * REQUIRES one since M4.6d — nothing to do with counting.
 *
 * <p>⚠️ ONE COPY, and the duplication it replaces is the argument. M4.6d's
 * wiring first landed as six pasted private helpers across {@code http} and
 * {@code plugin}, each with fully-qualified names and each carrying the same
 * pasted rationale — which was false of five of the six. Centralising a paste
 * without checking it only gives the falsehood one authoritative address, which
 * is what happened here and what this comment is the correction of.
 *
 * <p>⚠️ NOT thread-safe beyond what {@link LocalSequencer} itself guarantees,
 * and it ACQUIRES A LEASE on construction: two calls against one store and
 * prefix, with the first still held, give the second nothing. That is the lease
 * behaving correctly, not a fixture limitation — a test wanting the second to
 * win closes the first.
 */
public final class TestSequencers {

    private TestSequencers() {
    }

    /**
     * ⚠️ Long enough that no fixture lapses mid-test. A test asserting anything
     * ABOUT expiry builds its own {@link LeaseManager} with its own clock; this
     * TTL exists so that tests which merely need a sequencer never see one.
     */
    public static final Duration TTL = Duration.ofSeconds(10);

    private static final Duration RENEW = Duration.ofSeconds(3);

    /** ⚠️ The caller's seal-redrive budget, per M4.6c — derived from the renew interval. */
    public static final int SEAL_REDRIVE_BUDGET = 8;

    /** A {@code LeaseManager} for one pod, exposed so a test can attempt a SECOND acquisition. */
    public static LeaseManager leases(BinStore store, String prefix, String podId) {
        return leases(store, prefix, podId, Clock.systemUTC());
    }

    /**
     * The same, on a caller-supplied clock.
     *
     * <p>⚠️ A TEST ASSERTING ANYTHING ABOUT EXPIRY MUST USE THIS. On
     * {@link Clock#systemUTC()} the {@link #TTL} is a wall-clock race: a test
     * that proves exclusivity by "nobody else could acquire while the term was
     * held" passes having proved nothing if the machine stalls past the TTL
     * between two statements. A {@link Clock#fixed} clock makes the held lease
     * unlappable and a RELEASED one still takeable, which is the distinction such
     * a test exists to draw.
     */
    public static LeaseManager leases(BinStore store, String prefix, String podId, Clock clock) {
        return new LeaseManager(store, new LeaseConfig(prefix, podId, "", TTL, RENEW), clock);
    }

    /**
     * A started {@code LocalSequencer} holding the lease for {@code podId}.
     *
     * @throws IOException if the lease is already held, which for a fixture is a
     *     broken test rather than a condition to handle — so it FAILS rather than
     *     returning an empty optional a caller might ignore.
     */
    public static Sequencer leased(BinStore store, String prefix, String podId)
            throws IOException {
        return leased(store, prefix, podId, Clock.systemUTC());
    }

    /** The same, on a caller-supplied clock — see {@link #leases(BinStore, String, String, Clock)}. */
    public static Sequencer leased(BinStore store, String prefix, String podId, Clock clock)
            throws IOException {
        return LocalSequencer.start(store, prefix, leases(store, prefix, podId, clock),
                        SEAL_REDRIVE_BUDGET)
                .orElseThrow(() -> new IOException(
                        "the fixture could not acquire the lease for " + podId + " at " + prefix));
    }
}
