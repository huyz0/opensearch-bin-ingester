// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import binjava.binstore.BinStore;
import binjava.format.RunKey;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

/**
 * The fixture M4.9's two bounded-recovery suites share.
 *
 * <p>⚠️ Extracted when the single suite passed the 500-line limit —
 * code-structure.md rule 1, split rather than raise. The seam is the row's own:
 * what the bound COSTS is asserted by request count, and what it RECONSTRUCTS
 * is asserted by offsets, and neither test is any use without the other.
 */
final class BoundedRecoveryFixture {

    private BoundedRecoveryFixture() {
    }

    static final String PREFIX = "bins/cluster-a";
    static final Duration TTL = Duration.ofSeconds(10);
    static final Duration RENEW = Duration.ofSeconds(3);
    static final UUID A = UUID.fromString("00000000-0000-0000-0000-0000000000aa");
    static final RunKey RA = new RunKey(A, 0);

    static final class TestClock extends Clock {
        private long millis = 1_000_000L;

        @Override public long millis() {
            return millis;
        }

        @Override public Instant instant() {
            return Instant.ofEpochMilli(millis);
        }

        @Override public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override public Clock withZone(ZoneId zone) {
            return this;
        }
    }

    static LeaseManager manager(BinStore store, String podId) {
        return new LeaseManager(store, new LeaseConfig(PREFIX, podId, "", TTL, RENEW),
                new TestClock());
    }

    static CheckpointWriter.Ticker frozen() {
        return () -> new CountDownLatch(1).await();
    }

    /**
     * ⚠️ THE RENEW TICKER IS FROZEN TOO, inside a counting window. The real one
     * sleeps for {@code RENEW} and then issues a conditional PUT; three seconds
     * is longer than these fixtures take, so it has never fired mid-measurement
     * — which makes it a latent flake rather than a safe one.
     */
    static LocalSequencer.RenewTicker noRenew() {
        return () -> new CountDownLatch(1).await();
    }

    static CommitRequest request(String pod, long flushSeq, String segment) {
        return request(pod, flushSeq, segment, RA);
    }

    /**
     * ⚠️ THE STREAM IS A PARAMETER, and a fixture that commits ONE stream in
     * every term cannot see whether the ancestry walk works at all. Offsets in a
     * {@code RunCommit} are ABSOLUTE, not incremental, so replaying only the
     * NEWEST ancestor already yields the final offset for any stream that
     * ancestor touched. What needs the walk is a stream committed in an OLDER
     * term and not since — measured: with one shared stream, a mutant that
     * stopped the walk after the first ancestor passed.
     */
    static CommitRequest request(String pod, long flushSeq, String segment, RunKey key) {
        Map<RunKey, Integer> counts = new LinkedHashMap<>();
        counts.put(key, 1);
        return new CommitRequest(pod, "i1", flushSeq, segment, counts);
    }

    /** A stream unique to term {@code t}, so only that term's entries carry it. */
    static RunKey streamOfTerm(int t) {
        return new RunKey(new UUID(0x7e20_0000L, t), t);
    }
}
