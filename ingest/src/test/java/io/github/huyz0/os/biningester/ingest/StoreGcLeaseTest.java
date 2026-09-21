// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.StoreCounts;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.sequencer.LeaseConfig;
import io.github.huyz0.os.biningester.sequencer.LeaseManager;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The GC role as a real lease on the store (M8.5, FR-9).
 *
 * <p>⚠️ **TWO NODES OVER ONE STORE**, because the property is exclusion and a
 * single node cannot demonstrate it: a lease that answered "yes" to everyone
 * passes every single-node case.
 */
class StoreGcLeaseTest {

    private static final Duration TTL = Duration.ofSeconds(10);

    private static final class MutableClock extends Clock {
        private Instant now = Instant.parse("2026-09-17T12:00:00Z");

        void advance(Duration by) {
            now = now.plus(by);
        }

        @Override
        public Instant instant() {
            return now;
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }
    }

    private final MemoryBinStore backing = new MemoryBinStore();
    private final MutableClock clock = new MutableClock();

    @AfterEach
    void close() throws Exception {
        backing.close();
    }

    private StoreGcLease leaseFor(BinStore store, String podId) {
        return new StoreGcLease(new LeaseManager(store,
                new LeaseConfig("bins/c/gc", podId, "http://" + podId + ":8080", TTL,
                        Duration.ofSeconds(3)), clock), clock);
    }

    @Test
    void ONENodeHoldsTheRoleAndTheOTHERIsRefusedUntilItIsReleased() {
        StoreGcLease first = leaseFor(backing, "pod1");
        StoreGcLease second = leaseFor(backing, "pod2");

        assertThat(first.acquire()).isTrue();
        assertThat(second.acquire())
                .as("⚠️ NEVER TWO PODS AT ONCE: two passes decide what is unread from two "
                        + "views of the watermark table, and what one keeps the other deletes")
                .isFalse();

        first.release();
        assertThat(second.acquire())
                .as("⚠️ AND RELEASED, NOT LEFT TO EXPIRE, so the next pass does not wait "
                        + "out a TTL")
                .isTrue();
    }

    @Test
    void askingAGAINWhileHoldingItIsStillYES() {
        // ⚠️ `LeaseManager.tryAcquire` ANSWERS EMPTY TO AN INSTANCE THAT
        // ALREADY HOLDS THE LEASE -- its own javadoc says so. Reading that as
        // "someone else has it" makes a healthy GC leader skip every pass after
        // its first.
        StoreGcLease lease = leaseFor(backing, "pod1");

        assertThat(lease.acquire()).isTrue();
        assertThat(lease.acquire()).isTrue();
    }

    @Test
    void stillHeldISSUESNoRequestAndGoesFALSEBeforeTheLeaseExpires() {
        CountingBinStore store = new CountingBinStore(backing);
        StoreGcLease lease = leaseFor(store, "pod1");
        assertThat(lease.acquire()).isTrue();

        StoreCounts before = store.counts();
        assertThat(lease.stillHeld()).isTrue();
        assertThat(store.counts().total() - before.total())
                .as("⚠️ ASKED BEFORE EVERY DELETE BATCH: a check that cost a GET would be "
                        + "the most expensive thing in the pass")
                .isZero();

        clock.advance(TTL.minus(StoreGcLease.EXPIRY_MARGIN).plusMillis(1));
        assertThat(lease.stillHeld())
                .as("⚠️ INSIDE THE MARGIN IT IS ALREADY GONE: a batch sent with a millisecond "
                        + "of lease left lands after another pod has taken the role")
                .isFalse();
    }

    @Test
    void anUNREACHABLEStoreIsNOAndNeverAnException() {
        // ⚠️ THIS RUNS ON THE GC SCHEDULE. An exception out of `acquire`
        // would end GC on this node for good; not deleting is the safe answer
        // to not knowing.
        BinStore down = new ForwardingIngestStore(backing) {
            @Override
            public java.util.Optional<io.github.huyz0.os.biningester.binstore.ObjectStat> stat(String key)
                    throws java.io.IOException {
                throw new java.io.IOException("unreachable");
            }

            @Override
            public java.io.InputStream get(String key) throws java.io.IOException {
                throw new java.io.IOException("unreachable");
            }

            @Override
            public java.util.Optional<io.github.huyz0.os.biningester.binstore.Version> putIfAbsent(String key,
                    io.github.huyz0.os.biningester.binstore.Body body) throws java.io.IOException {
                throw new java.io.IOException("unreachable");
            }
        };

        assertThat(leaseFor(down, "pod1").acquire()).isFalse();
    }
}
