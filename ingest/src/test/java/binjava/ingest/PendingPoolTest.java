// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.format.OpType;
import binjava.format.SegmentRecord;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

/**
 * Records waiting for their index's shape (M6.5, FR-13, ADR-0015 § 3,
 * ADR-0046).
 *
 * <p>⚠️ THE ALTERNATIVE TO THIS POOL IS PUNISHING A PRODUCER FOR A RACE IT
 * CANNOT SEE. A producer that starts before the plugin connects would otherwise
 * be refused for every write until the cluster state happens to be pushed, and
 * a plugin reconnect would do it again -- which ADR-0015's Consequences name in
 * as many words.
 *
 * <p>⚠️ AND THE TIMEOUT REJECTS RATHER THAN FOLDING. Placing an unregistered
 * index's records in partition 0 funnels a whole index into one shard while
 * returning 202, which is a data shape nobody can undo afterwards.
 */
class PendingPoolTest {

    /** ⚠️ ADVANCED, never slept on: the timeout is the behaviour under test. */
    private static final class TestClock extends Clock {
        private long millis = 1_700_000_000_000L;

        @Override
        public long millis() {
            return millis;
        }

        void advance(Duration d) {
            millis += d.toMillis();
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis);
        }

        @Override
        public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(java.time.ZoneId zone) {
            return this;
        }
    }

    private static SegmentRecord record(String id, int payloadBytes) {
        return new SegmentRecord(id, OpType.INDEX, OptionalLong.of(1),
                new byte[payloadBytes]);
    }

    private static PendingPool pool(TestClock clock) {
        return new PendingPool(clock, Duration.ofSeconds(5), 1 << 20);
    }

    @Test
    void aRecordWAITSAndIsTakenWhenItsIndexArrives() {
        TestClock clock = new TestClock();
        PendingPool pool = pool(clock);

        assertThat(pool.offer("logs", "tenant-a", record("doc-1", 16))).isTrue();
        assertThat(pool.offer("logs", "tenant-b", record("doc-2", 16))).isTrue();

        List<PendingPool.Pending> taken = pool.take("logs");

        assertThat(taken.stream().map(p -> p.record().id()))
                .as("FIFO -- the order the producer sent them, which is the order their "
                        + "offsets will be assigned in; re-ordering here makes a replay "
                        + "produce a different log than the original run")
                .containsExactly("doc-1", "doc-2");
        assertThat(taken.get(0).routing())
                .as("the routing value is held WITH the record: it is what places it, and "
                        + "nothing else in the ingester still has it by then")
                .isEqualTo("tenant-a");
        assertThat(pool.bytesHeldFor("logs"))
                .as("and taking them frees the bytes, or the second wait for the same index "
                        + "starts full")
                .isZero();
    }

    @Test
    void takingONEIndexLeavesTheOTHERSWaiting() {
        TestClock clock = new TestClock();
        PendingPool pool = pool(clock);
        pool.offer("logs", "a", record("doc-1", 16));
        pool.offer("metrics", "b", record("doc-2", 16));

        pool.take("logs");

        assertThat(pool.take("metrics"))
                .as("a registration for one index must not discard another index's records -- "
                        + "they are waiting for a push that has not arrived yet")
                .hasSize(1);
    }

    @Test
    void takingANINDEXWithNothingWaitingIsEMPTYRatherThanAnError() {
        assertThat(pool(new TestClock()).take("never-written-to"))
                .as("a registration arrives for every index this node hosts, and most of them "
                        + "will have nothing waiting -- that is the ordinary case, not a fault")
                .isEmpty();
    }

    /**
     * The timeout REJECTS, and the caller is handed what to reject (M6.5).
     *
     * <p>⚠️ ADR-0006's "reject, never fold". Folding to partition 0 returns 202
     * for records that will sit in one shard of an index that has none.
     */
    @Test
    void aRecordThatWAITEDTooLongIsHandedBackForREFUSAL() {
        TestClock clock = new TestClock();
        PendingPool pool = pool(clock);
        pool.offer("logs", "a", record("doc-1", 16));

        assertThat(pool.expire())
                .as("PREMISE: nothing expires before the timeout, or the pool is useless -- "
                        + "the whole point is that a registration arriving in milliseconds "
                        + "finds its records still there")
                .isEmpty();
        clock.advance(Duration.ofSeconds(5));
        List<PendingPool.Pending> expired = pool.expire();

        assertThat(expired.stream().map(p -> p.record().id()))
                .as("handed back so the caller can refuse them -- which HTTP status a "
                        + "timed-out write gets belongs with the adapter, not here")
                .containsExactly("doc-1");
        assertThat(pool.bytesHeldFor("logs"))
                .as("and the bytes are freed, or an index that timed out once can never wait "
                        + "again")
                .isZero();
    }

    @Test
    void expiringSWEEPSEveryIndexNotJustTheOneAskedAbout() {
        TestClock clock = new TestClock();
        PendingPool pool = pool(clock);
        pool.offer("logs", "a", record("doc-1", 16));
        pool.offer("metrics", "b", record("doc-2", 16));
        clock.advance(Duration.ofSeconds(5));

        assertThat(pool.expire())
                .as("an index nobody writes to again would otherwise hold its records forever "
                        + "-- the leak the bound exists to prevent, arriving by another road")
                .hasSize(2);
        assertThat(pool.waitingIndices())
                .as("and the empty entries go with them")
                .isZero();
    }

    @Test
    void aRECORDStillINSIDETheTimeoutSurvivesASweep() {
        TestClock clock = new TestClock();
        PendingPool pool = pool(clock);
        pool.offer("logs", "a", record("old", 16));
        clock.advance(Duration.ofSeconds(4));
        pool.offer("logs", "a", record("new", 16));
        clock.advance(Duration.ofSeconds(1));

        List<PendingPool.Pending> expired = pool.expire();

        assertThat(expired.stream().map(p -> p.record().id()))
                .as("per RECORD, not per index: the older one has waited five seconds and the "
                        + "younger one has waited one, and refusing both would punish a write "
                        + "whose registration may still be milliseconds away")
                .containsExactly("old");
        assertThat(pool.take("logs").stream().map(p -> p.record().id()))
                .as("and the younger one is still there to be placed")
                .containsExactly("new");
    }

    /**
     * The bound is IN BYTES and PER INDEX (M6.5, M6 criterion 10).
     *
     * <p>⚠️ RECORDS OF DIFFERENT SIZES, because a bound on the record COUNT is
     * not unbounded, refuses the flood, names the index -- and still holds N
     * times the largest record. Equal-sized records cannot tell the two apart.
     *
     * <p>⚠️ AND A SECOND INDEX SURVIVES THE FLOOD, because a global bound
     * passes every single-index case while letting one unknown index refuse
     * the records of every other index waiting on an ordinary reconnect.
     */
    @Test
    void theBoundIsInBYTESAndPERIndex() {
        TestClock clock = new TestClock();
        PendingPool pool = new PendingPool(clock, Duration.ofSeconds(5), 4_096);
        pool.offer("metrics", "b", record("neighbour", 64));

        int accepted = 0;
        for (int i = 0; i < 200; i++) {
            // ⚠️ SIZES THAT VARY BY 30x: under a count bound the flood is
            // accepted until the count runs out, holding far more than 4 KiB.
            if (pool.offer("logs", "a", record("doc-" + i, i % 2 == 0 ? 16 : 480))) {
                accepted++;
            }
        }

        assertThat(pool.bytesHeldFor("logs"))
                .as("the bound is in bytes: %d records of 16 and 480 bytes were offered and "
                        + "what is held never exceeds 4096", 200)
                .isLessThanOrEqualTo(4_096);
        assertThat(accepted)
                .as("PREMISE: the flood really was refused part-way, or the bound was never "
                        + "reached and the case measures nothing")
                .isLessThan(200);
        assertThat(pool.take("metrics"))
                .as("and the OTHER index's record survives -- a global bound lets one unknown "
                        + "index refuse every other index's writes during an ordinary plugin "
                        + "reconnect")
                .hasSize(1);
    }

    @Test
    void aZEROOrNEGATIVETimeoutIsREFUSEDAtConstruction() {
        assertThatThrownBy(() -> new PendingPool(new TestClock(), Duration.ZERO, 1024))
                .as("a zero timeout rejects every record before its registration could "
                        + "possibly arrive, which is the pool doing the opposite of its job")
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PendingPool(new TestClock(), Duration.ofSeconds(5), 0))
                .as("and a zero bound accepts nothing")
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * Nothing is lost when a take races an offer (M6.5, round 1's blocking
     * finding).
     *
     * <p>⚠️ MEASURED ON THE PREVIOUS VERSION: one offer thread of 200,000
     * records against one consumer lost 3,510 of them — {@code offer} resolved
     * the entry outside the map's lock, {@code take} unmapped it in between,
     * and the record landed in an object nothing could reach. Never taken,
     * never expired, and the producer holding a 202.
     *
     * <p>⚠️ THE ASSERTION IS A CONSERVATION LAW, not a count: everything
     * accepted is either taken or still waiting, and a lost record shows up as
     * a shortfall however the threads interleave.
     */
    @Test
    void nothingIsLOSTWhenATakeRacesAnOffer() throws Exception {
        TestClock clock = new TestClock();
        PendingPool pool = new PendingPool(clock, Duration.ofHours(1), 1L << 30);
        int records = 50_000;
        java.util.concurrent.atomic.AtomicInteger accepted =
                new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.atomic.AtomicInteger taken =
                new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.atomic.AtomicBoolean offering =
                new java.util.concurrent.atomic.AtomicBoolean(true);
        Thread consumer = new Thread(() -> {
            while (offering.get()) {
                taken.addAndGet(pool.take("logs").size());
            }
        });
        consumer.start();

        for (int i = 0; i < records; i++) {
            if (pool.offer("logs", "a", record("doc-" + i, 8))) {
                accepted.incrementAndGet();
            }
        }
        offering.set(false);
        consumer.join(java.util.concurrent.TimeUnit.SECONDS.toMillis(30));
        int left = pool.take("logs").size();

        assertThat(accepted.get())
                .as("PREMISE: the bound is far above the flood, so every offer was accepted "
                        + "and a shortfall below can only be a LOST record")
                .isEqualTo(records);
        assertThat(taken.get() + left)
                .as("every record accepted is either taken or still waiting -- one that is "
                        + "neither was added to an entry the map had already dropped, and the "
                        + "producer was told 202 for it")
                .isEqualTo(records);
    }

    /**
     * The byte count is EXACT, and the routing value is part of it (M6.5,
     * round 1's test major).
     *
     * <p>⚠️ AN UPPER BOUND DOES NOT DISCHARGE THE CRITERION. Measured: deleting
     * the routing term from {@code Pending.bytes()}, and deleting the
     * subtraction in {@code expire}, both left the suite green — every case
     * asserted {@code <= 4096} or read the count after the entry had been
     * removed, where it reads 0 from the absent branch either way.
     */
    @Test
    void theBYTECountIsEXACTAndCOUNTSTheRoutingValue() {
        TestClock clock = new TestClock();
        PendingPool pool = new PendingPool(clock, Duration.ofSeconds(5), 1 << 20);
        SegmentRecord record = record("doc-1", 100);
        long framed = Accumulator.estimatedFramedBytes(record);

        pool.offer("logs", "tenant-abcdefgh", record);

        assertThat(pool.bytesHeldFor("logs"))
                .as("the record's framed estimate plus the routing value's own bytes -- the "
                        + "routing string is held HERE and nowhere else, so leaving it out "
                        + "under-counts a pool of small records with long tenant ids by more "
                        + "than the records themselves")
                .isEqualTo(framed + "tenant-abcdefgh".length());
    }

    @Test
    void aPARTIALExpiryLeavesTheREMAININGBytesExact() {
        TestClock clock = new TestClock();
        PendingPool pool = new PendingPool(clock, Duration.ofSeconds(5), 1 << 20);
        SegmentRecord old = record("old", 100);
        pool.offer("logs", "a", old);
        clock.advance(Duration.ofSeconds(4));
        SegmentRecord fresh = record("new", 40);
        pool.offer("logs", "bb", fresh);
        clock.advance(Duration.ofSeconds(1));

        pool.expire();

        assertThat(pool.bytesHeldFor("logs"))
                .as("the expired record's bytes are RETURNED, exactly -- a pool that expired "
                        + "records without crediting their bytes fills up and refuses writes "
                        + "for an index whose records all timed out an hour ago")
                .isEqualTo(Accumulator.estimatedFramedBytes(fresh) + 2);
        assertThat(pool.waitingIndices())
                .as("and the index is still waiting, with one record")
                .isEqualTo(1);
    }

    @Test
    void takingAnIndexRETURNSItsBytesToo() {
        TestClock clock = new TestClock();
        PendingPool pool = new PendingPool(clock, Duration.ofSeconds(5), 200);
        pool.offer("logs", "a", record("doc-1", 100));

        pool.take("logs");

        assertThat(pool.offer("logs", "a", record("doc-2", 100)))
                .as("a second wait for the same index starts EMPTY -- if taking left the "
                        + "bytes charged, the index could only ever be written to once before "
                        + "its pool was permanently full")
                .isTrue();
        assertThat(pool.bytesHeldFor("logs"))
                .isEqualTo(Accumulator.estimatedFramedBytes(record("doc-2", 100)) + 1);
    }
}
