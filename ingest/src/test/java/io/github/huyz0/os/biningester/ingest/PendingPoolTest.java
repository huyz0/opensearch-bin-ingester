// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
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
 *
 * <p>⚠️ THE UNIT IS A BATCH -- ONE REQUEST'S RECORDS WITH ONE ROUTING VALUE --
 * AND M6.6's ROUND-1 REVIEW IS WHY. Keyed on the index alone, the first waiter
 * to wake drained every request's records and wrote all of them at the one
 * partition its own routing value named: one producer's records at another's
 * partition, and the owner holding an exception for a write that had happened.
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
    void aBatchWAITSAndIsTakenByItsOWNER() {
        TestClock clock = new TestClock();
        PendingPool pool = pool(clock);
        PendingPool.Batch batch = pool.open("logs", "tenant-a");

        assertThat(batch.offer(record("doc-1", 16))).isTrue();
        assertThat(batch.offer(record("doc-2", 16))).isTrue();
        List<SegmentRecord> taken = batch.take();

        assertThat(taken.stream().map(SegmentRecord::id))
                .as("in order -- the order the producer sent them, which is the order their "
                        + "offsets will be assigned in; re-ordering here makes a replay "
                        + "produce a different log than the original run")
                .containsExactly("doc-1", "doc-2");
        assertThat(batch.routing())
                .as("the routing value belongs to the BATCH: it is what places these records "
                        + "and nothing else in the ingester still has it by then")
                .isEqualTo("tenant-a");
        assertThat(pool.bytesHeldFor("logs"))
                .as("and taking frees the bytes, or a second wait for the same index starts "
                        + "full")
                .isZero();
    }

    /**
     * TWO requests for the SAME unregistered index do not take each other's
     * records (M6.6's round-1 blocking finding).
     *
     * <p>⚠️ MEASURED ON THE INDEX-KEYED VERSION: producer A's records landed in
     * producer B's partition while A received an exception for a write that had
     * happened -- and reversing the wake order made B return success having
     * written nothing. Two producers writing to one unregistered index is the
     * ordinary case during a plugin reconnect, not an edge one.
     */
    @Test
    void TWOBatchesForTheSAMEIndexAreINDEPENDENT() {
        TestClock clock = new TestClock();
        PendingPool pool = pool(clock);
        PendingPool.Batch a = pool.open("logs", "tenant-a");
        PendingPool.Batch b = pool.open("logs", "tenant-b");
        a.offer(record("a-1", 16));
        b.offer(record("b-1", 16));
        b.offer(record("b-2", 16));

        assertThat(a.take().stream().map(SegmentRecord::id))
                .as("each batch takes ITS OWN records, or one producer's write is placed by "
                        + "another producer's routing value")
                .containsExactly("a-1");
        assertThat(b.take().stream().map(SegmentRecord::id))
                .as("and the other batch is untouched -- with the index-keyed version it was "
                        + "already gone, and its owner returned success having written nothing")
                .containsExactly("b-1", "b-2");
    }

    @Test
    void takingABatchTWICEIsEMPTYTheSecondTime() {
        TestClock clock = new TestClock();
        PendingPool pool = pool(clock);
        PendingPool.Batch batch = pool.open("logs", "a");
        batch.offer(record("doc-1", 16));
        batch.take();

        assertThat(batch.take())
                .as("settled once: a caller that took its records and then saw the sweeper "
                        + "hand them over too would write them twice")
                .isEmpty();
    }

    @Test
    void takingONEIndexLeavesTheOTHERSWaiting() {
        TestClock clock = new TestClock();
        PendingPool pool = pool(clock);
        PendingPool.Batch logs = pool.open("logs", "a");
        PendingPool.Batch metrics = pool.open("metrics", "b");
        logs.offer(record("doc-1", 16));
        metrics.offer(record("doc-2", 16));

        logs.take();

        assertThat(metrics.take())
                .as("a registration for one index must not discard another index's records -- "
                        + "they are waiting for a push that has not arrived yet")
                .hasSize(1);
    }

    /**
     * The timeout REJECTS, and the caller is handed what to reject (M6.5).
     *
     * <p>⚠️ ADR-0006's "reject, never fold". Folding to partition 0 returns 202
     * for records that will sit in one shard of an index that has none.
     */
    @Test
    void aBatchThatWAITEDTooLongIsHandedBackForREFUSAL() {
        TestClock clock = new TestClock();
        PendingPool pool = pool(clock);
        PendingPool.Batch batch = pool.open("logs", "a");
        batch.offer(record("doc-1", 16));

        assertThat(pool.expire())
                .as("PREMISE: nothing expires before the timeout, or the pool is useless -- "
                        + "the whole point is that a registration arriving in milliseconds "
                        + "finds its records still there")
                .isEmpty();
        clock.advance(Duration.ofSeconds(5));
        List<PendingPool.Batch> expired = pool.expire();

        assertThat(expired).hasSize(1);
        assertThat(expired.get(0).index()).isEqualTo("logs");
        assertThat(batch.take())
                .as("and the owner sees EMPTY, which is how it learns to refuse rather than "
                        + "appending nothing and reporting success")
                .isEmpty();
        assertThat(pool.bytesHeldFor("logs"))
                .as("the bytes are freed, or an index that timed out once can never wait again")
                .isZero();
    }

    /**
     * A batch of MANY records is ONE batch (found while writing M6.6's
     * two-in-flight case).
     *
     * <p>⚠️ MEASURED: listing the batch on every offer put a two-record batch
     * in its index's list twice, so the sweeper was handed the same batch
     * twice -- and {@code remove} drops one copy, leaving a settled batch in
     * the list for the lifetime of the process.
     */
    @Test
    void aBatchOfMANYRecordsIsSweptONCEAndLeavesNOTHINGBehind() {
        TestClock clock = new TestClock();
        PendingPool pool = pool(clock);
        PendingPool.Batch batch = pool.open("logs", "a");
        batch.offer(record("doc-1", 16));
        batch.offer(record("doc-2", 16));
        batch.offer(record("doc-3", 16));

        assertThat(pool.waitingBatches())
                .as("three records are ONE request and therefore one batch")
                .isEqualTo(1);
        clock.advance(Duration.ofSeconds(5));

        assertThat(pool.expire())
                .as("and the sweeper is handed it once -- twice means the same records are "
                        + "refused, and counted, twice")
                .hasSize(1);
        assertThat(pool.waitingBatches())
                .as("and nothing is left in the index's list")
                .isZero();
    }

    @Test
    void expiringSWEEPSEveryIndexNotJustOne() {
        TestClock clock = new TestClock();
        PendingPool pool = pool(clock);
        pool.open("logs", "a").offer(record("doc-1", 16));
        pool.open("metrics", "b").offer(record("doc-2", 16));
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
    void aBATCHStillINSIDETheTimeoutSurvivesASweep() {
        TestClock clock = new TestClock();
        PendingPool pool = pool(clock);
        PendingPool.Batch old = pool.open("logs", "a");
        old.offer(record("old", 16));
        clock.advance(Duration.ofSeconds(4));
        PendingPool.Batch fresh = pool.open("logs", "a");
        fresh.offer(record("new", 16));
        clock.advance(Duration.ofSeconds(1));

        List<PendingPool.Batch> expired = pool.expire();

        assertThat(expired)
                .as("per BATCH, not per index: the older one has waited five seconds and the "
                        + "younger one has waited one, and refusing both would punish a write "
                        + "whose registration may still be milliseconds away")
                .hasSize(1);
        assertThat(fresh.take().stream().map(SegmentRecord::id))
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
        PendingPool.Batch neighbour = pool.open("metrics", "b");
        neighbour.offer(record("neighbour", 64));
        PendingPool.Batch flood = pool.open("logs", "a");

        int accepted = 0;
        for (int i = 0; i < 200; i++) {
            if (flood.offer(record("doc-" + i, i % 2 == 0 ? 16 : 480))) {
                accepted++;
            }
        }

        assertThat(pool.bytesHeldFor("logs"))
                .as("the bound is in bytes: records of 16 and 480 bytes were offered and what "
                        + "is held never exceeds 4096")
                .isLessThanOrEqualTo(4_096);
        assertThat(accepted)
                .as("PREMISE: the flood really was refused part-way, or the bound was never "
                        + "reached and the case measures nothing")
                .isLessThan(200);
        assertThat(neighbour.take())
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
     * A batch is settled EXACTLY once -- by its owner or by the sweeper, never
     * by both (M6.5 round 1, re-shaped for batches).
     *
     * <p>⚠️ BOTH ORDERS, because the two are symmetrical and only one of them
     * is the one that ships. A batch handed to the sweeper AND returned to its
     * owner is the same records written once and refused once: the producer
     * retries a write that happened.
     */
    @Test
    void aBatchIsSettledEXACTLYOnceByTakeOrBySweep() {
        TestClock clock = new TestClock();
        PendingPool pool = pool(clock);
        PendingPool.Batch owner = pool.open("logs", "a");
        owner.offer(record("doc-1", 16));
        PendingPool.Batch swept = pool.open("metrics", "b");
        swept.offer(record("doc-2", 16));
        clock.advance(Duration.ofSeconds(5));

        List<SegmentRecord> takenFirst = owner.take();
        List<PendingPool.Batch> sweep = pool.expire();
        List<PendingPool.Batch> secondSweep = pool.expire();

        assertThat(takenFirst)
                .as("the owner got there first and holds the records")
                .hasSize(1);
        assertThat(sweep.stream().map(PendingPool.Batch::index))
                .as("and the sweeper is handed the OTHER batch only: expiring one whose "
                        + "owner already took it refuses a write that is at that moment "
                        + "being written, and both batches were past the timeout")
                .containsExactly("metrics");
        assertThat(secondSweep)
                .as("and a batch is swept once, not once per sweep")
                .isEmpty();
        assertThat(swept.take())
                .as("and the sweeper got there first for that one, so ITS owner sees empty "
                        + "and refuses rather than writing records the sweeper already "
                        + "dropped")
                .isEmpty();
    }

    /**
     * Nothing is lost when many requests to ONE index open, offer and take at
     * the same time (M6.5's round-1 blocking finding, re-shaped for batches).
     *
     * <p>⚠️ MEASURED ON THE FIRST VERSION: one offer thread of 200,000 records
     * lost 3,510 of them -- {@code offer} resolved the index's entry outside
     * the map's lock, a concurrent settle unmapped it in between, and the
     * record landed in an object nothing could reach. The batches are per
     * request; the index's entry and its byte count are still shared.
     */
    @Test
    void nothingIsLOSTWhenMANYRequestsToONEIndexRace() throws Exception {
        TestClock clock = new TestClock();
        PendingPool pool = new PendingPool(clock, Duration.ofHours(1), 1L << 30);
        int threads = 8;
        int perThread = 2_000;
        java.util.concurrent.atomic.AtomicInteger taken =
                new java.util.concurrent.atomic.AtomicInteger();
        List<Thread> workers = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            int id = t;
            workers.add(Thread.ofPlatform().start(() -> {
                for (int i = 0; i < perThread; i++) {
                    PendingPool.Batch batch = pool.open("logs", "tenant-" + id);
                    batch.offer(record(id + "-" + i, 8));
                    taken.addAndGet(batch.take().size());
                }
            }));
        }
        for (Thread worker : workers) {
            worker.join(java.util.concurrent.TimeUnit.SECONDS.toMillis(60));
        }

        assertThat(taken.get())
                .as("every record offered is taken by the request that offered it -- one "
                        + "that is neither taken nor held was added to an index entry the "
                        + "map had already dropped")
                .isEqualTo(threads * perThread);
        assertThat(pool.bytesHeldFor("logs"))
                .as("and the index is left charged NOTHING, or its bound erodes by whatever "
                        + "the race lost until the index can never be written to again")
                .isZero();
    }

    /**
     * The byte count is EXACT, and the routing value is part of it (M6.5,
     * round 1's test major).
     */
    @Test
    void theBYTECountIsEXACTAndCOUNTSTheRoutingValue() {
        TestClock clock = new TestClock();
        PendingPool pool = new PendingPool(clock, Duration.ofSeconds(5), 1 << 20);
        SegmentRecord record = record("doc-1", 100);
        long framed = Accumulator.estimatedFramedBytes(record);

        pool.open("logs", "tenant-abcdefgh").offer(record);

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
        pool.open("logs", "a").offer(old);
        clock.advance(Duration.ofSeconds(4));
        SegmentRecord fresh = record("new", 40);
        pool.open("logs", "bb").offer(fresh);
        clock.advance(Duration.ofSeconds(1));

        pool.expire();

        assertThat(pool.bytesHeldFor("logs"))
                .as("the expired batch's bytes are RETURNED, exactly -- a pool that expired "
                        + "batches without crediting their bytes fills up and refuses writes "
                        + "for an index whose records all timed out an hour ago")
                .isEqualTo(Accumulator.estimatedFramedBytes(fresh) + 2);
        assertThat(pool.waitingIndices())
                .as("and the index is still waiting, with one batch")
                .isEqualTo(1);
    }

    @Test
    void takingABatchRETURNSItsBytesToo() {
        TestClock clock = new TestClock();
        PendingPool pool = new PendingPool(clock, Duration.ofSeconds(5), 200);
        PendingPool.Batch first = pool.open("logs", "a");
        first.offer(record("doc-1", 100));

        first.take();

        PendingPool.Batch second = pool.open("logs", "a");
        assertThat(second.offer(record("doc-2", 100)))
                .as("a second wait for the same index starts EMPTY -- if taking left the "
                        + "bytes charged, the index could only ever be written to once before "
                        + "its pool was permanently full")
                .isTrue();
        assertThat(pool.bytesHeldFor("logs"))
                .isEqualTo(Accumulator.estimatedFramedBytes(record("doc-2", 100)) + 1);
    }

    @Test
    void aDISCARDEDBatchReleasesItsBytesWithoutHandingThemBack() {
        TestClock clock = new TestClock();
        PendingPool pool = new PendingPool(clock, Duration.ofSeconds(5), 1 << 20);
        PendingPool.Batch batch = pool.open("logs", "a");
        batch.offer(record("doc-1", 100));

        batch.discard();

        assertThat(pool.bytesHeldFor("logs"))
                .as("a caller that is refusing its own write releases what it took -- a "
                        + "partially-offered request that threw without discarding leaves its "
                        + "records charged and hands them to whoever expires them, which is a "
                        + "partial write under a routing value nobody sent")
                .isZero();
        assertThat(batch.take())
                .as("and the records are gone rather than takeable")
                .isEmpty();
    }
}
