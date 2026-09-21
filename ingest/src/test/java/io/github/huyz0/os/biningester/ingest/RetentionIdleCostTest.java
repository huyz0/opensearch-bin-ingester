// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.ConsumerProgress;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import io.github.huyz0.os.biningester.format.SegmentKey;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * NFR-2 with M7's retention path live (M7.12, criterion 13).
 *
 * <p>⚠️ SPLIT OUT OF {@code IdleConsumerCostTest} RATHER THAN ADDED TO IT:
 * that file reached 707 lines against code-structure.md's 700, and raising the
 * cap so one file fits is what non-negotiable 2 forbids. The seam is real —
 * that file is about the CONSUMER path being idle, this one about the
 * RETENTION path being idle.
 *
 * <p>⚠️ M7 ADDS TWO THINGS THAT COULD BUY A REQUEST. A progress frame arrives
 * on the subscription every interval for every stream on every node; a GC loop
 * exists precisely to call the store. The frame must cost nothing, and a GC
 * pass with NOTHING EXPIRED must cost nothing either — a pass that listed to
 * find out, or issued an empty delete, would make an idle cluster pay per pod
 * per interval forever, which is NFR-2 failing through the GC door.
 *
 * <p>⚠️ AND THE PREMISES ARE ASSERTED, because a zero over an empty path is not
 * evidence. This file's sibling records that failure mode from M6.11, where a
 * sweep over an empty pool left a zero green.
 */
class RetentionIdleCostTest {

    /** Criterion 13's floor is 1,000 consumers over 1,000 distinct streams. */
    private static final int CONSUMERS = 1_000;

    /** Criterion 13's floor is 3,000 intervals. */
    private static final int INTERVALS = 3_000;

    /**
     * The index this case's chain and frames BOTH name.
     *
     * <p>⚠️ IT IS BASE64URL, because that is what a progress frame carries and
     * what {@link RunKey#ofIndexUuid} joins on — the chain is built through the
     * same call, so the two halves cannot drift apart. Review MEASURED the
     * version where they did: the frames named one uuid and the chain another,
     * and the rule never consulted a watermark that existed.
     */
    private static final String IDLE_UUID = "nVzgup36TLqWp7VBBREj1w";

    private static final java.util.UUID SUBSCRIBED_INDEX =
            java.util.UUID.fromString("00000000-0000-0000-0000-0000000000aa");

    /** ⚠️ ADVANCED, never slept on: 3,000 real intervals cannot fit L0's budget. */
    private static final class TestClock extends Clock {
        private long millis = 1_700_000_000_000L;

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        void advance(Duration by) {
            millis += by.toMillis();
        }
    }

    /**
     * NFR-2 with the RETENTION path live (M7.12, criterion 13).
     *
     * <p>⚠️ M7 ADDS TWO THINGS THAT COULD BUY A REQUEST, and this is where they
     * are refused. A progress frame arrives on the subscription every interval
     * for every stream on every node; a GC loop exists precisely to call the
     * store. The frame must cost nothing, and a GC pass with NOTHING EXPIRED
     * must cost nothing either — a pass that listed to find out, or issued an
     * empty delete, would make an idle cluster pay per pod per interval
     * forever, which is NFR-2 failing through the GC door.
     *
     * <p>⚠️ AND THE PREMISES ARE ASSERTED, because a zero over an empty path is
     * not evidence: the table really holds a watermark per stream, the chain
     * really holds segments SEVEN HOURS OLD, and the rule really kept them on
     * the WATERMARK branch rather than returning at the time floor.
     *
     * <p>⚠️ THE PROGRESS HALF AND THE GRANT ZERO ARE STILL BY CONSTRUCTION, and
     * this file's other idle case discloses the same shape rather than
     * pretending otherwise: {@link WatermarkTable} holds no {@code BinStore}
     * and the {@code GrantIssuer} here is handed to nothing, so an
     * implementation that minted or read would still read zero. What THIS case
     * buys is the GC branch, where the class under test does hold a store.
     */
    @Test
    @Timeout(60)
    void theWATERMARKPathAndTheGCLoopCostZEROOfBothNumbersWhenIDLE() throws Exception {
        TestClock clock = new TestClock();
        IngestConfig config = IngestConfig.defaults("cluster-a");
        Accumulator accumulator = new Accumulator(config, clock);
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        SegmentPublisher publisher = new SegmentPublisher(store, "bins/cluster-a", "pod1");
        GrantIssuer grants = new GrantIssuer(new StoreFakes.CanPresign());
        SubscriptionHub hub = new SubscriptionHub();
        AtomicLong delivered = new AtomicLong();
        List<AutoCloseable> handles = new ArrayList<>(CONSUMERS);
        for (int i = 0; i < CONSUMERS; i++) {
            handles.add(hub.subscribe(new RunKey(SUBSCRIBED_INDEX, i),
                    SubscriptionHub.assembling(push -> delivered.incrementAndGet())));
        }

        WatermarkTable watermarks = new WatermarkTable(clock, Duration.ofSeconds(30),
                Duration.ofHours(12), Duration.ofHours(6));
        List<String> alarms = new ArrayList<>();
        AtomicLong consulted =
                new AtomicLong();
        // ⚠️ A COUNTING SEAM, because the reported boundary CANNOT tell the two
        // keep branches apart: with nothing deleted the report is a pure
        // function of the chain, identical whether the rule returned at the
        // time floor or consulted a watermark. Review MEASURED that -- forcing
        // the floor guard to `true` left the whole case green -- and this
        // counter is what makes "the watermark branch really ran" an assertion
        // rather than a sentence.
        RetentionRule.Watermarks counting = stream -> {
            consulted.incrementAndGet();
            return watermarks.of(stream);
        };
        RetentionRule rule = new RetentionRule(clock, Duration.ofHours(6), Duration.ofHours(24),
                0, (key, age) -> alarms.add(key), counting);
        List<Map<RunKey, Long>> reported = new ArrayList<>();
        RetentionPass pass = new RetentionPass(new SegmentGc(store, rule, 1000),
                reported::add);
        // ⚠️ A CHAIN THAT IS NOT EMPTY, or the GC loop below walks nothing and
        // the zero is about an empty path -- the by-construction shape this
        // file exists to refuse.
        List<CommitDelta> chain = idleChain(clock);

        try {
            long before = store.counts().total();
            for (int interval = 0; interval < INTERVALS; interval++) {
                // Every node reports every stream it hosts, every interval.
                watermarks.observe(progressFrame(interval));
                pass.run(chain, Map.of());
                // ⚠️ THE PRODUCTION FLUSH LOOP'S SHAPE, not a hand-fed flush:
                // `DefaultIngest.flushLoop` asks `isFlushDue()` on every
                // wake-up and publishes only when it says yes.
                clock.advance(config.intervalFloor());
                if (accumulator.isFlushDue()) {
                    publisher.publish(accumulator);
                }
            }

            assertThat(store.counts().total() - before)
                    .as("%d idle consumers, %d intervals, a progress frame every interval "
                            + "and a GC pass every interval cost ZERO object-store "
                            + "requests. A frame that read the store would cost one per "
                            + "node per interval; a pass that LISTED to find out whether "
                            + "anything expired would cost one per pod per interval, "
                            + "forever", CONSUMERS, INTERVALS)
                    .isZero();
            assertThat(grants.grantsIssued())
                    .as("and no grant: a watermark is not a read, and nothing on this path "
                            + "authorises one")
                    .isZero();
            assertThat(watermarks.size())
                    .as("PREMISE: the table really holds the streams the frames named, or "
                            + "the zeros are about a path nothing travelled")
                    .isEqualTo(4);
            assertThat(reported)
                    .as("PREMISE: and the pass really ran")
                    .isNotEmpty();
            assertThat(reported.get(0))
                    .as("PREMISE: the boundary really is the chain's own stream")
                    .containsEntry(RunKey.ofIndexUuid(IDLE_UUID, 0), 0L);
            assertThat(consulted.get())
                    .as("PREMISE: and the pass ran through the WATERMARK branch rather "
                            + "than returning at the time floor -- the segments are seven "
                            + "hours old, so what keeps them is the frozen consumer, which "
                            + "is the steady state on an idle cluster and the branch a "
                            + "store request would hide in")
                    .isEqualTo((long) INTERVALS * 4);
            assertThat(alarms).as("nothing crossed the ceiling in seven idle hours").isEmpty();
            assertThat(delivered.get())
                    .as("PREMISE: and the consumers really were idle")
                    .isZero();
        } finally {
            for (AutoCloseable handle : handles) {
                handle.close();
            }
        }
    }

    /**
     * Four segments, SEVEN HOURS OLD, kept by the watermark rather than by the
     * time floor.
     *
     * <p>⚠️ THE AGE IS THE POINT, and review MEASURED the young version passing
     * for the wrong reason: dated at "now", every pass returned at the time
     * floor and the watermark branch — the one that runs in steady state on an
     * idle cluster, once the oldest data is past `minRetention` — was never
     * reached at all. A store request added on that branch survived the whole
     * module.
     *
     * <p>⚠️ NO OBJECT IS WRITTEN. Nothing is deleted here, so the chain is all
     * GC reads — and putting the fixture's own segments through the counted
     * store would spend the budget before the loop began.
     */
    private static List<CommitDelta> idleChain(TestClock clock) {
        List<CommitDelta> chain = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            String key = new SegmentKey("bins/cluster-a",
                    clock.instant().minus(Duration.ofHours(7)).toEpochMilli(), "pod1", i,
                    12).key();
            chain.add(new CommitDelta(i, List.of(
                    new SegmentCommit(key,
                            List.of(new RunCommit(
                                    RunKey.ofIndexUuid(IDLE_UUID, i), 5,
                                    i * 5L)),
                            new SegmentCommit.Attribution("pod1", "i1", i)))));
        }
        return chain;
    }

    /**
     * One node's frame for the CHAIN'S OWN four streams, moving a little each
     * interval.
     *
     * <p>⚠️ THE SAME STREAMS THE CHAIN NAMES, and review measured the version
     * that did not: with the frames reporting one uuid and the chain another,
     * the rule never consulted a watermark that existed and the two halves of
     * this case shared nothing.
     *
     * <p>⚠️ AND THE POSITION STAYS BEHIND THE SEGMENTS' END OFFSETS — the
     * stream ends at offset 19 and this never passes 3 — because a consumer
     * that caught up would let the segments be DELETED, and a pass that
     * deletes is allowed to cost a request. Idle means idle.
     */
    private static ConsumerProgress progressFrame(int interval) {
        List<ConsumerProgress.Entry> entries = new ArrayList<>();
        for (int partition = 0; partition < 4; partition++) {
            entries.add(new ConsumerProgress.Entry(IDLE_UUID, partition,
                    "alloc-" + partition, interval % 4));
        }
        return new ConsumerProgress(entries);
    }

}
