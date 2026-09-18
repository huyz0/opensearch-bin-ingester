// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.binstore.Body;
import binjava.binstore.CountingBinStore;
import binjava.binstore.StoreCounts;
import binjava.binstore.backend.MemoryBinStore;
import binjava.format.CommitDelta;
import binjava.format.RunKey;
import binjava.format.SegmentKey;
import binjava.sequencer.CommitLog;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A retention pass reads the chain from MEMORY, and costs no request for it
 * (M8.3, M7.25, NFR-3, cost rules R1 and R2).
 *
 * <p>⚠️ **THIS IS THE CRITERION M7 COULD NOT WRITE.** M7 asserted "0 LIST, 0
 * GET" around {@code SegmentGc}, which takes a {@code List<CommitDelta>} it is
 * handed — so the assertion was true of `SegmentGc` itself and said nothing about a
 * PASS. Nothing in {@code src/main} could produce that list at any price, and
 * the answer within reach was the recovery walk: one LIST per 1,000 deltas plus
 * one GET per delta, on every pass, for ever.
 *
 * <p>⚠️ **THE PREMISES ARE ASSERTED, because a zero over an empty path is not
 * evidence.** The chain really holds three deltas, the segments really are
 * seven hours old, and the pass really deletes — the reads being zero is then a
 * fact about where the chain came from rather than about a pass that did
 * nothing.
 */
class ChainSourcedPassTest {

    private static final UUID INDEX = new UUID(0x1111_2222_3333_4444L, 1);
    private static final RunKey STREAM = new RunKey(INDEX, 0);

    private static final class TestClock extends Clock {
        private final Instant now = Instant.parse("2026-09-17T12:00:00Z");

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

    private final TestClock clock = new TestClock();

    private static Map<RunKey, Integer> counts(int records) {
        Map<RunKey, Integer> m = new LinkedHashMap<>();
        m.put(STREAM, records);
        return m;
    }

    @Test
    void aPASSOverTheINMEMORYChainCostsZEROLISTAndZEROGET() throws Exception {
        try (MemoryBinStore backing = new MemoryBinStore()) {
            CountingBinStore store = new CountingBinStore(backing);
            CommitLog log = new CommitLog(store, "chain/", 0);

            List<String> segments = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                String key = new SegmentKey("bucket",
                        clock.instant().minus(Duration.ofHours(7)).toEpochMilli(),
                        "poda", i, 12).key();
                store.put(key, Body.ofBytes(new byte[] {1}));
                segments.add(key);
                log.commit(key, counts(5));
            }

            List<CommitDelta> chain = log.chain().snapshot().deltas();
            assertThat(chain)
                    .as("⚠️ THE PREMISE: the chain really holds what was committed, and the "
                            + "pass below is reading THAT rather than an empty list")
                    .hasSize(3);

            // ⚠️ THE METER GOES ROUND THE PASS AND NOTHING ELSE. Everything
            // above it -- the three segment PUTs and the three commit
            // appends -- is setup and is deliberately outside the window.
            StoreCounts before = store.counts();
            List<Map<RunKey, Long>> reported = new ArrayList<>();
            RetentionRule rule = new RetentionRule(clock, Duration.ofHours(6),
                    Duration.ofHours(24), 0, (key, age) -> { },
                    stream -> new WatermarkTable.Watermark(true, true, 15L));
            SegmentGc.Result result = new RetentionPass(new SegmentGc(store, rule, 1000),
                    reported::add).run(chain, Map.of(STREAM, 15L));
            StoreCounts after = store.counts();

            assertThat(result.deleted())
                    .as("⚠️ AND THE PASS REALLY WORKED: a consumer at 15 has read every "
                            + "record of all three segments, and they are seven hours old")
                    .isEqualTo(3);
            assertThat(after.lists() - before.lists())
                    .as("⚠️ ZERO LIST. The alternative in the tree before this row was the "
                            + "recovery walk, at one LIST per 1,000 deltas -- per pass, for "
                            + "ever, on a loop that runs on a timer")
                    .isZero();
            assertThat(after.gets() - before.gets())
                    .as("⚠️ ZERO GET. The same walk costs one GET per delta, which grows "
                            + "with the length of the term rather than with the work")
                    .isZero();
            assertThat(after.stats() - before.stats())
                    .as("⚠️ AND ZERO STAT, or the pass would be paying per delta by another "
                            + "name")
                    .isZero();
            assertThat(after.deletes() - before.deletes())
                    .as("⚠️ THE DELETES ARE THE WORK AND ARE BATCHED: one request for the "
                            + "three condemned segments, which is cost rule R7")
                    .isEqualTo(1);
        }
    }

    @Test
    void aPASSAfterARESTARTReadsTheChainTheLOGHolds() throws Exception {
        try (MemoryBinStore backing = new MemoryBinStore()) {
            CommitLog writer = new CommitLog(backing, "chain/", 0);
            for (int i = 0; i < 2; i++) {
                String key = new SegmentKey("bucket",
                        clock.instant().minus(Duration.ofHours(7)).toEpochMilli(),
                        "poda", i, 12).key();
                backing.put(key, Body.ofBytes(new byte[] {1}));
                writer.commit(key, counts(5));
            }

            // ⚠️ A NEW PROCESS, WHICH IS THE CASE THAT MATTERS. A leader that
            // has just taken over has committed nothing of its own; a chain
            // accumulated from process uptime would be EMPTY, and its first GC
            // pass would condemn nothing for as long as the term lasted while
            // every segment of the term stayed billed.
            CountingBinStore counting = new CountingBinStore(backing);
            CommitLog restarted = new CommitLog(counting, "chain/", 0);
            restarted.recover();
            StoreCounts afterRecovery = counting.counts();

            List<CommitDelta> chain = restarted.chain().snapshot().deltas();
            List<Map<RunKey, Long>> reported = new ArrayList<>();
            RetentionRule rule = new RetentionRule(clock, Duration.ofHours(6),
                    Duration.ofHours(24), 0, (key, age) -> { },
                    stream -> new WatermarkTable.Watermark(true, true, 10L));
            SegmentGc.Result result = new RetentionPass(new SegmentGc(counting, rule, 1000),
                    reported::add).run(chain, Map.of(STREAM, 10L));

            assertThat(result.deleted())
                    .as("⚠️ THE RECOVERED CHAIN IS A REAL ONE: the pass condemns the "
                            + "segments the LOG describes, not the ones this process watched")
                    .isEqualTo(2);
            assertThat(counting.counts().lists() - afterRecovery.lists())
                    .as("⚠️ AND THE PASS ITSELF STILL READS NOTHING. The recovery walk is "
                            + "paid ONCE, at startup, where R2 permits it")
                    .isZero();
            assertThat(counting.counts().gets() - afterRecovery.gets()).isZero();
        }
    }
}
