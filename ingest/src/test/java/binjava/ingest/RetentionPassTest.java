// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.binstore.Body;
import binjava.binstore.backend.MemoryBinStore;
import binjava.format.CommitDelta;
import binjava.format.RunCommit;
import binjava.format.RunKey;
import binjava.format.SegmentCommit;
import binjava.format.SegmentKey;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A GC pass reports the boundary it moved (M7.10, FR-9).
 *
 * <p>⚠️ WITHOUT THIS THE TWO HALVES NEVER MEET, and review found exactly that:
 * `SegmentGc` knew what it deleted, `CheckpointWriter` had a method to be told,
 * and nothing in the tree connected them — so a deployed ingester would keep
 * writing `oldestRetainedOffset = 0` after every pass.
 */
class RetentionPassTest {

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
    private final MemoryBinStore store = new MemoryBinStore();
    private final List<Map<RunKey, Long>> reported = new ArrayList<>();

    private String put(Duration age, long sequence) throws Exception {
        String key = new SegmentKey("bucket", clock.instant().minus(age).toEpochMilli(),
                "poda", sequence, 12).key();
        store.put(key, Body.ofBytes(new byte[] {1}));
        return key;
    }

    private static SegmentCommit commit(String key, long firstOffset, int count) {
        return new SegmentCommit(key, List.of(new RunCommit(STREAM, count, firstOffset)),
                new SegmentCommit.Attribution("poda", "i1", 1));
    }

    private RetentionPass pass(long watermark) {
        RetentionRule rule = new RetentionRule(clock, Duration.ofHours(6), Duration.ofHours(24),
                0, (key, age) -> { }, stream -> new WatermarkTable.Watermark(true, true,
                watermark));
        return new RetentionPass(new SegmentGc(store, rule, 1000), reported::add);
    }

    @Test
    void aPassThatDELETESReportsWhereTheStreamNowSTARTS() throws Exception {
        String oldest = put(Duration.ofHours(7), 1);
        String newer = put(Duration.ofHours(7), 2);
        List<CommitDelta> chain = List.of(new CommitDelta(0,
                List.of(commit(oldest, 0, 5), commit(newer, 5, 5))));
        SegmentGc.Result result = pass(5).run(chain, Map.of(STREAM, 10L));
        assertThat(result.deleted())
                .as("a consumer at 5 has read 0..4 and nothing more, so the first segment "
                        + "goes and the second stays")
                .isEqualTo(1);
        assertThat(reported)
                .as("⚠️ AND THE BOUNDARY IS REPORTED. Without this the checkpoint keeps "
                        + "saying 0 after every pass, and a consumer seeking into "
                        + "collected records meets a 404 rather than a refusal")
                .containsExactly(Map.of(STREAM, 5L));
    }

    @Test
    void aPassThatDELETESNOTHINGStillReportsTheStreamStartsWhereItDid() throws Exception {
        String only = put(Duration.ofMinutes(20), 1);
        SegmentGc.Result result = pass(10_000)
                .run(List.of(new CommitDelta(0, List.of(commit(only, 0, 5)))),
                        Map.of(STREAM, 5L));
        assertThat(result.deleted()).isZero();
        assertThat(reported)
                .as("reporting the unchanged boundary is how a checkpoint written after a "
                        + "quiet pass still carries the truth rather than a stale value")
                .containsExactly(Map.of(STREAM, 0L));
    }

    @Test
    void aFAILEDDeleteMovesNOBoundary() throws Exception {
        String oldest = put(Duration.ofHours(7), 1);
        String newer = put(Duration.ofHours(7), 2);
        RetentionRule rule = new RetentionRule(clock, Duration.ofHours(6), Duration.ofHours(24),
                0, (key, age) -> { }, stream -> new WatermarkTable.Watermark(true, true, 5));
        RetentionPass failing = new RetentionPass(
                new SegmentGc(new FailingDeleteStore(store), rule, 1000), reported::add);
        failing.run(List.of(new CommitDelta(0,
                List.of(commit(oldest, 0, 5), commit(newer, 5, 5)))), Map.of(STREAM, 10L));
        assertThat(reported)
                .as("⚠️ WHAT WAS DELETED, NOT WHAT WAS CONDEMNED. The batch failed, so the "
                        + "records are still readable, and a boundary moved over them "
                        + "refuses a consumer for data that is there")
                .containsExactly(Map.of(STREAM, 0L));
    }

    @Test
    void aStreamWhoseEVERYSegmentGoesReportsItsNEXTOffset() throws Exception {
        String oldest = put(Duration.ofHours(7), 1);
        String newer = put(Duration.ofHours(7), 2);
        SegmentGc.Result result = pass(10_000).run(List.of(new CommitDelta(0,
                List.of(commit(oldest, 0, 5), commit(newer, 5, 5)))), Map.of(STREAM, 10L));
        assertThat(result.deleted()).isEqualTo(2);
        assertThat(reported)
                .as("⚠️ THE WHOLE-STREAM-EXPIRED PATH, and the only case that constrains "
                        + "`nextOffsets` reaching `RetainedOffsets` at all: with every "
                        + "segment gone the boundary IS nextOffset, and a pass that "
                        + "dropped the map would leave the stream out of the report, the "
                        + "checkpoint at 0, and a consumer of a fully collected stream "
                        + "meeting a 404 instead of a refusal")
                .containsExactly(Map.of(STREAM, 10L));
    }

    @Test
    void anEMPTYChainReportsNOTHINGRatherThanAnEmptyMap() {
        pass(10_000).run(List.of(), Map.of(STREAM, 10L));
        assertThat(reported)
                .as("a pass with no chain has learned nothing, and calling the sink with "
                        + "an empty map would make a checkpoint write look like a report")
                .isEmpty();
    }

    /** A store whose delete always refuses. */
    private static final class FailingDeleteStore extends ForwardingIngestStore {
        FailingDeleteStore(binjava.binstore.BinStore delegate) {
            super(delegate);
        }

        @Override
        public void delete(List<String> keys) throws java.io.IOException {
            throw new java.io.IOException("the store is unreachable");
        }
    }
}
