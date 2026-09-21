// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunEntry;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import io.github.huyz0.os.biningester.format.SegmentKey;
import io.github.huyz0.os.biningester.format.SegmentReader;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.format.SegmentWriter;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Comparator;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * NFR-13: a consumer offline for the whole retention window loses nothing, and
 * past the ceiling it loses loudly (M7.11).
 *
 * <p>⚠️ T2 AND NOT T4, DELIBERATELY. The property is OUR rule over a simulated
 * six hours: a real node adds no evidence about whether
 * {@link RetentionRule} keeps what it must, and it costs six hours to find out.
 * What a real node WOULD add is whether OpenSearch resumes from where it said
 * it was, which is ADR-0005's territory and measurement M5's (M7.14).
 *
 * <p>⚠️ THE ASSERTION IS BY COUNT AND BY CONTENT. A resume that reads nothing
 * and throws nothing passes an absence-only case, which is the shape this
 * milestone's own SPEC warns about.
 */
class ConsumerOutageToleranceTest {

    private static final UUID INDEX = new UUID(0x1111_2222_3333_4444L, 1);
    private static final RunKey STREAM = new RunKey(INDEX, 0);

    private static final Duration MIN_RETENTION = Duration.ofHours(6);
    private static final Duration MAX_RETENTION = Duration.ofHours(24);
    private static final Duration FLUSH = Duration.ofMinutes(15);
    private static final int RECORDS_PER_SEGMENT = 5;

    private static final class TestClock extends Clock {
        private Instant now = Instant.parse("2026-09-17T00:00:00Z");

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

        void advance(Duration by) {
            now = now.plus(by);
        }
    }

    private final TestClock clock = new TestClock();
    private final MemoryBinStore store = new MemoryBinStore();
    private final List<String> alarms = new ArrayList<>();
    private final List<CommitDelta> chain = new ArrayList<>();
    private final Map<String, Long> firstOffsetOf = new LinkedHashMap<>();
    private long nextOffset;
    private long nextSequence;

    /** Writes one segment of five records and commits it into the chain. */
    private String flushOne() throws Exception {
        SegmentWriter writer = new SegmentWriter();
        for (int i = 0; i < RECORDS_PER_SEGMENT; i++) {
            String id = "r" + (nextOffset + i);
            writer.add(STREAM, new SegmentRecord(id, OpType.INDEX, OptionalLong.of(1),
                    ("{\"id\":\"" + id + "\"}").getBytes(StandardCharsets.UTF_8)),
                    clock.instant().toEpochMilli());
        }
        String key = new SegmentKey("bucket", clock.instant().toEpochMilli(), "poda",
                nextSequence, 12).key();
        store.put(key, Body.ofBytes(writer.toByteArray(clock.instant().toEpochMilli())));
        chain.add(new CommitDelta(nextSequence, List.of(new SegmentCommit(key,
                List.of(new RunCommit(STREAM, RECORDS_PER_SEGMENT, nextOffset)),
                new SegmentCommit.Attribution("poda", "i1", nextSequence)))));
        firstOffsetOf.put(key, nextOffset);
        nextOffset += RECORDS_PER_SEGMENT;
        nextSequence++;
        return key;
    }

    /** Every record id still readable from the store, in offset order. */
    private List<String> readableIds() throws Exception {
        List<String> ids = new ArrayList<>();
        List<String> keys = new ArrayList<>(firstOffsetOf.keySet());
        keys.sort(Comparator.comparingLong(firstOffsetOf::get));
        for (String key : keys) {
            if (store.stat(key).isEmpty()) {
                continue;
            }
            SegmentReader reader = SegmentReader.open(store.get(key).readAllBytes());
            Optional<RunEntry> run = reader.find(STREAM);
            if (run.isEmpty()) {
                continue;
            }
            for (SegmentRecord record : reader.read(run.get())) {
                ids.add(record.id());
            }
        }
        return ids;
    }

    /** A pass over the whole chain, with the one consumer frozen at {@code watermark}. */
    private SegmentGc.Result gcPass(long watermark, List<Map<RunKey, Long>> reported) {
        RetentionRule rule = new RetentionRule(clock, MIN_RETENTION, MAX_RETENTION, 0,
                (key, age) -> alarms.add(key),
                stream -> new WatermarkTable.Watermark(true, true, watermark));
        return new RetentionPass(new SegmentGc(store, rule, 1000), reported::add)
                .run(chain, Map.of(STREAM, nextOffset));
    }

    @Test
    void aConsumerOfflineForTheWHOLERetentionWindowLosesNOTHING() throws Exception {
        List<String> written = new ArrayList<>();
        List<Map<RunKey, Long>> reported = new ArrayList<>();
        // ⚠️ SIX HOURS PLUS AN HOUR, at a flush every 15 minutes, with a GC
        // pass after every one and the one consumer frozen at offset 0
        // throughout. The extra hour is what makes this a test: stopping AT six
        // hours leaves every segment inside the time floor, so a rule that
        // deleted on AGE ALONE would keep everything too and the case would
        // pass for the wrong reason. Past the floor, the frozen watermark is
        // the only thing holding the oldest segments.
        Duration window = MIN_RETENTION.plus(Duration.ofHours(1));
        for (Duration elapsed = Duration.ZERO; elapsed.compareTo(window) <= 0;
                elapsed = elapsed.plus(FLUSH)) {
            flushOne();
            for (int i = 0; i < RECORDS_PER_SEGMENT; i++) {
                written.add("r" + (nextOffset - RECORDS_PER_SEGMENT + i));
            }
            gcPass(0, reported);
            clock.advance(FLUSH);
        }
        assertThat(written).hasSize(145);
        assertThat(readableIds())
                .as("⚠️ BY COUNT AND BY CONTENT. The consumer has read nothing for six "
                        + "hours and every record it has not read is still there -- which "
                        + "is what the outage budget IS (NFR-13), and a rule that deleted "
                        + "on age alone would have collected the first twenty-five of "
                        + "these an hour ago")
                .containsExactlyElementsOf(written);
        assertThat(alarms).as("and nothing has crossed the ceiling").isEmpty();
    }

    @Test
    void aConsumerThatKEEPSUpLetsItsREADRecordsGo() throws Exception {
        List<Map<RunKey, Long>> reported = new ArrayList<>();
        for (int i = 0; i < 40; i++) {
            flushOne();
            clock.advance(FLUSH);
        }
        // The consumer has read everything; six hours have passed for the
        // oldest segments.
        SegmentGc.Result result = gcPass(nextOffset, reported);
        assertThat(result.deleted())
                .as("⚠️ THE OTHER DIRECTION, WITHOUT WHICH A KEEP-EVERYTHING RULE PASSES "
                        + "THE CASE ABOVE: records past the floor that every copy has read "
                        + "must actually go, or retention is not a cost dial at all. "
                        + "⚠️ AN EXACT COUNT RATHER THAN `isPositive`, which review "
                        + "MEASURED as satisfied by a pass that deleted ONE key of "
                        + "sixteen: 40 flushes at 15 minutes is ten hours, so the 16 "
                        + "segments older than the six-hour floor go and the 24 inside it "
                        + "stay")
                .isEqualTo(16);
        List<String> left = readableIds();
        List<String> everWritten = new ArrayList<>();
        for (long o = 0; o < nextOffset; o++) {
            everWritten.add("r" + o);
        }
        assertThat(left)
                .as("what is left is a SUFFIX -- the NEWEST records -- never a hole in the "
                        + "middle: retention deletes from the oldest end, and a rule that "
                        + "left a hole would make an offset inside the window unreadable "
                        + "while the ones around it worked")
                .isEqualTo(everWritten.subList(everWritten.size() - left.size(),
                        everWritten.size()));
        assertThat(alarms).isEmpty();
    }

    @Test
    void PASTTheCeilingTheDataGoesAndTheAlarmFIRES() throws Exception {
        List<Map<RunKey, Long>> reported = new ArrayList<>();
        String oldest = flushOne();
        clock.advance(MAX_RETENTION.plus(Duration.ofMinutes(1)));
        SegmentGc.Result result = gcPass(0, reported);
        assertThat(result.ceilingDeleted())
                .as("a consumer frozen at 0 for twenty-four hours pins retention forever, "
                        + "which is what the ceiling exists to stop")
                .isEqualTo(1);
        assertThat(alarms)
                .as("⚠️ AND IT IS LOUD. Crossing maxRetention means a consumer WILL lose "
                        + "data, and deleting it quietly is worse than the storage bill it "
                        + "prevents")
                .containsExactly(oldest);
        assertThat(readableIds()).isEmpty();
    }

    @Test
    void theBOUNDARYAConsumerWouldBeREFUSEDAgainstIsREPORTED() throws Exception {
        List<Map<RunKey, Long>> reported = new ArrayList<>();
        flushOne();
        flushOne();
        clock.advance(MAX_RETENTION.plus(Duration.ofMinutes(1)));
        gcPass(0, reported);
        assertThat(reported.get(reported.size() - 1).get(STREAM))
                .as("⚠️ EVERYTHING IS GONE, SO THE STREAM NOW STARTS WHERE THE NEXT RECORD "
                        + "WILL BE WRITTEN, and that is the number a returning consumer is "
                        + "refused against. ⚠️ THE REFUSAL ITSELF IS ASSERTED IN "
                        + "`RetainedOffsetRefusalTest` (M7.16) RATHER THAN HERE, because "
                        + "`ingest` does not depend on `client` and must not: the module "
                        + "seam is what keeps the consumer library free of the ingester's "
                        + "internals. What this case owns is that the boundary REACHES the "
                        + "reporting path at all -- without it the refusal has nothing to "
                        + "fire on")
                .isEqualTo(nextOffset);
    }
}
