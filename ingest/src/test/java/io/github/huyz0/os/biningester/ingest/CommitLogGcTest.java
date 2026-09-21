// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CommitDelta;
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
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * GC driven from the commit log (M7.6, FR-9, cost rules R2 and R8).
 *
 * <p>⚠️ WHAT IT COSTS IS AS MUCH THE POINT AS WHAT IT DELETES. Everything GC
 * needs is already in the chain — {@code CommitDelta → SegmentCommit(segmentKey,
 * runs) → RunCommit(key, recordCount, firstOffset)} — so a segment's last offset
 * for a stream is arithmetic and its age is in the key. A LIST here costs what a
 * PUT costs (R2), a GET per segment makes collection proportional to the objects
 * being collected, and a DELETE per key throws away the 1,000-key batch that
 * makes GC essentially free.
 */
class CommitLogGcTest {

    private static final UUID INDEX = new UUID(0x1111_2222_3333_4444L, 1);
    private static final RunKey STREAM = new RunKey(INDEX, 0);
    private static final RunKey OTHER = new RunKey(INDEX, 1);

    private static final Duration MIN_RETENTION = Duration.ofHours(6);
    private static final Duration MAX_RETENTION = Duration.ofHours(24);

    private static final class TestClock extends Clock {
        private Instant now = Instant.parse("2026-09-17T12:00:00Z");

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
    private final List<String> alarms = new ArrayList<>();
    private final MemoryBinStore backing = new MemoryBinStore();
    private final CountingBinStore store = new CountingBinStore(backing);

    private RetentionRule rule(RetentionRule.Watermarks watermarks) {
        return new RetentionRule(clock, MIN_RETENTION, MAX_RETENTION, 0,
                (key, age) -> alarms.add(key), watermarks);
    }

    private static RetentionRule.Watermarks caughtUpTo(long position) {
        return stream -> new WatermarkTable.Watermark(true, true, position);
    }

    /** A segment key written {@code age} ago, and the object behind it. */
    private String segmentWritten(Duration age, long sequence) throws Exception {
        long millis = clock.instant().minus(age).toEpochMilli();
        String key = new SegmentKey("bucket", millis, "podа".replace("а", "a"), sequence, 12)
                .key();
        backing.put(key, Body.ofBytes(new byte[] {1, 2, 3}));
        return key;
    }

    private static CommitDelta delta(long sequence, List<SegmentCommit> segments) {
        return new CommitDelta(sequence, segments);
    }

    private static SegmentCommit commit(String key, RunKey stream, long firstOffset, int count) {
        return new SegmentCommit(key, List.of(new RunCommit(stream, count, firstOffset)),
                new SegmentCommit.Attribution("poda", "i1", 1));
    }

    @Test
    void anEXPIREDSegmentIsDELETEDAndTheObjectIsGONE() throws Exception {
        String key = segmentWritten(Duration.ofHours(7), 1);
        SegmentGc gc = new SegmentGc(store, rule(caughtUpTo(10_000)), 1000);
        SegmentGc.Result result = gc.collect(List.of(delta(0, List.of(commit(key, STREAM, 0, 5)))));
        assertThat(result.deleted()).isEqualTo(1);
        assertThat(backing.stat(key)).isEmpty();
    }

    @Test
    void aSegmentINSIDETheFloorIsKEPTAndSTILLThere() throws Exception {
        String key = segmentWritten(Duration.ofMinutes(20), 1);
        SegmentGc gc = new SegmentGc(store, rule(caughtUpTo(10_000)), 1000);
        SegmentGc.Result result = gc.collect(List.of(delta(0, List.of(commit(key, STREAM, 0, 5)))));
        assertThat(result.deleted()).isZero();
        assertThat(result.kept()).isEqualTo(1);
        assertThat(backing.stat(key)).isNotEmpty();
    }

    @Test
    void aSegmentWhoseOTHERStreamIsUNREADIsKEPT() throws Exception {
        String key = segmentWritten(Duration.ofHours(7), 1);
        SegmentCommit shared = new SegmentCommit(key,
                List.of(new RunCommit(STREAM, 5, 0), new RunCommit(OTHER, 5, 0)),
                new SegmentCommit.Attribution("poda", "i1", 1));
        RetentionRule.Watermarks mixed = k -> k.equals(STREAM)
                ? new WatermarkTable.Watermark(true, true, 10_000)
                : new WatermarkTable.Watermark(true, true, 2);
        SegmentGc gc = new SegmentGc(store, rule(mixed), 1000);
        assertThat(gc.collect(List.of(delta(0, List.of(shared)))).deleted())
                .as("one object, two streams, and only one of them read -- which at 1,600 "
                        + "streams per segment is the normal case")
                .isZero();
        assertThat(backing.stat(key)).isNotEmpty();
    }

    @Test
    void theENDOffsetIsFirstOffsetPlusCountMinusONE() throws Exception {
        String key = segmentWritten(Duration.ofHours(7), 1);
        // Records 100..104, so the segment's last offset is 104 and a consumer
        // that has consumed up to 105 (EXCLUSIVE) has read all of them.
        SegmentCommit five = commit(key, STREAM, 100, 5);
        assertThat(new SegmentGc(store, rule(caughtUpTo(105)), 1000)
                .collect(List.of(delta(0, List.of(five)))).deleted())
                .as("⚠️ THE INCLUSIVE/EXCLUSIVE JOIN, and one record of silent loss per "
                        + "segment if it is wrong: firstOffset 100 + count 5 - 1 = 104, "
                        + "and an exclusive 105 means every one of them is indexed")
                .isEqualTo(1);
        String second = segmentWritten(Duration.ofHours(7), 2);
        assertThat(new SegmentGc(store, rule(caughtUpTo(104)), 1000)
                .collect(List.of(delta(0, List.of(commit(second, STREAM, 100, 5))))).deleted())
                .as("and at 104 the LAST record has not been indexed yet, so the segment "
                        + "stays -- the boundary pinned from both sides")
                .isZero();
    }

    @Test
    void TWOThousandFiveHundredExpiredSegmentsCostZEROListAndTHREEDeletes() throws Exception {
        List<SegmentCommit> commits = new ArrayList<>();
        for (int i = 0; i < 2500; i++) {
            commits.add(commit(segmentWritten(Duration.ofHours(7), i), STREAM, i * 10L, 5));
        }
        long putsBefore = store.counts().puts();
        SegmentGc gc = new SegmentGc(store, rule(caughtUpTo(10_000_000)), 1000);
        SegmentGc.Result result = gc.collect(List.of(delta(0, commits)));
        assertThat(result.deleted()).isEqualTo(2500);
        assertThat(store.counts().lists())
                .as("a LIST costs what a PUT costs (R2), and the chain already says which "
                        + "objects exist -- so GC that listed would pay twice for what it "
                        + "already knows")
                .isZero();
        assertThat(store.counts().gets())
                .as("no segment is READ to be deleted: a GET per object makes collection "
                        + "proportional to the objects being collected")
                .isZero();
        assertThat(store.counts().deletes())
                .as("⚠️ THE BUDGET: <=1 DELETE CALL PER 1,000 KEYS. DeleteObjects batches "
                        + "1,000 per call, and one call per key is 2,500 requests for the "
                        + "same work")
                .isEqualTo(3);
        assertThat(store.counts().puts() - putsBefore)
                .as("and GC writes nothing")
                .isZero();
    }

    @Test
    void EXACTLYOneThousandKeysIsONECall() throws Exception {
        List<SegmentCommit> commits = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            commits.add(commit(segmentWritten(Duration.ofHours(7), i), STREAM, i * 10L, 5));
        }
        new SegmentGc(store, rule(caughtUpTo(10_000_000)), 1000)
                .collect(List.of(delta(0, commits)));
        assertThat(store.counts().deletes())
                .as("the batch boundary: 1,000 keys is one call, and an off-by-one sends a "
                        + "second call carrying one key")
                .isEqualTo(1);
    }

    @Test
    void aCEILINGDeleteALARMSAndIsCOUNTEDSeparately() throws Exception {
        String key = segmentWritten(Duration.ofHours(25), 1);
        SegmentGc.Result result = new SegmentGc(store, rule(caughtUpTo(0)), 1000)
                .collect(List.of(delta(0, List.of(commit(key, STREAM, 0, 5)))));
        assertThat(result.deleted()).isEqualTo(1);
        assertThat(result.ceilingDeleted())
                .as("a ceiling delete is an INCIDENT, not routine collection, and a result "
                        + "that folded the two together would report a healthy-looking "
                        + "number for data being lost")
                .isEqualTo(1);
        assertThat(alarms).containsExactly(key);
    }

    @Test
    void aKeyThatIsNOTASegmentKeyIsKEPTRatherThanGuessedAt() throws Exception {
        backing.put("bucket/data/not-a-segment-key", Body.ofBytes(new byte[] {1}));
        SegmentGc.Result result = new SegmentGc(store, rule(caughtUpTo(10_000)), 1000)
                .collect(List.of(delta(0,
                        List.of(commit("bucket/data/not-a-segment-key", STREAM, 0, 5)))));
        assertThat(result.deleted())
                .as("a key whose age cannot be read is a key whose retention cannot be "
                        + "judged, and the safe answer is to keep it and say so -- guessing "
                        + "an age of zero keeps it too, but guessing NOW deletes it")
                .isZero();
        assertThat(result.unreadable()).isEqualTo(1);
        assertThat(backing.stat("bucket/data/not-a-segment-key")).isNotEmpty();
    }

    @Test
    void aSegmentNAMEDTwiceInTheChainIsDeletedONCE() throws Exception {
        String key = segmentWritten(Duration.ofHours(7), 1);
        SegmentGc.Result result = new SegmentGc(store, rule(caughtUpTo(10_000)), 1000)
                .collect(List.of(delta(0, List.of(commit(key, STREAM, 0, 5))),
                        delta(1, List.of(commit(key, OTHER, 0, 5)))));
        assertThat(result.deleted())
                .as("a segment carrying runs for two streams can be named by two deltas, "
                        + "and deleting it twice is a wasted request per duplicate")
                .isEqualTo(1);
        assertThat(store.counts().deletes()).isEqualTo(1);
    }

    @Test
    void aSegmentNamedTwiceIsJudgedOnALLOfItsRuns() throws Exception {
        String key = segmentWritten(Duration.ofHours(7), 1);
        RetentionRule.Watermarks mixed = k -> k.equals(STREAM)
                ? new WatermarkTable.Watermark(true, true, 10_000)
                : new WatermarkTable.Watermark(true, true, 2);
        SegmentGc.Result result = new SegmentGc(store, rule(mixed), 1000)
                .collect(List.of(delta(0, List.of(commit(key, STREAM, 0, 5))),
                        delta(1, List.of(commit(key, OTHER, 0, 5)))));
        assertThat(result.deleted())
                .as("⚠️ THE RUNS MERGE ACROSS DELTAS. Judging each mention separately "
                        + "deletes on the first one that says yes, which is this fixture: "
                        + "stream A is read, stream B is not, and the object holds both")
                .isZero();
    }

    @Test
    void aFAILEDDeleteIsNOTCountedAndTheObjectSTAYS() throws Exception {
        String key = segmentWritten(Duration.ofHours(7), 1);
        BinStore refusing = new ForwardingIngestStore(backing) {
            @Override
            public void delete(List<String> keys) throws java.io.IOException {
                throw new java.io.IOException("the store is unreachable");
            }
        };
        SegmentGc.Result result = new SegmentGc(refusing, rule(caughtUpTo(10_000)), 1000)
                .collect(List.of(delta(0, List.of(commit(key, STREAM, 0, 5)))));
        assertThat(result.deleted())
                .as("nothing was deleted, and a count taken from the VERDICT rather than "
                        + "from the outcome would say one -- while the object is still in "
                        + "the bucket and the next pass judges it again from the same "
                        + "chain, which is why GC needs no memory of its own")
                .isZero();
        assertThat(backing.stat(key)).isNotEmpty();
    }

    @Test
    void aFAILEDDeleteDoesNotABANDONTheRestOfThePass() throws Exception {
        List<SegmentCommit> commits = new ArrayList<>();
        for (int i = 0; i < 1500; i++) {
            commits.add(commit(segmentWritten(Duration.ofHours(7), i), STREAM, i * 10L, 5));
        }
        BinStore firstBatchFails = new ForwardingIngestStore(backing) {
            private int calls;

            @Override
            public void delete(List<String> keys) throws java.io.IOException {
                if (calls++ == 0) {
                    throw new java.io.IOException("the store is unreachable");
                }
                backing.delete(keys);
            }
        };
        SegmentGc.Result result = new SegmentGc(firstBatchFails, rule(caughtUpTo(10_000_000)),
                1000).collect(List.of(delta(0, commits)));
        assertThat(result.deleted())
                .as("the second batch still goes: a throw that escaped `collect` would "
                        + "abandon 500 keys because 1,000 others could not be reached, and "
                        + "the pass would report nothing at all")
                .isEqualTo(500);
    }

    @Test
    void aFAILEDCeilingDeleteIsNOTCountedAsDataLost() throws Exception {
        String key = segmentWritten(Duration.ofHours(25), 1);
        BinStore refusing = new ForwardingIngestStore(backing) {
            @Override
            public void delete(List<String> keys) throws java.io.IOException {
                throw new java.io.IOException("the store is unreachable");
            }
        };
        SegmentGc.Result result = new SegmentGc(refusing, rule(caughtUpTo(0)), 1000)
                .collect(List.of(delta(0, List.of(commit(key, STREAM, 0, 5)))));
        assertThat(result.ceilingDeleted())
                .as("⚠️ A CEILING COUNT IS AN INCIDENT NUMBER -- data a consumer had not "
                        + "read is now GONE. Counting the verdict rather than the outcome "
                        + "reports the loss of an object that is still in the bucket")
                .isZero();
    }

    @Test
    void theSAMEStreamNamedTWICEKeepsTheHIGHESTEndOffset() throws Exception {
        String key = segmentWritten(Duration.ofHours(7), 1);
        // The same segment, the same stream, two deltas: 0..4 and 100..104.
        SegmentGc.Result result = new SegmentGc(store, rule(caughtUpTo(50)), 1000)
                .collect(List.of(delta(0, List.of(commit(key, STREAM, 0, 5))),
                        delta(1, List.of(commit(key, STREAM, 100, 5)))));
        assertThat(result.deleted())
                .as("⚠️ THE COMBINER IS WHAT THIS CASE EXISTS FOR. A consumer at 50 is "
                        + "past the FIRST mention's end (4) and nowhere near the second's "
                        + "(104), so `min` -- or a plain `put`, whose answer depends on "
                        + "delta order -- deletes an object holding unread records")
                .isZero();
    }

    @Test
    void theSAMEStreamNamedTwiceTheOTHERWayRoundKeepsTheHighestToo() throws Exception {
        String key = segmentWritten(Duration.ofHours(7), 1);
        // The HIGH mention first, so a plain `put` -- whose answer is simply
        // the LAST one -- leaves the low end and deletes.
        SegmentGc.Result result = new SegmentGc(store, rule(caughtUpTo(50)), 1000)
                .collect(List.of(delta(0, List.of(commit(key, STREAM, 100, 5))),
                        delta(1, List.of(commit(key, STREAM, 0, 5)))));
        assertThat(result.deleted())
                .as("⚠️ THE PAIR IS WHAT PINS THE COMBINER RATHER THAN THE ORDER: with the "
                        + "low mention last, `put` answers 4, the consumer at 50 is past "
                        + "it, and an object holding records 100..104 that nobody has read "
                        + "is deleted")
                .isZero();
    }

    @Test
    void anEMPTYChainDeletesNOTHINGAndCostsNOTHING() {
        SegmentGc.Result result = new SegmentGc(store, rule(caughtUpTo(10_000)), 1000)
                .collect(List.of());
        assertThat(result.deleted()).isZero();
        assertThat(store.counts().total())
                .as("⚠️ NOTHING TO COLLECT MUST COST NOTHING. A pass that issued an empty "
                        + "delete, or listed to check, would make an idle cluster pay per "
                        + "GC interval forever -- NFR-2 through the other door")
                .isZero();
    }

    @Test
    void aPassWhereEVERYTHINGIsKeptIssuesNODeleteAtAll() throws Exception {
        String key = segmentWritten(Duration.ofMinutes(20), 1);
        new SegmentGc(store, rule(caughtUpTo(10_000)), 1000)
                .collect(List.of(delta(0, List.of(commit(key, STREAM, 0, 5)))));
        assertThat(store.counts().deletes()).isZero();
    }
}
