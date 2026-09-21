// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.format.FetchMode;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.format.SegmentWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Records this consumer was never handed are REPORTED, not skipped (M6.1,
 * FR-10, M6 criterion 7).
 *
 * <p>⚠️ THE BEHAVIOUR THESE CASES REPLACE IS SILENT DATA LOSS.
 * {@link ConsumerClient#deliver} offers into a bounded queue and drops when it
 * is full -- correct, and its javadoc says why: blocking would stall the
 * ingester's commit path for every stream on the node. What was missing is that
 * NOTHING NOTICED. The shard read offset 41 after offset 12 and indexed it, so
 * the records between were gone with no exception on any path, no counter, and
 * a shard reporting itself healthy.
 *
 * <p>⚠️ CONTIGUITY IS THE COMMIT LOG'S PROPERTY, not this test's assumption:
 * offsets are assigned per run at commit, so a delivery of {@code n} records
 * starting at {@code f} is followed by one starting at {@code f + n}.
 *
 * <p>⚠️ AND IT IS COUNTED AND LOGGED, NEVER THROWN, which the corpus decides
 * rather than this class: "Any exception thrown from {@code readNext} pauses
 * ingestion for that shard ... and requires operator intervention to resume ...
 * only throw for genuinely unrecoverable states" (research doc 02 § 6). A gap
 * is the opposite of unrecoverable -- the records still exist upstream -- so a
 * throw would turn a queue that overflowed for a second into a shard an
 * operator must restart by hand, which is a WORSE outcome than the silence it
 * replaces. An earlier draft of this file asserted the throw; round-1 review
 * found the corpus paragraph that rules it out.
 */
class DeliveryGapTest {

    private static final UUID INDEX = UUID.fromString("00000000-0000-0000-0000-0000000000aa");
    private static final RunKey KEY = new RunKey(INDEX, 0);

    private static byte[] segmentOf(String... ids) throws Exception {
        SegmentWriter w = new SegmentWriter();
        for (String id : ids) {
            w.add(KEY, new SegmentRecord(id, OpType.INDEX, OptionalLong.of(1),
                    ("{\"id\":\"" + id + "\"}").getBytes(StandardCharsets.UTF_8)), 1L);
        }
        return w.toByteArray(1L);
    }

    private static Delivery delivery(long firstOffset, String... ids) throws Exception {
        return new Delivery(KEY, "seg-" + firstOffset, ids.length, firstOffset,
                FetchMode.INLINE, segmentOf(ids));
    }

    /** ⚠️ FED, holding no subscription: the node-scoped shape since M5.62. */
    private static ConsumerClient fed(int queueCapacity) {
        return new ConsumerClient(KEY, queueCapacity, null);
    }

    private static List<String> drain(ConsumerClient client, int n) throws Exception {
        List<String> ids = new java.util.ArrayList<>();
        for (int i = 0; i < n; i++) {
            Optional<ConsumerRecord> r = client.readNext(Duration.ofMillis(50));
            if (r.isEmpty()) {
                return ids;
            }
            ids.add(r.get().record().id());
        }
        return ids;
    }

    @Test
    void aDROPPEDDeliveryIsCOUNTEDRatherThanSKIPPEDSILENTLY() throws Exception {
        // ⚠️ CAPACITY 1, so a delivery arriving while one is still queued is
        // dropped on the push path exactly as a slow consumer's would be.
        // ⚠️ AND THE CONSUMER KEEPS READING, because a gap is only REVEALED by
        // a later delivery: with everything after the first dropped there is
        // nothing to compare against and the stream is merely idle, which is
        // the shape the first draft of this case had.
        ConsumerClient client = fed(1);
        client.deliver(delivery(0, "a"));
        assertThat(drain(client, 1))
                .as("PREMISE: the first delivery was taken, or the case measures an empty queue")
                .containsExactly("a");
        client.deliver(delivery(1, "b"));
        client.deliver(delivery(2, "c"));
        assertThat(drain(client, 1))
                .as("PREMISE: the queued delivery is read, freeing the queue")
                .containsExactly("b");
        client.deliver(delivery(3, "d"));

        assertThat(drain(client, 1))
                .as("the stream KEEPS WORKING across the gap -- research doc 02 § 6: an "
                        + "exception here pauses the shard until an operator resumes it")
                .containsExactly("d");
        assertThat(client.gapsDetected())
                .as("offset 2 was dropped by THIS consumer's full queue, and handing the "
                        + "engine offset 3 after offset 1 with NOTHING recorded is the silent "
                        + "data loss this task removes")
                .isEqualTo(1);
    }

    @Test
    void theGapNAMESTheOffsetsAndSaysWhoDroppedThem() throws Exception {
        ConsumerClient client = fed(1);
        client.deliver(delivery(0, "a"));
        drain(client, 1);
        client.deliver(delivery(1, "b"));
        client.deliver(delivery(2, "c"));
        drain(client, 1);
        client.deliver(delivery(3, "d"));
        drain(client, 1);

        DeliveryGapException gap = client.lastGap().orElseThrow();

        assertThat(gap.expectedOffset())
                .as("the first offset not delivered -- offset 2 was dropped while offset 1 was "
                        + "still queued")
                .isEqualTo(2);
        assertThat(gap.receivedOffset())
                .as("the first offset that WAS delivered after the gap")
                .isEqualTo(3);
        assertThat(gap.missingRecords()).isEqualTo(1);
        assertThat(gap.droppedLocally())
                .as("this consumer's own queue dropped them, which is a capacity to raise -- "
                        + "an operator told only that records are missing cannot tell that from "
                        + "a gap upstream, where raising the capacity would change nothing")
                .isTrue();
        assertThat(gap.getMessage())
                .as("the message carries the offsets, because a report that says only 'gap' "
                        + "sends whoever reads it back to the code")
                .contains("[2, 3)");
    }

    /**
     * A gap NOT caused by a local drop is reported as such (M6.1).
     *
     * <p>⚠️ THE TWO CASES NEED DIFFERENT ANSWERS FROM AN OPERATOR. A local drop
     * means this node fell behind. A gap with nothing dropped means the records
     * never arrived -- the other end of the channel -- and no amount of queue
     * capacity here would have helped.
     */
    @Test
    void aGapWithNOLocalDropIsReportedAsUPSTREAM() throws Exception {
        // ⚠️ ROOM FOR EVERYTHING, so nothing is dropped here: the missing
        // delivery was never offered at all.
        ConsumerClient client = fed(16);
        client.deliver(delivery(0, "a"));
        client.deliver(delivery(7, "h"));
        drain(client, 2);

        DeliveryGapException gap = client.lastGap().orElseThrow();

        assertThat(client.droppedDeliveries())
                .as("PREMISE: this consumer dropped nothing, or the case is the local one again")
                .isZero();
        assertThat(gap.droppedLocally())
                .as("nothing was dropped here, so the gap is upstream and the capacity on this "
                        + "node is not the problem")
                .isFalse();
        assertThat(gap.missingRecords()).isEqualTo(6);
    }

    /**
     * An EARLIER local drop does not make a LATER upstream gap look local
     * (M6.1, round 1's major).
     *
     * <p>⚠️ MEASURED: comparing the LIFETIME drop count against zero labels
     * every gap after the first drop "local", which is the misdiagnosis this
     * type exists to prevent -- and the two have opposite remedies, one being
     * a queue capacity on this node and the other being nothing this node can
     * do at all.
     */
    @Test
    void anEARLIERLocalDropDoesNotMakeALATERUpstreamGapLookLOCAL() throws Exception {
        ConsumerClient client = fed(1);
        client.deliver(delivery(0, "a"));
        client.deliver(delivery(1, "b"));
        drain(client, 1);
        client.deliver(delivery(2, "c"));
        drain(client, 1);
        assertThat(client.lastGap().orElseThrow().droppedLocally())
                .as("PREMISE: the FIRST gap really is local, or the case proves nothing about "
                        + "telling them apart")
                .isTrue();
        assertThat(client.droppedDeliveries()).isEqualTo(1);

        // ⚠️ NOTHING IS DROPPED FROM HERE ON: the queue is empty each time.
        client.deliver(delivery(9, "j"));
        drain(client, 1);

        assertThat(client.lastGap().orElseThrow().droppedLocally())
                .as("the drop was three deliveries ago and nothing was dropped IN this gap, so "
                        + "it is upstream -- a lifetime comparison says local forever")
                .isFalse();
        assertThat(client.gapsDetected()).isEqualTo(2);
    }

    @Test
    void aCONTIGUOUSStreamNeverReportsAGap() throws Exception {
        ConsumerClient client = fed(16);
        client.deliver(delivery(0, "a", "b"));
        client.deliver(delivery(2, "c"));
        client.deliver(delivery(3, "d", "e"));

        assertThat(drain(client, 5))
                .as("five records across three contiguous deliveries -- a detector that fired "
                        + "here would make every healthy stream unreadable, which is a worse "
                        + "failure than the one it is for")
                .containsExactly("a", "b", "c", "d", "e");
        assertThat(client.gapsDetected()).isZero();
        assertThat(client.lastGap()).isEmpty();
    }

    /**
     * A REPEATED window is not a gap, and does not manufacture one (M6.1).
     *
     * <p>⚠️ THE TRACKER IS NEVER REWOUND. A delivery starting BEFORE what was
     * expected is the hub re-sending a window a resubscribe already covered;
     * moving the expectation backwards would then report a gap on the next
     * ordinary delivery — inventing the defect this class exists to find.
     */
    @Test
    void aREPEATEDWindowIsNotAGapAndDoesNotManufactureOne() throws Exception {
        // ⚠️ THE REPEAT IS STRICTLY EARLIER AND SHORTER THAN WHAT HAS BEEN
        // SEEN, which round-2 review measured as the difference between a case
        // that pins the rewind and one that cannot: repeating the LAST window
        // makes `max(end, expected)` and `end` the same number, so the
        // mutation survives and the case asserts nothing it claims to.
        ConsumerClient client = fed(16);
        client.deliver(delivery(0, "a", "b"));
        client.deliver(delivery(2, "c"));
        client.deliver(delivery(0, "a", "b"));
        client.deliver(delivery(3, "d"));

        drain(client, 6);

        assertThat(client.gapsDetected())
                .as("a repeat is not a gap, and the ordinary delivery after it is contiguous "
                        + "with the FURTHEST point reached -- a tracker rewound to 2 by the "
                        + "repeat invents a gap [2, 3) on a perfectly healthy stream")
                .isZero();
    }

    /**
     * The FIRST delivery carries any offset (M6.1).
     *
     * <p>⚠️ A RESUMED SESSION ANSWERS AT THE NEXT RECORD, NOT AT THE BEGINNING
     * (M5.15b), and a consumer subscribing to a stream that has been running
     * for a week starts at that week's offset. Comparing the first delivery
     * against 0 would report a gap for every resume in the fleet.
     */
    @Test
    void theFIRSTDeliveryAfterSubscribingIsNeverAGap() throws Exception {
        ConsumerClient client = fed(16);
        client.deliver(delivery(9_000_000L, "a"));

        assertThat(drain(client, 1))
                .as("subscribing mid-stream is the normal case, not a gap")
                .containsExactly("a");
        assertThat(client.gapsDetected()).isZero();
    }

    /**
     * EVERY gap is counted, not just the first (M6.1, round 1's test major).
     *
     * <p>⚠️ MEASURED: a `gapReported` flag with an early return leaves the
     * whole suite green when every other case has at most one gap. A consumer
     * that reports the first loss and silently skips every later one is the
     * behaviour this task removes, reached by a different route.
     */
    @Test
    void EVERYGapIsCountedNotJustTheFirst() throws Exception {
        ConsumerClient client = fed(16);
        client.deliver(delivery(0, "a"));
        client.deliver(delivery(4, "e"));
        client.deliver(delivery(9, "j"));

        drain(client, 3);

        assertThat(client.gapsDetected())
                .as("two gaps, [1, 4) and [5, 9) -- reporting only the first is the same "
                        + "silence one layer along")
                .isEqualTo(2);
        assertThat(client.lastGap().orElseThrow().expectedOffset())
                .as("and the LAST one is the one held, not the first")
                .isEqualTo(5);
    }

    @Test
    void theDROPPEDCounterCountsWhatTheQueueRefused() throws Exception {
        ConsumerClient client = fed(1);
        client.deliver(delivery(0, "a"));
        client.deliver(delivery(1, "b"));
        client.deliver(delivery(2, "c"));

        assertThat(client.droppedDeliveries())
                .as("two deliveries did not fit -- a counter that never moved would say "
                        + "'upstream' about every local drop, which is the opposite diagnosis")
                .isEqualTo(2);
    }
}
