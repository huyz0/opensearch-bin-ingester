// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The run entry's lane byte carries a real value (M10.5, ADR-0074, M10
 * criterion 7).
 *
 * <p>⚠️ A RUN'S LANE IS THE MAXIMUM OF ITS RECORDS'. One partition has one
 * offset space and one run per segment, so a lane can never split a run; a
 * partition mixing lanes is scheduled at its most urgent member's pace. The
 * minimum would let one background record demote a user-visible write.
 */
class SegmentWriterLaneTest {

    private static final UUID A = UUID.fromString("00000000-0000-0000-0000-0000000000aa");
    private static final UUID B = UUID.fromString("00000000-0000-0000-0000-0000000000bb");
    private static final UUID C = UUID.fromString("00000000-0000-0000-0000-0000000000cc");

    private static SegmentRecord record(String id) {
        return new SegmentRecord(id, OpType.INDEX, OptionalLong.of(1), new byte[] {'{', '}'});
    }

    /** The mixed-lane fixture: exactly the inputs the golden was written from. */
    static byte[] mixedLaneSegment() throws Exception {
        SegmentWriter w = new SegmentWriter();
        w.add(new RunKey(A, 0), record("a1"), 1000L, (byte) -1);
        w.add(new RunKey(A, 0), record("a2"), 1001L, (byte) 2);
        w.add(new RunKey(A, 0), record("a3"), 1002L, (byte) 0);
        w.add(new RunKey(B, 0), record("b1"), 1003L, (byte) -2);
        w.add(new RunKey(C, 3), record("c1"), 1004L, (byte) 1);
        return w.toByteArray(1_790_000_000_000L);
    }

    private static byte[] golden() throws Exception {
        try (var in = SegmentWriterLaneTest.class.getResourceAsStream(
                "/golden/segment-v1-lanes.bin")) {
            assertThat(in).as("golden/segment-v1-lanes.bin must be on the test classpath")
                    .isNotNull();
            return in.readAllBytes();
        }
    }

    @Test
    void aRunsLaneIsTheMaximumOfItsRecordsLanesAndRoundTrips() throws Exception {
        SegmentReader r = SegmentReader.open(mixedLaneSegment());

        assertThat(r.directory()).extracting(RunEntry::key, RunEntry::lane).containsExactly(
                org.assertj.core.groups.Tuple.tuple(new RunKey(A, 0), (byte) 2),
                org.assertj.core.groups.Tuple.tuple(new RunKey(B, 0), (byte) -2),
                org.assertj.core.groups.Tuple.tuple(new RunKey(C, 3), (byte) 1));
        assertThat(r.read(r.find(new RunKey(A, 0)).orElseThrow()))
                .as("⚠️ A LANE NEVER REORDERS A PARTITION: arrival order within the run")
                .extracting(SegmentRecord::id).containsExactly("a1", "a2", "a3");

        // The three-argument add is lane 0, and a lane-0 record lifts a
        // negative run to 0 -- the maximum, never the minimum.
        SegmentWriter w = new SegmentWriter();
        w.add(new RunKey(B, 0), record("y"), 1L, (byte) -1);
        w.add(new RunKey(B, 0), record("z"), 1L);
        w.add(new RunKey(C, 0), record("q"), 1L, (byte) -1);
        assertThat(SegmentReader.open(w.toByteArray(1L)).directory())
                .extracting(RunEntry::lane).containsExactly((byte) 0, (byte) -1);
    }

    @Test
    void theWriterReproducesTheMixedLaneGoldenByteForByte() throws Exception {
        // ⚠️ Do NOT regenerate the golden to make this pass: a difference means
        // the v1 layout moved, which is a wire-format change with its own ADR.
        assertThat(mixedLaneSegment()).isEqualTo(golden());
    }
}
