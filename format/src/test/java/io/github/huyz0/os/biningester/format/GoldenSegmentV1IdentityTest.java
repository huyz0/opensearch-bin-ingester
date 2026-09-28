// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * M10 criterion 7's byte identity (M11.17, H12): a segment written TODAY
 * with every record at lane 0 is byte-for-byte the M9 golden
 * {@code segment-v1.bin}. The lane byte became real in M10 and must cost an
 * all-lane-0 writer nothing -- not one byte of the directory or the records.
 *
 * <p>⚠️ THE INPUTS ARE THE GOLDEN'S OWN, read back from it once and written
 * down here: two runs, {@code doc-1} INDEX v1 {@code {"n":1}} and
 * {@code doc-2} DELETE v2 in A's, {@code doc-3} CREATE v9 with 200 bytes of
 * {@code x} in B's, created at 1,700,000,000,000. Do NOT regenerate the
 * golden to make this pass: a difference is a wire-format change.
 */
class GoldenSegmentV1IdentityTest {

    private static final UUID A = UUID.fromString("00000000-0000-0000-0000-0000000000aa");
    private static final UUID B = UUID.fromString("00000000-0000-0000-0000-0000000000bb");

    private static byte[] golden() throws Exception {
        try (var in = GoldenSegmentV1IdentityTest.class.getResourceAsStream(
                "/golden/segment-v1.bin")) {
            assertThat(in).as("golden/segment-v1.bin must be on the test classpath").isNotNull();
            return in.readAllBytes();
        }
    }

    private static SegmentRecord doc(String id, OpType op, long version, String payload) {
        return new SegmentRecord(id, op, OptionalLong.of(version),
                payload.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void aFreshAllLaneZeroWriteEqualsTheM9GoldenByteForByte() throws Exception {
        for (boolean explicitLane : new boolean[] {false, true}) {
            SegmentWriter w = new SegmentWriter();
            SegmentRecord[] a = {doc("doc-1", OpType.INDEX, 1, "{\"n\":1}"),
                    doc("doc-2", OpType.DELETE, 2, "")};
            long[] at = {1000L, 1001L};
            for (int i = 0; i < a.length; i++) {
                if (explicitLane) {
                    w.add(new RunKey(A, 0), a[i], at[i], (byte) 0);
                } else {
                    w.add(new RunKey(A, 0), a[i], at[i]);
                }
            }
            SegmentRecord b = doc("doc-3", OpType.CREATE, 9, "x".repeat(200));
            if (explicitLane) {
                w.add(new RunKey(B, 0), b, 2000L, (byte) 0);
            } else {
                w.add(new RunKey(B, 0), b, 2000L);
            }

            assertThat(w.toByteArray(1_700_000_000_000L))
                    .as("lane 0 %s is the M9 golden, every byte",
                            explicitLane ? "named" : "by default")
                    .isEqualTo(golden());
        }
    }
}
