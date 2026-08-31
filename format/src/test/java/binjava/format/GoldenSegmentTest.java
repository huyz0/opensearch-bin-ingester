// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A segment written by an EARLIER BUILD must still parse.
 *
 * <p>⚠️ This is the one check a round-trip test cannot make. Encoding and
 * decoding with the same code agrees with itself: change a field width in both
 * and every round trip still passes, while every object already in the store
 * becomes unreadable. The bytes in {@code golden/segment-v0.bin} were written
 * once and committed; if this test fails, either the format changed — in which
 * case non-negotiable 8 applies and the format, both sides, the fakes, the
 * golden file and an ADR move together — or the reader broke.
 *
 * <p>⚠️ Do NOT regenerate this file to make the test pass. That converts the
 * only guard against a silent format break into a formality.
 */
class GoldenSegmentTest {

    private static final UUID A = UUID.fromString("00000000-0000-0000-0000-0000000000aa");
    private static final UUID B = UUID.fromString("00000000-0000-0000-0000-0000000000bb");

    private static byte[] golden() throws Exception {
        try (var in = GoldenSegmentTest.class.getResourceAsStream("/golden/segment-v0.bin")) {
            assertThat(in).as("golden/segment-v0.bin must be on the test classpath").isNotNull();
            return in.readAllBytes();
        }
    }

    @Test
    void theCommittedGoldenSegmentStillParses() throws Exception {
        SegmentReader r = SegmentReader.open(golden());
        assertThat(r.createdAtMillis()).isEqualTo(1_700_000_000_000L);
        // ⚠️ Exact directory shape: three runs, sorted, with the timestamps and
        // counts they were written with.
        assertThat(r.directory()).hasSize(3);
        assertThat(r.directory().get(0).key()).isEqualTo(new RunKey(A, 0));
        assertThat(r.directory().get(0).recordCount()).isEqualTo(2);
        assertThat(r.directory().get(0).minTimestampMillis()).isEqualTo(1000L);
        assertThat(r.directory().get(1).key()).isEqualTo(new RunKey(A, 3));
        assertThat(r.directory().get(2).key()).isEqualTo(new RunKey(B, 0));
    }

    @Test
    void everyRecordInTheGoldenSegmentDecodesToWhatItWasWrittenAs() throws Exception {
        SegmentReader r = SegmentReader.open(golden());

        List<SegmentRecord> a0 = r.read(r.find(new RunKey(A, 0)).orElseThrow());
        assertThat(a0).hasSize(2);
        assertThat(a0.get(0).id()).isEqualTo("doc-1");
        assertThat(a0.get(0).opType()).isEqualTo(OpType.INDEX);
        assertThat(a0.get(0).version()).hasValue(1);
        assertThat(new String(a0.get(0).payload(), StandardCharsets.UTF_8)).isEqualTo("{\"n\":1}");
        // ⚠️ A delete, with no payload and its op type intact.
        assertThat(a0.get(1).opType()).isEqualTo(OpType.DELETE);
        assertThat(a0.get(1).payload()).isEmpty();

        // ⚠️ A record with NO version: the absent field must still be absent.
        List<SegmentRecord> a3 = r.read(r.find(new RunKey(A, 3)).orElseThrow());
        assertThat(a3).singleElement().satisfies(rec -> assertThat(rec.version()).isEmpty());

        // ⚠️ A 200-byte id, so the multi-byte uvarint length is exercised by the
        // committed bytes and not only by a freshly written one.
        List<SegmentRecord> b0 = r.read(r.find(new RunKey(B, 0)).orElseThrow());
        assertThat(b0).singleElement().satisfies(rec -> {
            assertThat(rec.id()).hasSize(200);
            assertThat(rec.opType()).isEqualTo(OpType.CREATE);
            assertThat(rec.version()).hasValue(9);
        });
    }
}
