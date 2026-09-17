// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The bytes of a forwarded commit, pinned (M8.33, `wire-format-change`).
 *
 * <p>⚠️ A ROUND-TRIP TEST CANNOT SEE A FORMAT CHANGE. Encode-then-decode
 * passes for any self-consistent pair of methods, including one that swaps
 * {@code flushSeq} and a record count — the writer and the reader move together
 * while every request in flight between two pods running different builds
 * commits the wrong number of records under the wrong flush. A stored file is
 * the only thing that notices.
 *
 * <p>⚠️ EVERY VALUE IN THE FIXTURE IS DISTINCT, deliberately, because equal
 * values are where a field swap hides: two different uuids, two different
 * partitions, two different counts, and a {@code flushSeq} equal to none of
 * them.
 */
class GoldenCommitRequestTest {

    private static final UUID LOGS = UUID.fromString("00000000-0000-4000-8000-000000000001");
    private static final UUID METRICS = UUID.fromString("00000000-0000-4000-8000-000000000002");

    private static byte[] golden(String name) throws IOException {
        try (var in = GoldenCommitRequestTest.class.getResourceAsStream("/golden/" + name)) {
            assertThat(in).as("missing golden file %s", name).isNotNull();
            return in.readAllBytes();
        }
    }

    private static CommitRequestFrame fixture() {
        // ⚠️ INSERTED IN THE ORDER THE SORT MUST UNDO. With the streams
        // inserted already-sorted, an encoder that dropped the sort produced
        // these exact bytes and the golden stayed GREEN -- measured. The
        // fixture only discriminates if its insertion order is the wrong one.
        Map<RunKey, Integer> counts = new LinkedHashMap<>();
        counts.put(new RunKey(METRICS, 7), 41822);
        counts.put(new RunKey(LOGS, 3), 100);
        return new CommitRequestFrame("poda", "inc-7f3c", 42,
                "bins/cluster-a/data/2026/09/18/12/0000000000000000123-poda-0000000000000042-h11-A.bseg",
                counts);
    }

    @Test
    void aREQUESTEncodesToItsStoredBytes() throws Exception {
        assertThat(fixture().encode())
                .as("the bytes a live pod forwards are the bytes already stored")
                .isEqualTo(golden("commit-request-v1.bin"));
    }

    @Test
    void theSTOREDRequestStillDecodesToWhatItMeant() throws Exception {
        CommitRequestFrame decoded = CommitRequestFrame.decode(golden("commit-request-v1.bin"));

        assertThat(decoded.podId()).isEqualTo("poda");
        assertThat(decoded.incarnationId())
                .as("⚠️ ADR-0036's half of the idempotency key, and the one a reader is "
                        + "most likely to drop as noise")
                .isEqualTo("inc-7f3c");
        assertThat(decoded.flushSeq()).isEqualTo(42);
        assertThat(decoded.segmentKey()).isEqualTo(fixture().segmentKey());
        assertThat(decoded.recordCounts())
                .containsEntry(new RunKey(LOGS, 3), 100)
                .containsEntry(new RunKey(METRICS, 7), 41822)
                .hasSize(2);
    }

    @Test
    void aSINGLEStreamRequestEncodesToItsStoredBytes() throws Exception {
        CommitRequestFrame one = new CommitRequestFrame("podb", "inc-0", 0, "seg-0",
                Map.of(new RunKey(LOGS, 0), 1));
        assertThat(one.encode())
                .as("⚠️ THE SMALLEST LEGAL REQUEST, stored too: the zero flushSeq and the "
                        + "single-entry count are where an off-by-one in the varints shows")
                .isEqualTo(golden("commit-request-one-v1.bin"));
    }
}
