// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The bytes of a progress frame, pinned (M7.1, `wire-format-change`).
 *
 * <p>⚠️ A ROUND-TRIP TEST CANNOT SEE A FORMAT CHANGE. Encode-then-decode passes
 * for any self-consistent pair of methods, including one that swaps
 * {@code partition} and {@code consumedUpTo} — the writer and the reader move
 * together while every frame already in flight becomes a position reported for
 * the wrong stream. A stored file is the only thing that notices, and for THIS
 * format the consequence of not noticing is a deleted segment rather than a
 * failed parse.
 *
 * <p>⚠️ EVERY VALUE IN THE FIXTURE IS DISTINCT, deliberately: equal values are
 * where a field swap hides. The partitions, the positions, the uuids and the
 * allocation ids are all different from one another, so exchanging any two
 * fields of an entry changes these bytes.
 */
class GoldenConsumerProgressTest {

    private static final String LOGS = "nVzgup36TLqWp7VBBREj1w";
    private static final String METRICS = "8Gk1lQ2HRs-TvA4pZ0bXyQ";

    private static byte[] golden(String name) throws IOException {
        try (var in = GoldenConsumerProgressTest.class.getResourceAsStream("/golden/" + name)) {
            assertThat(in).as("missing golden file %s", name).isNotNull();
            return in.readAllBytes();
        }
    }

    @Test
    void aBATCHEncodesToItsStoredBytes() throws Exception {
        ConsumerProgress frame = new ConsumerProgress(List.of(
                new ConsumerProgress.Entry(LOGS, 0, "alloc-a", 41822L),
                new ConsumerProgress.Entry(LOGS, 3, "alloc-b", 17L),
                new ConsumerProgress.Entry(METRICS, 7, "alloc-c", 900_001L)));
        assertThat(frame.encode())
                .as("the bytes a live reporter produces are the bytes already stored -- a "
                        + "mismatch is a format change, and the question is whether every "
                        + "reader moved with it")
                .isEqualTo(golden("consumer-progress-v1.bin"));
    }

    @Test
    void aSTOREDBatchStillDecodesToWhatItMeant() throws Exception {
        assertThat(ConsumerProgress.decode(golden("consumer-progress-v1.bin")).entries())
                .as("the reader half of the pin: bytes written by an older build must "
                        + "still mean what they meant, because a frame in flight during a "
                        + "rolling deploy is read by the new one")
                .containsExactly(
                        new ConsumerProgress.Entry(LOGS, 0, "alloc-a", 41822L),
                        new ConsumerProgress.Entry(LOGS, 3, "alloc-b", 17L),
                        new ConsumerProgress.Entry(METRICS, 7, "alloc-c", 900_001L));
    }

    @Test
    void theSTOREDZeroFrameStillDecodesToZERO() throws Exception {
        assertThat(ConsumerProgress.decode(golden("consumer-progress-zero-v1.bin")).entries())
                .as("the reader half of the zero pin: a stored frame from a booting shard "
                        + "must still mean 'has consumed nothing', because that is the copy "
                        + "whose whole stream must be kept")
                .containsExactly(new ConsumerProgress.Entry(LOGS, 0, "alloc-a", 0L));
    }

    @Test
    void aSINGLEEntryAtZEROEncodesToItsStoredBytes() throws Exception {
        ConsumerProgress fresh = new ConsumerProgress(
                List.of(new ConsumerProgress.Entry(LOGS, 0, "alloc-a", 0L)));
        assertThat(fresh.encode())
                .as("the booting shard's frame: a zero position and a zero partition are "
                        + "the two values a varint encoder is most likely to special-case")
                .isEqualTo(golden("consumer-progress-zero-v1.bin"));
    }
}
