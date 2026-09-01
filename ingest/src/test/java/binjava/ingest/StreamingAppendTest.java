// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import static binjava.ingest.IngestTestSupport.appendOnce;
import static binjava.ingest.IngestTestSupport.ingest;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.binstore.CountingBinStore;
import binjava.binstore.backend.MemoryBinStore;
import binjava.format.OpType;
import binjava.format.SegmentRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * M1.7b: {@code Ingest.append} takes a {@code RecordSource}, not a
 * {@code List}, so a caller streams records in rather than collecting them
 * first (java-style.md rule 8). Split out of {@code DefaultIngestTest} at the
 * 500-line limit (code-structure.md rule 1).
 *
 * <p>⚠️ EVERY test here has a deadline, the same reason {@code DefaultIngestTest}
 * gives: {@code append} blocks until a flush carries its records, and a
 * regression here is a HANG, not a failed assertion.
 */
@Timeout(30)
class StreamingAppendTest {

    /**
     * ⚠️ THE MUTATION THIS EXISTS TO KILL: {@code bufferedPerStream} updated
     * once after {@code forEachRecord} returns, instead of once per record.
     * A {@code RecordSource} that fails partway through hands SOME records to
     * the accumulator before throwing (Ingest.append's javadoc: safe, because
     * of external versioning) -- but if the per-stream buffered count is not
     * kept in sync as that happens, the NEXT append to the same stream reads a
     * stale {@code before} and is handed an offset slice that does not match
     * what the eventual commit assigns.
     */
    @Test
    void recordsAlreadyHandedToTheAccumulatorSurviveASourceThatFailsPartway() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        try (DefaultIngest ingest = ingest(store)) {
            RuntimeException boom = new RuntimeException("simulated mid-stream failure");
            assertThatThrownBy(() -> ingest.append(IngestTestSupport.PRINCIPAL, "logs", 0, sink -> {
                sink.accept(doc("a"));
                sink.accept(doc("b"));
                throw boom;
            })).isSameAs(boom);

            assertThat(ingest.pendingAppends())
                    .as("the failed append registered no waiter of its own")
                    .isZero();

            // ⚠️ THE ASSERTION: this SECOND, successful append must be handed
            // offsets starting AFTER the 2 records the failed append already
            // buffered -- [2, 5), not [0, 3) as if those 2 had never happened.
            AppendResult result = appendOnce(ingest, "logs", 0, 3);
            assertThat(result.firstOffset())
                    .as("offsets after 2 records already buffered by the failed append")
                    .isEqualTo(2L);
            assertThat(result.recordCount()).isEqualTo(3);
            assertThat(result.lastOffset()).isEqualTo(4L);
        }
    }

    private static SegmentRecord doc(String id) {
        return new SegmentRecord(id, OpType.INDEX, java.util.OptionalLong.of(1),
                "{}".getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }
}
