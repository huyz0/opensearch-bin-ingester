// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static io.github.huyz0.os.biningester.ingest.IngestTestSupport.appendOnce;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The flush spacing in force, as the cost governor reads it from the pod
 * (M10.11, ADR-0075 §3).
 *
 * <p>⚠️ **THE ADAPTIVE INTERVAL, NOT THE FLOOR.** A governor told the floor for
 * ever expects 240 data PUTs a minute where the controller has lengthened to
 * 12, and a per-record regression at a low rate then reads as a fifth of what
 * it is -- under the halt it exists to trip.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class DefaultIngestFlushSpacingTest {

    @Test
    void aFRESHIngestAnswersItsFLOORAndALengthenedOneItsCEILING() throws Exception {
        // ⚠️ A ZERO LENGTHEN DELAY, so one near-empty flush lengthens the
        // interval at once: the fill ratio is far under the low threshold.
        IngestConfig config = new IngestConfig(Duration.ofMillis(250), 8L << 20, "cluster-a",
                IngestConfig.DEFAULT_MAX_QUEUED_PUSH_BYTES, Duration.ofSeconds(5),
                IngestConfig.DEFAULT_FILL_RATIO_LOW_THRESHOLD,
                IngestConfig.DEFAULT_FILL_RATIO_HIGH_THRESHOLD, Duration.ZERO,
                IngestConfig.DEFAULT_INTERVAL_SHORTEN_DELAY);
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        try (DefaultIngest ingest = new DefaultIngest(config, store, IngestTestSupport.PREFIX,
                "pod1", IngestTestSupport.sequencer(store, "pod1"), new SubscriptionHub(),
                Clock.systemUTC(), index -> IngestTestSupport.LOGS)) {
            assertThat(ingest.flushSpacingMillis())
                    .as("every pod starts at the floor (ADR-0017)").isEqualTo(250);

            appendOnce(ingest, "logs", 0, 3);

            // ⚠️ A DEADLINE, NOT A SLEEP: the flushed buffer's adapted interval
            // is carried back to the active one after the append is answered.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (ingest.flushSpacingMillis() != 5000 && System.nanoTime() < deadline) {
                Thread.onSpinWait();
            }
            assertThat(ingest.flushSpacingMillis())
                    .as("⚠️ THE LENGTHENED INTERVAL IS THE SPACING IN FORCE").isEqualTo(5000);
        }
    }
}
