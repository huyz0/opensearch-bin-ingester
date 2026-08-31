// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;

/** ⚠️ The flush interval is the primary cost dial (R1b), so its bounds are a gate. */
class IngestConfigTest {

    @Test
    void theDefaultsAreM1sStatedOperatingPoint() {
        IngestConfig c = IngestConfig.defaults("cluster-a");
        // ⚠️ 250 ms is the point R1b prices at ~$311/month against $15.55 at 5 s.
        // M1 accepts that KNOWINGLY; if this number drifts, the cost model in
        // every milestone document is describing a different system.
        assertThat(c.flushInterval()).isEqualTo(Duration.ofMillis(250));
        assertThat(c.maxSegmentBytes()).isEqualTo(8L * 1024 * 1024);
        assertThat(c.trustDomain()).isEqualTo("cluster-a");
    }

    @Test
    void aZeroFlushIntervalIsRefused() {
        // ⚠️ Zero means one PUT per append, which is a request rate that scales
        // with RECORDS -- the shape non-negotiable 6 forbids outright.
        assertThatThrownBy(() -> new IngestConfig(Duration.ZERO, 1024, "d"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IngestConfig(Duration.ofMillis(-1), 1024, "d"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aNonPositiveSegmentSizeIsRefused() {
        assertThatThrownBy(() -> new IngestConfig(Duration.ofMillis(250), 0, "d"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IngestConfig(Duration.ofMillis(250), -1, "d"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aBlankTrustDomainIsRefused() {
        // ⚠️ Not a label: segments are never bundled across trust domains
        // (ADR-0021), so a blank one would silently share a stream.
        assertThatThrownBy(() -> new IngestConfig(Duration.ofMillis(250), 1024, "  "))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IngestConfig(Duration.ofMillis(250), 1024, null))
                .isInstanceOf(NullPointerException.class);
    }
    @Test
    void aNonPositiveQueuedPushBudgetIsRefused() {
        // ⚠️ The sibling guards each have a case; this one shipped without.
        // Deleting it left the whole suite green.
        assertThatThrownBy(() -> new IngestConfig(Duration.ofMillis(250), 8L << 20, "d", 0L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxQueuedPushBytes");
        assertThatThrownBy(() -> new IngestConfig(Duration.ofMillis(250), 8L << 20, "d", -1L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void theDefaultQueuedPushBudgetIs64MiB() {
        // ⚠️ Pinned because it is a HEAP bound: a quarter of the 256 MB
        // criterion 8 budgets. Raising it silently is how the ingester starts
        // dying of an OutOfMemoryError under a slow subscriber.
        assertThat(IngestConfig.DEFAULT_MAX_QUEUED_PUSH_BYTES).isEqualTo(64L * 1024 * 1024);
        assertThat(IngestConfig.defaults("d").maxQueuedPushBytes())
                .isEqualTo(IngestConfig.DEFAULT_MAX_QUEUED_PUSH_BYTES);
    }
}
