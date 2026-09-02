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
        // M1 accepted that KNOWINGLY; if this number drifts, the cost model in
        // every milestone document is describing a different system.
        assertThat(c.intervalFloor()).isEqualTo(Duration.ofMillis(250));
        assertThat(c.maxSegmentBytes()).isEqualTo(8L * 1024 * 1024);
        assertThat(c.trustDomain()).isEqualTo("cluster-a");
    }

    @Test
    void theDefaultsIncludeM3sAdaptiveRange() {
        // ⚠️ M3; ADR-0016 §2/ADR-0017: the ceiling ADR-0017's own $15.55/month
        // headline number is priced at, and the hysteresis band ADR-0016 §2b
        // names, carried forward unchanged in direction (see the M3 SPEC's
        // own Design section for why "scale up"/"scale down" there must NOT
        // be read literally as which way the interval moves).
        IngestConfig c = IngestConfig.defaults("cluster-a");
        assertThat(c.intervalCeiling()).isEqualTo(Duration.ofSeconds(5));
        assertThat(c.fillRatioLowThreshold()).isEqualTo(0.4);
        assertThat(c.fillRatioHighThreshold()).isEqualTo(0.9);
        assertThat(c.intervalLengthenDelay()).isEqualTo(Duration.ofMinutes(2));
        assertThat(c.intervalShortenDelay()).isEqualTo(Duration.ZERO);
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

    private static IngestConfig withAdaptiveFields(Duration ceiling, double low, double high,
            Duration lengthenDelay, Duration shortenDelay) {
        return new IngestConfig(Duration.ofMillis(250), 8L << 20, "d",
                IngestConfig.DEFAULT_MAX_QUEUED_PUSH_BYTES, ceiling, low, high, lengthenDelay,
                shortenDelay);
    }

    @Test
    void aCeilingBelowTheFloorIsRefused() {
        // ⚠️ M3: the range collapses (or inverts) if the ceiling is allowed
        // below the floor -- a config an operator could otherwise construct
        // that Accumulator would have no correct way to honor.
        assertThatThrownBy(() -> withAdaptiveFields(Duration.ofMillis(100), 0.4, 0.9,
                Duration.ofMinutes(2), Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("intervalCeiling");
    }

    @Test
    void aCeilingEqualToTheFloorIsAccepted() {
        // ⚠️ A degenerate but valid range: the interval never actually
        // adapts (floor == ceiling), which is a legitimate way to pin the
        // fixed M1 operating point under the new shape, not an error.
        IngestConfig c = withAdaptiveFields(Duration.ofMillis(250), 0.4, 0.9,
                Duration.ofMinutes(2), Duration.ZERO);
        assertThat(c.intervalCeiling()).isEqualTo(c.intervalFloor());
    }

    @Test
    void fillRatioThresholdsOutsideZeroToOneAreRefused() {
        assertThatThrownBy(() -> withAdaptiveFields(Duration.ofSeconds(5), 0.0, 0.9,
                Duration.ofMinutes(2), Duration.ZERO))
                .as("low threshold must be strictly greater than 0")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("fillRatioLowThreshold");
        // ⚠️ test-reviewer round 1: an exact message, not `.hasMessageContaining`
        // -- low=0.4/high=0.0 ALSO trips the separate low<high guard below
        // (0.4 >= 0.0), whose OWN message happens to contain the substring
        // "fillRatioHighThreshold" too, so a `.hasMessageContaining` check
        // here would pass even if this guard's own ">0.0" half were deleted
        // and control fell through to that other guard instead.
        assertThatThrownBy(() -> withAdaptiveFields(Duration.ofSeconds(5), 0.4, 0.0,
                Duration.ofMinutes(2), Duration.ZERO))
                .as("high threshold must be strictly greater than 0")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("fillRatioHighThreshold must be in (0, 1]: 0.0");
        assertThatThrownBy(() -> withAdaptiveFields(Duration.ofSeconds(5), 0.4, 1.1,
                Duration.ofMinutes(2), Duration.ZERO))
                .as("high threshold may be 1.0 but not above it")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("fillRatioHighThreshold");
        // ⚠️ Exactly 1.0 IS a valid high threshold -- "shorten only once a
        // segment is completely full" is a real, if extreme, operating point.
        assertThat(withAdaptiveFields(Duration.ofSeconds(5), 0.4, 1.0, Duration.ofMinutes(2),
                Duration.ZERO).fillRatioHighThreshold()).isEqualTo(1.0);
    }

    @Test
    void aLowThresholdAtOrAboveTheHighThresholdIsRefused() {
        // ⚠️ An inverted or collided band would make BOTH conditions true (or
        // neither reachable) for the same fillRatio observation -- exactly
        // the ambiguity the M3 SPEC's Design section spent a whole warning on
        // getting the direction of.
        assertThatThrownBy(() -> withAdaptiveFields(Duration.ofSeconds(5), 0.9, 0.9,
                Duration.ofMinutes(2), Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("fillRatioLowThreshold");
        assertThatThrownBy(() -> withAdaptiveFields(Duration.ofSeconds(5), 0.9, 0.4,
                Duration.ofMinutes(2), Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("fillRatioLowThreshold");
    }

    @Test
    void negativeHysteresisDelaysAreRefused() {
        assertThatThrownBy(() -> withAdaptiveFields(Duration.ofSeconds(5), 0.4, 0.9,
                Duration.ofMillis(-1), Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("intervalLengthenDelay");
        assertThatThrownBy(() -> withAdaptiveFields(Duration.ofSeconds(5), 0.4, 0.9,
                Duration.ofMinutes(2), Duration.ofMillis(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("intervalShortenDelay");
    }


    @Test
    void aZeroLengthenDelayIsAccepted() {
        // ⚠️ Not the M3 default, but a legitimate configuration: "lengthen
        // immediately once fillRatio dips, no sustained window required."
        // Only NEGATIVE delays are nonsensical; zero on either side is valid.
        IngestConfig c = withAdaptiveFields(Duration.ofSeconds(5), 0.4, 0.9, Duration.ZERO,
                Duration.ZERO);
        assertThat(c.intervalLengthenDelay()).isEqualTo(Duration.ZERO);
    }
}
