// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** ⚠️ The key is a wire format: changing it strands every object already written. */
class SegmentKeyTest {

    private static final long T = 1_700_000_000_000L;   // 2023-11-14T22:13:20Z

    @Test
    void theKeyCarriesTheTimePathAndTheHeaderLength() {
        String key = new SegmentKey("bins/cluster-a", T, "pod7", 42, 96).key();
        assertThat(key).startsWith("bins/cluster-a/data/2023/11/14/22/");
        // ⚠️ h<headerLen> is what lets a reader fetch preamble AND directory in
        // ONE ranged GET with no guess. AutoMQ has to estimate its index size
        // and sometimes pays a second request; this removes that failure mode.
        assertThat(key).contains("-h96-").endsWith(".bseg");
        assertThat(SegmentKey.headerLenOf(key)).isEqualTo(96);
    }

    @Test
    void keysSortChronologicallyWithinAnHour() {
        List<String> keys = new ArrayList<>();
        for (long t : new long[] {T + 9999, T + 10, T + 100000, T}) {
            keys.add(new SegmentKey("p", t, "pod", 1, 48).key());
        }
        List<String> sorted = new ArrayList<>(keys);
        java.util.Collections.sort(sorted);
        // ⚠️ Zero padding is what makes this true. Unpadded, "9999" sorts after
        // "10000" and a recovery walk visits the hour out of order — which for a
        // start-after resume means silently skipping objects.
        assertThat(sorted).containsExactly(
                new SegmentKey("p", T, "pod", 1, 48).key(),
                new SegmentKey("p", T + 10, "pod", 1, 48).key(),
                new SegmentKey("p", T + 9999, "pod", 1, 48).key(),
                new SegmentKey("p", T + 100000, "pod", 1, 48).key());
    }

    @Test
    void paddingIsWhatMakesTheOrderingHoldAcrossDigitCounts() {
        // ⚠️ Every modern epoch-millis value is 13 digits, so a fixture built
        // from realistic timestamps CANNOT observe the padding — the mutation
        // that removes it survived exactly that way. These values differ in
        // digit count and are in the same hour, which is where lexicographic
        // and chronological order part company without it.
        String shorter = new SegmentKey("p", 999L, "pod", 1, 48).key();
        String longer = new SegmentKey("p", 1_000L, "pod", 1, 48).key();
        assertThat(shorter).as("999 ms must sort before 1000 ms").isLessThan(longer);
        // ⚠️ And the guard is load-bearing beyond year 2286, when epoch millis
        // reach 14 digits and every key written before then would sort after
        // every key written after.
        assertThat(new SegmentKey("p", 9_999_999_999_999L, "pod", 1, 48).key())
                .isLessThan(new SegmentKey("p", 10_000_000_000_000L, "pod", 1, 48).key());
    }

    @Test
    void twoPodsNeverCollideAndOneStreamIsMonotonic() {
        String a = new SegmentKey("p", T, "podA", 1, 48).key();
        String b = new SegmentKey("p", T, "podB", 1, 48).key();
        // ⚠️ Uniqueness WITHOUT coordination: any pod writes any partition's
        // bytes at any time, and ordering is decided at commit. A collision here
        // would silently overwrite another pod's segment.
        assertThat(a).isNotEqualTo(b);
        assertThat(new SegmentKey("p", T, "podA", 1, 48).key())
                .isLessThan(new SegmentKey("p", T, "podA", 2, 48).key());
    }

    @Test
    void theHourPrefixBoundsARecoveryWalk() {
        String prefix = SegmentKey.hourPrefix("bins/c", T);
        assertThat(prefix).isEqualTo("bins/c/data/2023/11/14/22/");
        // ⚠️ R2 confines LIST to recovery and GC; this prefix is what bounds
        // that walk to one hour instead of the whole bucket.
        assertThat(new SegmentKey("bins/c", T, "pod", 1, 48).key()).startsWith(prefix);
        assertThat(new SegmentKey("bins/c", T + 3_600_000, "pod", 1, 48).key())
                .as("the next hour is a different prefix").doesNotStartWith(prefix);
    }

    @Test
    void aKeyOverTheProviderLimitIsRefused() {
        // ⚠️ S3, GCS and Azure all cap at 1024 bytes. Producing a longer key
        // fails the PUT at the provider, which is a far worse place to learn it.
        SegmentKey tooLong = new SegmentKey("p".repeat(1100), T, "pod", 1, 48);
        assertThatThrownBy(tooLong::key).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void aBlankPodIdOrNegativeHeaderIsRefused() {
        assertThatThrownBy(() -> new SegmentKey("p", T, " ", 1, 48))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SegmentKey("p", T, "pod", 1, -1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SegmentKey("p", -1, "pod", 1, 48))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void theHeaderLengthSurvivesARoundTripThroughTheKey() {
        for (int headerLen : new int[] {0, 48, 96, 76_800}) {
            String key = new SegmentKey("p", T, "pod", 1, headerLen).key();
            // ⚠️ 76,800 is 1,600 runs of 48 bytes — the real upper end, and the
            // case where a fixed-width assumption about the digits would break.
            assertThat(SegmentKey.headerLenOf(key)).isEqualTo(headerLen);
        }
    }
}
