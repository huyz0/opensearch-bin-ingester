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

    @Test
    void headerLenOfParsesCorrectlyEvenWhenTheFilterPayloadContainsDashH() {
        // ⚠️ M2.6, THE hazard ADR-0003/the M2 SPEC's own risk section names:
        // base64url's alphabet legally contains '-', so once the filter
        // component can be anything other than the literal "N", a payload
        // containing the two characters "-h" is a real, reachable input --
        // this exact string was found by search, not hand-crafted, and
        // genuinely contains "-h" partway through a Z (exact bitmap) payload.
        // lastIndexOf("-h") over the whole key would find THIS occurrence
        // instead of the real header-length marker and misparse 96 as
        // whatever digits happen to follow it.
        String filterWithEmbeddedDashH = "Z8_5Z-hCEZLc";
        assertThat(filterWithEmbeddedDashH).contains("-h");
        String key = new SegmentKey("bins/cluster-a", T, "pod7", 42, 96, filterWithEmbeddedDashH).key();
        assertThat(SegmentKey.headerLenOf(key)).isEqualTo(96);
    }

    @Test
    void theFilterIsEmbeddedInTheKeyAndRoundTripsThroughDecode() throws Exception {
        MembershipFilter filter = new MembershipFilter.All();
        String key = new SegmentKey("p", T, "pod", 1, 48, filter.encode()).key();
        assertThat(key).contains("-A.bseg");
        // ⚠️ The embedded string is not just present in the key -- it must
        // still be a filter MembershipFilter.decode() can parse back out.
        String embedded = key.substring(key.lastIndexOf('-') + 1, key.length() - ".bseg".length());
        assertThat(MembershipFilter.decode(embedded)).isEqualTo(filter);
    }

    @Test
    void theLegacyConstructorDefaultsTheFilterToN() {
        // ⚠️ Every pre-M2.6 call site (record IDs, timestamps, header
        // lengths) uses the 5-arg constructor and does not know the filter
        // exists -- it must keep meaning exactly what M1 hardcoded.
        String key = new SegmentKey("p", T, "pod", 1, 48).key();
        assertThat(key).endsWith("-N.bseg");
    }

    @Test
    void aPodShortIdContainingDashOrSlashIsRefused() {
        // ⚠️ round-1 review (M2.6): headerLenOf's tail-anchoring alone is
        // not sufficient if podShortId can itself smuggle in a "-h<digits>-"
        // marker, e.g. "pod-0123456789abcdef-h5" reproduces the exact hazard
        // this task exists to close, through a different field than the
        // filter. Refusing '-' and '/' here is what makes "the leftmost match
        // in the tail is always the real one" actually true.
        assertThatThrownBy(() -> new SegmentKey("p", T, "pod-0123456789abcdef-h5", 1, 48))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SegmentKey("p", T, "pod/7", 1, 48))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void headerLenOfIgnoresASpuriousMarkerInsidePrefix() {
        // ⚠️ test-reviewer (M2.6, round 1): the tail-anchoring guard's OWN
        // commentary claimed this route was "closed... found analytically
        // rather than by the reviewer" but was never actually reproduced by
        // a test -- this is that reproduction. `prefix` carries no character
        // restriction (only podShortId does), so a prefix shaped like a
        // header-length marker is a real, reachable input; the tail-anchoring
        // guard (searching only after the key's LAST '/') is what must keep
        // it from being mistaken for the real one.
        String prefix = "bins-0123456789abcdef-h7-x";
        String key = new SegmentKey(prefix, T, "pod", 1, 96).key();
        assertThat(key).as("the marker-shaped text is in the prefix, not the tail")
                .startsWith(prefix + "/data/");
        assertThat(SegmentKey.headerLenOf(key))
                .as("must read the REAL header length (96), not the 7 embedded in prefix")
                .isEqualTo(96);
    }

    @Test
    void anUnparsableFilterIsRefusedAtConstruction() {
        // ⚠️ Refused HERE, not discovered later by a reader with no way to
        // say why its own key will not parse.
        assertThatThrownBy(() -> new SegmentKey("p", T, "pod", 1, 48, "not-a-filter"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
