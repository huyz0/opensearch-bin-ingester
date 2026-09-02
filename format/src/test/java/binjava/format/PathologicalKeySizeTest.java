// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

/**
 * NFR-12's own pathological-input test (M2.8): at this project's target
 * scale of 10,000 registered indices (research doc 02 §0), the emitted key
 * is MEASURED, not assumed, to stay within the provider's 1024-byte cap --
 * for every tag the writer's candidate selection can choose, not only the
 * degrade-to-{@code N} case.
 *
 * <p>⚠️ {@code FilterCandidates.chooseShortestFitting} used to judge every
 * candidate against a FIXED 900-byte constant, on the assumption that the
 * REST of the key -- prefix, date path, timestamp, pod id, sequence, header
 * length, punctuation -- fits in the remaining ~124 bytes (research doc
 * 02 §0: "~90 bytes of fixed key structure"). Round-1 test review of this
 * very file found that arithmetic was never checked anywhere: reproduced
 * against the real, unmodified code at a plain ~1,120-distinct-index
 * segment (nowhere near this project's 10,000-index pathological scale)
 * where a Bloom filter correctly measured at 897 bytes against the
 * hardcoded 900-byte assumption still made {@code SegmentKey.key()} throw,
 * because this fixture's own prefix/pod id leave only 894 bytes, not 900.
 * Fixed by computing the real budget per key via {@link
 * SegmentKey#filterBudgetBytes}, which every test below now uses instead of
 * a literal.
 */
class PathologicalKeySizeTest {

    private static final long T = 1_700_000_000_000L;
    // ⚠️ Deliberately generous, not the minimal fixtures used elsewhere in
    // this suite -- a long cluster-scoped prefix and a realistic-length pod
    // id are exactly the inputs that erode the margin available for the
    // filter, which is why this fixture measures the real budget rather than
    // assuming one.
    private static final String PREFIX = "bins/prod-us-east-1-ingest-cluster-007";
    // ⚠️ No '-' or '/' -- SegmentKey's own constructor refuses either
    // (M2.6, round-1 review), so a realistic pod id under that restriction.
    private static final String POD_SHORT_ID = "ingesterworkerpodzz07";
    // ⚠️ 10,000 runs at 48 bytes/entry (the v0 layout's fixed entry size) --
    // a segment actually touching every registered index's worth of runs.
    private static final int HEADER_LEN = 10_000 * 48;
    private static final int TOTAL_REGISTERED = 10_000;
    private static final long SEQUENCE = 1;

    // ⚠️ MEASURED (not assumed) at 894 for this fixture's own prefix/pod/
    // headerLen -- 6 bytes less than the 900 this suite used before the
    // round-1 fix, which is exactly the margin that let the ~1,120-index
    // overflow through undetected.
    private static final int BUDGET_BYTES =
            SegmentKey.filterBudgetBytes(PREFIX, T, POD_SHORT_ID, SEQUENCE, HEADER_LEN);

    private static int keyLength(MembershipFilter filter) {
        String key = new SegmentKey(PREFIX, T, POD_SHORT_ID, SEQUENCE, HEADER_LEN, filter.encode()).key();
        return key.getBytes(StandardCharsets.UTF_8).length;
    }

    private static MembershipFilter choose(Set<Integer> members) {
        return FilterCandidates.chooseShortestFitting(members, TOTAL_REGISTERED, BUDGET_BYTES);
    }

    @Test
    void theAllCandidateAtTenThousandIndicesFitsTheKeyBudget() {
        Set<Integer> members = IntStream.range(0, TOTAL_REGISTERED).boxed().collect(Collectors.toSet());
        MembershipFilter filter = choose(members);
        assertThat(filter).isInstanceOf(MembershipFilter.All.class);
        // ⚠️ MEASURED (not assumed): 131 bytes against a 1024 cap, ~893
        // bytes of headroom -- pinned so a future widening of the fixed key
        // structure shows up as a changed number here, not silent erosion.
        assertThat(keyLength(filter)).isEqualTo(131);
    }

    @Test
    void theExactBitmapCandidateAtTenThousandRegisteredIndicesFitsTheKeyBudget() {
        // ⚠️ A handful of low ordinals out of 10,000 registered -- Z fits
        // trivially on its own, but the KEY around it uses the deliberately
        // generous prefix/pod/headerLen fixture, not a minimal one.
        Set<Integer> members = Set.of(0, 5, 9);
        MembershipFilter filter = choose(members);
        assertThat(filter).isInstanceOf(MembershipFilter.ExactBitmap.class);
        assertThat(keyLength(filter)).as("measured, not assumed").isEqualTo(134);
    }

    @Test
    void theRunLengthCandidateAtTenThousandRegisteredIndicesFitsTheKeyBudget() {
        Set<Integer> members = Set.of(0, 1, 2, 7_000, 7_001, 7_002);
        MembershipFilter filter = choose(members);
        assertThat(filter).isInstanceOf(MembershipFilter.RunLength.class);
        assertThat(keyLength(filter)).as("measured, not assumed").isEqualTo(138);
    }

    @Test
    void theBloomCandidateAtThisProjectsExpectedScaleFitsTheKeyBudget() {
        // ⚠️ research doc 02 §5: ~500 distinct indices per segment, sparse --
        // too many/sparse for Z or R, comfortably sized for B, and the
        // expected DEFAULT outcome at this project's real scale.
        Set<Integer> members = new HashSet<>();
        for (int i = 0; i < 500; i++) {
            members.add(i * 17);
        }
        MembershipFilter filter = choose(members);
        assertThat(filter).isInstanceOf(MembershipFilter.Bloom.class);
        // ⚠️ MEASURED: 532 bytes -- the biggest of the five typical-scale
        // tags here (a real Bloom payload, not a degenerate small one),
        // still well under the 1024 cap and comfortably under this
        // fixture's real 894-byte filter budget (402 of the 532 bytes are
        // the filter itself; the rest is the deliberately generous prefix,
        // pod id and header length).
        assertThat(keyLength(filter)).as("measured, not assumed").isEqualTo(532);
    }

    @Test
    void thePathologicalTenThousandIndexSegmentDegradesToNAndTheKeyFitsTheBudget() {
        // ⚠️ NFR-12's own named pathological case: every-other ordinal across
        // 10,000 -- 5,000 members, maximally fragmented so RLE cannot
        // collapse it, too many and too sparse for the exact bitmap or a
        // Bloom filter at this project's own 10% FPR target. Not "all
        // 10,000" (members == total), which would correctly choose A
        // instead -- see FilterCandidatesTest's own version of this fixture.
        Set<Integer> members = new HashSet<>();
        for (int i = 0; i < TOTAL_REGISTERED; i += 2) {
            members.add(i);
        }
        MembershipFilter filter = choose(members);
        assertThat(filter).as("degrades to N rather than overflowing")
                .isInstanceOf(MembershipFilter.None.class);
        // ⚠️ MEASURED: 131 bytes, identical to the All case above -- N's
        // payload is the same one character as A's, so the pathological
        // case that makes every OTHER tag too large to fit is, once
        // degraded, the SMALLEST possible key, not the largest. The
        // adversarial part is entirely in the candidate SELECTION (proving
        // the writer never overflows trying to fit a filter that cannot
        // fit), not in the emitted key's own size.
        assertThat(keyLength(filter))
                .as("measured, not assumed, to stay under the provider's 1024-byte cap")
                .isEqualTo(131);
    }

    @Test
    void everyPopulationFromOneToTenThousandIndicesNeverOverflowsTheKeyBudget() {
        // ⚠️ Round-1 test review's own reproduction, generalised: the defect
        // was not specific to n=1,120 -- it was that NOTHING checked the
        // whole-key length against a real budget at all. This walks every
        // population from 1 to 10,000 sparse ordinals (the shape that
        // produces the LARGEST filter for a given n, per the Bloom test
        // above) and proves the emitted key never exceeds MAX_KEY_BYTES,
        // whichever tag ends up chosen.
        int maxObservedKeyLength = 0;
        for (int n = 1; n <= TOTAL_REGISTERED; n++) {
            Set<Integer> members = new HashSet<>();
            for (int i = 0; i < n; i++) {
                members.add((i * 17) % 100_000);
            }
            MembershipFilter filter = choose(members);
            int len = keyLength(filter);
            assertThat(len).as("n=%d, tag=%s must never overflow the provider's cap", n, filter.tag())
                    .isLessThanOrEqualTo(SegmentKey.MAX_KEY_BYTES);
            maxObservedKeyLength = Math.max(maxObservedKeyLength, len);
        }
        // ⚠️ MEASURED: the walk actually reaches the cap exactly (a Bloom
        // filter at n=1,116 lands at precisely 1024 bytes) rather than
        // merely staying comfortably clear of it -- proof this budget is
        // tight, not padded with unexamined slack that could be hiding a
        // smaller version of the same defect.
        assertThat(maxObservedKeyLength).as("the tightest population actually reaches the cap exactly")
                .isEqualTo(SegmentKey.MAX_KEY_BYTES);
    }

    @Test
    void theSpecificPopulationThatUsedToOverflowNowDegradesCleanly() {
        // ⚠️ The EXACT reproduction from round-1 test review: 1,120 sparse
        // ordinals (i*17 spacing) made FilterCandidates choose a Bloom
        // filter of 897 bytes -- which fit the OLD hardcoded 900-byte
        // budget, but not this fixture's real 894-byte one, so the
        // resulting key used to throw IllegalStateException from
        // SegmentKey.key(). It must now degrade to N instead.
        Set<Integer> members = new HashSet<>();
        for (int i = 0; i < 1_120; i++) {
            members.add(i * 17);
        }
        MembershipFilter filter = choose(members);
        assertThat(filter).as("897 bytes no longer fits this fixture's real 894-byte budget")
                .isInstanceOf(MembershipFilter.None.class);
        assertThat(keyLength(filter)).isEqualTo(131);
    }
}
