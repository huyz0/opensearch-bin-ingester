// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * {@link MembershipFilter.Bloom}'s Kirsch-Mitzenmacher construction (M2.5;
 * ADR-0003, research doc 02 §7): no false negatives, ever, and a measured
 * false-positive rate within tolerance of the analytic prediction.
 *
 * <p>⚠️ M2.4's own test file proves the codec reproduces whatever bits it was
 * handed; this file proves {@link MembershipFilter.Bloom#of} and {@link
 * MembershipFilter.Bloom#mightContain} agree with each other and with the
 * maths research doc 02 §3 computes.
 *
 * <p>⚠️ FIXED SEEDS throughout. A property test that picks its own random
 * population on every run is not reproducible, and testing.md forbids
 * unseeded randomness in a test for exactly that reason.
 */
class BloomConstructionTest {

    /** research doc 02 §3: 10% target FPR, ~4.79 bits/item, k=3. */
    private static final double BITS_PER_ITEM_10_PCT = 4.79;
    private static final int K_10_PCT = 3;

    private static int mFor(int n, double bitsPerItem) {
        int raw = (int) Math.ceil(n * bitsPerItem);
        return ((raw + 7) / 8) * 8; // round up to a byte boundary -- Bloom's own constructor requires it
    }

    private static Set<Integer> randomDistinctOrdinals(Random rnd, int count, int universe) {
        Set<Integer> ordinals = new HashSet<>();
        while (ordinals.size() < count) {
            ordinals.add(rnd.nextInt(universe));
        }
        return ordinals;
    }

    @Test
    void ofRoundsAnUnalignedMUpToTheNextMultipleOfEight() {
        // ⚠️ round-1 review (M2.5): every other test pre-rounds m itself
        // (mFor already rounds to a byte boundary before calling Bloom.of),
        // so Bloom.of's OWN rounding branch never ran on an unaligned input
        // anywhere in this suite -- exactly the common case once M2.6 computes
        // m from a real bits-per-item budget, which will essentially never
        // land on a multiple of 8 by chance. 479 is the unrounded ceiling
        // ceil(100 * 4.79); the correct answer is 480, not 472 (a floor).
        MembershipFilter.Bloom filter = MembershipFilter.Bloom.of(Set.of(1, 2, 3), 479, 3);
        assertThat(filter.m()).isEqualTo(480);
    }

    @Test
    void mightContainRejectsANegativeOrdinal() {
        // ⚠️ round-1 test-review (M2.5): ExactBitmap and RunLength both
        // reject a negative ordinal explicitly (M2.4); Bloom had no such
        // guard and no test either way -- silently hashing and answering
        // instead, an inconsistency across MembershipFilter's own sealed
        // variants.
        MembershipFilter.Bloom filter = MembershipFilter.Bloom.of(Set.of(1, 2, 3), 32, 3);
        assertThatThrownBy(() -> filter.mightContain(-1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void neverAFalseNegativeAcrossRandomisedPopulationsAtSeveralSizes() {
        for (int n : new int[] {10, 100, 500, 1000}) {
            Random rnd = new Random(1_000 + n);
            int universe = n * 20;
            Set<Integer> members = randomDistinctOrdinals(rnd, n, universe);
            int m = mFor(n, BITS_PER_ITEM_10_PCT);
            MembershipFilter.Bloom filter = MembershipFilter.Bloom.of(members, m, K_10_PCT);
            for (int member : members) {
                assertThat(filter.mightContain(member))
                        .as("member %d must never be a false negative at n=%d", member, n)
                        .isTrue();
            }
        }
    }

    @Test
    void measuredFalsePositiveRateAtOneThousandIndicesMatchesTheAnalyticPrediction() {
        measureAndAssert(1_000);
    }

    @Test
    void measuredFalsePositiveRateAtFiveHundredIndicesMatchesTheAnalyticPrediction() {
        measureAndAssert(500);
    }

    @Test
    void measuredFalsePositiveRateAtOneHundredIndicesMatchesTheAnalyticPrediction() {
        measureAndAssert(100);
    }

    /**
     * research doc 02 §8: "measured FPR within tolerance of the analytic
     * prediction, at n = 100/500/1,000."
     */
    private static void measureAndAssert(int n) {
        int m = mFor(n, BITS_PER_ITEM_10_PCT);
        int universe = n * 50; // plenty of room to sample non-members from
        Random rnd = new Random(42 + n);
        Set<Integer> members = randomDistinctOrdinals(rnd, n, universe);
        MembershipFilter.Bloom filter = MembershipFilter.Bloom.of(members, m, K_10_PCT);

        int trials = 20_000;
        int falsePositives = 0;
        int sampled = 0;
        while (sampled < trials) {
            int candidate = rnd.nextInt(universe);
            if (members.contains(candidate)) {
                continue; // only non-members count toward the FALSE positive rate
            }
            sampled++;
            if (filter.mightContain(candidate)) {
                falsePositives++;
            }
        }
        double measured = (double) falsePositives / trials;
        // ⚠️ Analytic prediction, research doc 02 §3's own formula:
        // (1 - e^(-k*n/m))^k. Computed here, not hardcoded, so this test
        // tracks the SAME formula for every n rather than three magic numbers.
        double analytic = Math.pow(1 - Math.exp(-K_10_PCT * (double) n / m), K_10_PCT);
        // ⚠️ round-1 test-review (M2.5): 0.03 was wide enough to miss a
        // dropped-probe bug (one fewer effective k, verified to measure
        // 0.0107-0.0217 off analytic at these three n) entirely, while every
        // correct run measures under 0.004 off at a fixed seed. 0.008 sits
        // roughly 4 standard errors above the correct-code noise floor
        // (SE = sqrt(0.1*0.9/20000) ~= 0.0021 at p=0.10, 20,000 trials) and
        // comfortably below the dropped-probe mutation's smallest observed
        // deviation -- tight enough to separate the two, not tuned to one run.
        // Do NOT widen this tolerance to make a flaky run pass -- a
        // SYSTEMATIC deviation (a hashing or bit-indexing bug) is the signal
        // this test exists to catch, not sampling noise.
        assertThat(measured).as("measured FPR at n=%d (m=%d, k=%d): analytic %.4f, measured %.4f",
                        n, m, K_10_PCT, analytic, measured)
                .isCloseTo(analytic, org.assertj.core.data.Offset.offset(0.008));
    }
}
