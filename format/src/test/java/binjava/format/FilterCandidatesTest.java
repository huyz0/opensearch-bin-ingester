// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * The writer's shortest-fitting candidate selection (M2.6; ADR-0003, research
 * doc 02 §7): {@code A} if literally every registered index is present,
 * {@code Z}/{@code R} if the exact bitmap fits, {@code B} (Bloom) as the
 * default at scale, {@code N} if nothing fits.
 */
class FilterCandidatesTest {

    @Test
    void allWhenEveryRegisteredIndexIsPresent() {
        Set<Integer> members = Set.of(0, 1, 2);
        MembershipFilter chosen = FilterCandidates.chooseShortestFitting(members, 3);
        assertThat(chosen).isInstanceOf(MembershipFilter.All.class);
    }

    @Test
    void notAllWhenTheSegmentIsMissingARegisteredIndex() {
        // ⚠️ 3 members, but 4 are registered in total -- one registered index
        // is genuinely absent from this segment, so A would be a lie.
        Set<Integer> members = Set.of(0, 1, 2);
        MembershipFilter chosen = FilterCandidates.chooseShortestFitting(members, 4);
        assertThat(chosen).isNotInstanceOf(MembershipFilter.All.class);
    }

    @Test
    void exactBitmapForASmallScopedOrdinalSpace() {
        // ⚠️ A handful of ordinals in a small space -- Z fits easily and is
        // tried before R/B (cheapest-first, per the design).
        Set<Integer> members = Set.of(0, 5, 9);
        MembershipFilter chosen = FilterCandidates.chooseShortestFitting(members, 1000);
        assertThat(chosen).isInstanceOf(MembershipFilter.ExactBitmap.class);
        for (int m : members) {
            assertThat(((MembershipFilter.ExactBitmap) chosen).mightContain(m)).isTrue();
        }
    }

    @Test
    void runLengthWhenTheExactBitmapIsDenseWithGapsAndTooLongButItsRleFits() {
        // ⚠️ A small number of members spread across a WIDE ordinal range: the
        // raw exact bitmap (one bit per ordinal up to the highest) is too long
        // to fit the budget, but the same set, RLE-encoded, collapses to a
        // handful of run lengths and DOES fit.
        Set<Integer> members = Set.of(0, 1, 2, 7_000, 7_001, 7_002);
        MembershipFilter chosen = FilterCandidates.chooseShortestFitting(members, 8_000);
        assertThat(chosen).isInstanceOf(MembershipFilter.RunLength.class);
        for (int m : members) {
            assertThat(((MembershipFilter.RunLength) chosen).mightContain(m)).isTrue();
        }
    }

    @Test
    void bloomAsTheDefaultAtThisProjectsExpectedScale() {
        // ⚠️ research doc 02 §5: ~500 distinct indices per segment is the
        // expected scale once the per-stream trickle flush policy does its
        // job -- too many, too sparse for Z or R to fit, comfortably sized
        // for B.
        Set<Integer> members = new HashSet<>();
        for (int i = 0; i < 500; i++) {
            members.add(i * 17); // sparse: spread across a 8,500-wide ordinal space
        }
        MembershipFilter chosen = FilterCandidates.chooseShortestFitting(members, 10_000);
        assertThat(chosen).isInstanceOf(MembershipFilter.Bloom.class);
        for (int m : members) {
            assertThat(((MembershipFilter.Bloom) chosen).mightContain(m))
                    .as("no false negatives, ever").isTrue();
        }
        assertThat(chosen.encode().getBytes(StandardCharsets.UTF_8).length)
                .as("fits the ~900-byte filter budget").isLessThanOrEqualTo(900);
    }

    @Test
    void degradesToNWhenNothingFitsTheBudget() {
        // ⚠️ NFR-12's own pathological case (M2.8 proves it at the full
        // 10,000-index scale, this is the shape of it): EVERY OTHER ordinal
        // across most of a 10,000-wide range -- 5,000 members, deliberately
        // maximally fragmented so RLE cannot collapse it (every run is length
        // 1), too many and too sparse for the exact bitmap, and too many even
        // for Bloom at this project's own 10% target FPR. Not "all 10,000"
        // (members == total), which would correctly choose A instead.
        Set<Integer> members = new HashSet<>();
        for (int i = 0; i < 10_000; i += 2) {
            members.add(i);
        }
        MembershipFilter chosen = FilterCandidates.chooseShortestFitting(members, 10_000);
        assertThat(chosen).isInstanceOf(MembershipFilter.None.class);
    }

    @Test
    void anEmptyMemberSetDegradesToNRatherThanThrowing() {
        MembershipFilter chosen = FilterCandidates.chooseShortestFitting(Set.of(), 5);
        assertThat(chosen).isInstanceOf(MembershipFilter.None.class);
    }
}
