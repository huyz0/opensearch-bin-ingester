// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.sequencer.CommitProtocolSimulation.Issued;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A real pod's {@code flushSeq} is per-pod and advances on every flush ATTEMPT.
 * The driver issued it from two fleet-global counters incremented only on
 * SUCCESS, so a failed commit left the counter unmoved and the next attempt
 * reused the same number for a DIFFERENT segment.
 *
 * <p>⚠️ THIS BLOCKS M4.10 RATHER THAN BEING IT. M4.10 dedups on
 * {@code (podId, flushSeq)}; against a driver that reissues a pair for a new
 * segment, that dedup would SUPPRESS A GENUINE COMMIT — worse than the
 * duplication it exists to prevent, and it would look like a passing test.
 *
 * <p>⚠️ ASSERTED OVER THE ISSUED TRIPLES, read from the {@code CommitRequest}
 * that is actually sent. M4.27 records a `Result` counter that was declared
 * rather than derived and stayed green when hard-coded.
 *
 * <p>⚠️ UNIQUENESS ALONE DOES NOT PIN "PER POD", and the first version of this
 * test claimed it did. A single fleet-wide counter incremented per attempt is
 * trivially pair-unique, and review measured it green against every assertion
 * here. DENSITY is what separates them: per pod the sequence is exactly
 * 0..n-1, which one shared counter cannot produce. That is what M4.10 rests
 * on — under a global counter a dedup that dropped `podId` from the key would
 * be indistinguishable from a correct one.
 *
 * <p>⚠️ THE FAULTED PROFILE IS NOT WHAT REACHES THE DEFECT, though an earlier
 * javadoc said so. Measured: with the allocator reverted it fails under
 * {@code Faults.none()} too, because a fenced zombie's IOException needs no
 * injected fault. ROUGH is a superset and is kept for that reason, not this one.
 */
@Timeout(value = 120, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class CommitProtocolSimulationFlushSeqTest {

    private static final FaultInjectingStore.Faults ROUGH =
            new FaultInjectingStore.Faults(0.05, 0.05, 0.1);

    /**
     * ⚠️ THE ANTI-VACUITY GUARDS ARE IN THIS METHOD, not a sibling @Test. A
     * control that passes against the defect has no honest red record, and a
     * uniqueness assertion over an empty list is green for the wrong reason.
     */
    @Test
    void noPodIdAndFlushSeqPairIsEverIssuedForTwoDifferentSegments() throws Exception {
        int issuedCount = 0;
        int landed = 0;
        Set<String> pods = new HashSet<>();
        for (long seed = 0; seed < 30; seed++) {
            var result = CommitProtocolSimulation.run(seed, 120, 3, ROUGH);
            issuedCount += result.issued().size();
            landed += result.commits() + result.zombieWrites();
            Map<String, String> seen = new HashMap<>();
            for (Issued i : result.issued()) {
                pods.add(i.podId());
                String pair = i.podId() + "#" + i.flushSeq();
                String previous = seen.putIfAbsent(pair, i.segmentKey());
                if (previous != null) {
                    assertThat(previous)
                            .as("seed %d reissued %s for %s after %s -- dedup on this pair "
                                    + "would suppress a real commit",
                                    seed, pair, i.segmentKey(), previous)
                            .isEqualTo(i.segmentKey());
                }
            }
        }
        assertThat(issuedCount).as("the sweep issued nothing, so uniqueness is vacuous")
                .isPositive();
        assertThat(issuedCount).as("no attempt ever failed, so the reissue path is never reached")
                .isGreaterThan(landed);
        assertThat(pods).as("a sweep touching one pod name says nothing about either")
                .hasSizeGreaterThan(1);
    }

    /**
     * ⚠️ THIS IS THE TEST THAT KILLS A SHARED COUNTER; uniqueness above passes
     * against one. The kill is DENSITY. The shared-value count is this test's
     * ANTI-VACUITY GUARD: density-from-zero plus two issuing pods forces a
     * shared value by construction, so on correct code the two coincide,
     * measured at 19 of 30 seeds.
     */
    @Test
    void everyPodIssuesADenseSequenceFromZeroAndTwoPodsReachTheSameValue() throws Exception {
        int seedsWithSharedValue = 0;
        for (long seed = 0; seed < 30; seed++) {
            var result = CommitProtocolSimulation.run(seed, 120, 3, ROUGH);
            Map<String, List<Long>> byPod = new HashMap<>();
            for (Issued i : result.issued()) {
                byPod.computeIfAbsent(i.podId(), unused -> new ArrayList<>()).add(i.flushSeq());
            }
            Map<Long, String> valueOwner = new HashMap<>();
            boolean shared = false;
            for (var pod : byPod.entrySet()) {
                List<Long> expected = new ArrayList<>();
                for (long n = 0; n < pod.getValue().size(); n++) {
                    expected.add(n);
                }
                assertThat(pod.getValue())
                        .as("seed %d: pod %s must issue 0..n-1, so the allocator is PER POD "
                                + "rather than one shared counter", seed, pod.getKey())
                        .isEqualTo(expected);
                for (Long v : pod.getValue()) {
                    String owner = valueOwner.putIfAbsent(v, pod.getKey());
                    if (owner != null && !owner.equals(pod.getKey())) {
                        shared = true;
                    }
                }
            }
            if (shared) {
                seedsWithSharedValue++;
            }
        }
        assertThat(seedsWithSharedValue)
                .as("no seed had two pods reach the same flushSeq -- that is what a single "
                        + "shared counter looks like, and M4.10's key would not need podId")
                .isPositive();
    }
}
