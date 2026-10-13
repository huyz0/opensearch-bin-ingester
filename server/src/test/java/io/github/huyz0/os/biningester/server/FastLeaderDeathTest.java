// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static io.github.huyz0.os.biningester.server.TakeoverRig.S;
import static io.github.huyz0.os.biningester.server.TakeoverRig.entry;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.Body;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A leader dies at each point of its upload (SPEC test-plan row 12; ADR-0081
 * §5, §7; M13.33s): before it, between the segment's PUT and its delta, and
 * between the delta and the RELEASE. Its successor recovers each exposed
 * entry at its assigned offset exactly once, never commits again an entry
 * the chain already holds, and releases holders only after the delta.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class FastLeaderDeathTest {

    @Test
    void aLEADERDeadBeforeItsUploadHasItsEntryRecoveredAtItsOffsetThenReleased()
            throws Exception {
        try (TakeoverRig rig = new TakeoverRig(1, 5, Map.of("uid-m", List.of(entry(1, 5))))) {
            assertThat(rig.takeOver()).isTrue();

            assertThat(rig.term.committedNext(S)).as("the entry at its offset").isEqualTo(6);
            assertThat(rig.uploads).as("one recovery segment").hasValue(1);
            assertThat(rig.released).as("every RELEASE after the delta committed it")
                    .isNotEmpty().allSatisfy(r -> assertThat(r).endsWith("@6"));
        }
    }

    @Test
    void aLEADERDeadBetweenItsPutAndItsDeltaHasItsEntryRecoveredOnce() throws Exception {
        try (TakeoverRig rig = new TakeoverRig(1, 5, Map.of("uid-m", List.of(entry(1, 5))))) {
            // the dead leader's segment, PUT and never named by a delta
            rig.store.put("p/data/dead-leader-seg", Body.ofBytes(new byte[] {1, 2, 3}));

            assertThat(rig.takeOver()).isTrue();

            assertThat(rig.term.committedNext(S)).as("once, at its offset").isEqualTo(6);
            assertThat(rig.uploads).hasValue(1);
        }
    }

    @Test
    void aLEADERDeadBetweenItsDeltaAndItsReleaseHasNothingCommittedAgain() throws Exception {
        try (TakeoverRig rig = new TakeoverRig(1, 6, Map.of("uid-m", List.of(entry(1, 5))))) {
            assertThat(rig.takeOver()).isTrue();

            assertThat(rig.term.committedNext(S)).as("the chain already holds 5")
                    .isEqualTo(6);
            assertThat(rig.uploads).as("nothing below the chain's next offset again")
                    .hasValue(0);
            assertThat(rig.released).extracting(r -> r.substring(0, r.indexOf('@')))
                    .as("its holders released all the same")
                    .contains("uid-m");
        }
    }
}
