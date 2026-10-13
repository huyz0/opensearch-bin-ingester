// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static io.github.huyz0.os.biningester.server.TakeoverRig.S;
import static io.github.huyz0.os.biningester.server.TakeoverRig.entry;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A takeover over more than one unclosed term (SPEC test-plan row 12; ADR-0081
 * §5; M13.33s): it collects every unclosed term, not only the last, recovers
 * each term's exposed entries at their offsets, and closes every term once
 * decided.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class FastMultiTermTakeoverTest {

    @Test
    void aTAKEOVERCollectsEveryUnclosedTermNotOnlyTheLast() throws Exception {
        try (TakeoverRig rig = new TakeoverRig(2, 5,
                Map.of("uid-m", List.of(entry(1, 5), entry(2, 6))))) {
            assertThat(rig.takeOver()).isTrue();

            assertThat(rig.term.committedNext(S))
                    .as("term 1's entry at 5 and term 2's at 6").isEqualTo(7);
            assertThat(rig.roster(1).closed()).isTrue();
            assertThat(rig.roster(2).closed()).isTrue();
        }
    }
}
