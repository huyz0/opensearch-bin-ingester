// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static io.github.huyz0.os.biningester.server.TakeoverRig.entry;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A takeover's recovery, once its entry commits, is handed on to be pushed
 * live under the new term's epoch (M13.33w; ADR-0081 §5 step 6) -- where it
 * went only to a metric, so a subscriber of the new leader was pushed nothing
 * of the recovered offsets.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class FastTakeoverRecoveredTest {

    @Test
    void aCOMMITTEDRecoveryIsHandedOnWithItsSegmentsAndTheNewTermsEpoch() throws Exception {
        try (TakeoverRig rig = new TakeoverRig(1, 5,
                Map.of("uid-m", List.of(entry(1, 5), entry(1, 6))))) {
            assertThat(rig.takeOver()).isTrue();

            assertThat(rig.recovered).as("both entries, one run, in term 2")
                    .containsExactly("seg-r1:5+2@2");
        }
    }
}
