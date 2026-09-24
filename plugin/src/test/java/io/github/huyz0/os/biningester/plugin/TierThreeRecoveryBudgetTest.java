// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class TierThreeRecoveryBudgetTest {

    @Test
    void retainedByteBudgetAcceptsExactLimitAndZeroButRejectsOutsideRange() {
        TierThreeRecovery.Budget budget = new TierThreeRecovery.Budget();

        assertThat(budget.reserveRetained(TierThreeRecovery.MAX_EPISODE_BYTES)).isTrue();
        assertThat(budget.reserveRetained(0)).isTrue();
        assertThat(budget.reserveRetained(1)).isFalse();
        assertThat(new TierThreeRecovery.Budget().reserveRetained(-1)).isFalse();
    }
}
