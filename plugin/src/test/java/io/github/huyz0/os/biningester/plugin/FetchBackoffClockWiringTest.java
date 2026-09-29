// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.client.SubscriptionTransport;
import io.github.huyz0.os.biningester.format.RunKey;
import org.junit.jupiter.api.Test;

/**
 * M12.26: the host clock the node already hands its segment hold also becomes
 * the clock the node's clients' fetch backoffs are due on -- without it a
 * catch-up backing off keeps its quantum turn and live waits out its backoff.
 * A node with no hold (no {@code direct}) still gets it: the catch-up lane
 * backs off either way.
 */
class FetchBackoffClockWiringTest {

    private static SubscriptionTransport quiet() {
        return new SubscriptionTransport() {
            @Override
            public AutoCloseable subscribe(RunKey key, Listener listener) {
                return () -> { };
            }
        };
    }

    @Test
    void theHostClockBecomesTheClientsBackoffClockWithOrWithoutAHold() {
        NodeSubscriptions noHold = new NodeSubscriptions(quiet(), 16, null);
        assertThat(noHold.fetchRetry().clockMillis()).as("the premise: no clock yet").isNull();

        noHold.holdFailuresWith(() -> 5_000L);

        assertThat(noHold.fetchRetry().clockMillis())
                .as("⚠️ THE CLIENTS' BACKOFFS ARE DUE ON THE HOST's CLOCK").isNotNull();
        assertThat(noHold.fetchRetry().clockMillis().getAsLong()).isEqualTo(5_000L);
    }
}
