// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.FetchMode;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SubscriptionEvent;
import io.github.huyz0.os.biningester.ingest.SubscriptionHub;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A void push goes on the wire as the v5 void event (M13.33x; M13.25e), which
 * the consumer's live lane already skips and counts -- where a records event
 * naming no segment and no bytes would be refused and fail the poll.
 */
class SubscriptionServiceVoidTest {

    @Test
    void aVOIDPushIsWrittenAsTheVoidEventUnderItsChainSequence() throws Exception {
        RunKey a = new RunKey(new UUID(1, 1), 0);
        SubscriptionHub.Push push = new SubscriptionHub.Push(a, "", 3, 10, FetchMode.INLINE,
                new byte[0], null, 2, 7);

        SubscriptionEvent wire = SubscriptionEvent.decode(
                SubscriptionService.eventFor(push, "s").encode());

        assertThat(wire.voided()).isTrue();
        assertThat(wire.key()).isEqualTo(a);
        assertThat(wire.firstOffset()).isEqualTo(10);
        assertThat(wire.recordCount()).isEqualTo(3);
        assertThat(wire.sequencerEpoch()).isEqualTo(2);
        assertThat(wire.chainSequence()).isEqualTo(7);
    }
}
