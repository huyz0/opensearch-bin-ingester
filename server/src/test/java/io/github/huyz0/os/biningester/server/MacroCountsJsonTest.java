// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.binstore.StoreCounts;
import org.junit.jupiter.api.Test;

class MacroCountsJsonTest {

    @Test
    void macroCountSnapshotCarriesEveryCounter() {
        String podId = "pod\n\"a\\\t" + (char) 0;
        assertThat(FrontDoor.macroCountsJson(podId, new StoreCounts(3, 5, 7, 11, 13)))
                .isEqualTo("{\"podId\":\"pod\\n\\\"a\\\\\\t\\u0000\",\"puts\":3,\"gets\":5,\"lists\":7,\"stats\":11,\"deletes\":13}\n");
        assertThat(FrontDoor.macroCountsJson("pod", new StoreCounts(3, 5, 7, 11, 13),
                CrossAzBytes.untracked()))
                .isEqualTo("{\"podId\":\"pod\",\"puts\":3,\"gets\":5,\"lists\":7,\"stats\":11,\"deletes\":13}\n");

        CrossAzBytes crossAz = new CrossAzBytes("az-a");
        crossAz.sent(CrossAzBytes.Transport.PROXY_READ, "az-b", 2);
        crossAz.sent(CrossAzBytes.Transport.INLINE_PUSH, "az-b", 3);
        crossAz.sent(CrossAzBytes.Transport.CONSUMER_POLL, "az-b", 5);
        crossAz.sent(CrossAzBytes.Transport.COMMIT_FORWARD, "az-b", 7);
        crossAz.sent(CrossAzBytes.Transport.INBOX_DRAIN, "az-b", 11);
        crossAz.sent(CrossAzBytes.Transport.DURABLE_SEGMENT_SIGNAL, "az-b", 13);
        assertThat(FrontDoor.macroCountsJson("pod", new StoreCounts(0, 0, 0, 0, 0), crossAz))
                .contains("\"crossAzBytes\":41")
                .contains("\"proxyRead\":2")
                .contains("\"inlinePush\":3")
                .contains("\"consumerPoll\":5")
                .contains("\"commitForward\":7")
                .contains("\"inboxDrain\":11")
                .contains("\"durableSegmentSignal\":13");
    }
}
