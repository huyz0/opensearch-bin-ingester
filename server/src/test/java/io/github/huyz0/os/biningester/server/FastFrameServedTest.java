// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.binstore.StoreCounts;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.http.HttpFastTransport;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * A started node serves the fast frames' route, fenced by its own epoch fence,
 * and reports the fast frames it sent among its cross-AZ bytes (M13.27h).
 */
class FastFrameServedTest {

    private IngesterNode node;

    @AfterEach
    void stop() throws Exception {
        if (node != null) {
            node.close();
        }
    }

    private static Map<String, String> settings() {
        Map<String, String> settings = new HashMap<>();
        settings.put(ServerProperties.POD_ID, "pod1");
        settings.put(ServerProperties.POD_UID, "uid-pod1");
        settings.put(ServerProperties.POD_AZ, "az-a");
        settings.put(ServerProperties.TRUST_DOMAIN, "cluster-a");
        settings.put(ServerProperties.PREFIX, "bins/cluster-a");
        settings.put(ServerProperties.STORE_KIND, "memory");
        settings.put(ServerProperties.ENDPOINT, "http://localhost:0");
        settings.put(ServerProperties.HTTP_PORT, "0");
        settings.put(ServerProperties.PRODUCER_SUBJECT, "producer-1");
        settings.put(ServerProperties.PRODUCER_ALLOWED_INDICES, "logs");
        return settings;
    }

    @Test
    void aNODEServesTheRouteBehindItsOwnFence() throws Exception {
        node = IngesterNode.start(ServerProperties.parse(settings()), Clock.systemUTC());
        Roster.Incarnation pod = new Roster.Incarnation("p", "uid-p", "az-b", "");
        HttpFastTransport transport = new HttpFastTransport(Duration.ofSeconds(5),
                CrossAzBytes.untracked());

        byte[] answer = transport.exchange("http://localhost:" + node.port(),
                FastFrame.encode(0, "uid-p", "uid-pod1",
                        new FastFrame.Join(pod, FastFrame.Held.NONE)));

        FastFrame.Frame frame = FastFrame.decode(answer);
        assertThat(((FastFrame.Refused) frame.body()).reason())
                .as("epoch 0 is below the node's fence at its boot term")
                .isEqualTo(FastFrame.Reason.LOWER_EPOCH);
        assertThat(frame.header().epoch()).isEqualTo(1);
        assertThat(frame.header().senderUid()).isEqualTo("uid-pod1");
    }

    @Test
    void theMACROCountsReportFastFramesSentToAnotherZone() {
        CrossAzBytes crossAz = new CrossAzBytes("az-a");
        crossAz.sent(CrossAzBytes.Transport.FAST_CONTROL, "az-b", 17);

        assertThat(FrontDoor.macroCountsJson("pod", new StoreCounts(0, 0, 0, 0, 0), crossAz))
                .contains("\"fastControl\":17")
                .contains("\"crossAzBytes\":17");
    }
}
