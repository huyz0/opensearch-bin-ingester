// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.http.HttpFastTransport;
import java.io.InputStream;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * A pod that joined and departs over the node's route is written
 * {@code DEPARTED} in the leader's roster before it is answered (ADR-0081 §9;
 * M13.27k review round 1, T1).
 */
class FastDepartServedRosterTest {

    private static final Roster.Incarnation POD =
            new Roster.Incarnation("p", "uid-p", "az-b", "http://p:1");

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
        settings.put(ServerProperties.PEER_TLS, "off");
        settings.put(ServerProperties.PEER_PORT, "0");
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

    private FastFrame.Body exchange(FastFrame.Body ask) throws Exception {
        byte[] answer = new HttpFastTransport(Duration.ofSeconds(5), CrossAzBytes.untracked())
                .exchange("http://localhost:" + node.peerPort(),
                        FastFrame.encode(1, POD.podUid(), "uid-pod1", ask));
        return FastFrame.decode(answer).body();
    }

    private Roster roster1() throws Exception {
        try (InputStream in = node.assembly().store().get(Roster.key("bins/cluster-a", 1))) {
            return Roster.decode(in.readAllBytes());
        }
    }

    @Test
    void aJOINEDPodThatDepartsIsWrittenDepartedBeforeItIsAnswered() throws Exception {
        node = IngesterNode.start(ServerProperties.parse(settings()), Clock.systemUTC());
        assertThat(exchange(new FastFrame.Join(POD, FastFrame.Held.NONE)))
                .isInstanceOf(FastFrame.Joined.class);

        FastFrame.Body answer = exchange(new FastFrame.Depart(POD, 2, FastFrame.Held.NONE));

        assertThat(answer).isEqualTo(new FastFrame.HeldStatusReport(FastFrame.HeldStatus.NONE));
        assertThat(roster1().member(POD.podUid()).orElseThrow().state())
                .isEqualTo(Roster.State.DEPARTED);
    }
}
