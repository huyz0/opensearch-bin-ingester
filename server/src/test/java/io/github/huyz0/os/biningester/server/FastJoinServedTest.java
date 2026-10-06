// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
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
 * A node answers a JOIN of the term it leads over its peer route, and a pod
 * leading no term refuses one (ADR-0081 §1; M13.27j).
 */
class FastJoinServedTest {

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
    void theLEADERAnswersAJoinOfItsTermJoined() throws Exception {
        node = IngesterNode.start(ServerProperties.parse(settings()), Clock.systemUTC());
        HttpFastTransport transport = new HttpFastTransport(Duration.ofSeconds(5),
                CrossAzBytes.untracked());

        byte[] answer = transport.exchange("http://localhost:" + node.port(),
                FastFrame.encode(1, POD.podUid(), "uid-pod1",
                        new FastFrame.Join(POD, FastFrame.Held.NONE)));

        FastFrame.Frame frame = FastFrame.decode(answer);
        assertThat(frame.body()).isEqualTo(new FastFrame.Joined(0, FastFrame.HeldStatus.NONE));
        assertThat(frame.header().epoch()).as("the term asked").isEqualTo(1);
    }

    @Test
    void aPODLeadingNoTermRefusesAJoin() throws Exception {
        FastFrame.Body answer = FastJoins.answer(null,
                new FastFrame.Header(FastFrame.KIND_JOIN, 1, POD.podUid(), "uid-pod1"),
                new FastFrame.Join(POD, FastFrame.Held.NONE));

        assertThat(((FastFrame.Refused) answer).reason()).isEqualTo(FastFrame.Reason.NOT_ROSTERED);
    }
}
