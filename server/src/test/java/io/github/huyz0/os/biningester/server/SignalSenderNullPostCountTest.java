// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.format.DurableSegmentSignalFrame;
import io.github.huyz0.os.biningester.format.SegmentKey;
import io.github.huyz0.os.biningester.http.EndpointSliceView;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * The sender built with no post of the node's own -- {@code off}'s default --
 * counts its lost hints too (M13.70 review round 1, P2).
 */
class SignalSenderNullPostCountTest {

    @Test
    void aDEFAULTPostSendersLostHintIsCounted() {
        Map<String, String> settings = new HashMap<>();
        settings.put(ServerProperties.POD_ID, "pod1");
        settings.put(ServerProperties.POD_UID, "uid-pod1");
        settings.put(ServerProperties.POD_AZ, "az-a");
        settings.put(ServerProperties.TRUST_DOMAIN, "cluster-a");
        settings.put(ServerProperties.PREFIX, "bins/cluster-a");
        settings.put(ServerProperties.STORE_KIND, "memory");
        settings.put(ServerProperties.ENDPOINT, "http://localhost:9443");
        settings.put(ServerProperties.HTTP_PORT, "0");
        settings.put(ServerProperties.PRODUCER_SUBJECT, "producer-1");
        settings.put(ServerProperties.PRODUCER_ALLOWED_INDICES, "logs");
        settings.put(ServerProperties.PEER_TLS, "off");
        settings.put(ServerProperties.PEER_PORT, "9443");
        AtomicInteger lost = new AtomicInteger();

        EndpointMembership.signalSender(ServerProperties.parse(settings), new EndpointSliceView(),
                CrossAzBytes.untracked(), null, lost::incrementAndGet)
                .send(new DurableSegmentSignalFrame("pod1", "az-a",
                                new SegmentKey("bins/cluster-a", 1, "pod1", 1, 48).key()),
                        // TEST-NET-1 (RFC 5737): routed nowhere, and no resolver asked
                        List.of(new EndpointSliceView.Endpoint("pod9", "192.0.2.1", "az-b")));

        assertThat(lost).hasValue(1);
    }
}
