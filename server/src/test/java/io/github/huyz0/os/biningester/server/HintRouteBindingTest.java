// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.DurableSegmentSignalFrame;
import io.github.huyz0.os.biningester.format.SegmentKey;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * {@code /ctl/durable-segment} as a {@code mutual} node serves it binds the
 * hint's writer to the client certificate (ADR-0084 decision 8; M13.52g
 * review round 1, T1): the node's own EndpointSlice view lists every writer at
 * this host, so only the binding can refuse.
 */
class HintRouteBindingTest {

    private IngesterNode node;

    @BeforeEach
    void start() throws Exception {
        node = PeerTestClients.mutualPod1();
    }

    /** The node's view: {@code pod} the one ready endpoint at this host. */
    private void listed(String pod) {
        node.assembly().peerView().apply("{\"type\":\"ADDED\",\"object\":{\"metadata\":"
                + "{\"name\":\"ingesters\"},\"endpoints\":[{\"addresses\":[\"127.0.0.1\"],"
                + "\"zone\":\"az-b\",\"conditions\":{\"ready\":true},\"targetRef\":"
                + "{\"name\":\"" + pod + "\"}}]}} ");
    }

    @AfterEach
    void stop() throws Exception {
        if (node != null) {
            node.close();
        }
    }

    private int hint(HttpClient client, String writer) throws Exception {
        byte[] body = new DurableSegmentSignalFrame(writer, "az-b",
                new SegmentKey("bins/cluster-a", 1, writer, 1, 48).key()).encode();
        return client.send(HttpRequest.newBuilder(URI.create("https://127.0.0.1:"
                        + node.peerPort() + "/ctl/durable-segment"))
                .timeout(Duration.ofSeconds(5))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body)).build(),
                HttpResponse.BodyHandlers.discarding()).statusCode();
    }

    @Test
    void theWRITERsOwnCertificateIsServed() throws Exception {
        listed("pod2");

        assertThat(hint(PeerTestClients.client("pod2"), "pod2"))
                .as("the premise: the view authorizes pod2 at this host").isEqualTo(204);
    }

    @Test
    void aHINTWhoseWriterIsNotTheCertificatesPodIsRefused() throws Exception {
        listed("pod2");

        assertThat(hint(PeerTestClients.client("pod1"), "pod2")).isEqualTo(403);
    }

    @Test
    void theWRITERsNameUnderAnotherFleetIsRefused() throws Exception {
        listed("pod1");

        // otherns names pod-1 and uid-pod1 -- the writer's own -- under ns/other
        assertThat(hint(PeerTestClients.client("otherns"), "pod1")).isEqualTo(403);
    }
}
