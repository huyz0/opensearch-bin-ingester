// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.Roster;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Through a node's real peer listener, under mutual TLS: a correctly chained
 * certificate for a pod the membership view no longer lists moves nothing
 * (M13.71's row).
 *
 * <p>⚠️ pod2's certificate stands in for a REPLACED pod's: the test CA's key
 * was deleted once the fixtures were made, so no certificate for a new UID can
 * be minted -- and what makes a pod gone is the view not listing it, which
 * this case drives on the node's own view.
 */
class FastRouteLivenessNodeTest {

    private static final Roster.Incarnation POD2 =
            new Roster.Incarnation("pod2", "uid-pod2", "az-b", "");

    private IngesterNode node;

    @AfterEach
    void stop() throws Exception {
        if (node != null) {
            node.close();
        }
    }

    private static String slice(String name, String pod, String uid) {
        return "{\"type\":\"ADDED\",\"object\":{\"kind\":\"EndpointSlice\","
                + "\"metadata\":{\"name\":\"" + name + "\"},\"endpoints\":[{\"addresses\":"
                + "[\"10.0.0.1\"],\"conditions\":{\"ready\":true},\"targetRef\":{\"kind\":"
                + "\"Pod\",\"name\":\"" + pod + "\",\"uid\":\"" + uid + "\"}}]}}";
    }

    private FastFrame.Frame post(HttpClient client, byte[] frame) throws Exception {
        HttpResponse<byte[]> response = client.send(HttpRequest.newBuilder(URI.create(
                        "https://localhost:" + node.peerPort() + "/ctl/fast"))
                .timeout(Duration.ofSeconds(5))
                .POST(HttpRequest.BodyPublishers.ofByteArray(frame)).build(),
                HttpResponse.BodyHandlers.ofByteArray());
        assertThat(response.statusCode()).as("past the binding").isEqualTo(200);
        return FastFrame.decode(response.body());
    }

    private static FastFrame.Reason reason(FastFrame.Frame answer) {
        return answer.body() instanceof FastFrame.Refused refused ? refused.reason() : null;
    }

    @Test
    void aGONEPodsBoundFramesMoveNothingAndAListedOnesAreAnswered() throws Exception {
        node = PeerTestClients.mutualPod1();
        node.assembly().peerView().apply(slice("s1", "pod-1", "uid-pod1"));
        HttpClient pod2 = PeerTestClients.client("pod2");

        FastFrame.Frame join = post(pod2, FastFrame.encode(Long.MAX_VALUE, "uid-pod2",
                "uid-pod1", new FastFrame.Join(POD2, FastFrame.Held.NONE)));
        FastFrame.Frame held = post(pod2, FastFrame.encode(Long.MAX_VALUE, "uid-pod2",
                "uid-pod1", new FastFrame.HeldReport(FastFrame.Held.NONE)));

        assertThat(reason(join)).isEqualTo(FastFrame.Reason.NOT_ROSTERED);
        assertThat(reason(held)).isEqualTo(FastFrame.Reason.NOT_ROSTERED);
        assertThat(held.header().epoch()).as("⚠️ THE FENCE NEVER Long.MAX_VALUE")
                .isLessThan(Long.MAX_VALUE);

        node.assembly().peerView().apply(slice("s2", "pod-2", "uid-pod2"));
        FastFrame.Frame listed = post(pod2, FastFrame.encode(held.header().epoch(), "uid-pod2",
                "uid-pod1", new FastFrame.Join(POD2, FastFrame.Held.NONE)));
        assertThat(reason(listed)).as("once listed, it is past the liveness check")
                .isNotEqualTo(FastFrame.Reason.NOT_ROSTERED);
    }
}
