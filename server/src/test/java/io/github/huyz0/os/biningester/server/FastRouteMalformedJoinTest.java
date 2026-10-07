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
import java.util.Arrays;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * A JOIN whose header reads but whose body does not is answered {@code 400}
 * by the route, before the router could admit its epoch (M13.52g review
 * round 1, T2): binding it needs its body, so a malformed one never reaches
 * the fence.
 */
class FastRouteMalformedJoinTest {

    private static final Roster.Incarnation POD2 =
            new Roster.Incarnation("pod2", "uid-pod2", "az-b", "");

    private IngesterNode node;

    @AfterEach
    void stop() throws Exception {
        if (node != null) {
            node.close();
        }
    }

    private HttpResponse<byte[]> post(HttpClient client, byte[] body) throws Exception {
        return client.send(HttpRequest.newBuilder(URI.create("https://localhost:"
                        + node.peerPort() + "/ctl/fast")).timeout(Duration.ofSeconds(5))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body)).build(),
                HttpResponse.BodyHandlers.ofByteArray());
    }

    @Test
    void aMALFORMEDJoinIsRefused400AndLeavesTheFenceUnmoved() throws Exception {
        node = PeerTestClients.mutualPod1();
        HttpClient pod2 = PeerTestClients.client("pod2");
        byte[] join = FastFrame.encode(Long.MAX_VALUE, "uid-pod2", "uid-pod1",
                new FastFrame.Join(POD2, FastFrame.Held.NONE));
        byte[] trailing = Arrays.copyOf(join, join.length + 1);

        assertThat(post(pod2, trailing).statusCode()).isEqualTo(400);

        HttpResponse<byte[]> own = post(pod2, FastFrame.encode(0, "uid-pod2", "uid-pod1",
                new FastFrame.Join(POD2, FastFrame.Held.NONE)));
        assertThat(own.statusCode()).isEqualTo(200);
        assertThat(FastFrame.decode(own.body()).header().epoch())
                .as("the fence never Long.MAX_VALUE").isLessThan(Long.MAX_VALUE);
    }
}
