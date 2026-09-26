// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.DeltaHintFrame;
import io.helidon.webclient.api.WebClient;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;

/** {@code /ctl/hint}: accepted only by this AZ's relay, from one ready pod (M10.20). */
class DeltaHintServiceTest {

    /** The loopback source in az-a, plus this pod, "relay-self", in az-b at 10.0.1.5. */
    private static EndpointSliceView members(String selfPod) {
        EndpointSliceView view = new EndpointSliceView();
        view.apply("{\"type\":\"ADDED\",\"object\":{\"metadata\":{\"name\":\"s1\"},"
                + "\"endpoints\":["
                + endpoint("127.0.0.1", "az-a", "leader") + ","
                // The LOWEST pod id overall is in another AZ: the relay is chosen
                // within this AZ, not across the fleet.
                + endpoint("10.0.2.1", "az-c", "a-lowest") + ","
                + endpoint("10.0.1.5", "az-b", selfPod) + ","
                + endpoint("10.0.1.9", "az-b", "zz-other") + "]}}");
        return view;
    }

    private static String endpoint(String address, String az, String pod) {
        return "{\"addresses\":[\"" + address + "\"],\"zone\":\"" + az + "\","
                + "\"conditions\":{\"ready\":true},\"targetRef\":{\"name\":\"" + pod
                + "\",\"uid\":\"uid-" + pod + "\"}}";
    }

    private static WebServer serving(EndpointSliceView members, String selfPod,
            List<DeltaHintFrame> into) {
        return WebServer.builder().port(0).routing(HttpRouting.builder().register(
                new DeltaHintService(members, selfPod, "az-b", into::add))).build().start();
    }

    private static int post(WebServer server, byte[] body) {
        try (var response = WebClient.builder().baseUri("http://127.0.0.1:" + server.port())
                .build().post(DeltaHintService.PATH).submit(body)) {
            return response.status().code();
        }
    }

    @Test
    void theRelayQueuesAHintFromAReadyPod() {
        List<DeltaHintFrame> queued = new CopyOnWriteArrayList<>();
        WebServer server = serving(members("b-relay"), "b-relay", queued);
        try {
            assertThat(post(server, new DeltaHintFrame(4, 17).encode())).isEqualTo(204);
            assertThat(queued).containsExactly(new DeltaHintFrame(4, 17));
        } finally {
            server.stop();
        }
    }

    @Test
    void aPodThatIsNotItsAzsRelayRefusesTheHint() {
        List<DeltaHintFrame> queued = new CopyOnWriteArrayList<>();
        // "zz-self" sorts after "zz-other": the relay of az-b is the other pod.
        WebServer server = serving(members("zz-self"), "zz-self", queued);
        try {
            assertThat(post(server, new DeltaHintFrame(4, 17).encode())).isEqualTo(403);
            assertThat(queued).isEmpty();
        } finally {
            server.stop();
        }
    }

    @Test
    void anUnknownSourceIsRefused() {
        List<DeltaHintFrame> queued = new CopyOnWriteArrayList<>();
        EndpointSliceView noLoopback = new EndpointSliceView();
        noLoopback.apply("{\"type\":\"ADDED\",\"object\":{\"metadata\":{\"name\":\"s1\"},"
                + "\"endpoints\":[" + endpoint("10.0.1.5", "az-b", "b-relay") + "]}}");
        WebServer server = serving(noLoopback, "b-relay", queued);
        try {
            assertThat(post(server, new DeltaHintFrame(4, 17).encode())).isEqualTo(403);
            assertThat(queued).isEmpty();
        } finally {
            server.stop();
        }
    }

    @Test
    void aMalformedOrOversizedHintIsRefused() {
        List<DeltaHintFrame> queued = new CopyOnWriteArrayList<>();
        WebServer server = serving(members("b-relay"), "b-relay", queued);
        try {
            byte[] badMagic = new DeltaHintFrame(4, 17).encode();
            badMagic[0] = 0;
            assertThat(post(server, badMagic)).isEqualTo(400);
            assertThat(post(server, new byte[DeltaHintFrame.BYTES + 1])).isEqualTo(413);
            assertThat(queued).isEmpty();
        } finally {
            server.stop();
        }
    }

    @Test
    void anAddressTwoReadyPodsShareIsNoSource() {
        EndpointSliceView shared = new EndpointSliceView();
        shared.apply("{\"type\":\"ADDED\",\"object\":{\"metadata\":{\"name\":\"s1\"},"
                + "\"endpoints\":[" + endpoint("10.0.0.7", "az-a", "p1") + ","
                + endpoint("10.0.0.7", "az-a", "p2") + ","
                + endpoint("10.0.1.5", "az-b", "b-relay") + "]}}");
        DeltaHintService service = new DeltaHintService(shared, "b-relay", "az-b", hint -> true);

        assertThat(service.authorized("10.0.0.7")).isFalse();
        assertThat(new DeltaHintService(members("b-relay"), "b-relay", "az-b", hint -> true)
                .authorized("10.0.1.9")).as("any ready pod may be the source").isTrue();
    }
}
