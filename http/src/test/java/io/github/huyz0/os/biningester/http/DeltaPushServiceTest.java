// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.DeltaPushFrame;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.helidon.webclient.api.WebClient;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** {@code /ctl/push}: a whole delta, accepted only from a ready pod of this AZ (M10.19). */
class DeltaPushServiceTest {

    private static final RunKey KEY = new RunKey(UUID.randomUUID(), 0);

    private record Received(long epoch, long sequence) {
    }

    /** The loopback address, as a ready pod of {@code az}. */
    private static EndpointSliceView loopbackIn(String az) {
        EndpointSliceView view = new EndpointSliceView();
        view.apply("{\"type\":\"ADDED\",\"object\":{\"metadata\":{\"name\":\"s1\"},"
                + "\"endpoints\":[{\"addresses\":[\"127.0.0.1\"],\"zone\":\"" + az + "\","
                + "\"conditions\":{\"ready\":true},"
                + "\"targetRef\":{\"name\":\"peer\",\"uid\":\"uid-peer\"}}]}}");
        return view;
    }

    private static CommitDelta delta(long sequence) {
        return new CommitDelta(sequence, "bins/cluster-a/data/s.bseg",
                List.of(new RunCommit(KEY, 2, 0)));
    }

    private static int post(WebServer server, byte[] body) {
        try (var response = WebClient.builder().baseUri("http://127.0.0.1:" + server.port())
                .build().post(DeltaPushService.PATH).submit(body)) {
            return response.status().code();
        }
    }

    private static WebServer serving(EndpointSliceView members, List<Received> into) {
        return WebServer.builder().port(0).routing(HttpRouting.builder().register(
                new DeltaPushService(members, "az-a",
                        (epoch, delta) -> into.add(new Received(epoch, delta.sequence())))))
                .build().start();
    }

    @Test
    void aPushFromAReadyPodOfThisAzIsHandedToThePublisher() {
        List<Received> received = new CopyOnWriteArrayList<>();
        WebServer server = serving(loopbackIn("az-a"), received);
        try {
            assertThat(post(server, new DeltaPushFrame(4, delta(11)).encode())).isEqualTo(204);
            assertThat(received).containsExactly(new Received(4, 11));
        } finally {
            server.stop();
        }
    }

    @Test
    void aPushFromAnotherAzOrAnUnknownSourceIsRefusedBeforeItIsRead() {
        List<Received> received = new CopyOnWriteArrayList<>();
        WebServer otherAz = serving(loopbackIn("az-b"), received);
        WebServer unknown = serving(new EndpointSliceView(), received);
        try {
            byte[] body = new DeltaPushFrame(4, delta(11)).encode();
            assertThat(post(otherAz, body)).as("across AZs only a hint travels").isEqualTo(403);
            assertThat(post(unknown, body)).isEqualTo(403);
            assertThat(received).isEmpty();
        } finally {
            otherAz.stop();
            unknown.stop();
        }
    }

    @Test
    void anAddressTwoReadyPodsShareIdentifiesNeither() {
        EndpointSliceView ambiguous = new EndpointSliceView();
        ambiguous.apply("{\"type\":\"ADDED\",\"object\":{\"metadata\":{\"name\":\"s1\"},"
                + "\"endpoints\":["
                + "{\"addresses\":[\"10.0.0.7\"],\"zone\":\"az-a\",\"conditions\":{\"ready\":true},"
                + "\"targetRef\":{\"name\":\"p1\",\"uid\":\"uid-p1\"}},"
                + "{\"addresses\":[\"10.0.0.7\"],\"zone\":\"az-a\",\"conditions\":{\"ready\":true},"
                + "\"targetRef\":{\"name\":\"p2\",\"uid\":\"uid-p2\"}}]}}");
        DeltaPushService service = new DeltaPushService(ambiguous, "az-a", (epoch, delta) -> { });

        assertThat(ambiguous.readyEndpoints()).as("the premise: both are ready").hasSize(2);
        assertThat(service.authorized("10.0.0.7")).isFalse();
        assertThat(new DeltaPushService(loopbackIn("az-a"), "az-a", (epoch, delta) -> { })
                .authorized("127.0.0.1")).as("one ready pod of this AZ is").isTrue();

        EndpointSliceView two = new EndpointSliceView();
        two.apply("{\"type\":\"ADDED\",\"object\":{\"metadata\":{\"name\":\"s1\"},"
                + "\"endpoints\":["
                + "{\"addresses\":[\"10.0.0.8\"],\"zone\":\"az-a\",\"conditions\":{\"ready\":true},"
                + "\"targetRef\":{\"name\":\"p1\",\"uid\":\"uid-p1\"}},"
                + "{\"addresses\":[\"10.0.0.9\"],\"zone\":\"az-a\",\"conditions\":{\"ready\":true},"
                + "\"targetRef\":{\"name\":\"p2\",\"uid\":\"uid-p2\"}}]}}");
        assertThat(new DeltaPushService(two, "az-a", (epoch, delta) -> { }).authorized("10.0.0.9"))
                .as("the pod at that address, not every ready pod").isTrue();
    }

    @Test
    void aMalformedOrOversizedPushIsRefused() {
        List<Received> received = new CopyOnWriteArrayList<>();
        WebServer server = serving(loopbackIn("az-a"), received);
        try {
            byte[] good = new DeltaPushFrame(4, delta(11)).encode();
            byte[] badMagic = good.clone();
            badMagic[0] = 0;
            assertThat(post(server, badMagic)).isEqualTo(400);
            assertThat(post(server, new byte[(int) DeltaPushService.MAX_BODY_BYTES + 1]))
                    .isEqualTo(413);
            assertThat(received).isEmpty();
        } finally {
            server.stop();
        }
    }

    @Test
    void theFanOutsOwnHttpSenderReachesTheRoute() throws Exception {
        List<Received> received = new CopyOnWriteArrayList<>();
        EndpointSliceView members = loopbackIn("az-a");
        WebServer server = serving(members, received);
        List<EndpointSliceView.Endpoint> fleet = List.of(
                new EndpointSliceView.Endpoint("self", "10.9.9.9", "az-a"),
                new EndpointSliceView.Endpoint("peer", "127.0.0.1", "az-a"));
        try (DeltaFanOut fanOut = new DeltaFanOut("self", "az-a", server.port(),
                new CrossAzBytes("az-a"), () -> fleet, (delta, epoch) -> { })) {
            fanOut.committed(delta(5), 2);
            fanOut.committed(delta(6), 2);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (fanOut.delivered() < 2 && System.nanoTime() < deadline) {
                Thread.onSpinWait();
            }
            assertThat(received).containsExactly(new Received(2, 5), new Received(2, 6));
            assertThat(fanOut.dropped()).isZero();
        } finally {
            server.stop();
        }
    }
}
