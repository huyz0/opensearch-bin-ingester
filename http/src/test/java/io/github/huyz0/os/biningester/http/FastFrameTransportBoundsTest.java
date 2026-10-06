// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.sequencer.EpochFence;
import io.github.huyz0.os.biningester.sequencer.FastFrameRouter;
import io.helidon.http.Status;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The transport's counting and its answer bound (M13.27h review round 1, T2-T4):
 * bytes counted against the peer's own zone, counted when the send fails too,
 * and a peer's answer past the bound refused rather than allocated.
 */
class FastFrameTransportBoundsTest {

    private static final Roster.Incarnation POD =
            new Roster.Incarnation("p", "uid-p", "az-b", "");

    private WebServer server;

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop();
        }
    }

    private static byte[] join() {
        return FastFrame.encode(3, POD.podUid(), "uid-l",
                new FastFrame.Join(POD, FastFrame.Held.NONE));
    }

    private String serveRouter() throws IOException {
        FastFrameRouter router = new FastFrameRouter("uid-l",
                EpochFence.start(new MemoryBinStore(), "k", Optional.empty()));
        router.handle(FastFrame.KIND_JOIN, (header, body) -> new FastFrame.Refused(
                FastFrame.Reason.NOT_FAST, Optional.empty(), "x".repeat(200)));
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().register(new FastFrameService(router)))
                .build().start();
        return "http://localhost:" + server.port();
    }

    @Test
    void aFRAMEToAPeerInThisZoneIsCountedSameAz() throws Exception {
        String endpoint = serveRouter();
        CrossAzBytes crossAz = new CrossAzBytes("az-a",
                e -> e.equals(endpoint) ? Optional.of("az-a") : Optional.empty());
        byte[] frame = join();

        new HttpFastTransport(Duration.ofSeconds(5), crossAz).exchange(endpoint, frame);

        assertThat(crossAz.sameAzBytes(CrossAzBytes.Transport.FAST_FRAME))
                .isEqualTo(frame.length);
        assertThat(crossAz.crossAzBytes(CrossAzBytes.Transport.FAST_FRAME)).isZero();
    }

    @Test
    void aFAILEDSendIsCountedAllTheSame() throws Exception {
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().post(HttpFastTransport.PATH,
                        (req, res) -> res.status(Status.INTERNAL_SERVER_ERROR_500).send("no")))
                .build().start();
        String endpoint = "http://localhost:" + server.port();
        CrossAzBytes crossAz = new CrossAzBytes("az-a");
        byte[] frame = join();

        assertThatThrownBy(() -> new HttpFastTransport(Duration.ofSeconds(5), crossAz)
                .exchange(endpoint, frame)).isInstanceOf(IOException.class);

        assertThat(crossAz.crossAzBytes(CrossAzBytes.Transport.FAST_FRAME))
                .as("the bytes were spent whether or not an answer came")
                .isEqualTo(frame.length);
    }

    @Test
    void anANSWERPastTheBoundIsRefusedNotRead() throws Exception {
        String endpoint = serveRouter();

        assertThatThrownBy(() -> new HttpFastTransport(Duration.ofSeconds(5),
                CrossAzBytes.untracked(), 64).exchange(endpoint, join()))
                .isInstanceOf(IOException.class).hasMessageContaining("passed 64 bytes");
    }
}
