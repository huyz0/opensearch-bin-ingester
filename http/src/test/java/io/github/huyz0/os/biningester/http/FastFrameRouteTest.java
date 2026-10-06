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
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The fast frames' peer route and its client (ADR-0082 §2; M13.27h): one
 * endpoint answering with the router's answer, its body bounded while read,
 * and every frame sent counted against the peer's zone.
 */
class FastFrameRouteTest {

    private static final Roster.Incarnation POD =
            new Roster.Incarnation("p", "uid-p", "az-b", "");

    private WebServer server;

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop();
        }
    }

    private static FastFrameRouter router() throws IOException {
        FastFrameRouter router = new FastFrameRouter("uid-l",
                EpochFence.start(new MemoryBinStore(), "k", Optional.empty()));
        router.handle(FastFrame.KIND_JOIN, (header, body) -> new FastFrame.Refused(
                FastFrame.Reason.NOT_FAST, Optional.empty(), "joined " + header.senderUid()));
        return router;
    }

    private String serve(FastFrameService service) {
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().register(service)).build().start();
        return "http://localhost:" + server.port();
    }

    private static byte[] join(String target) {
        return FastFrame.encode(3, POD.podUid(), target,
                new FastFrame.Join(POD, FastFrame.Held.NONE));
    }

    @Test
    void anEXCHANGEReturnsTheRoutersAnswerAndCountsTheBytesSent() throws Exception {
        String endpoint = serve(new FastFrameService(router()));
        CrossAzBytes crossAz = new CrossAzBytes("az-a");
        HttpFastTransport transport = new HttpFastTransport(Duration.ofSeconds(5), crossAz);
        byte[] frame = join("uid-l");

        byte[] answer = transport.exchange(endpoint, frame);

        assertThat(((FastFrame.Refused) FastFrame.decode(answer).body()).text())
                .isEqualTo("joined uid-p");
        assertThat(crossAz.crossAzBytes(CrossAzBytes.Transport.FAST_CONTROL))
                .as("an endpoint of no known zone counts as cross-AZ").isEqualTo(frame.length);
    }

    @Test
    void aKINDNobodyHandlesIsAnErrorToTheSender() throws Exception {
        String endpoint = serve(new FastFrameService(router()));
        HttpFastTransport transport = new HttpFastTransport(Duration.ofSeconds(5),
                CrossAzBytes.untracked());
        byte[] depart = FastFrame.encode(3, POD.podUid(), "uid-l",
                new FastFrame.Depart(POD, 1, FastFrame.Held.NONE));

        assertThatThrownBy(() -> transport.exchange(endpoint, depart))
                .isInstanceOf(IOException.class).hasMessageContaining("501");
    }

    @Test
    void aMALFORMEDFrameIsRefused400() throws Exception {
        String endpoint = serve(new FastFrameService(router()));
        HttpFastTransport transport = new HttpFastTransport(Duration.ofSeconds(5),
                CrossAzBytes.untracked());

        assertThatThrownBy(() -> transport.exchange(endpoint, new byte[] {1, 2, 3}))
                .isInstanceOf(IOException.class).hasMessageContaining("400");
    }

    @Test
    void aFRAMEPastTheCapIsRefused413WhileRead() throws Exception {
        byte[] frame = join("uid-l");
        String endpoint = serve(new FastFrameService(router(), frame.length - 1));
        HttpFastTransport transport = new HttpFastTransport(Duration.ofSeconds(5),
                CrossAzBytes.untracked());

        assertThatThrownBy(() -> transport.exchange(endpoint, frame))
                .isInstanceOf(IOException.class).hasMessageContaining("413");
    }

    @Test
    void aFAILEDHandlerIsAnErrorToTheSender() throws Exception {
        FastFrameRouter failing = new FastFrameRouter("uid-l",
                EpochFence.start(new MemoryBinStore(), "k", Optional.empty()));
        failing.handle(FastFrame.KIND_JOIN, (header, body) -> {
            throw new IOException("store down");
        });
        String endpoint = serve(new FastFrameService(failing));
        HttpFastTransport transport = new HttpFastTransport(Duration.ofSeconds(5),
                CrossAzBytes.untracked());

        assertThatThrownBy(() -> transport.exchange(endpoint, join("uid-l")))
                .isInstanceOf(IOException.class).hasMessageContaining("500");
    }
}
