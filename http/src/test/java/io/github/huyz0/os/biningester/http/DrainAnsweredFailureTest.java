// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.Sequencer;
import io.github.huyz0.os.biningester.sequencer.SequencerTransport;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * A drain the leaseholder RAN and could not finish is told apart from one that
 * never got an answer (M13.80): only the first read the whole inbox, so only
 * the first backs a deferring pod's retry off.
 */
class DrainAnsweredFailureTest {

    private WebServer server;

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop();
        }
    }

    private static final Sequencer HELD = new Sequencer() {
        @Override
        public CommitDelta commitAll(List<CommitRequest> requests) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }
    };

    @Test
    void aDRAINThatRanAndLeftIntentsIsAnsweredAsFailed() {
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().register(new CommitService(() -> HELD,
                        (term, requester) -> {
                            throw new IOException("1 pod(s) still have intents in the inbox");
                        })))
                .build().start();
        String endpoint = "http://localhost:" + server.port();

        try (HttpSequencerTransport transport = new HttpSequencerTransport(Duration.ofSeconds(5))) {
            assertThatThrownBy(() -> transport.drain(endpoint, "podb"))
                    .isInstanceOf(SequencerTransport.DrainFailedException.class)
                    .hasMessageContaining("still have intents");
        }
    }

    @Test
    void anotherREFUSALIsNotAnAnsweredFailure() {
        // ⚠️ M13.80 review T3: only the route's 500 is a drain that ran; a proxy's
        // 503 or a gate's refusal read no inbox.
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().post(HttpSequencerTransport.DRAIN_PATH,
                        (request, response) -> response.status(
                                io.helidon.http.Status.SERVICE_UNAVAILABLE_503).send("busy")))
                .build().start();
        String endpoint = "http://localhost:" + server.port();

        try (HttpSequencerTransport transport = new HttpSequencerTransport(Duration.ofSeconds(5))) {
            assertThatThrownBy(() -> transport.drain(endpoint, "podb"))
                    .isInstanceOf(IOException.class)
                    .isNotInstanceOf(SequencerTransport.DrainFailedException.class);
        }
    }

    @Test
    void anUNREACHABLELeaseholderIsNotAnAnsweredFailure() throws Exception {
        int closedPort;
        try (java.net.ServerSocket probe = new java.net.ServerSocket(0)) {
            closedPort = probe.getLocalPort();
        }
        try (HttpSequencerTransport transport = new HttpSequencerTransport(Duration.ofSeconds(5))) {
            assertThatThrownBy(() -> transport.drain("http://localhost:" + closedPort, "podb"))
                    .isInstanceOf(IOException.class)
                    .isNotInstanceOf(SequencerTransport.DrainFailedException.class);
        }
    }
}
