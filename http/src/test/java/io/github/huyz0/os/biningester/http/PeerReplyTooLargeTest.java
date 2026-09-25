// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.SequencerTransport;
import io.helidon.http.Status;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class PeerReplyTooLargeTest {

    private WebServer server;

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    void a400WithOversizedPeerReplyIsKnownAndDoesNotBlameTheProducer() throws Exception {
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().post(HttpSequencerTransport.PATH,
                        (req, res) -> res.status(Status.BAD_REQUEST_400)
                                .send("x".repeat(5 << 10))))
                .build().start();

        try (HttpSequencerTransport transport = new HttpSequencerTransport(Duration.ofSeconds(5))) {
            assertThatThrownBy(() -> transport.send("http://localhost:" + server.port(), request()))
                    .isInstanceOf(IOException.class)
                    .isNotInstanceOf(SequencerTransport.NotTheLeaseholderException.class)
                    .hasMessageNotContaining("UNKNOWN")
                    .hasMessageContaining("nothing was applied")
                    .hasMessageContaining("<no readable body>")
                    .hasMessageNotContaining("request body exceeds");
        }
    }

    @Test
    void a400WithReadablePeerReplyIsKnown() throws Exception {
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().post(HttpSequencerTransport.PATH,
                        (req, res) -> res.status(Status.BAD_REQUEST_400)
                                .send("invalid commit frame")))
                .build().start();

        try (HttpSequencerTransport transport = new HttpSequencerTransport(Duration.ofSeconds(5))) {
            assertThatThrownBy(() -> transport.send("http://localhost:" + server.port(), request()))
                    .isInstanceOf(IOException.class)
                    .isNotInstanceOf(SequencerTransport.NotTheLeaseholderException.class)
                    .hasMessageNotContaining("UNKNOWN")
                    .hasMessageContaining("nothing was applied")
                    .hasMessageContaining("invalid commit frame")
                    .hasMessageNotContaining("request body exceeds");
        }
    }

    private static CommitRequest request() {
        return new CommitRequest("poda", "inc-1", 7, "bins/c/data/seg-1",
                Map.of(new RunKey(UUID.fromString("00000000-0000-4000-8000-000000000001"), 3),
                        100));
    }
}
