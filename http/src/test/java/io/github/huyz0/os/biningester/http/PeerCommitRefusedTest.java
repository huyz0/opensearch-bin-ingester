// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.sequencer.CommitRefusedException;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.Sequencer;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * A commit the leaseholder REFUSED crosses the socket as a definite refusal
 * (M13.34g review round 1, P1): the leader's {@link CommitRefusedException}
 * is answered 422, and the forwarding side raises the same type -- never the
 * 500 a forward reads as ambiguous and defers to the inbox.
 */
class PeerCommitRefusedTest {

    private WebServer server;

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    void aREFUSEDCommitIsA422AndTheSameDefiniteTypeAtTheForwarder() throws Exception {
        Sequencer refusing = new Sequencer() {
            @Override
            public CommitDelta commitAll(List<CommitRequest> requests) throws IOException {
                throw new CommitRefusedException("stream is fast at the leader");
            }

            @Override
            public void close() {
            }
        };
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().register(new CommitService(() -> refusing)))
                .build().start();
        String endpoint = "http://localhost:" + server.port();

        try (HttpSequencerTransport transport =
                new HttpSequencerTransport(Duration.ofSeconds(5))) {
            assertThatThrownBy(() -> transport.send(endpoint, new CommitRequest("poda", "inc-1",
                    7, "bins/c/data/seg-1", Map.of(new RunKey(new UUID(1, 1), 3), 100))))
                    .isInstanceOf(CommitRefusedException.class)
                    .hasMessageContaining("fast at the leader");
        }
        assertThat(HttpSequencerTransport.COMMIT_REFUSED.code()).isEqualTo(422);
    }
}
