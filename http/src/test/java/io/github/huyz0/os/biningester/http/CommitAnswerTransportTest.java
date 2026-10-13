// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.format.CommitAnswer;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.Sequencer;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The commit answer over the wire (M13.34k; ADR-0082 §7): a writer that sends
 * the {@code Accept} is answered with the frame -- its redirected runs with
 * it -- one that does not with the bare delta, and an older leader's bare
 * delta read as an answer of that one delta.
 */
class CommitAnswerTransportTest {

    private static final RunKey A = new RunKey(new UUID(1, 1), 0);
    private static final RunKey FAST = new RunKey(new UUID(2, 2), 0);
    private static final CommitDelta DELTA = new CommitDelta(11, List.of(new SegmentCommit(
            "seg/1", List.of(new RunCommit(A, 4, 100)),
            new SegmentCommit.Attribution("poda", "inc-1", 7))));
    private static final CommitAnswer ANSWER = new CommitAnswer(List.of(DELTA), List.of(FAST));
    private static final CommitRequest REQUEST =
            new CommitRequest("poda", "inc-1", 7, "seg/1", Map.of(A, 4, FAST, 2));

    private WebServer server;

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop();
        }
    }

    private String peer() {
        Sequencer leader = new Sequencer() {
            @Override
            public CommitDelta commitAll(List<CommitRequest> requests) {
                return DELTA;
            }

            @Override
            public CommitAnswer commitAnswering(CommitRequest request) {
                return ANSWER;
            }

            @Override
            public void close() {
            }
        };
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().register(new CommitService(() -> leader)))
                .build().start();
        return "http://localhost:" + server.port();
    }

    @Test
    void aWRITERThatAsksIsAnsweredWithItsRedirectsOneThatDoesNotWithTheDelta()
            throws Exception {
        String peer = peer();
        try (var transport = new HttpSequencerTransport(Duration.ofSeconds(5),
                CrossAzBytes.untracked())) {
            assertThat(transport.sendAnswering(peer, REQUEST)).isEqualTo(ANSWER);
            assertThat(transport.send(peer, REQUEST)).as("no Accept: the bare delta")
                    .isEqualTo(DELTA);
        }
    }

    @Test
    void anOLDERLeadersBareDeltaIsReadAsAnAnswerOfThatDelta() throws Exception {
        assertThat(HttpSequencerTransport.answerOf(DELTA.encode()))
                .isEqualTo(new CommitAnswer(List.of(DELTA), List.of()));
        assertThat(HttpSequencerTransport.answerOf(ANSWER.encode())).isEqualTo(ANSWER);
    }
}
