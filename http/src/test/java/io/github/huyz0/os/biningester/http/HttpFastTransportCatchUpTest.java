// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.format.CatchUpEndFrame;
import io.github.huyz0.os.biningester.format.CatchUpRequestFrame;
import io.github.huyz0.os.biningester.format.RunKey;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A catch-up forwarded to the leader's peer path (M13.32i; ADR-0082 §2): its
 * frames relayed when the answer's fast epoch is admitted -- and NOTHING
 * relayed when the epoch is below this pod's fence, when the answer states
 * none, or when it is not a 200.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class HttpFastTransportCatchUpTest {

    private static final UUID REQUEST = new UUID(5, 5);
    private static final CatchUpRequestFrame ASKED = new CatchUpRequestFrame(REQUEST,
            List.of(new CatchUpRequestFrame.Stream(new RunKey(new UUID(1, 1), 0), 0)));

    private static void forward(long leaderEpoch, long fence, boolean failing,
            List<Integer> relayed) throws Exception {
        WebServer server = WebServer.builder().port(0).routing(HttpRouting.builder().register(
                new CatchUpService((CatchUpService.StreamingResponder) (request, sink) -> {
                    if (failing) {
                        throw new IOException("no chain");
                    }
                    sink.write(new CatchUpEndFrame(REQUEST).encode());
                    sink.write(new CatchUpEndFrame(REQUEST).encode());
                }, () -> leaderEpoch).onPeerPath())).build().start();
        try {
            new HttpFastTransport(Duration.ofSeconds(5), CrossAzBytes.untracked())
                    .forwardCatchUp("http://localhost:" + server.port(), ASKED,
                            frame -> relayed.add(frame.length), epoch -> epoch >= fence);
        } finally {
            server.stop();
        }
    }

    @Test
    void anADMITTEDAnswerIsRelayed() throws Exception {
        List<Integer> relayed = new ArrayList<>();

        forward(7, 7, false, relayed);

        assertThat(relayed).as("at the fence, every frame").hasSize(2);
    }

    @Test
    void anANSWERBelowTheFenceStatingNoTermOrNotA200RelaysNothing() {
        List<Integer> relayed = new ArrayList<>();

        assertThatThrownBy(() -> forward(6, 7, false, relayed)).isInstanceOf(IOException.class)
                .hasMessageContaining("below this pod's fence");
        assertThatThrownBy(() -> forward(0, 7, false, relayed)).isInstanceOf(IOException.class)
                .hasMessageContaining("stating no term");
        assertThatThrownBy(() -> forward(7, 7, true, relayed)).isInstanceOf(IOException.class)
                .hasMessageContaining("503");
        assertThat(relayed).as("no frame of any of them").isEmpty();
    }
}
