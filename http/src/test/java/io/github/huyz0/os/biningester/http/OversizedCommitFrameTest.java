// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * A commit frame past the peer's cap is refused before it is sent (M13.49): at
 * 2 MiB or more the peer closes the connection under it rather than answer its
 * 413, which read as an ambiguous failure -- the frame's size is known first,
 * and the answer is the 413's: refused, nothing applied.
 */
class OversizedCommitFrameTest {

    private WebServer server;

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    void aFRAMEPastThePeersCapIsRefusedWithoutBeingSent() throws Exception {
        AtomicInteger reached = new AtomicInteger();
        server = WebServer.builder().port(0).routing(HttpRouting.builder()
                .post(HttpSequencerTransport.PATH, (req, res) -> {
                    reached.incrementAndGet();
                    res.status(io.helidon.http.Status.OK_200).send();
                })).build().start();
        // ⚠️ ENOUGH STREAMS TO PASS 1 MiB, a few MiB of frame in all
        Map<RunKey, Integer> counts = new HashMap<>();
        for (int i = 0; i < 120_000; i++) {
            counts.put(new RunKey(new UUID(1, i), 0), 1);
        }
        CommitRequest huge = new CommitRequest("poda", "inc-1", 7, "bins/c/data/seg-1", counts);

        try (HttpSequencerTransport transport = new HttpSequencerTransport(Duration.ofSeconds(5))) {
            assertThatThrownBy(() -> transport.send("http://localhost:" + server.port(), huge))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("REFUSED")
                    .hasMessageContaining("nothing was applied");
        }
        assertThat(reached).as("never sent").hasValue(0);
    }

    private static CommitRequest streams(int count) {
        Map<RunKey, Integer> counts = new HashMap<>();
        for (int i = 0; i < count; i++) {
            counts.put(new RunKey(new UUID(2, i), 0), 1);
        }
        return new CommitRequest("poda", "inc-1", 7, "k", counts);
    }

    private static long size(CommitRequest request) {
        return HttpSequencerTransport.frameOf(request).encode().length;
    }

    /** A request whose frame is exactly {@code bytes} long, padded through its segment key. */
    private static CommitRequest frameOf(long bytes) {
        // ⚠️ EACH STREAM ADDS A FIXED SIZE, so the count is computed, not grown
        // one encode at a time (which re-encodes a megabyte per stream)
        long one = size(streams(1));
        long each = size(streams(2)) - one;
        int count = (int) ((bytes - 64 - (one - each)) / each);
        CommitRequest request = streams(count);
        int size = (int) size(request);
        String key = "k" + "x".repeat((int) (bytes - size));
        request = new CommitRequest("poda", "inc-1", 7, key, request.recordCounts());
        assertThat(HttpSequencerTransport.frameOf(request).encode().length)
                .as("the premise: the frame is exactly %d bytes", bytes).isEqualTo(bytes);
        return request;
    }

    @Test
    void aFRAMEOfExactlyTheCapIsSentAndOneByteMoreIsNot() throws Exception {
        AtomicInteger reached = new AtomicInteger();
        server = WebServer.builder().port(0).routing(HttpRouting.builder()
                .post(HttpSequencerTransport.PATH, (req, res) -> {
                    reached.incrementAndGet();
                    req.content().consume();
                    res.status(io.helidon.http.Status.CONFLICT_409).send("not the leaseholder");
                })).build().start();
        String peer = "http://localhost:" + server.port();
        // ⚠️ ACROSS A ZONE, so a counted send shows: an unsent frame counts nothing
        io.github.huyz0.os.biningester.binstore.CrossAzBytes crossAz =
                new io.github.huyz0.os.biningester.binstore.CrossAzBytes("az-a",
                        endpoint -> java.util.Optional.of("az-b"));

        try (HttpSequencerTransport transport = new HttpSequencerTransport(Duration.ofSeconds(5),
                crossAz)) {
            assertThatThrownBy(() -> transport.send(peer,
                    frameOf(CommitService.MAX_FRAME_BYTES + 1)))
                    .hasMessageContaining("REFUSED before sending");
            assertThat(reached).as("one byte past the cap: never sent").hasValue(0);
            assertThat(crossAz.crossAzBytes(
                    io.github.huyz0.os.biningester.binstore.CrossAzBytes.Transport.COMMIT_FORWARD))
                    .as("and no byte counted for it").isZero();

            assertThatThrownBy(() -> transport.send(peer,
                    frameOf(CommitService.MAX_FRAME_BYTES)))
                    .as("exactly the cap: sent, and answered by the peer")
                    .isInstanceOf(
                            io.github.huyz0.os.biningester.sequencer.SequencerTransport
                                    .NotTheLeaseholderException.class);
            assertThat(reached).hasValue(1);
        }
    }
}
