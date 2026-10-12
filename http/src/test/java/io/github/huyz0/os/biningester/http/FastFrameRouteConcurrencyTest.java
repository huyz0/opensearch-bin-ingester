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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The peer route holds a bounded number of frames at once (M13.27z;
 * M13.27h's review round 1, P2): past it a sender is answered 503 and
 * retries, so concurrent 64 MiB bodies never buffer gigabytes on the leader.
 */
@Timeout(60)
class FastFrameRouteConcurrencyTest {

    private static final Roster.Incarnation POD =
            new Roster.Incarnation("p", "uid-p", "az-b", "");

    private WebServer server;

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop();
        }
    }

    private String serve(FastFrameService service) {
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().register(service)).build().start();
        return "http://localhost:" + server.port();
    }

    private static byte[] join() {
        return FastFrame.encode(3, POD.podUid(), "uid-l",
                new FastFrame.Join(POD, FastFrame.Held.NONE));
    }

    @Test
    void aFRAMEPastTheBoundIsAnswered503AndTheNextIsServed() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch open = new CountDownLatch(1);
        FastFrameRouter router = new FastFrameRouter("uid-l",
                EpochFence.start(new MemoryBinStore(), "k", Optional.empty()));
        router.handle(FastFrame.KIND_JOIN, (header, body) -> {
            entered.countDown();
            try {
                open.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return new FastFrame.Refused(FastFrame.Reason.NOT_FAST, Optional.empty(), "held");
        });
        String endpoint = serve(new FastFrameService(router, FastFrameService.MAX_FRAME_BYTES, 1));
        HttpFastTransport transport = new HttpFastTransport(Duration.ofSeconds(20),
                CrossAzBytes.untracked());
        CompletableFuture<byte[]> first = CompletableFuture.supplyAsync(() -> {
            try {
                return transport.exchange(endpoint, join());
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        });
        assertThat(entered.await(20, TimeUnit.SECONDS)).as("the first frame is held").isTrue();

        assertThatThrownBy(() -> transport.exchange(endpoint, join()))
                .isInstanceOf(IOException.class).hasMessageContaining("503");

        open.countDown();
        assertThat(first.get(20, TimeUnit.SECONDS)).isNotEmpty();
        assertThat(transport.exchange(endpoint, join())).as("its slot freed").isNotEmpty();
    }

    @Test
    void theBOUNDIsEightFramesOfAtMost64MiB() {
        // ⚠️ 512 MiB at worst on the leader, against ~3.2 GiB for fifty posts.
        assertThat(FastFrameService.MAX_CONCURRENT_FRAMES).isEqualTo(8);
        assertThat(FastFrameService.MAX_FRAME_BYTES).isEqualTo(64L << 20);
    }
}
