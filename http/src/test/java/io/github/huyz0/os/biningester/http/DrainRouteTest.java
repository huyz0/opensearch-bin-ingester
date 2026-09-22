// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
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
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The drain a deferring pod asks for, over a real socket (M8.14a, ADR-0058).
 *
 * <p>⚠️ **ONLY A 200 LETS THE POD STOP DEFERRING**: a node that holds no term
 * answers as a commit would (409), and a drain that could not apply every
 * intent answers 500 -- either way the pod keeps writing intents rather than
 * forwarding past its own.
 */
class DrainRouteTest {

    private static final UUID STREAM = UUID.fromString("00000000-0000-4000-8000-000000000001");

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

    private static CommitRequest request() {
        return new CommitRequest("poda", "inc-1", 7, "bins/c/data/seg-1",
                Map.of(new io.github.huyz0.os.biningester.format.RunKey(STREAM, 3), 100));
    }

    private static CommitDelta delta() {
        return new CommitDelta(11, List.of(new io.github.huyz0.os.biningester.format.SegmentCommit(
                "bins/c/data/seg-1",
                List.of(new io.github.huyz0.os.biningester.format.RunCommit(
                        new io.github.huyz0.os.biningester.format.RunKey(STREAM, 3), 100, 5000)),
                new io.github.huyz0.os.biningester.format.SegmentCommit.Attribution(
                        "poda", "inc-1", 7))));
    }

    private String serve(Sequencer term, CommitService.Drainer drainer) {
        return serve(() -> term, drainer);
    }

    private String serve(Supplier<Sequencer> term, CommitService.Drainer drainer) {
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().register(new CommitService(term, drainer)))
                .build().start();
        return "http://localhost:" + server.port();
    }

    @Test
    void aDRAINThatAppliedEverythingIsA200AndRUNSThroughTheHeldTerm() throws Exception {
        AtomicInteger drained = new AtomicInteger();
        java.util.concurrent.atomic.AtomicReference<String> asker =
                new java.util.concurrent.atomic.AtomicReference<>();
        String endpoint = serve(HELD, (term, requester) -> {
            drained.incrementAndGet();
            asker.set(requester);
            return term == HELD ? 3 : -1;
        });

        try (HttpSequencerTransport transport = new HttpSequencerTransport(Duration.ofSeconds(5))) {
            assertThatCode(() -> transport.drain(endpoint, "podb")).doesNotThrowAnyException();
        }
        org.assertj.core.api.Assertions.assertThat(drained.get()).isEqualTo(1);
        org.assertj.core.api.Assertions.assertThat(asker.get())
                .as("⚠️ THE ASKER TRAVELS: a drain fails only for the pod whose intents stuck")
                .isEqualTo("podb");
    }

    @Test
    void aNODEHoldingNoTermREFUSESTheDrainAsItWouldACommit() {
        String endpoint = serve((Sequencer) null, (term, requester) -> 0);

        try (HttpSequencerTransport transport = new HttpSequencerTransport(Duration.ofSeconds(5))) {
            assertThatThrownBy(() -> transport.drain(endpoint, "podb"))
                    .isInstanceOf(SequencerTransport.NotTheLeaseholderException.class);
        }
    }

    @Test
    void aDRAINThatLeftIntentsIsAFailureSoThePodKEEPSDeferring() {
        String endpoint = serve(HELD, (term, requester) -> {
            throw new IOException("1 pod(s) still have intents in the inbox");
        });

        try (HttpSequencerTransport transport = new HttpSequencerTransport(Duration.ofSeconds(5))) {
            assertThatThrownBy(() -> transport.drain(endpoint, "podb"))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("still have intents");
        }
    }

    @Test
    void aDRAINBlocksLatePeerCommitsUntilItsInboxBarrierHasCompleted() throws Exception {
        CountDownLatch drainStarted = new CountDownLatch(1);
        CountDownLatch releaseDrain = new CountDownLatch(1);
        CountDownLatch commitEntered = new CountDownLatch(1);
        CountDownLatch commitReachedBarrier = new CountDownLatch(1);
        AtomicInteger supplied = new AtomicInteger();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Sequencer held = new Sequencer() {
            @Override
            public CommitDelta commitAll(List<CommitRequest> requests) {
                commitEntered.countDown();
                return delta();
            }

            @Override
            public void close() {
            }
        };
        String endpoint = serve(() -> {
            if (supplied.incrementAndGet() == 2) {
                commitReachedBarrier.countDown();
            }
            return held;
        }, (term, requester) -> {
            drainStarted.countDown();
            try {
                releaseDrain.await();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IOException("drain test interrupted", interrupted);
            }
            return 0;
        });

        Thread drain = Thread.ofVirtual().start(() -> {
            try (HttpSequencerTransport transport =
                    new HttpSequencerTransport(Duration.ofSeconds(5))) {
                transport.drain(endpoint, "podb");
            } catch (Throwable failed) {
                failure.set(failed);
            }
        });
        assertThat(drainStarted.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();

        Thread commit = Thread.ofVirtual().start(() -> {
            try (HttpSequencerTransport transport =
                    new HttpSequencerTransport(Duration.ofSeconds(5))) {
                transport.send(endpoint, request());
            } catch (Throwable failed) {
                failure.set(failed);
            }
        });
        assertThat(commitReachedBarrier.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        assertThat(commitEntered.await(1, java.util.concurrent.TimeUnit.SECONDS))
                .as("a late peer commit must wait behind the drain barrier")
                .isFalse();
        releaseDrain.countDown();
        drain.join(5_000);
        commit.join(5_000);

        assertThat(failure.get()).isNull();
        assertThat(commitEntered.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
    }
}
