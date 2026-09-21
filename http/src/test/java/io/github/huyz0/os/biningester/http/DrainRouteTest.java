// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

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
import java.util.concurrent.atomic.AtomicInteger;
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

    private String serve(Sequencer term, CommitService.Drainer drainer) {
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().register(new CommitService(() -> term, drainer)))
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
        String endpoint = serve(null, (term, requester) -> 0);

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
}
