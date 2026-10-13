// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.Test;

/**
 * An exchange bounded whole (M13.35e): answered by its bound however long
 * the transport under it takes, and otherwise exactly as that transport
 * answers.
 */
class WholeExchangeTest {

    @Test
    void aSLOWExchangeIsAnsweredAtItsBound() throws Exception {
        CountDownLatch never = new CountDownLatch(1);
        WholeExchange bounded = new WholeExchange((endpoint, frame) -> {
            try {
                never.await();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
            return new byte[0];
        }, Duration.ofMillis(200));
        long started = System.nanoTime();

        assertThatThrownBy(() -> bounded.exchange("http://slow:1", new byte[] {1}))
                .isInstanceOf(IOException.class);

        assertThat(Duration.ofNanos(System.nanoTime() - started))
                .isLessThan(Duration.ofSeconds(2));
    }

    @Test
    void anANSWEROrAFailureIsTheTransportsOwn() throws Exception {
        WholeExchange answering = new WholeExchange((endpoint, frame) -> new byte[] {7},
                Duration.ofSeconds(5));
        WholeExchange failing = new WholeExchange((endpoint, frame) -> {
            throw new IOException("refused");
        }, Duration.ofSeconds(5));

        assertThat(answering.exchange("http://a:1", new byte[] {1})).containsExactly(7);
        assertThatThrownBy(() -> failing.exchange("http://a:1", new byte[] {1}))
                .isInstanceOf(IOException.class).hasMessage("refused");
    }

    @Test
    void anUNCHECKEDFailureIsTheTransportsOwnToo() {
        WholeExchange failing = new WholeExchange((endpoint, frame) -> {
            throw new IllegalStateException("broken");
        }, Duration.ofSeconds(5));

        assertThatThrownBy(() -> failing.exchange("http://a:1", new byte[] {1}))
                .isInstanceOf(IllegalStateException.class).hasMessage("broken");
    }
}
