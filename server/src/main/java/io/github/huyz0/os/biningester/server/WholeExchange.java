// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import io.github.huyz0.os.biningester.sequencer.TermJoiner;
import java.io.IOException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * An exchange bounded WHOLE (M13.35e; M13.27p review round 2, P5): the peer
 * transport's timeout bounds its connect and each read, so one exchange can
 * take several of them; a departure fitted to the shutdown budget needs the
 * exchange itself bounded, connect, send and answer together.
 *
 * <p>⚠️ THE EXCHANGE RUNS ON ITS OWN VIRTUAL THREAD, interrupted when its
 * bound passes: the caller is answered by then, and the thread ends at its
 * transport's own timeout at the latest.
 */
final class WholeExchange implements TermJoiner.Transport {

    private final TermJoiner.Transport inner;
    private final Duration bound;

    WholeExchange(TermJoiner.Transport inner, Duration bound) {
        this.inner = Objects.requireNonNull(inner, "inner");
        this.bound = Objects.requireNonNull(bound, "bound");
    }

    @Override
    public byte[] exchange(String endpoint, byte[] frame) throws IOException {
        return within(bound, "a fast exchange with " + endpoint,
                () -> inner.exchange(endpoint, frame));
    }

    /** One blocking call that may throw an {@link IOException}. */
    @FunctionalInterface
    interface Call<T> {
        T call() throws IOException;
    }

    /** {@code call}, answered within {@code bound}: a lease read's bound too (M13.35e). */
    static <T> T within(Duration bound, String what, Call<T> call) throws IOException {
        CompletableFuture<T> answer = new CompletableFuture<>();
        Thread running = Thread.ofVirtual().name("fast-exchange").start(() -> {
            try {
                answer.complete(call.call());
            } catch (Throwable failed) {
                answer.completeExceptionally(failed);
            }
        });
        try {
            return answer.get(bound.toNanos(), TimeUnit.NANOSECONDS);
        } catch (TimeoutException late) {
            running.interrupt();
            throw new IOException(what + " took over " + bound);
        } catch (InterruptedException stopping) {
            running.interrupt();
            Thread.currentThread().interrupt();
            throw new IOException("interrupted: " + what, stopping);
        } catch (ExecutionException failed) {
            Throwable cause = failed.getCause();
            if (cause instanceof IOException io) {
                throw io;
            }
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw new IOException(what + " failed", cause);
        }
    }
}
