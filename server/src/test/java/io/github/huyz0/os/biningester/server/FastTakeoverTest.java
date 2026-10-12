// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.sequencer.RecoveryWriter;
import io.github.huyz0.os.biningester.sequencer.TakeoverRun;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * The server's start of a term's takeover (M13.33r; ADR-0081 §5): its
 * segments uploaded at most one per {@code min_upload_interval}, and the term
 * given back when the takeover cannot finish -- its leader is left holding
 * every stream, and a newer term or a restart takes over again.
 */
class FastTakeoverTest {

    @Test
    void uploadsAREPacedAtTheMinimumInterval() throws Exception {
        AtomicLong now = new AtomicLong(0);
        List<Long> slept = new ArrayList<>();
        RecoveryWriter.SegmentUpload paced = FastTakeover.paced(runs -> "k", now::get,
                Duration.ofMillis(250), nanos -> {
                    slept.add(nanos);
                    now.addAndGet(nanos);
                });

        paced.upload(Map.of());
        now.addAndGet(Duration.ofMillis(100).toNanos());
        paced.upload(Map.of());
        now.addAndGet(Duration.ofMillis(300).toNanos());
        paced.upload(Map.of());

        assertThat(slept).as("the first never waits; the second waits out the rest")
                .containsExactly(Duration.ofMillis(150).toNanos());
    }

    private static final class Term implements java.io.Closeable {
        final AtomicBoolean closed = new AtomicBoolean();

        @Override
        public void close() {
            closed.set(true);
        }
    }

    @Test
    void aFAILEDTakeoverGivesTheTermBack() {
        Term term = new Term();

        FastTakeover.runOrGiveBack(() -> {
            throw new IOException("the chain append failed");
        }, term);

        assertThat(term.closed).isTrue();
    }

    @Test
    void aDEPOSEDTakeoverGivesTheTermBack() {
        Term term = new Term();

        FastTakeover.runOrGiveBack(() -> new TakeoverRun.Deposed(9), term);

        assertThat(term.closed).isTrue();
    }

    @Test
    void anUNEXPECTEDFailureGivesTheTermBack() {
        Term term = new Term();

        FastTakeover.runOrGiveBack(() -> {
            throw new IllegalStateException("two entries at one offset");
        }, term);

        assertThat(term.closed).isTrue();
    }

    @Test
    void anINTERRUPTEDTakeoverGivesTheTermBackAndKeepsTheInterrupt() {
        Term term = new Term();

        FastTakeover.runOrGiveBack(() -> {
            throw new InterruptedException();
        }, term);

        assertThat(term.closed).isTrue();
        assertThat(Thread.interrupted()).as("the flag restored, then cleared here").isTrue();
    }

    @Test
    void anERRORGivesTheTermBackAndIsRethrown() {
        Term term = new Term();

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> FastTakeover.runOrGiveBack(
                () -> {
                    throw new StackOverflowError("a step recursed");
                }, term)).isInstanceOf(StackOverflowError.class);
        assertThat(term.closed).isTrue();
    }

    @Test
    void aFINISHEDTakeoverKeepsTheTerm() {
        Term term = new Term();

        FastTakeover.runOrGiveBack(() -> new TakeoverRun.Done(Map.of(), 1), term);

        assertThat(term.closed).isFalse();
    }

    @Test
    void aPODLeadingNoTermStartsNoTakeover() throws Exception {
        java.time.Clock wall =
                java.time.Clock.fixed(java.time.Instant.EPOCH, java.time.ZoneOffset.UTC);

        assertThat(FastTakeover.start(null, null, null, null, null, null, null, "p", wall,
                Runnable::run)).isFalse();
    }
}
