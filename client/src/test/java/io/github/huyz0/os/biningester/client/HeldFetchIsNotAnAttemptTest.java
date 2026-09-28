// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.format.FetchMode;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.format.SegmentWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * A fetch answered from a node's HELD failure is not the run's attempt
 * (M10.28 review F1): the run waits out the rest of the hold and asks again,
 * however many held answers it meets, and surfaces the failure only when the
 * node's own fetches reach the attempt budget.
 */
class HeldFetchIsNotAnAttemptTest {

    private static final RunKey KEY = new RunKey(
            UUID.fromString("00000000-0000-0000-0000-0000000000aa"), 0);

    private static byte[] segmentOf(String id) throws Exception {
        SegmentWriter w = new SegmentWriter();
        w.add(KEY, new SegmentRecord(id, OpType.INDEX, OptionalLong.of(1),
                ("{\"id\":\"" + id + "\"}").getBytes(StandardCharsets.UTF_8)), 1L);
        return w.toByteArray(1L);
    }

    private static SegmentFetchHeldException held(int nodeAttempts) {
        return new SegmentFetchHeldException("held", new IOException("503 from the store"),
                Duration.ofSeconds(5), nodeAttempts);
    }

    /** Answers from the hold {@code heldAnswers} times, then with the segment. */
    private static final class HeldThenServed implements SegmentSource {
        private final int heldAnswers;
        private final int nodeAttempts;
        private final byte[] bytes;
        final AtomicInteger asked = new AtomicInteger();

        HeldThenServed(int heldAnswers, int nodeAttempts, byte[] bytes) {
            this.heldAnswers = heldAnswers;
            this.nodeAttempts = nodeAttempts;
            this.bytes = bytes;
        }

        @Override
        public byte[] fetch(io.github.huyz0.os.biningester.format.Grant grant) {
            throw new AssertionError("proxy only");
        }

        @Override
        public byte[] fetchSegment(String segmentKey) throws IOException {
            if (asked.incrementAndGet() <= heldAnswers) {
                throw held(nodeAttempts);
            }
            return bytes;
        }
    }

    @Test
    void manyHeldAnswersSpendNoAttemptAndTheRunWaitsOutTheHold() throws Exception {
        List<Duration> waited = new CopyOnWriteArrayList<>();
        SegmentFetchRetry threeAttempts = new SegmentFetchRetry(Duration.ofSeconds(1),
                Duration.ofSeconds(30), 3, waited::add);
        HeldThenServed source = new HeldThenServed(10, 1, segmentOf("doc-1"));
        try (ConsumerClient client = new ConsumerClient(KEY, 16, source, threeAttempts)) {
            client.deliver(new Delivery(KEY, "seg", 1, 0L, FetchMode.PROXY, new byte[0]));

            for (int poll = 0; poll < 10; poll++) {
                assertThat(client.readNext(Duration.ofSeconds(10)))
                        .as("⚠️ HELD ANSWER %d OF 10 IS AN EMPTY POLL, NOT A SURFACED "
                                + "FAILURE: three attempts would be spent by the third", poll + 1)
                        .isEmpty();
            }
            assertThat(client.readNext(Duration.ofSeconds(10))).isPresent();
        }
        assertThat(waited).as("each held answer's remaining hold is waited out")
                .hasSize(10).containsOnly(Duration.ofSeconds(5));
    }

    /** Answers from the hold, reporting whatever node count the test sets. */
    private static final class ScriptedHold implements SegmentSource {
        final AtomicInteger nodeAttempts = new AtomicInteger(1);

        @Override
        public byte[] fetch(io.github.huyz0.os.biningester.format.Grant grant) {
            throw new AssertionError("proxy only");
        }

        @Override
        public byte[] fetchSegment(String segmentKey) throws IOException {
            throw held(nodeAttempts.get());
        }
    }

    @Test
    void aRunPausesAfterTheBudgetOfNodeFetchesSinceItBeganWaitingAndAResumeStartsANewRound()
            throws Exception {
        SegmentFetchRetry threeAttempts = new SegmentFetchRetry(Duration.ofSeconds(1),
                Duration.ofSeconds(30), 3, wait -> { });
        ScriptedHold source = new ScriptedHold();
        try (ConsumerClient client = new ConsumerClient(KEY, 16, source, threeAttempts)) {
            client.deliver(new Delivery(KEY, "seg", 1, 0L, FetchMode.PROXY, new byte[0]));

            source.nodeAttempts.set(1);
            assertThat(client.readNext(Duration.ofSeconds(10))).isEmpty();
            source.nodeAttempts.set(2);
            assertThat(client.readNext(Duration.ofSeconds(10))).isEmpty();
            source.nodeAttempts.set(3);
            assertThatThrownBy(() -> client.readNext(Duration.ofSeconds(10)))
                    .as("⚠️ THE NODE HAS FAILED THREE TIMES SINCE THE RUN BEGAN WAITING: it "
                            + "pauses where an operator sees it, though it fetched nothing")
                    .hasStackTraceContaining("attempt 3 of 3")
                    .hasRootCauseMessage("503 from the store");

            // ⚠️ THE OPERATOR's RESUME, inside the same hold (review F4).
            assertThat(client.readNext(Duration.ofSeconds(10)))
                    .as("⚠️ A RESUMED RUN GETS A FRESH ROUND: an empty poll, not a pause on "
                            + "its first poll at the node's old count")
                    .isEmpty();
            source.nodeAttempts.set(4);
            assertThat(client.readNext(Duration.ofSeconds(10))).isEmpty();
            source.nodeAttempts.set(5);
            assertThatThrownBy(() -> client.readNext(Duration.ofSeconds(10)))
                    .as("three node failures since the resume: count 3 was the first of them")
                    .hasStackTraceContaining("attempt 3 of 3");
        }
    }

    @Test
    void aRunThatFirstMeetsTheKeyPastTheBudgetIsNotPausedOnItsFirstPoll() throws Exception {
        SegmentFetchRetry threeAttempts = new SegmentFetchRetry(Duration.ofSeconds(1),
                Duration.ofSeconds(30), 3, wait -> { });
        ScriptedHold source = new ScriptedHold();
        source.nodeAttempts.set(10);
        try (ConsumerClient client = new ConsumerClient(KEY, 16, source, threeAttempts)) {
            client.deliver(new Delivery(KEY, "seg", 1, 0L, FetchMode.PROXY, new byte[0]));

            assertThat(client.readNext(Duration.ofSeconds(10)))
                    .as("⚠️ A NEWLY ASSIGNED RUN, the node's count already past the budget")
                    .isEmpty();
            source.nodeAttempts.set(1);
            assertThat(client.readNext(Duration.ofSeconds(10)))
                    .as("and a count the node reset starts a new round rather than an endless "
                            + "one")
                    .isEmpty();
            source.nodeAttempts.set(2);
            assertThat(client.readNext(Duration.ofSeconds(10))).isEmpty();
            source.nodeAttempts.set(3);
            assertThatThrownBy(() -> client.readNext(Duration.ofSeconds(10)))
                    .hasStackTraceContaining("attempt 3 of 3");
        }
    }

    /** Answers each fetch with the next scripted answer: a held count, a failure, or bytes. */
    private static final class Script implements SegmentSource {
        private final java.util.ArrayDeque<Object> answers = new java.util.ArrayDeque<>();

        Script then(Object answer) {
            answers.add(answer);
            return this;
        }

        @Override
        public byte[] fetch(io.github.huyz0.os.biningester.format.Grant grant) {
            throw new AssertionError("proxy only");
        }

        @Override
        public byte[] fetchSegment(String segmentKey) throws IOException {
            Object answer = answers.poll();
            if (answer instanceof Integer nodeAttempts) {
                throw held(nodeAttempts);
            }
            if (answer instanceof IOException failure) {
                throw failure;
            }
            if (answer instanceof byte[] bytes) {
                return bytes;
            }
            throw new AssertionError("the script ran out at " + segmentKey);
        }
    }

    private static byte[] segmentAt(String id, long offset) throws Exception {
        SegmentWriter w = new SegmentWriter();
        w.add(KEY, new SegmentRecord(id, OpType.INDEX, OptionalLong.of(1),
                ("{\"id\":\"" + id + "\"}").getBytes(StandardCharsets.UTF_8)), offset);
        return w.toByteArray(offset);
    }

    @Test
    void aSuccessEndsTheRoundSoTheNextSegmentsHeldCountIsNotMeasuredFromTheLast()
            throws Exception {
        SegmentFetchRetry threeAttempts = new SegmentFetchRetry(Duration.ofSeconds(1),
                Duration.ofSeconds(30), 3, wait -> { });
        Script source = new Script().then(1).then(segmentAt("doc-a", 0)).then(10);
        try (ConsumerClient client = new ConsumerClient(KEY, 16, source, threeAttempts)) {
            client.deliver(new Delivery(KEY, "seg-a", 1, 0L, FetchMode.PROXY, new byte[0]));
            client.deliver(new Delivery(KEY, "seg-b", 1, 1L, FetchMode.PROXY, new byte[0]));

            assertThat(client.readNext(Duration.ofSeconds(10))).as("seg-a held once").isEmpty();
            assertThat(client.readNext(Duration.ofSeconds(10))).as("seg-a served").isPresent();
            assertThat(client.readNext(Duration.ofSeconds(10)))
                    .as("⚠️ seg-b FIRST MET AT THE NODE's TENTH FAILURE: seg-a's success ended "
                            + "the round, so this is a new one, not ten failures since a "
                            + "baseline of zero")
                    .isEmpty();
        }
    }

    @Test
    void aPauseThroughTheHoldAlsoEndsTheRunsOwnRoundOfAttempts() throws Exception {
        SegmentFetchRetry threeAttempts = new SegmentFetchRetry(Duration.ofSeconds(1),
                Duration.ofSeconds(30), 3, wait -> { });
        Script source = new Script()
                .then(new IOException("own 1")).then(new IOException("own 2"))
                .then(1).then(2).then(3)
                .then(new IOException("own after resume"));
        try (ConsumerClient client = new ConsumerClient(KEY, 16, source, threeAttempts)) {
            client.deliver(new Delivery(KEY, "seg", 1, 0L, FetchMode.PROXY, new byte[0]));

            for (int poll = 0; poll < 2 * 2; poll++) {
                // two real failures, each followed by a poll that serves its backoff
                assertThat(client.readNext(Duration.ofSeconds(10))).isEmpty();
            }
            assertThat(client.readNext(Duration.ofSeconds(10))).as("held, node 1").isEmpty();
            assertThat(client.readNext(Duration.ofSeconds(10))).as("held, node 2").isEmpty();
            assertThatThrownBy(() -> client.readNext(Duration.ofSeconds(10)))
                    .as("the premise: paused through the hold, the run's own count at two")
                    .hasStackTraceContaining("attempt 3 of 3");

            assertThat(client.readNext(Duration.ofSeconds(10)))
                    .as("⚠️ THE RESUMED RUN's FIRST REAL FAILURE IS ITS FIRST, not its third: "
                            + "the pause reset its own count too")
                    .isEmpty();
        }
    }
}
