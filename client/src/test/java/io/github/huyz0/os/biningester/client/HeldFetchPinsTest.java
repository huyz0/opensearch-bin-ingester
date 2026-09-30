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
import java.util.ArrayDeque;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;

/**
 * M12.19b (M10.28a T2, T3): three edges of a run's answer to a node's HELD
 * failure that no case pinned -- a hold with nothing left of it still defers
 * the run by a millisecond; a node count that falls back to exactly the run's
 * baseline starts a new round; and a pause surfaced by held answers resets the
 * run's own backoff to the floor.
 */
class HeldFetchPinsTest {

    private static final RunKey KEY = new RunKey(
            UUID.fromString("00000000-0000-0000-0000-0000000000ab"), 0);

    private static byte[] segment() throws Exception {
        SegmentWriter w = new SegmentWriter();
        w.add(KEY, new SegmentRecord("doc", OpType.INDEX, OptionalLong.of(1),
                "{\"id\":\"doc\"}".getBytes(StandardCharsets.UTF_8)), 1L);
        return w.toByteArray(1L);
    }

    private static SegmentFetchHeldException held(Duration remaining, int nodeAttempts) {
        return new SegmentFetchHeldException("held", new IOException("503 from the store"),
                remaining, nodeAttempts);
    }

    /** Answers each fetch with the next scripted answer: an exception to throw, or bytes. */
    private static final class Script implements SegmentSource {
        private final ArrayDeque<Object> answers = new ArrayDeque<>();

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
            Object next = answers.poll();
            if (next instanceof IOException failure) {
                throw failure;
            }
            if (next instanceof byte[] bytes) {
                return bytes;
            }
            throw new AssertionError("no answer scripted for this fetch");
        }
    }

    private static ConsumerClient client(Script source, int attempts, List<Duration> waited) {
        return new ConsumerClient(KEY, 16, source, TestRetries.sleepAdvanced(Duration.ofSeconds(1),
                Duration.ofSeconds(30), attempts, waited::add));
    }

    @Test
    void aHeldAnswerWithNothingLeftOfItsHoldStillDefersTheRunByAMillisecond() throws Exception {
        List<Duration> waited = new CopyOnWriteArrayList<>();
        Script source = new Script().then(held(Duration.ZERO, 1)).then(segment());
        try (ConsumerClient client = client(source, 3, waited)) {
            client.deliver(new Delivery(KEY, "seg", 1, 0L, FetchMode.PROXY, new byte[0]));

            assertThat(client.readNext(Duration.ofSeconds(10))).as("the held answer").isEmpty();
            assertThat(client.readNext(Duration.ofSeconds(10))).isPresent();
        }
        assertThat(waited)
                .as("⚠️ ONE MILLISECOND, NOT NONE: an owed zero would read as 'not backing "
                        + "off' and the run would ask again at once, in a loop")
                .containsExactly(Duration.ofMillis(1));
    }

    @Test
    void aNodeCountBackAtExactlyTheRunsBaselineStartsANewRound() throws Exception {
        Duration hold = Duration.ofSeconds(5);
        // Count 5 sets the baseline to 4; count 4 is AT it, which starts a new
        // round (baseline 3); count 6 is then the third since, and pauses.
        Script source = new Script().then(held(hold, 5)).then(held(hold, 4)).then(held(hold, 6));
        try (ConsumerClient client = client(source, 3, new CopyOnWriteArrayList<>())) {
            client.deliver(new Delivery(KEY, "seg", 1, 0L, FetchMode.PROXY, new byte[0]));

            assertThat(client.readNext(Duration.ofSeconds(10))).isEmpty();
            assertThat(client.readNext(Duration.ofSeconds(10))).isEmpty();
            assertThatThrownBy(() -> client.readNext(Duration.ofSeconds(10)))
                    .as("⚠️ AT THE BASELINE IS A RESET: counted from it, 6 is only the second")
                    .hasStackTraceContaining("attempt 3 of 3");
        }
    }

    @Test
    void aPauseSurfacedByHeldAnswersResetsTheRunsOwnBackoffToTheFloor() throws Exception {
        List<Duration> waited = new CopyOnWriteArrayList<>();
        Duration hold = Duration.ofSeconds(5);
        IOException own = new IOException("connection reset");
        // Two own failures grow the backoff 1 s -> 2 s -> 4 s; three held answers
        // then pause the run; the resumed run's next own failure owes the floor.
        Script source = new Script().then(own).then(own)
                .then(held(hold, 1)).then(held(hold, 2)).then(held(hold, 3))
                .then(new IOException("reset again"));
        try (ConsumerClient client = client(source, 3, waited)) {
            client.deliver(new Delivery(KEY, "seg", 1, 0L, FetchMode.PROXY, new byte[0]));

            // ⚠️ POLLED UNTIL THE PAUSE, not a fixed count: an own failure spends
            // one poll failing and the next waiting out its backoff.
            RuntimeException paused = null;
            for (int poll = 0; poll < 12 && paused == null; poll++) {
                try {
                    assertThat(client.readNext(Duration.ofSeconds(10))).isEmpty();
                } catch (RuntimeException surfaced) {
                    paused = surfaced;
                }
            }
            assertThat(paused).as("the premise: the held answers paused the run").isNotNull();
            assertThat(paused).hasStackTraceContaining("attempt 3 of 3");
            assertThat(client.readNext(Duration.ofSeconds(10))).as("resumed; fails again")
                    .isEmpty();
            assertThat(client.readNext(Duration.ofSeconds(10))).as("and waits its backoff")
                    .isEmpty();
        }
        Duration last = waited.get(waited.size() - 1);
        assertThat(last)
                .as("⚠️ THE FLOOR's JITTER (under 1.5 s), not the 4 s the two failures before "
                        + "the pause had grown it to (at least 2 s)")
                .isLessThan(Duration.ofMillis(1_500));
    }
}
