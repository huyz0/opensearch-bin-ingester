// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.format.FetchMode;
import io.github.huyz0.os.biningester.format.Grant;
import io.github.huyz0.os.biningester.format.RunKey;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A run paused by its OWN failed fetches ends its held round too (M10.28a
 * review T1): resumed, it meets the node's hold as a new run does, not measured
 * from the node count it saw before it paused.
 */
class HeldRoundAfterOwnPauseTest {

    private static final RunKey KEY = new RunKey(
            UUID.fromString("00000000-0000-0000-0000-0000000000aa"), 0);

    @Test
    void aResumeAfterTheRunsOwnFailuresSurfacedMeetsTheHoldAsANewRound() throws Exception {
        ArrayDeque<Object> script = new ArrayDeque<>();
        script.add(1);
        script.add(2);
        script.add(new IOException("own 1"));
        script.add(new IOException("own 2"));
        script.add(new IOException("own 3"));
        script.add(3);
        SegmentSource source = new SegmentSource() {
            @Override
            public byte[] fetch(Grant grant) {
                throw new AssertionError("proxy only");
            }

            @Override
            public byte[] fetchSegment(String segmentKey) throws IOException {
                Object answer = script.poll();
                if (answer instanceof Integer nodeAttempts) {
                    throw new SegmentFetchHeldException("held",
                            new IOException("503 from the store"), Duration.ofSeconds(5),
                            nodeAttempts);
                }
                if (answer instanceof IOException failure) {
                    throw failure;
                }
                throw new AssertionError("the script ran out");
            }
        };
        SegmentFetchRetry threeAttempts = new SegmentFetchRetry(Duration.ofSeconds(1),
                Duration.ofSeconds(30), 3, wait -> { });
        try (ConsumerClient client = new ConsumerClient(KEY, 16, source, threeAttempts)) {
            client.deliver(new Delivery(KEY, "seg", 1, 0L, FetchMode.PROXY, new byte[0]));

            assertThat(client.readNext(Duration.ofSeconds(10))).as("held, node 1").isEmpty();
            assertThat(client.readNext(Duration.ofSeconds(10))).as("held, node 2").isEmpty();
            for (int poll = 0; poll < 2 * 2; poll++) {
                // two own failures, each followed by the poll that serves its backoff
                assertThat(client.readNext(Duration.ofSeconds(10))).isEmpty();
            }
            assertThatThrownBy(() -> client.readNext(Duration.ofSeconds(10)))
                    .as("the premise: the run's own third failure pauses it")
                    .hasMessageContaining("own 3");

            assertThat(client.readNext(Duration.ofSeconds(10)))
                    .as("⚠️ RESUMED AT THE NODE's THIRD FAILURE: a new round, not three "
                            + "failures since the baseline it took before it paused")
                    .isEmpty();
        }
    }
}
