// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.FastWriteFrame;
import io.github.huyz0.os.biningester.format.RunKey;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A writer's EXPOSEDs, by batch identity (M13.29a): one arriving before its
 * ASSIGNED -- a batch at {@code wal_quorum 1} is exposed at its flush
 * (M13.27w's carry) -- is kept for the writer that asks, and the kept ones
 * are bounded.
 */
@Timeout(30)
class ExposedInboxTest {

    private static final RunKey S = new RunKey(new UUID(1, 1), 0);

    private static FastWriteFrame.Exposed exposed(long assignedAfter, long first) {
        return new FastWriteFrame.Exposed(assignedAfter,
                List.of(new FastWriteFrame.RunAt(S, first)));
    }

    private static List<FastWriteFrame.RunAt> at(long first) {
        return List.of(new FastWriteFrame.RunAt(S, first));
    }

    @Test
    void anEXPOSEDDeliveredFirstIsFoundByTheWriterThatAsks() throws Exception {
        ExposedInbox inbox = new ExposedInbox();
        inbox.deliver(7, exposed(3, 100));

        assertThat(inbox.await(7, 3, at(100), Duration.ZERO)).isTrue();
    }

    @Test
    void aWAITINGWriterIsWokenByItsExposed() throws Exception {
        ExposedInbox inbox = new ExposedInbox();
        CompletableFuture<Boolean> waiting = CompletableFuture.supplyAsync(() -> {
            try {
                return inbox.await(7, 3, at(100), Duration.ofSeconds(20));
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        });

        while (!inbox.waiting(7, 3, at(100))) {
            Thread.onSpinWait();
        }
        inbox.deliver(7, exposed(3, 100));

        assertThat(waiting.get()).isTrue();
    }

    @Test
    void anotherBATCHsExposedWakesNobody() throws Exception {
        ExposedInbox inbox = new ExposedInbox();
        inbox.deliver(7, exposed(3, 101));
        inbox.deliver(8, exposed(3, 100));
        inbox.deliver(7, exposed(4, 100));

        assertThat(inbox.await(7, 3, at(100), Duration.ofMillis(20))).isFalse();
    }

    @Test
    void theKEPTExposedsAreBounded() throws Exception {
        ExposedInbox inbox = new ExposedInbox();
        for (long i = 0; i <= ExposedInbox.CAPACITY; i++) {
            inbox.deliver(7, exposed(3, i));
        }

        assertThat(inbox.await(7, 3, at(0), Duration.ZERO)).as("the oldest dropped").isFalse();
        assertThat(inbox.await(7, 3, at(ExposedInbox.CAPACITY), Duration.ZERO)).isTrue();
    }
}
