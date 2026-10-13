// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.FastWriteFrame;
import io.github.huyz0.os.biningester.format.RunKey;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A DISCARDED in the writer's inbox (M13.34t; ADR-0081 §2.3): it ends its
 * batch's wait at once, never exposed; one arriving before its writer asks
 * -- the ASSIGNED still on its way -- is kept for it, taken once, and the
 * kept ones are bounded.
 */
@Timeout(30)
class ExposedInboxDiscardTest {

    private static final RunKey S = new RunKey(new UUID(1, 1), 0);
    private static final Duration LONG = Duration.ofSeconds(60);

    private static FastJournalRecord.IdempotencyKey key(long seq) {
        return new FastJournalRecord.IdempotencyKey("w", new UUID(9, 9), seq);
    }

    private static List<FastWriteFrame.RunAt> at(long first) {
        return List.of(new FastWriteFrame.RunAt(S, first));
    }

    private static FastWriteFrame.Exposed exposed(long assignedAfter, long first) {
        return new FastWriteFrame.Exposed(assignedAfter, at(first));
    }

    private static CompletableFuture<Boolean> awaiting(ExposedInbox inbox,
            FastJournalRecord.IdempotencyKey key, long assignedAfter, long first) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return inbox.await(7, key, assignedAfter, at(first), LONG);
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    @Test
    void aWAITINGWriterIsWokenNotExposedByItsBatchsDiscard() throws Exception {
        ExposedInbox inbox = new ExposedInbox();
        CompletableFuture<Boolean> waiting = awaiting(inbox, key(1), 3, 100);
        while (!inbox.waiting(7, 3, at(100))) {
            Thread.onSpinWait();
        }

        inbox.discarded(7, key(1));

        assertThat(waiting.get(10, TimeUnit.SECONDS)).isFalse();
    }

    @Test
    void aDISCARDBeforeItsWriterAsksIsFoundAtOnceAndTakenOnce() throws Exception {
        ExposedInbox inbox = new ExposedInbox();
        inbox.discarded(7, key(1));

        assertThat(awaiting(inbox, key(1), 3, 100).get(10, TimeUnit.SECONDS)).isFalse();
        inbox.deliver(7, exposed(4, 100));
        assertThat(inbox.await(7, key(1), 4, at(100), Duration.ZERO))
                .as("taken once: the batch's retry is exposed").isTrue();
    }

    @Test
    void anotherBATCHsOrTERMsDiscardEndsNoWait() throws Exception {
        ExposedInbox inbox = new ExposedInbox();
        inbox.discarded(7, key(2));
        inbox.discarded(8, key(1));
        inbox.deliver(7, exposed(3, 100));

        assertThat(inbox.await(7, key(1), 3, at(100), Duration.ZERO)).isTrue();
    }

    @Test
    void theKEPTDiscardsAreBounded() throws Exception {
        ExposedInbox inbox = new ExposedInbox();
        for (long seq = 0; seq <= ExposedInbox.CAPACITY; seq++) {
            inbox.discarded(7, key(seq));
        }
        inbox.deliver(7, exposed(3, 0));
        inbox.deliver(7, exposed(3, 1));

        assertThat(inbox.await(7, key(0), 3, at(0), Duration.ZERO)).as("the oldest dropped")
                .isTrue();
        assertThat(inbox.await(7, key(ExposedInbox.CAPACITY), 3, at(1), Duration.ZERO))
                .isFalse();
    }
}
