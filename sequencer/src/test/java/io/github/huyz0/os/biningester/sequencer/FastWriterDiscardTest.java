// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastWriteFrame;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A writer told its batch was discarded retries at once (M13.34t; ADR-0081
 * §2.3: "retry at once rather than after a timeout"), never after its
 * EXPOSED wait -- here a minute.
 */
class FastWriterDiscardTest {

    @Test
    @Timeout(value = 20, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void aDISCARDEDBatchIsRetriedAtOnceNotAfterItsWait() throws Exception {
        FastWriteFrame.Commit commit = FastWriterTest.commit("w");
        TermJoiner.Transport leader = (endpoint, frame) -> FastFrame.encode(7, "uid-l",
                FastWriterTest.SELF.podUid(), new FastWriteFrame.Assigned(3, List.of(
                        new FastWriteFrame.AssignedRun(FastWriterTest.S, 100, 2, false))));
        ExposedInbox inbox = new ExposedInbox();
        FastWriter writer = new FastWriter(FastWriterTest.SELF, leader,
                FastWriterTest.fenceAt(7), Optional.empty(), inbox, Duration.ofSeconds(60));
        List<FastWriteFrame.RunAt> runs = List.of(new FastWriteFrame.RunAt(FastWriterTest.S, 100));
        CompletableFuture<Void> told = CompletableFuture.runAsync(() -> {
            while (!inbox.waiting(7, 3, runs)) {
                Thread.onSpinWait();
            }
            inbox.discarded(7, commit.key());
        });

        FastWriter.Outcome outcome = writer.write(7, "uid-l", "http://l:1", commit);

        told.get();
        assertThat(outcome).as("a retry, within the test's 20 s, not the wait's minute")
                .isInstanceOf(FastWriter.Retry.class);
    }
}
