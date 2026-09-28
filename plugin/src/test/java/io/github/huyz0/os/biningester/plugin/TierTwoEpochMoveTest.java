// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * A delta read under one chain epoch is dropped when the cursor moves to a
 * NEWER epoch while it is read (M11.12 review T1) -- even when the new epoch's
 * sequence is exactly the one the read continued from, so the sequence half
 * of the re-check cannot tell the two apart.
 */
class TierTwoEpochMoveTest {

    @Test
    void aDeltaReadUnderAnEpochTheCursorLeftIsDropped() throws Exception {
        CountDownLatch reading = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        List<CommitDelta> consumed = new CopyOnWriteArrayList<>();
        RunKey key = new RunKey(UUID.fromString("00000000-0000-0000-0000-00000000000a"), 0);
        TierTwoChainPoller poller = new TierTwoChainPoller(3, 0, (epoch, sequence) -> {
            reading.countDown();
            try {
                release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return Optional.of(new CommitDelta(sequence, "seg/" + epoch + "/" + sequence,
                    List.of(new RunCommit(key, 1, 0))));
        }, () -> false, consumed::add);

        Thread poll = Thread.ofVirtual().start(() -> poller.poll(1));
        assertThat(reading.await(5, TimeUnit.SECONDS)).isTrue();
        poller.observeCursor(4, 0);
        release.countDown();
        poll.join(5_000);

        assertThat(consumed)
                .as("⚠️ EPOCH 3's DELTA 1 IS NOT EPOCH 4's: handed on, it would stand in for "
                        + "a delta never read")
                .isEmpty();
    }
}
