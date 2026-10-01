// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.Recovery;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Tier 2 reads a recovery slot and moves past it (M13.25a, review round 3 P1
 * and T5).
 *
 * <p>⚠️ IT READ EVERY SLOT THROUGH {@code CommitDelta.decode}, which refuses a
 * recovery: the poller took the refusal for an unavailable read and retried
 * the same slot at every interval, so tier 2 stopped for good at the first
 * recovery -- in exactly the case tier 2 is for, an ingester taken over.
 */
class TierTwoRecoverySlotTest {

    private static final RunKey A = new RunKey(new UUID(0, 1), 0);

    private static TierTwoChainPoller poller(Map<Long, byte[]> chain, List<CommitDelta> into) {
        return new TierTwoChainPoller(3, 0, new TierTwoChainPoller.DeltaReader() {
            @Override
            public Optional<CommitDelta> get(long epoch, long sequence) {
                throw new AssertionError("tier 2 reads slots, not deltas");
            }

            @Override
            public Optional<TierTwoChainPoller.Slot> slot(long epoch, long sequence)
                    throws IOException {
                byte[] bytes = chain.get(sequence);
                Optional<InputStream> body = bytes == null ? Optional.empty()
                        : Optional.of(new ByteArrayInputStream(bytes));
                return BinStorePlugin.decodeSlot(body);
            }
        }, () -> false, into::add);
    }

    @Test
    void aVOIDSOnlyRecoveryIsPassedAndTheNextDeltaIsRead() {
        CommitDelta next = new CommitDelta(2, "seg/2", List.of(new RunCommit(A, 1, 9)));
        List<CommitDelta> consumed = new ArrayList<>();
        TierTwoChainPoller poller = poller(Map.of(
                1L, new Recovery(1, List.of(), List.of(new Recovery.VoidRange(A, 0, 9)))
                        .encode(),
                2L, next.encode()), consumed);

        poller.poll(1);
        poller.poll(2);

        assertThat(consumed).as("slot 1 commits nothing; slot 2 is read after it")
                .containsExactly(next);
    }

    @Test
    void aRECOVERYsCommitsReachTheConsumerAsADelta() {
        SegmentCommit recovered = new SegmentCommit("seg/1", List.of(new RunCommit(A, 4, 0)));
        List<CommitDelta> consumed = new ArrayList<>();
        TierTwoChainPoller poller = poller(Map.of(1L, new Recovery(1, List.of(recovered),
                List.of(new Recovery.VoidRange(A, 4, 9))).encode()), consumed);

        poller.poll(1);

        assertThat(consumed).as("the recovered run is a commit like a delta's")
                .containsExactly(new CommitDelta(1, List.of(recovered)));
    }
}
