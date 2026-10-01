// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.Recovery;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.Seal;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The plugin's own tier-2 chain reader, as {@code enableTierTwo} wires it,
 * moves past a recovery slot (M13.25a review round 1, T1).
 *
 * <p>⚠️ A READER THAT ANSWERS ONLY {@code get} compiles and stalls: the
 * poller's {@code slot} default reads deltas only, so this drives the
 * production reader, not one built for the test.
 */
class TierTwoStoreChainReaderTest {

    private static final RunKey A = new RunKey(new UUID(0, 1), 0);

    private static TierTwoChainPoller poller(Map<String, byte[]> objects,
            List<CommitDelta> consumed) {
        return new TierTwoChainPoller(3, 0,
                BinStorePlugin.chainReader((bucket, prefix, key) -> {
                    byte[] bytes = objects.get(key);
                    return bytes == null ? Optional.<InputStream>empty()
                            : Optional.of(new ByteArrayInputStream(bytes));
                }, "b", "p"), () -> false, consumed::add);
    }

    @Test
    void theWIREDReaderStopsAtTheEpochsSeal() {
        List<CommitDelta> consumed = new ArrayList<>();
        TierTwoChainPoller poller = poller(Map.of(
                "p/ctl/log/0/0000000000000003/0000000000000001.delta", new Seal(1, 4).encode(),
                "p/ctl/log/0/0000000000000003/0000000000000002.delta",
                new CommitDelta(2, "seg/2", List.of(new RunCommit(A, 1, 0))).encode()),
                consumed);

        poller.poll(1);
        poller.poll(2);

        assertThat(consumed).as("past a seal is another epoch's chain, not this one's next slot")
                .isEmpty();
    }

    @Test
    void theWIREDReaderPassesAVoidsOnlyRecoveryAndReadsTheNextDelta() {
        CommitDelta next = new CommitDelta(2, "seg/2", List.of(new RunCommit(A, 1, 9)));
        Map<String, byte[]> objects = Map.of(
                "p/ctl/log/0/0000000000000003/0000000000000001.delta",
                new Recovery(1, List.of(), List.of(new Recovery.VoidRange(A, 0, 9))).encode(),
                "p/ctl/log/0/0000000000000003/0000000000000002.delta", next.encode());
        List<CommitDelta> consumed = new ArrayList<>();
        TierTwoChainPoller poller = poller(objects, consumed);

        poller.poll(1);
        poller.poll(2);

        assertThat(consumed).containsExactly(next);
    }
}
