// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.ChainEntry;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.Continue;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.Seal;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class ChainAuditTest {

    private static final String ROOT = ChaosBucket.PREFIX + "/ctl/log/0/";
    private static final RunKey STREAM = new RunKey(
            UUID.fromString("00000000-0000-0000-0000-000000000001"), 0);

    @Test
    void anOpenedEmptySkippedEpochAndDroppedHistoryBothAppearInTheExactAuditResult()
            throws Exception {
        Map<String, byte[]> objects = new HashMap<>();
        put(objects, 0, 0, delta(0, "dropped"));
        put(objects, 1, 0, delta(0, "kept"));
        put(objects, 1, 1, new Seal(1, 2));
        put(objects, 2, 0, new Continue(0, 1, 1));
        put(objects, 3, 0, new Continue(0, 1, 1));

        ChainAudit audit = ChainAudit.of(new FakeChainReader(objects));

        assertThat(audit.violations()).containsExactly(
                "link: epoch 3 continues from 1@1 but that chain's first SEAL is "
                        + "Seal[sequence=1, continuedAt=2]",
                "history: epoch 0 holds deltas no later epoch continues from");
    }

    private static CommitDelta delta(long sequence, String segment) {
        return new CommitDelta(sequence, List.of(new SegmentCommit(segment,
                List.of(new RunCommit(STREAM, 1, 0)))));
    }

    private static void put(Map<String, byte[]> objects, long epoch, long sequence,
            ChainEntry entry) {
        objects.put(ROOT + "%016x/%016x.delta".formatted(epoch, sequence), entry.encode());
    }

    private record FakeChainReader(Map<String, byte[]> objects)
            implements ChainAudit.ChainBucketReader {

        @Override
        public List<String> keys(String prefix) {
            return objects.keySet().stream().filter(key -> key.startsWith(prefix)).sorted().toList();
        }

        @Override
        public byte[] get(String key) {
            return objects.get(key);
        }
    }
}
