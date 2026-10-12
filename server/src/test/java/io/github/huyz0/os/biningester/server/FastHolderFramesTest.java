// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.FastWriteFrame;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.sequencer.FastFrameRouter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A node answers a REPLICA through its fast disk (M13.28a): its journal,
 * the terms its watch joined and its holder gate -- the same ones the
 * departure takes; refused {@code NOT_FAST} without a journal.
 */
class FastHolderFramesTest {

    private static final RunKey A = new RunKey(new UUID(1, 1), 0);

    private static FastWriteFrame.Replica replica() {
        return new FastWriteFrame.Replica(List.of(new FastJournalRecord.Entry(3, A, 10, 2, 4,
                new FastJournalRecord.IdempotencyKey("w", new UUID(9, 9), 1),
                List.of(new SegmentRecord("d", OpType.INDEX, OptionalLong.empty(),
                        new byte[] {1})))));
    }

    private static FastFrameRouter.Handler registered(FastDisk disk) {
        Map<Integer, FastFrameRouter.Handler> handlers = new HashMap<>();
        FastHolderFrames.register(handlers::put, disk);
        return handlers.get(FastWriteFrame.KIND_REPLICA);
    }

    private static FastFrame.Header fromLeader() {
        return new FastFrame.Header(FastWriteFrame.KIND_REPLICA, 3, "uid-l", "uid-pod1");
    }

    @Test
    void aPODWithoutAFastJournalRefusesNotFast() throws Exception {
        try (FastDisk disk = FastDisk.open(new MemoryBinStore(), "lease", Optional.empty(),
                new FastDiskTest.Files())) {
            FastFrame.Body answer = registered(disk).answer(fromLeader(), replica());

            assertThat(((FastFrame.Refused) answer).reason()).isEqualTo(FastFrame.Reason.NOT_FAST);
        }
    }

    @Test
    void aJOINEDTermsReplicaIsHeldInTheDisksJournal() throws Exception {
        try (FastDisk disk = FastDisk.open(new MemoryBinStore(), "lease",
                Optional.of(new FastJournalConfig("/var/fast", 1L << 20)),
                new FastDiskTest.Files())) {
            disk.joined().joined(3, 0, "uid-l");

            FastFrame.Body answer = registered(disk).answer(fromLeader(), replica());

            assertThat(answer).isInstanceOf(FastWriteFrame.ReplicaAck.class);
            assertThat(disk.journal().orElseThrow().held()).hasSize(1);
        }
    }

    @Test
    void aDEPARTINGPodsGateRefusesTheReplica() throws Exception {
        try (FastDisk disk = FastDisk.open(new MemoryBinStore(), "lease",
                Optional.of(new FastJournalConfig("/var/fast", 1L << 20)),
                new FastDiskTest.Files())) {
            disk.joined().joined(3, 0, "uid-l");
            disk.gate().depart();

            FastFrame.Body answer = registered(disk).answer(fromLeader(), replica());

            assertThat(((FastFrame.Refused) answer).reason())
                    .isEqualTo(FastFrame.Reason.DEPARTING);
        }
    }
}
