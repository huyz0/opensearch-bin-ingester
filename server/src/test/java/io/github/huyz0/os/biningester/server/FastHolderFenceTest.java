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
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A node's holder re-checks the pod's own fence under its own gate (M13.33b
 * review round 1, P1): a REPLICA of a term the fence has passed is refused,
 * so none lands between a FENCE's pages.
 */
class FastHolderFenceTest {

    @Test
    void aREPLICAOfATermThePodsFenceHasPassedIsRefused() throws Exception {
        MemoryBinStore store = FastDepartureTest.leaseHeldBy("uid-l");
        try (FastDisk disk = FastDepartureTest.journaledDisk(store)) {
            long term = FastDepartureTest.read(store).epoch();
            disk.joined().joined(term, 0, "uid-l");
            disk.fence().raise(term + 1);
            Map<Integer, FastFrameRouter.Handler> handlers = new HashMap<>();
            FastHolderFrames.register(handlers::put, disk);
            int held = disk.journal().orElseThrow().held().size();

            FastFrame.Body answer = handlers.get(FastWriteFrame.KIND_REPLICA).answer(
                    new FastFrame.Header(FastWriteFrame.KIND_REPLICA, term, "uid-l", "uid-pod1"),
                    new FastWriteFrame.Replica(List.of(new FastJournalRecord.Entry(term,
                            new RunKey(new UUID(5, 5), 0), 0, 2, 0,
                            new FastJournalRecord.IdempotencyKey("w", new UUID(9, 9), 1),
                            List.of(new SegmentRecord("d", OpType.INDEX, OptionalLong.empty(),
                                    new byte[] {1}))))));

            assertThat(((FastFrame.Refused) answer).reason())
                    .isEqualTo(FastFrame.Reason.LOWER_EPOCH);
            assertThat(disk.journal().orElseThrow().held()).hasSize(held);
        }
    }
}
