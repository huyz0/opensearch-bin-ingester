// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastWriteFrame;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.sequencer.FastFrameRouter;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A node receives an EXPOSED into its pod's inbox (M13.29a): only from the
 * leader of a term it joined -- a reused name or address never acks a
 * producer on another's word.
 */
class FastExposedReceivedTest {

    private static final List<FastWriteFrame.RunAt> RUNS =
            List.of(new FastWriteFrame.RunAt(new RunKey(new UUID(1, 1), 0), 100));

    private static FastFrameRouter.Handler registered(FastDisk disk) {
        Map<Integer, FastFrameRouter.Handler> handlers = new HashMap<>();
        FastHolderFrames.register(handlers::put, disk);
        return handlers.get(FastWriteFrame.KIND_EXPOSED);
    }

    private static FastFrame.Header from(String sender) {
        return new FastFrame.Header(FastWriteFrame.KIND_EXPOSED, 3, sender, "uid-pod1");
    }

    @Test
    void anEXPOSEDFromItsTermsLeaderReachesTheInbox() throws Exception {
        try (FastDisk disk = FastDisk.open(new MemoryBinStore(), "lease", Optional.empty(),
                new FastDiskTest.Files())) {
            disk.joined().joined(3, 0, "uid-l");

            FastFrame.Body answer = registered(disk).answer(from("uid-l"),
                    new FastWriteFrame.Exposed(4, RUNS));

            assertThat(answer)
                    .isEqualTo(new FastFrame.HeldStatusReport(FastFrame.HeldStatus.NONE));
            assertThat(disk.exposed().await(3, 4, RUNS, Duration.ZERO)).isTrue();
        }
    }

    @Test
    void anEXPOSEDFromAnyoneElseIsRefusedAndAcksNothing() throws Exception {
        try (FastDisk disk = FastDisk.open(new MemoryBinStore(), "lease", Optional.empty(),
                new FastDiskTest.Files())) {
            disk.joined().joined(3, 0, "uid-l");

            FastFrame.Body answer = registered(disk).answer(from("uid-x"),
                    new FastWriteFrame.Exposed(4, RUNS));

            assertThat(((FastFrame.Refused) answer).reason())
                    .isEqualTo(FastFrame.Reason.NOT_ROSTERED);
            assertThat(disk.exposed().await(3, 4, RUNS, Duration.ZERO)).isFalse();
        }
    }
}
