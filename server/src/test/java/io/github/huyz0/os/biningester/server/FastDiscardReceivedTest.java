// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
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
 * A node receives a pushed DISCARDED into its pod's inbox (M13.34t; ADR-0081
 * §2.3): the writer's route for it, admitted exactly as an EXPOSED is -- only
 * from the leader of a term it joined, or from itself for the term it leads
 * -- so nobody else's word cuts a write's wait short.
 */
class FastDiscardReceivedTest {

    private static final List<FastWriteFrame.RunAt> RUNS =
            List.of(new FastWriteFrame.RunAt(new RunKey(new UUID(1, 1), 0), 100));
    private static final FastJournalRecord.IdempotencyKey KEY =
            new FastJournalRecord.IdempotencyKey("pod1", new UUID(9, 9), 1);
    private static final FastFrame.Refused DISCARDED = new FastFrame.Refused(
            FastFrame.Reason.DISCARDED, Optional.of(KEY), "discarded");

    /** On pod1, which leads term 3 and no other. */
    private static FastFrameRouter.Handler leadingThree(FastDisk disk) {
        Map<Integer, FastFrameRouter.Handler> handlers = new HashMap<>();
        FastHolderFrames.register(handlers::put, disk, epoch -> epoch == 3, "uid-pod1");
        return handlers.get(FastFrame.KIND_REFUSED);
    }

    private static FastFrame.Header from(long epoch, String sender) {
        return new FastFrame.Header(FastFrame.KIND_REFUSED, epoch, sender, "uid-pod1");
    }

    /** Whether the batch's wait ends unexposed at once: its discard was taken. */
    private static boolean cutShort(FastDisk disk, long epoch) throws Exception {
        disk.exposed().deliver(epoch, new FastWriteFrame.Exposed(4, RUNS));
        return !disk.exposed().await(epoch, KEY, 4, RUNS, Duration.ZERO);
    }

    @Test
    void aDISCARDFromAJoinedTermsLeaderReachesTheInbox() throws Exception {
        try (FastDisk disk = FastDisk.open(new MemoryBinStore(), "lease", Optional.empty(),
                new FastDiskTest.Files())) {
            disk.joined().joined(2, 0, "uid-l");

            FastFrame.Body answer = leadingThree(disk).answer(from(2, "uid-l"), DISCARDED);

            assertThat(answer)
                    .isEqualTo(new FastFrame.HeldStatusReport(FastFrame.HeldStatus.NONE));
            assertThat(cutShort(disk, 2)).isTrue();
        }
    }

    @Test
    void aDISCARDTheLeaderSentItselfReachesItsInbox() throws Exception {
        try (FastDisk disk = FastDisk.open(new MemoryBinStore(), "lease", Optional.empty(),
                new FastDiskTest.Files())) {
            leadingThree(disk).answer(from(3, "uid-pod1"), DISCARDED);

            assertThat(cutShort(disk, 3)).isTrue();
        }
    }

    @Test
    void aDISCARDFromAnyoneElseIsRefusedAndEndsNoWait() throws Exception {
        try (FastDisk disk = FastDisk.open(new MemoryBinStore(), "lease", Optional.empty(),
                new FastDiskTest.Files())) {
            disk.joined().joined(2, 0, "uid-l");

            FastFrame.Body answer = leadingThree(disk).answer(from(2, "uid-x"), DISCARDED);

            assertThat(((FastFrame.Refused) answer).reason())
                    .isEqualTo(FastFrame.Reason.NOT_ROSTERED);
            assertThat(cutShort(disk, 2)).isFalse();
        }
    }

    @Test
    void aPUSHEDRefusalOfAnotherReasonIsRefused() throws Exception {
        try (FastDisk disk = FastDisk.open(new MemoryBinStore(), "lease", Optional.empty(),
                new FastDiskTest.Files())) {
            disk.joined().joined(2, 0, "uid-l");

            FastFrame.Body answer = leadingThree(disk).answer(from(2, "uid-l"),
                    new FastFrame.Refused(FastFrame.Reason.BACKPRESSURE, Optional.empty(), "x"));

            assertThat(answer).isInstanceOf(FastFrame.Refused.class);
        }
    }
}
