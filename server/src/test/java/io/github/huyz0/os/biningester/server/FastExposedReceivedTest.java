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

    /** On pod1, which leads term 3 and no other. */
    private static FastFrameRouter.Handler leadingThree(FastDisk disk) {
        Map<Integer, FastFrameRouter.Handler> handlers = new HashMap<>();
        FastHolderFrames.register(handlers::put, disk, epoch -> epoch == 3, "uid-pod1");
        return handlers.get(FastWriteFrame.KIND_EXPOSED);
    }

    @Test
    void anEXPOSEDTheLeaderSentItselfReachesItsInbox() throws Exception {
        // ⚠️ M13.84: the leader writes through its own term, joined by no one.
        try (FastDisk disk = FastDisk.open(new MemoryBinStore(), "lease", Optional.empty(),
                new FastDiskTest.Files())) {
            FastFrame.Body answer = leadingThree(disk).answer(from("uid-pod1"),
                    new FastWriteFrame.Exposed(4, RUNS));

            assertThat(answer)
                    .isEqualTo(new FastFrame.HeldStatusReport(FastFrame.HeldStatus.NONE));
            assertThat(disk.exposed().await(3, 4, RUNS, Duration.ZERO)).isTrue();
        }
    }

    @Test
    void anEXPOSEDFromAnotherPodForTheTermThisPodLeadsIsRefused() throws Exception {
        // ⚠️ its review round 1, T1: only this pod's own word is admitted.
        try (FastDisk disk = FastDisk.open(new MemoryBinStore(), "lease", Optional.empty(),
                new FastDiskTest.Files())) {
            FastFrame.Body answer = leadingThree(disk).answer(from("uid-pod2"),
                    new FastWriteFrame.Exposed(4, RUNS));

            assertThat(answer).isInstanceOf(FastFrame.Refused.class);
            assertThat(disk.exposed().await(3, 4, RUNS, Duration.ZERO)).isFalse();
        }
    }

    @Test
    void aPODLeadingNoTermAdmitsNoEXPOSEDOfItsOwn() throws Exception {
        // ⚠️ its review round 1, P1: such a pod reports its own term as 0.
        try (FastDisk disk = FastDisk.open(new MemoryBinStore(), "lease", Optional.empty(),
                new FastDiskTest.Files())) {
            Map<Integer, FastFrameRouter.Handler> handlers = new HashMap<>();
            FastHolderFrames.register(handlers::put, disk, epoch -> epoch == 0, "uid-pod1");

            FastFrame.Body answer = handlers.get(FastWriteFrame.KIND_EXPOSED).answer(
                    new FastFrame.Header(FastWriteFrame.KIND_EXPOSED, 0, "uid-pod1", "uid-pod1"),
                    new FastWriteFrame.Exposed(4, RUNS));

            assertThat(answer).isInstanceOf(FastFrame.Refused.class);
        }
    }

    @Test
    void anEXPOSEDInThisPodsNameForATermItDoesNotLeadIsRefused() throws Exception {
        try (FastDisk disk = FastDisk.open(new MemoryBinStore(), "lease", Optional.empty(),
                new FastDiskTest.Files())) {
            FastFrame.Body answer = leadingThree(disk).answer(
                    new FastFrame.Header(FastWriteFrame.KIND_EXPOSED, 2, "uid-pod1", "uid-pod1"),
                    new FastWriteFrame.Exposed(4, RUNS));

            assertThat(answer).isInstanceOf(FastFrame.Refused.class);
            assertThat(disk.exposed().await(2, 4, RUNS, Duration.ZERO)).isFalse();
        }
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
