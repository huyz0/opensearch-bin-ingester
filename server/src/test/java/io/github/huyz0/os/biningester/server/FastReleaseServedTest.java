// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastTakeoverFrame;
import io.github.huyz0.os.biningester.sequencer.FastFrameRouter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Every node answers a RELEASE through its fast disk (M13.28b): its journal
 * and the terms its watch joined; a diskless pod holds nothing to release.
 */
class FastReleaseServedTest {

    private static FastFrameRouter.Handler registered(FastDisk disk) {
        Map<Integer, FastFrameRouter.Handler> handlers = new HashMap<>();
        FastHolderFrames.register(handlers::put, disk);
        return handlers.get(FastTakeoverFrame.KIND_RELEASE);
    }

    private static FastTakeoverFrame.Release releaseAll() {
        return new FastTakeoverFrame.Release(List.of(
                new FastTakeoverFrame.ReleaseAt(FastDepartureTest.STREAM, 1_000)));
    }

    @Test
    void aDISKLESSPodAcknowledgesHoldingNothing() throws Exception {
        try (FastDisk disk = FastDisk.open(new MemoryBinStore(), "lease", Optional.empty(),
                new FastDiskTest.Files())) {
            assertThat(registered(disk).answer(new FastFrame.Header(
                    FastTakeoverFrame.KIND_RELEASE, 1, "uid-l", "uid-pod1"), releaseAll()))
                    .isEqualTo(new FastFrame.HeldStatusReport(FastFrame.HeldStatus.NONE));
        }
    }

    @Test
    void aJOURNALEDPodReleasesForItsJoinedTermsLeader() throws Exception {
        MemoryBinStore store = FastDepartureTest.leaseHeldBy("uid-l");
        try (FastDisk disk = FastDepartureTest.journaledDisk(store)) {
            long term = FastDepartureTest.read(store).epoch();
            disk.joined().joined(term, 0, "uid-l");

            registered(disk).answer(new FastFrame.Header(FastTakeoverFrame.KIND_RELEASE, term,
                    "uid-l", "uid-pod1"), releaseAll());

            assertThat(disk.journal().orElseThrow().held()).isEmpty();
        }
    }

    @Test
    void aJOURNALEDPodRefusesARELEASEOfATermItNeverJoined() throws Exception {
        MemoryBinStore store = FastDepartureTest.leaseHeldBy("uid-l");
        try (FastDisk disk = FastDepartureTest.journaledDisk(store)) {
            FastFrame.Body answer = registered(disk).answer(new FastFrame.Header(
                    FastTakeoverFrame.KIND_RELEASE, 1, "uid-l", "uid-pod1"), releaseAll());

            assertThat(((FastFrame.Refused) answer).reason())
                    .isEqualTo(FastFrame.Reason.NOT_ROSTERED);
            assertThat(disk.journal().orElseThrow().held()).hasSize(1);
        }
    }
}
