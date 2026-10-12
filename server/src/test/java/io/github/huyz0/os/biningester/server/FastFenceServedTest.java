// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastTakeoverFrame;
import io.github.huyz0.os.biningester.http.FastFrameService;
import io.github.huyz0.os.biningester.http.HttpFastTransport;
import io.github.huyz0.os.biningester.sequencer.FastFrameRouter;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Every node answers a successor's FENCE (M13.33b): from its journal, in
 * pages of one peer frame; a diskless pod holds nothing, so answers one empty
 * page marked last.
 */
class FastFenceServedTest {

    @TempDir
    Path dir;

    private IngesterNode node;

    @AfterEach
    void stop() throws Exception {
        if (node != null) {
            node.close();
        }
    }

    private static FastFrameRouter.Handler registered(FastDisk disk) {
        Map<Integer, FastFrameRouter.Handler> handlers = new HashMap<>();
        FastHolderFrames.register(handlers::put, disk);
        return handlers.get(FastTakeoverFrame.KIND_FENCE);
    }

    @Test
    void aDISKLESSPodAnswersOneEmptyPageMarkedLast() throws Exception {
        try (FastDisk disk = FastDisk.open(new MemoryBinStore(), "lease", Optional.empty(),
                new FastDiskTest.Files())) {
            FastFrame.Body answer = registered(disk).answer(
                    new FastFrame.Header(FastTakeoverFrame.KIND_FENCE, 9, "uid-s", "uid-m"),
                    new FastTakeoverFrame.Fence(List.of(7L), 6, 2));

            assertThat(answer).isEqualTo(new FastTakeoverFrame.Collected(2, true, List.of()));
        }
    }

    @Test
    void aJOURNALEDPodAnswersWhatItHoldsOfTheFencedTerms() throws Exception {
        MemoryBinStore store = FastDepartureTest.leaseHeldBy("uid-l");
        try (FastDisk disk = FastDepartureTest.journaledDisk(store)) {
            // the disk holds one entry of term 1
            FastFrame.Body answer = registered(disk).answer(
                    new FastFrame.Header(FastTakeoverFrame.KIND_FENCE, 2, "uid-s", "uid-m"),
                    new FastTakeoverFrame.Fence(List.of(1L), 0));

            assertThat(((FastTakeoverFrame.Collected) answer).entries()).hasSize(1);
        }
    }

    @Test
    void aPAGEIsOnePeerFrameLessItsFraming() {
        assertThat(FastHolderFrames.COLLECTED_PAGE_BYTES)
                .isEqualTo(FastFrameService.MAX_FRAME_BYTES - (1L << 20));
    }

    @Test
    void aNODERoutesAFenceToItsJournal() throws Exception {
        Map<String, String> settings = new HashMap<>();
        settings.put(ServerProperties.POD_ID, "pod1");
        settings.put(ServerProperties.POD_UID, "uid-pod1");
        settings.put(ServerProperties.PEER_TLS, "off");
        settings.put(ServerProperties.PEER_PORT, "0");
        settings.put(ServerProperties.POD_AZ, "az-a");
        settings.put(ServerProperties.TRUST_DOMAIN, "cluster-a");
        settings.put(ServerProperties.PREFIX, "bins/cluster-a");
        settings.put(ServerProperties.STORE_KIND, "memory");
        settings.put(ServerProperties.ENDPOINT, "http://localhost:0");
        settings.put(ServerProperties.HTTP_PORT, "0");
        settings.put(ServerProperties.PRODUCER_SUBJECT, "producer-1");
        settings.put(ServerProperties.PRODUCER_ALLOWED_INDICES, "logs");
        settings.put(ServerProperties.FAST_JOURNAL_DIR, dir.resolve("journal").toString());
        node = IngesterNode.start(ServerProperties.parse(settings), Clock.systemUTC());

        FastFrame.Body answer = FastFrame.decode(new HttpFastTransport(Duration.ofSeconds(5),
                CrossAzBytes.untracked()).exchange("http://localhost:" + node.peerPort(),
                FastFrame.encode(9, "uid-s", "uid-pod1",
                        new FastTakeoverFrame.Fence(List.of(7L), 6)))).body();

        // ⚠️ REACHED THE MEMBER: an unrouted kind gets a 501 and no answer.
        assertThat(answer).isEqualTo(new FastTakeoverFrame.Collected(0, true, List.of()));
    }
}
