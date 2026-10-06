// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.Lease;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
import io.github.huyz0.os.biningester.sequencer.FastFrameRouter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * What a node's JOIN carries, and where its router counts a DEPART's answer
 * (M13.27n review round 2, T7, T11).
 */
class FastPeerJoinerTest {

    private static final String LEASE = "p/ctl/lease/0.json";

    private static ServerConfig config(String az) {
        return new ServerConfig("pod1", az, "cluster-a", "bins/cluster-a",
                new StoreConfig("memory", Optional.empty()),
                Duration.ofSeconds(10), Duration.ofSeconds(3), "http://pod1:8080",
                IngestConfig.defaults("cluster-a"), 0, "producer-1", java.util.Set.of("logs"),
                RetentionConfig.defaults(), Optional.empty(), "uid-pod1",
                io.github.huyz0.os.biningester.ingest.CostTopKReporter.DEFAULT_INTERVAL,
                io.github.huyz0.os.biningester.ingest.IndexQuotas.Config.none(), false,
                Optional.empty());
    }

    private static MemoryBinStore leaseAt1() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        store.put(LEASE, Body.ofBytes(new Lease(1, "l", "uid-l", "http://l:1", 1_000L).encode()));
        return store;
    }

    @Test
    void aJOINCarriesWhatTheJournalHolds() throws Exception {
        FastDiskTest.Files files = new FastDiskTest.Files();
        RunKey stream = new RunKey(new UUID(1, 1), 0);
        files.open("/var/fast", FastDisk.JOURNAL).append(new FastJournalRecord.Entry(1, stream,
                10, 2, 0, new FastJournalRecord.IdempotencyKey("pod1", new UUID(2, 2), 1),
                List.of(new SegmentRecord("d", OpType.INDEX, OptionalLong.empty(),
                        new byte[] {1}))).encode());
        List<FastFrame.Join> sent = new ArrayList<>();
        try (FastDisk disk = FastDisk.open(leaseAt1(), LEASE,
                Optional.of(new FastJournalConfig("/var/fast", 1L << 20)), files)) {
            FastPeer.joiner(config("az-a"), disk, (endpoint, frame) -> {
                FastFrame.Frame asked = FastFrame.decode(frame);
                sent.add((FastFrame.Join) asked.body());
                return FastFrame.encode(asked.header().epoch(), asked.header().targetUid(),
                        asked.header().senderUid(),
                        new FastFrame.Joined(0, FastFrame.HeldStatus.NONE));
            }).join(1, "uid-l", "http://l:1");
        }

        assertThat(sent).singleElement().satisfies(join -> assertThat(join.held().streams())
                .extracting(FastFrame.HeldStream::stream).containsExactly(stream));
    }

    @Test
    void aDEPARTsAnswerIsCountedAgainstTheDepartingPodsZone() throws Exception {
        CrossAzBytes crossAz = new CrossAzBytes("az-a");
        try (FastDisk disk = FastDisk.open(leaseAt1(), LEASE, Optional.empty(),
                new FastDiskTest.Files())) {
            FastFrameRouter router = FastPeer.router(config("az-a"), disk, crossAz,
                    new PeerZones());
            router.handle(FastFrame.KIND_DEPART, (header, body) ->
                    new FastFrame.HeldStatusReport(FastFrame.HeldStatus.NONE));
            Roster.Incarnation pod = new Roster.Incarnation("p", "uid-p", "az-a", "http://p:1");

            router.answer(FastFrame.encode(1, pod.podUid(), "uid-pod1",
                    new FastFrame.Depart(pod, 2, FastFrame.Held.NONE)));
        }

        assertThat(crossAz.sameAzBytes(CrossAzBytes.Transport.FAST_CONTROL)).isPositive();
        assertThat(crossAz.crossAzBytes(CrossAzBytes.Transport.FAST_CONTROL)).isZero();
    }
}
