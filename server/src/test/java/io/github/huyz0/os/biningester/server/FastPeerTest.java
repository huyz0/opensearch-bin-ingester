// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.Lease;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.SequencerTransport;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * What the watch asks: whether this pod leads a SERVING term, and what its
 * journal holds (M13.27n review round 1, P1, T1, T3).
 */
class FastPeerTest {

    /** A clock a test moves. */
    static final class MovingClock extends Clock {
        final AtomicLong millis = new AtomicLong(1_700_000_000_000L);

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis.get());
        }

        @Override
        public long millis() {
            return millis.get();
        }
    }

    private static ServerConfig config() {
        return new ServerConfig("pod1", "az-a", "cluster-a", "bins/cluster-a",
                new StoreConfig("memory", Optional.empty()),
                Duration.ofSeconds(10), Duration.ofSeconds(9), "http://pod1:8080",
                IngestConfig.defaults("cluster-a"), 0, "producer-1", java.util.Set.of("logs"),
                RetentionConfig.defaults(), Optional.empty(), "uid-pod1",
                io.github.huyz0.os.biningester.ingest.CostTopKReporter.DEFAULT_INTERVAL,
                io.github.huyz0.os.biningester.ingest.IndexQuotas.Config.none(), false,
                Optional.empty());
    }

    private static SequencerTransport noPeers() {
        return new SequencerTransport() {
            @Override
            public io.github.huyz0.os.biningester.format.CommitDelta send(String endpoint,
                    CommitRequest request) {
                throw new UnsupportedOperationException("no peer expected: " + endpoint);
            }

            @Override
            public void close() {
            }
        };
    }

    @Test
    void aPODLeadsOnlyWhileItsTermServes() throws Exception {
        MovingClock clock = new MovingClock();
        try (Assembly assembly = Assembly.open(config(), new MemoryBinStore(), noPeers(),
                clock)) {
            assertThat(FastPeer.leading(assembly)).as("its own term, unexpired").isTrue();

            clock.millis.addAndGet(Duration.ofSeconds(11).toMillis());

            assertThat(assembly.heldTerm()).as("still held: no commit has thrown").isNotNull();
            assertThat(FastPeer.leading(assembly))
                    .as("its lease expired: it reads the lease again").isFalse();
        }
    }

    @Test
    void aJOINReportsWhatTheJournalHolds() throws Exception {
        FastDiskTest.Files files = new FastDiskTest.Files();
        RunKey stream = new RunKey(new UUID(1, 1), 0);
        files.open("/var/fast", FastDisk.JOURNAL).append(new FastJournalRecord.Entry(1, stream,
                10, 2, 0, new FastJournalRecord.IdempotencyKey("pod1", new UUID(2, 2), 1),
                List.of(new SegmentRecord("d", OpType.INDEX, OptionalLong.empty(),
                        new byte[] {1}))).encode());
        MemoryBinStore store = new MemoryBinStore();
        store.put("p/ctl/lease/0.json", Body.ofBytes(new Lease(1, "pod1", "uid-pod1",
                "http://pod1:1", 1_000L).encode()));

        try (FastDisk disk = FastDisk.open(store, "p/ctl/lease/0.json",
                Optional.of(new FastJournalConfig("/var/fast", 1L << 20)), files)) {
            assertThat(FastPeer.held(disk).streams()).extracting(FastFrame.HeldStream::stream)
                    .containsExactly(stream);
        }
        try (FastDisk diskless = FastDisk.open(store, "p/ctl/lease/0.json", Optional.empty(),
                files)) {
            assertThat(FastPeer.held(diskless)).isEqualTo(FastFrame.Held.NONE);
        }
    }
}
