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
import io.github.huyz0.os.biningester.sequencer.HeldStatusAnswers;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * A pod's departure at a graceful stop (ADR-0081 §9; M13.27p): a follower
 * reports what it holds and, once nothing is pending, asks to be marked
 * departed; a leader departs nothing here; a pending entry or an unreachable
 * leader keeps it until the grace ends, and it stops undeparted.
 */
class FastDepartureTest {

    static final String LEASE = "p/ctl/lease/0.json";
    static final RunKey STREAM = new RunKey(new UUID(1, 1), 0);
    private static final Duration GRACE = Duration.ofSeconds(10);

    static ServerConfig config() {
        return new ServerConfig("pod2", "az-b", "cluster-a", "bins/cluster-a",
                new StoreConfig("memory", Optional.empty()),
                Duration.ofSeconds(10), Duration.ofSeconds(3), "http://pod2:8080",
                IngestConfig.defaults("cluster-a"), 0, "producer-1", java.util.Set.of("logs"),
                RetentionConfig.defaults(), Optional.empty(), "uid-pod2",
                io.github.huyz0.os.biningester.ingest.CostTopKReporter.DEFAULT_INTERVAL,
                io.github.huyz0.os.biningester.ingest.IndexQuotas.Config.none(), false,
                Optional.empty(), PeerConfig.off(0));
    }

    static MemoryBinStore leaseHeldBy(String uid) throws IOException {
        MemoryBinStore store = new MemoryBinStore();
        store.put(LEASE, Body.ofBytes(new Lease(1, "l", uid, "http://l:1", 1_000L).encode()));
        return store;
    }

    static Lease read(MemoryBinStore store) throws IOException {
        try (var in = store.get(LEASE)) {
            return Lease.decode(in.readAllBytes());
        }
    }

    /** A leader answering every DEPART and HELD with {@code pending} or nothing. */
    static final class Leader implements io.github.huyz0.os.biningester.sequencer.TermJoiner
            .Transport {
        final List<String> asked = new ArrayList<>();
        boolean pending;
        boolean down;

        @Override
        public byte[] exchange(String endpoint, byte[] frame) throws IOException {
            FastFrame.Frame f = FastFrame.decode(frame);
            asked.add(switch (f.body()) {
                case FastFrame.Depart d -> "DEPART" + d.phase();
                case FastFrame.HeldReport h -> "HELD";
                default -> "?";
            });
            if (down) {
                throw new IOException("unreachable");
            }
            FastFrame.Held held = switch (f.body()) {
                case FastFrame.Depart d -> d.held();
                case FastFrame.HeldReport h -> h.held();
                default -> FastFrame.Held.NONE;
            };
            FastFrame.HeldStatus status = pending
                    ? HeldStatusAnswers.answer(held, Map.of(STREAM, 0L), List.of(), 0)
                    : HeldStatusAnswers.answer(held, Map.of(STREAM, 1_000L), List.of(), 0);
            return FastFrame.encode(f.header().epoch(), f.header().targetUid(),
                    f.header().senderUid(), new FastFrame.HeldStatusReport(status));
        }
    }

    /** Moves the monotonic clock by each pause. */
    static final class Time {
        final AtomicLong nanos = new AtomicLong(1_000_000_000L);
    }

    static FastDisk journaledDisk(MemoryBinStore store) throws Exception {
        FastDiskTest.Files files = new FastDiskTest.Files();
        files.open("/var/fast", FastDisk.JOURNAL).append(new FastJournalRecord.Entry(1, STREAM,
                10, 2, 0, new FastJournalRecord.IdempotencyKey("pod2", new UUID(2, 2), 1),
                List.of(new SegmentRecord("d", OpType.INDEX, OptionalLong.empty(),
                        new byte[] {1}))).encode());
        return FastDisk.open(store, LEASE,
                Optional.of(new FastJournalConfig("/var/fast", 1L << 20)), files);
    }

    private static FastDeparture.Result depart(MemoryBinStore store, FastDisk disk,
            Leader leader, boolean leading, Time time) {
        return FastDeparture.depart(config(), () -> read(store), disk, leader, leading, GRACE,
                time.nanos::get, interval -> time.nanos.addAndGet(interval.toNanos()));
    }

    @Test
    void aFOLLOWERReportsWhatItHoldsThenLeaves() throws Exception {
        MemoryBinStore store = leaseHeldBy("uid-l");
        Leader leader = new Leader();
        try (FastDisk disk = journaledDisk(store)) {
            assertThat(depart(store, disk, leader, false, new Time()))
                    .isEqualTo(FastDeparture.Result.DEPARTED);
        }

        assertThat(leader.asked).containsExactly("DEPART1", "DEPART2");
    }

    @Test
    void aDISKLESSFollowerLeavesWithNothingToReport() throws Exception {
        MemoryBinStore store = leaseHeldBy("uid-l");
        Leader leader = new Leader();
        try (FastDisk disk = FastDisk.open(store, LEASE, Optional.empty(),
                new FastDiskTest.Files())) {
            assertThat(depart(store, disk, leader, false, new Time()))
                    .isEqualTo(FastDeparture.Result.DEPARTED);
        }

        assertThat(leader.asked).containsExactly("DEPART1", "DEPART2");
    }

    @Test
    void aLEADERDepartsNothingHere() throws Exception {
        // ⚠️ A LEASE NAMING ANOTHER POD: the leading flag alone must stop it.
        MemoryBinStore store = leaseHeldBy("uid-l");
        Leader leader = new Leader();
        try (FastDisk disk = journaledDisk(store)) {
            assertThat(depart(store, disk, leader, true, new Time()))
                    .isEqualTo(FastDeparture.Result.NOT_A_FOLLOWER);
        }

        assertThat(leader.asked).isEmpty();
    }

    @Test
    void aPENDINGEntryKeepsItUntilTheGraceEndsAndItStopsUndeparted() throws Exception {
        MemoryBinStore store = leaseHeldBy("uid-l");
        Leader leader = new Leader();
        leader.pending = true;
        Time time = new Time();
        long started = time.nanos.get();
        try (FastDisk disk = journaledDisk(store)) {
            assertThat(depart(store, disk, leader, false, time))
                    .isEqualTo(FastDeparture.Result.STOPPED_UNDEPARTED);
        }

        assertThat(leader.asked).as("never asks to leave with a copy pending")
                .doesNotContain("DEPART2").contains("DEPART1", "HELD");
        assertThat(time.nanos.get() - started).as("bounded by the grace")
                .isLessThanOrEqualTo(GRACE.toNanos() + Duration.ofSeconds(1).toNanos());
    }

    @Test
    void anUNREACHABLELeaderIsAskedAgainUntilTheGraceEnds() throws Exception {
        MemoryBinStore store = leaseHeldBy("uid-l");
        Leader leader = new Leader();
        leader.down = true;
        try (FastDisk disk = FastDisk.open(store, LEASE, Optional.empty(),
                new FastDiskTest.Files())) {
            assertThat(depart(store, disk, leader, false, new Time()))
                    .isEqualTo(FastDeparture.Result.STOPPED_UNDEPARTED);
        }

        assertThat(leader.asked).hasSizeGreaterThan(1);
    }
}
