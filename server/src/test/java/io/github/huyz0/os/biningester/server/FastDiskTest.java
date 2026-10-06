// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryJournalFile;
import io.github.huyz0.os.biningester.format.FastEpochFile;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.Lease;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A pod's epoch fence and fast journal at startup (ADR-0081 §3, ADR-0082 §4,
 * ADR-0083; M13.27i): the fence always, from the lease and the epoch file when
 * the pod keeps one; the journal only where a directory is configured.
 */
class FastDiskTest {

    private static final String LEASE_KEY = "p/ctl/lease/0.json";

    /** Opens in-memory files by name, recording each open. */
    static final class Files implements FastDisk.Opener {
        final Map<String, MemoryJournalFile> byName = new HashMap<>();
        final List<String> opened = new ArrayList<>();

        @Override
        public MemoryJournalFile open(String directory, String name) {
            opened.add(directory + "/" + name);
            return byName.computeIfAbsent(directory + "/" + name, n -> new MemoryJournalFile());
        }
    }

    private static MemoryBinStore leaseAt(long epoch) throws IOException {
        MemoryBinStore store = new MemoryBinStore();
        store.put(LEASE_KEY, Body.ofBytes(new Lease(epoch, "pod1", "uid-pod1", "http://pod1:1",
                1_000L).encode()));
        return store;
    }

    @Test
    void aDISKLESSPodFencesAtTheLeaseAndOpensNoFile() throws Exception {
        Files files = new Files();

        try (FastDisk disk = FastDisk.open(leaseAt(4), LEASE_KEY, Optional.empty(), files)) {
            assertThat(disk.fence().highest()).isEqualTo(4);
            assertThat(disk.journal()).isEmpty();
        }
        assertThat(files.opened).isEmpty();
    }

    @Test
    void aJOURNALEDPodKeepsItsEpochFileAndRecoversItsJournal() throws Exception {
        Files files = new Files();
        FastJournalRecord.Entry held = new FastJournalRecord.Entry(3,
                new RunKey(new UUID(1, 1), 0), 10, 2, 1,
                new FastJournalRecord.IdempotencyKey("pod1", new UUID(2, 2), 1),
                List.of(new SegmentRecord("d10", OpType.INDEX, OptionalLong.empty(),
                        new byte[] {1})));
        files.open("/var/fast", "journal").append(held.encode());

        try (FastDisk disk = FastDisk.open(leaseAt(4), LEASE_KEY,
                Optional.of(new FastJournalConfig("/var/fast", 1L << 20)), files)) {
            assertThat(disk.journal().orElseThrow().held()).extracting(e -> e.encode())
                    .containsExactly(held.encode());
            assertThat(FastEpochFile.decode(files.byName.get("/var/fast/epoch").readAll()))
                    .as("written before it is relied on").isEqualTo(4);
        }
    }

    @Test
    void anEPOCHFileAboveTheLeaseRaisesTheFence() throws Exception {
        Files files = new Files();
        files.open("/var/fast", "epoch").replace(FastEpochFile.encode(9));

        try (FastDisk disk = FastDisk.open(leaseAt(4), LEASE_KEY,
                Optional.of(new FastJournalConfig("/var/fast", 1L << 20)), files)) {
            assertThat(disk.fence().highest()).isEqualTo(9);
        }
    }

    @Test
    void anUNDECODABLEEpochFileKeepsThePodFromStarting() throws Exception {
        Files files = new Files();
        files.open("/var/fast", "epoch").replace(new byte[] {1, 2, 3});

        assertThatThrownBy(() -> FastDisk.open(leaseAt(4), LEASE_KEY,
                Optional.of(new FastJournalConfig("/var/fast", 1L << 20)), files))
                .isInstanceOf(IOException.class);
    }

    @Test
    void theJOURNALIsRecoveredUnderTheConfiguredCap() throws Exception {
        Files files = new Files();

        try (FastDisk disk = FastDisk.open(leaseAt(1), LEASE_KEY,
                Optional.of(new FastJournalConfig("/var/fast", 100)), files)) {
            assertThat(disk.journal().orElseThrow().fits(100)).isTrue();
            assertThat(disk.journal().orElseThrow().fits(101)).isFalse();
        }
    }
}
