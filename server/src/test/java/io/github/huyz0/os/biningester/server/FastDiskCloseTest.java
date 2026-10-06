// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.JournalFile;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryJournalFile;
import io.github.huyz0.os.biningester.format.Lease;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * The files a fast disk opens are closed with it, and on a failed start
 * (M13.27i review round 1, T2).
 */
class FastDiskCloseTest {

    private static final String LEASE_KEY = "p/ctl/lease/0.json";
    private static final Optional<FastJournalConfig> JOURNALED =
            Optional.of(new FastJournalConfig("/var/fast", 1L << 20));

    /** An in-memory file that records being closed. */
    static final class Tracked implements JournalFile {
        final MemoryJournalFile inner = new MemoryJournalFile();
        boolean closed;

        @Override
        public byte[] readAll() {
            return inner.readAll();
        }

        @Override
        public void append(byte[] bytes) {
            inner.append(bytes);
        }

        @Override
        public void force() {
            inner.force();
        }

        @Override
        public void truncate(long length) {
            inner.truncate(length);
        }

        @Override
        public void replace(byte[] contents) {
            inner.replace(contents);
        }

        @Override
        public long size() {
            return inner.size();
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private static MemoryBinStore lease() throws IOException {
        MemoryBinStore store = new MemoryBinStore();
        store.put(LEASE_KEY, Body.ofBytes(new Lease(1, "pod1", "uid-pod1", "http://pod1:1",
                1_000L).encode()));
        return store;
    }

    @Test
    void bothFILESAreClosedWithTheDisk() throws Exception {
        Map<String, Tracked> files = new HashMap<>();
        FastDisk disk = FastDisk.open(lease(), LEASE_KEY, JOURNALED,
                (d, name) -> files.computeIfAbsent(name, n -> new Tracked()));

        disk.close();

        assertThat(files.get(FastDisk.EPOCH).closed).isTrue();
        assertThat(files.get(FastDisk.JOURNAL).closed).isTrue();
    }

    @Test
    void aFAILEDStartClosesTheEpochFileItOpened() throws Exception {
        Map<String, Tracked> files = new HashMap<>();
        Tracked epoch = new Tracked();
        epoch.replace(new byte[] {1, 2, 3});
        files.put(FastDisk.EPOCH, epoch);

        assertThatThrownBy(() -> FastDisk.open(lease(), LEASE_KEY, JOURNALED,
                (d, name) -> files.computeIfAbsent(name, n -> new Tracked())))
                .isInstanceOf(IOException.class);

        assertThat(epoch.closed).isTrue();
    }
}
