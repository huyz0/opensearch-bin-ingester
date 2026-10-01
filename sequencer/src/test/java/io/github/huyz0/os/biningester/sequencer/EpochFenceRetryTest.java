// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.JournalFile;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryJournalFile;
import io.github.huyz0.os.biningester.format.FastEpochFile;
import io.github.huyz0.os.biningester.format.Lease;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * A failed write retried, a file below the lease, and a lease that cannot be
 * read (M13.26c review round 1, P1, T1, T2).
 */
class EpochFenceRetryTest {

    private static final String LEASE = "p/ctl/lease/0.json";

    private static MemoryBinStore leaseAt(long epoch) throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        store.put(LEASE, Body.ofBytes(new Lease(epoch, "pod", "", 1_000).encode()));
        return store;
    }

    /** A file whose writes fail while {@code failing} is set. */
    private static final class FlakyFile implements JournalFile {
        final MemoryJournalFile inner = new MemoryJournalFile();
        boolean failing;

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
        public void replace(byte[] contents) throws IOException {
            if (failing) {
                throw new IOException("disk full");
            }
            inner.replace(contents);
        }

        @Override
        public long size() {
            return inner.size();
        }

        @Override
        public void close() {
        }
    }

    @Test
    void aFENCERetriedAfterAFailedWriteIsAnsweredOnlyOnceDurable() throws Exception {
        FlakyFile file = new FlakyFile();
        EpochFence fence = EpochFence.start(leaseAt(7), LEASE, Optional.of(file));
        file.failing = true;
        assertThatThrownBy(() -> fence.raise(9)).isInstanceOf(IOException.class);

        assertThatThrownBy(() -> fence.raise(9))
                .as("the same FENCE again: the file still holds 7").isInstanceOf(IOException.class);
        file.failing = false;
        fence.raise(9);

        assertThat(FastEpochFile.decode(file.readAll())).isEqualTo(9);
    }

    @Test
    void aFRAMEAtTheRaisedEpochAfterAFailedWriteRetriesTheWrite() throws Exception {
        FlakyFile file = new FlakyFile();
        EpochFence fence = EpochFence.start(leaseAt(7), LEASE, Optional.of(file));
        file.failing = true;
        assertThatThrownBy(() -> fence.admit(9)).isInstanceOf(IOException.class);
        file.failing = false;

        assertThat(fence.admit(9)).isTrue();

        assertThat(FastEpochFile.decode(file.readAll())).isEqualTo(9);
    }

    @Test
    void aFILEBelowTheLeaseDoesNotLowerTheFence() throws Exception {
        MemoryJournalFile file = new MemoryJournalFile();
        file.replace(FastEpochFile.encode(7));

        EpochFence fence = EpochFence.start(leaseAt(9), LEASE, Optional.of(file));

        assertThat(fence.highest()).isEqualTo(9);
        assertThat(fence.admit(8)).as("a deposed leader's frame of 8").isFalse();
        assertThat(FastEpochFile.decode(file.readAll())).isEqualTo(9);
    }

    @Test
    void aLEASEThatCannotBeReadKeepsThePodUnready() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        store.put(LEASE, Body.ofBytes("not a lease".getBytes(StandardCharsets.UTF_8)));

        assertThatThrownBy(() -> EpochFence.start(store, LEASE, Optional.empty()))
                .as("never started at 0, admitting every frame").isInstanceOf(IOException.class);
    }
}
