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
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * The epoch fence on every pod (ADR-0081 §3; M13.26c; criterion 10's last
 * clause: a holder restarted between seeing a higher epoch and a deposed
 * leader's frame of the lower one refuses it).
 */
class EpochFenceTest {

    private static final String LEASE = "p/ctl/lease/0.json";

    private static MemoryBinStore leaseAt(long epoch) throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        store.put(LEASE, Body.ofBytes(new Lease(epoch, "pod", "", 1_000).encode()));
        return store;
    }

    @Test
    void aSTARTRaisesTheFenceToTheLeasesEpoch() throws Exception {
        EpochFence fence = EpochFence.start(leaseAt(7), LEASE, Optional.empty());

        assertThat(fence.highest()).isEqualTo(7);
        assertThat(EpochFence.start(new MemoryBinStore(), LEASE, Optional.empty()).highest())
                .as("no lease was ever written").isZero();
    }

    @Test
    void aFRAMEBelowIsRefusedOneAboveRaisesAndPersists() throws Exception {
        MemoryJournalFile file = new MemoryJournalFile();
        EpochFence fence = EpochFence.start(leaseAt(7), LEASE, Optional.of(file));

        assertThat(fence.admit(6)).isFalse();
        assertThat(fence.admit(7)).isTrue();
        assertThat(fence.admit(9)).isTrue();

        assertThat(fence.highest()).isEqualTo(9);
        assertThat(fence.admit(8)).as("raised by the frame of 9").isFalse();
        file.crash();
        assertThat(FastEpochFile.decode(file.readAll())).as("durable before admit returned")
                .isEqualTo(9);
    }

    @Test
    void aRESTARTedHolderStillRefusesTheDeposedLeadersLowerFrame() throws Exception {
        MemoryJournalFile file = new MemoryJournalFile();
        MemoryBinStore store = leaseAt(7);
        EpochFence before = EpochFence.start(store, LEASE, Optional.of(file));
        before.raise(8);
        file.crash();

        EpochFence after = EpochFence.start(store, LEASE, Optional.of(file));

        assertThat(after.highest()).as("the file's 8 above the lease's 7").isEqualTo(8);
        assertThat(after.admit(7)).isFalse();
    }

    @Test
    void aLEADERWhoseEpochIsBelowTheFenceIsDeposed() throws Exception {
        EpochFence fence = EpochFence.start(leaseAt(7), LEASE, Optional.empty());
        assertThat(fence.deposes(7)).isFalse();

        fence.raise(8);

        assertThat(fence.deposes(7)).as("a successor's FENCE reached the leader of 7").isTrue();
        assertThat(fence.deposes(8)).isFalse();
    }

    @Test
    void anATTACHedFileReceivesTheFenceAndAHigherFileRaisesIt() throws Exception {
        EpochFence fence = EpochFence.start(leaseAt(7), LEASE, Optional.empty());
        MemoryJournalFile fresh = new MemoryJournalFile();
        fence.attach(fresh);
        assertThat(FastEpochFile.decode(fresh.readAll())).isEqualTo(7);

        MemoryJournalFile higher = new MemoryJournalFile();
        higher.replace(FastEpochFile.encode(11));
        fence.attach(higher);

        assertThat(fence.highest()).isEqualTo(11);
    }

    @Test
    void aDAMAGEDEpochFileKeepsThePodUnready() {
        MemoryJournalFile damaged = new MemoryJournalFile();
        damaged.replace(new byte[] {1, 2, 3});

        assertThatThrownBy(() -> EpochFence.start(leaseAt(7), LEASE, Optional.of(damaged)))
                .isInstanceOf(IOException.class);
    }

    /** A file whose every write fails. */
    private static final class FailingFile implements JournalFile {
        @Override
        public byte[] readAll() {
            return new byte[0];
        }

        @Override
        public void append(byte[] bytes) throws IOException {
            throw new IOException("disk gone");
        }

        @Override
        public void force() throws IOException {
            throw new IOException("disk gone");
        }

        @Override
        public void truncate(long length) throws IOException {
            throw new IOException("disk gone");
        }

        @Override
        public void replace(byte[] contents) throws IOException {
            throw new IOException("disk gone");
        }

        @Override
        public long size() {
            return 0;
        }

        @Override
        public void close() {
        }
    }

    @Test
    void aRAISEWhoseWriteFailsThrowsYetRefusesLowerAtOnce() throws Exception {
        EpochFence fence = EpochFence.start(leaseAt(7), LEASE, Optional.empty());
        assertThatThrownBy(() -> fence.attach(new FailingFile())).isInstanceOf(IOException.class);
        EpochFence failing = EpochFence.start(leaseAt(7), LEASE, Optional.empty());
        try {
            failing.attach(new FailingFile());
        } catch (IOException expected) {
            // the file is kept; every later raise tries it again
        }

        assertThatThrownBy(() -> failing.raise(9))
                .as("the caller must not answer COLLECTED").isInstanceOf(IOException.class);
        assertThat(failing.admit(8)).as("yet a lower frame is already refused").isFalse();
    }
}
