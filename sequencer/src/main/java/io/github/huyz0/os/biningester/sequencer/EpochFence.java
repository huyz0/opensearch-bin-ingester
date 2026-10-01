// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.JournalFile;
import io.github.huyz0.os.biningester.format.FastEpochFile;
import io.github.huyz0.os.biningester.format.Lease;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.Optional;

/**
 * The epoch fence on every pod (ADR-0081 §3; M13.26c): the highest fast epoch
 * this pod has seen, raised by any frame carrying a higher one and by a
 * successor's FENCE, refusing every frame of a lower epoch.
 *
 * <p>⚠️ RAISED AT EVERY CONTAINER START from the lease object's epoch, read
 * once from the store: every frame of epoch {@code E} is sent by a holder of
 * lease {@code E}, so the lease's epoch is at least any epoch a frame carried.
 * A pod whose start-up read fails stays unready -- {@link #start} throws -- so
 * a pod that never journaled needs no file and no disk ({@code wal=false}
 * stays diskless).
 *
 * <p>⚠️ THE FILE IS A SECOND COPY, kept beside the journal once the pod has
 * one ({@link #attach}) -- ADR-0081 §3 keeps it for a pod whose store reads
 * fail as it restarts, but this start still requires the lease read, so here
 * it only ever raises the fence, never stands in for the store. It is written by the {@link JournalFile} seam's whole-file {@code replace}
 * (written beside, fsynced, renamed, the directory fsynced -- ADR-0083), and
 * every raise is persisted BEFORE it returns -- retried by the next raise
 * when a write failed, whether or not the epoch rose -- so an answer that depends on it
 * -- COLLECTED above all -- is sent only once it is durable.
 *
 * <p>⚠️ THE LEADER IS FENCED THE SAME WAY: once {@link #deposes} its own epoch,
 * it assigns and exposes nothing more.
 */
public final class EpochFence {

    private long highest;
    /** The epoch the file is known to hold; below {@link #highest} after a failed write. */
    private long persisted = -1;
    private JournalFile file;

    private EpochFence(long highest, JournalFile file) {
        this.highest = highest;
        this.file = file;
    }

    /**
     * The fence at a container start: the higher of the lease's epoch (0 when
     * no lease was ever written) and the epoch file's, when the pod keeps one.
     *
     * @throws IOException the lease could not be read, or the epoch file does
     *     not decode -- the pod stays unready, never starts lower
     */
    public static EpochFence start(BinStore store, String leaseKey, Optional<JournalFile> file)
            throws IOException {
        Objects.requireNonNull(store, "store");
        Objects.requireNonNull(leaseKey, "leaseKey");
        Objects.requireNonNull(file, "file");
        long leaseEpoch = 0;
        if (store.stat(leaseKey).isPresent()) {
            try (InputStream in = store.get(leaseKey)) {
                leaseEpoch = Lease.decode(in.readAllBytes()).epoch();
            }
        }
        EpochFence fence = new EpochFence(leaseEpoch, null);
        if (file.isPresent()) {
            fence.attach(file.get());
        }
        return fence;
    }

    /** The highest fast epoch this pod has seen. */
    public synchronized long highest() {
        return highest;
    }

    /**
     * A frame of {@code frameEpoch} arrived: whether to read it. A higher
     * epoch raises the fence first, durably; a lower one is refused.
     */
    public synchronized boolean admit(long frameEpoch) throws IOException {
        if (frameEpoch < highest) {
            return false;
        }
        raise(frameEpoch);
        return true;
    }

    /**
     * Raises the fence to {@code epoch} -- a successor's FENCE -- and returns
     * once it is durable where the pod keeps a file. Never lowers it.
     *
     * <p>⚠️ RAISED IN MEMORY FIRST: a lower frame is refused from this
     * instant, even when the write below fails and the caller cannot answer.
     */
    public synchronized void raise(long epoch) throws IOException {
        if (epoch > highest) {
            highest = epoch;
        }
        // ⚠️ WRITTEN WHENEVER THE FILE IS BEHIND, not only when the epoch rose
        // (M13.26c review round 1, P1): a write that failed left it behind,
        // and the successor's retried FENCE must not be answered before it
        // is durable.
        if (file != null && persisted < highest) {
            persist();
        }
    }

    /** Whether a leader of {@code ownEpoch} is fenced: it assigns and exposes nothing more. */
    public synchronized boolean deposes(long ownEpoch) {
        return highest > ownEpoch;
    }

    /**
     * Keeps the fence in {@code epochFile} from now on, the pod's journal
     * having been created: the file's value, if higher, raises the fence, and
     * the fence's is written before this returns.
     */
    public synchronized void attach(JournalFile epochFile) throws IOException {
        Objects.requireNonNull(epochFile, "epochFile");
        byte[] stored = epochFile.readAll();
        if (stored.length > 0) {
            highest = Math.max(highest, FastEpochFile.decode(stored));
        }
        file = epochFile;
        persisted = -1;
        persist();
    }

    private void persist() throws IOException {
        long writing = highest;
        file.replace(FastEpochFile.encode(writing));
        persisted = writing;
    }
}
