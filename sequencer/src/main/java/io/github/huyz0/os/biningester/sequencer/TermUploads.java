// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.format.RunKey;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A fast term's uploads: their pacing at {@code min_upload_interval}
 * (M13.31a) and the sequence each upload delta is keyed by (its review round
 * 1, P1). Split out of {@link FastLeaderTerm} at its ceiling (M13.34o), with
 * no behaviour changed.
 */
final class TermUploads {

    private final MonotonicClock mono;
    private final long refreshNanos;
    /** When this term last uploaded. */
    private long lastUploadNanos;
    private boolean uploaded;
    private long uploadSeq;
    /** The first offsets of the upload whose delta is not known committed; null when none. */
    private Map<RunKey, Long> pendingUpload;
    private final ReentrantLock uploading = new ReentrantLock();

    TermUploads(MonotonicClock mono, long refreshNanos) {
        this.mono = Objects.requireNonNull(mono, "mono");
        this.refreshNanos = refreshNanos;
    }

    /**
     * Whether an upload begins now: never on an idle desk with no upload's
     * commit outstanding (M13.87) -- no roster read on an idle tick -- and,
     * when {@code paced}, at most once per {@code min_upload_interval}.
     */
    synchronized boolean begin(boolean idle, boolean paced) {
        if (idle && pendingUpload == null) {
            return false;
        }
        long now = mono.nanos();
        if (paced && uploaded && now - lastUploadNanos < refreshNanos) {
            return false;
        }
        uploaded = true;
        lastUploadNanos = now;
        return true;
    }

    /** One upload at a time. */
    void lock() {
        uploading.lock();
    }

    void unlock() {
        uploading.unlock();
    }

    /**
     * The sequence of the upload delta at {@code firsts}: ⚠️ THE SAME UNTIL ONE
     * IS COMMITTED, so a retry of a delta that landed with its answer lost is
     * folded into it rather than committing its records twice.
     */
    synchronized long seq(Map<RunKey, Long> firsts) {
        if (!firsts.equals(pendingUpload)) {
            pendingUpload = Map.copyOf(firsts);
            uploadSeq++;
        }
        return uploadSeq;
    }

    /** The upload delta at {@code firsts} is committed: the next takes a new sequence. */
    synchronized void committed(Map<RunKey, Long> firsts) {
        if (firsts.equals(pendingUpload)) {
            pendingUpload = null;
        }
    }
}
