// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.JournalFile;
import io.github.huyz0.os.biningester.sequencer.EpochFence;
import io.github.huyz0.os.biningester.sequencer.FastJournal;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * A pod's epoch fence and fast journal, opened at startup (ADR-0081 §3,
 * ADR-0082 §4, ADR-0083; M13.27i).
 *
 * <p>⚠️ THE FENCE ALWAYS, THE JOURNAL ONLY WHERE A DIRECTORY IS CONFIGURED: a
 * diskless pod fences at the lease's epoch read from the store and opens no
 * file (the glossary's {@code wal=false} ingester); a journaled one keeps the
 * fence in its epoch file too, written before it is relied on, and recovers
 * its journal under the configured cap.
 *
 * <p>⚠️ A FAILED START KEEPS THE POD FROM STARTING: an unreadable lease, an
 * epoch file that does not decode or a journal record this build cannot read
 * throws, and the node does not start -- never a lower fence or a guessed
 * journal (ADR-0082 §4).
 */
final class FastDisk implements Closeable {

    /** The journal's file in the directory. */
    static final String JOURNAL = "journal";
    /** The epoch file, beside it. */
    static final String EPOCH = "epoch";

    /** Opens a named file in a directory: {@code FileJournalFile::in} in production. */
    @FunctionalInterface
    interface Opener {
        JournalFile open(String directory, String name) throws IOException;
    }

    private final EpochFence fence;
    private final Optional<FastJournal> journal;
    /** The files this disk opened, closed with it. */
    private final List<JournalFile> files;

    private FastDisk(EpochFence fence, Optional<FastJournal> journal, JournalFile... files) {
        this.fence = fence;
        this.journal = journal;
        this.files = List.of(files);
    }

    /**
     * The fence from the lease at {@code leaseKey}, and the journal where
     * {@code config} names a directory.
     */
    static FastDisk open(BinStore store, String leaseKey, Optional<FastJournalConfig> config,
            Opener opener) throws IOException {
        Objects.requireNonNull(store, "store");
        Objects.requireNonNull(leaseKey, "leaseKey");
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(opener, "opener");
        if (config.isEmpty()) {
            return new FastDisk(EpochFence.start(store, leaseKey, Optional.empty()),
                    Optional.empty());
        }
        String dir = config.get().directory();
        JournalFile epochFile = opener.open(dir, EPOCH);
        JournalFile journalFile = null;
        try {
            EpochFence fence = EpochFence.start(store, leaseKey, Optional.of(epochFile));
            journalFile = opener.open(dir, JOURNAL);
            FastJournal journal = FastJournal.recover(journalFile, config.get().capBytes());
            return new FastDisk(fence, Optional.of(journal), epochFile, journalFile);
        } catch (IOException | RuntimeException failed) {
            closeQuietly(epochFile, failed);
            if (journalFile != null) {
                closeQuietly(journalFile, failed);
            }
            throw failed;
        }
    }

    EpochFence fence() {
        return fence;
    }

    Optional<FastJournal> journal() {
        return journal;
    }

    @Override
    public void close() throws IOException {
        IOException first = null;
        for (JournalFile file : files) {
            try {
                file.close();
            } catch (Exception failed) {
                IOException wrapped = failed instanceof IOException io ? io
                        : new IOException(failed);
                if (first == null) {
                    first = wrapped;
                } else {
                    first.addSuppressed(wrapped);
                }
            }
        }
        if (first != null) {
            throw first;
        }
    }

    private static void closeQuietly(JournalFile file, Exception failed) {
        try {
            file.close();
        } catch (Exception alsoFailed) {
            failed.addSuppressed(alsoFailed);
        }
    }
}
