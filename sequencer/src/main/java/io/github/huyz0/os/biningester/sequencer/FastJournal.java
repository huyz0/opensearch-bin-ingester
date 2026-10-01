// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.binstore.JournalFile;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.RunKey;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * A pod's fast journal: the entries it holds until they are committed (M13.24;
 * ADR-0081, ADR-0082 §4). The leader and every holder keep one.
 *
 * <p>⚠️ APPENDED IS NOT ANSWERED. {@link #append} only buffers; the caller
 * answers nothing about an entry -- no ack, no CONFIRM, no REPLICA_ACK, no
 * exposure -- until a {@link #sync} that covers it has returned, and syncs once
 * per group of batches (ADR-0081 §2). So everything after the first TEAR at
 * recovery was never answered; a whole record this build cannot read may have
 * been, and recovery refuses rather than cut it.
 *
 * <p>⚠️ RECOVERY REWRITES THE FILE BEFORE ANYTHING IS APPENDED, through an
 * atomic replace of the held entries it read (M13.24 review round 3, P1). That
 * cuts a torn tail -- an entry written after an uncut tear would be unreadable
 * behind it at the next recovery (ADR-0082 §4; M13.22c review round 2, P1) --
 * and, after a failed fsync, it makes durable exactly what the new journal
 * holds: the bytes read back may have come from a page cache whose writeback
 * failed, so trusting the old file would put answered entries behind a hole a
 * reboot cuts. The cost is one rewrite of at most the held bytes per recovery.
 *
 * <p>⚠️ THE CAP BOUNDS HELD BYTES, AND A REFUSAL IS BACKPRESSURE. An entry that
 * would take the held entries past the cap is refused and nothing of it is
 * written; the caller holds the batch (cost.md rule 14) and triggers an
 * upload. Released and dropped bytes do not count; they are compacted away
 * once they pass half the file -- a new file renamed over the old, never a
 * rewrite in place.
 *
 * <p>⚠️ THE FIRST I/O FAILURE ENDS THIS JOURNAL (M13.24 review round 2, P1).
 * After an {@code IOException} from the file every later call is refused: a
 * failed append may have left half an entry in the file, and a failed fsync
 * must not be retried, because Linux may drop the unwritten pages and report
 * the retry a success. The owner recovers a new journal from the file, which
 * rewrites it before anything is appended.
 *
 * <p>⚠️ NOT THREAD-SAFE BY ITSELF: the leader's sequencer and a holder's
 * replica endpoint each serialise their journal under their own lock, which
 * is where the order of append, sync and answer is decided.
 */
public final class FastJournal {

    /** An entry held: its append order, and its encoded size so the cap never re-encodes. */
    private record Held(long order, FastJournalRecord.Entry entry, int bytes) {
    }

    private final JournalFile file;
    private final long capBytes;

    /**
     * ⚠️ BY STREAM, so a release or drop scans one stream's entries rather than
     * every held entry (M13.24 review round 3, P3); {@link Held#order} keeps the
     * append order across streams.
     */
    private final Map<RunKey, List<Held>> held = new HashMap<>();
    private long nextOrder;
    private long heldBytes;
    private IOException failed;

    private FastJournal(JournalFile file, long capBytes) {
        this.file = file;
        this.capBytes = capBytes;
    }

    /**
     * Reads {@code file} and rewrites it, atomically, to the entries held --
     * before anything is appended.
     *
     * @throws IOException for a whole record this build cannot read, without
     *     changing the file: some build fsynced it, so its copies may have
     *     been answered, and the pod stays unready until a build reads it
     */
    public static FastJournal recover(JournalFile file, long capBytes) throws IOException {
        Objects.requireNonNull(file, "file");
        if (capBytes <= 0) {
            throw new IllegalArgumentException("capBytes is never " + capBytes);
        }
        FastJournal journal = new FastJournal(file, capBytes);
        FastJournalRecord.Recovered read = FastJournalRecord.decodeAll(file.readAll());
        if (read.unreadable()) {
            throw new IOException("the fast journal holds a whole record this build cannot read "
                    + "at byte " + read.validLength() + " -- refusing to recover rather than "
                    + "cut copies that may have been answered");
        }
        for (FastJournalRecord record : read.records()) {
            journal.apply(record);
        }
        journal.io(() -> file.replace(journal.encodeHeld()));
        return journal;
    }

    private void apply(FastJournalRecord record) {
        switch (record) {
            case FastJournalRecord.Entry entry -> add(entry, entry.encode().length);
            case FastJournalRecord.Release release -> removeIf(release.key(),
                    e -> e.endOffset() <= release.releaseBelow());
            case FastJournalRecord.Drop drop -> removeIf(drop.key(),
                    e -> e.epoch() == drop.epoch() && e.assignedAfter() == drop.assignedAfter()
                            && e.firstOffset() >= drop.dropFrom());
        }
    }

    private void add(FastJournalRecord.Entry entry, int bytes) {
        held.computeIfAbsent(entry.key(), k -> new ArrayList<>())
                .add(new Held(nextOrder++, entry, bytes));
        heldBytes += bytes;
    }

    private void removeIf(RunKey key, Predicate<FastJournalRecord.Entry> gone) {
        List<Held> stream = held.get(key);
        if (stream == null) {
            return;
        }
        Iterator<Held> it = stream.iterator();
        while (it.hasNext()) {
            Held h = it.next();
            if (gone.test(h.entry())) {
                heldBytes -= h.bytes();
                it.remove();
            }
        }
        if (stream.isEmpty()) {
            held.remove(key);
        }
    }

    private List<Held> inOrder() {
        List<Held> all = new ArrayList<>();
        held.values().forEach(all::addAll);
        all.sort(Comparator.comparingLong(Held::order));
        return all;
    }

    private byte[] encodeHeld() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (Held h : inOrder()) {
            out.writeBytes(h.entry().encode());
        }
        return out.toByteArray();
    }

    /** Refuses any call once an I/O failure has ended this journal. */
    private void requireUsable() {
        if (failed != null) {
            throw new IllegalStateException("this fast journal failed and must be recovered from "
                    + "its file before it is used again", failed);
        }
    }

    /** Runs one file operation, ending this journal if it fails. */
    private void io(IoStep step) throws IOException {
        requireUsable();
        try {
            step.run();
        } catch (IOException failure) {
            failed = failure;
            throw failure;
        }
    }

    @FunctionalInterface
    private interface IoStep {
        void run() throws IOException;
    }

    /** Appends {@code entry}, or refuses it -- false -- if held bytes would pass the cap. */
    public boolean append(FastJournalRecord.Entry entry) throws IOException {
        requireUsable();
        byte[] bytes = entry.encode();
        if (heldBytes + bytes.length > capBytes) {
            return false;
        }
        io(() -> file.append(bytes));
        add(entry, bytes.length);
        return true;
    }

    /** Makes everything appended durable, then compacts if released bytes pass half. */
    public void sync() throws IOException {
        io(file::force);
        long size = file.size();
        if (size - heldBytes > size / 2) {
            byte[] compacted = encodeHeld();
            io(() -> file.replace(compacted));
        }
    }

    /**
     * Releases the entries of {@code key} wholly below {@code below}.
     *
     * <p>⚠️ DURABLE AT THE NEXT {@link #sync}: a crash before it brings the
     * released entries back as held, which is safe -- they were committed --
     * and costs one HELD report (ADR-0081 §5.7).
     */
    public void release(RunKey key, long below) throws IOException {
        FastJournalRecord.Release release = new FastJournalRecord.Release(key, below);
        io(() -> file.append(release.encode()));
        apply(release);
    }

    /**
     * Drops the {@code (epoch, assignedAfter)} group of {@code key} whose entries
     * START at or above {@code from}. ⚠️ An entry straddling {@code from} is
     * kept: a decision's resume offset is a quorum frontier, which lies on an
     * entry boundary, so a straddle is not a shape the protocol produces.
     * Durable at the next {@link #sync}, as a release is.
     */
    public void drop(RunKey key, long epoch, long assignedAfter, long from) throws IOException {
        FastJournalRecord.Drop drop = new FastJournalRecord.Drop(key, epoch, assignedAfter, from);
        io(() -> file.append(drop.encode()));
        apply(drop);
    }

    /** The entries held, in the order they were appended. */
    public List<FastJournalRecord.Entry> held() {
        return inOrder().stream().map(Held::entry).toList();
    }

    /** The encoded bytes of the entries held. */
    public long heldBytes() {
        return heldBytes;
    }

    /** The journal file's length. */
    public long fileBytes() {
        return file.size();
    }
}
