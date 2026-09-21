// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Where each shard copy on one node has got to (M7.1, FR-9,
 * <a href="../../../../../../docs/internal/product/decisions/0005-no-consumer-offset-store.md">ADR-0005</a>).
 *
 * <p>⚠️ THIS IS NOT AN OFFSET STORE, AND THE DIFFERENCE IS THE WHOLE DECISION.
 * OpenSearch persists the consumer position inside the same Lucene commit as
 * the documents, atomically, which is stronger than anything an ingester could
 * hold; ADR-0005 therefore refuses a second copy of it. What travels here
 * serves exactly one purpose — a <b>conservative GC watermark</b> — and it may
 * only ever <b>extend</b> retention, never shorten it below the time floor.
 * Nothing resumes from these bytes and nothing seeks with them.
 *
 * <p>⚠️ THE POSITION IS OPTIMISTIC BY CONSTRUCTION, and sizing
 * {@code safetyMargin} is how that is paid for rather than fixed.
 * {@code IngestionEngine.getIngestionState()} reports
 * {@code streamPoller.getBatchStartPointer()} — the IN-MEMORY pointer — while
 * only {@code lastCommittedBatchStartPointer} survives a crash, and OpenSearch
 * exposes no way to read the latter. So every position here can be AHEAD of
 * what a restart would resume from, by up to one Lucene commit interval.
 *
 * <p>⚠️ {@code consumedUpTo} IS EXCLUSIVE: it is the offset the copy would
 * resume FROM, so every record BELOW it is durably indexed and the record AT it
 * is not. That is what {@code StreamPoller.BATCH_START} means, and an
 * implementation reading it as the last-consumed offset is off by one in the
 * deleting direction on every stream.
 *
 * <p>⚠️ IDENTITY IS {@code (indexUuid, partition, shardCopy)}, AND THE COPY IS
 * LOAD-BEARING. With {@code all_active = true} — which document replication
 * requires — every shard copy consumes the partition independently, so a
 * lagging replica in another AZ is a consumer whose data must not be deleted
 * (research 09 §6.2). A frame keyed by partition alone cannot express that and
 * deletes the replica's data silently.
 *
 * <p>⚠️ ONE FRAME PER NODE, NOT PER SHARD. The entries are batched across every
 * partition the node hosts, which is why this record is a LIST rather than a
 * single entry: at eight partitions a frame per shard is eight times the
 * channel for the same information, and the reporting unit is the node because
 * that is what holds the subscription.
 *
 * <p>⚠️ AND IT COSTS NO OBJECT-STORE REQUEST. It rides the subscription the
 * plugin already holds — metadata-only, same-AZ, and the same shape ADR-0047
 * gave {@link IndexRegistration} for the same reason (NFR-2, cost rule R3).
 */
public record ConsumerProgress(List<Entry> entries) {

    /**
     * One copy's position in one stream.
     *
     * <p>⚠️ {@code shardCopy} IS THE ALLOCATION ID. It is how a relocation is
     * told from a restart: a moved shard reports under a NEW id while the old
     * one goes quiet, and both may legitimately exist at once mid-relocation
     * (research 09 §6.4). Retiring the old one because a new one appeared is
     * how a copy's data is deleted while it is still being read.
     *
     * <p>⚠️ {@code consumedUpTo} MAY GO BACKWARDS BETWEEN FRAMES, and the later
     * frame is the true one. The pointer is the in-memory one, so a copy that
     * restarts resumes from its last Lucene commit and reports LOWER than it
     * did before the crash. Monotonicity belongs to the retention RULE, never
     * to this feed.
     */
    public record Entry(String indexUuid, int partition, String shardCopy, long consumedUpTo) {

        public Entry {
            requireText(indexUuid, "indexUuid");
            requireText(shardCopy, "shardCopy");
            if (partition < 0) {
                throw new IllegalArgumentException("partition is never negative: " + partition);
            }
            if (consumedUpTo < 0) {
                // ⚠️ ZERO IS LEGAL AND MEANS "HAS READ NOTHING". A booting
                // shard reports it, and refusing it would make the one copy
                // that needs its whole stream kept the one copy that cannot say
                // so.
                throw new IllegalArgumentException(
                        "consumedUpTo is never negative: " + consumedUpTo);
            }
        }

        /**
         * This COPY, as a key — never "this stream".
         *
         * <p>⚠️ THE NAME IS THE POINT. glossary.md binds <b>stream</b> to
         * {@code (indexUuid, partition)}, and this key carries the copy as
         * well, because what must be unique WITHIN a frame is the copy. A
         * reader that grouped by this thinking it were the stream would take
         * {@code min()} over groups of one, the lagging replica would never
         * hold the watermark back, and its data would be deleted silently —
         * which is the whole reason {@code shardCopy} is in the frame.
         * {@code check-terminology.sh} cannot see that: the word is permitted,
         * the concept is wrong.
         */
        CopyKey copyKey() {
            return new CopyKey(indexUuid, partition, shardCopy);
        }
    }

    /**
     * One copy of one stream, as a value.
     *
     * <p>⚠️ A RECORD RATHER THAN A JOINED STRING. {@code uuid + '/' + partition
     * + '/' + copy} collides: {@code ("a/1", 2, "c")} and
     * {@code ("a", 1, "2/c")} render identically, and the collision refuses a
     * legitimate frame — fail-closed, but a frame dropped for a reason no
     * message can explain.
     */
    record CopyKey(String indexUuid, int partition, String shardCopy) {
    }

    /** {@code "BPCG"} — big-endian, so an operator sees it in a hex dump. */
    public static final int MAGIC = 0x42504347;

    /** The only shape that has ever been written. */
    public static final int VERSION_1 = 1;

    public ConsumerProgress {
        Objects.requireNonNull(entries, "entries");
        entries = List.copyOf(entries);
        if (entries.isEmpty()) {
            // ⚠️ AN EMPTY FRAME IS NOT A REPORT, AND SILENCE ALREADY MEANS
            // SOMETHING HERE. A copy that stops reporting FREEZES at its last
            // value rather than being dropped from the min() (research 09
            // §6.3); a frame carrying no entry that still counted as a report
            // would keep a dead node's copies fresh forever, which is the one
            // thing freshness exists to notice. A node hosting nothing sends
            // nothing.
            throw new IllegalArgumentException("a progress frame with no entry reports nothing");
        }
        Set<CopyKey> seen = new HashSet<>();
        for (Entry entry : entries) {
            if (!seen.add(entry.copyKey())) {
                // ⚠️ TWO POSITIONS FOR ONE COPY IN ONE FRAME IS UNDECIDABLE,
                // and the arbitrary answer is the dangerous one: taking the
                // LATER entry deletes the data the earlier one still needs.
                throw new IllegalArgumentException("two positions for one copy in one frame: "
                        + entry.shardCopy() + " on " + entry.indexUuid() + "/"
                        + entry.partition());
            }
        }
    }

    private static void requireText(String value, String what) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(what + " is never blank");
        }
    }

    /** These bytes, as {@link #VERSION_1}. */
    public byte[] encode() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteBuffer head = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
        head.putInt(MAGIC);
        head.putInt(VERSION_1);
        out.writeBytes(head.array());
        SegmentWriter.putUvarint(out, entries.size());
        for (Entry entry : entries) {
            putString(out, entry.indexUuid());
            SegmentWriter.putUvarint(out, entry.partition());
            putString(out, entry.shardCopy());
            SegmentWriter.putUvarint(out, entry.consumedUpTo());
        }
        return out.toByteArray();
    }

    /**
     * Reads one frame.
     *
     * <p>⚠️ AN UNKNOWN VERSION STOPS; IT DOES NOT SKIP. A reader that took the
     * part it understood would hand the watermark table a position whose copy
     * identity came from a shape nobody sent, and the consequence is a deleted
     * segment rather than a failed parse.
     *
     * <p>⚠️ AND THE ENTRY COUNT IS BOUNDED BY THE BYTES REMAINING before it
     * becomes an allocation, which is {@link Cursor#remaining}'s whole reason:
     * a torn frame claiming {@code 0x7FFFFFFF} entries sized a list before
     * reading one, and the reporting channel died with an
     * {@code OutOfMemoryError} past its own {@code throws IOException}.
     */
    public static ConsumerProgress decode(byte[] bytes) throws IOException {
        Objects.requireNonNull(bytes, "bytes");
        Cursor c = new Cursor(bytes, 0, "consumer progress");
        int magic = ByteBuffer.wrap(c.bytes(4)).order(ByteOrder.BIG_ENDIAN).getInt();
        if (magic != MAGIC) {
            throw new IOException("not a consumer progress frame: magic 0x"
                    + Integer.toHexString(magic));
        }
        int version = ByteBuffer.wrap(c.bytes(4)).order(ByteOrder.BIG_ENDIAN).getInt();
        // ⚠️ != AND NOT >. A version BELOW the known one is what zeroed torn
        // bytes carry, and `> VERSION_1` parses it as v1 -- accepting a shape
        // nobody wrote, on the frame that decides what is deleted.
        if (version != VERSION_1) {
            throw new IOException("consumer progress version " + version
                    + " is not readable by this build, which knows " + VERSION_1
                    + " -- refusing rather than guessing at a shape that decides what is "
                    + "deleted");
        }
        long count = c.uvarint();
        if (count < 0 || count > c.remaining()) {
            throw new IOException("consumer progress claims " + count + " entries with only "
                    + c.remaining() + " bytes left");
        }
        List<Entry> entries = new ArrayList<>((int) count);
        for (long i = 0; i < count; i++) {
            String indexUuid = getString(c);
            int partition = toInt(c.uvarint(), "partition");
            String shardCopy = getString(c);
            long consumedUpTo = c.uvarint();
            if (consumedUpTo < 0) {
                throw new IOException("consumedUpTo " + consumedUpTo + " does not fit a long");
            }
            try {
                entries.add(new Entry(indexUuid, partition, shardCopy, consumedUpTo));
            } catch (IllegalArgumentException refused) {
                throw new IOException("consumer progress entry is not valid: "
                        + refused.getMessage(), refused);
            }
        }
        if (!c.atEnd()) {
            // ⚠️ TRAILING BYTES ARE A REFUSAL, NOT SLACK: a different shape,
            // two frames run together, or a torn stream -- and the one thing
            // they are not is this frame.
            throw new IOException("consumer progress has " + c.remaining()
                    + " trailing bytes after its last entry");
        }
        try {
            return new ConsumerProgress(entries);
        } catch (IllegalArgumentException refused) {
            // ⚠️ AN IOException, NOT AN IAE, because this arrived over a wire:
            // an unchecked throw out of a decode is what kills the loop reading
            // the channel, and this channel going down stops every watermark on
            // the node.
            throw new IOException("consumer progress is not valid: " + refused.getMessage(),
                    refused);
        }
    }

    private static int toInt(long value, String what) throws IOException {
        if (value < 0 || value > Integer.MAX_VALUE) {
            throw new IOException(what + " " + value + " does not fit an int");
        }
        return (int) value;
    }

    private static void putString(ByteArrayOutputStream out, String value) {
        byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
        SegmentWriter.putUvarint(out, utf8.length);
        out.writeBytes(utf8);
    }

    private static String getString(Cursor c) throws IOException {
        long length = c.uvarint();
        if (length < 0 || length > c.remaining()) {
            throw new IOException("consumer progress claims a " + length
                    + "-byte string with only " + c.remaining() + " bytes left");
        }
        return new String(c.bytes((int) length), StandardCharsets.UTF_8);
    }
}
