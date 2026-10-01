// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.zip.CRC32C;

/**
 * One record of a pod's fast journal (M13.24, ADR-0082 §4).
 *
 * <p>Every record is {@code magic u32, version u8, length u32, CRC32C u32}
 * followed by {@code length} bytes -- the kind and the kind's fields -- and
 * the checksum covers exactly those bytes. Integers are big-endian; a RunKey
 * is the index UUID (two i64) and the partition (i32), as in the fast frames.
 *
 * <p>⚠️ RECOVERY STOPS AT THE FIRST RECORD IT CANNOT TAKE; IT NEVER SKIPS.
 * {@link #decodeAll} reports how many bytes the whole records span and which
 * stop it was: after a TEAR the journal rewrites the file to what it read
 * before it appends again (entries are answered only after their group's
 * fsync, so everything after a tear was unanswered); at a whole record this
 * build cannot READ it refuses and cuts nothing (ADR-0082 §4).
 */
public sealed interface FastJournalRecord
        permits FastJournalRecord.Entry, FastJournalRecord.Release, FastJournalRecord.Drop {

    /** {@code "BFJE"}. */
    int MAGIC = 0x42464A45;

    /** The only version written. */
    int VERSION = 1;

    /** Magic, version, length and CRC32C: the bytes before the kind. */
    int HEADER_BYTES = 13;

    /** The largest {@code assignedAfter} a u32 holds. */
    long MAX_U32 = 0xFFFF_FFFFL;

    /** These bytes, header included. */
    byte[] encode();

    /** The idempotency key of a fast batch, as in {@code COMMIT} (ADR-0082 §2). */
    record IdempotencyKey(String podId, UUID incarnationId, long fastSeq) {
        public IdempotencyKey {
            Objects.requireNonNull(podId, "podId");
            Objects.requireNonNull(incarnationId, "incarnationId");
        }
    }

    /**
     * An entry the pod holds: one run of one batch, at its assigned offsets.
     *
     * <p>⚠️ {@code assignedAfter} WITH {@code epoch} IS THE ENTRY'S IDENTITY
     * (ADR-0081 §5): after a switch's discard one term may hold two entries of
     * a stream at one offset, told apart only by it.
     */
    record Entry(long epoch, RunKey key, long firstOffset, int walQuorum, long assignedAfter,
            IdempotencyKey idempotencyKey, List<SegmentRecord> records)
            implements FastJournalRecord {
        public Entry {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(idempotencyKey, "idempotencyKey");
            Objects.requireNonNull(records, "records");
            records = List.copyOf(records);
            if (records.isEmpty()) {
                throw new IllegalArgumentException("an entry with no records holds nothing");
            }
            if (firstOffset < 0) {
                throw new IllegalArgumentException("firstOffset is never " + firstOffset);
            }
            if (walQuorum < 1 || walQuorum > 3) {
                throw new IllegalArgumentException("walQuorum " + walQuorum + " is not 1, 2 or 3");
            }
            requireU32(assignedAfter, "assignedAfter");
        }

        /** The offset after this entry's last record. */
        public long endOffset() {
            return firstOffset + records.size();
        }

        @Override
        public byte[] encode() {
            ByteArrayOutputStream rest = new ByteArrayOutputStream();
            rest.write(KIND_ENTRY);
            rest.writeBytes(ByteBuffer.allocate(8).putLong(epoch).array());
            putRunKey(rest, key);
            rest.writeBytes(ByteBuffer.allocate(12).putLong(firstOffset)
                    .putInt(records.size()).array());
            rest.write(walQuorum);
            rest.writeBytes(ByteBuffer.allocate(4).putInt((int) assignedAfter).array());
            byte[] podId = idempotencyKey.podId().getBytes(StandardCharsets.UTF_8);
            SegmentWriter.putUvarint(rest, podId.length);
            rest.writeBytes(podId);
            rest.writeBytes(ByteBuffer.allocate(24)
                    .putLong(idempotencyKey.incarnationId().getMostSignificantBits())
                    .putLong(idempotencyKey.incarnationId().getLeastSignificantBits())
                    .putLong(idempotencyKey.fastSeq()).array());
            rest.writeBytes(SegmentWriter.encodeRecords(records));
            return frame(rest.toByteArray());
        }
    }

    /** Entries of {@code key} wholly below {@code releaseBelow} are released. */
    record Release(RunKey key, long releaseBelow) implements FastJournalRecord {
        public Release {
            Objects.requireNonNull(key, "key");
        }

        @Override
        public byte[] encode() {
            ByteArrayOutputStream rest = new ByteArrayOutputStream();
            rest.write(KIND_RELEASE);
            putRunKey(rest, key);
            rest.writeBytes(ByteBuffer.allocate(8).putLong(releaseBelow).array());
            return frame(rest.toByteArray());
        }
    }

    /**
     * The pod no longer holds the {@code (epoch, assignedAfter)} group of
     * {@code key} at or above {@code dropFrom}: superseded, or of a closed term.
     */
    record Drop(RunKey key, long epoch, long assignedAfter, long dropFrom)
            implements FastJournalRecord {
        public Drop {
            Objects.requireNonNull(key, "key");
            requireU32(assignedAfter, "assignedAfter");
        }

        @Override
        public byte[] encode() {
            ByteArrayOutputStream rest = new ByteArrayOutputStream();
            rest.write(KIND_DROP);
            putRunKey(rest, key);
            rest.writeBytes(ByteBuffer.allocate(20).putLong(epoch).putInt((int) assignedAfter)
                    .putLong(dropFrom).array());
            return frame(rest.toByteArray());
        }
    }

    /**
     * What a recovery read found: the whole records, the bytes they span, and
     * whether the read stopped at a record that is WHOLE but unreadable.
     *
     * <p>⚠️ A TEAR AND AN UNREADABLE RECORD ARE DIFFERENT STOPS (M13.24 review
     * round 1, P1). A bad magic, a length past the end or a checksum that fails
     * is a tear -- bytes no fsync covered -- and is cut. A record whose
     * checksum VERIFIES but whose version, kind or fields this build cannot
     * read was fsynced by some build, so its copies may have been answered;
     * cutting it would durably erase them after a rollback, invisibly to the
     * quorum-loss predicate. {@code unreadable} reports that stop, and the
     * journal refuses to recover rather than truncate.
     */
    record Recovered(List<FastJournalRecord> records, int validLength, boolean unreadable) {
        public Recovered {
            records = List.copyOf(records);
        }
    }

    /** Every whole record from the start of {@code file}, stopping at the first that is not. */
    static Recovered decodeAll(byte[] file) {
        Objects.requireNonNull(file, "file");
        List<FastJournalRecord> out = new ArrayList<>();
        int at = 0;
        boolean unreadable = false;
        while (file.length - at >= HEADER_BYTES) {
            ByteBuffer head = ByteBuffer.wrap(file, at, HEADER_BYTES).order(ByteOrder.BIG_ENDIAN);
            int magic = head.getInt();
            int version = head.get() & 0xFF;
            long length = head.getInt() & 0xFFFF_FFFFL;
            int crc = head.getInt();
            if (magic != MAGIC || length > file.length - at - HEADER_BYTES) {
                break;
            }
            int start = at + HEADER_BYTES;
            CRC32C check = new CRC32C();
            check.update(file, start, (int) length);
            if ((int) check.getValue() != crc) {
                break;
            }
            // ⚠️ FROM HERE THE RECORD IS WHOLE: a version, kind or field this
            // build cannot read is UNREADABLE, never a tear (see Recovered).
            if (version != VERSION) {
                unreadable = true;
                break;
            }
            FastJournalRecord record;
            try {
                record = decodeRest(java.util.Arrays.copyOfRange(file, start, start + (int) length));
            } catch (IOException | IllegalArgumentException notReadable) {
                unreadable = true;
                break;
            }
            out.add(record);
            at = start + (int) length;
        }
        return new Recovered(out, at, unreadable);
    }

    /** The kinds, as written. */
    int KIND_ENTRY = 1;

    /** A release record. */
    int KIND_RELEASE = 2;

    /** A drop record. */
    int KIND_DROP = 3;

    private static FastJournalRecord decodeRest(byte[] rest) throws IOException {
        Cursor c = new Cursor(rest, 0, "fast journal record");
        int kind = c.bytes(1)[0];
        return switch (kind) {
            case KIND_ENTRY -> {
                long epoch = i64(c);
                RunKey key = runKey(c);
                long firstOffset = i64(c);
                long count = u32(c);
                int walQuorum = c.bytes(1)[0];
                long assignedAfter = u32(c);
                long podLength = c.uvarint();
                if (podLength < 0 || podLength > c.remaining()) {
                    throw new IOException("pod id overruns the record");
                }
                String podId = new String(c.bytes((int) podLength), StandardCharsets.UTF_8);
                UUID incarnation = new UUID(i64(c), i64(c));
                long fastSeq = i64(c);
                if (count > c.remaining()) {
                    throw new IOException("record count " + count + " overruns the record");
                }
                List<SegmentRecord> records = SegmentReader.decodeRecords(
                        c.bytes(c.remaining()), (int) count);
                yield new Entry(epoch, key, firstOffset, walQuorum, assignedAfter,
                        new IdempotencyKey(podId, incarnation, fastSeq), records);
            }
            case KIND_RELEASE -> {
                Release release = new Release(runKey(c), i64(c));
                requireEnd(c);
                yield release;
            }
            case KIND_DROP -> {
                Drop drop = new Drop(runKey(c), i64(c), u32(c), i64(c));
                requireEnd(c);
                yield drop;
            }
            default -> throw new IOException("fast journal record kind " + kind + " is unknown");
        };
    }

    private static void requireEnd(Cursor c) throws IOException {
        if (!c.atEnd()) {
            throw new IOException("fast journal record has " + c.remaining() + " trailing bytes");
        }
    }

    private static long i64(Cursor c) throws IOException {
        return ByteBuffer.wrap(c.bytes(8)).getLong();
    }

    private static long u32(Cursor c) throws IOException {
        return ByteBuffer.wrap(c.bytes(4)).getInt() & 0xFFFF_FFFFL;
    }

    private static RunKey runKey(Cursor c) throws IOException {
        long msb = i64(c);
        long lsb = i64(c);
        int partition = ByteBuffer.wrap(c.bytes(4)).getInt();
        return new RunKey(new UUID(msb, lsb), partition);
    }

    private static void putRunKey(ByteArrayOutputStream out, RunKey key) {
        out.writeBytes(ByteBuffer.allocate(20)
                .putLong(key.indexId().getMostSignificantBits())
                .putLong(key.indexId().getLeastSignificantBits())
                .putInt(key.partitionId()).array());
    }

    private static byte[] frame(byte[] rest) {
        CRC32C crc = new CRC32C();
        crc.update(rest);
        return ByteBuffer.allocate(HEADER_BYTES + rest.length).order(ByteOrder.BIG_ENDIAN)
                .putInt(MAGIC).put((byte) VERSION).putInt(rest.length).putInt((int) crc.getValue())
                .put(rest).array();
    }

    private static void requireU32(long value, String what) {
        if (value < 0 || value > MAX_U32) {
            throw new IllegalArgumentException(what + " " + value + " is not a u32");
        }
    }
}
