// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * The fast write's frames (ADR-0082 §2; M13.27e): COMMIT, ASSIGNED, CONFIRM,
 * EXPOSED, REPLICA and REPLICA_ACK, under {@link FastFrame}'s header, in a
 * file of their own so neither stays past the size limit.
 *
 * <p>⚠️ EVERY ANSWER ABOUT AN ENTRY NAMES IT WHOLE (ADR-0081 §2.4): with the
 * header's epoch, {@code assignedAfter} and each run's RunKey and first offset
 * -- so a late answer about a discarded entry never counts toward a later one
 * at the same offset.
 *
 * <p>Layout after the header, big-endian, a count a u32 bounded by the bytes
 * left: COMMIT is the idempotency key (podId string, incarnation UUID as two
 * i64, {@code fastSeq} i64) then its runs, each a RunKey, its record count u32,
 * the byte length of its records u32 and the records in the segment record
 * encoding; ASSIGNED is {@code assignedAfter} u32 then per run a RunKey, the
 * first offset i64, {@code walQuorum} u8 and {@code copyRequired} u8; CONFIRM
 * is the idempotency key, {@code assignedAfter}, the runs (RunKey, first
 * offset) and {@code copyJournaled} u8; EXPOSED and REPLICA_ACK are
 * {@code assignedAfter} then the runs; REPLICA is a count of journal entries,
 * each its byte length u32 then the whole journal record (ADR-0082 §4).
 */
public final class FastWriteFrame {

    public static final int KIND_COMMIT = 1;
    public static final int KIND_ASSIGNED = 2;
    public static final int KIND_CONFIRM = 3;
    public static final int KIND_EXPOSED = 4;
    public static final int KIND_REPLICA = 5;
    public static final int KIND_REPLICA_ACK = 6;

    private FastWriteFrame() {
    }

    /** One run of a COMMIT: a stream and its records. */
    public record CommitRun(RunKey stream, List<SegmentRecord> records) {
        public CommitRun {
            Objects.requireNonNull(stream, "stream");
            records = List.copyOf(records);
            if (records.isEmpty()) {
                throw new IllegalArgumentException("a run commits at least one record");
            }
        }
    }

    /** COMMIT, writer to leader. */
    public record Commit(FastJournalRecord.IdempotencyKey key, List<CommitRun> runs)
            implements FastFrame.Body {
        public Commit {
            Objects.requireNonNull(key, "key");
            runs = List.copyOf(runs);
            if (runs.isEmpty()) {
                throw new IllegalArgumentException("a COMMIT carries at least one run");
            }
        }

        @Override
        public int kind() {
            return KIND_COMMIT;
        }
    }

    /** One run as ASSIGNED answers it: everything the writer's journal entry needs. */
    public record AssignedRun(RunKey stream, long firstOffset, int walQuorum,
            boolean copyRequired) {
        public AssignedRun {
            Objects.requireNonNull(stream, "stream");
            if (firstOffset < 0 || walQuorum < 1 || walQuorum > 3) {
                throw new IllegalArgumentException("an assigned run is at a non-negative "
                        + "offset under a q of 1 to 3");
            }
        }
    }

    /** ASSIGNED, leader to writer. */
    public record Assigned(long assignedAfter, List<AssignedRun> runs) implements FastFrame.Body {
        public Assigned {
            requireU32(assignedAfter);
            runs = List.copyOf(runs);
        }

        @Override
        public int kind() {
            return KIND_ASSIGNED;
        }
    }

    /** A run named by its stream and first offset. */
    public record RunAt(RunKey stream, long firstOffset) {
        public RunAt {
            Objects.requireNonNull(stream, "stream");
            if (firstOffset < 0) {
                throw new IllegalArgumentException("an offset is never negative");
            }
        }
    }

    /** CONFIRM, writer to leader: its copy journaled with the offsets, or not. */
    public record Confirm(FastJournalRecord.IdempotencyKey key, long assignedAfter,
            List<RunAt> runs, boolean copyJournaled) implements FastFrame.Body {
        public Confirm {
            Objects.requireNonNull(key, "key");
            requireU32(assignedAfter);
            runs = List.copyOf(runs);
        }

        @Override
        public int kind() {
            return KIND_CONFIRM;
        }
    }

    /** EXPOSED, leader to writer: the batch's quorum is complete and exposed. */
    public record Exposed(long assignedAfter, List<RunAt> runs) implements FastFrame.Body {
        public Exposed {
            requireU32(assignedAfter);
            runs = List.copyOf(runs);
        }

        @Override
        public int kind() {
            return KIND_EXPOSED;
        }
    }

    /** REPLICA, leader to holder: one batch's journal entries, one per run. */
    public record Replica(List<FastJournalRecord.Entry> entries) implements FastFrame.Body {
        public Replica {
            entries = List.copyOf(entries);
            if (entries.isEmpty()) {
                throw new IllegalArgumentException("a REPLICA carries at least one entry");
            }
        }

        @Override
        public int kind() {
            return KIND_REPLICA;
        }
    }

    /** REPLICA_ACK, holder to leader, once its group is fsynced. */
    public record ReplicaAck(long assignedAfter, List<RunAt> runs) implements FastFrame.Body {
        public ReplicaAck {
            requireU32(assignedAfter);
            runs = List.copyOf(runs);
        }

        @Override
        public int kind() {
            return KIND_REPLICA_ACK;
        }
    }

    private static void requireU32(long value) {
        if (value < 0 || value > FastFrame.MAX_U32) {
            throw new IllegalArgumentException("assignedAfter is a u32: " + value);
        }
    }

    static void encode(ByteArrayOutputStream out, FastFrame.Body body) {
        switch (body) {
            case Commit c -> {
                key(out, c.key());
                FastFrame.u32(out, c.runs().size());
                for (CommitRun r : c.runs()) {
                    FastFrame.runKey(out, r.stream());
                    byte[] records = SegmentWriter.encodeRecords(r.records());
                    FastFrame.u32(out, r.records().size());
                    FastFrame.u32(out, records.length);
                    out.writeBytes(records);
                }
            }
            case Assigned a -> {
                FastFrame.u32(out, a.assignedAfter());
                FastFrame.u32(out, a.runs().size());
                for (AssignedRun r : a.runs()) {
                    FastFrame.runKey(out, r.stream());
                    FastFrame.i64(out, r.firstOffset());
                    out.write(r.walQuorum());
                    out.write(r.copyRequired() ? 1 : 0);
                }
            }
            case Confirm c -> {
                key(out, c.key());
                FastFrame.u32(out, c.assignedAfter());
                runs(out, c.runs());
                out.write(c.copyJournaled() ? 1 : 0);
            }
            case Exposed e -> {
                FastFrame.u32(out, e.assignedAfter());
                runs(out, e.runs());
            }
            case Replica r -> {
                FastFrame.u32(out, r.entries().size());
                for (FastJournalRecord.Entry e : r.entries()) {
                    byte[] bytes = e.encode();
                    FastFrame.u32(out, bytes.length);
                    out.writeBytes(bytes);
                }
            }
            case ReplicaAck a -> {
                FastFrame.u32(out, a.assignedAfter());
                runs(out, a.runs());
            }
            default -> throw new IllegalArgumentException("not a write frame: " + body.kind());
        }
    }

    static FastFrame.Body decode(int kind, Cursor c) throws IOException {
        return switch (kind) {
            case KIND_COMMIT -> {
                FastJournalRecord.IdempotencyKey key = key(c);
                long n = FastFrame.count(c, 28);
                List<CommitRun> runs = new ArrayList<>();
                for (long i = 0; i < n; i++) {
                    RunKey stream = FastFrame.runKey(c);
                    long count = FastFrame.u32(c);
                    long length = FastFrame.u32(c);
                    if (length > c.remaining() || count > length) {
                        throw new IOException("a COMMIT run overruns the frame");
                    }
                    runs.add(new CommitRun(stream, decodeRecords(c.bytes((int) length),
                            (int) count)));
                }
                yield new Commit(key, runs);
            }
            case KIND_ASSIGNED -> {
                long assignedAfter = FastFrame.u32(c);
                long n = FastFrame.count(c, 30);
                List<AssignedRun> runs = new ArrayList<>();
                for (long i = 0; i < n; i++) {
                    RunKey stream = FastFrame.runKey(c);
                    long first = FastFrame.i64(c);
                    int q = c.bytes(1)[0] & 0xFF;
                    runs.add(new AssignedRun(stream, first, q, flag(c)));
                }
                yield new Assigned(assignedAfter, runs);
            }
            case KIND_CONFIRM -> {
                FastJournalRecord.IdempotencyKey key = key(c);
                long assignedAfter = FastFrame.u32(c);
                List<RunAt> runs = runs(c);
                yield new Confirm(key, assignedAfter, runs, flag(c));
            }
            case KIND_EXPOSED -> new Exposed(FastFrame.u32(c), runs(c));
            case KIND_REPLICA -> {
                long n = FastFrame.count(c, 4 + FastJournalRecord.HEADER_BYTES);
                List<FastJournalRecord.Entry> entries = new ArrayList<>();
                for (long i = 0; i < n; i++) {
                    long length = FastFrame.u32(c);
                    if (length > c.remaining()) {
                        throw new IOException("a REPLICA entry overruns the frame");
                    }
                    entries.add(entry(c.bytes((int) length)));
                }
                yield new Replica(entries);
            }
            case KIND_REPLICA_ACK -> new ReplicaAck(FastFrame.u32(c), runs(c));
            default -> throw new IOException("fast frame kind " + kind
                    + " is not readable by this build");
        };
    }

    /** The run's records, which must fill its length exactly (the reader refuses a tail). */
    private static List<SegmentRecord> decodeRecords(byte[] bytes, int count)
            throws IOException {
        return SegmentReader.decodeRecords(bytes, count);
    }

    private static FastJournalRecord.Entry entry(byte[] bytes) throws IOException {
        FastJournalRecord.Recovered read = FastJournalRecord.decodeAll(bytes);
        if (read.unreadable() || read.validLength() != bytes.length
                || read.records().size() != 1
                || !(read.records().get(0) instanceof FastJournalRecord.Entry e)) {
            throw new IOException("a REPLICA entry is not one whole journal entry");
        }
        return e;
    }

    private static boolean flag(Cursor c) throws IOException {
        int b = c.bytes(1)[0] & 0xFF;
        if (b > 1) {
            throw new IOException("a flag is 0 or 1, not " + b);
        }
        return b == 1;
    }

    private static void key(ByteArrayOutputStream out, FastJournalRecord.IdempotencyKey key) {
        FastFrame.string(out, key.podId());
        FastFrame.i64(out, key.incarnationId().getMostSignificantBits());
        FastFrame.i64(out, key.incarnationId().getLeastSignificantBits());
        FastFrame.i64(out, key.fastSeq());
    }

    private static FastJournalRecord.IdempotencyKey key(Cursor c) throws IOException {
        return new FastJournalRecord.IdempotencyKey(FastFrame.string(c),
                new UUID(FastFrame.i64(c), FastFrame.i64(c)), FastFrame.i64(c));
    }

    private static void runs(ByteArrayOutputStream out, List<RunAt> runs) {
        FastFrame.u32(out, runs.size());
        for (RunAt r : runs) {
            FastFrame.runKey(out, r.stream());
            FastFrame.i64(out, r.firstOffset());
        }
    }

    private static List<RunAt> runs(Cursor c) throws IOException {
        long n = FastFrame.count(c, 28);
        List<RunAt> runs = new ArrayList<>();
        for (long i = 0; i < n; i++) {
            runs.add(new RunAt(FastFrame.runKey(c), FastFrame.i64(c)));
        }
        return runs;
    }
}
