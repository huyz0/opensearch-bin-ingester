// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * A pod-to-pod fast frame (ADR-0082 §2; M13.26d lands the header and the JOIN,
 * JOINED and REFUSED kinds with their first reader and writer, M13.26h the
 * DEPART, HELD and HELD_STATUS kinds, M13.27e the write's six in
 * {@link FastWriteFrame}, the rest landing with theirs, M13.25b).
 *
 * <p>Header: magic {@code 0x42465354} ("BFST") u32, version 1 u8, kind u8,
 * {@code epoch} i64, the sender's and the target's pod UID (each a uvarint
 * length then UTF-8). Every field after it is big-endian and fixed-width but
 * the strings, as the table in ADR-0082 §2 gives them; a RunKey is the index
 * UUID (two i64) and the partition (i32), a count a u32.
 *
 * <p>⚠️ THE HEADER FIRST, SO THE FENCE FIRST: {@link #header} reads only it,
 * so a receiver applies its epoch fence (ADR-0081 §3) and refuses a frame
 * addressed to another incarnation before it reads anything else.
 *
 * <p>⚠️ A KIND THIS BUILD CANNOT READ IS REFUSED, never skipped: a frame's
 * meaning is its kind, and a reader that guessed would act on a frame nobody
 * sent. Trailing bytes are refused for the same reason.
 */
public final class FastFrame {

    public static final int MAGIC = 0x42465354;
    public static final int VERSION = 1;
    public static final int KIND_REFUSED = 14;
    public static final int KIND_JOIN = 12;
    public static final int KIND_DEPART = 13;
    public static final int KIND_HELD = 16;
    public static final int KIND_HELD_STATUS = 17;
    public static final int KIND_JOINED = 15;

    /**
     * Whether {@code kind} is a control frame (M13.66, ADR-0081 §12): the
     * per-pod, per-term JOIN, JOINED, DEPART, HELD and HELD_STATUS. Every other
     * kind carries a write's data.
     */
    public static boolean isControl(int kind) {
        return kind == KIND_JOIN || kind == KIND_JOINED || kind == KIND_DEPART
                || kind == KIND_HELD || kind == KIND_HELD_STATUS;
    }
    /** A pod UID or any other string here: generous for a Kubernetes UID or an endpoint. */
    public static final int MAX_STRING_BYTES = 1024;
    public static final long MAX_U32 = 0xFFFF_FFFFL;

    private FastFrame() {
    }

    /**
     * The header every fast frame carries.
     *
     * @param kind the frame's kind, 1 to 17
     * @param epoch the sender's fast epoch, never negative
     * @param senderUid the sending incarnation's pod UID
     * @param targetUid the incarnation this frame is addressed to
     */
    public record Header(int kind, long epoch, String senderUid, String targetUid) {
        public Header {
            Objects.requireNonNull(senderUid, "senderUid");
            Objects.requireNonNull(targetUid, "targetUid");
            if (kind < 1 || kind > 17) {
                throw new IllegalArgumentException("a fast frame's kind is 1 to 17: " + kind);
            }
            if (epoch < 0) {
                throw new IllegalArgumentException("an epoch is never negative: " + epoch);
            }
            requireUid(senderUid, "senderUid");
            requireUid(targetUid, "targetUid");
        }
    }

    /** A frame's body, by kind. */
    public sealed interface Body permits Join, Joined, Refused, Depart, HeldReport,
            HeldStatusReport, FastWriteFrame.Commit, FastWriteFrame.Assigned,
            FastWriteFrame.Confirm, FastWriteFrame.Exposed, FastWriteFrame.Replica,
            FastWriteFrame.ReplicaAck {
        int kind();
    }

    /** One held group of a stream: an {@code (epoch, assignedAfter)} and its offsets. */
    public record HeldGroup(long epoch, long assignedAfter, long lowest, long highest) {
        public HeldGroup {
            if (epoch < 0 || assignedAfter < 0 || assignedAfter > MAX_U32 || lowest < 0
                    || highest < lowest) {
                throw new IllegalArgumentException("a held group is an epoch, a u32 "
                        + "assignedAfter and lowest <= highest offsets, all non-negative");
            }
        }
    }

    /** What a pod's journal holds of one stream. */
    public record HeldStream(RunKey stream, List<HeldGroup> groups) {
        public HeldStream {
            Objects.requireNonNull(stream, "stream");
            groups = List.copyOf(groups);
        }
    }

    /** A HELD body: every stream a pod's journal holds an entry of. */
    public record Held(List<HeldStream> streams) {
        public static final Held NONE = new Held(List.of());

        public Held {
            streams = List.copyOf(streams);
        }
    }

    /** How the leader answers one held group (ADR-0082 §2, HELD_STATUS). */
    public enum Status {
        COMMITTED(1), SUPERSEDED(2), CLOSED_TERM(3), PENDING(4);

        final int code;

        Status(int code) {
            this.code = code;
        }
    }

    /** One group's answer: where a decision supersedes it, and the status below that. */
    public record GroupStatus(long epoch, long assignedAfter, long supersededFrom,
            Status status) {
        public GroupStatus {
            Objects.requireNonNull(status, "status");
            if (epoch < 0 || assignedAfter < 0 || assignedAfter > MAX_U32
                    || supersededFrom < 0) {
                throw new IllegalArgumentException("a group status is non-negative, its "
                        + "assignedAfter a u32");
            }
        }
    }

    /** One stream's answer: its committed next offset, and each reported group's. */
    public record StreamStatus(RunKey stream, long releaseBelow, List<GroupStatus> groups) {
        public StreamStatus {
            Objects.requireNonNull(stream, "stream");
            groups = List.copyOf(groups);
            if (releaseBelow < 0) {
                throw new IllegalArgumentException("releaseBelow is never negative");
            }
        }
    }

    /** A HELD_STATUS body. */
    public record HeldStatus(List<StreamStatus> streams) {
        public static final HeldStatus NONE = new HeldStatus(List.of());

        public HeldStatus {
            streams = List.copyOf(streams);
        }
    }

    /** JOIN, pod to leader: its incarnation and what its journal still holds. */
    public record Join(Roster.Incarnation incarnation, Held held) implements Body {
        public Join {
            Objects.requireNonNull(incarnation, "incarnation");
            Objects.requireNonNull(held, "held");
            // ⚠️ THE DECODER'S LIMIT, HERE TOO (M13.26d review round 1, P2): a
            // JOIN every leader refuses as malformed would never join a term.
            requireString(incarnation.podId(), "podId");
            requireString(incarnation.podUid(), "podUid");
            requireString(incarnation.az(), "az");
            requireString(incarnation.endpoint(), "endpoint");
        }

        @Override
        public int kind() {
            return KIND_JOIN;
        }
    }

    /**
     * DEPART, pod to leader (ADR-0081 §9): phase 1 asks for an upload and
     * reports what the pod holds; phase 2 asks to be marked departed, and
     * carries nothing more.
     */
    public record Depart(Roster.Incarnation incarnation, int phase, Held held) implements Body {
        public Depart {
            Objects.requireNonNull(incarnation, "incarnation");
            Objects.requireNonNull(held, "held");
            if (phase != 1 && phase != 2) {
                throw new IllegalArgumentException("a departure's phase is 1 or 2: " + phase);
            }
            if (phase == 2 && !held.streams().isEmpty()) {
                throw new IllegalArgumentException("phase 2 reports nothing held");
            }
            requireString(incarnation.podId(), "podId");
            requireString(incarnation.podUid(), "podUid");
            requireString(incarnation.az(), "az");
            requireString(incarnation.endpoint(), "endpoint");
        }

        @Override
        public int kind() {
            return KIND_DEPART;
        }
    }

    /** HELD, holder to leader: what the pod's journal holds. */
    public record HeldReport(Held held) implements Body {
        public HeldReport {
            Objects.requireNonNull(held, "held");
        }

        @Override
        public int kind() {
            return KIND_HELD;
        }
    }

    /**
     * HELD_STATUS, leader to holder: the answer to a HELD or a DEPART. A
     * JOIN's report is answered inside JOINED, never by this.
     */
    public record HeldStatusReport(HeldStatus status) implements Body {
        public HeldStatusReport {
            Objects.requireNonNull(status, "status");
        }

        @Override
        public int kind() {
            return KIND_HELD_STATUS;
        }
    }

    /** JOINED, leader to pod: the highest closed epoch, and the reported streams' status. */
    public record Joined(long closedThrough, HeldStatus status) implements Body {
        public Joined {
            Objects.requireNonNull(status, "status");
            if (closedThrough < 0) {
                throw new IllegalArgumentException("closedThrough is never negative");
            }
        }

        @Override
        public int kind() {
            return KIND_JOINED;
        }
    }

    /** Why a frame was refused. */
    public enum Reason {
        LOWER_EPOCH(1), NOT_ROSTERED(2), NOT_FAST(3), BACKPRESSURE(4), DEPARTING(5),
        DISCARDED(6);

        final int code;

        Reason(int code) {
            this.code = code;
        }
    }

    /**
     * REFUSED, any answer: a reason, the discarded batch's idempotency key
     * exactly when the reason is {@code DISCARDED}, and text.
     */
    public record Refused(Reason reason, Optional<FastJournalRecord.IdempotencyKey> discarded,
            String text) implements Body {
        public Refused {
            Objects.requireNonNull(reason, "reason");
            Objects.requireNonNull(discarded, "discarded");
            Objects.requireNonNull(text, "text");
            if ((reason == Reason.DISCARDED) != discarded.isPresent()) {
                throw new IllegalArgumentException("a discard names its batch's key, and only a "
                        + "discard does");
            }
            requireString(text, "text");
            discarded.ifPresent(k -> requireString(k.podId(), "podId"));
        }

        @Override
        public int kind() {
            return KIND_REFUSED;
        }
    }

    /** A decoded frame. */
    public record Frame(Header header, Body body) {
        public Frame {
            Objects.requireNonNull(header, "header");
            Objects.requireNonNull(body, "body");
            if (header.kind() != body.kind()) {
                throw new IllegalArgumentException("the header's kind is the body's");
            }
        }
    }

    private static void requireUid(String value, String what) {
        requireString(value, what);
        if (value.isBlank()) {
            throw new IllegalArgumentException(what + " is never blank");
        }
    }

    private static void requireString(String value, String what) {
        if (value.getBytes(StandardCharsets.UTF_8).length > MAX_STRING_BYTES) {
            throw new IllegalArgumentException(what + " is at most " + MAX_STRING_BYTES
                    + " bytes");
        }
    }

    /** The frame's bytes. */
    public static byte[] encode(long epoch, String senderUid, String targetUid, Body body) {
        Header header = new Header(body.kind(), epoch, senderUid, targetUid);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(ByteBuffer.allocate(6).putInt(MAGIC).put((byte) VERSION)
                .put((byte) header.kind()).array());
        i64(out, epoch);
        string(out, senderUid);
        string(out, targetUid);
        switch (body) {
            case Join join -> {
                incarnation(out, join.incarnation());
                held(out, join.held());
            }
            case Joined joined -> {
                i64(out, joined.closedThrough());
                status(out, joined.status());
            }
            case Depart depart -> {
                incarnation(out, depart.incarnation());
                out.write(depart.phase());
                if (depart.phase() == 1) {
                    held(out, depart.held());
                }
            }
            case HeldReport report -> held(out, report.held());
            case HeldStatusReport report -> status(out, report.status());
            case FastWriteFrame.Commit b -> FastWriteFrame.encode(out, b);
            case FastWriteFrame.Assigned b -> FastWriteFrame.encode(out, b);
            case FastWriteFrame.Confirm b -> FastWriteFrame.encode(out, b);
            case FastWriteFrame.Exposed b -> FastWriteFrame.encode(out, b);
            case FastWriteFrame.Replica b -> FastWriteFrame.encode(out, b);
            case FastWriteFrame.ReplicaAck b -> FastWriteFrame.encode(out, b);
            case Refused refused -> {
                out.write(refused.reason().code);
                refused.discarded().ifPresent(k -> {
                    string(out, k.podId());
                    i64(out, k.incarnationId().getMostSignificantBits());
                    i64(out, k.incarnationId().getLeastSignificantBits());
                    i64(out, k.fastSeq());
                });
                string(out, refused.text());
            }
        }
        return out.toByteArray();
    }

    /** Reads only the header, so the fence and the addressee are checked first. */
    public static Header header(byte[] bytes) throws IOException {
        return readHeader(new Cursor(Objects.requireNonNull(bytes, "bytes"), 0, "fast frame"));
    }

    /** Reads a whole frame of a kind this build reads. */
    public static Frame decode(byte[] bytes) throws IOException {
        Cursor c = new Cursor(Objects.requireNonNull(bytes, "bytes"), 0, "fast frame");
        Header header = readHeader(c);
        try {
            Body body = switch (header.kind()) {
                case KIND_JOIN -> new Join(incarnation(c), held(c));
                case KIND_JOINED -> new Joined(i64(c), heldStatus(c));
                case KIND_DEPART -> {
                    Roster.Incarnation i = incarnation(c);
                    int phase = c.bytes(1)[0] & 0xFF;
                    yield new Depart(i, phase, phase == 1 ? held(c) : Held.NONE);
                }
                case KIND_HELD -> new HeldReport(held(c));
                case KIND_HELD_STATUS -> new HeldStatusReport(heldStatus(c));
                case FastWriteFrame.KIND_COMMIT, FastWriteFrame.KIND_ASSIGNED,
                        FastWriteFrame.KIND_CONFIRM, FastWriteFrame.KIND_EXPOSED,
                        FastWriteFrame.KIND_REPLICA, FastWriteFrame.KIND_REPLICA_ACK ->
                        FastWriteFrame.decode(header.kind(), c);
                case KIND_REFUSED -> {
                    Reason reason = reason(c);
                    Optional<FastJournalRecord.IdempotencyKey> key = Optional.empty();
                    if (reason == Reason.DISCARDED) {
                        key = Optional.of(new FastJournalRecord.IdempotencyKey(string(c),
                                new UUID(i64(c), i64(c)), i64(c)));
                    }
                    yield new Refused(reason, key, string(c));
                }
                default -> throw new IOException("fast frame kind " + header.kind()
                        + " is not readable by this build");
            };
            if (!c.atEnd()) {
                throw new IOException("fast frame has " + c.remaining() + " trailing bytes");
            }
            return new Frame(header, body);
        } catch (IllegalArgumentException invalid) {
            throw new IOException("fast frame is not valid: " + invalid.getMessage(), invalid);
        }
    }

    private static Header readHeader(Cursor c) throws IOException {
        ByteBuffer head = ByteBuffer.wrap(c.bytes(6));
        int magic = head.getInt();
        if (magic != MAGIC) {
            throw new IOException("not a fast frame: magic 0x" + Integer.toHexString(magic));
        }
        int version = head.get() & 0xFF;
        if (version != VERSION) {
            throw new IOException("fast frame version " + version + " is not readable by this "
                    + "build, which knows " + VERSION);
        }
        int kind = head.get() & 0xFF;
        try {
            return new Header(kind, i64(c), string(c), string(c));
        } catch (IllegalArgumentException invalid) {
            throw new IOException("fast frame header is not valid: " + invalid.getMessage(),
                    invalid);
        }
    }

    private static void incarnation(ByteArrayOutputStream out, Roster.Incarnation i) {
        string(out, i.podId());
        string(out, i.podUid());
        string(out, i.az());
        string(out, i.endpoint());
    }

    private static void held(ByteArrayOutputStream out, Held held) {
        u32(out, held.streams().size());
        for (HeldStream s : held.streams()) {
            runKey(out, s.stream());
            u32(out, s.groups().size());
            for (HeldGroup g : s.groups()) {
                i64(out, g.epoch());
                u32(out, g.assignedAfter());
                i64(out, g.lowest());
                i64(out, g.highest());
            }
        }
    }

    private static void status(ByteArrayOutputStream out, HeldStatus status) {
        u32(out, status.streams().size());
        for (StreamStatus s : status.streams()) {
            runKey(out, s.stream());
            i64(out, s.releaseBelow());
            u32(out, s.groups().size());
            for (GroupStatus g : s.groups()) {
                i64(out, g.epoch());
                u32(out, g.assignedAfter());
                i64(out, g.supersededFrom());
                out.write(g.status().code);
            }
        }
    }

    private static Roster.Incarnation incarnation(Cursor c) throws IOException {
        return new Roster.Incarnation(string(c), string(c), string(c), string(c));
    }

    private static Held held(Cursor c) throws IOException {
        long streams = count(c, 24);
        List<HeldStream> held = new ArrayList<>();
        for (long s = 0; s < streams; s++) {
            RunKey key = runKey(c);
            long groups = count(c, 28);
            List<HeldGroup> list = new ArrayList<>();
            for (long g = 0; g < groups; g++) {
                list.add(new HeldGroup(i64(c), u32(c), i64(c), i64(c)));
            }
            held.add(new HeldStream(key, list));
        }
        return new Held(held);
    }

    private static HeldStatus heldStatus(Cursor c) throws IOException {
        long streams = count(c, 32);
        List<StreamStatus> status = new ArrayList<>();
        for (long s = 0; s < streams; s++) {
            RunKey key = runKey(c);
            long releaseBelow = i64(c);
            long groups = count(c, 21);
            List<GroupStatus> list = new ArrayList<>();
            for (long g = 0; g < groups; g++) {
                list.add(new GroupStatus(i64(c), u32(c), i64(c), status(c)));
            }
            status.add(new StreamStatus(key, releaseBelow, list));
        }
        return new HeldStatus(status);
    }

    static void i64(ByteArrayOutputStream out, long value) {
        out.writeBytes(ByteBuffer.allocate(8).putLong(value).array());
    }

    static void u32(ByteArrayOutputStream out, long value) {
        out.writeBytes(ByteBuffer.allocate(4).putInt((int) value).array());
    }

    static void string(ByteArrayOutputStream out, String value) {
        byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
        SegmentWriter.putUvarint(out, utf8.length);
        out.writeBytes(utf8);
    }

    static void runKey(ByteArrayOutputStream out, RunKey key) {
        i64(out, key.indexId().getMostSignificantBits());
        i64(out, key.indexId().getLeastSignificantBits());
        out.writeBytes(ByteBuffer.allocate(4).putInt(key.partitionId()).array());
    }

    static long i64(Cursor c) throws IOException {
        return ByteBuffer.wrap(c.bytes(8)).getLong();
    }

    static long u32(Cursor c) throws IOException {
        return ByteBuffer.wrap(c.bytes(4)).getInt() & MAX_U32;
    }

    /** A count, bounded by the bytes left at {@code minBytes} per element. */
    static long count(Cursor c, int minBytes) throws IOException {
        long n = u32(c);
        if (n > c.remaining() / minBytes) {
            throw new IOException("fast frame claims " + n + " elements with only "
                    + c.remaining() + " bytes left");
        }
        return n;
    }

    static String string(Cursor c) throws IOException {
        long length = c.uvarint();
        if (length < 0 || length > MAX_STRING_BYTES || length > c.remaining()) {
            throw new IOException("fast frame claims a " + length + "-byte string");
        }
        return new String(c.bytes((int) length), StandardCharsets.UTF_8);
    }

    static RunKey runKey(Cursor c) throws IOException {
        UUID index = new UUID(i64(c), i64(c));
        int partition = ByteBuffer.wrap(c.bytes(4)).getInt();
        if (partition < 0) {
            throw new IOException("fast frame names a negative partition");
        }
        return new RunKey(index, partition);
    }

    private static Status status(Cursor c) throws IOException {
        int code = c.bytes(1)[0] & 0xFF;
        for (Status s : Status.values()) {
            if (s.code == code) {
                return s;
            }
        }
        throw new IOException("fast frame names an unknown status " + code);
    }

    private static Reason reason(Cursor c) throws IOException {
        int code = c.bytes(1)[0] & 0xFF;
        for (Reason r : Reason.values()) {
            if (r.code == code) {
                return r;
            }
        }
        throw new IOException("fast frame names an unknown reason " + code);
    }
}
