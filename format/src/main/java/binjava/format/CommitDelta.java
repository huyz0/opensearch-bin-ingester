// SPDX-License-Identifier: Apache-2.0
package binjava.format;

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
 * One entry in the commit log: a segment became part of the log, and these
 * streams gained these offsets.
 *
 * <p>⚠️ WRITTEN WITH putIfAbsent AT A SEQUENTIAL SLOT (ADR-0002). That is the
 * only coordination this system has: the store decides who wins slot N, the
 * loser re-reads and retries at N+1, and invariant I1 — no seq is ever written
 * twice — holds without a lock, a lease or a consensus round.
 *
 * <p>⚠️ M1 writes slot 0 and epoch 0. Leases, epochs and the seal are M4; their
 * SLOTS are in the key grammar from the first object so that adding them is not
 * a key-grammar change. // SKELETON: slot/epoch fixed until M4
 */
public record CommitDelta(long sequence, List<SegmentCommit> segments)
        implements ChainEntry {

    public CommitDelta {
        if (sequence < 0) {
            throw new IllegalArgumentException("sequence is never negative: " + sequence);
        }
        segments = List.copyOf(Objects.requireNonNull(segments, "segments"));
        if (segments.isEmpty()) {
            // ⚠️ An empty delta would consume a sequence number and commit
            // nothing, so a replay would see a gap it cannot explain.
            throw new IllegalArgumentException("a delta with no segments commits nothing");
        }
        // ⚠️ RUNG 1: attribution rests on this, so it is made unrepresentable
        // rather than checked on the way in. A caller finds ITS offsets by the
        // segment key it submitted (see `Sequencer.commit`), and
        // `CommitLog.commitAll` refuses a batch whose submissions collide — but
        // enforcing it only there leaves the invariant unable to survive the
        // bytes: a delta with duplicate keys arriving from a torn object or a
        // foreign writer would decode without complaint, and the caller's
        // `findFirst` would silently take the wrong segment's offsets.
        Set<String> keys = new HashSet<>();
        for (SegmentCommit s : segments) {
            if (!keys.add(s.segmentKey())) {
                throw new IllegalArgumentException(
                        "a delta names the same segment twice, so no caller could tell "
                                + "which offsets are its own: " + s.segmentKey());
            }
        }
    }

    /**
     * One segment's worth — the shape every caller wrote before ADR-0032, and
     * still the canonical one for a window that carried a single flush.
     *
     * <p>⚠️ NOT a "default": the segment is named, explicitly, by the caller.
     * What ADR-0032 removed is a delta that can name only one, not the ability
     * to build one that names exactly one.
     */
    public CommitDelta(long sequence, String segmentKey, List<RunCommit> runs) {
        this(sequence, List.of(new SegmentCommit(segmentKey, runs)));
    }

    /**
     * The single segment this delta names.
     *
     * @throws IllegalStateException if it names several — ⚠️ DELIBERATELY, and
     *     this refusal is the point. A batched delta carries many pods' flushes,
     *     and a caller that asks a multi-segment delta for "the" segment key has
     *     a bug whose symptom is a consumer fetching the WRONG OBJECT: silent,
     *     data-corrupting, and invisible in every test that happens to batch one
     *     flush. Returning the first segment, or any segment, would hide exactly
     *     that. Callers that must handle both use {@link #segments()}.
     */
    public String segmentKey() {
        return only().segmentKey();
    }

    /**
     * The runs of the single segment this delta names.
     *
     * @throws IllegalStateException if it names several — see {@link
     *     #segmentKey()}. ⚠️ A flat view across segments would divorce each run
     *     from the object holding its records, which is the one pairing
     *     ADR-0032 exists to keep. {@link #allRuns()} is for callers that
     *     genuinely do not care.
     */
    public List<RunCommit> runs() {
        return only().runs();
    }

    private SegmentCommit only() {
        if (segments.size() != 1) {
            throw new IllegalStateException(
                    "this delta batches " + segments.size() + " segments; ask for segments() and "
                            + "pair each run with its own, or the records are read from the "
                            + "wrong object");
        }
        return segments.get(0);
    }

    /**
     * Every run across every segment, for callers that genuinely do not care
     * which object holds the records.
     *
     * <p>⚠️ THERE IS EXACTLY ONE SUCH CALLER TODAY and it is worth naming:
     * {@code ChainReplay.fold}, which folds offsets forward to learn where each
     * stream has reached. An offset is a stream fact, not a segment fact, so the
     * pairing is genuinely irrelevant there. Anything that DELIVERS records —
     * {@code SubscriptionHub} — must use {@link #segments()} instead.
     */
    public List<RunCommit> allRuns() {
        List<RunCommit> all = new ArrayList<>();
        for (SegmentCommit s : segments) {
            all.addAll(s.runs());
        }
        return List.copyOf(all);
    }

    /**
     * ⚠️ The commit log is a wire format too: non-negotiable 8 applies.
     *
     * <p>⚠️ ENCODING IS CANONICAL AND VERSION-SELECTING (ADR-0032): one segment
     * emits **v0**, byte-for-byte what every delta written before M4.7 says;
     * two or more emit **v1 kinded, kind 0**, the kind ADR-0028's implementation
     * reserved in as many words for "a FUTURE delta layout … at which point v0
     * and it are genuinely different shapes". This is that layout.
     */
    @Override
    public byte[] encode() {
        // ⚠️ ATTRIBUTION SELECTS THE LAYOUT, never mutates an existing one: a
        // delta carrying none encodes byte-for-byte as it did before ADR-0036,
        // which is what keeps both goldens and every bucket readable.
        boolean attributed = false;
        for (SegmentCommit s : segments) {
            if (s.attribution() != null) {
                attributed = true;
                break;
            }
        }
        if (attributed) {
            ByteArrayOutputStream out = ChainEntry.kinded(ChainEntry.KIND_DELTA_ATTRIBUTED);
            SegmentWriter.putUvarint(out, sequence);
            SegmentWriter.putUvarint(out, segments.size());
            for (SegmentCommit s : segments) {
                writeSegment(out, s);
                writeAttribution(out, s.attribution());
            }
            return out.toByteArray();
        }
        if (segments.size() == 1) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            out.writeBytes(ChainEntry.header(ChainEntry.VERSION_DELTA));
            SegmentWriter.putUvarint(out, sequence);
            writeSegment(out, segments.get(0));
            return out.toByteArray();
        }
        ByteArrayOutputStream out = ChainEntry.kinded(ChainEntry.KIND_DELTA);
        SegmentWriter.putUvarint(out, sequence);
        SegmentWriter.putUvarint(out, segments.size());
        for (SegmentCommit s : segments) {
            writeSegment(out, s);
        }
        return out.toByteArray();
    }

    /**
     * ⚠️ PER SEGMENT, immediately after that segment's runs. Written once per
     * DELTA instead, every fixture in the tree still round-trips -- no
     * production delta is multi-segment -- and M5's first forwarded multi-pod
     * batch drops every pod but one.
     */
    private static void writeAttribution(ByteArrayOutputStream out,
            SegmentCommit.Attribution a) {
        if (a == null) {
            throw new IllegalStateException(
                    "an attributed delta cannot mix attributed and unattributed segments");
        }
        byte[] pod = a.podId().getBytes(StandardCharsets.UTF_8);
        SegmentWriter.putUvarint(out, pod.length);
        out.writeBytes(pod);
        byte[] inc = a.incarnationId().getBytes(StandardCharsets.UTF_8);
        SegmentWriter.putUvarint(out, inc.length);
        out.writeBytes(inc);
        SegmentWriter.putUvarint(out, a.flushSeq());
    }

    private static int attributionLen(Cursor c, String field) throws IOException {
        long len = c.uvarint();
        if (len < 0 || len > c.remaining()) {
            throw new IOException("a delta claims a " + len + "-byte " + field
                    + " but only " + c.remaining() + " bytes remain");
        }
        return (int) len;
    }

    static CommitDelta decodeAttributedBody(Cursor c) throws IOException {
        long sequence = c.uvarint();
        long count = c.uvarint();
        if (count <= 0 || count > c.remaining()) {
            throw new IOException("delta claims " + count + " segments");
        }
        List<SegmentCommit> segments = new ArrayList<>((int) count);
        for (long i = 0; i < count; i++) {
            SegmentCommit bare = readSegment(c);
            // ⚠️ BOUND BEFORE THE CAST, both fields. The length is 64-bit on the
            // wire and this cast is where it stops being: `0x1_0000_0004`
            // narrows to 4 -- a VALID length -- so an unbounded read ACCEPTS the
            // object carrying the wrong pod, and a negative-reading value
            // narrows to a positive int the same way. Measured on the sibling
            // fields in `Checkpoint` and recorded there; this kind inherited the
            // norm and, until now, none of the guards.
            String pod = new String(c.bytes(attributionLen(c, "podId")), StandardCharsets.UTF_8);
            String inc = new String(c.bytes(attributionLen(c, "incarnationId")),
                    StandardCharsets.UTF_8);
            long flushSeq = c.uvarint();
            segments.add(new SegmentCommit(bare.segmentKey(), bare.runs(),
                    new SegmentCommit.Attribution(pod, inc, flushSeq)));
        }
        return new CommitDelta(sequence, segments);
    }

    /**
     * ⚠️ ONE WRITER FOR EVERY VERSION, so the per-segment bytes cannot drift
     * apart. The kinded batch is v0's body repeated under a count, and the
     * attributed kind is that with a triple after each segment — two copies of
     * this loop is how they would stop agreeing on a field nobody re-read.
     */
    private static void writeSegment(ByteArrayOutputStream out, SegmentCommit segment) {
        byte[] key = segment.segmentKey().getBytes(StandardCharsets.UTF_8);
        SegmentWriter.putUvarint(out, key.length);
        out.writeBytes(key);
        SegmentWriter.putUvarint(out, segment.runs().size());
        for (RunCommit r : segment.runs()) {
            ByteBuffer id = ByteBuffer.allocate(16).order(ByteOrder.BIG_ENDIAN);
            id.putLong(r.key().indexId().getMostSignificantBits());
            id.putLong(r.key().indexId().getLeastSignificantBits());
            out.writeBytes(id.array());
            SegmentWriter.putUvarint(out, r.key().partitionId());
            SegmentWriter.putUvarint(out, r.recordCount());
            SegmentWriter.putUvarint(out, r.firstOffset());
        }
    }

    /**
     * ⚠️ Narrows {@link ChainEntry#decode} to the delta case. A chain now
     * carries three shapes, so a caller that can only handle one must say so
     * and be refused rather than mis-cast.
     */
    public static CommitDelta decode(byte[] bytes) throws IOException {
        ChainEntry entry = ChainEntry.decode(bytes);
        if (entry instanceof CommitDelta delta) {
            return delta;
        }
        throw new IOException("expected a delta, found " + entry.getClass().getSimpleName());
    }

    /** ⚠️ Body only: {@link ChainEntry#decode} has consumed the v0 header. */
    static CommitDelta decodeBody(Cursor c) throws IOException {
        long sequence = c.uvarint();
        return new CommitDelta(sequence, List.of(readSegment(c)));
    }

    /**
     * ⚠️ Body only: {@link ChainEntry#decode} has consumed the v1 header AND the
     * kind. The batched layout of ADR-0032 — a segment COUNT, then that many of
     * exactly the bytes v0 carries once.
     */
    static CommitDelta decodeBatchedBody(Cursor c) throws IOException {
        long sequence = c.uvarint();
        long count = c.uvarint();
        if (count <= 0) {
            // ⚠️ Refused rather than left to the constructor, so the message
            // names the BYTES that were wrong. A zero here is a truncation or a
            // foreign writer, not a delta that happens to commit nothing.
            throw new IOException("a batched delta claims " + count + " segments");
        }
        // ⚠️ A COUNT MUST NOT BECOME AN ALLOCATION SIZE -- `Cursor`'s own header
        // states that invariant, and it was closed for length-prefixed fields
        // and left open for this repeat count. The smallest possible segment is
        // 2 bytes (a zero key length and a zero run count, both of which the
        // record then refuses), so a count exceeding what is left cannot be
        // honest. Measured before this guard: a 15-byte object claiming
        // 0x7FFFFFFF segments raised OutOfMemoryError -- past `recover`'s own
        // `throws IOException` and every caller catching it -- rather than
        // failing as the corrupt object it is.
        if (count > c.remaining()) {
            throw new IOException("a batched delta claims " + count + " segments but only "
                    + c.remaining() + " bytes remain");
        }
        List<SegmentCommit> segments = new ArrayList<>((int) count);
        for (long i = 0; i < count; i++) {
            segments.add(readSegment(c));
        }
        return new CommitDelta(sequence, segments);
    }

    /** ⚠️ ONE READER FOR BOTH VERSIONS, mirroring {@link #writeSegment}. */
    private static SegmentCommit readSegment(Cursor c) throws IOException {
        String key = new String(c.bytes((int) c.uvarint()), StandardCharsets.UTF_8);
        long runCount = c.uvarint();
        // ⚠️ THE SECOND REPEAT COUNT, bounded for the same reason as the first
        // and measured with the same result: on the bytes as they stood, a v0
        // delta claiming 0x7FFFFFFF runs raised `OutOfMemoryError: Requested
        // array size exceeds VM limit` out of `ChainEntry.decode`, past
        // `recover`'s own `throws IOException`. That predates ADR-0032 and is
        // reachable from the v0 path every bucket already holds, so it is fixed
        // here rather than left behind a comment claiming the class was closed:
        // the smallest run is 19 bytes, so a count exceeding the bytes left
        // cannot be honest.
        if (runCount < 0 || runCount > c.remaining()) {
            throw new IOException("a segment claims " + runCount + " runs but only "
                    + c.remaining() + " bytes remain");
        }
        List<RunCommit> runs = new ArrayList<>((int) runCount);
        for (long i = 0; i < runCount; i++) {
            byte[] id = c.bytes(16);
            ByteBuffer ib = ByteBuffer.wrap(id).order(ByteOrder.BIG_ENDIAN);
            RunKey rk = new RunKey(new java.util.UUID(ib.getLong(), ib.getLong()),
                    (int) c.uvarint());
            runs.add(new RunCommit(rk, (int) c.uvarint(), c.uvarint()));
        }
        return new SegmentCommit(key, runs);
    }
}
