// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A point the commit chain can be replayed FROM, rather than replayed to
 * (M4.8a).
 *
 * <p>⚠️ THE RECORD AND ITS CODEC ONLY. Nothing writes one — M4.8b2 decides when
 * a checkpoint is written and under which key, and M4.9 is what reads one to
 * bound recovery. The precedent for shipping a value type with no producer is
 * M4.1, which landed the {@code Sequencer} seam and its fake the same way.
 *
 * <p>⚠️ DELIBERATELY NOT THE OFFSET-TO-SEGMENT INDEX. That stays in the deltas.
 * It is the whole reason a checkpoint is tens of kilobytes at 1,600 streams
 * instead of growing with the chain's history, and a checkpoint that inlined it
 * would reintroduce the unbounded read M4.9 exists to remove.
 *
 * <p>⚠️ {@code oldestRetainedOffset} IS CARRIED FROM THE FIRST COMMIT although
 * nothing consumes it before M7, for the reason {@code CommitRequest} carried
 * {@code (podId, flushSeq)} from M4.1: adding a field later is a format change,
 * and this is the one commit where a format change costs nothing.
  *
 * <p>⚠️ {@code sequence} IS EXCLUSIVE — the next chain slot NOT covered by this
 * checkpoint — so a reader resumes by replaying deltas FROM {@code sequence}
 * forward, never from {@code sequence + 1}. It is stated here, on the record
 * itself, because the writer that chose it (M4.8b2's {@code CheckpointWriter})
 * is package-private in another module and M4.9 reads only these bytes. An
 * off-by-one here silently drops one delta's offsets, which is I2.
 */
public record Checkpoint(long sequence,
        Map<RunKey, StreamOffsets> streams,
        Map<String, PodState> pods) {

    /**
     * One pod's idempotency slot (ADR-0036): the incarnation, the watermark,
     * and a POINTER to the delta that last applied for that pod.
     *
     * <p>⚠️ THE POINTER IS WHAT MAKES A REPLAY ANSWERABLE. Without it,
     * detection is unbounded-depth while answering is bounded to the
     * uncheckpointed tail, since entries below the newest checkpoint are never
     * GET — the very defect that rejects bounding the window to the tail.
     *
     * <p>⚠️ THE INCARNATION IS A VALUE HERE, never a qualifier on the pod id:
     * qualifying the id adds an entry per restart, so the checkpoint would grow
     * with history against ADR-0033.
     *
     * <p>⚠️ {@code incarnationId} is null in a v0 checkpoint, which carried a
     * bare watermark and no pointer. {@code epoch} and {@code sequence} are
     * then {@code -1}, NOT zero: {@code (0, 0)} is a real delta address, and a
     * slot that looked addressable would answer a replay from the wrong object.
     */
    public record PodState(String incarnationId, long lastAppliedFlushSeq,
            long epoch, long sequence) {
        public PodState {
            if (lastAppliedFlushSeq < 0) {
                throw new IllegalArgumentException(
                        "lastAppliedFlushSeq is never negative: " + lastAppliedFlushSeq);
            }
            boolean pointed = epoch >= 0 && sequence >= 0;
            boolean absent = epoch == -1 && sequence == -1;
            if (!pointed && !absent) {
                throw new IllegalArgumentException(
                        "a pointer is both parts or neither: " + epoch + "/" + sequence);
            }
            if (incarnationId != null && incarnationId.isBlank()) {
                throw new IllegalArgumentException("incarnationId is never blank");
            }
            if ((incarnationId == null) != absent) {
                throw new IllegalArgumentException(
                        "an incarnation and a pointer travel together");
            }
        }

        /** A v0 slot: a watermark with no incarnation and no pointer. */
        public static PodState bare(long lastAppliedFlushSeq) {
            return new PodState(null, lastAppliedFlushSeq, -1, -1);
        }

        public boolean hasPointer() {
            return incarnationId != null;
        }
    }

    /** ⚠️ Its own magic: a checkpoint is not a chain entry and never decodes as one. */
    public static final int MAGIC = 0x42434B50;

    public static final int VERSION = 0;

    /**
     * A checkpoint whose pod slots carry an incarnation and a pointer
     * (ADR-0036). ⚠️ A NEW VERSION, because a version IS a layout: a v0 object
     * already in a bucket must keep decoding for the retention window, and
     * `golden/checkpoint-v0.bin` asserts its bytes.
     */
    public static final int VERSION_ATTRIBUTED = 1;

    /** Where a stream stands: the next offset to assign, and the oldest still kept. */
    public record StreamOffsets(long nextOffset, long oldestRetainedOffset) {
        public StreamOffsets {
            if (nextOffset < 0) {
                throw new IllegalArgumentException("nextOffset is never negative: " + nextOffset);
            }
            if (oldestRetainedOffset < 0) {
                throw new IllegalArgumentException(
                        "oldestRetainedOffset is never negative: " + oldestRetainedOffset);
            }
            if (oldestRetainedOffset > nextOffset) {
                throw new IllegalArgumentException("oldestRetainedOffset " + oldestRetainedOffset
                        + " is past nextOffset " + nextOffset);
            }
        }
    }

    /**
     * ⚠️ COPIED, AND THE COPY PRESERVES THE CALLER'S ORDER. A defensive copy is
     * the point; preserving order is what lets a test hand in entries in one
     * order and assert the ENCODER puts them in another, which is the only
     * deterministic way to catch a deleted sort. {@code Map.copyOf} would
     * randomise iteration per JVM and make that test flaky at 1 run in n!.
     */
    public Checkpoint {
        Objects.requireNonNull(streams, "streams");
        Objects.requireNonNull(pods, "pods");
        if (sequence < 0) {
            throw new IllegalArgumentException("sequence is never negative: " + sequence);
        }
        streams = Collections.unmodifiableMap(new LinkedHashMap<>(streams));
        pods = Collections.unmodifiableMap(new LinkedHashMap<>(pods));
        // ⚠️ BOTH MAPS, and an earlier draft validated only `pods`. A
        // `LinkedHashMap` copy accepts a null key or a null value where
        // `Map.copyOf` would refuse both, so the asymmetry turned a caller
        // error into an NPE raised later, inside `encode()`, on the write path.
        for (Map.Entry<RunKey, StreamOffsets> e : streams.entrySet()) {
            Objects.requireNonNull(e.getKey(), "a stream key");
            Objects.requireNonNull(e.getValue(), "stream offsets");
        }
        for (Map.Entry<String, PodState> p : pods.entrySet()) {
            if (p.getKey().isBlank()) {
                throw new IllegalArgumentException("a podId is never blank");
            }
            // ⚠️ PodState's own constructor checks the watermark and the
            // pointer-pair invariant; this loop still rejects a null VALUE,
            // which Map.copyOf would not reach through an unmodifiable view.
            Objects.requireNonNull(p.getValue(), "pod state for " + p.getKey());
        }
    }

    /**
     * ⚠️ SORTED, ALWAYS. The bytes are a function of the CONTENT, not of the
     * order a caller happened to build the maps in — two checkpoints carrying
     * the same facts encode identically, which is what makes a golden file
     * meaningful and a byte comparison a real assertion.
     */
    public byte[] encode() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteBuffer head = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
        // ⚠️ THE SLOTS SELECT THE LAYOUT, never mutate one: a checkpoint whose
        // pods carry no incarnation encodes byte-for-byte as it did before
        // ADR-0036, which is what keeps `checkpoint-v0.bin` and every written
        // checkpoint readable.
        boolean attributed = pods.values().stream().anyMatch(PodState::hasPointer);
        head.putInt(MAGIC);
        head.putInt(attributed ? VERSION_ATTRIBUTED : VERSION);
        out.writeBytes(head.array());
        SegmentWriter.putUvarint(out, sequence);

        List<RunKey> keys = new ArrayList<>(streams.keySet());
        Collections.sort(keys);
        SegmentWriter.putUvarint(out, keys.size());
        for (RunKey k : keys) {
            ByteBuffer id = ByteBuffer.allocate(16).order(ByteOrder.BIG_ENDIAN);
            id.putLong(k.indexId().getMostSignificantBits());
            id.putLong(k.indexId().getLeastSignificantBits());
            out.writeBytes(id.array());
            SegmentWriter.putUvarint(out, k.partitionId());
            StreamOffsets o = streams.get(k);
            SegmentWriter.putUvarint(out, o.nextOffset());
            SegmentWriter.putUvarint(out, o.oldestRetainedOffset());
        }

        List<String> podIds = new ArrayList<>(pods.keySet());
        Collections.sort(podIds);
        SegmentWriter.putUvarint(out, podIds.size());
        for (String pod : podIds) {
            byte[] raw = pod.getBytes(StandardCharsets.UTF_8);
            SegmentWriter.putUvarint(out, raw.length);
            out.writeBytes(raw);
            PodState s = pods.get(pod);
            SegmentWriter.putUvarint(out, s.lastAppliedFlushSeq());
            if (attributed) {
                // ⚠️ A PER-SLOT FLAG, because MIXED IS THE NORMAL STATE after an
                // upgrade: a pod that has committed since carries an incarnation,
                // one recovered from a v0 checkpoint does not, and both live in
                // one map until the next checkpoint. A version-wide flag would
                // force the writer to invent an incarnation for the second.
                SegmentWriter.putUvarint(out, s.hasPointer() ? 1 : 0);
                if (s.hasPointer()) {
                    byte[] inc = s.incarnationId().getBytes(StandardCharsets.UTF_8);
                    SegmentWriter.putUvarint(out, inc.length);
                    out.writeBytes(inc);
                    SegmentWriter.putUvarint(out, s.epoch());
                    SegmentWriter.putUvarint(out, s.sequence());
                }
            }
        }
        return out.toByteArray();
    }

    public static Checkpoint decode(byte[] bytes) throws IOException {
        if (bytes.length < 8) {
            throw new IOException("checkpoint is shorter than its own header");
        }
        ByteBuffer b = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        if (b.getInt(0) != MAGIC) {
            throw new IOException("not a checkpoint: bad magic");
        }
        int version = b.getInt(4);
        if (version != VERSION && version != VERSION_ATTRIBUTED) {
            throw new IOException("unsupported checkpoint version: " + version);
        }
        Cursor c = new Cursor(bytes, 8);
        try {
            return decodeBody(c, version);
        } catch (IllegalArgumentException refused) {
            // ⚠️ A VALUE THE WIRE CAN CARRY BUT A RECORD REFUSES IS CORRUPT
            // INPUT, not a programming error -- the rule `ChainEntry.decodeKinded`
            // already states and this decoder is the most exposed to, because a
            // checkpoint carries a CROSS-FIELD invariant (`oldest <= next`) no
            // chain entry has. Unwrapped, a torn or zero-filled object throws an
            // unchecked exception straight past every `catch (IOException)` a
            // reader writes -- and M4.9 is about to be written against this
            // contract. ⚠️ NO COUNT OF THE ROUTES IS GIVEN, and that is
            // deliberate: this comment has carried a wrong one in three
            // consecutive rounds -- "five", then "four", both measured wrong,
            // the second introduced by the fix for the first -- and nothing
            // pins the number, so any value here survives every mutation.
            // EVERY value the wire carries into a validating record reaches
            // this wrap: both offsets, the sequence, a flushSeq, a podId.
            // ⚠️ A partitionId NO LONGER DOES, and that is the point worth
            // carrying to the next narrowing field: a value the wire can hold
            // but an int cannot is refused by the DECODER, not left to the
            // record it feeds. Leaving it to the record is what silently
            // attributed one stream's offsets to another.
            throw new IOException("corrupt checkpoint: " + refused.getMessage(), refused);
        }
    }

    /**
     * ⚠️ ENUMERATED, not `== 1`. Any other value decoded as a BARE slot, so a
     * corrupt flag silently dropped a pod's pointer instead of refusing the
     * object — and `!= 0` survived the whole build. The archive row's word for
     * this format is "enumerated", and this is the field it has to be true of.
     */
    private static boolean attributedSlot(Cursor c) throws IOException {
        long flag = c.uvarint();
        if (flag != 0 && flag != 1) {
            throw new IOException("a checkpoint pod slot claims presence flag " + flag);
        }
        return flag == 1;
    }

    private static Checkpoint decodeBody(Cursor c, int version) throws IOException {
        long sequence = c.uvarint();

        long streamCount = c.uvarint();
        // ⚠️ THE COUNT IS NOT AN ALLOCATION SIZE. A torn or hostile object can
        // claim billions of streams, and sizing a map from it raises
        // OutOfMemoryError out of a method declared `throws IOException`. The
        // same defect was measured in a delta at M4.7a; bound it against the
        // bytes that actually remain.
        if (streamCount < 0 || streamCount > c.remaining() / 18) {
            throw new IOException("a checkpoint claims " + streamCount + " streams but only "
                    + c.remaining() + " bytes remain");
        }
        Map<RunKey, StreamOffsets> streams = new LinkedHashMap<>();
        for (long i = 0; i < streamCount; i++) {
            ByteBuffer ib = ByteBuffer.wrap(c.bytes(16)).order(ByteOrder.BIG_ENDIAN);
            // ⚠️ NARROWED ONLY AFTER IT IS BOUNDED, and this field is the one
            // that says WHICH stream the offsets belong to. Measured before the
            // guard: `0x1_0000_0001` decoded to partitionId 1 and was returned
            // as valid -- one stream's offsets attributed to another, with no
            // exception for a reader's `catch (IOException)` to see. Only
            // values narrowing to a NEGATIVE int were refused, and by `RunKey`
            // rather than here.
            long partitionId = c.uvarint();
            if (partitionId < 0 || partitionId > Integer.MAX_VALUE) {
                throw new IOException("a checkpoint claims partitionId " + partitionId
                        + ", which is not a partition any writer can produce");
            }
            RunKey key = new RunKey(new java.util.UUID(ib.getLong(), ib.getLong()),
                    (int) partitionId);
            long next = c.uvarint();
            // ⚠️ REFUSED, NOT LAST-WINS. A repeated key would otherwise decode
            // to whichever copy came last -- measured, an offset going BACKWARDS
            // from 900 to 5 with the trailing-bytes check still green.
            // `CommitDelta` refuses the identical shape for segment keys.
            if (streams.put(key, new StreamOffsets(next, c.uvarint())) != null) {
                throw new IOException("a checkpoint repeats stream " + key);
            }
        }

        long podCount = c.uvarint();
        if (podCount < 0 || podCount > c.remaining()) {
            throw new IOException("a checkpoint claims " + podCount + " pods but only "
                    + c.remaining() + " bytes remain");
        }
        Map<String, PodState> pods = new LinkedHashMap<>();
        for (long i = 0; i < podCount; i++) {
            // ⚠️ THE LENGTH IS 64-BIT ON THE WIRE and this cast is where it
            // stops being. Measured: `0x1_0000_0004` narrows to 4 and the object
            // decodes as VALID rather than being refused -- a silent misread,
            // not an allocation hazard, since `Cursor.bytes` bounds the read.
            long podLen = c.uvarint();
            if (podLen < 0 || podLen > c.remaining()) {
                throw new IOException("a checkpoint claims a " + podLen + "-byte podId but only "
                        + c.remaining() + " bytes remain");
            }
            String pod = new String(c.bytes((int) podLen), StandardCharsets.UTF_8);
            long watermark = c.uvarint();
            PodState state = PodState.bare(watermark);
            if (version == VERSION_ATTRIBUTED && attributedSlot(c)) {
                long incLen = c.uvarint();
                if (incLen < 0 || incLen > c.remaining()) {
                    throw new IOException("a checkpoint claims a " + incLen
                            + "-byte incarnationId but only " + c.remaining() + " bytes remain");
                }
                String inc = new String(c.bytes((int) incLen), StandardCharsets.UTF_8);
                state = new PodState(inc, watermark, c.uvarint(), c.uvarint());
            }
            if (pods.put(pod, state) != null) {
                throw new IOException("a checkpoint repeats pod " + pod);
            }
        }
        if (!c.atEnd()) {
            throw new IOException("checkpoint has bytes after its last field");
        }
        return new Checkpoint(sequence, streams, pods);
    }
}
