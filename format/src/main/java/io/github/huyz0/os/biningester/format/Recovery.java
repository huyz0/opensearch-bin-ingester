// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * A takeover's recovery: its segment commits and its voids in ONE chain entry
 * (M13.25, ADR-0081 §5 invariant c, ADR-0082 §5), under
 * {@link ChainEntry#KIND_RECOVERY}.
 *
 * <p>⚠️ ONE ENTRY, BECAUSE THE CHAIN'S FOLD TAKES THE MAXIMUM. Written as a
 * void entry beside ordinary deltas, a crash between them leaves an exposed
 * hole below the next offset or a void past a surviving copy (M13.22 review
 * round 1, P3); in one entry a crash leaves all of it or none.
 *
 * <p>⚠️ A VOID IS A COMMITTED HOLE: it advances its stream's next offset to
 * {@code toOffsetExclusive} exactly as a run of records would, so no later
 * commit assigns inside it, and it moves no bytes. Voids are sorted by
 * {@link RunKey}, never overlap one another, and never overlap this entry's
 * own runs; that they never overlap an offset committed by an EARLIER entry is
 * the writer's obligation (ADR-0081 §5), checked by the chain invariants.
 *
 * <p>⚠️ ITS SEGMENTS ARE HELD TO A DELTA'S RULES AT CONSTRUCTION (M13.25 review
 * round 1, P1): built lazily, a segment a delta refuses -- one named twice --
 * decoded cleanly and then threw unchecked out of every reader that asked for
 * {@link #delta()}. And they carry no attribution, which this kind does not
 * encode: an attributed segment is refused rather than silently stripped.
 *
 * <p>Body after the kind: {@code sequence} uvarint, the segment count and the
 * segments exactly as a batched delta writes them (zero allowed), then the
 * void count and each void as the index UUID (two i64), the partition, the
 * from offset and the exclusive to offset (uvarints).
 */
public record Recovery(long sequence, List<SegmentCommit> segments, List<VoidRange> voids)
        implements ChainEntry {

    /** A committed hole: offsets {@code [fromOffset, toOffsetExclusive)} of {@code key}. */
    public record VoidRange(RunKey key, long fromOffset, long toOffsetExclusive) {
        public VoidRange {
            Objects.requireNonNull(key, "key");
            if (fromOffset < 0 || toOffsetExclusive <= fromOffset) {
                throw new IllegalArgumentException("a void [" + fromOffset + ", "
                        + toOffsetExclusive + ") of " + key + " voids nothing");
            }
        }
    }

    public Recovery {
        Objects.requireNonNull(segments, "segments");
        Objects.requireNonNull(voids, "voids");
        segments = List.copyOf(segments);
        voids = List.copyOf(voids);
        if (sequence < 0) {
            throw new IllegalArgumentException("sequence is never " + sequence);
        }
        if (segments.isEmpty() && voids.isEmpty()) {
            throw new IllegalArgumentException("a recovery that commits and voids nothing");
        }
        if (!segments.isEmpty()) {
            new CommitDelta(sequence, segments);
        }
        // ⚠️ VOIDS BY STREAM, so the overlap check is a lookup per run and not
        // runs x voids on every decode by every reader (M13.25 review round 2, P2).
        java.util.Map<RunKey, List<VoidRange>> voidsByStream = new java.util.HashMap<>();
        for (VoidRange v : voids) {
            voidsByStream.computeIfAbsent(v.key(), k -> new ArrayList<>()).add(v);
        }
        for (SegmentCommit segment : segments) {
            if (segment.attribution() != null) {
                throw new IllegalArgumentException("a recovery's segment " + segment.segmentKey()
                        + " carries an attribution this kind does not encode");
            }
            for (RunCommit run : segment.runs()) {
                List<VoidRange> stream = voidsByStream.get(run.key());
                if (stream == null) {
                    continue;
                }
                long end = run.firstOffset() + run.recordCount();
                // sorted and disjoint: the first void ending past the run's start
                // is the only one that can overlap it
                int lo = 0;
                int hi = stream.size();
                while (lo < hi) {
                    int mid = (lo + hi) >>> 1;
                    if (stream.get(mid).toOffsetExclusive() <= run.firstOffset()) {
                        lo = mid + 1;
                    } else {
                        hi = mid;
                    }
                }
                if (lo < stream.size() && stream.get(lo).fromOffset() < end) {
                    throw new IllegalArgumentException("void " + stream.get(lo) + " overlaps run "
                            + run + " of the same recovery");
                }
            }
        }
        for (int i = 1; i < voids.size(); i++) {
            VoidRange before = voids.get(i - 1);
            VoidRange after = voids.get(i);
            int order = before.key().compareTo(after.key());
            if (order > 0 || (order == 0 && after.fromOffset() < before.toOffsetExclusive())) {
                throw new IllegalArgumentException("voids are sorted by stream and disjoint; "
                        + before + " precedes " + after);
            }
        }
    }

    /** The segment commits, as a delta -- empty when the entry only voids. */
    public Optional<CommitDelta> delta() {
        return segments.isEmpty() ? Optional.empty()
                : Optional.of(new CommitDelta(sequence, segments));
    }

    @Override
    public byte[] encode() {
        ByteArrayOutputStream out = ChainEntry.kinded(ChainEntry.KIND_RECOVERY);
        SegmentWriter.putUvarint(out, sequence);
        CommitDelta.writeSegments(out, segments);
        SegmentWriter.putUvarint(out, voids.size());
        for (VoidRange v : voids) {
            out.writeBytes(ByteBuffer.allocate(16)
                    .putLong(v.key().indexId().getMostSignificantBits())
                    .putLong(v.key().indexId().getLeastSignificantBits()).array());
            SegmentWriter.putUvarint(out, v.key().partitionId());
            SegmentWriter.putUvarint(out, v.fromOffset());
            SegmentWriter.putUvarint(out, v.toOffsetExclusive());
        }
        return out.toByteArray();
    }

    /** ⚠️ Body only: {@link ChainEntry#decode} has consumed the header and the kind. */
    static Recovery decodeBody(Cursor c) throws IOException {
        long sequence = c.uvarint();
        List<SegmentCommit> segments = CommitDelta.readSegments(c);
        long count = c.uvarint();
        // ⚠️ BOUNDED BEFORE IT SIZES A LIST: a void is at least 19 bytes.
        if (count < 0 || count > c.remaining()) {
            throw new IOException("recovery claims " + count + " voids with only "
                    + c.remaining() + " bytes left");
        }
        List<VoidRange> voids = new ArrayList<>((int) count);
        for (long i = 0; i < count; i++) {
            ByteBuffer id = ByteBuffer.wrap(c.bytes(16));
            UUID index = new UUID(id.getLong(), id.getLong());
            long partition = c.uvarint();
            if (partition < 0 || partition > Integer.MAX_VALUE) {
                throw new IOException("void partition " + partition + " does not fit an int");
            }
            voids.add(new VoidRange(new RunKey(index, (int) partition), c.uvarint(),
                    c.uvarint()));
        }
        return new Recovery(sequence, segments, voids);
    }
}
