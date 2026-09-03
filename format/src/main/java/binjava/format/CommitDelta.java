// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

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
public record CommitDelta(long sequence, String segmentKey, List<RunCommit> runs)
        implements ChainEntry {


    public CommitDelta {
        Objects.requireNonNull(segmentKey, "segmentKey");
        if (sequence < 0) {
            throw new IllegalArgumentException("sequence is never negative: " + sequence);
        }
        if (segmentKey.isEmpty()) {
            throw new IllegalArgumentException("a delta names a segment");
        }
        runs = List.copyOf(Objects.requireNonNull(runs, "runs"));
        if (runs.isEmpty()) {
            // ⚠️ An empty delta would consume a sequence number and commit
            // nothing, so a replay would see a gap it cannot explain.
            throw new IllegalArgumentException("a delta with no runs commits nothing");
        }
    }

    /**
     * ⚠️ The commit log is a wire format too: non-negotiable 8 applies.
     *
     * <p>⚠️ STILL v0. M4.5 added two more shapes at this key and a v1 header
     * that carries a kind, but nothing about a DELTA changed — so re-encoding
     * one would churn every byte in the bucket and both golden files to say
     * exactly what v0 already says. See {@link ChainEntry}.
     */
    @Override
    public byte[] encode() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(ChainEntry.header(ChainEntry.VERSION_DELTA));
        SegmentWriter.putUvarint(out, sequence);
        byte[] key = segmentKey.getBytes(StandardCharsets.UTF_8);
        SegmentWriter.putUvarint(out, key.length);
        out.writeBytes(key);
        SegmentWriter.putUvarint(out, runs.size());
        for (RunCommit r : runs) {
            ByteBuffer id = ByteBuffer.allocate(16).order(ByteOrder.BIG_ENDIAN);
            id.putLong(r.key().indexId().getMostSignificantBits());
            id.putLong(r.key().indexId().getLeastSignificantBits());
            out.writeBytes(id.array());
            SegmentWriter.putUvarint(out, r.key().partitionId());
            SegmentWriter.putUvarint(out, r.recordCount());
            SegmentWriter.putUvarint(out, r.firstOffset());
        }
        return out.toByteArray();
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

    /** ⚠️ Body only: {@link ChainEntry#decode} has consumed the header. */
    static CommitDelta decodeBody(Cursor c) throws IOException {
        long sequence = c.uvarint();
        String key = new String(c.bytes((int) c.uvarint()), StandardCharsets.UTF_8);
        int runCount = (int) c.uvarint();
        List<RunCommit> runs = new ArrayList<>(runCount);
        for (int i = 0; i < runCount; i++) {
            byte[] id = c.bytes(16);
            ByteBuffer ib = ByteBuffer.wrap(id).order(ByteOrder.BIG_ENDIAN);
            RunKey rk = new RunKey(new java.util.UUID(ib.getLong(), ib.getLong()),
                    (int) c.uvarint());
            runs.add(new RunCommit(rk, (int) c.uvarint(), c.uvarint()));
        }
        return new CommitDelta(sequence, key, runs);
    }
}
