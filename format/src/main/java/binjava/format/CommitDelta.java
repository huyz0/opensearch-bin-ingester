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
public record CommitDelta(long sequence, String segmentKey, List<RunCommit> runs) {

    private static final int MAGIC = 0x42444C54;   // 'BDLT'
    private static final int VERSION = 0;

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

    /** ⚠️ The commit log is a wire format too: non-negotiable 8 applies. */
    public byte[] encode() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteBuffer head = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
        head.putInt(MAGIC);
        head.putInt(VERSION);
        out.writeBytes(head.array());
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

    public static CommitDelta decode(byte[] bytes) throws IOException {
        if (bytes.length < 8) {
            throw new IOException("commit delta is shorter than its own header");
        }
        ByteBuffer b = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        if (b.getInt(0) != MAGIC) {
            throw new IOException("not a commit delta: bad magic");
        }
        if (b.getInt(4) != VERSION) {
            throw new IOException("unsupported commit delta version: " + b.getInt(4));
        }
        Cursor c = new Cursor(bytes, 8);
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
        if (!c.atEnd()) {
            throw new IOException("commit delta has bytes after its last run");
        }
        return new CommitDelta(sequence, key, runs);
    }

    /** ⚠️ Bounds-checked before allocating: the log is untrusted input too. */
    private static final class Cursor {
        private final byte[] a;
        private int i;

        Cursor(byte[] a, int from) {
            this.a = a;
            this.i = from;
        }

        boolean atEnd() {
            return i == a.length;
        }

        long uvarint() throws IOException {
            long value = 0;
            int shift = 0;
            while (true) {
                if (i >= a.length) {
                    throw new IOException("commit delta ends inside a varint");
                }
                int b = a[i++] & 0xFF;
                value |= (long) (b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    return value;
                }
                shift += 7;
                if (shift > 63) {
                    throw new IOException("varint longer than 64 bits");
                }
            }
        }

        byte[] bytes(int n) throws IOException {
            if (n < 0 || i + n > a.length) {
                throw new IOException("commit delta ends inside a field");
            }
            byte[] out = new byte[n];
            System.arraycopy(a, i, out, 0, n);
            i += n;
            return out;
        }
    }
}
