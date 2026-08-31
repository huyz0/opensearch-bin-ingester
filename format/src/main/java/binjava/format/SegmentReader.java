// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.zip.CRC32C;

/**
 * Reads a v0 segment (M1.5).
 *
 * <p>⚠️ EVERY FIELD IS VALIDATED BEFORE IT IS TRUSTED. A segment arrives from an
 * object store and may be truncated, torn or written by an older build; a reader
 * that trusts a length prefix turns any of those into an allocation the size of
 * whatever the bytes happened to say (security.md — anything parsing untrusted
 * input).
 */
public final class SegmentReader {

    private final ByteBuffer buf;
    private final List<RunEntry> directory;
    private final long createdAtMillis;

    private SegmentReader(ByteBuffer buf, List<RunEntry> directory, long createdAtMillis) {
        this.buf = buf;
        this.directory = directory;
        this.createdAtMillis = createdAtMillis;
    }

    public static SegmentReader open(byte[] segment) throws IOException {
        ByteBuffer b = ByteBuffer.wrap(segment).order(ByteOrder.BIG_ENDIAN);
        if (segment.length < SegmentFormat.PREAMBLE_BYTES + SegmentFormat.FOOTER_BYTES) {
            throw new IOException("segment is shorter than its own preamble and footer");
        }
        if (b.getInt(0) != SegmentFormat.MAGIC) {
            throw new IOException("not a segment: bad magic");
        }
        int version = b.getShort(4) & 0xFFFF;
        if (version != SegmentFormat.VERSION) {
            // ⚠️ Refuse, never best-effort. A future version may reuse a field,
            // so "read what I recognise" silently misinterprets it.
            throw new IOException("unsupported segment version: " + version);
        }
        int headerLen = b.getInt(8);
        int runCount = b.getInt(24);
        if (headerLen != runCount * SegmentFormat.DIRECTORY_ENTRY_BYTES) {
            throw new IOException("headerLen " + headerLen + " disagrees with runCount " + runCount);
        }
        if (SegmentFormat.PREAMBLE_BYTES + (long) headerLen + SegmentFormat.FOOTER_BYTES
                > segment.length) {
            throw new IOException("directory extends past the end of the segment");
        }

        // ⚠️ The FOOTER is checked too, because a truncated object still has a
        // valid preamble: the header says what should be there, and only the
        // footer says the writer got that far.
        int f = segment.length - SegmentFormat.FOOTER_BYTES;
        if (b.getLong(f + 12) != SegmentFormat.FOOTER_MAGIC) {
            throw new IOException("segment is truncated: no footer magic");
        }
        if (b.getInt(f + 8) != headerLen) {
            throw new IOException("footer headerLen disagrees with the preamble");
        }

        CRC32C headerCrc = new CRC32C();
        headerCrc.update(segment, SegmentFormat.PREAMBLE_BYTES, headerLen);
        if ((int) headerCrc.getValue() != b.getInt(12)) {
            throw new IOException("directory checksum mismatch");
        }

        List<RunEntry> dir = new ArrayList<>(runCount);
        for (int i = 0; i < runCount; i++) {
            int e = SegmentFormat.PREAMBLE_BYTES + i * SegmentFormat.DIRECTORY_ENTRY_BYTES;
            UUID indexId = new UUID(b.getLong(e), b.getLong(e + 8));
            long byteStart = b.getLong(e + 24);
            int byteLen = b.getInt(e + 32);
            if (byteStart < 0 || byteLen < 0 || byteStart + byteLen > f) {
                throw new IOException("run " + i + " points outside the segment");
            }
            dir.add(new RunEntry(new RunKey(indexId, b.getInt(e + 16)), b.getInt(e + 20),
                    byteStart, byteLen, b.getLong(e + 36), b.getInt(e + 44)));
        }
        return new SegmentReader(b, List.copyOf(dir), b.getLong(16));
    }

    public long createdAtMillis() {
        return createdAtMillis;
    }

    public List<RunEntry> directory() {
        return directory;
    }

    /**
     * The entry for one stream, or empty.
     *
     * <p>⚠️ BINARY SEARCH over fixed-width entries, in the writer's order. The
     * bound must admit the FIRST and LAST entries: an off-by-one that excludes
     * either passes every test whose fixture happens to look in the middle.
     */
    public Optional<RunEntry> find(RunKey key) {
        int lo = 0;
        int hi = directory.size() - 1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            int cmp = directory.get(mid).key().compareTo(key);
            if (cmp == 0) {
                return Optional.of(directory.get(mid));
            }
            if (cmp < 0) {
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return Optional.empty();
    }

    /** Every record in a run, decoded. */
    public List<SegmentRecord> read(RunEntry entry) throws IOException {
        int at = (int) entry.byteStart();
        int uncompressedLen = buf.getInt(at);
        int declaredCrc = buf.getInt(at + 4);
        if (uncompressedLen < 0 || uncompressedLen > entry.byteLen() - 8) {
            throw new IOException("block length " + uncompressedLen + " overruns its run");
        }
        byte[] body = new byte[uncompressedLen];
        buf.duplicate().position(at + 8).get(body);

        CRC32C crc = new CRC32C();
        crc.update(body);
        if ((int) crc.getValue() != declaredCrc) {
            throw new IOException("block checksum mismatch");
        }

        List<SegmentRecord> out = new ArrayList<>(entry.recordCount());
        Cursor c = new Cursor(body);
        for (int i = 0; i < entry.recordCount(); i++) {
            int flags = c.u8();
            OpType op = SegmentFormat.opTypeOf(flags);
            String id = new String(c.bytes((int) c.uvarint()), StandardCharsets.UTF_8);
            OptionalLong version = SegmentFormat.hasVersion(flags)
                    ? OptionalLong.of(c.uvarint())
                    : OptionalLong.empty();
            byte[] payload = c.bytes((int) c.uvarint());
            out.add(new SegmentRecord(id, op, version, payload));
        }
        if (!c.atEnd()) {
            // ⚠️ recordCount and the bytes must AGREE. Trusting the count alone
            // would silently drop a trailing record; trusting the bytes alone
            // would read a corrupt tail as data.
            throw new IOException("run has bytes after its last declared record");
        }
        return out;
    }

    /** A bounds-checked walk over a run's bytes. */
    private static final class Cursor {
        private final byte[] a;
        private int i;

        Cursor(byte[] a) {
            this.a = a;
        }

        boolean atEnd() {
            return i == a.length;
        }

        int u8() throws IOException {
            need(1);
            return a[i++] & 0xFF;
        }

        long uvarint() throws IOException {
            long value = 0;
            int shift = 0;
            while (true) {
                need(1);
                int b = a[i++] & 0xFF;
                value |= (long) (b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    return value;
                }
                shift += 7;
                if (shift > 63) {
                    throw new IOException("uvarint is longer than 64 bits");
                }
            }
        }

        byte[] bytes(int n) throws IOException {
            if (n < 0) {
                throw new IOException("negative length: " + n);
            }
            need(n);
            byte[] out = new byte[n];
            System.arraycopy(a, i, out, 0, n);
            i += n;
            return out;
        }

        private void need(int n) throws IOException {
            // ⚠️ Checked BEFORE allocating. A length prefix from a torn object
            // would otherwise size an array from whatever the bytes said.
            if (i + n > a.length) {
                throw new IOException("run ends inside a record");
            }
        }
    }
}
