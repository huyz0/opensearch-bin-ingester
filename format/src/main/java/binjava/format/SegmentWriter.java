// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.CRC32C;

/**
 * Builds one segment (M1.4). Always emits {@link SegmentFormat#VERSION} (M3;
 * ADR-0025) -- v0 is a read-only shape {@link SegmentReader} still parses.
 *
 * <p>⚠️ Runs are emitted sorted by {@code (indexId, partitionId)}, so one
 * consumer's records are CONTIGUOUS and its fetch is a single coalesced range —
 * cost rules R4 and R5. A writer that emitted arrival order would turn every
 * consumer's read into one range GET per run.
 *
 * <p>⚠️ THE HEADER IS AT THE FRONT and names byte offsets into data that has not
 * been written yet, so the data is buffered and the header emitted once the
 * layout is known. That is a bounded buffer, not a violation of C8: the ingester
 * flushes at 8 MiB and the budget counts 50 such buffers in a pool.
 */
public final class SegmentWriter {

    private final Map<RunKey, List<SegmentRecord>> runs = new TreeMap<>();
    private final Map<RunKey, Long> minTimestamps = new TreeMap<>();

    /** Adds one record to a run, creating the run on first use. */
    public void add(RunKey key, SegmentRecord record, long timestampMillis) {
        runs.computeIfAbsent(key, k -> new ArrayList<>()).add(record);
        minTimestamps.merge(key, timestampMillis, Math::min);
    }

    public boolean isEmpty() {
        return runs.isEmpty();
    }

    /** ⚠️ uvarint, LEB128: the same encoding the reader must use. */
    static void putUvarint(ByteArrayOutputStream out, long value) {
        long v = value;
        while ((v & ~0x7FL) != 0) {
            out.write((int) ((v & 0x7F) | 0x80));
            v >>>= 7;
        }
        out.write((int) v);
    }

    private static byte[] encodeRun(List<SegmentRecord> records) {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        for (SegmentRecord r : records) {
            byte[] id = r.id().getBytes(StandardCharsets.UTF_8);
            byte[] payload = r.payload();
            boolean hasVersion = r.version().isPresent();
            body.write(SegmentFormat.recordFlags(r.opType(), hasVersion));
            putUvarint(body, id.length);
            body.writeBytes(id);
            if (hasVersion) {
                putUvarint(body, r.version().getAsLong());
            }
            putUvarint(body, payload.length);
            body.writeBytes(payload);
        }
        byte[] bytes = body.toByteArray();
        CRC32C crc = new CRC32C();
        crc.update(bytes);
        ByteBuffer block = ByteBuffer.allocate(8 + bytes.length).order(ByteOrder.BIG_ENDIAN);
        block.putInt(bytes.length);
        block.putInt((int) crc.getValue());
        block.put(bytes);
        return block.array();
    }

    /**
     * Serialises the segment.
     *
     * @param createdAtMillis stamped into the preamble; passed in rather than
     *     read from a clock, because business logic touches no clock directly
     *     (non-negotiable 7) and a golden file must be reproducible.
     */
    public void writeTo(OutputStream out, long createdAtMillis) throws IOException {
        List<RunKey> keys = new ArrayList<>(runs.keySet());
        int runCount = keys.size();
        int headerLen = runCount * SegmentFormat.DIRECTORY_ENTRY_BYTES;

        List<byte[]> blocks = new ArrayList<>(runCount);
        for (RunKey k : keys) {
            blocks.add(encodeRun(runs.get(k)));
        }

        ByteBuffer dir = ByteBuffer.allocate(headerLen).order(ByteOrder.BIG_ENDIAN);
        long byteStart = SegmentFormat.PREAMBLE_BYTES + (long) headerLen;
        for (int i = 0; i < runCount; i++) {
            RunKey k = keys.get(i);
            byte[] block = blocks.get(i);
            dir.putLong(k.indexId().getMostSignificantBits());
            dir.putLong(k.indexId().getLeastSignificantBits());
            dir.putInt(k.partitionId());
            dir.putInt(runs.get(k).size());
            dir.putLong(byteStart);
            dir.putInt(block.length);
            dir.putLong(minTimestamps.get(k));
            dir.putInt(SegmentFormat.CODEC_NONE);
            // ⚠️ M3; ADR-0025: reserved, always 0 -- no caller has a real
            // lane to pass yet (RunKey itself gains no lane component until
            // M10 wires the concept in above this layer).
            dir.put((byte) 0);
            byteStart += block.length;
        }
        byte[] directory = dir.array();

        CRC32C headerCrc = new CRC32C();
        headerCrc.update(directory);

        ByteBuffer preamble =
                ByteBuffer.allocate(SegmentFormat.PREAMBLE_BYTES).order(ByteOrder.BIG_ENDIAN);
        preamble.putInt(SegmentFormat.MAGIC);
        preamble.putShort((short) SegmentFormat.VERSION);
        preamble.putShort((short) 0);
        preamble.putInt(headerLen);
        preamble.putInt((int) headerCrc.getValue());
        preamble.putLong(createdAtMillis);
        preamble.putInt(runCount);
        preamble.putInt(0);

        out.write(preamble.array());
        out.write(directory);
        for (byte[] block : blocks) {
            out.write(block);
        }

        // ⚠️ The footer duplicates the header's location so a TRUNCATED object
        // is detectable: a reader that only trusted the preamble would parse a
        // half-written segment as a whole one.
        ByteBuffer footer =
                ByteBuffer.allocate(SegmentFormat.FOOTER_BYTES).order(ByteOrder.BIG_ENDIAN);
        footer.putLong(SegmentFormat.PREAMBLE_BYTES);
        footer.putInt(headerLen);
        footer.putLong(SegmentFormat.FOOTER_MAGIC);
        CRC32C footerCrc = new CRC32C();
        footerCrc.update(footer.array(), 0, 20);
        footer.putInt((int) footerCrc.getValue());
        out.write(footer.array());
    }

    /** The whole segment as bytes. Bounded by the flush trigger, not by a document. */
    public byte[] toByteArray(long createdAtMillis) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeTo(out, createdAtMillis);
        return out.toByteArray();
    }
}
