// SPDX-License-Identifier: Apache-2.0
package binjava.format;

/**
 * The v0 segment layout, as constants shared by the writer and the reader.
 *
 * <p>⚠️ These numbers ARE the wire format. Changing one updates the format, both
 * sides, the fakes, the golden files and an ADR in a single commit
 * (non-negotiable 8).
 *
 * <pre>
 * PREAMBLE (32 B)  magic 'BSEG' | u16 version | u16 flags | u32 headerLen
 *                  u32 headerCrc32c | u64 createdAtMillis | u32 runCount | u32 pad
 * DIRECTORY        runCount x 48 B, sorted by (indexId, partitionId)
 *                  u128 indexId | u32 partitionId | u32 recordCount
 *                  u64 byteStart | u32 byteLen | u64 minTimestampMillis | u32 codecFlags
 * DATA             per run: [u32 uncompressedLen][u32 crc32c][records]
 *                  record: [u8 flags][uvarint idLen][id][uvarint version]?
 *                          [uvarint payloadLen][payload]
 * FOOTER (24 B)    u64 headerStart | u32 headerLen | u64 magic | u32 crc32c
 * </pre>
 */
public final class SegmentFormat {

    /** 'BSEG'. */
    public static final int MAGIC = 0x42534547;

    /** The 64-bit footer magic, so a truncated file cannot look complete. */
    public static final long FOOTER_MAGIC = 0x4253454746545221L;

    public static final int VERSION = 0;
    public static final int PREAMBLE_BYTES = 32;

    /**
     * ⚠️ FIXED WIDTH, so the directory is binary-searchable with no parsing —
     * the property AutoMQ's DataBlockIndex has and the reason a run lookup is
     * O(log n) over a mapped range rather than a scan.
     */
    public static final int DIRECTORY_ENTRY_BYTES = 48;

    public static final int FOOTER_BYTES = 24;

    /** Uncompressed, no per-record CRC. M2 adds codecs. */
    public static final int CODEC_NONE = 0;

    private SegmentFormat() {}

    /** Flags byte of a record: op type in the low 2 bits, version-present in bit 2. */
    public static int recordFlags(OpType opType, boolean versionPresent) {
        return opType.ordinal() | (versionPresent ? 0b100 : 0);
    }

    public static OpType opTypeOf(int flags) {
        return OpType.fromWire(flags & 0b11);
    }

    public static boolean hasVersion(int flags) {
        return (flags & 0b100) != 0;
    }
}
