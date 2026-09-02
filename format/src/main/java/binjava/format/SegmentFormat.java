// SPDX-License-Identifier: Apache-2.0
package binjava.format;

/**
 * The segment layout, as constants shared by the writer and the reader.
 *
 * <p>⚠️ These numbers ARE the wire format. Changing one updates the format, both
 * sides, the fakes, the golden files and an ADR in a single commit
 * (non-negotiable 8).
 *
 * <pre>
 * PREAMBLE (32 B)  magic 'BSEG' | u16 version | u16 flags | u32 headerLen
 *                  u32 headerCrc32c | u64 createdAtMillis | u32 runCount | u32 pad
 * DIRECTORY        runCount x {@link #directoryEntryBytesFor entry width}, sorted
 *                  by (indexId, partitionId) --
 *                  u128 indexId | u32 partitionId | u32 recordCount
 *                  u64 byteStart | u32 byteLen | u64 minTimestampMillis | u32 codecFlags
 *                  [ | i8 lane -- v1 only, ADR-0025 ]
 * DATA             per run: [u32 uncompressedLen][u32 crc32c][records]
 *                  record: [u8 flags][uvarint idLen][id][uvarint version]?
 *                          [uvarint payloadLen][payload]
 * FOOTER (24 B)    u64 headerStart | u32 headerLen | u64 magic | u32 crc32c
 * </pre>
 *
 * <p>⚠️ TWO VERSIONS, BOTH READABLE (M3; ADR-0025; the {@code
 * wire-format-change} skill's own rule: "a reader must handle the old shape
 * until every possible writer of it has aged out"). {@code v0}'s 48-byte
 * directory entry has no lane byte; {@code v1} appends one, always {@code 0}
 * for now -- M10 is what starts writing a real value. {@link #VERSION} is
 * what {@link SegmentWriter} emits; {@code v0} exists only for {@link
 * SegmentReader} to keep parsing segments an earlier build already wrote. A
 * new field on a versioned struct is NOT a contract change per the skill's
 * own "is a version bump enough" test -- the bump exists so this addition
 * costs one new golden file, not a rewrite of the old one.
 */
public final class SegmentFormat {

    /** 'BSEG'. */
    public static final int MAGIC = 0x42534547;

    /** The 64-bit footer magic, so a truncated file cannot look complete. */
    public static final long FOOTER_MAGIC = 0x4253454746545221L;

    /** The version {@link SegmentWriter} emits. Readers also accept {@link #VERSION_0}. */
    public static final int VERSION = 1;

    /** ⚠️ Read-only. No writer in this codebase emits this version anymore. */
    public static final int VERSION_0 = 0;

    public static final int PREAMBLE_BYTES = 32;

    /**
     * ⚠️ FIXED WIDTH, so the directory is binary-searchable with no parsing —
     * the property AutoMQ's DataBlockIndex has and the reason a run lookup is
     * O(log n) over a mapped range rather than a scan. This is {@link
     * #VERSION}'s (v1's) width; {@link #VERSION_0} segments are 1 byte
     * narrower — use {@link #directoryEntryBytesFor} rather than this
     * constant when the segment's own version is not already known to be
     * {@link #VERSION}.
     */
    public static final int DIRECTORY_ENTRY_BYTES = 49;

    /** ⚠️ Read-only, matching {@link #VERSION_0} — narrower by the lane byte. */
    public static final int DIRECTORY_ENTRY_BYTES_V0 = 48;

    public static final int FOOTER_BYTES = 24;

    /** Uncompressed, no per-record CRC. M2 adds codecs. */
    public static final int CODEC_NONE = 0;

    private SegmentFormat() {}

    /**
     * The directory entry width for a segment claiming this {@code version}
     * in its preamble.
     *
     * @throws java.io.IOException if the version is neither {@link #VERSION}
     *     nor {@link #VERSION_0} — the same refusal {@link SegmentReader}
     *     already makes for any other unrecognised version.
     */
    public static int directoryEntryBytesFor(int version) throws java.io.IOException {
        return switch (version) {
            case VERSION -> DIRECTORY_ENTRY_BYTES;
            case VERSION_0 -> DIRECTORY_ENTRY_BYTES_V0;
            default -> throw new java.io.IOException("unsupported segment version: " + version);
        };
    }

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
