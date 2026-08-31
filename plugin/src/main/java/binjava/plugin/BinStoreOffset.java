// SPDX-License-Identifier: Apache-2.0
package binjava.plugin;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.search.Query;
import org.opensearch.index.IngestionShardPointer;

/**
 * The pointer OpenSearch stores in every document: a single logical offset.
 *
 * <p>WARNING: A SINGLE LONG, deliberately. The pointer becomes a Lucene point
 * field with a range query, which rules out a composite like
 * {@code (objectId, byteOffset)}: composites are awkward to range-query and, worse,
 * are NOT STABLE ACROSS COMPACTION -- the object holding a record changes, while
 * the record's position in the log must not. The mapping from offset to
 * (object, byte range) lives in the commit log, not in the pointer (ADR-0001).
 *
 * <p>WARNING: {@code asString} is zero-padded so the string form sorts in the
 * same order as the numeric one. OpenSearch persists this string as
 * {@code batch_start} and compares pointers after a restart; an unpadded "10"
 * sorts before "9" and a resume would silently rewind.
 */
public final class BinStoreOffset implements IngestionShardPointer {

    /** Wide enough for any non-negative long. */
    private static final int STRING_WIDTH = 19;

    private final long offset;

    public BinStoreOffset(long offset) {
        if (offset < 0) {
            throw new IllegalArgumentException("offsets are never negative: " + offset);
        }
        this.offset = offset;
    }

    public long offset() {
        return offset;
    }

    public static BinStoreOffset fromString(String value) {
        return new BinStoreOffset(Long.parseLong(value.trim()));
    }

    @Override
    public byte[] serialize() {
        return ByteBuffer.allocate(Long.BYTES).order(ByteOrder.BIG_ENDIAN).putLong(offset).array();
    }

    public static BinStoreOffset deserialize(byte[] bytes) {
        if (bytes == null || bytes.length != Long.BYTES) {
            throw new IllegalArgumentException("a serialized offset is 8 bytes");
        }
        return new BinStoreOffset(
                ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getLong());
    }

    @Override
    public String asString() {
        return String.format("%0" + STRING_WIDTH + "d", offset);
    }

    @Override
    public Field asPointField(String fieldName) {
        return new LongPoint(fieldName, offset);
    }

    @Override
    public Query newRangeQueryGreaterThan(String fieldName) {
        // WARNING: STRICTLY greater. Including the start would replay the last
        // record of every batch on every resume -- duplicates OpenSearch cannot
        // detect, because they carry the same _id and a later _version.
        return LongPoint.newRangeQuery(fieldName, offset + 1, Long.MAX_VALUE);
    }

    @Override
    public int compareTo(IngestionShardPointer other) {
        if (!(other instanceof BinStoreOffset o)) {
            throw new IllegalArgumentException(
                    "cannot compare a BinStoreOffset with " + other.getClass().getName());
        }
        return Long.compare(offset, o.offset);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof BinStoreOffset other && other.offset == offset;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(offset);
    }

    @Override
    public String toString() {
        return "BinStoreOffset[" + offset + "]";
    }

    /** UTF-8 bytes of the padded string form, for logging seams that want text. */
    public byte[] asStringBytes() {
        return asString().getBytes(StandardCharsets.UTF_8);
    }
}
