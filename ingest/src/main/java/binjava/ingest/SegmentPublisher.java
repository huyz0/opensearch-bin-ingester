// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import binjava.binstore.BinStore;
import binjava.binstore.Body;
import binjava.format.SegmentFormat;
import binjava.format.SegmentKey;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Writes a drained segment to the store (M1.9).
 *
 * <p>⚠️ ONE FLUSH IS ONE PUT. That is the request rate this system exists to
 * control: it scales with flushes, and a flush carries every stream that
 * accumulated during the interval. Nothing here may issue a request per record,
 * per partition or per index (non-negotiable 6).
 *
 * <p>⚠️ The header length is read back OUT of the segment and put INTO the key,
 * so a reader fetches preamble and directory in one ranged GET with no guess.
 * Deriving it from the buffer rather than recomputing it means the key cannot
 * disagree with the bytes.
 */
public final class SegmentPublisher {

    private final BinStore store;
    private final String prefix;
    private final String podShortId;
    private final AtomicLong sequence = new AtomicLong();

    public SegmentPublisher(BinStore store, String prefix, String podShortId) {
        this.store = Objects.requireNonNull(store, "store");
        this.prefix = Objects.requireNonNull(prefix, "prefix");
        this.podShortId = Objects.requireNonNull(podShortId, "podShortId");
        if (podShortId.isBlank()) {
            throw new IllegalArgumentException("podShortId is never blank");
        }
    }

    /**
     * Publishes one segment.
     *
     * @return the key it was written under, or empty if there was nothing to write
     */
    public Optional<String> publish(Accumulator accumulator) throws IOException {
        Optional<byte[]> drained = accumulator.drain();
        if (drained.isEmpty()) {
            // ⚠️ Nothing buffered means NO REQUEST. An idle stream that still
            // PUT an empty object would cost one request per interval forever,
            // which is the criterion-3 failure in its simplest form.
            return Optional.empty();
        }
        byte[] segment = drained.get();
        ByteBuffer view = ByteBuffer.wrap(segment).order(ByteOrder.BIG_ENDIAN);
        int headerLen = view.getInt(8);
        long createdAt = view.getLong(16);

        String key = new SegmentKey(prefix, createdAt, podShortId,
                sequence.getAndIncrement(), headerLen).key();

        // ⚠️ put, not putIfAbsent: the key already contains a pod id and a
        // per-pod sequence, so it is unique WITHOUT coordination. Paying for a
        // conditional write here would buy nothing — the commit log is where
        // write-once matters (M1.10), because that is where two writers can
        // legitimately race for the same slot.
        store.put(key, new Body(segment.length, () -> new ByteArrayInputStream(segment)));
        return Optional.of(key);
    }

    /** The exact byte range a reader needs for preamble plus directory. */
    public static long headerRangeEndInclusive(String key) {
        return SegmentFormat.PREAMBLE_BYTES + SegmentKey.headerLenOf(key) - 1L;
    }
}
