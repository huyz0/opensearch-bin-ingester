// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import binjava.binstore.BinStore;
import binjava.binstore.Body;
import binjava.format.FilterCandidates;
import binjava.format.MembershipFilter;
import binjava.format.SegmentFormat;
import binjava.format.SegmentKey;
import binjava.format.RunKey;
import binjava.format.SegmentReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
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
 *
 * <p>⚠️ THE FILTER (M2.6; ADR-0003) costs NOTHING beyond a genuinely new
 * index's own first-ever registration (ADR-0008: "~0/s, only on index
 * creation") — {@link IndexOrdinalRegistry#registeredCount()} answers from the
 * local cache with zero requests, and {@link IndexOrdinalRegistry#ordinalFor}
 * costs nothing once an index is already known to this pod's registry
 * instance, which is every index after its first flush. Scales with distinct
 * NEW indices, never with records, and never recurs per flush the way the PUT
 * itself does.
 */
public final class SegmentPublisher {

    private final BinStore store;
    private final String prefix;
    private final String podShortId;
    private final IndexOrdinalRegistry ordinals;
    private final AtomicLong sequence = new AtomicLong();

    public SegmentPublisher(BinStore store, String prefix, String podShortId) {
        this.store = Objects.requireNonNull(store, "store");
        this.prefix = Objects.requireNonNull(prefix, "prefix");
        this.podShortId = Objects.requireNonNull(podShortId, "podShortId");
        if (podShortId.isBlank()) {
            throw new IllegalArgumentException("podShortId is never blank");
        }
        // ⚠️ Same restriction SegmentKey's own constructor enforces (round-1
        // review, M2.6) -- checked HERE too so a bad podShortId fails at
        // construction, not silently deferred to this publisher's first flush.
        if (podShortId.indexOf('-') >= 0 || podShortId.indexOf('/') >= 0) {
            throw new IllegalArgumentException(
                    "podShortId may not contain '-' or '/': " + podShortId);
        }
        // ⚠️ ONE instance for this publisher's whole lifetime, not one per
        // flush: the registry's own cache (ADR-0008, "cached indefinitely")
        // only pays off if it is reused across every flush this pod ever does.
        this.ordinals = new IndexOrdinalRegistry(store, prefix);
    }

    /**
     * What one flush produced: the key it was stored under, the bytes, and how
     * many records each stream contributed.
     *
     * <p>⚠️ All three, because the caller needs all three and deriving them
     * twice is how they diverge. The commit log needs the counts to assign
     * offsets, the subscription hub needs the bytes to push inline, and neither
     * can be recovered from the key alone without a GET — a request the write
     * path must not spend.
     */
    public record Published(String key, byte[] segment, Map<RunKey, Integer> recordCounts) {

        public Published {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(segment, "segment");
            recordCounts = Map.copyOf(recordCounts);
        }
    }

    /**
     * Publishes one segment.
     *
     * @return what was written, or empty if there was nothing to write
     */
    public Optional<Published> publish(Accumulator accumulator) throws IOException {
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

        // ⚠️ Read back out of the SEGMENT rather than counted on the way in.
        // The directory is what a consumer will read, so counting the bytes that
        // were actually written is the only count that cannot disagree with it.
        // Read ONCE, before the key is built -- the filter needs the same
        // distinct-index set the counts are built from, and a second parse of
        // bytes already in hand would buy nothing.
        var directory = SegmentReader.open(segment).directory();
        Map<RunKey, Integer> counts = new LinkedHashMap<>();
        Set<UUID> distinctIndices = new HashSet<>();
        for (var entry : directory) {
            counts.put(entry.key(), entry.recordCount());
            distinctIndices.add(entry.key().indexId());
        }

        // ⚠️ Read ONCE, before the filter is chosen: the budget the filter is
        // judged against depends on this SAME sequence number (M2.8, round-1
        // test review), so it must be the one that ends up in the key, not a
        // second draw that would desynchronize the two.
        long thisSequence = sequence.getAndIncrement();
        MembershipFilter filter = chooseFilter(distinctIndices, createdAt, thisSequence, headerLen);

        String key = new SegmentKey(prefix, createdAt, podShortId,
                thisSequence, headerLen, filter.encode()).key();

        // ⚠️ put, not putIfAbsent: the key already contains a pod id and a
        // per-pod sequence, so it is unique WITHOUT coordination. Paying for a
        // conditional write here would buy nothing — the commit log is where
        // write-once matters (M1.10), because that is where two writers can
        // legitimately race for the same slot.
        store.put(key, new Body(segment.length, () -> new ByteArrayInputStream(segment)));

        return Optional.of(new Published(key, segment, counts));
    }

    /** The exact byte range a reader needs for preamble plus directory. */
    public static long headerRangeEndInclusive(String key) {
        return SegmentFormat.PREAMBLE_BYTES + SegmentKey.headerLenOf(key) - 1L;
    }

    /**
     * Resolves each distinct index's ordinal (typically free -- see the class
     * javadoc) and picks the shortest-fitting filter (M2.6; ADR-0003) against
     * the REAL remaining key budget for THIS key's own fields, not a fixed
     * guess (M2.8, round-1 test review: a fixed 900-byte budget let a filter
     * that "fit" its own assumption still overflow the whole key once
     * combined with a real prefix and pod id).
     */
    private MembershipFilter chooseFilter(Set<UUID> distinctIndices, long timestampMillis,
            long sequenceValue, int headerLen) throws IOException {
        Set<Integer> memberOrdinals = new HashSet<>();
        for (UUID indexId : distinctIndices) {
            memberOrdinals.add(ordinals.ordinalFor(indexId.toString()));
        }
        int totalRegistered = ordinals.registeredCount();
        int budgetBytes = SegmentKey.filterBudgetBytes(
                prefix, timestampMillis, podShortId, sequenceValue, headerLen);
        return FilterCandidates.chooseShortestFitting(memberOrdinals, totalRegistered, budgetBytes);
    }
}
