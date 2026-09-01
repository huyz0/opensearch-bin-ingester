// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import binjava.binstore.BinStore;
import binjava.binstore.Body;
import binjava.binstore.ObjectStat;
import binjava.binstore.Version;
import binjava.format.IndexRegistry;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

/**
 * {@code <prefix>/ctl/registry/indices.json}: the CAS-updated, cached
 * read/write side of {@link IndexRegistry} (research doc 01 §6; ADR-0008,
 * M2.3). {@code IndexRegistry} itself is pure encode/decode; this class is
 * the thing that actually touches {@link BinStore}.
 *
 * <p>⚠️ FIRST-REGISTRATION-WINS. Two ingester pods racing to register the
 * SAME new index must converge on ONE ordinal, not each mint their own —
 * the loser re-reads the winner's write and adopts its assignment rather
 * than retrying into a second ordinal for the same UUID (acceptance
 * criterion 3).
 *
 * <p>⚠️ CACHED INDEFINITELY, revalidated with one {@code stat()} per miss —
 * ADR-0008: "~0/s, only on index creation." An index already known to THIS
 * instance costs zero object-store requests; one not yet known costs a
 * {@code stat()} to check whether another pod registered it first, and a
 * {@code get()} only if the version actually moved.
 *
 * <p>⚠️ NOT THREAD-SAFE BY ACCIDENT — {@code synchronized}, because the
 * read-modify-CAS-retry cycle must not interleave with itself: two virtual
 * threads racing INSIDE one instance would otherwise both compute the same
 * "next ordinal" from the same stale snapshot.
 */
public final class IndexOrdinalRegistry {

    private final BinStore store;
    private final String key;

    private IndexRegistry cached = IndexRegistry.EMPTY;
    private Version cachedVersion;

    public IndexOrdinalRegistry(BinStore store, String prefix) {
        this.store = Objects.requireNonNull(store, "store");
        Objects.requireNonNull(prefix, "prefix");
        this.key = prefix + "/ctl/registry/indices.json";
    }

    /**
     * The ordinal for {@code indexUUID}, registering it (first-registration-wins)
     * if it is not already known.
     */
    public synchronized int ordinalFor(String indexUUID) throws IOException {
        Objects.requireNonNull(indexUUID, "indexUUID");
        Optional<Integer> known = boxed(cached.ordinalFor(indexUUID));
        if (known.isPresent()) {
            // ⚠️ ZERO requests: already known to THIS instance's cache.
            return known.get();
        }
        refresh();
        known = boxed(cached.ordinalFor(indexUUID));
        if (known.isPresent()) {
            return known.get();
        }
        return register(indexUUID);
    }

    private static Optional<Integer> boxed(java.util.OptionalInt o) {
        return o.isPresent() ? Optional.of(o.getAsInt()) : Optional.empty();
    }

    /**
     * Re-reads the registry if the store's copy has moved since this instance
     * last saw it — one {@code stat()}, and a {@code get()} only when the
     * version actually differs.
     */
    private void refresh() throws IOException {
        Optional<ObjectStat> stat = store.stat(key);
        if (stat.isEmpty()) {
            cached = IndexRegistry.EMPTY;
            cachedVersion = null;
            return;
        }
        if (cachedVersion != null && cachedVersion.equals(stat.get().version())) {
            // ⚠️ Already have this exact version cached -- no GET.
            return;
        }
        try (var in = store.get(key)) {
            cached = IndexRegistry.decode(in.readAllBytes());
        }
        cachedVersion = stat.get().version();
    }

    /**
     * First-registration-wins CAS assignment of a new ordinal for
     * {@code indexUUID}, retrying against whatever the losing side of a race
     * left behind.
     */
    private int register(String indexUUID) throws IOException {
        while (true) {
            Optional<Integer> alreadyWon = boxed(cached.ordinalFor(indexUUID));
            if (alreadyWon.isPresent()) {
                // ⚠️ A PRIOR ITERATION's refresh() (below) already picked up
                // someone else's registration of THIS SAME index -- adopt it
                // rather than minting a second ordinal for the same UUID.
                return alreadyWon.get();
            }
            int candidate = cached.nextOrdinal();
            IndexRegistry updated = cached.with(indexUUID, candidate);
            Body body = Body.ofBytes(updated.encode());
            Optional<Version> won = cachedVersion == null
                    ? store.putIfAbsent(key, body)
                    : store.putIfMatch(key, body, cachedVersion);
            if (won.isPresent()) {
                cached = updated;
                cachedVersion = won.get();
                return candidate;
            }
            // ⚠️ LOST THE RACE. Someone else wrote first -- re-read their
            // state (which may or may not include OUR indexUUID) and retry.
            refresh();
        }
    }
}
