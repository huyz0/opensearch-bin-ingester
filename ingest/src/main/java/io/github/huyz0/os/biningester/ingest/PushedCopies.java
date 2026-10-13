// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.format.RunKey;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The fast entries pushed to this pod, kept by their fast keys for a proxied
 * read before their upload (ADR-0081 §6, ADR-0082 §6; M13.30l): each dropped
 * as a holder drops a released copy, once its stream's committed next offset
 * passes it.
 *
 * <p>⚠️ BOUNDED IN BYTES, OLDEST FIRST: a copy evicted early is answered
 * {@code 404}, as a released one is, and its subscriber reads the committed
 * segment -- best effort, as publication is. One larger than the bound is
 * never kept.
 */
public final class PushedCopies {

    /** The bytes kept when no other bound is given: a few uploads' worth. */
    public static final long DEFAULT_MAX_BYTES = 64L << 20;

    private record Copy(RunKey stream, long end, byte[] bytes) {
    }

    private final long maxBytes;
    private final Map<String, Copy> copies = new LinkedHashMap<>();
    private long heldBytes;

    public PushedCopies(long maxBytes) {
        if (maxBytes <= 0) {
            throw new IllegalArgumentException("a bound is positive: " + maxBytes);
        }
        this.maxBytes = maxBytes;
    }

    /** Keeps {@code bytes}, the entry of {@code stream} at {@code firstOffset}, under its key. */
    public synchronized void put(String fastKey, RunKey stream, long firstOffset,
            int recordCount, byte[] bytes) {
        Objects.requireNonNull(fastKey, "fastKey");
        Objects.requireNonNull(stream, "stream");
        Objects.requireNonNull(bytes, "bytes");
        if (bytes.length > maxBytes) {
            return;
        }
        remove(copies.remove(fastKey));
        copies.put(fastKey, new Copy(stream, firstOffset + recordCount, bytes));
        heldBytes += bytes.length;
        Iterator<Copy> oldest = copies.values().iterator();
        while (heldBytes > maxBytes && oldest.hasNext()) {
            heldBytes -= oldest.next().bytes().length;
            oldest.remove();
        }
    }

    /** The copy kept under {@code fastKey}, if any. */
    public synchronized Optional<byte[]> copy(String fastKey) {
        return Optional.ofNullable(copies.get(fastKey)).map(Copy::bytes);
    }

    /** Drops every copy of {@code stream} wholly below {@code below}. */
    public synchronized void release(RunKey stream, long below) {
        Iterator<Copy> all = copies.values().iterator();
        while (all.hasNext()) {
            Copy c = all.next();
            if (c.stream().equals(stream) && c.end() <= below) {
                heldBytes -= c.bytes().length;
                all.remove();
            }
        }
    }

    /** The streams a copy is kept of. */
    public synchronized Set<RunKey> streams() {
        return copies.values().stream().map(Copy::stream).collect(Collectors.toSet());
    }

    synchronized long heldBytes() {
        return heldBytes;
    }

    private void remove(Copy replaced) {
        if (replaced != null) {
            heldBytes -= replaced.bytes().length;
        }
    }
}
