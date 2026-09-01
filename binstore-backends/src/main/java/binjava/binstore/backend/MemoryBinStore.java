// SPDX-License-Identifier: Apache-2.0
package binjava.binstore.backend;

import binjava.binstore.BinStore;
import binjava.binstore.Body;
import binjava.binstore.Capabilities;
import binjava.binstore.CostTable;
import binjava.binstore.ListPage;
import binjava.binstore.MultipartWriter;
import binjava.binstore.ObjectStat;
import binjava.binstore.Version;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An in-memory {@link BinStore}, for tests and for the conformance suite's own
 * baseline. // SKELETON: never a production backend
 *
 * <p>⚠️ A {@link ConcurrentSkipListMap}, not a HashMap, because {@code list} must
 * return keys in lexicographic order and resume strictly after a given key —
 * recovery walks that listing in order, and an unordered map would pass a
 * count-based test while breaking resumption.
 */
public final class MemoryBinStore implements BinStore {

    private record Entry(byte[] data, Version version) {}

    private final ConcurrentSkipListMap<String, Entry> objects = new ConcurrentSkipListMap<>();
    private final AtomicLong versions = new AtomicLong();

    private Version nextVersion() {
        // ⚠️ Monotonic, so a version CHANGES on every write. A constant token
        // would let every compare-and-set succeed against stale data.
        return new Version(Long.toString(versions.incrementAndGet()));
    }

    private Entry require(String key) throws IOException {
        Entry e = objects.get(key);
        if (e == null) {
            // ⚠️ Not empty bytes: a zero-length object and a missing one must be
            // distinguishable, or a segment reader parses absence as a valid
            // empty segment.
            throw new IOException("no such key: " + key);
        }
        return e;
    }

    @Override
    public InputStream get(String key) throws IOException {
        return new ByteArrayInputStream(require(key).data());
    }

    @Override
    public InputStream getRange(String key, long start, long endIncl) throws IOException {
        byte[] data = require(key).data();
        if (start < 0 || endIncl < start) {
            throw new IOException("range [" + start + "," + endIncl + "] is not a range");
        }
        if (start >= data.length) {
            throw new IOException(
                    "range starts at " + start + " but " + key + " is " + data.length + " bytes");
        }
        // ⚠️ CLAMP past the end, as S3, GCS and Azure all do. Throwing instead
        // would force every caller into stat-then-getRange, doubling the request
        // count per segment read (R7) to learn something the read itself knows.
        long last = Math.min(endIncl, data.length - 1);
        // ⚠️ endIncl is INCLUSIVE, hence the +1. Off by one here truncates the
        // last byte of every run read through a segment directory.
        return new ByteArrayInputStream(data, (int) start, (int) (last - start + 1));
    }

    @Override
    public Optional<ObjectStat> stat(String key) {
        Entry e = objects.get(key);
        return e == null ? Optional.empty()
                         : Optional.of(new ObjectStat(key, e.data().length, e.version()));
    }

    private static final long MAX_KEY_BYTES = 1024;

    private void checkKey(String key) throws IOException {
        // ⚠️ ENFORCED, so the conformance suite can PROBE the advertised limit
        // rather than take it on advertisement. A backend that claims 1024 and
        // rejects at 255 passes an isPositive() assertion.
        if (key.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > MAX_KEY_BYTES) {
            throw new IOException("key longer than " + MAX_KEY_BYTES + " bytes: " + key.length());
        }
    }

    @Override
    public Version put(String key, Body body) throws IOException {
        checkKey(key);
        Version v = nextVersion();
        objects.put(key, new Entry(body.readFully(), v));
        return v;
    }

    @Override
    public Optional<Version> putIfAbsent(String key, Body body) throws IOException {
        checkKey(key);
        byte[] data = body.readFully();
        Version v = nextVersion();
        // ⚠️ ATOMIC. The whole coordination substrate rests on this (ADR-0002);
        // a get-then-put would let two writers both observe absence and the
        // loser would overwrite the winner's record with no error.
        Entry existing = objects.putIfAbsent(key, new Entry(data, v));
        return existing == null ? Optional.of(v) : Optional.empty();
    }

    @Override
    public Optional<Version> putIfMatch(String key, Body body, Version expected) throws IOException {
        checkKey(key);
        java.util.Objects.requireNonNull(expected, "expected");
        Entry current = objects.get(key);
        if (current == null) {
            // ⚠️ NOT empty. There is no version to have moved from -- an empty
            // Optional is reserved for a genuine lost race against a real prior
            // write, and folding the two together would make a lease renewal
            // unable to tell "someone beat me to it" from "this was never
            // written" (ADR-0008).
            throw new IOException("no such key: " + key + " (nothing to match version against)");
        }
        if (!current.version().equals(expected)) {
            // ⚠️ CHECKED BEFORE the body is touched. round-1 review (M2.0) found
            // this reading body.readFully() first, so a version-mismatch against
            // a body whose declared length disagreed with its stream threw
            // IOException instead of returning empty -- contradicting this
            // method's own contract ("never an exception" on a moved version) and
            // diverging from LocalFsBinStore, which already checked the version
            // first. A write already known to be stale must not pay for reading a
            // body it is about to discard, and must not let that body's own
            // defects surface as the wrong kind of failure.
            return Optional.empty();
        }
        byte[] data = body.readFully();
        Version next = nextVersion();
        // ⚠️ ATOMIC, same reasoning as putIfAbsent: replace(key, oldValue,
        // newValue) succeeds only if the map's CURRENT mapping is reference-equal
        // to the Entry this call just read. A writer that races in between the
        // version check above and this call changes that mapping, so a second
        // writer computing the SAME expected version cannot also win --
        // ConcurrentSkipListMap.replace(K,V,V) is the compare-and-set, not the
        // version-equality check above, which only short-circuits a write
        // already known to be stale.
        boolean replaced = objects.replace(key, current, new Entry(data, next));
        return replaced ? Optional.of(next) : Optional.empty();
    }

    @Override
    public MultipartWriter multipart(String key) throws IOException {
        checkKey(key);
        return new MemoryMultipartWriter(key);
    }

    /**
     * ⚠️ Parts keyed by number in a {@link java.util.concurrent.ConcurrentSkipListMap},
     * not a list, for the same reason {@link #objects} is one: {@link #complete}
     * must assemble in PART-NUMBER order, never arrival order, and re-uploading a
     * part number must replace it rather than append a duplicate.
     */
    private final class MemoryMultipartWriter implements MultipartWriter {
        private final String key;
        private final ConcurrentSkipListMap<Integer, byte[]> parts = new ConcurrentSkipListMap<>();
        private volatile boolean done;

        MemoryMultipartWriter(String key) {
            this.key = key;
        }

        @Override
        public void uploadPart(int partNumber, Body body) throws IOException {
            if (partNumber < 1) {
                throw new IOException("part numbers start at 1: " + partNumber);
            }
            parts.put(partNumber, body.readFully());
        }

        @Override
        public Version complete() throws IOException {
            if (parts.isEmpty()) {
                throw new IOException("multipart upload of " + key + " has no parts to assemble");
            }
            // ⚠️ ONLY the highest-numbered part is exempt from minPartSize --
            // which part is last is not knowable until complete(), since a
            // caller may still be about to upload a higher-numbered one when an
            // earlier part arrives.
            int lastPartNumber = parts.lastKey();
            long minPartSize = capabilities().minPartSize();
            for (var e : parts.entrySet()) {
                if (e.getKey() != lastPartNumber && e.getValue().length < minPartSize) {
                    throw new IOException("part " + e.getKey() + " of " + key + " is "
                            + e.getValue().length + " bytes, below the minimum non-final part size "
                            + minPartSize + " bytes");
                }
            }
            java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
            for (byte[] part : parts.values()) {
                out.write(part, 0, part.length);
            }
            Version v = put(key, Body.ofBytes(out.toByteArray()));
            done = true;
            parts.clear();
            return v;
        }

        @Override
        public void abort() {
            done = true;
            parts.clear();
        }

        @Override
        public void close() {
            // ⚠️ close() alone is NOT an implicit complete() -- MultipartWriter's
            // own contract. `done` guards against re-clearing after an explicit
            // complete()/abort() already ran, though clearing twice is itself
            // harmless here; kept for symmetry with LocalFsBinStore, where it
            // is NOT harmless (see there).
            if (!done) {
                parts.clear();
            }
        }
    }

    @Override
    public ListPage list(String prefix, String startAfter, int maxKeys) {
        // ⚠️ Seek from the LATER of prefix and startAfter. Seeking from
        // startAfter alone returned NOTHING whenever it sorted before the
        // prefix -- takeWhile stopped at the first non-matching key -- so
        // list("p/", "") reported an empty store while p/a..p/c existed, and
        // recovery would conclude "nothing to recover" with no error. S3 ignores
        // a below-prefix start-after; so does this.
        var from = startAfter == null || startAfter.compareTo(prefix) < 0
                ? objects.tailMap(prefix, true)
                : objects.tailMap(startAfter, false);
        List<ObjectStat> page = new java.util.ArrayList<>();
        String next = null;
        for (var e : from.entrySet()) {
            if (!e.getKey().startsWith(prefix)) {
                break;
            }
            if (page.size() == maxKeys) {
                next = page.get(page.size() - 1).key();
                break;
            }
            page.add(new ObjectStat(e.getKey(), e.getValue().data().length, e.getValue().version()));
        }
        return new ListPage(page, Optional.ofNullable(next));
    }

    @Override
    public void delete(List<String> keys) {
        // ⚠️ An absent key is not an error: GC re-runs after a crash and would
        // otherwise fail permanently on its own completed work.
        keys.forEach(objects::remove);
    }

    @Override
    public Capabilities capabilities() {
        return new Capabilities(true, true, MAX_KEY_BYTES, 5L * 1024 * 1024, CostTable.free());
    }

    @Override
    public void close() {
        objects.clear();
    }
}
