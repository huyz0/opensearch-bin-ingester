// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import binjava.binstore.BinStore;
import binjava.binstore.Body;
import binjava.binstore.CountingBinStore;
import binjava.binstore.backend.MemoryBinStore;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Fixtures shared by {@link SegmentProxyTest} and
 * {@link SegmentProxyFailureTest}.
 *
 * <p>⚠️ SPLIT OUT WHEN {@code SegmentProxyTest} CROSSED 700 LINES --
 * code-structure.md rule 1, split rather than raise, the same way
 * {@code DeadPeerTest} was split from {@code FleetSequencerTest}. The seam is
 * real rather than convenient: one file asks what {@code proxy} costs when
 * everything works, the other what it does when a consumer or the store fails,
 * and the two share only their scaffolding.
 *
 * <p>⚠️ {@link StubStore} IS THE REASON THIS FILE IS WORTH HAVING.
 * {@code MemoryBinStore} cannot produce a short read, a zero read, a mid-read
 * failure, or an observable {@code close()} -- and review measured a separate
 * production defect hiding behind each of those four absences.
 */
final class SegmentProxyFixtures {

    private SegmentProxyFixtures() {
    }

    static final String KEY = "seg/proxy";

    /** 1 MiB, so a buffer-then-forward hand-off is 16x the 64 KiB chunk. */
    static final int SEGMENT_BYTES = 1024 * 1024;

    static byte[] segment() {
        byte[] b = new byte[SEGMENT_BYTES];
        for (int i = 0; i < b.length; i++) {
            // ⚠️ NOT all-zero and not constant: a proxy that delivered the
            // right COUNT of the wrong bytes would pass a length check, and
            // a repeating byte would hide a chunk delivered twice or skipped.
            b[i] = (byte) (i * 31 + (i >> 8));
        }
        return b;
    }

    /** Records every hand-off it is given, and the largest of them. */
    static final class RecordingSink implements SegmentSink {
        final ByteArrayOutputStream received = new ByteArrayOutputStream();
        final java.util.Set<Integer> arrayIdentities = new java.util.HashSet<>();
        int largestHandOff;
        int handOffs;
        int emptyHandOffs;

        @Override
        public void write(byte[] buffer, int offset, int length) {
            largestHandOff = Math.max(largestHandOff, length);
            handOffs++;
            // ⚠️ COUNTED, because nothing else here can see one. A zero-length
            // hand-off writes nothing, does not move `largestHandOff`, and
            // leaves the received bytes identical -- so `if (read == 0)
            // continue;` was deletable with all sixteen tests green until this
            // counter existed. Round-3 review measured it.
            if (length == 0) {
                emptyHandOffs++;
            }
            arrayIdentities.add(System.identityHashCode(buffer));
            received.write(buffer, offset, length);
        }
    }

    static CountingBinStore storeHolding(byte[] bytes) throws IOException {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        store.put(KEY, Body.ofBytes(bytes));
        return store;
    }

    static List<RecordingSink> sinks(int k) {
        List<RecordingSink> all = new ArrayList<>();
        for (int i = 0; i < k; i++) {
            all.add(new RecordingSink());
        }
        return all;
    }


    /**
     * A {@link BinStore} whose stream reports how much has been read, records
     * being closed, and can return SHORT and ZERO reads.
     *
     * <p>⚠️ IT EXISTS BECAUSE {@code MemoryBinStore} CANNOT SHOW ANY OF THIS.
     * Its {@code ByteArrayInputStream} always fills the buffer, never returns
     * 0, and has a {@code close()} that does nothing -- so three separate
     * production behaviours were invisible to every test in this file, and
     * round-2 review measured all three surviving. A real HTTP-backed stream
     * produces short and zero reads routinely.
     */
    static final class StubStore implements BinStore {
        final byte[] content;
        final long[] readSoFar;
        final int shortEvery;
        final int failAtRead;
        boolean streamClosed;
        int reads;

        StubStore(byte[] content, long[] readSoFar, int shortEvery) {
            this(content, readSoFar, shortEvery, 0);
        }

        StubStore(byte[] content, long[] readSoFar, int shortEvery, int failAtRead) {
            this.content = content;
            this.readSoFar = readSoFar;
            this.shortEvery = shortEvery;
            this.failAtRead = failAtRead;
        }

        @Override public java.io.InputStream get(String key) {
            return new java.io.FilterInputStream(new java.io.ByteArrayInputStream(content)) {
                @Override public void close() throws IOException {
                    // ⚠️ THE STREAM, NOT THE STORE. An earlier version of this
                    // stub recorded the STORE handle closing, which
                    // `SegmentProxy` correctly never does -- so the flag was
                    // permanently false and never read. Round-3 review found
                    // it, and the real close-recording lived in a second,
                    // near-identical anonymous stub that has now been deleted.
                    streamClosed = true;
                    super.close();
                }

                @Override public int read(byte[] b, int off, int len) throws IOException {
                    reads++;
                    if (failAtRead > 0 && reads >= failAtRead) {
                        throw new IOException("the store went away mid-segment");
                    }
                    // ⚠️ A ZERO-LENGTH READ THAT IS NOT END OF STREAM, which
                    // `InputStream` permits and a socket produces when nothing
                    // has arrived yet. It must not be mistaken for the end.
                    if (shortEvery > 0 && reads % shortEvery == 0) {
                        return 0;
                    }
                    int cap = shortEvery > 0 ? Math.min(len, 1 + (reads * 37) % len) : len;
                    int n = super.read(b, off, cap);
                    if (n > 0) {
                        readSoFar[0] += n;
                    }
                    return n;
                }
            };
        }

        @Override public void close() {
        }

        @Override public java.io.InputStream getRange(String k, long a, long b) {
            throw new UnsupportedOperationException();
        }

        @Override public java.util.Optional<binjava.binstore.ObjectStat> stat(String k) {
            throw new UnsupportedOperationException();
        }

        @Override public binjava.binstore.Version put(String k, Body b) {
            throw new UnsupportedOperationException();
        }

        @Override public java.util.Optional<binjava.binstore.Version> putIfAbsent(String k, Body b) {
            throw new UnsupportedOperationException();
        }

        @Override public java.util.Optional<binjava.binstore.Version> putIfMatch(
                String k, Body b, binjava.binstore.Version v) {
            throw new UnsupportedOperationException();
        }

        @Override public binjava.binstore.MultipartWriter multipart(String k) {
            throw new UnsupportedOperationException();
        }

        @Override public binjava.binstore.ListPage list(String p, String a, int m) {
            throw new UnsupportedOperationException();
        }

        @Override public void delete(List<String> keys) {
            throw new UnsupportedOperationException();
        }

        @Override public binjava.binstore.Capabilities capabilities() {
            return new MemoryBinStore().capabilities();
        }
    }
}
