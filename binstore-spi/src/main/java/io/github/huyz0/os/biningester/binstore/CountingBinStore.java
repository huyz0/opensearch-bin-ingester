// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.binstore;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;

/**
 * Counts every request a delegate issues (cost rule R9).
 *
 * <p>⚠️ THE METER IS A DECORATOR so no backend can forget to call it. Counting
 * inside each implementation would put the same code in eight places and make
 * "did we count this one?" a per-backend question — and the one that forgets is
 * the one whose bill surprises somebody.
 *
 * <p>⚠️ A REQUEST IS COUNTED EVEN WHEN IT FAILS. S3 bills a 500 and a 412 like
 * any other request, and the commit-log retry loop (ADR-0002) is built out of
 * losing races. Counting only successes would under-report exactly the workload
 * that costs most.
 *
 * <p>⚠️ Counted BEFORE delegating, so a throw cannot skip the increment.
 */
public final class CountingBinStore implements BinStore {

    private final BinStore delegate;
    private final LongAdder dataPuts = new LongAdder();
    private final LongAdder commitPuts = new LongAdder();
    private final LongAdder checkpointPuts = new LongAdder();
    private final LongAdder leasePuts = new LongAdder();
    private final LongAdder otherPuts = new LongAdder();
    private final LongAdder gets = new LongAdder();
    private final LongAdder lists = new LongAdder();
    private final LongAdder stats = new LongAdder();
    private final LongAdder deletes = new LongAdder();

    public CountingBinStore(BinStore delegate) {
        this.delegate = java.util.Objects.requireNonNull(delegate, "delegate");
    }

    /** What has been issued so far. */
    public StoreCounts counts() {
        return new StoreCounts(putPurposeCounts().total(), gets.sum(), lists.sum(), stats.sum(),
                deletes.sum());
    }

    /** PUT counts by bounded object-key purpose; their sum equals {@link #counts()}'s PUTs. */
    public PutPurposeCounts putPurposeCounts() {
        return new PutPurposeCounts(dataPuts.sum(), commitPuts.sum(), checkpointPuts.sum(),
                leasePuts.sum(), otherPuts.sum());
    }

    private void countPut(String key) {
        if (hasPathSegment(key, "/ctl/log/", "ctl/log/")
                && hasPathSegment(key, "/ckpt/", "ckpt/")
                && (key.endsWith(".ckpt") || key.endsWith("/LATEST"))) {
            checkpointPuts.increment();
        } else if (hasPathSegment(key, "/ctl/log/", "ctl/log/")
                && key.endsWith(".delta")) {
            commitPuts.increment();
        } else if (hasPathSegment(key, "/ctl/lease/", "ctl/lease/")
                && key.endsWith(".json")) {
            leasePuts.increment();
        } else if (isDataSegment(key)) {
            dataPuts.increment();
        } else {
            otherPuts.increment();
        }
    }

    /**
     * Whether {@code key} names a data segment -- the one classifier this
     * meter and {@code GoverningBinStore} share, so the governed series and
     * the counted one cannot drift apart.
     */
    static boolean isDataSegment(String key) {
        return hasPathSegment(key, "/data/", "data/") && key.endsWith(".bseg");
    }

    private static boolean hasPathSegment(String key, String nested, String root) {
        return key.startsWith(root) || key.contains(nested);
    }

    @Override
    public InputStream get(String key) throws IOException {
        gets.increment();
        return delegate.get(key);
    }

    @Override
    public InputStream getRange(String key, long start, long endIncl) throws IOException {
        // ⚠️ A ranged read is a GET. Cost rule R4 is about not SPLITTING one
        // coalesced read into many; a range that replaces a whole-object read is
        // the same single request.
        gets.increment();
        return delegate.getRange(key, start, endIncl);
    }

    @Override
    public Optional<ObjectStat> stat(String key) throws IOException {
        stats.increment();
        return delegate.stat(key);
    }

    @Override
    public Version put(String key, Body body) throws IOException {
        countPut(key);
        return delegate.put(key, body);
    }

    @Override
    public Optional<Version> putIfAbsent(String key, Body body) throws IOException {
        // ⚠️ A LOST race is still a request. The commit log is a chain of these
        // and the losers are billed, so counting only the winner would report a
        // contended cluster as costing the same as an idle one.
        countPut(key);
        return delegate.putIfAbsent(key, body);
    }

    @Override
    public Optional<Version> putIfMatch(String key, Body body, Version expected) throws IOException {
        // ⚠️ A LOST match is still a request, same reasoning as putIfAbsent: a
        // lease renewal or registry update that lost the race was still billed.
        countPut(key);
        return delegate.putIfMatch(key, body, expected);
    }

    @Override
    public MultipartWriter multipart(String key) throws IOException {
        // ⚠️ CreateMultipartUpload is a real request too, same as every other
        // write here -- and the returned writer's own operations (UploadPart,
        // CompleteMultipartUpload, AbortMultipartUpload) are each billed calls
        // in their own right, so the writer itself must be decorated, not just
        // this one call.
        countPut(key);
        return new CountingMultipartWriter(key, delegate.multipart(key));
    }

    /**
     * ⚠️ Tracks {@code done} itself, rather than trusting the delegate's own
     * internal state, so {@link #close()} counts an IMPLICIT abort (a caller
     * that never called {@link #complete()} or {@link #abort()} explicitly)
     * exactly once -- a real AbortMultipartUpload request -- without double
     * counting when {@code close()} follows a {@code complete()} or {@code
     * abort()} that already ran and was already counted.
     */
    private final class CountingMultipartWriter implements MultipartWriter {
        private final String key;
        private final MultipartWriter delegate;
        private volatile boolean done;

        CountingMultipartWriter(String key, MultipartWriter delegate) {
            this.key = key;
            this.delegate = delegate;
        }

        @Override
        public void uploadPart(int partNumber, Body body) throws IOException {
            countPut(key);
            delegate.uploadPart(partNumber, body);
        }

        @Override
        public Version complete() throws IOException {
            countPut(key);
            // ⚠️ `done` is set only on the SUCCESS path. Round-1 review found
            // this set unconditionally, before delegating: when complete()
            // THROWS (e.g. a minPartSize violation, which by design is only
            // catchable at complete()-time), the underlying upload is still
            // open, and a caller's ordinary try-with-resources unwind then
            // calls close() -- which performs a REAL implicit abort (real
            // cleanup work in LocalFsBinStore's case) that must still be
            // counted. Marking `done` before the delegate call let that second,
            // genuinely separate request go uncounted.
            Version v = delegate.complete();
            done = true;
            return v;
        }

        @Override
        public void abort() throws IOException {
            countPut(key);
            // ⚠️ Same reasoning as complete(): only mark done on success, so a
            // failing abort() does not silently swallow a subsequent close()'s
            // own cleanup attempt.
            delegate.abort();
            done = true;
        }

        @Override
        public void close() throws IOException {
            if (!done) {
                // ⚠️ A close() that never saw complete()/abort() still issues a
                // real AbortMultipartUpload -- MultipartWriter's own contract
                // ("close() behaves exactly like abort()") makes this a real
                // request, not free cleanup.
                countPut(key);
                done = true;
            }
            delegate.close();
        }
    }

    @Override
    public ListPage list(String prefix, String startAfter, int maxKeys) throws IOException {
        // ⚠️ ONE PER PAGE, which is what makes this meter honest. When `list`
        // returned a lazy Stream the decorator saw one invocation whether the
        // backend issued one request or ten thousand (ADR-0022).
        lists.increment();
        return delegate.list(prefix, startAfter, maxKeys);
    }

    @Override
    public void delete(List<String> keys) throws IOException {
        // ⚠️ ONE per call, not one per key: a batch delete IS one request, and
        // counting keys would make GC look proportional to objects when the
        // bill is proportional to calls.
        deletes.increment();
        delegate.delete(keys);
    }

    /**
     * ⚠️ FORWARDED, AND NOT COUNTED. Signing issues no object-store request
     * (ADR-0041), so counting it would put a floor under every request-rate
     * criterion that this class exists to measure.
     *
     * <p>⚠️ FORGETTING THIS ONE WAS A REAL DEFECT, not a hypothetical: review
     * MEASURED a capable backend behind this meter passing
     * {@code requirePresignedUrls()} at startup and then throwing
     * {@code UnsupportedOperationException} at the first {@code direct} fetch,
     * blaming a backend that could in fact sign. A {@code default} method on
     * the SPI cannot force a decorator to forward it -- which is why
     * {@code CountingBinStoreTest} now runs the whole conformance suite rather
     * than relying on anyone remembering.
     */
    @Override
    public SignedUrl presign(String key, java.time.Duration ttl) throws java.io.IOException {
        return delegate.presign(key, ttl);
    }

    @Override
    public Capabilities capabilities() {
        // ⚠️ NOT counted. Capabilities are read at startup from local state and
        // issue no request; counting them would put a floor under every
        // zero-idle-cost assertion.
        return delegate.capabilities();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
