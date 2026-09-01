// SPDX-License-Identifier: Apache-2.0
package binjava.binstore;

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
    private final LongAdder puts = new LongAdder();
    private final LongAdder gets = new LongAdder();
    private final LongAdder lists = new LongAdder();
    private final LongAdder stats = new LongAdder();
    private final LongAdder deletes = new LongAdder();

    public CountingBinStore(BinStore delegate) {
        this.delegate = java.util.Objects.requireNonNull(delegate, "delegate");
    }

    /** What has been issued so far. */
    public StoreCounts counts() {
        return new StoreCounts(puts.sum(), gets.sum(), lists.sum(), stats.sum(), deletes.sum());
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
        puts.increment();
        return delegate.put(key, body);
    }

    @Override
    public Optional<Version> putIfAbsent(String key, Body body) throws IOException {
        // ⚠️ A LOST race is still a request. The commit log is a chain of these
        // and the losers are billed, so counting only the winner would report a
        // contended cluster as costing the same as an idle one.
        puts.increment();
        return delegate.putIfAbsent(key, body);
    }

    @Override
    public Optional<Version> putIfMatch(String key, Body body, Version expected) throws IOException {
        // ⚠️ A LOST match is still a request, same reasoning as putIfAbsent: a
        // lease renewal or registry update that lost the race was still billed.
        puts.increment();
        return delegate.putIfMatch(key, body, expected);
    }

    @Override
    public MultipartWriter multipart(String key) throws IOException {
        // ⚠️ CreateMultipartUpload is a real request too, same as every other
        // write here -- and the returned writer's own operations (UploadPart,
        // CompleteMultipartUpload, AbortMultipartUpload) are each billed calls
        // in their own right, so the writer itself must be decorated, not just
        // this one call.
        puts.increment();
        return new CountingMultipartWriter(delegate.multipart(key));
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
        private final MultipartWriter delegate;
        private volatile boolean done;

        CountingMultipartWriter(MultipartWriter delegate) {
            this.delegate = delegate;
        }

        @Override
        public void uploadPart(int partNumber, Body body) throws IOException {
            puts.increment();
            delegate.uploadPart(partNumber, body);
        }

        @Override
        public Version complete() throws IOException {
            puts.increment();
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
            puts.increment();
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
                puts.increment();
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
