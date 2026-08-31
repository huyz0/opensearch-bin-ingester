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
