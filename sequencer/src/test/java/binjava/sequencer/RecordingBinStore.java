// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import binjava.binstore.BinStore;
import binjava.binstore.Body;
import binjava.binstore.ListPage;
import binjava.binstore.ObjectStat;
import binjava.binstore.Version;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Records the KEY and BYTES of every PUT, in order.
 *
 * <p>⚠️ {@code CountingBinStore} counts requests and cannot tell one key from
 * another, and a LIST of the result cannot tell one PUT from ten to the same
 * key. Both blind spots hide the same defect: a writer with a constant
 * {@code seq} overwriting a single object passes every count assertion and every
 * "the object is there" assertion. M4.8b2's row calls that out by name, so the
 * PUTs are recorded here as a SEQUENCE.
 *
 * <p>⚠️ A failure can be armed for the NEXT PUT only, which is what the
 * wedged-writer test needs: swallowing an IOException and never writing again
 * passes any test that merely checks the commit survived.
 */
final class RecordingBinStore implements BinStore {

    private final BinStore delegate;
    /** ⚠️ ONE list of PAIRS, not two lists: `capture` appends from the commit
     * thread and the ticker thread, and two appends are not one append. */
    private record Put(String key, byte[] body) { }

    private final List<Put> puts = new CopyOnWriteArrayList<>();
    private volatile IOException armed;
    private volatile RuntimeException armedUnchecked;

    RecordingBinStore(BinStore delegate) {
        this.delegate = delegate;
    }

    /**
     * The next CHECKPOINT PUT — and only that one — fails with {@code failure}.
     *
     * <p>⚠️ SCOPED TO `.ckpt` ON PURPOSE. Arming the next PUT of any kind hits
     * the delta that {@code commitAll} writes FIRST, so the commit fails and the
     * test proves nothing about the checkpoint path. Measured: that is exactly
     * what a first draft of the wedged-writer test did.
     */
    void failNextCheckpointPut(IOException failure) {
        this.armed = failure;
    }

    /**
     * EVERY checkpoint PUT fails with an UNCHECKED throw until cleared.
     *
     * <p>⚠️ A DIFFERENT PATH FROM {@link #failNextCheckpointPut}, not a
     * variation on it: an IOException is caught where it is thrown, while an
     * unchecked one escapes {@code writeIfDirty} entirely and is swallowed by
     * {@code observe}. Review measured the two behaving differently.
     */
    void failEveryCheckpointPutUnchecked(RuntimeException failure) {
        this.armedUnchecked = failure;
    }

    /** Stop failing — the "until cleared" half of the method above. */
    void stopFailingCheckpointPuts() {
        this.armedUnchecked = null;
        this.armed = null;
    }

    /** Keys of every PUT so far, in order, including repeats of the same key. */
    List<String> putKeys() {
        return puts.stream().map(Put::key).toList();
    }

    /** PUT keys under the checkpoint segment, in order. */
    List<String> checkpointKeys() {
        List<String> out = new ArrayList<>();
        for (Put p : puts) {
            if (p.key().endsWith(".ckpt")) {
                out.add(p.key());
            }
        }
        return out;
    }

    /** The bytes of EVERY checkpoint PUT, in order — a stale-body detector. */
    List<byte[]> checkpointBodies() {
        List<byte[]> out = new ArrayList<>();
        for (Put p : puts) {
            if (p.key().endsWith(".ckpt")) {
                out.add(p.body());
            }
        }
        return out;
    }

    /** The bytes of the LAST checkpoint PUT, or empty if none. */
    Optional<byte[]> lastCheckpointBody() {
        for (int i = puts.size() - 1; i >= 0; i--) {
            if (puts.get(i).key().endsWith(".ckpt")) {
                return Optional.of(puts.get(i).body());
            }
        }
        return Optional.empty();
    }

    private Body replay(byte[] bytes) {
        return new Body(bytes.length, () -> new ByteArrayInputStream(bytes));
    }

    private byte[] capture(String key, Body body) throws IOException {
        byte[] bytes = body.readFully();
        puts.add(new Put(key, bytes));
        if (key.endsWith(".ckpt")) {
            RuntimeException unchecked = armedUnchecked;
            if (unchecked != null) {
                throw unchecked;
            }
            IOException fail = armed;
            if (fail != null) {
                armed = null;
                throw fail;
            }
        }
        return bytes;
    }

    @Override
    public Version put(String key, Body body) throws IOException {
        return delegate.put(key, replay(capture(key, body)));
    }

    @Override
    public Optional<Version> putIfAbsent(String key, Body body) throws IOException {
        return delegate.putIfAbsent(key, replay(capture(key, body)));
    }

    @Override
    public Optional<Version> putIfMatch(String key, Body body, Version expected) throws IOException {
        return delegate.putIfMatch(key, replay(capture(key, body)), expected);
    }

    @Override
    public InputStream get(String key) throws IOException {
        return delegate.get(key);
    }

    @Override
    public InputStream getRange(String key, long start, long endIncl) throws IOException {
        return delegate.getRange(key, start, endIncl);
    }

    @Override
    public Optional<ObjectStat> stat(String key) throws IOException {
        return delegate.stat(key);
    }

    @Override
    public ListPage list(String prefix, String startAfter, int maxKeys) throws IOException {
        return delegate.list(prefix, startAfter, maxKeys);
    }

    @Override
    public void delete(List<String> keys) throws IOException {
        delegate.delete(keys);
    }

    @Override
    public binjava.binstore.Capabilities capabilities() {
        return delegate.capabilities();
    }

    @Override
    public binjava.binstore.MultipartWriter multipart(String key) throws IOException {
        return delegate.multipart(key);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
