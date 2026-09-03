// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import binjava.binstore.BinStore;
import binjava.binstore.Body;
import binjava.binstore.Capabilities;
import binjava.binstore.ListPage;
import binjava.binstore.MultipartWriter;
import binjava.binstore.ObjectStat;
import binjava.binstore.Version;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;

/**
 * Makes the FIRST {@code putIfMatch} ambiguous — the outcome the store SPI
 * cannot report and a caller cannot infer.
 *
 * <p>⚠️ AMBIGUITY IS NOT FAILURE, and the difference is the whole task. A
 * conditional write that returns empty definitively LOST; one that throws may
 * have landed and lost only its response. {@link Mode#LANDED} is the dangerous
 * half — the write took effect and moved the version, so the caller's cached
 * {@code Version} is now stale in a way nothing told it about.
 *
 * <p>⚠️ Only the first call is ambiguous, so a test can observe what the NEXT
 * one does. That is where a self-fence would show up: the second write loses to
 * the first one's own bytes.
 */
public final class AmbiguousPutStore implements BinStore {

    /** Whether the ambiguous write took effect before the response was lost. */
    public enum Mode {
        /** ⚠️ The write LANDED and the version moved. */
        LANDED,
        /** The store never saw it; the version is unchanged. */
        LOST
    }

    private final BinStore delegate;
    private final Mode mode;
    private boolean fired;

    public AmbiguousPutStore(BinStore delegate, Mode mode) {
        this.delegate = delegate;
        this.mode = mode;
    }

    @Override
    public Optional<Version> putIfMatch(String key, Body body, Version expected)
            throws IOException {
        if (!fired) {
            fired = true;
            if (mode == Mode.LANDED) {
                // ⚠️ SELF-CHECKING. If the delegate's write lost, this fake
                // would report LANDED while actually running the harmless
                // half, and a test named for the dangerous one would be a lie.
                if (delegate.putIfMatch(key, body, expected).isEmpty()) {
                    throw new IllegalStateException(
                            "LANDED requires the underlying write to win; it lost");
                }
            }
            throw new IOException("response lost after a " + mode + " conditional write");
        }
        return delegate.putIfMatch(key, body, expected);
    }

    @Override
    public InputStream get(String k) throws IOException {
        return delegate.get(k);
    }

    @Override
    public InputStream getRange(String k, long a, long b) throws IOException {
        return delegate.getRange(k, a, b);
    }

    @Override
    public Optional<ObjectStat> stat(String k) throws IOException {
        return delegate.stat(k);
    }

    @Override
    public Version put(String k, Body b) throws IOException {
        return delegate.put(k, b);
    }

    @Override
    public Optional<Version> putIfAbsent(String k, Body b) throws IOException {
        return delegate.putIfAbsent(k, b);
    }

    @Override
    public MultipartWriter multipart(String k) throws IOException {
        return delegate.multipart(k);
    }

    @Override
    public ListPage list(String p, String a, int m) throws IOException {
        return delegate.list(p, a, m);
    }

    @Override
    public void delete(List<String> keys) throws IOException {
        delegate.delete(keys);
    }

    @Override
    public Capabilities capabilities() {
        return delegate.capabilities();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
