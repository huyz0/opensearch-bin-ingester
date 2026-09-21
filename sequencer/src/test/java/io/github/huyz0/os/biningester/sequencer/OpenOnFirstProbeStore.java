// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.Body;
import java.io.IOException;
import java.util.Optional;

/**
 * Runs {@code onFirstProbe} the first time a chosen key is STATted and found
 * absent, then passes everything through.
 *
 * <p>⚠️ THE RACE ADR-0037's SEAL LOOP HAS TO SURVIVE, made deterministic. The
 * takeover probes slot 0 of each epoch to decide which is burned, and seals
 * afterwards; the two are not atomic and nothing renews the lease during
 * {@code start}. So a pod that stalled past its TTL can open a "burned" epoch in
 * exactly that window. Determinism comes from hanging the side effect on the
 * probe itself rather than from controlling a scheduler --
 * {@code GateFirstPutStore}'s argument, applied to a read.
 *
 * <p>⚠️ ONLY THE FIRST ABSENT PROBE FIRES IT. Firing on every probe would mean
 * the epoch is re-opened under later reads too, and the test would be observing
 * the fake rather than the code.
 */
final class OpenOnFirstProbeStore implements BinStore {

    /** What to do when the probe finds the key absent. */
    interface Interleave {
        void run() throws IOException;
    }

    private final BinStore delegate;
    private final String probed;
    private final Interleave onFirstProbe;
    private boolean fired;

    OpenOnFirstProbeStore(BinStore delegate, String probed, Interleave onFirstProbe) {
        this.delegate = delegate;
        this.probed = probed;
        this.onFirstProbe = onFirstProbe;
    }

    @Override public Optional<io.github.huyz0.os.biningester.binstore.ObjectStat> stat(String k) throws IOException {
        Optional<io.github.huyz0.os.biningester.binstore.ObjectStat> answer = delegate.stat(k);
        if (!fired && k.equals(probed) && answer.isEmpty()) {
            fired = true;
            onFirstProbe.run();
        }
        return answer;
    }

    @Override public Optional<io.github.huyz0.os.biningester.binstore.Version> putIfAbsent(String key, Body body)
            throws IOException {
        return delegate.putIfAbsent(key, body);
    }

    @Override public java.io.InputStream get(String k) throws IOException {
        return delegate.get(k);
    }

    @Override public java.io.InputStream getRange(String k, long a, long b) throws IOException {
        return delegate.getRange(k, a, b);
    }

    @Override public io.github.huyz0.os.biningester.binstore.Version put(String k, Body b) throws IOException {
        return delegate.put(k, b);
    }

    @Override public Optional<io.github.huyz0.os.biningester.binstore.Version> putIfMatch(String k, Body b,
            io.github.huyz0.os.biningester.binstore.Version v) throws IOException {
        return delegate.putIfMatch(k, b, v);
    }

    @Override public io.github.huyz0.os.biningester.binstore.MultipartWriter multipart(String k) throws IOException {
        return delegate.multipart(k);
    }

    @Override public io.github.huyz0.os.biningester.binstore.ListPage list(String p, String a, int m) throws IOException {
        return delegate.list(p, a, m);
    }

    @Override public void delete(java.util.List<String> keys) throws IOException {
        delegate.delete(keys);
    }

    @Override public io.github.huyz0.os.biningester.binstore.Capabilities capabilities() {
        return delegate.capabilities();
    }

    @Override public io.github.huyz0.os.biningester.binstore.SignedUrl presign(String key, java.time.Duration ttl)
            throws java.io.IOException {
        return delegate.presign(key, ttl);
    }

    @Override public void close() throws IOException {
        delegate.close();
    }
}
