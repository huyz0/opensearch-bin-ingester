// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.Capabilities;
import io.github.huyz0.os.biningester.binstore.ListPage;
import io.github.huyz0.os.biningester.binstore.MultipartWriter;
import io.github.huyz0.os.biningester.binstore.ObjectStat;
import io.github.huyz0.os.biningester.binstore.Version;
import java.io.IOException;
import java.io.InputStream;
import java.lang.System.Logger;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Runs a GC pass only while this pod holds the role, and fences the one verb
 * that cannot be taken back (M7.9, FR-9, research 06 §4).
 *
 * <p>⚠️ THE GUARANTEE IS THE ABSENCE OF A DELETE, NOT THE PRESENCE OF AN
 * EXCEPTION. A pass that notices the loss on its next store call has already
 * deleted; the store handed to the pass therefore checks the lease BEFORE each
 * batch reaches the real one, so a batch after the loss never becomes a
 * request.
 *
 * <p>⚠️ ONLY {@code delete} IS FENCED. Reading after a lease is lost is
 * harmless, and refusing reads too would turn an ordinary loss — a pause, a
 * slow renew — into a cascade of failures on a pod that is otherwise fine.
 * {@code put} and {@code multipart} are unfenced as well, which is safe only
 * because a GC pass writes nothing: if one ever does, it belongs behind this
 * fence, because a write from a pod that has lost the role is the same class of
 * defect as a delete from one.
 *
 * <p>⚠️ A POD THAT HOLDS NOTHING ISSUES NO REQUEST AT ALL. Five pods out of six
 * hold no lease every interval forever, so a follower that listed to find out,
 * or read a checkpoint before checking, would make the fleet pay per pod per
 * interval — NFR-2 through the GC door.
 */
public final class LeasedGc {

    private static final Logger LOG = java.lang.System.getLogger(LeasedGc.class.getName());

    private final GcLease lease;
    private final BinStore store;

    public LeasedGc(GcLease lease, BinStore store) {
        this.lease = Objects.requireNonNull(lease, "lease");
        this.store = Objects.requireNonNull(store, "store");
    }

    /**
     * Runs {@code pass} with a fenced store, or does nothing at all.
     *
     * <p>⚠️ NOTHING ESCAPES. A pass that threw would take the schedule down
     * with it and stop GC on this pod permanently, and — worse — would strand
     * the lease for a whole TTL, which is precisely when storage is growing.
     */
    public void runIfLeader(Consumer<BinStore> pass) {
        Objects.requireNonNull(pass, "pass");
        if (!lease.acquire()) {
            return;
        }
        try {
            pass.accept(new FencedDeletes(store, lease));
        } catch (RuntimeException failed) {
            LOG.log(Logger.Level.WARNING, () -> "a GC pass did not finish; the next one "
                    + "judges the same objects again: " + failed);
        } finally {
            try {
                lease.release();
            } catch (RuntimeException failed) {
                LOG.log(Logger.Level.WARNING, () -> "a GC lease was not released cleanly; "
                        + "the next pod waits for its TTL: " + failed);
            }
        }
    }

    /** Everything the store does, except deleting without the role. */
    private record FencedDeletes(BinStore delegate, GcLease lease) implements BinStore {

        @Override
        public void delete(List<String> keys) throws IOException {
            if (!lease.stillHeld()) {
                // ⚠️ REFUSED BEFORE IT IS A REQUEST. The pass's own catch turns
                // this into "not deleted, not counted, judged again next pass",
                // which is the behaviour every GC in this milestone already has
                // for an unreachable store.
                throw new IOException("this pod no longer holds the GC role, so " + keys.size()
                        + " keys were NOT deleted -- another pod is judging them now, and "
                        + "two passes deleting from two views of the watermark table is "
                        + "what the lease exists to prevent");
            }
            delegate.delete(keys);
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
        public Version put(String key, Body body) throws IOException {
            return delegate.put(key, body);
        }

        @Override
        public Optional<Version> putIfAbsent(String key, Body body) throws IOException {
            return delegate.putIfAbsent(key, body);
        }

        @Override
        public Optional<Version> putIfMatch(String key, Body body, Version expected)
                throws IOException {
            return delegate.putIfMatch(key, body, expected);
        }

        @Override
        public MultipartWriter multipart(String key) throws IOException {
            return delegate.multipart(key);
        }

        @Override
        public ListPage list(String prefix, String startAfter, int maxKeys) throws IOException {
            return delegate.list(prefix, startAfter, maxKeys);
        }

        @Override
        public Capabilities capabilities() {
            return delegate.capabilities();
        }

        @Override
        public io.github.huyz0.os.biningester.binstore.SignedUrl presign(String key, java.time.Duration ttl)
                throws IOException {
            // ⚠️ FORWARDED RATHER THAN LEFT TO THE INTERFACE DEFAULT, which
            // refuses: `capabilities()` here is the delegate's, so a store that
            // says it CAN presign must not be wrapped into one that cannot.
            return delegate.presign(key, ttl);
        }

        @Override
        public void close() {
            // ⚠️ THE PASS DOES NOT OWN THE STORE. Closing the delegate here
            // would take the node's shared client down at the end of a GC pass.
        }
    }
}
