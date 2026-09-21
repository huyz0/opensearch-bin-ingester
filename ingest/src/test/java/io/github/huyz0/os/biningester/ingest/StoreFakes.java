// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.Capabilities;
import io.github.huyz0.os.biningester.binstore.ListPage;
import io.github.huyz0.os.biningester.binstore.MultipartWriter;
import io.github.huyz0.os.biningester.binstore.ObjectStat;
import io.github.huyz0.os.biningester.binstore.Version;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Store decorators that misbehave on purpose, so a test can observe ordering and
 * failure paths a well-behaved fake cannot show.
 *
 * <p>⚠️ Split out of {@code DefaultIngestTest} when that file passed the 500-line
 * limit. code-structure.md rule 1: split it, never raise the limit.
 */
final class StoreFakes {

    private StoreFakes() {
    }

    /** Holds the commit's putIfAbsent open so the ack ordering is observable. */
    static final class GatedCommit implements BinStore {
        private final BinStore delegate;
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);

        GatedCommit(BinStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public Optional<Version> putIfAbsent(String key, Body body) throws IOException {
            entered.countDown();
            try {
                release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
            return delegate.putIfAbsent(key, body);
        }

        @Override public Version put(String k, Body b) throws IOException {
            return delegate.put(k, b);
        }

        @Override public InputStream get(String k) throws IOException { return delegate.get(k); }

        @Override public InputStream getRange(String k, long s, long e) throws IOException {
            return delegate.getRange(k, s, e);
        }

        @Override public ListPage list(String p, String a, int m) throws IOException {
            return delegate.list(p, a, m);
        }

        @Override public void delete(List<String> k) throws IOException { delegate.delete(k); }

        @Override public Capabilities capabilities() { return delegate.capabilities(); }
    @Override
    public io.github.huyz0.os.biningester.binstore.SignedUrl presign(String key, java.time.Duration ttl)
            throws java.io.IOException {
        // ⚠️ FORWARDED because a `default` method on the SPI cannot force a
        // decorator to do it, and review MEASURED that forgetting it makes a
        // capable backend pass the startup check and throw at first fetch.
        return delegate.presign(key, ttl);
    }


        @Override public Optional<ObjectStat> stat(String k) throws IOException {
            return delegate.stat(k);
        }

        @Override public Optional<Version> putIfMatch(String k, Body b, Version v) throws IOException {
            return delegate.putIfMatch(k, b, v);
        }

        @Override public MultipartWriter multipart(String k) throws IOException {
            return delegate.multipart(k);
        }

        @Override public void close() throws IOException { delegate.close(); }
    }

    /**
     * A store whose PUTs fail, so a flush fails with waiters already attached.
     *
     * <p>⚠️ Delegates rather than extends — {@code MemoryBinStore} is final, and
     * a decorator is the shape the SPI is built for anyway.
     */
    record FailingPuts(BinStore delegate) implements BinStore {
        FailingPuts() {
            this(new MemoryBinStore());
        }

        @Override
        public Version put(String key, Body body) throws IOException {
            throw new IOException("the store is unavailable");
        }

        @Override
        public Capabilities capabilities() {
            return delegate.capabilities();
        }

        @Override
        public io.github.huyz0.os.biningester.binstore.SignedUrl presign(String key, java.time.Duration ttl)
                throws java.io.IOException {
            return delegate.presign(key, ttl);
        }

        @Override
        public Optional<ObjectStat> stat(String key) throws IOException {
            return delegate.stat(key);
        }

        @Override
        public Optional<Version> putIfAbsent(String key, Body body) throws IOException {
            return delegate.putIfAbsent(key, body);
        }

        @Override
        public Optional<Version> putIfMatch(String key, Body body, Version expected) throws IOException {
            return delegate.putIfMatch(key, body, expected);
        }

        @Override
        public MultipartWriter multipart(String key) throws IOException {
            return delegate.multipart(key);
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
        public ListPage list(String prefix, String startAfter, int maxKeys) throws IOException {
            return delegate.list(prefix, startAfter, maxKeys);
        }

        @Override
        public void delete(List<String> keys) throws IOException {
            delegate.delete(keys);
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    /**
     * A store whose WRITES all start failing on command, reads left working.
     *
     * <p>⚠️ BOTH WRITE PRIMITIVES, and that is the point: the shutdown path
     * publishes a segment ({@code put}), commits a delta ({@code putIfAbsent})
     * and releases the lease ({@code putIfMatch}). A fake that failed only one of
     * them could not produce the case where a failed final flush and a failed
     * release are in flight at once, which is the only case that distinguishes
     * "suppressed" from "substituted".
     */
    static final class FailWritesOnCommand implements BinStore {
        private final BinStore delegate;
        private volatile boolean failing;

        FailWritesOnCommand(BinStore delegate) {
            this.delegate = delegate;
        }

        void failEveryWriteFromNowOn() {
            this.failing = true;
        }

        private void refuse(String op, String key) throws IOException {
            if (failing) {
                throw new IOException("injected: the store refused " + op + " on " + key);
            }
        }

        @Override
        public Optional<Version> putIfAbsent(String key, Body body) throws IOException {
            refuse("putIfAbsent", key);
            return delegate.putIfAbsent(key, body);
        }

        @Override
        public Optional<Version> putIfMatch(String key, Body body, Version expected)
                throws IOException {
            refuse("putIfMatch", key);
            return delegate.putIfMatch(key, body, expected);
        }

        @Override public Version put(String k, Body b) throws IOException {
            refuse("put", k);
            return delegate.put(k, b);
        }

        @Override public InputStream get(String k) throws IOException { return delegate.get(k); }

        @Override
        public InputStream getRange(String key, long start, long endIncl) throws IOException {
            return delegate.getRange(key, start, endIncl);
        }

        @Override public Optional<ObjectStat> stat(String k) throws IOException {
            return delegate.stat(k);
        }

        @Override public MultipartWriter multipart(String k) throws IOException {
            return delegate.multipart(k);
        }

        @Override
        public ListPage list(String prefix, String startAfter, int maxKeys) throws IOException {
            return delegate.list(prefix, startAfter, maxKeys);
        }

        @Override public void delete(List<String> keys) throws IOException {
            delegate.delete(keys);
        }

        @Override public Capabilities capabilities() { return delegate.capabilities(); }

        @Override public void close() throws IOException { delegate.close(); }
    }

    /**
     * A backend that CAN presign, which neither shipping one can.
     *
     * <p>⚠️ WITHOUT IT, HALF OF M5.43's WIRING IS UNREACHABLE. A pod with
     * {@code directEnabled} refuses to start against a store that cannot sign,
     * so every test constructing a pod had to leave the flag off -- and
     * review measured the consequence: dropping {@code config.directEnabled()}
     * from the policy `DefaultIngest` builds survived, because the enabled arm
     * was never executed. When a real signer lands, an operator enabling
     * {@code direct} would have got a pod that starts and answers
     * {@code PROXY} forever, with every test green.
     */
    static final class CanPresign implements BinStore {
        private final BinStore delegate = new MemoryBinStore();

        /**
         * ⚠️ HONOURS ITS {@code ttl}, and the fixed expiry it used to return was
         * a trap for M5.45d. That row must assert ADR-0010's 60 s clamp, and
         * this is the only signing fake in {@code ingest} -- so a
         * {@code GrantIssuer} that DROPPED the clamp and passed {@code ttl}
         * straight through would still have read as clamped against a stand-in
         * whose expiry never moved.
         *
         * <p>⚠️ AND THAT NARROWS THE TRAP WITHOUT CLOSING IT, which review
         * measured. {@code Instant.EPOCH} is 1970, so this expiry is always in
         * the PAST: a clamp assertion phrased against the wall clock --
         * {@code expiresAt().isBefore(Instant.now().plusSeconds(60))} -- passes
         * under the clamped issuer AND the unclamped one. Assert relative to
         * {@code Instant.EPOCH} instead: {@code Duration.between(EPOCH,
         * url.expiresAt())} at most ADR-0010's 60 s is what kills
         * {@code ttl = requested}.
         *
         * <p>⚠️ BOTH ARE NOW CONSTRAINED, and M5.45d is what closed them. An
         * earlier draft of this paragraph said "nothing calls this yet, so the
         * {@code ttl} it honours is unconstrained too -- replacing this body
         * with a throw leaves the suite green", which was true when written and
         * became false in the same commit that made it: replacing the body with
         * a throw now reds
         * {@code DirectServingTest#aSIGNINGFailureNamesTheSegmentAndDoesNotRollBack},
         * and {@code theTTLIsCLAMPEDToSixtySecondsMeasuredFromTheEPOCH} pins the
         * expiry in the EPOCH-relative form the paragraph above asks for. A
         * later author reading "unconstrained" would edit exactly what these
         * paragraphs exist to protect.
         */

        /** When set, every {@code presign} throws it -- the signing-failure path. */
        java.io.IOException failSigningWith;

        /** How many times anything asked this backend to sign. */
        final java.util.concurrent.atomic.AtomicInteger signings =
                new java.util.concurrent.atomic.AtomicInteger();

        @Override public io.github.huyz0.os.biningester.binstore.SignedUrl presign(String key, java.time.Duration ttl)
                throws java.io.IOException {
            // ⚠️ COUNTED, BECAUSE `CountingBinStore` DELIBERATELY DOES NOT.
            // ADR-0041 says signing issues no object-store request, so the meter
            // is right to ignore it -- and that leaves the SIGNING rate with no
            // observer at all. Review measured the consequence: minting once per
            // RUN instead of once per segment left the whole module green,
            // because `Grant` is a record with value equality and this fake is
            // deterministic per key, so 64 mints are 64 EQUAL values and
            // counting `distinct()` sees one.
            signings.incrementAndGet();
            // ⚠️ THE CHECKED ONE, which is what `presign` declares. Wrapping
            // it in `UncheckedIOException` walked straight past `mintGrant`'s
            // `catch (IOException)` and out of `publish` with the fake's own
            // message -- so the case passed through the path it meant to test
            // and asserted on the wrong exception.
            if (failSigningWith != null) {
                throw failSigningWith;
            }
            return new io.github.huyz0.os.biningester.binstore.SignedUrl(
                    "https://store.example/" + key,
                    java.time.Instant.EPOCH.plus(ttl));
        }

        @Override public Capabilities capabilities() {
            Capabilities real = delegate.capabilities();
            return new Capabilities(real.conditionalWrites(), real.batchDelete(), true,
                    real.maxKeyBytes(), real.minPartSize(), real.costs());
        }

        @Override public InputStream get(String k) throws IOException { return delegate.get(k); }

        @Override public InputStream getRange(String k, long s, long e) throws IOException {
            return delegate.getRange(k, s, e);
        }

        @Override public Optional<Version> putIfAbsent(String k, Body b) throws IOException {
            return delegate.putIfAbsent(k, b);
        }

        @Override public Optional<Version> putIfMatch(String k, Body b, Version v)
                throws IOException {
            return delegate.putIfMatch(k, b, v);
        }

        @Override public Version put(String k, Body b) throws IOException {
            return delegate.put(k, b);
        }

        @Override public Optional<ObjectStat> stat(String k) throws IOException {
            return delegate.stat(k);
        }

        @Override public MultipartWriter multipart(String k) throws IOException {
            return delegate.multipart(k);
        }

        @Override public ListPage list(String prefix, String startAfter, int maxKeys)
                throws IOException {
            return delegate.list(prefix, startAfter, maxKeys);
        }

        @Override public void delete(List<String> keys) throws IOException {
            delegate.delete(keys);
        }

        @Override public void close() throws IOException { delegate.close(); }
    }

    /**
     * A store whose read hands over a PREFIX and then throws.
     *
     * <p>⚠️ THE ONLY WAY TO SEE "A PREFIX IS NEVER CACHED". A store that fails
     * on its first byte proves nothing about admission, because there is
     * nothing to admit; the defect is caching what was read BEFORE the failure
     * and serving it to a later subscriber as a whole segment.
     */
    static final class ReadThrowsPartWayThrough implements BinStore {
        private final BinStore delegate;
        private final int bytesBeforeFailure;

        ReadThrowsPartWayThrough(BinStore delegate, int bytesBeforeFailure) {
            this.delegate = delegate;
            this.bytesBeforeFailure = bytesBeforeFailure;
        }

        @Override public InputStream get(String key) throws IOException {
            InputStream real = delegate.get(key);
            return new InputStream() {
                private int served;

                @Override public int read() throws IOException {
                    byte[] one = new byte[1];
                    return read(one, 0, 1) == -1 ? -1 : one[0] & 0xff;
                }

                @Override public int read(byte[] b, int off, int len) throws IOException {
                    if (served >= bytesBeforeFailure) {
                        throw new IOException("the stream died part way through");
                    }
                    int n = real.read(b, off, Math.min(len, bytesBeforeFailure - served));
                    if (n > 0) {
                        served += n;
                    }
                    return n;
                }

                @Override public void close() throws IOException {
                    real.close();
                }
            };
        }

        @Override public InputStream getRange(String k, long s, long e) throws IOException {
            return delegate.getRange(k, s, e);
        }

        @Override public Optional<Version> putIfAbsent(String k, Body b) throws IOException {
            return delegate.putIfAbsent(k, b);
        }

        @Override public Optional<Version> putIfMatch(String k, Body b, Version v)
                throws IOException {
            return delegate.putIfMatch(k, b, v);
        }

        @Override public Version put(String k, Body b) throws IOException {
            return delegate.put(k, b);
        }

        @Override public Optional<ObjectStat> stat(String k) throws IOException {
            return delegate.stat(k);
        }

        @Override public MultipartWriter multipart(String k) throws IOException {
            return delegate.multipart(k);
        }

        @Override public ListPage list(String prefix, String startAfter, int maxKeys)
                throws IOException {
            return delegate.list(prefix, startAfter, maxKeys);
        }

        @Override public void delete(List<String> keys) throws IOException {
            delegate.delete(keys);
        }

        @Override public Capabilities capabilities() { return delegate.capabilities(); }

        @Override public void close() throws IOException { delegate.close(); }
    }

    /**
     * A store whose failed read does NOT name the object it failed on.
     *
     * <p>⚠️ THIS IS WHAT A REAL BACKEND DOES. S3 answers a bad read with
     * {@code Access Denied} or a reset connection; the SDK's exception carries a
     * request id, not an object key. {@link io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore}
     * is the exception rather than the rule -- it throws
     * {@code IOException("no such key: " + key)}, and a test driven through it
     * cannot tell a caller that names the failing key from one that does not,
     * because the key arrives in the cause's message either way. Review measured
     * exactly that: reverting {@code SegmentServingPath.deliver} to
     * {@code new UncheckedIOException(storeFailed)} left every assertion green.
     */
    static final class ReadFailsWithoutNamingTheKey implements BinStore {
        private final BinStore delegate;

        ReadFailsWithoutNamingTheKey(BinStore delegate) {
            this.delegate = delegate;
        }

        @Override public InputStream get(String key) throws IOException {
            try {
                return delegate.get(key);
            } catch (IOException anonymised) {
                throw new IOException("connection reset by peer");
            }
        }

        @Override public InputStream getRange(String key, long start, long endIncl)
                throws IOException {
            try {
                return delegate.getRange(key, start, endIncl);
            } catch (IOException anonymised) {
                throw new IOException("connection reset by peer");
            }
        }

        @Override public Optional<Version> putIfAbsent(String k, Body b) throws IOException {
            return delegate.putIfAbsent(k, b);
        }

        @Override public Optional<Version> putIfMatch(String k, Body b, Version v)
                throws IOException {
            return delegate.putIfMatch(k, b, v);
        }

        @Override public Version put(String k, Body b) throws IOException {
            return delegate.put(k, b);
        }

        @Override public Optional<ObjectStat> stat(String k) throws IOException {
            return delegate.stat(k);
        }

        @Override public MultipartWriter multipart(String k) throws IOException {
            return delegate.multipart(k);
        }

        @Override public ListPage list(String prefix, String startAfter, int maxKeys)
                throws IOException {
            return delegate.list(prefix, startAfter, maxKeys);
        }

        @Override public void delete(List<String> keys) throws IOException {
            delegate.delete(keys);
        }

        @Override public Capabilities capabilities() { return delegate.capabilities(); }

        @Override public void close() throws IOException { delegate.close(); }
    }
}
