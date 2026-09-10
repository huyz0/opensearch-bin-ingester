// SPDX-License-Identifier: Apache-2.0
package binjava.binstore.backend;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.binstore.BinStore;
import binjava.binstore.Body;
import binjava.binstore.CountingBinStore;
import binjava.binstore.SignedUrl;
import binjava.binstore.StoreCounts;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The cost meter (R9). ⚠️ Every acceptance criterion about request rates is
 * asserted through this class, so a counter that is wrong in the LOW direction
 * makes those criteria pass over requests that were really issued.
 */
class CountingBinStoreTest extends binjava.binstore.BinStoreConformance {

    /**
     * ⚠️ THE DECORATOR RUNS THE WHOLE SUITE, and it did not until M5.10. Review
     * MEASURED a capable backend behind this meter passing
     * {@code requirePresignedUrls()} at startup and throwing
     * {@code UnsupportedOperationException} at the first `direct` fetch,
     * because `presign` is a {@code default} method and nothing forces a
     * decorator to forward it. A rule the implementer must remember is the rung
     * this project rejects for callers.
     *
     * <p>⚠️ THE DELEGATE IS THE CAPABLE STAND-IN, not {@code MemoryBinStore},
     * and that is the whole reason this helps. Review MEASURED that with an
     * incapable delegate the conformance suite takes the refusing branch
     * whether or not the decorator forwards, so the suite closed NOTHING about
     * `presign` -- an earlier version of this javadoc claimed it did, which was
     * a safeguard asserted rather than present. Pointed here, the capable
     * branch of {@link binjava.binstore.PresignConformance} executes for the
     * first time in the tree.
     *
     * <p>⚠️ AND THE GENERAL TRAP IS STILL OPEN: the next {@code default} method
     * added to {@code BinStore} has the identical shape, and nothing here
     * catches it. That is M0.108's class of problem, not this test's.
     */
    @Override
    protected BinStore newStore() {
        return new CountingBinStore(new CapableStore());
    }



    private static CountingBinStore store() {
        return new CountingBinStore(new MemoryBinStore());
    }

    @Test
    void aFreshStoreHasIssuedNothing() throws Exception {
        try (CountingBinStore s = store()) {
            assertThat(s.counts().total()).isZero();
            assertThat(s.counts()).isEqualTo(new StoreCounts(0, 0, 0, 0, 0));
        }
    }

    @Test
    void eachKindIsCountedSeparatelyAndInTheTotal() throws Exception {
        try (CountingBinStore s = store()) {
            s.put("k", Body.ofBytes(bytes("v")));
            s.putIfAbsent("j", Body.ofBytes(bytes("v")));
            s.get("k").close();
            s.getRange("k", 0, 0).close();
            s.stat("k");
            s.list("", null, 10);
            s.delete(List.of("j"));
            // ⚠️ Exact per-kind counts, not just the total: a decorator that
            // increments the wrong adder keeps the total right while making
            // "no LIST on a hot path" (R2) unassertable.
            assertThat(s.counts()).isEqualTo(new StoreCounts(2, 2, 1, 1, 1));
            assertThat(s.counts().total()).isEqualTo(7);
        }
    }

    @Test
    void aRangedReadIsOneGetNotTwo() throws Exception {
        try (CountingBinStore s = store()) {
            s.put("k", Body.ofBytes(bytes("0123456789")));
            s.getRange("k", 2, 5).close();
            // ⚠️ R4 is about not SPLITTING a coalesced read; a range that
            // replaces a whole-object read is one request, and counting it as a
            // stat-plus-get would make every segment read look twice as costly.
            assertThat(s.counts().gets()).isEqualTo(1);
            assertThat(s.counts().stats()).isZero();
        }
    }

    @Test
    void aLostPutIfAbsentRaceIsStillABilledRequest() throws Exception {
        try (CountingBinStore s = store()) {
            s.putIfAbsent("k", Body.ofBytes(bytes("first")));
            assertThat(s.putIfAbsent("k", Body.ofBytes(bytes("second")))).isEmpty();
            // ⚠️ The commit log is a chain of these and the LOSERS are billed.
            // Counting only the winner reports a contended cluster as costing
            // what an idle one does.
            assertThat(s.counts().puts()).isEqualTo(2);
        }
    }

    @Test
    void aLostPutIfMatchRaceIsStillABilledRequest() throws Exception {
        try (CountingBinStore s = store()) {
            var v1 = s.put("k", Body.ofBytes(bytes("first")));
            s.put("k", Body.ofBytes(bytes("second"))); // moves the version away from v1
            // ⚠️ round-1 review (M2.0): the sibling putIfAbsent case above was
            // the only CAS primitive this meter's own test file actually
            // exercised. A lease renewal or registry update (ADR-0008) that
            // loses a putIfMatch race is still a request S3 bills, same
            // reasoning as a lost putIfAbsent -- and nothing caught the
            // decorator's increment line going missing until this was added.
            assertThat(s.putIfMatch("k", Body.ofBytes(bytes("third")), v1)).isEmpty();
            assertThat(s.counts().puts()).as("put + moving put + lost putIfMatch").isEqualTo(3);
        }
    }

    @Test
    void aCompletedMultipartUploadCountsCreateEachPartAndComplete() throws Exception {
        try (CountingBinStore s = store()) {
            long min = s.capabilities().minPartSize();
            byte[] part = new byte[(int) min];
            try (var w = s.multipart("k")) {
                w.uploadPart(1, Body.ofBytes(part));
                w.uploadPart(2, Body.ofBytes(bytes("tail")));
                w.complete();
            }
            // ⚠️ CreateMultipartUpload (the multipart() call itself) + 2
            // UploadPart + 1 CompleteMultipartUpload = 4 real, billed requests
            // -- undercounting any one of them under-reports exactly the
            // workload a large segment's multipart upload actually costs.
            assertThat(s.counts().puts()).as("create + 2 uploadPart + complete").isEqualTo(4);
        }
    }

    @Test
    void anAbortedMultipartUploadCountsCreateEachPartAndAbort() throws Exception {
        try (CountingBinStore s = store()) {
            long min = s.capabilities().minPartSize();
            try (var w = s.multipart("k")) {
                w.uploadPart(1, Body.ofBytes(new byte[(int) min]));
                w.abort();
            }
            // create + 1 uploadPart + abort = 3, same "billed even when it
            // fails/aborts" reasoning as every other case in this class.
            assertThat(s.counts().puts()).as("create + uploadPart + abort").isEqualTo(3);
        }
    }

    @Test
    void closingWithoutCompletingCountsAsOneImplicitAbort() throws Exception {
        try (CountingBinStore s = store()) {
            long min = s.capabilities().minPartSize();
            var w = s.multipart("k");
            w.uploadPart(1, Body.ofBytes(new byte[(int) min]));
            w.close(); // no explicit complete()/abort()
            // create + uploadPart + (the implicit abort close() performs) = 3,
            // counted exactly once -- not zero (an uncounted free cleanup) and
            // not twice (double-counting a close() that follows an explicit
            // complete()/abort(), checked next).
            assertThat(s.counts().puts()).as("create + uploadPart + implicit abort").isEqualTo(3);

            w.close(); // idempotent: a second close() must not count again
            assertThat(s.counts().puts()).as("a second close() counts nothing new").isEqualTo(3);
        }
    }

    @Test
    void closingAfterAFailedCompleteStillCountsTheRealImplicitAbort() throws Exception {
        // ⚠️ round-1 review (M2.2): a caller's ordinary try-with-resources
        // unwind on complete()'s exception path calls close() next, and that
        // close() performs a REAL implicit abort (LocalFsBinStore, say,
        // actually deletes the staged part files) -- a genuinely separate
        // billed request from the failed complete() itself, which the
        // decorator must count too, not silently swallow.
        try (CountingBinStore s = store()) {
            long min = s.capabilities().minPartSize();
            var w = s.multipart("k");
            w.uploadPart(1, Body.ofBytes(bytes("short"))); // undersized, non-final
            w.uploadPart(2, Body.ofBytes(new byte[(int) min]));
            assertThatThrownBy(w::complete).isInstanceOf(java.io.IOException.class);
            w.close();
            // create + 2 uploadPart + failed complete + the implicit abort
            // close() actually performs = 5.
            assertThat(s.counts().puts())
                    .as("create + 2 uploadPart + failed complete + implicit abort").isEqualTo(5);
        }
    }

    @Test
    void aFailedRequestIsStillCounted() throws Exception {
        try (CountingBinStore s = store()) {
            assertThatThrownBy(() -> s.get("missing").close()).isInstanceOf(java.io.IOException.class);
            // ⚠️ S3 bills a 404 and a 500 like any other request. Counting only
            // successes under-reports exactly the workload that costs most, and
            // the increment is before the delegate call so a throw cannot skip it.
            assertThat(s.counts().gets()).isEqualTo(1);
        }
    }

    @Test
    void oneListCallIsOneRequestWhateverThePageHolds() throws Exception {
        try (CountingBinStore s = store()) {
            for (int i = 0; i < 20; i++) {
                s.put("p/" + i, Body.ofBytes(bytes("v")));
            }
            long before = s.counts().lists();
            s.list("p/", null, 5);
            s.list("p/", "p/12", 5);
            // ⚠️ Per PAGE, not per object. This is the property ADR-0022 changed
            // the SPI to make countable: a lazy Stream hid paging inside the
            // backend and the meter saw one invocation for any number of pages.
            assertThat(s.counts().lists() - before).isEqualTo(2);
        }
    }

    @Test
    void aBatchDeleteIsOneRequestNotOnePerKey() throws Exception {
        try (CountingBinStore s = store()) {
            s.put("a", Body.ofBytes(bytes("v")));
            s.put("b", Body.ofBytes(bytes("v")));
            long before = s.counts().deletes();
            s.delete(List.of("a", "b", "never-existed"));
            // ⚠️ The bill is per call. Counting keys would make GC look
            // proportional to objects, which is the shape R2 exists to forbid.
            assertThat(s.counts().deletes() - before).isEqualTo(1);
        }
    }

    @Test
    void readingCapabilitiesIssuesNoRequest() throws Exception {
        try (CountingBinStore s = store()) {
            s.capabilities();
            s.capabilities();
            // ⚠️ Startup state, read locally. Counting it would put a floor
            // under every zero-idle-cost assertion in the milestone.
            assertThat(s.counts().total()).isZero();
        }
    }

    @Test
    void anIdleStoreIssuesNothingAtAll() throws Exception {
        try (CountingBinStore s = store()) {
            s.put("k", Body.ofBytes(bytes("v")));
            StoreCounts afterWork = s.counts();
            // ⚠️ The shape of M1's headline criterion: nothing happens, so
            // nothing is billed. A meter that ticks on its own -- a background
            // refresh, a lazily-retried close -- would make that criterion
            // unprovable, and this is where that would show up.
            assertThat(s.counts()).isEqualTo(afterWork);
            assertThat(s.counts()).isEqualTo(afterWork);
        }
    }

    @Test
    void countsAreCorrectUnderConcurrentUse() throws Exception {
        try (CountingBinStore s = store()) {
            int threads = 8;
            int perThread = 50;
            var pool = java.util.concurrent.Executors.newFixedThreadPool(threads);
            try {
                var tasks = new java.util.ArrayList<java.util.concurrent.Callable<Void>>();
                for (int t = 0; t < threads; t++) {
                    final int id = t;
                    tasks.add(() -> {
                        for (int i = 0; i < perThread; i++) {
                            s.put(id + "/" + i, Body.ofBytes(bytes("v")));
                        }
                        return null;
                    });
                }
                for (var f : pool.invokeAll(tasks)) {
                    f.get();
                }
            } finally {
                pool.shutdownNow();
            }
            // ⚠️ A non-atomic counter loses increments under contention, and it
            // loses them DOWNWARD -- so the cost assertions would pass while the
            // real request count was higher. That is the direction that hurts.
            assertThat(s.counts().puts()).isEqualTo((long) threads * perThread);
        }
    }

    @Test
    void closingTheMeterClosesTheStoreUnderIt() throws Exception {
        var closed = new java.util.concurrent.atomic.AtomicBoolean();
        CountingBinStore s = new CountingBinStore(new ClosingSpy(closed));
        s.close();
        assertThat(closed.get()).as("the decorator must not swallow close").isTrue();
    }

    /** ⚠️ MemoryBinStore is final, so the spy delegates rather than extends. */
    private record ClosingSpy(java.util.concurrent.atomic.AtomicBoolean closed) implements BinStore {
        private static final MemoryBinStore INNER = new MemoryBinStore();

        @Override
        public java.io.InputStream get(String key) throws java.io.IOException {
            return INNER.get(key);
        }

        @Override
        public java.io.InputStream getRange(String key, long a, long b) throws java.io.IOException {
            return INNER.getRange(key, a, b);
        }

        @Override
        public java.util.Optional<binjava.binstore.ObjectStat> stat(String key)
                throws java.io.IOException {
            return INNER.stat(key);
        }

        @Override
        public binjava.binstore.Version put(String key, Body body) throws java.io.IOException {
            return INNER.put(key, body);
        }

        @Override
        public java.util.Optional<binjava.binstore.Version> putIfAbsent(String key, Body body)
                throws java.io.IOException {
            return INNER.putIfAbsent(key, body);
        }

        @Override
        public java.util.Optional<binjava.binstore.Version> putIfMatch(
                String key, Body body, binjava.binstore.Version expected) throws java.io.IOException {
            return INNER.putIfMatch(key, body, expected);
        }

        @Override
        public binjava.binstore.MultipartWriter multipart(String key) throws java.io.IOException {
            return INNER.multipart(key);
        }

        @Override
        public binjava.binstore.ListPage list(String p, String after, int max)
                throws java.io.IOException {
            return INNER.list(p, after, max);
        }

        @Override
        public void delete(List<String> keys) throws java.io.IOException {
            INNER.delete(keys);
        }

        @Override
        public binjava.binstore.Capabilities capabilities() {
            return INNER.capabilities();
        }

        @Override
        public void close() {
            closed.set(true);
        }
    }

    /**
     * The meter FORWARDS a capability it does not itself implement (M5.10).
     *
     * <p>⚠️ NEITHER SHIPPING BACKEND CAN PRESIGN, so the conformance suite
     * takes the refusing branch through this decorator whether or not it
     * forwards -- the defect is invisible to it. The delegate here stands in
     * for the first backend that CAN, and the property under test is the
     * decorator's rather than the fake's.
     *
     * <p>⚠️ MEASURED BEFORE THE FIX: a capable backend behind this meter passed
     * the startup check and threw at the first fetch, blaming a backend that
     * could in fact sign.
     */
    @Test
    void theMeterFORWARDSPresignToACapableDelegateUNCHANGED() throws Exception {
        CapableStore capable = new CapableStore();
        try (CountingBinStore metered = new CountingBinStore(capable)) {
            assertThat(metered.capabilities().presignedUrls()).isTrue();
            metered.capabilities().requirePresignedUrls();

            java.time.Duration ttl = java.time.Duration.ofMinutes(5);
            SignedUrl signed = metered.presign("seg/1", ttl);

            // ⚠️ THE ARGUMENTS, NOT JUST THE CALL. Review MEASURED that a
            // decorator passing `key + "/shadow"` or `ttl.multipliedBy(1000)`
            // survived a test that only checked the returned url -- a grant
            // over the wrong object, or a thousandfold-longer one, both looked
            // identical from the outside.
            assertThat(capable.lastKey).isEqualTo("seg/1");
            assertThat(capable.lastTtl).isEqualTo(ttl);
            assertThat(signed.url()).contains("seg/1");

            // ⚠️ NO REQUEST PER SIGNATURE, which is the contract `presign`'s
            // javadoc states and the one half of it this tree can measure.
            // The stand-in signs from nothing, so zero here IS the whole
            // of it; a backend amortising one fetched key over many URLs
            // still satisfies the contract and would not read zero.
            // ⚠️ `total()`, NOT A HAND-SUMMED SUBSET. Review MEASURED that
            // `deletes.increment()` in `presign` survived a sum of four
            // counters -- the fifth existed and the sum did not name it, which
            // is what `total()` is for.
            assertThat(metered.counts().total())
                    .as("signing issues no request, so the meter counts none")
                    .isZero();
        }
    }

    /**
     * The meter reports the delegate's capabilities, not its own idea of them.
     *
     * <p>⚠️ THE INCAPABLE DELEGATE IS THE ONE THAT MATTERS, and an earlier
     * version of this test asked only the capable one. Review MEASURED the
     * hole: a meter forwarding every component but hardcoding
     * {@code presignedUrls = true} passed all 1,089 tests, because the only
     * delegate anything asked about already answered true. Under it a
     * deployment on either shipping backend passes
     * {@code requirePresignedUrls()} at startup and throws at the first
     * `direct` fetch -- acceptance criterion (1) verbatim.
     *
     * <p>⚠️ AND POINTING {@code newStore()} AT THE CAPABLE DELEGATE IS WHAT
     * OPENED IT. That change (round two) bought the forwarding direction and
     * sold the over-advertising one: under a metered {@code MemoryBinStore} the
     * conformance suite would have taken the capable branch and hit the
     * throwing default. Both directions are asked here now.
     */
    @Test
    void theMeterFORWARDSCapabilitiesRatherThanInventingThem() throws Exception {
        CapableStore capable = new CapableStore();
        try (CountingBinStore metered = new CountingBinStore(capable)) {
            assertThat(metered.capabilities()).isEqualTo(capable.capabilities());
        }
        MemoryBinStore plain = new MemoryBinStore();
        try (CountingBinStore metered = new CountingBinStore(plain)) {
            assertThat(metered.capabilities())
                    .as("an INCAPABLE delegate must stay incapable through the meter, or the "
                            + "startup check passes for a backend that cannot presign")
                    .isEqualTo(plain.capabilities());

            // ⚠️ THE FOURTH QUADRANT. capable/incapable x capabilities/presign
            // has four cells and three were pinned; this is the fourth, and
            // review MEASURED that it was empty: a meter CATCHING the default's
            // `UnsupportedOperationException` and returning a bogus
            // `SignedUrl` survived the whole suite. It survived because
            // `newStore()` points the conformance suite at the CAPABLE
            // stand-in, so the refusing branch never runs through the meter.
            assertThatThrownBy(() -> metered.presign("k", java.time.Duration.ofMinutes(5)))
                    .as("an incapable delegate must REFUSE through the meter -- a decorator "
                            + "that swallows the refusal hands out a URL that fetches nothing")
                    .isInstanceOf(UnsupportedOperationException.class);
        }
    }

    /** A stand-in for the first backend that can sign, which neither shipping one can. */
    private static final class CapableStore implements BinStore {
        private final BinStore delegate = new MemoryBinStore();
        private String lastKey;
        private java.time.Duration lastTtl;

        @Override public binjava.binstore.Version put(String k, Body b) throws java.io.IOException {
            return delegate.put(k, b);
        }

        @Override public java.util.Optional<binjava.binstore.Version> putIfAbsent(String k, Body b)
                throws java.io.IOException {
            return delegate.putIfAbsent(k, b);
        }

        @Override public java.util.Optional<binjava.binstore.Version> putIfMatch(
                String k, Body b, binjava.binstore.Version v) throws java.io.IOException {
            return delegate.putIfMatch(k, b, v);
        }

        @Override public java.io.InputStream get(String k) throws java.io.IOException {
            return delegate.get(k);
        }

        @Override public java.io.InputStream getRange(String k, long a, long b)
                throws java.io.IOException {
            return delegate.getRange(k, a, b);
        }

        @Override public java.util.Optional<binjava.binstore.ObjectStat> stat(String k)
                throws java.io.IOException {
            return delegate.stat(k);
        }

        @Override public binjava.binstore.ListPage list(String p, String a, int m)
                throws java.io.IOException {
            return delegate.list(p, a, m);
        }

        @Override public void delete(List<String> keys) throws java.io.IOException {
            delegate.delete(keys);
        }

        @Override public binjava.binstore.MultipartWriter multipart(String k)
                throws java.io.IOException {
            return delegate.multipart(k);
        }

        @Override public void close() throws java.io.IOException {
            delegate.close();
        }

        @Override public binjava.binstore.Capabilities capabilities() {
            binjava.binstore.Capabilities c = delegate.capabilities();
            return new binjava.binstore.Capabilities(c.conditionalWrites(), c.batchDelete(), true,
                    c.maxKeyBytes(), c.minPartSize(), c.costs());
        }

        @Override public SignedUrl presign(String key, java.time.Duration ttl) {
            if (ttl.isNegative() || ttl.isZero()) {
                // ⚠️ BOTH HALVES. Review MEASURED that dropping `isNegative()`
                // survived, because only the zero case was probed.
                throw new IllegalArgumentException("a TTL must be positive");
            }
            this.lastKey = key;
            this.lastTtl = ttl;
            return new SignedUrl("https://store/" + key + "?sig=x",
                    java.time.Instant.now().plus(ttl));
        }
    }
}
