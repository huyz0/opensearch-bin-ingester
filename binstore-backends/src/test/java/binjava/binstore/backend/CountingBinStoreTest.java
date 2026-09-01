// SPDX-License-Identifier: Apache-2.0
package binjava.binstore.backend;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.binstore.BinStore;
import binjava.binstore.Body;
import binjava.binstore.CountingBinStore;
import binjava.binstore.StoreCounts;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The cost meter (R9). ⚠️ Every acceptance criterion about request rates is
 * asserted through this class, so a counter that is wrong in the LOW direction
 * makes those criteria pass over requests that were really issued.
 */
class CountingBinStoreTest {

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
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
}
