// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.binstore.BinStore;
import binjava.binstore.CountingBinStore;
import binjava.binstore.backend.MemoryBinStore;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;

/**
 * The CAS-updated, cached side of the index-ordinal registry (M2.3; ADR-0008).
 */
class IndexOrdinalRegistryTest {

    private static final String A = "00000000-0000-0000-0000-0000000000aa";
    private static final String B = "00000000-0000-0000-0000-0000000000bb";

    @Test
    void firstRegistrationAssignsOrdinalZeroThenEachNewIndexIncrements() throws Exception {
        IndexOrdinalRegistry r = new IndexOrdinalRegistry(new MemoryBinStore(), "p");
        assertThat(r.ordinalFor(A)).isZero();
        assertThat(r.ordinalFor(B)).isEqualTo(1);
    }

    @Test
    void registeringTheSameIndexTwiceReturnsTheSameOrdinal() throws Exception {
        IndexOrdinalRegistry r = new IndexOrdinalRegistry(new MemoryBinStore(), "p");
        assertThat(r.ordinalFor(A)).isZero();
        assertThat(r.ordinalFor(A)).as("re-registering must not mint a second ordinal").isZero();
    }

    @Test
    void registeredCountReflectsWhatThisInstanceHasRegisteredWithNoFurtherRequest() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        IndexOrdinalRegistry r = new IndexOrdinalRegistry(store, "p");
        assertThat(r.registeredCount()).isZero();
        r.ordinalFor(A);
        long before = store.counts().total();
        // ⚠️ M2.6: registeredCount() answers from the local cache, never
        // revalidating -- ordinalFor(A) just wrote the CAS itself, so the
        // cache is already exactly as fresh as it can be, at zero extra cost.
        assertThat(r.registeredCount()).isEqualTo(1);
        assertThat(store.counts().total()).as("no request for the count itself").isEqualTo(before);
        r.ordinalFor(B);
        assertThat(r.registeredCount()).isEqualTo(2);
    }

    @Test
    void registeredCountOnAFreshInstanceIsZeroUntilItLooksSomethingUp() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        // ⚠️ Register A and B through ONE instance...
        IndexOrdinalRegistry writer = new IndexOrdinalRegistry(store, "p");
        writer.ordinalFor(A);
        writer.ordinalFor(B);

        // ⚠️ ...a SEPARATE, fresh instance (a different pod) does not know
        // this yet -- registeredCount() never revalidates, so it answers 0
        // until this instance's own ordinalFor() calls populate its cache.
        // This is the DELIBERATE tradeoff the class javadoc explains: the
        // filter's A candidate can only be made wrong in the SAFE direction
        // (a false positive, never a false negative) by this staleness.
        IndexOrdinalRegistry fresh = new IndexOrdinalRegistry(store, "p");
        assertThat(fresh.registeredCount()).isZero();
        // ⚠️ refresh() (inside ordinalFor) fetches the WHOLE registry object,
        // not just A's entry -- so this one lookup populates B's ordinal too.
        fresh.ordinalFor(A);
        assertThat(fresh.registeredCount())
                .as("one full read brought back both A and B, not just A").isEqualTo(2);
    }

    @Test
    void anAlreadyKnownIndexIssuesNoFurtherStoreRequest() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        IndexOrdinalRegistry r = new IndexOrdinalRegistry(store, "p");
        r.ordinalFor(A);
        long before = store.counts().total();
        // ⚠️ ADR-0008: "~0/s, only on index creation" -- an index already
        // known to THIS instance's cache costs ZERO requests, not merely a
        // cheap one.
        assertThat(r.ordinalFor(A)).isZero();
        assertThat(store.counts().total()).as("a cached index issues nothing").isEqualTo(before);
    }

    @Test
    void aFreshInstanceReadsTheStoreOnceThenCachesTheResult() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        new IndexOrdinalRegistry(store, "p").ordinalFor(A);

        // ⚠️ A SEPARATE instance (a different pod, in practice) that has never
        // read anything must do exactly one full read -- a stat() plus a
        // get(), since its cached version genuinely differs from what is on
        // the store -- the first time it needs an index it does not yet have.
        long before = store.counts().total();
        IndexOrdinalRegistry fresh = new IndexOrdinalRegistry(store, "p");
        assertThat(fresh.ordinalFor(A)).isZero();
        assertThat(store.counts().total()).as("one stat + one get for the first, uncached lookup")
                .isEqualTo(before + 2);

        // ⚠️ And once cached, asking again for the SAME index on the SAME
        // instance costs nothing further -- the property
        // anAlreadyKnownIndexIssuesNoFurtherStoreRequest already pins for a
        // registry that registered the index itself; this pins it for one
        // that instead learned it from a full read.
        long afterFirstRead = store.counts().total();
        assertThat(fresh.ordinalFor(A)).isZero();
        assertThat(store.counts().total()).as("A is already cached on `fresh`")
                .isEqualTo(afterFirstRead);
    }

    @Test
    void aCacheMissForAGenuinelyNewIndexSkipsTheRedundantGetWhenTheVersionHasNotMoved() throws Exception {
        // ⚠️ round-1 test-review (M2.3): neither
        // anAlreadyKnownIndexIssuesNoFurtherStoreRequest nor
        // aFreshInstanceReadsTheStoreOnceThenCachesTheResult ever reaches the
        // SPECIFIC optimization in refresh() that skips get() when the
        // store's version already matches what this instance cached -- the
        // first never calls refresh() at all (the outer cache answers it),
        // and the second's refresh() calls always had a null/stale
        // cachedVersion, so the "versions match" branch was never taken.
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        IndexOrdinalRegistry r = new IndexOrdinalRegistry(store, "p");
        r.ordinalFor(A); // registers A; r's cachedVersion now matches the store's

        long before = store.counts().total();
        // A genuinely NEW index, not yet registered by anyone -- a cache MISS
        // on r's local map, but the STORE's own copy has not moved since r
        // last saw it (nothing else has touched it), so refresh() must skip
        // the wasted get() before falling through to registering B.
        assertThat(r.ordinalFor(B)).isEqualTo(1);
        // ⚠️ ONE stat (revalidating, finds nothing changed) + ONE put
        // (registering B) = 2. A get() here would be a real, billed request
        // for bytes this instance already holds -- exactly what "a get() only
        // if the version actually moved" promises never to issue.
        assertThat(store.counts().total()).as("stat + putIfMatch, no wasted get()")
                .isEqualTo(before + 2);
    }

    @Test
    void concurrentRegistrationOfDifferentIndicesOnTheSameInstanceAssignsDistinctOrdinals()
            throws Exception {
        // ⚠️ round-1 test-review (M2.3): every other concurrency test gives
        // each racing thread its OWN IndexOrdinalRegistry instance -- the
        // realistic separate-pod scenario, but it never exercises `ordinalFor`
        // being `synchronized`, which this class's own javadoc says exists
        // specifically to serialize virtual threads sharing ONE instance
        // inside a single pod.
        final int writers = 8;
        IndexOrdinalRegistry shared = new IndexOrdinalRegistry(new MemoryBinStore(), "p");
        ExecutorService pool = Executors.newFixedThreadPool(writers);
        try {
            CyclicBarrier start = new CyclicBarrier(writers);
            java.util.List<Callable<Integer>> tasks = new java.util.ArrayList<>();
            for (int i = 0; i < writers; i++) {
                final String uuid = "shared-instance-index-" + i;
                tasks.add(() -> {
                    start.await();
                    return shared.ordinalFor(uuid);
                });
            }
            Set<Integer> results = new HashSet<>();
            for (Future<Integer> f : pool.invokeAll(tasks)) {
                results.add(f.get());
            }
            // ⚠️ Without `synchronized`, two threads could both read the same
            // stale `cached`/`cachedVersion` snapshot, compute the SAME
            // nextOrdinal(), and -- since they are racing on ONE instance's
            // fields, not just the store's CAS -- one thread's field write
            // could be clobbered by the other's, producing a collision even
            // though the store itself never accepted two writes for the same
            // ordinal.
            assertThat(results).as("every writer on the shared instance gets a distinct ordinal")
                    .hasSize(writers);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void concurrentFirstRegistrationOfTheSameIndexConvergesOnOneOrdinal() throws Exception {
        // ⚠️ THE reason first-registration-wins exists (ADR-0008, acceptance
        // criterion 3): every writer races the SAME new UUID through its OWN
        // registry instance (separate ingester pods, in practice) against one
        // shared store. A loser that retried into its OWN next ordinal instead
        // of adopting the winner's would silently assign two ordinals to the
        // same index.
        final int writers = 8;
        BinStore store = new MemoryBinStore();
        ExecutorService pool = Executors.newFixedThreadPool(writers);
        try {
            CyclicBarrier start = new CyclicBarrier(writers);
            java.util.List<Callable<Integer>> tasks = new java.util.ArrayList<>();
            for (int i = 0; i < writers; i++) {
                tasks.add(() -> {
                    IndexOrdinalRegistry r = new IndexOrdinalRegistry(store, "p");
                    start.await();
                    return r.ordinalFor(A);
                });
            }
            Set<Integer> results = new HashSet<>();
            for (Future<Integer> f : pool.invokeAll(tasks)) {
                results.add(f.get());
            }
            assertThat(results).as("every writer converges on the SAME ordinal")
                    .containsExactly(0);
        } finally {
            pool.shutdownNow();
        }
        // ⚠️ And the registry itself holds exactly one entry -- not, say, an
        // entry that got overwritten `writers` times leaving stray state.
        assertThat(new IndexOrdinalRegistry(store, "p").ordinalFor(A)).isZero();
    }

    @Test
    void concurrentRegistrationOfDifferentIndicesAssignsDistinctOrdinals() throws Exception {
        final int writers = 8;
        BinStore store = new MemoryBinStore();
        ExecutorService pool = Executors.newFixedThreadPool(writers);
        try {
            CyclicBarrier start = new CyclicBarrier(writers);
            java.util.List<Callable<Integer>> tasks = new java.util.ArrayList<>();
            for (int i = 0; i < writers; i++) {
                final String uuid = "index-" + i;
                tasks.add(() -> {
                    IndexOrdinalRegistry r = new IndexOrdinalRegistry(store, "p");
                    start.await();
                    return r.ordinalFor(uuid);
                });
            }
            Set<Integer> results = new HashSet<>();
            for (Future<Integer> f : pool.invokeAll(tasks)) {
                results.add(f.get());
            }
            // ⚠️ EXACTLY `writers` distinct ordinals -- a race that let two
            // writers both compute the same "next" ordinal from a stale
            // snapshot would collapse two of these into one value.
            assertThat(results).hasSize(writers);
        } finally {
            pool.shutdownNow();
        }
    }
}
