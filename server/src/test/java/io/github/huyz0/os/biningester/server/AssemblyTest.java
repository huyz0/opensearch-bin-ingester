// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.format.Lease;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.ingest.RegistrationTimeoutException;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
import io.github.huyz0.os.biningester.ingest.SubscriptionHub;
import io.github.huyz0.os.biningester.security.Principal;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.LeaseManager;
import io.github.huyz0.os.biningester.sequencer.SequencerTransport;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;

/**
 * The composition root: configuration in, object graph out (M8.1).
 *
 * <p>⚠️ WHAT THIS PINS IS THAT THE GRAPH IS CONNECTED, not that each piece
 * works — every piece has its own suite. The defect this class exists to catch
 * is a root that builds a correct-looking graph whose pieces do not reach each
 * other: an ingest over one store and a sequencer over another, a hub nothing
 * publishes to, a prefix the writer and the commit log disagree on. Each of
 * those compiles, starts, and loses data in a way no unit test of any single
 * component can see, which is why eight milestones of green suites did not
 * catch that none of them had ever been assembled.
 */
class AssemblyTest {

    private static final String INDEX = "logs";
    private static final String INDEX_UUID = uuidToBase64Url(UUID.randomUUID());
    private static final Principal PRINCIPAL =
            new Principal("cluster-a", "producer-1", java.util.Set.of(INDEX, "never-registered"));

    private static String uuidToBase64Url(UUID uuid) {
        ByteBuffer b = ByteBuffer.allocate(16);
        b.putLong(uuid.getMostSignificantBits());
        b.putLong(uuid.getLeastSignificantBits());
        return Base64.getUrlEncoder().withoutPadding().encodeToString(b.array());
    }

    private static ServerConfig config(String podId) {
        return new ServerConfig(podId, "az-a", "cluster-a", "bins/cluster-a",
                new StoreConfig("memory", Optional.empty()),
                Duration.ofSeconds(10), Duration.ofSeconds(3),
                "http://" + podId + ":8080",
                IngestConfig.defaults("cluster-a"), 0, "producer-1", java.util.Set.of("logs"), RetentionConfig.defaults(), java.util.Optional.empty(), "uid-" + podId, io.github.huyz0.os.biningester.ingest.CostTopKReporter.DEFAULT_INTERVAL, io.github.huyz0.os.biningester.ingest.IndexQuotas.Config.none(), false);
    }

    /**
     * ⚠️ REFUSES rather than silently succeeding: every case here leads its own
     * slots, so forwarding must never be reached — and a fake that ANSWERED
     * would hide a root that wired forwarding where it does not belong.
     */
    private static SequencerTransport noPeers() {
        return new SequencerTransport() {
            @Override
            public io.github.huyz0.os.biningester.format.CommitDelta send(String endpoint, CommitRequest request) {
                throw new UnsupportedOperationException("no peer expected: " + endpoint);
            }

            @Override
            public void close() {
            }
        };
    }

    private static ServerConfig config(String podId, String podUid) {
        return new ServerConfig(podId, "az-a", "cluster-a", "bins/cluster-a",
                new StoreConfig("memory", Optional.empty()),
                Duration.ofSeconds(10), Duration.ofSeconds(3), "http://" + podId + ":8080",
                IngestConfig.defaults("cluster-a"), 0, "producer-1", java.util.Set.of("logs"),
                new RetentionConfig(Duration.ofMinutes(1), Duration.ofHours(2),
                        Duration.ofSeconds(10), Duration.ofHours(3), Duration.ofDays(1)),
                Optional.empty(), podUid, io.github.huyz0.os.biningester.ingest.CostTopKReporter.DEFAULT_INTERVAL, io.github.huyz0.os.biningester.ingest.IndexQuotas.Config.none(), false);
    }

    private static final class CloseTrackingTransport implements SequencerTransport {
        private int closes;

        @Override
        public io.github.huyz0.os.biningester.format.CommitDelta send(
                String endpoint, CommitRequest request) {
            throw new UnsupportedOperationException("no peer expected: " + endpoint);
        }

        @Override
        public void close() {
            closes++;
        }
    }

    private static void registerLogs(Assembly assembly) {
        assembly.catalog().register(new IndexRegistration(INDEX_UUID, INDEX, List.of(), 4, 4, 1, 1));
    }

    private static SegmentRecord record(String id) {
        return new SegmentRecord(id, io.github.huyz0.os.biningester.format.OpType.INDEX,
                java.util.OptionalLong.of(1), id.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * ⚠️ THE PUSH IS ASYNCHRONOUS. `append` returns when the records are
     * DURABLE; the fan-out to subscribers happens after, so asserting
     * immediately is a race that fails on a fast machine and passes on a slow
     * one — or the reverse. A deadline, not a sleep.
     */
    private static void awaitPush(List<SubscriptionHub.Push> pushed) {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(20);
        while (pushed.isEmpty() && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
    }

    private static void write(Assembly assembly, String id) throws Exception {
        assembly.ingest().append(PRINCIPAL, INDEX, 0,
                sink -> sink.accept(record(id)));
    }

    @Test
    void aROOTDoesNotCloseAnINJECTEDTransportItDoesNotOwn() throws Exception {
        CloseTrackingTransport transport = new CloseTrackingTransport();
        try (BinStore shared = StoreFactory.open(new StoreConfig("memory", Optional.empty()))) {
            try (Assembly ignored = Assembly.open(config("pod1"), shared, transport,
                    Clock.systemUTC())) {
                assertThat(transport.closes).isZero();
            }
        }
        assertThat(transport.closes)
                .as("the caller owns an injected transport and decides when it closes")
                .isZero();
    }

    @Test
    void aTakeoverBackfillIsWiredToTheTermServingPredicate() throws Exception {
        AtomicReference<BooleanSupplier> serving = new AtomicReference<>();
        Assembly.BackfillStarter starter = (store, prefix, chain, live) -> {
            serving.set(live);
            return Thread.ofVirtual().start(() -> { });
        };

        try (BinStore shared = StoreFactory.open(new StoreConfig("memory", Optional.empty()))) {
            try (Assembly first = Assembly.open(config("pod1"), shared, noPeers(),
                    Clock.systemUTC())) {
                registerLogs(first);
                write(first, "before-takeover");
            }
            try (Assembly second = Assembly.openForTest(config("pod2"), shared, noPeers(),
                    Clock.systemUTC(), starter)) {
                assertThat(serving).as("a takeover must pass a live-term predicate")
                        .isNotNull();
                assertThat(serving.get().getAsBoolean()).isTrue();
            }
            assertThat(serving.get().getAsBoolean())
                    .as("the predicate must observe the term closing")
                    .isFalse();
        }
    }

    @Test
    void aWRITEThroughTheASSEMBLEDGraphLandsInTheASSEMBLEDStore() throws Exception {
        try (Assembly assembly = Assembly.open(config("pod1"), noPeers(), Clock.systemUTC())) {
            registerLogs(assembly);
            write(assembly, "doc-1");

            assertThat(assembly.store().list("bins/cluster-a", null, 100).objects())
                    .as("⚠️ THE SAME STORE THE ROOT BUILT, UNDER THE PREFIX IT WAS "
                            + "CONFIGURED WITH. An ingest over one store and a `store()` "
                            + "accessor returning another passes every component's own "
                            + "suite and is what this asserts against")
                    .isNotEmpty();
        }
    }

    @Test
    void theSEQUENCERAndTheWRITERShareTheSTOREAndThePREFIX() throws Exception {
        try (Assembly assembly = Assembly.open(config("pod1"), noPeers(), Clock.systemUTC())) {
            registerLogs(assembly);
            write(assembly, "doc-1");

            assertThat(assembly.store().list("bins/cluster-a/ctl/log/", null, 100).objects())
                    .as("⚠️ THE COMMIT LOG IS IN THE SAME STORE AND UNDER THE SAME PREFIX — "
                            + "`<prefix>/ctl/log/...` (`LogKeys`), which is the sequencer's "
                            + "own path and not one the writer touches. A sequencer given a "
                            + "store or a prefix of its own commits where nothing reads, and "
                            + "every append still returns an offset. ⚠️ Asserted on "
                            + "`ctl/log/` and NOT on `ctl/`, because the writer's index "
                            + "registry lives at `ctl/registry/indices.json` — MEASURED, "
                            + "and it made the first draft of this case pass against a "
                            + "sequencer wired to a different store entirely")
                    .isNotEmpty();
            assertThat(assembly.store().list("bins/cluster-a/ctl/lease/", null, 100).objects())
                    .as("and the LEASE too, which is what a peer reads to find the leader")
                    .isNotEmpty();
        }
    }

    @Test
    void theAssembledSequencerLeaseCarriesTheConfiguredPodUid() throws Exception {
        String uid = "uid-assembly-pod1";
        try (BinStore shared = StoreFactory.open(new StoreConfig("memory", Optional.empty()));
                Assembly assembly = Assembly.open(config("pod1", uid), shared, noPeers(),
                        Clock.systemUTC())) {
            try (var in = shared.get("bins/cluster-a/ctl/lease/0.json")) {
                assertThat(Lease.decode(in.readAllBytes()).holderPodUid()).isEqualTo(uid);
            }
        }
    }

    @Test
    void theAssembledGcLeaseCarriesTheConfiguredPodUid() throws Exception {
        String uid = "uid-assembly-pod1";
        AtomicReference<LeaseManager> gcLease = new AtomicReference<>();
        try (BinStore shared = StoreFactory.open(new StoreConfig("memory", Optional.empty()));
                Assembly assembly = Assembly.openForTestWithLeaseManagerFactory(
                        config("pod1", uid), shared, noPeers(),
                        Clock.systemUTC(), (RetentionAssembly.LeaseManagerFactory)
                                (store, leaseConfig, clock, challenge) -> {
                                    LeaseManager manager = new LeaseManager(store, leaseConfig,
                                            clock, challenge);
                                    if (leaseConfig.prefix().endsWith("/gc")) {
                                        gcLease.set(manager);
                                    }
                                    return manager;
                                })) {
            LeaseManager manager = gcLease.get();
            assertThat(manager).as("Assembly must wire its GC lease through the factory")
                    .isNotNull();
            manager.tryAcquire().orElseThrow();
            try (var in = shared.get("bins/cluster-a/gc/ctl/lease/0.json")) {
                assertThat(Lease.decode(in.readAllBytes()).holderPodUid()).isEqualTo(uid);
            } finally {
                manager.release();
            }
        }
    }

    @Test
    void thePUBLISHEDSegmentReachesASUBSCRIBEROfTheASSEMBLEDHub() throws Exception {
        try (Assembly assembly = Assembly.open(config("pod1"), noPeers(), Clock.systemUTC())) {
            registerLogs(assembly);
            List<SubscriptionHub.Push> pushed = new CopyOnWriteArrayList<>();
            RunKey stream = RunKey.ofIndexUuid(INDEX_UUID, 0);
            try (var ignored = assembly.hub().subscribe(stream,
                    SubscriptionHub.assembling(pushed::add))) {
                write(assembly, "doc-1");
                awaitPush(pushed);
            }
            assertThat(pushed)
                    .as("⚠️ THE HUB THE ROOT EXPOSES IS THE HUB THE WRITER PUBLISHES TO. "
                            + "Two hubs is a consumer that subscribes successfully, is never "
                            + "pushed to, and is indistinguishable from an idle cluster")
                    .isNotEmpty();
        }
    }

    @Test
    void theSTREAMAWriteLandsOnIsTheONEItsINDEXUuidNames() throws Exception {
        try (Assembly assembly = Assembly.open(config("pod1"), noPeers(), Clock.systemUTC())) {
            registerLogs(assembly);
            List<SubscriptionHub.Push> pushed = new CopyOnWriteArrayList<>();
            try (var ignored = assembly.hub().subscribe(RunKey.ofIndexUuid(INDEX_UUID, 2),
                    SubscriptionHub.assembling(pushed::add))) {
                assembly.ingest().append(PRINCIPAL, INDEX, 2,
                        sink -> sink.accept(record("doc-2")));
                awaitPush(pushed);
            }
            assertThat(pushed)
                    .as("⚠️ THE RESOLVER IS THE PIECE THIS CATCHES. A root that made up a "
                            + "stream id -- one per index NAME, say -- publishes to a key no "
                            + "consumer subscribes to, and the plugin derives its own key "
                            + "from the index UUID (`RunKey.ofIndexUuid`, M7.2). Two "
                            + "mappings that disagree do not throw; they make two streams "
                            + "for one index and the consumer's never moves")
                    .isNotEmpty();
        }
    }

    @Test
    void anUNREGISTEREDIndexIsREFUSEDRatherThanGivenAMadeUpStream() throws Exception {
        try (Assembly assembly = Assembly.open(config("pod1"), noPeers(), Clock.systemUTC())) {
            assertThatThrownBy(() -> assembly.ingest().append(PRINCIPAL, "never-registered", 0,
                    sink -> sink.accept(record("d"))))
                    .as("⚠️ A REFUSAL, after waiting PENDING_TIMEOUT for the registration "
                            + "(M10.30): a made-up stream for an index nobody registered "
                            + "writes records no consumer will ever subscribe to, and returns "
                            + "202 for them -- and the refusal is the one the front door "
                            + "answers 503, not the bare IllegalArgumentException it answered "
                            + "500")
                    .isInstanceOf(RegistrationTimeoutException.class);
        }
    }

    @Test
    void CLOSINGTheAssemblyRELEASESTheLeaseSoASUCCESSORCanLead() throws Exception {
        // ⚠️ ONE store across both assemblies, because a lease is a fact IN THE
        // STORE. With `memory` each root would build its own and the second
        // would lead trivially -- which is why this case injects the store.
        try (BinStore shared = StoreFactory.open(new StoreConfig("memory", Optional.empty()))) {
            try (Assembly first = Assembly.open(config("pod1"), shared, noPeers(),
                    Clock.systemUTC())) {
                assertThat(first.leading()).isTrue();
                assertThat(heldBy(shared))
                        .as("the lease is taken IN THE STORE THE ROOT WAS GIVEN — a "
                                + "sequencer over a store of its own leads every time and "
                                + "fences nobody, which is invisible to `leading()`")
                        .contains("pod1");
            }
            assertThat(expiryOf(heldBy(shared)))
                    .as("⚠️ A ROOT THAT NEVER RELEASES TURNS EVERY ROLLING DEPLOY INTO A TTL "
                            + "WAIT — research 08 §7 step 5 measures that a voluntary release "
                            + "turns a 10 s visibility stall into sub-second, and most "
                            + "real-world AZ-resilience incidents are rollouts. ⚠️ ASSERTED "
                            + "ON THE LEASE OBJECT'S OWN EXPIRY, which a release moves into "
                            + "the past: the holder's NAME stays (that is how a successor "
                            + "knows whom it replaced), and a successor simply LEADING is "
                            + "also what happens when each pod has a store of its own — so "
                            + "neither of those can carry this assertion")
                    .isLessThanOrEqualTo(System.currentTimeMillis());
            // ⚠️ WHOSE PROPERTY IS THIS? Review MEASURED that the release
            // survives removing the root's own `close` of the SEQUENCER, because
            // `DefaultIngest` closes the sequencer it was given. What the root
            // owns is closing the WRITER at all -- delete that and nothing
            // releases. Said here because a case named for the root asserting a
            // property of one of its parts is exactly the confusion this
            // milestone exists to end.
            try (Assembly second = Assembly.open(config("pod2"), shared, noPeers(),
                    Clock.systemUTC())) {
                assertThat(second.leading()).isTrue();
                assertThat(heldBy(shared)).contains("pod2");
            }
        }
    }

    /** What the lease object in {@code store} currently says, or {@code ""}. */
    private static String heldBy(BinStore store) throws Exception {
        var page = store.list("bins/cluster-a/ctl/lease/", null, 10);
        if (page.objects().isEmpty()) {
            return "";
        }
        try (var in = store.get(page.objects().get(0).key())) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    /** {@code expiresAtMillis} out of the lease object's JSON. */
    private static long expiryOf(String leaseJson) {
        java.util.regex.Matcher m =
                java.util.regex.Pattern.compile("\"expiresAtMillis\":(\\d+)").matcher(leaseJson);
        if (!m.find()) {
            throw new AssertionError("no expiresAtMillis in: " + leaseJson);
        }
        return Long.parseLong(m.group(1));
    }

    @Test
    void theROOTIsTheONLYPlaceABackendIsCHOSEN() throws Exception {
        try (Assembly assembly = Assembly.open(config("pod1"), noPeers(), Clock.systemUTC())) {
            assertThat(assembly.store().getClass().getPackageName())
                    .as("⚠️ criterion 2. The graph holds a BACKEND, chosen here from a "
                            + "configured name; every other module holds the `BinStore` "
                            + "INTERFACE and cannot name this package at all")
                    .isEqualTo("io.github.huyz0.os.biningester.binstore.backend");
        }
    }

    @Test
    void aSTARTUPRefusalRELEASESTheTermItAlreadyTook() throws Exception {
        // ⚠️ THE ORDER IS THE DEFECT: the sequencer's constructor ELECTS, and
        // `DefaultIngest`'s refuses when `direct` is configured over a backend
        // that cannot sign (M5.43) -- which is both shipping backends. Between
        // those two lines the lease is held by a node that is about to fail to
        // start, and nothing but this unwind will ever release it.
        io.github.huyz0.os.biningester.ingest.IngestConfig direct = new io.github.huyz0.os.biningester.ingest.IngestConfig(
                java.time.Duration.ofMillis(250), 8L << 20, "cluster-a",
                64L << 20, java.time.Duration.ofSeconds(5), 0.25, 0.75,
                java.time.Duration.ofMinutes(2), java.time.Duration.ofMinutes(2), true);
        ServerConfig refuses = new ServerConfig("pod1", "az-a", "cluster-a", "bins/cluster-a",
                new StoreConfig("memory", Optional.empty()), Duration.ofSeconds(10),
                Duration.ofSeconds(3), "http://pod1:8080", direct, 0, "producer-1",
                java.util.Set.of("logs"), RetentionConfig.defaults(), java.util.Optional.empty(), "uid-pod1", io.github.huyz0.os.biningester.ingest.CostTopKReporter.DEFAULT_INTERVAL, io.github.huyz0.os.biningester.ingest.IndexQuotas.Config.none(), false);

        try (BinStore shared = StoreFactory.open(new StoreConfig("memory", Optional.empty()))) {
            assertThatThrownBy(() -> Assembly.open(refuses, shared, noPeers(), Clock.systemUTC()))
                    .as("the startup refusal M5.43 exists for still happens")
                    .isInstanceOf(IllegalStateException.class);

            assertThat(expiryOf(heldBy(shared)))
                    .as("⚠️ AND THE TERM IT TOOK ON THE WAY IS RELEASED. Without the unwind "
                            + "the lease names a node that failed to start and is renewed "
                            + "every interval FOR THE LIFE OF THE JVM -- the fleet stops "
                            + "committing and nothing says why, which is the failure "
                            + "`FleetSequencer`'s own constructor comment guards against one "
                            + "level up. It is also an NFR-2 defect: one `putIfMatch` per "
                            + "renew interval from a node that is not running")
                    .isLessThanOrEqualTo(System.currentTimeMillis());
        }
    }

    @Test
    void aNULLConfigTransportOrClockIsREFUSED() {
        assertThatThrownBy(() -> Assembly.open(null, noPeers(), Clock.systemUTC()))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> Assembly.open(config("pod1"), null, Clock.systemUTC()))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> Assembly.open(config("pod1"), noPeers(), null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void aStoreCloseFailureIsSuppressedOnThePrimaryAssemblyFailure() throws Exception {
        IOException primary = new IOException("assembly failed");
        IOException closeFailure = new IOException("store close failed");
        AutoCloseable resource = () -> {
            throw closeFailure;
        };

        var closeQuietly = Assembly.class.getDeclaredMethod(
                "closeQuietly", AutoCloseable.class, Throwable.class);
        closeQuietly.setAccessible(true);
        closeQuietly.invoke(null, resource, primary);

        assertThat(primary.getSuppressed()).containsExactly(closeFailure);
    }

    @Test
    void CLOSINGTwiceIsNotAnError() throws Exception {
        Assembly assembly = Assembly.open(config("pod1"), noPeers(), Clock.systemUTC());
        assembly.close();
        assembly.close();
    }

    @Test
    void anASSEMBLYGivenAStoreDoesNOTCloseWhatItDidNotOpen() throws Exception {
        java.util.concurrent.atomic.AtomicInteger closes =
                new java.util.concurrent.atomic.AtomicInteger();
        try (BinStore shared = countingCloses(
                StoreFactory.open(new StoreConfig("memory", Optional.empty())), closes)) {
            Assembly assembly = Assembly.open(config("pod1"), shared, noPeers(),
                    Clock.systemUTC());
            assembly.close();
            assertThat(closes.get())
                    .as("⚠️ COUNTED AT THE STORE, because `MemoryBinStore` STAYS WRITABLE "
                            + "after `close()` -- MEASURED by review, which is why the first "
                            + "draft of this case (a `put` after close) passed against a "
                            + "root that closed the store unconditionally. A root that "
                            + "closes an injected store takes down whatever else holds it, "
                            + "and in the chaos harness that is the other node in the same "
                            + "JVM -- read at the store as an outage nobody injected")
                    .isZero();
        }
        assertThat(closes.get())
                .as("and the OWNER's own close still works, so this is not a store that "
                        + "cannot be closed at all")
                .isOne();
    }

    @Test
    void anASSEMBLYThatOPENEDTheStoreDOESCloseIt() throws Exception {
        Assembly assembly = Assembly.open(config("pod1"), noPeers(), Clock.systemUTC());
        registerLogs(assembly);
        write(assembly, "doc-1");
        BinStore owned = assembly.store();
        assembly.close();

        assertThat(owned.list("bins/cluster-a", null, 100).objects())
                .as("⚠️ A ROOT THAT OPENS A BACKEND AND NEVER CLOSES IT LEAKS WHAT THE "
                        + "BACKEND HOLDS -- file handles for `local-fs`, a connection pool "
                        + "for a real one -- once per start, on a node that restarts. "
                        + "⚠️ **OBSERVED AS A CLEARED STORE AND NOT AS A FAILED CALL**: an "
                        + "earlier draft recorded this direction as untestable because "
                        + "neither shipping backend throws after `close()`, which is true "
                        + "and beside the point -- `MemoryBinStore.close()` CLEARS, and "
                        + "`store()` hands back the very reference the root opened. Review "
                        + "MEASURED the omission: with the ownership flag ignored, every "
                        + "other case here stayed green")
                .isEmpty();
    }

    /** A store that counts {@code close()} and otherwise forwards. */
    private static BinStore countingCloses(BinStore delegate,
            java.util.concurrent.atomic.AtomicInteger closes) {
        return (BinStore) java.lang.reflect.Proxy.newProxyInstance(
                BinStore.class.getClassLoader(), new Class<?>[] {BinStore.class},
                (proxy, method, args) -> {
                    if ("close".equals(method.getName()) && method.getParameterCount() == 0) {
                        closes.incrementAndGet();
                        return null;
                    }
                    try {
                        return method.invoke(delegate, args);
                    } catch (java.lang.reflect.InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }
}
