// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.CostGovernor;
import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.GoverningBinStore;
import io.github.huyz0.os.biningester.binstore.HealthTrackingBinStore;
import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import io.github.huyz0.os.biningester.binstore.PutPurposeCounts;
import io.github.huyz0.os.biningester.binstore.StoreCounts;
import io.github.huyz0.os.biningester.ingest.DefaultIngest;
import io.github.huyz0.os.biningester.ingest.IndexCatalog;
import io.github.huyz0.os.biningester.ingest.Ingest;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
import io.github.huyz0.os.biningester.ingest.PendingPool;
import io.github.huyz0.os.biningester.ingest.RoutedIngest;
import io.github.huyz0.os.biningester.ingest.SubscriptionHub;
import io.github.huyz0.os.biningester.ingest.RetainedFloors;
import io.github.huyz0.os.biningester.ingest.RetentionLoop;
import io.github.huyz0.os.biningester.ingest.SegmentGc;
import io.github.huyz0.os.biningester.ingest.WatermarkTable;
import io.github.huyz0.os.biningester.ingest.SegmentPrefetcher;
import io.github.huyz0.os.biningester.ingest.DurableCatchUpResponder;
import io.github.huyz0.os.biningester.ingest.SnapshotCommittedDeltaSource;
import io.github.huyz0.os.biningester.format.DurableSegmentSignalFrame;
import io.github.huyz0.os.biningester.format.CatchUpRequestFrame;
import io.github.huyz0.os.biningester.http.CatchUpService;
import io.github.huyz0.os.biningester.http.DurableSegmentSignalSender;
import io.github.huyz0.os.biningester.http.EndpointSliceView;
import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.sequencer.Checkpoints;
import io.github.huyz0.os.biningester.sequencer.BatchingSequencer;
import io.github.huyz0.os.biningester.sequencer.ChainBackfill;
import io.github.huyz0.os.biningester.sequencer.ChainMemory;
import io.github.huyz0.os.biningester.sequencer.InboxDrain;
import io.github.huyz0.os.biningester.sequencer.ChainCollector;
import io.github.huyz0.os.biningester.sequencer.FleetSequencer;
import io.github.huyz0.os.biningester.sequencer.LeaseConfig;
import io.github.huyz0.os.biningester.sequencer.LeaseChallenge;
import io.github.huyz0.os.biningester.sequencer.LeaseManager;
import io.github.huyz0.os.biningester.sequencer.LocalSequencer;
import io.github.huyz0.os.biningester.sequencer.SequencerTransport;
import io.github.huyz0.os.biningester.sequencer.Sequencer;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BooleanSupplier;

/**
 * The composition root: configuration in, object graph out (M8.1,
 * <a href="../../../../../../../docs/internal/product/decisions/0052-m8-owns-assembly-and-the-first-real-backend-because-its-evidence-is-unbuyable-without-them.md">ADR-0052</a>).
 *
 * <p>⚠️ **THE PIECES HAVE ALL BEEN TESTED AND HAVE NEVER BEEN IN ONE OBJECT
 * GRAPH.** That is what this class is for, and it is why its own tests assert
 * CONNECTEDNESS rather than behaviour: a root that hands the writer one store
 * and the sequencer another, or exposes a hub nothing publishes to, compiles,
 * starts, and loses data in a way no unit test of any single component can see.
 *
 * <p>⚠️ **NOTHING HERE LISTENS ON A SOCKET.** The HTTP front door is
 * {@code FrontDoor}'s (M8.4). The retention loop and the GC lease are built
 * here (M8.5) and run on this graph's own scheduler; the progress reporter is
 * M8.6's. Each lands as one commit, because a root that arrives last discovers
 * every composition failure at once.
 *
 * <p>⚠️ **THE TRANSPORT IS A PARAMETER AND NOT BUILT HERE**, because no
 * production {@link SequencerTransport} exists yet — M5.6e, owed since M5 and
 * scheduled as M8.20. Taking it as an argument is the honest shape until then:
 * this root can be assembled and asserted today, and M8.20 changes one call
 * site rather than this class's design.
 */
public final class Assembly implements AutoCloseable {

    @FunctionalInterface
    interface BackfillStarter {
        Thread start(BinStore store, String prefix, ChainMemory chain, BooleanSupplier serving);
    }

    private final ServerConfig config;
    private final BinStore store;
    private final CountingBinStore counting;
    private final CostGovernor governor;
    private final GovernorMetrics governorMetrics;
    private final HealthTrackingBinStore health;
    private final IngesterMetrics metrics;
    private final BinStore backend;
    private final SubscriptionHub hub;
    private final IndexCatalog catalog;
    private final WatermarkTable watermarks;
    private final RetentionLoop retention;
    private final RetainedFloors floors;
    private final FleetSequencer sequencer;
    private final DefaultIngest ingest;
    private final EndpointSliceView peerView;
    private final SegmentPrefetcher prefetcher;
    private final RoutedIngest routed;
    private final Deque<AutoCloseable> toClose = new ArrayDeque<>();
    private volatile boolean closed;

    /**
     * Builds the graph, opening the store {@code config} names.
     *
     * <p>⚠️ **THIS OVERLOAD OWNS THE STORE IT OPENS AND CLOSES IT.** The other
     * one does not close what it was handed — see there.
     */
    public static Assembly open(ServerConfig config, SequencerTransport transport, Clock clock)
            throws IOException {
        return open(config, transport, clock, LeaseChallenge.NEVER);
    }

    /**
     * The same, taking the sequencer term early on {@code challenge}'s evidence
     * that its holder is gone (M8.13, NFR-9).
     *
     * <p>⚠️ **THE SEQUENCER'S LEASE ONLY.** The GC lease keeps waiting out its
     * TTL: a GC pass a few seconds late costs nothing, and a second holder of
     * it would be fenced anyway.
     */
    public static Assembly open(ServerConfig config, SequencerTransport transport, Clock clock,
            LeaseChallenge challenge) throws IOException {
        return open(config, transport, clock, challenge, null, null);
    }

    /** Builds the graph with the live EndpointSlice view used by durable-segment hints. */
    public static Assembly open(ServerConfig config, SequencerTransport transport, Clock clock,
            LeaseChallenge challenge, EndpointSliceView peerView, CrossAzBytes crossAz)
            throws IOException {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(challenge, "challenge");
        BinStore store = StoreFactory.open(config.store());
        try {
            return new Assembly(config, store, true, transport, clock, challenge,
                    ChainBackfill::inBackground, peerView, crossAz, null);
        } catch (RuntimeException | IOException failed) {
            // ⚠️ THE STORE IS OURS AND THE CONSTRUCTOR THREW, so nobody else
            // holds a reference that could close it. Without this, a failure
            // half-way through assembly leaks whatever the store holds -- file
            // handles for `local-fs`, a connection pool for a real backend.
            closeQuietly(store, failed);
            throw failed;
        }
    }

    /**
     * Builds the graph over a store the CALLER owns.
     *
     * <p>⚠️ **{@link #close()} DOES NOT CLOSE IT**, which matters more than it
     * looks: the chaos harness runs several pods over one store in one JVM, and
     * a root that closed an injected store would take the other pods down with
     * it — read at the store as an outage that nobody injected.
     */
    public static Assembly open(ServerConfig config, BinStore store,
            SequencerTransport transport, Clock clock) throws IOException {
        return open(config, store, transport, clock, null, null);
    }

    /** Same as {@link #open(ServerConfig, BinStore, SequencerTransport, Clock)}, with peer hints. */
    public static Assembly open(ServerConfig config, BinStore store,
            SequencerTransport transport, Clock clock, EndpointSliceView peerView,
            CrossAzBytes crossAz) throws IOException {
        return new Assembly(config, Objects.requireNonNull(store, "store"), false,
                transport, clock, LeaseChallenge.NEVER, ChainBackfill::inBackground,
                peerView, crossAz, null);
    }

    static Assembly openForTest(ServerConfig config, BinStore store,
            SequencerTransport transport, Clock clock, EndpointSliceView peerView,
            CrossAzBytes crossAz, DurableSegmentSignalSender.PeerPost signalPost)
            throws IOException {
        return new Assembly(config, Objects.requireNonNull(store, "store"), false,
                transport, clock, LeaseChallenge.NEVER, ChainBackfill::inBackground,
                peerView, crossAz, signalPost);
    }

    static Assembly openForTest(ServerConfig config, BinStore store,
            SequencerTransport transport, Clock clock, BackfillStarter backfillStarter)
            throws IOException {
        return openForTest(config, store, transport, clock, backfillStarter,
                GovernorWiring.DEFAULT);
    }

    static Assembly openForTest(ServerConfig config, BinStore store,
            SequencerTransport transport, Clock clock, BackfillStarter backfillStarter,
            GovernorWiring.GovernorFactory governorFactory) throws IOException {
        return new Assembly(config, Objects.requireNonNull(store, "store"), false,
                transport, clock, LeaseChallenge.NEVER, backfillStarter, null, null, null,
                LeaseManager::new, governorFactory);
    }

    static Assembly openForTestWithLeaseManagerFactory(ServerConfig config, BinStore store,
            SequencerTransport transport, Clock clock,
            RetentionAssembly.LeaseManagerFactory leaseManagerFactory)
            throws IOException {
        return new Assembly(config, Objects.requireNonNull(store, "store"), false,
                transport, clock, LeaseChallenge.NEVER, ChainBackfill::inBackground,
                null, null, null, leaseManagerFactory, GovernorWiring.DEFAULT);
    }

    private Assembly(ServerConfig config, BinStore raw, boolean ownsStore,
            SequencerTransport transport, Clock clock, LeaseChallenge challenge,
            BackfillStarter backfillStarter, EndpointSliceView peerView, CrossAzBytes crossAz,
            DurableSegmentSignalSender.PeerPost signalPost)
            throws IOException {
        this(config, raw, ownsStore, transport, clock, challenge, backfillStarter,
                peerView, crossAz, signalPost, LeaseManager::new, GovernorWiring.DEFAULT);
    }

    private Assembly(ServerConfig config, BinStore raw, boolean ownsStore,
            SequencerTransport transport, Clock clock, LeaseChallenge challenge,
            BackfillStarter backfillStarter, EndpointSliceView peerView, CrossAzBytes crossAz,
            DurableSegmentSignalSender.PeerPost signalPost,
            RetentionAssembly.LeaseManagerFactory leaseManagerFactory,
            GovernorWiring.GovernorFactory governorFactory) throws IOException {
        this.config = Objects.requireNonNull(config, "config");
        Objects.requireNonNull(transport, "transport");
        Objects.requireNonNull(clock, "clock");
        Objects.requireNonNull(backfillStarter, "backfillStarter");
        Objects.requireNonNull(leaseManagerFactory, "leaseManagerFactory");
        Objects.requireNonNull(governorFactory, "governorFactory");
        // ⚠️ EVERY STORE CALL THIS NODE MAKES GOES THROUGH THE HEALTH TRACKER
        // (M8.15), so readiness reflects the store without a request of its
        // own. The RAW store is what is closed: the tracker holds nothing.
        this.backend = raw;
        this.peerView = peerView == null ? new EndpointSliceView() : peerView;
        this.counting = new CountingBinStore(raw);
        // ⚠️ THE GOVERNOR SITS ABOVE THE COUNTER (M10.11, ADR-0075), so a LIST it
        // refuses never reached the store and is never counted as a request.
        GovernorWiring.Spacing spacing = new GovernorWiring.Spacing(config);
        this.governor = governorFactory.create(config, clock, spacing);
        this.governorMetrics = GovernorMetrics.bind(governor);
        this.health = new HealthTrackingBinStore(new GoverningBinStore(counting, governor), clock,
                HealthTrackingBinStore.DEFAULT_STALL, HealthTrackingBinStore.DEFAULT_FAILURES);
        this.store = health;
        this.metrics = new IngesterMetrics();
        // ⚠️ `raw` IS NAMED SO THAT NOTHING BELOW CAN USE IT BY ACCIDENT: an
        // earlier draft kept the parameter called `store`, which shadowed the
        // field, and every consumer below was handed the UNTRACKED store --
        // readiness never saw a call. MEASURED by StorePartitionIT.
        BinStore store = this.store;
        if (ownsStore) {
            toClose.push(raw);
        }
        this.hub = new SubscriptionHub(config.az());
        this.catalog = new IndexCatalog();
        RetentionConfig kept = config.retention();
        this.watermarks = new WatermarkTable(clock, kept.reportTimeout(), kept.copyExpiry(),
                kept.minRetention());

        LeaseConfig leases = sequencerLeaseConfig(config);
        LeaseManager manager = leaseManagerFactory.create(store, leases, clock, challenge);
        this.sequencer = new FleetSequencer(store, leases, transport,
                () -> LocalSequencer.start(store, config.prefix(), manager, SEAL_REDRIVE_BUDGET)
                        .map(term -> {
                            // ⚠️ M8.42: THE CHAIN BELOW THE REPLAY, read once per
                            // takeover and off the election's path. HERE, not in
                            // `LocalSequencer.start`, which M4.9 bounds to a
                            // small constant and tests to the request.
                            // ⚠️ ONLY A TAKEOVER HAS A CHAIN BELOW IT: the first term
                            // of a cluster (epoch 1) would otherwise widen its sweep
                            // over a retention window of empty hours, a LIST each.
                            if (term.epoch() > 1) {
                                backfillStarter.start(store, config.prefix(),
                                        term.chain(), term::serving);
                            }
                            // ⚠️ M8.14a: the intents of pods that died deferring
                            // have nobody else to ask for a drain.
                            InboxDrain.inBackground(store, config.prefix(), term,
                                    metrics::failedIntentBatch);
                            return new BatchingSequencer(term, COMMIT_WINDOW);
                        }), challenge, false, metrics::failedIntentBatch);
        // ⚠️ NOT PUSHED ONTO `toClose`, AND THAT IS NOT AN OMISSION.
        // `DefaultIngest.close()` closes the sequencer it was given and says so
        // in its own javadoc, and `FleetSequencer.close()` has no idempotence
        // guard -- so closing it here too would run `leadership.close()` and
        // `closeTransport()` twice, the second on a transport THIS CLASS DOES
        // NOT OWN. MEASURED by review: with the duplicate push removed the
        // lease is still released, because the writer releases it.

        try {
            DurableSegmentSignalSender signalSender =
                    EndpointMembership.signalSender(config, peerView, crossAz, signalPost);
            this.ingest = new DefaultIngest(config.ingest(), store, config.prefix(),
                    config.podId(), this.sequencer, this.hub, clock,
                    name -> CatalogStreams.streamFor(catalog, name),
                    segmentKey -> {
                        if (signalSender != null) {
                            signalSender.send(new DurableSegmentSignalFrame(config.podId(),
                                    config.az(), segmentKey), this.peerView.readyEndpoints());
                        }
                    });
            this.prefetcher = new SegmentPrefetcher(new EndpointMembership(config, this.peerView),
                    this.ingest.segmentProxy(), governor::discretionaryAllowed);
        } catch (RuntimeException | IOException failed) {
            // ⚠️ THE TERM IS ALREADY TAKEN AT THIS POINT, and if this throws
            // nothing will ever hold a reference to the sequencer again.
            // `new Leadership(election)` ELECTS IN ITS CONSTRUCTOR -- it
            // acquires the lease and starts the renewer -- so a failure here
            // leaves the lease naming a node that failed to start, renewed
            // every interval for the life of the JVM. That is the failure
            // `FleetSequencer`'s own constructor comment guards a null argument
            // against, one level up: the fleet stops committing and nothing
            // says why. It is also an NFR-2 defect -- one `putIfMatch` per
            // renew interval, for ever, from a node that is not running.
            // ⚠️ AND THE TRIGGER IS CONFIGURED RATHER THAN HYPOTHETICAL:
            // `DefaultIngest` refuses at startup when `direct` is enabled over
            // a backend that cannot sign (M5.43), as the memory and
            // local-filesystem backends cannot.
            closeQuietly(this.sequencer, failed);
            throw failed;
        }
        // ⚠️ FROM HERE the governor reads the ingest's spacing; until now, the floor.
        spacing.attach(this.ingest);
        toClose.push(this.ingest);
        // ⚠️ THE ROUTED PATH IS WHAT THE FRONT DOOR IS HANDED (M8.32, FR-13).
        // Plain `DefaultIngest` has no catalog: it refuses every routed write,
        // and accepts an explicit partition the index does not have.
        this.routed = new RoutedIngest(ingest, catalog,
                new PendingPool(clock, PENDING_TIMEOUT, PENDING_BYTES_PER_INDEX),
                PENDING_TIMEOUT, clock);

        this.retention = RetentionAssembly.create(config, store, clock, this::retentionTerm,
                watermarks, leaseManagerFactory, governor::discretionaryAllowed);
        // ⚠️ THE FLOOR A CONSUMER IS TOLD, READ ON DEMAND (ADR-0056). The epoch
        // is the one this pod last committed under, known without a request;
        // below 1 there has been no lease and there is no chain to read.
        this.floors = new RetainedFloors(() -> {
            long epoch = sequencer.epoch();
            return epoch < 1 ? Optional.empty()
                    : Checkpoints.newest(store, config.prefix(), epoch);
        }, clock, RetainedFloors.DEFAULT_REFRESH);
        // ⚠️ PUSHED LAST, SO IT IS CLOSED FIRST. `toClose` is a stack, and a
        // retention tick that ran while the writer was closing would read a
        // chain whose term is being released under it -- and take the GC lease
        // on the way, which the shutdown then has to wait out.
        toClose.push(RetentionAssembly.schedule(retention, kept.passInterval()));
    }

    static LeaseConfig sequencerLeaseConfig(ServerConfig config) {
        return new LeaseConfig(config.prefix(), config.podId(), config.endpoint(),
                config.podUid(), config.leaseTtl(), config.leaseRenewInterval());
    }

    /**
     * The term the retention loop works on, if this node serves one.
     *
     * <p>⚠️ {@code serving()}, NOT MERELY {@code instanceof}: a term this node
     * still HOLDS may already have been fenced by a takeover, and its frozen
     * chain as a keep list deletes the successor's committed segments. Package-
     * private so a case can drive the chain GC this wires (M8.39).
     */
    Optional<RetentionLoop.Term> retentionTerm() {
        return LocalSequencer.underneath(sequencer.heldTerm()).filter(LocalSequencer::serving)
                .map(local -> new RetentionLoop.Term(local.chain(), local::observeRetained,
                        local::serving, fenced -> new ChainCollector(local, config.prefix())
                                .collect(fenced, SegmentGc.DEFAULT_DELETE_BATCH)));
    }

    /** Responds from this node's currently-served committed chain without electing a term. */
    void respondCatchUp(CatchUpRequestFrame request, CatchUpService.FrameSink sink)
            throws IOException {
        LocalSequencer local = LocalSequencer.underneath(sequencer.heldTerm())
                .filter(LocalSequencer::serving)
                .orElseThrow(() -> new IOException("this node has no serving committed chain"));
        ChainMemory.Snapshot snapshot = local.chain().snapshot();
        if (!snapshot.complete()) {
            throw new IOException("the committed chain is incomplete and cannot replay safely");
        }
        new DurableCatchUpResponder(store,
                new SnapshotCommittedDeltaSource(() -> snapshot), local::epoch,
                ingest.costLedger())
                .respond(request, sink::write);
    }

    /**
     * ⚠️ 8, the value every construction site in the tree passes — it bounds
     * how many ancestor seals one takeover will redrive before giving up.
     */
    private static final int SEAL_REDRIVE_BUDGET = 8;

    /**
     * How long the leader's commit window stays open once a commit arrives
     * (M8.50).
     *
     * <p>⚠️ **SHORT, BECAUSE THE BATCH COMES FROM THE PUT, NOT THE WAIT.** While
     * one delta is in the store, every commit that arrives queues, and the next
     * window takes them all: that is where one PUT per window comes from under
     * load. The window itself only adds latency to a commit that arrives alone.
     */
    static final Duration COMMIT_WINDOW = Duration.ofMillis(5);

    /** How long a routed write waits for its index's registration: ADR-0015's default. */
    static final Duration PENDING_TIMEOUT = Duration.ofSeconds(5);

    /**
     * ⚠️ THE BOUND ON WHAT ONE UNREGISTERED INDEX MAY HOLD (ADR-0015): one
     * default segment's worth, so a producer racing the plugin is absorbed and
     * one writing to an index nobody registers is refused rather than grown.
     */
    static final long PENDING_BYTES_PER_INDEX = IngestConfig.DEFAULT_MAX_SEGMENT_BYTES;


    public ServerConfig config() {
        return config;
    }

    /** The pod's one cost governor, which the store stack consults (M10.11). */
    public CostGovernor governor() {
        return governor;
    }

    /**
     * Whether the store is answering the calls this node makes (M8.15). The
     * readiness probe fails when it is not, so the load balancer stops sending
     * writes that could only wait.
     */
    public boolean storeHealthy() {
        return health.healthy();
    }

    /**
     * The backend the root chose, UNTRACKED (criterion 2).
     *
     * <p>⚠️ **NOT THE STORE THE GRAPH USES**, which is this wrapped in the
     * health tracker (M8.15). A caller reading through this one is outside the
     * node's own call path, and its calls say nothing about the node's health.
     */
    public BinStore store() {
        return backend;
    }

    /** The store the graph itself uses: tracked, governed, then counted (M10.11). */
    BinStore nodeStore() {
        return store;
    }

    EndpointSliceView peerView() {
        return peerView;
    }

    void prefetchDurableSegment(String segmentKey, String writerAz) throws IOException {
        prefetcher.onDurable(segmentKey, writerAz);
    }

    /** The same node-wide cache read path the subscription service serves from. */
    public io.github.huyz0.os.biningester.ingest.SegmentProxy segmentProxy() {
        return ingest.segmentProxy();
    }

    /** The requests issued by this node, including calls made by health checks. */
    public StoreCounts storeCounts() {
        return counting.counts();
    }

    /** PUT counts partitioned by object-key purpose; all categories remain in storeCounts().puts(). */
    public PutPurposeCounts putPurposeCounts() {
        return counting.putPurposeCounts();
    }

    IngesterMetrics metrics() {
        return metrics;
    }

    /** Each index's apportioned share of this pod's store requests (ADR-0077). */
    public IndexCostLedger costLedger() {
        return ingest.costLedger();
    }

    /** The data-segment GETs this node issued: the read side's denominator (M11.3). */
    long dataSegmentGets() {
        return counting.dataSegmentGets();
    }

    GovernorMetrics governorMetrics() {
        return governorMetrics;
    }

    public SubscriptionHub hub() {
        return hub;
    }

    public IndexCatalog catalog() {
        return catalog;
    }

    /**
     * The one table consumer positions are reported into and read out of.
     *
     * <p>⚠️ **ONE, SHARED.** The subscription service writes it and the
     * retention rule built in this class reads it; two tables is a GC pass
     * that sees no consumer
     * has reported and either deletes what is still being read or never
     * deletes anything, depending on which way the rule defaults.
     */
    public WatermarkTable watermarks() {
        return watermarks;
    }

    /**
     * The retained floors this node serves to a consumer that resumes (M8.6).
     *
     * <p>⚠️ **ONE PER NODE**, so its refresh bound is per node: a second
     * instance per service would double the checkpoint reads a reconnect burst
     * costs.
     */
    public RetainedFloors floors() {
        return floors;
    }

    /** The retention loop, for a test that ticks it by hand. */
    public RetentionLoop retention() {
        return retention;
    }

    /**
     * The term this pod holds, or {@code null} where it holds none — what
     * answers a commit a peer FORWARDED here (M8.4).
     *
     * <p>⚠️ **NOT {@link #ingest()}'s sequencer.** See
     * {@code FleetSequencer.heldTerm()}: applying a forwarded commit through
     * the fleet sequencer consults the lease and forwards it onward again.
     */
    public Sequencer heldTerm() {
        return sequencer.heldTerm();
    }

    public Ingest ingest() {
        return routed;
    }

    /**
     * PUTs and commits whatever is buffered now, and answers every append
     * waiting on it (M8.7, research 08 §7 steps 3 and 4).
     *
     * <p>⚠️ **IT DOES NOT RELEASE THE LEASE.** {@link #close()} does, after
     * its own last flush, and the two are separate so the shutdown can finish
     * the requests inside the door before it lets the term go.
     */
    public void flush() throws IOException {
        ingest.flushNow();
        journal.accept(FLUSHED);
    }

    static final String FLUSHED = "flushed";
    static final String GRAPH_CLOSED = "graph closed, lease released";

    private volatile java.util.function.Consumer<String> journal = event -> { };

    /**
     * Where {@link #flush()} and {@link #close()} say they happened (M8.7).
     *
     * <p>⚠️ **WRITTEN HERE, AT THE EFFECT**, so a shutdown that closed the
     * graph early shows it, whatever the sequence's own report says.
     */
    void journal(java.util.function.Consumer<String> sink) {
        this.journal = Objects.requireNonNull(sink, "sink");
    }

    /** Whether this pod currently holds a sequencer term. */
    public boolean leading() {
        return sequencer.leading();
    }

    /**
     * Shuts the graph down, releasing the lease.
     *
     * <p>⚠️ **RELEASING IS WHY THIS EXISTS**, not tidiness: research 08 §7 step
     * 5 measures that a voluntary release turns a 10 s visibility stall into
     * sub-second on every deploy, and most real-world "AZ resilience" incidents
     * are rollouts. ⚠️ The ORDER this does it in is M8.7's, which owns the full
     * shutdown sequence; here it is close-in-reverse-construction.
     *
     * <p>⚠️ **IDEMPOTENT**, because a shutdown hook and a try-with-resources
     * will both call it once M8.4 lands a process.
     */
    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        IOException first = null;
        while (!toClose.isEmpty()) {
            try {
                toClose.pop().close();
            } catch (Exception failed) {
                // ⚠️ EVERY PIECE IS CLOSED EVEN IF ONE THROWS, and the lease is
                // the reason: a writer that fails to flush must not leave the
                // term held, or the pod that replaces it waits out the TTL.
                first = first != null ? first : asIoException(failed);
            }
        }
        journal.accept(GRAPH_CLOSED);
        if (first != null) {
            throw first;
        }
    }

    private static IOException asIoException(Exception failed) {
        return failed instanceof IOException io ? io : new IOException(failed);
    }

    private static void closeQuietly(AutoCloseable resource, Throwable primary) {
        try {
            resource.close();
        } catch (Exception suppressed) {
            primary.addSuppressed(suppressed);
        }
    }
}
