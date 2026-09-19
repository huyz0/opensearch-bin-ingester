// SPDX-License-Identifier: Apache-2.0
package binjava.server;

import binjava.binstore.BinStore;
import binjava.binstore.HealthTrackingBinStore;
import binjava.format.IndexRegistration;
import binjava.format.RunKey;
import binjava.ingest.DefaultIngest;
import binjava.ingest.IndexCatalog;
import binjava.ingest.Ingest;
import binjava.ingest.IngestConfig;
import binjava.ingest.PendingPool;
import binjava.ingest.RoutedIngest;
import binjava.ingest.SubscriptionHub;
import binjava.ingest.LeasedGc;
import binjava.ingest.RetainedFloors;
import binjava.ingest.RetentionLoop;
import binjava.ingest.RetentionObservable;
import binjava.ingest.RetentionRule;
import binjava.ingest.SegmentGc;
import binjava.ingest.OrphanSweep;
import binjava.ingest.StoreGcLease;
import binjava.ingest.WatermarkTable;
import binjava.sequencer.Checkpoints;
import binjava.sequencer.BatchingSequencer;
import binjava.sequencer.ChainCollector;
import binjava.sequencer.FleetSequencer;
import binjava.sequencer.LeaseConfig;
import binjava.sequencer.LeaseChallenge;
import binjava.sequencer.LeaseManager;
import binjava.sequencer.LocalSequencer;
import binjava.sequencer.SequencerTransport;
import binjava.sequencer.Sequencer;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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

    private final ServerConfig config;
    private final BinStore store;
    private final HealthTrackingBinStore health;
    private final BinStore backend;
    private final SubscriptionHub hub;
    private final IndexCatalog catalog;
    private final WatermarkTable watermarks;
    private final RetentionLoop retention;
    private final RetainedFloors floors;
    private final FleetSequencer sequencer;
    private final DefaultIngest ingest;
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
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(challenge, "challenge");
        BinStore store = StoreFactory.open(config.store());
        try {
            return new Assembly(config, store, true, transport, clock, challenge);
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
        return new Assembly(config, Objects.requireNonNull(store, "store"), false,
                transport, clock, LeaseChallenge.NEVER);
    }

    private Assembly(ServerConfig config, BinStore raw, boolean ownsStore,
            SequencerTransport transport, Clock clock, LeaseChallenge challenge)
            throws IOException {
        this.config = Objects.requireNonNull(config, "config");
        Objects.requireNonNull(transport, "transport");
        Objects.requireNonNull(clock, "clock");
        // ⚠️ EVERY STORE CALL THIS NODE MAKES GOES THROUGH THE HEALTH TRACKER
        // (M8.15), so readiness reflects the store without a request of its
        // own. The RAW store is what is closed: the tracker holds nothing.
        this.backend = raw;
        this.health = new HealthTrackingBinStore(raw, clock,
                HealthTrackingBinStore.DEFAULT_STALL, HealthTrackingBinStore.DEFAULT_FAILURES);
        this.store = health;
        // ⚠️ `raw` IS NAMED SO THAT NOTHING BELOW CAN USE IT BY ACCIDENT: an
        // earlier draft kept the parameter called `store`, which shadowed the
        // field, and every consumer below was handed the UNTRACKED store --
        // readiness never saw a call. MEASURED by StorePartitionIT.
        BinStore store = this.store;
        if (ownsStore) {
            toClose.push(raw);
        }
        this.hub = new SubscriptionHub();
        this.catalog = new IndexCatalog();
        RetentionConfig kept = config.retention();
        this.watermarks = new WatermarkTable(clock, kept.reportTimeout(), kept.copyExpiry(),
                kept.minRetention());

        LeaseConfig leases = new LeaseConfig(config.prefix(), config.podId(), config.endpoint(),
                config.leaseTtl(), config.leaseRenewInterval());
        LeaseManager manager = new LeaseManager(store, leases, clock, challenge);
        this.sequencer = new FleetSequencer(store, leases, transport,
                () -> LocalSequencer.start(store, config.prefix(), manager, SEAL_REDRIVE_BUDGET)
                        .map(term -> new BatchingSequencer(term, COMMIT_WINDOW)), challenge);
        // ⚠️ NOT PUSHED ONTO `toClose`, AND THAT IS NOT AN OMISSION.
        // `DefaultIngest.close()` closes the sequencer it was given and says so
        // in its own javadoc, and `FleetSequencer.close()` has no idempotence
        // guard -- so closing it here too would run `leadership.close()` and
        // `closeTransport()` twice, the second on a transport THIS CLASS DOES
        // NOT OWN. MEASURED by review: with the duplicate push removed the
        // lease is still released, because the writer releases it.

        try {
            this.ingest = new DefaultIngest(config.ingest(), store, config.prefix(),
                    config.podId(), this.sequencer, this.hub, clock, this::streamFor);
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
        toClose.push(this.ingest);
        // ⚠️ THE ROUTED PATH IS WHAT THE FRONT DOOR IS HANDED (M8.32, FR-13).
        // Plain `DefaultIngest` has no catalog: it refuses every routed write,
        // and accepts an explicit partition the index does not have.
        this.routed = new RoutedIngest(ingest, catalog,
                new PendingPool(clock, PENDING_TIMEOUT, PENDING_BYTES_PER_INDEX),
                PENDING_TIMEOUT, clock);

        this.retention = retentionLoop(config, store, clock);
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
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
                Thread.ofVirtual().name("retention").factory());
        long every = kept.passInterval().toNanos();
        // ⚠️ FIXED DELAY, NOT FIXED RATE, AND THE FIRST TICK IS ONE INTERVAL
        // IN. A slow pass must not queue a burst of catch-up passes behind it,
        // and a node that has just started owns nothing old enough to collect.
        scheduler.scheduleWithFixedDelay(this::tickQuietly, every, every, TimeUnit.NANOSECONDS);
        toClose.push(() -> {
            scheduler.shutdownNow();
            // ⚠️ BOUNDED. A pass blocked on a store call is interrupted by
            // `shutdownNow`; one that is not answers within the bound or is
            // abandoned, because a shutdown that hung on GC would hold the
            // SEQUENCER term too -- and that is the lease whose release this
            // whole sequence exists to reach.
            scheduler.awaitTermination(5, TimeUnit.SECONDS);
        });
    }

    /**
     * The loop that collects expired segments and orphans on this node (M8.5).
     *
     * <p>⚠️ **THE GC LEASE IS ITS OWN OBJECT**, under {@code <prefix>/gc}:
     * sharing the sequencer's lease would make every pass contend with the
     * commit path, and releasing it at the end of a pass would hand the
     * SEQUENCER term to whoever asked next. See {@link StoreGcLease}.
     *
     * <p>⚠️ **THE SOURCE IS THE TERM THIS NODE HOLDS AND STILL SERVES, OR
     * NOTHING.** Held is not enough -- see {@code LocalSequencer.serving()}. A follower
     * writes no chain; a pass over a term it does not hold would condemn
     * segments it cannot fence. {@code heldTerm()} does not elect, so asking
     * costs nothing on the five pods in six that hold no term.
     */
    private RetentionLoop retentionLoop(ServerConfig config, BinStore store, Clock clock) {
        RetentionConfig kept = config.retention();
        LeaseConfig gcLease = new LeaseConfig(config.prefix() + "/gc", config.podId(),
                config.endpoint(), config.leaseTtl(), config.leaseRenewInterval());
        LeasedGc leased = new LeasedGc(
                new StoreGcLease(new LeaseManager(store, gcLease, clock), clock), store);
        RetentionRule rule = new RetentionRule(clock, kept.minRetention(), kept.maxRetention(),
                RetentionRule.DEFAULT_SAFETY_MARGIN,
                (segmentKey, age) -> LOG.log(System.Logger.Level.ERROR, () -> "a segment "
                        + segmentKey + " was deleted at the retention ceiling, " + age
                        + " old: a consumer had not read it and has lost data"),
                watermarks::of);
        RetentionObservable observable = new RetentionObservable(clock, kept.minRetention(),
                kept.maxRetention(), kept.reportTimeout(),
                alarm -> LOG.log(System.Logger.Level.WARNING, () -> "retention alarm "
                        + alarm.kind() + " on " + alarm.stream() + ": " + alarm.detail()));
        return new RetentionLoop(this::retentionTerm, leased, rule, observable, clock,
                config.prefix(), kept.minRetention(), OrphanSweep.DEFAULT_GRACE,
                SegmentGc.DEFAULT_DELETE_BATCH);
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

    /**
     * ⚠️ **A THROW OUT OF A SCHEDULED TASK CANCELS EVERY LATER RUN OF IT**, with
     * nothing logged and nothing to notice: GC would stop on this node for good
     * and storage would grow until someone looked at a bill. The loop already
     * contains its own failures; this is the belt to that brace.
     */
    private void tickQuietly() {
        try {
            retention.tick();
        } catch (RuntimeException failed) {
            LOG.log(System.Logger.Level.WARNING, () -> "a retention tick failed; the next one "
                    + "runs on schedule: " + failed);
        }
    }

    private static final System.Logger LOG = System.getLogger(Assembly.class.getName());

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

    /**
     * The stream an index name resolves to.
     *
     * <p>⚠️ **ONE MAPPING, AND IT IS THE PLUGIN'S.** The consumer derives its
     * subscription key from the index UUID ({@code RunKey.ofIndexUuid}, M7.2);
     * a root that made up its own — one per index NAME, say — would publish to
     * a key nobody subscribes to. Two mappings that disagree do not throw. They
     * make two streams for one index, and the consumer's never moves.
     *
     * @throws IllegalArgumentException if the index has not been registered.
     *     ⚠️ **REFUSED RATHER THAN INVENTED**, which M8.32 replaces with FR-13's
     *     pending pool: a made-up stream for an unregistered index accepts
     *     records no consumer will ever subscribe to and returns 202 for them.
     */
    private UUID streamFor(String indexOrAlias) {
        Optional<IndexRegistration> registration = catalog.resolve(indexOrAlias);
        if (registration.isEmpty()) {
            throw new IllegalArgumentException("index is not registered: " + indexOrAlias);
        }
        // ⚠️ THROUGH `RunKey.ofIndexUuid` RATHER THAN DECODING HERE, partition 0
        // discarded. The partition is the caller's and only the index half is
        // wanted, but routing the decode through the one method that owns it is
        // exactly M7.2's rule: an index uuid is BASE64URL, `UUID.fromString`
        // throws on every real one, and a second decoder that disagreed would
        // not throw -- it would make two streams for one index.
        return RunKey.ofIndexUuid(registration.get().indexUuid(), 0).indexId();
    }

    public ServerConfig config() {
        return config;
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
