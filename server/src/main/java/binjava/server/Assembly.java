// SPDX-License-Identifier: Apache-2.0
package binjava.server;

import binjava.binstore.BinStore;
import binjava.format.IndexRegistration;
import binjava.format.RunKey;
import binjava.ingest.DefaultIngest;
import binjava.ingest.IndexCatalog;
import binjava.ingest.Ingest;
import binjava.ingest.SubscriptionHub;
import binjava.ingest.WatermarkTable;
import binjava.sequencer.FleetSequencer;
import binjava.sequencer.LeaseConfig;
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
 * <p>⚠️ **NOTHING HERE LISTENS ON A SOCKET.** The HTTP front door, the
 * retention loop, the GC lease and the progress reporter are wired in later
 * tasks (M8.4, M8.5, M8.6), each one commit, because a root that arrives last
 * discovers every composition failure at once.
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
    private final SubscriptionHub hub;
    private final IndexCatalog catalog;
    private final WatermarkTable watermarks;
    private final FleetSequencer sequencer;
    private final DefaultIngest ingest;
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
        Objects.requireNonNull(config, "config");
        BinStore store = StoreFactory.open(config.store());
        try {
            return new Assembly(config, store, true, transport, clock);
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
                transport, clock);
    }

    private Assembly(ServerConfig config, BinStore store, boolean ownsStore,
            SequencerTransport transport, Clock clock) throws IOException {
        this.config = Objects.requireNonNull(config, "config");
        Objects.requireNonNull(transport, "transport");
        Objects.requireNonNull(clock, "clock");
        this.store = store;
        if (ownsStore) {
            toClose.push(store);
        }
        this.hub = new SubscriptionHub();
        this.catalog = new IndexCatalog();
        this.watermarks = new WatermarkTable(clock, WATERMARK_REPORT_TIMEOUT,
                WATERMARK_COPY_EXPIRY, MIN_RETENTION);

        LeaseConfig leases = new LeaseConfig(config.prefix(), config.podId(), config.endpoint(),
                config.leaseTtl(), config.leaseRenewInterval());
        LeaseManager manager = new LeaseManager(store, leases, clock);
        this.sequencer = new FleetSequencer(store, leases, transport,
                () -> LocalSequencer.start(store, config.prefix(), manager, SEAL_REDRIVE_BUDGET));
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
            // a backend that cannot sign (M5.43), which is both of the backends
            // that ship today.
            closeQuietly(this.sequencer, failed);
            throw failed;
        }
        toClose.push(this.ingest);
    }

    /**
     * ⚠️ **THREE CONSTANTS THAT M8.5 TURNS INTO CONFIGURATION**, and they are
     * here rather than in {@link ServerConfig} deliberately: the retention rule
     * that reads them is not wired yet, and a settings key an operator can
     * write which nothing consults is worse than no key at all. What needs them
     * TODAY is {@code SubscriptionService}, which takes the same
     * {@link WatermarkTable} the retention loop will — one table, so the
     * positions consumers report are the positions GC reads (M7.21n). ⚠️ The
     * ORDER matters and the table enforces it: the copy expiry must outlive
     * both the report timeout and the retention floor, or a shard copy is
     * retired while its data is still inside the outage budget.
     */
    static final Duration WATERMARK_REPORT_TIMEOUT = Duration.ofMinutes(1);

    static final Duration WATERMARK_COPY_EXPIRY = Duration.ofHours(2);

    static final Duration MIN_RETENTION = Duration.ofHours(1);

    /**
     * ⚠️ 8, the value every construction site in the tree passes — it bounds
     * how many ancestor seals one takeover will redrive before giving up.
     */
    private static final int SEAL_REDRIVE_BUDGET = 8;

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

    public BinStore store() {
        return store;
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
     * <p>⚠️ **ONE, SHARED.** The subscription service writes it and M8.5's
     * retention rule reads it; two tables is a GC pass that sees no consumer
     * has reported and either deletes what is still being read or never
     * deletes anything, depending on which way the rule defaults.
     */
    public WatermarkTable watermarks() {
        return watermarks;
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
        return ingest;
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
