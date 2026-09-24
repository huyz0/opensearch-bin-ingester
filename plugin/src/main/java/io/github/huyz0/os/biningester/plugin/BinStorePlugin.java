// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.opensearch.index.IngestionConsumerFactory;
import org.opensearch.plugins.IngestionConsumerPlugin;
import org.opensearch.plugins.Plugin;

/**
 * Registers this ingestion source with an OpenSearch node.
 *
 * <p>WARNING: extends {@code Plugin} AS WELL AS implementing
 * {@code IngestionConsumerPlugin}, because node-level shared state has nowhere
 * else to live: the factory is called once per shard, so a subscription or cache
 * built there is multiplied by the shard count (criterion 6, cost rule R5).
 *
 * <p>The index opts in with:
 * <pre>
 * index.ingestion_source.type: BINSTORE
 * index.ingestion_source.mapper_type: default
 * </pre>
 */
public final class BinStorePlugin extends Plugin implements IngestionConsumerPlugin {

    /** The value of {@code index.ingestion_source.type}. */
    public static final String TYPE = "BINSTORE";

    /**
     * How often this node reports its shard copies' committed positions
     * (M8.43).
     *
     * <p>⚠️ A THIRD OF THE INGESTER's DEFAULT REPORT TIMEOUT (one minute), so
     * one lost report does not freeze a copy's watermark.
     */
    public static final org.opensearch.common.settings.Setting<
            org.opensearch.common.unit.TimeValue> PROGRESS_INTERVAL =
            org.opensearch.common.settings.Setting.timeSetting("binstore.progress.interval",
                    org.opensearch.common.unit.TimeValue.timeValueSeconds(20),
                    org.opensearch.common.unit.TimeValue.timeValueMillis(100),
                    org.opensearch.common.settings.Setting.Property.NodeScope);

    /**
     * The ingester this node subscribes to (M8.31).
     *
     * <p>⚠️ **IT IS HOW A REAL NODE GETS SUBSCRIPTIONS AT ALL.** Only tests
     * call {@link #install}; a node loads this plugin reflectively, and before
     * this setting it held nothing -- no transport, no registrar, no fetcher.
     * Unset, the node still holds nothing, which is the plugin loaded on a
     * node that ingests from no ingester.
     */
    public static final org.opensearch.common.settings.Setting<String> INGESTER_ENDPOINT =
            org.opensearch.common.settings.Setting.simpleString("binstore.ingester.endpoint",
                    org.opensearch.common.settings.Setting.Property.NodeScope);

    /** Optional authenticated node-local reader; its credential is never a setting value. */
    public static final org.opensearch.common.settings.Setting<String> READER_ENDPOINT =
            org.opensearch.common.settings.Setting.simpleString("binstore.reader.endpoint", "",
                    org.opensearch.common.settings.Setting.Property.NodeScope);
    public static final org.opensearch.common.settings.Setting<String> READER_SECRET_FILE =
            org.opensearch.common.settings.Setting.simpleString("binstore.reader.secret_file", "",
                    org.opensearch.common.settings.Setting.Property.NodeScope);
    public static final org.opensearch.common.settings.Setting<String> STORE_BUCKET =
            org.opensearch.common.settings.Setting.simpleString("binstore.store.bucket", "",
                    org.opensearch.common.settings.Setting.Property.NodeScope);
    public static final org.opensearch.common.settings.Setting<String> STORE_PREFIX =
            org.opensearch.common.settings.Setting.simpleString("binstore.store.prefix", "",
                    org.opensearch.common.settings.Setting.Property.NodeScope);
    public static final org.opensearch.common.settings.Setting<
            org.opensearch.common.unit.TimeValue> TIER_TWO_INTERVAL =
            org.opensearch.common.settings.Setting.timeSetting("binstore.fallback.poll.interval",
                    org.opensearch.common.unit.TimeValue.timeValueSeconds(5),
                    org.opensearch.common.unit.TimeValue.timeValueMillis(100),
                    org.opensearch.common.settings.Setting.Property.NodeScope);

    /** This node's shards of this plugin's indices, for the progress reporter. */
    private final ShardPositions positions;

    @Override
    public java.util.List<org.opensearch.common.settings.Setting<?>> getSettings() {
        return java.util.List.of(PROGRESS_INTERVAL, INGESTER_ENDPOINT, READER_ENDPOINT,
                READER_SECRET_FILE, STORE_BUCKET, STORE_PREFIX, TIER_TWO_INTERVAL);
    }

    /**
     * ⚠️ ONLY THIS PLUGIN's INDICES are watched: a node's other shards commit
     * no {@code batch_start} of ours.
     */
    @Override
    public void onIndexModule(org.opensearch.index.IndexModule module) {
        if (TYPE.equalsIgnoreCase(module.getSettings().get("index.ingestion_source.type"))) {
            module.addIndexEventListener(positions);
        }
    }

    /**
     * How a deployment says what a node's subscriptions ARE, keyed by the node
     * that asks (M6.13).
     *
     * <p>⚠️ A FACTORY RATHER THAN AN INSTANCE, and that is the whole of this
     * task. The field it replaces was one {@code NodeSubscriptions} per JVM,
     * which is correct in production -- a JVM is a node -- and WRONG wherever
     * two nodes share a process: {@code InternalTestCluster} runs every node of
     * a cluster in one, so "each node holds ONE subscription" reads as 1
     * however many nodes there are, and M6.9's multi-node criterion becomes
     * unmeasurable rather than merely unmeasured. Handing out instances in
     * INSTALL ORDER would pass every deterministic boot and hand node B node
     * A's subscription the moment two start at once.
     *
     * <p>⚠️ IT IS STILL STATIC, because a node constructs this plugin
     * REFLECTIVELY and nothing can be passed in. What changed is that the
     * static is now a factory and a map keyed by the node's own name, so the
     * STATE is per node even though the entry point cannot be.
     */
    private static volatile java.util.function.Function<String, NodeSubscriptions> factory;

    /** Deliveries one stream may queue before it drops and reports a gap. */
    static final int QUEUE_CAPACITY = 1024;

    /**
     * ⚠️ KEYED BY {@code node.name}, WHICH THE NODE SUPPLIES. OpenSearch puts
     * it in the settings it hands the plugin's constructor, so the key is
     * something the node says rather than something a fixture assumed -- and
     * {@code computeIfAbsent} means one node asking twice, as it does across a
     * restart, gets the same subscription rather than a second subscriber the
     * first one's deliveries still reach.
     */
    private static final java.util.Map<String, NodeSubscriptions> PER_NODE =
            new java.util.concurrent.ConcurrentHashMap<>();

    private final String nodeName;
    private final NodeSubscriptions subscriptions;

    /**
     * The only public constructor: the node calls this one reflectively.
     *
     * <p>WARNING: it takes {@code Settings} rather than nothing, and
     * {@code PluginsService} supports exactly this -- it looks for a single
     * public constructor taking {@code (Settings, Path)}, then {@code
     * (Settings)}, then none. A plugin with TWO public constructors is refused
     * outright ("no unique public constructor"), which is why the test-only one
     * below is package-private.
     *
     * <p>⚠️ AND THE SETTINGS ARE WHAT MAKE THE STATE PER NODE. With a no-arg
     * constructor the plugin has nothing to key by, and the only thing left is
     * call order -- see {@link #factory}.
     */
    public BinStorePlugin(org.opensearch.common.settings.Settings settings) {
        this.positions = new ShardPositions();
        java.util.function.Function<String, NodeSubscriptions> installed = factory;
        String nodeName = settings == null ? "" : settings.get("node.name", "");
        this.nodeName = nodeName;
        // ⚠️ A NODE WITH NO NAME GETS NOTHING rather than a shared default:
        // keying two unnamed nodes together is the static this task removes,
        // wearing a default's clothes. Every real node has one.
        String endpoint = settings == null ? "" : INGESTER_ENDPOINT.get(settings);
        if (installed == null && !endpoint.isEmpty()) {
            // ⚠️ AN INSTALLED FACTORY WINS: a test cluster installs one because
            // its nodes share a JVM and a transport, and a setting it also
            // carried must not replace it.
            installed = name -> NodeSubscriptions.fetching(
                    NodeChannel.open(endpoint,
                            io.github.huyz0.os.biningester.client.HttpSubscriptionTransport.DEFAULT_RETRY_FLOOR,
                            io.github.huyz0.os.biningester.client.HttpSubscriptionTransport.DEFAULT_RETRY_CEILING,
                            NodeSubscriptions.SUBSCRIPTION_CONNECT_TIMEOUT),
                    QUEUE_CAPACITY, NodeSubscriptions.DEFAULT_SEGMENT_HOLD_BYTES);
        }
        this.subscriptions = installed == null || nodeName.isEmpty()
                ? null
                : PER_NODE.computeIfAbsent(nodeName, installed);
        this.progressInterval = PROGRESS_INTERVAL.get(
                settings == null ? org.opensearch.common.settings.Settings.EMPTY : settings);
        var configured = settings == null ? org.opensearch.common.settings.Settings.EMPTY : settings;
        this.tierTwoInterval = TIER_TWO_INTERVAL.get(configured);
        if (this.subscriptions != null) {
            enableTierTwo(configured, this.subscriptions);
        }
    }

    /** {@link #PROGRESS_INTERVAL} as this node was configured with it. */
    private final org.opensearch.common.unit.TimeValue progressInterval;
    private final org.opensearch.common.unit.TimeValue tierTwoInterval;

    static void enableTierTwo(org.opensearch.common.settings.Settings settings,
            NodeSubscriptions subscriptions) {
        String endpoint = READER_ENDPOINT.get(settings);
        String secretFile = READER_SECRET_FILE.get(settings);
        String bucket = STORE_BUCKET.get(settings);
        String prefix = STORE_PREFIX.get(settings);
        boolean configured = !endpoint.isEmpty() || !secretFile.isEmpty()
                || !bucket.isEmpty() || !prefix.isEmpty();
        if (!configured) {
            return;
        }
        if (endpoint.isEmpty() || secretFile.isEmpty() || bucket.isEmpty() || prefix.isEmpty()) {
            throw new IllegalArgumentException("Tier 2 reader endpoint, secret file, bucket and prefix "
                    + "must all be configured together");
        }
        final io.github.huyz0.os.biningester.client.NodeLocalStoreReaderClient reader;
        try {
            reader = io.github.huyz0.os.biningester.client.NodeLocalStoreReaderClient.open(
                    endpoint, secretFile, java.time.Duration.ofSeconds(5), 64L << 20);
        } catch (java.io.IOException | IllegalArgumentException invalid) {
            throw new IllegalArgumentException("could not configure the node-local Tier 2 reader",
                    invalid);
        }
        subscriptions.enableTierTwo((epoch, sequence) -> {
            String key = String.format(java.util.Locale.ROOT,
                    "%s/ctl/log/0/%016x/%016x.delta", prefix, epoch, sequence);
            return decodeDelta(reader.getIfPresent(bucket, prefix, key));
        }, subscriptions::ingesterAnswers,
                subscriptions::offerTierTwoDelta, reader);
    }

    static java.util.Optional<io.github.huyz0.os.biningester.format.CommitDelta> decodeDelta(
            java.util.Optional<java.io.InputStream> result) throws java.io.IOException {
        if (result.isEmpty()) {
            return java.util.Optional.empty();
        }
        try (var body = result.orElseThrow()) {
            return java.util.Optional.of(io.github.huyz0.os.biningester.format.CommitDelta
                    .decode(body.readAllBytes()));
        }
    }

    private final java.util.concurrent.atomic.AtomicLong tierTwoIntervals =
            new java.util.concurrent.atomic.AtomicLong();

    /**
     * Used by in-process tests that own the lifetime themselves.
     *
     * <p>WARNING: PACKAGE-PRIVATE, not public. OpenSearch requires exactly ONE
     * public constructor and refuses to load a plugin otherwise -- "no unique
     * public constructor". A second public one leaves every in-process test
     * green while making the plugin unloadable by a real node; only booting one
     * finds it.
     */
    BinStorePlugin(NodeSubscriptions subscriptions) {
        this(subscriptions, new ShardPositions());
    }

    /** Deterministic in-package construction for tests of the node entrypoint wiring. */
    BinStorePlugin(NodeSubscriptions subscriptions, ShardPositions positions) {
        this.nodeName = null;
        this.subscriptions = subscriptions;
        this.positions = java.util.Objects.requireNonNull(positions, "positions");
        this.progressInterval = PROGRESS_INTERVAL.getDefault(
                org.opensearch.common.settings.Settings.EMPTY);
        this.tierTwoInterval = TIER_TWO_INTERVAL.getDefault(
                org.opensearch.common.settings.Settings.EMPTY);
    }

    /** What this node's plugin instance holds, or null where none was installed. */
    NodeSubscriptions subscriptions() {
        return subscriptions;
    }

    /**
     * Installs the factory a reflectively-constructed plugin will ask for its
     * node's subscriptions.
     *
     * <p>⚠️ CALLED ONCE PER DEPLOYMENT, BEFORE ITS NODES START, and asked once
     * per NODE. In production a JVM is one node and the distinction costs
     * nothing; in a test cluster it is the difference between measuring
     * per-node behaviour and measuring one shared object eight times.
     *
     * <p>⚠️ AND IT FORGETS THE PREVIOUS INSTALLATION'S NODES, because "once per
     * process" is exactly what a test JVM does NOT do: eight IT classes install
     * from static initialisers in one JVM, nothing forks per class, and
     * {@code OpenSearchSingleNodeTestCase} reuses the node name -- so without
     * the clear the second class's node is handed the first class's transport.
     * MEASURED: deleting it leaves {@code :plugin:test} green and fails
     * {@code :plugin:clusterTest}.
     */
    public static void install(java.util.function.Function<String, NodeSubscriptions> factory) {
        BinStorePlugin.factory = factory;
        PER_NODE.clear();
    }

    /**
     * Forgets the factory and every node's state -- for a test that owns both.
     *
     * <p>⚠️ PACKAGE-PRIVATE, like the test-only constructor and for the same
     * reason: nothing outside {@code io.github.huyz0.os.biningester.plugin} has any business tearing a
     * running node's subscriptions out from under it, and both test source sets
     * are in this package.
     */
    static void uninstall() {
        factory = null;
        PER_NODE.clear();
    }

    /** Releases this node's shared subscriptions when OpenSearch closes the plugin. */
    @Override
    public void close() {
        if (subscriptions == null || nodeName == null || nodeName.isEmpty()
                || !PER_NODE.remove(nodeName, subscriptions)) {
            return;
        }
        subscriptions.close();
        NodeChannel channel = subscriptions.channel();
        if (channel != null) {
            channel.close();
        }
    }

    /**
     * WARNING: the raw {@code IngestionConsumerFactory} is UPSTREAM's signature,
     * not a slip here -- {@code IngestionConsumerPlugin} declares
     * {@code Map<String, IngestionConsumerFactory>} with no type arguments, and
     * parameterising it would fail to override. Suppressed on this one method
     * rather than relaxing -Werror for the module.
     */
    @SuppressWarnings("rawtypes")
    @Override
    public Map<String, IngestionConsumerFactory> getIngestionConsumerFactories() {
        return Map.of(TYPE, new BinStoreConsumerFactory(subscriptions));
    }

    /**
     * Starts the registration push for this node (M6.7, FR-16).
     *
     * <p>⚠️ HERE AND NOT IN {@code createShardConsumer}, which runs once per
     * SHARD: a listener built there pushes one copy of an index's shape per
     * shard of it this node holds, and every other M6 criterion stays green
     * while it does. {@code RegistrationPushTest} counts at K = 1 and K = 8.
     *
     * <p>⚠️ NOTHING IS RETURNED TO THE NODE. The registrar has no lifecycle of
     * its own -- it holds no clock, no socket and no thread, and pushes only
     * when the cluster tells it something changed -- so registering it as a
     * listener is the whole of its installation.
     *
     * <p>⚠️ IT IS HANDED NO NODE ID. {@code clusterService.localNode()} is not
     * answerable here -- this runs while the node is being built, before it has
     * joined anything -- and every cluster-state event carries the local node's
     * id anyway.
     *
     * <p>⚠️ THE PUSHES RUN ON THE GENERIC POOL, NOT ON THE APPLIER THREAD that
     * delivers the event. They are network calls with up to three attempts
     * each, and the applier thread applies every cluster-state update on this
     * node -- allocation, mappings, the ack to the cluster manager.
     */
    @Override
    public java.util.Collection<Object> createComponents(
            org.opensearch.transport.client.Client client,
            org.opensearch.cluster.service.ClusterService clusterService,
            org.opensearch.threadpool.ThreadPool threadPool,
            org.opensearch.watcher.ResourceWatcherService resourceWatcherService,
            org.opensearch.script.ScriptService scriptService,
            org.opensearch.core.xcontent.NamedXContentRegistry xContentRegistry,
            org.opensearch.env.Environment environment,
            org.opensearch.env.NodeEnvironment nodeEnvironment,
            org.opensearch.core.common.io.stream.NamedWriteableRegistry namedWriteableRegistry,
            org.opensearch.cluster.metadata.IndexNameExpressionResolver indexNameExpressionResolver,
            java.util.function.Supplier<org.opensearch.repositories.RepositoriesService> repositories) {
        installRegistrar(subscriptions, clusterService::addListener,
                threadPool.generic());
        if (subscriptions != null) {
            // ⚠️ ON THE GENERIC POOL: a report is a network call, and
            // `ProgressReporter.report` contains its own failures, so a bad
            // interval cannot cancel the schedule (M8.43).
            ProgressReporter reporter = new ProgressReporter(subscriptions.transport(),
                    positions);
            threadPool.scheduleWithFixedDelay(reporter::report, progressInterval,
                    org.opensearch.threadpool.ThreadPool.Names.GENERIC);
            NodeCatchUpCoordinator catchUp = new NodeCatchUpCoordinator(
                    subscriptions.transport(), subscriptions,
                    () -> catchUpSnapshot(clusterService, positions));
            threadPool.scheduleWithFixedDelay(catchUp::attempt, progressInterval,
                    org.opensearch.threadpool.ThreadPool.Names.GENERIC);
            threadPool.scheduleWithFixedDelay(
                    () -> subscriptions.pollTierTwo(tierTwoIntervals.incrementAndGet()),
                    tierTwoInterval, org.opensearch.threadpool.ThreadPool.Names.GENERIC);
            threadPool.generic().execute(catchUp::attempt);
        }
        return java.util.List.of();
    }

    private static java.util.Optional<java.util.List<
            io.github.huyz0.os.biningester.format.CatchUpRequestFrame.Stream>> catchUpSnapshot(
                    org.opensearch.cluster.service.ClusterService clusterService,
                    ShardPositions positions) {
        final org.opensearch.cluster.ClusterState state;
        try {
            state = clusterService.state();
        } catch (AssertionError notInitialized) {
            // The eager first attempt can race OpenSearch's initial state publication.
            return java.util.Optional.empty();
        }
        return positions.catchUpSnapshot(state);
    }

    /**
     * Registers this node's {@link IndexRegistrar}, or does nothing when this
     * deployment installed no subscriptions.
     *
     * <p>⚠️ PACKAGE-PRIVATE AND TAKING THE SINK RATHER THAN THE
     * {@code ClusterService}, so a T0 case can assert that a listener really is
     * added. Review MEASURED the alternative: deleting the body of
     * {@code createComponents} left `:plugin:test` green, because every case
     * built an {@code IndexRegistrar} by hand and nothing named the wiring --
     * so the whole of FR-16 could have shipped unregistered with the suite
     * green.
     *
     * <p>⚠️ A NODE WITH NO INSTALLED SUBSCRIPTIONS PUSHES NOTHING, and that is
     * the deployment that has not configured this plugin. Building a registrar
     * with no transport would refuse on the first cluster-state change of every
     * such node.
     */
    static void installRegistrar(NodeSubscriptions subscriptions,
            java.util.function.Consumer<org.opensearch.cluster.ClusterStateListener> sink,
            java.util.concurrent.Executor pusher) {
        if (subscriptions == null) {
            return;
        }
        // ⚠️ THE CHANNEL'S REGISTRAR WHERE THERE IS ONE, and a bare one only
        // where the deployment handed a bare transport. Review MEASURED the
        // difference: a registrar built here from `transport()` is reached by
        // no reconnect, so the node never re-pushes after the ingester
        // restarts -- the whole of M6.15, surviving a class that wires the
        // cycle but that nothing on this path used.
        NodeChannel channel = subscriptions.channel();
        if (channel == null) {
            // ⚠️ NAMED, because it is M6.15's failure with nothing else naming it
            // (M8.35): correct for a bare transport, and a node that never
            // re-pushes after the ingester restarts.
            LogManager.getLogger(BinStorePlugin.class).info(
                    "index registrations will not be re-pushed after an ingester restart: "
                            + "this node's subscriptions are a bare transport, which no "
                            + "reconnect reaches; install them through a NodeChannel");
        }
        sink.accept(channel != null ? channel.registrar(pusher)
                : new IndexRegistrar(subscriptions.transport(), pusher));
    }

    /**
     * Refuses an index configured for a mapper shape this plugin's records are
     * not in, AT CREATION (M6.8, FR-7).
     *
     * <p>⚠️ THE FACTORY'S OWN REFUSAL IS NOT ENOUGH, and a booting node is what
     * showed it. {@code DefaultStreamPoller} CATCHES whatever
     * {@code createShardConsumer} throws, logs "Failed to create consumer for
     * shard 0" at WARN, and retries -- forever. MEASURED on this tree before
     * this validator existed: the index went GREEN, the shard reported started,
     * the WARN repeated every poll, and nothing was ever indexed. That is
     * ADR-0020's "zero documents indexed and nothing in the log" arriving one
     * level up from where ADR-0020 found it.
     *
     * <p>⚠️ SO THE REFUSAL IS MOVED TO WHERE A HUMAN IS LOOKING: the create
     * request itself fails, with the setting and the value in the response.
     * The factory keeps its own check -- an index can be created before this
     * plugin is installed, or restored from a snapshot into a cluster that has
     * it -- but the one an operator meets is this one. (Not a settings update:
     * `mapper_type` is `Property.Final`.)
     */
    @Override
    public java.util.Collection<org.opensearch.index.IndexCreationValidator>
            getIndexCreationValidators() {
        return java.util.List.of((mapperService, indexSettings) ->
                BinStoreConsumerFactory.refuseUnservedMapper(indexSettings.getIndexMetadata()));
    }

    @Override
    public String getType() {
        return TYPE;
    }
}
