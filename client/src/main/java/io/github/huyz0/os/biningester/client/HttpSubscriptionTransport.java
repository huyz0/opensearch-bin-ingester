// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import io.github.huyz0.os.biningester.format.CatchUpRequestFrame;
import io.github.huyz0.os.biningester.format.ConsumerProgress;
import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SubscriptionEvent;
import io.helidon.http.Status;
import io.helidon.webclient.api.HttpClientResponse;
import io.helidon.webclient.api.WebClient;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * The consumer's half of the subscription channel (M8.21, M5.6e, FR-16, FR-9).
 *
 * <p>⚠️ **THIS IS THE SEAM M6.15 HAS BEEN WAITING ON.**
 * {@code IndexRegistrar.onReconnect()} exists, is tested, and is called by
 * nothing, because no production transport existed to have a reconnect. A
 * registration is state the INGESTER holds in memory, so when it restarts it
 * knows no index's shape and every routed write to this node's indices is
 * refused when its wait expires — with nothing naming the cause. The reconnect
 * callback is what recovers that, and it fires here.
 *
 * <p>⚠️ **ONE VIRTUAL THREAD PER SUBSCRIPTION, LONG-POLLING FOR EVER.** A
 * dropped connection is ordinary — a rolling deploy drops every one of them —
 * so the loop backs off and comes back rather than reporting an error nobody
 * can act on. What it must never do is stay silent: the first answer after a
 * failure invokes {@code onReconnect}, because the ingester that answers it may
 * be a different process with an empty catalog.
 *
 * <p>⚠️ **A POLL, NOT AN OPEN STREAM**, for the reason
 * {@code SubscriptionService} records with its measurement: Helidon's client
 * stream does not block across a chunk boundary, so a held-open response
 * delivered heartbeats and then died on the first real frame.
 *
 * <p>⚠️ **BACKOFF IS BOUNDED AND JITTERED.** Every consumer on every node
 * reconnects at the same instant after a deployment, and an unjittered retry
 * turns that into a synchronised storm against a node that has just started —
 * research 08 §7 step 2's thundering herd, arriving from the other direction.
 */
public final class HttpSubscriptionTransport implements SubscriptionTransport, AutoCloseable {

    /**
     * ⚠️ **THE PATHS LIVE HERE AND THE INGESTER READS THEM**, not the other way
     * round: `client` may not depend on `http` (architecture.md — `http` is the
     * ingester's adapter and this is the consumer), and two copies of a path
     * are two things to get wrong.
     */
    public static final String SUBSCRIBE_PREFIX = "/sub/";

    /** {@code POST} target for an index's shape (ADR-0047). */
    public static final String REGISTER_PATH = "/ctl/register";

    /**
     * Set to {@code 1} on a poll to be sent the stream's retained floor
     * (ADR-0056).
     *
     * <p>⚠️ **THIS TRANSPORT ASKS WHEN ITS LISTENER WANTS ONE**, which is
     * while a resume waits on a fresh floor, for a bounded number of polls.
     * An ingester that predates ADR-0056 ignores the parameter and sends none;
     * the asks run out and the floor stays unknown -- the behaviour before it
     * existed.
     */
    public static final String FLOOR_PARAM = "floor";

    /**
     * Set on a poll to the zone this consumer runs in (M9.2, NFR-5).
     *
     * <p>⚠️ **IT IS WHAT MAKES A CROSS-AZ SERVE COUNTABLE AT ALL.** The
     * ingester holds an address for this node and no zone, so without this
     * parameter every byte it serves is unattributed -- counted against NFR-5
     * as cross-AZ, which is the safe side and a number nobody can act on.
     * ⚠️ An ingester that predates the parameter ignores it, exactly as one
     * that predates {@link #FLOOR_PARAM} ignores that.
     */
    public static final String AZ_PARAM = "az";

    /** {@code POST} target for a node's consumer progress (ADR-0049). */
    public static final String PROGRESS_PATH = "/ctl/progress";

    private final WebClient client;
    private final HttpCatchUpExchange catchUpExchange;
    private final String az;
    private final Runnable onReconnect;
    private final Duration retryFloor;
    private final Duration retryCeiling;
    private final Duration pollWait;
    /**
     * ⚠️ 25 s, under the ingester's own 30 s ceiling so IT answers
     * first: a client whose read timeout expires first tears down a connection
     * the server was about to write to, and the push it was about to receive
     * waits for the next poll.
     */
    public static final Duration DEFAULT_POLL_WAIT = Duration.ofSeconds(25);

    /**
     * The first retry's backoff, for a deployment that sets none (M8.16).
     *
     * <p>⚠️ **ITS JOB IS TO SPREAD A HERD, AND ONE SECOND IS WHAT SPREADS IT.** A
     * draining ingester answers every waiting poll at once (research 08 §7 step
     * 2), so every consumer on it retries at the same instant, each after
     * {@link #jitteredMillis}: anywhere in [0.5, 1.5] times this. One second
     * spreads the reconnects over a full second -- ten 100 ms windows, and
     * criterion 13 allows no window more than 20% of them. 100 ms would put them
     * all in one or two.
     */
    public static final Duration DEFAULT_RETRY_FLOOR = Duration.ofSeconds(1);

    /**
     * How far the backoff may grow, for a deployment that sets none: long
     * enough not to hammer an ingester that is down, short enough that one
     * that came back is found within half a minute.
     */
    public static final Duration DEFAULT_RETRY_CEILING = Duration.ofSeconds(30);

    /**
     * ⚠️ The ingester's own floor: the first poll of a connection asks for this
     * so that "the ingester answered" is known in milliseconds rather than in
     * half a minute.
     */
    static final Duration HANDSHAKE_WAIT = Duration.ofMillis(50);

    /**
     * ⚠️ 32 MiB. One answer drains everything queued, so it is legitimately
     * several segments; anything beyond this is not an answer this deployment
     * produces, and buffering it would be a peer choosing this node's heap.
     */
    public static final int MAX_ANSWER_BYTES = 32 << 20;

    /**
     * ⚠️ **INJECTABLE SO THE REFUSAL CAN BE ASSERTED WITHOUT A 32 MiB FIXTURE.**
     * A case that has to produce the real cap asserts that the number was
     * COMPARED; one that lowers the cap asserts that the answer was refused
     * BEFORE it was buffered, which is the property that matters.
     */
    private final int maxAnswerBytes;

    private final SubscriptionMetrics metrics = new SubscriptionMetrics();

    /**
     * ⚠️ **THE LADDER, EXECUTED, PER SUBSCRIPTION** (M8.28, ADR-0057).
     * {@link FallbackLadder} shipped in M5 as a policy nothing called; each
     * subscription's reader now asks it for its tier at every transition it
     * observes -- a first answer, a lost one -- and counts what it entered.
     *
     * <p>⚠️ **ONE PER READER, NOT ONE PER TRANSPORT**: a node shares one
     * transport across every subscription it holds (review MEASURED it), so a
     * transport-wide field let one subscription's reconnect report the node as
     * pushing while another was still down, and counted one outage twice. A
     * reader is one thread, so its ladder needs no lock.
     */
    private static final class Ladder {
        private volatile FallbackLadder.AutomaticTier tier =
                FallbackLadder.tierFor(FallbackLadder.Health.CONNECTION_LOST);
    }

    private final java.util.Map<String, Ladder> ladders =
            new java.util.concurrent.ConcurrentHashMap<>();

    private static SubscriptionMetrics.Counter tierCounter(FallbackLadder.AutomaticTier tier) {
        return switch (tier) {
            case PUSH -> SubscriptionMetrics.Counter.FALLBACK_TIER_PUSH_ENTRIES;
            case RECONNECT -> SubscriptionMetrics.Counter.FALLBACK_TIER_RECONNECT_ENTRIES;
            case POLL_CHAIN -> SubscriptionMetrics.Counter.FALLBACK_TIER_POLL_CHAIN_ENTRIES;
            case RECOVER -> SubscriptionMetrics.Counter.FALLBACK_TIER_RECOVER_ENTRIES;
        };
    }

    /**
     * The WORST tier any live subscription is in -- so a node reads as pushing
     * only when every stream it holds is -- or RECONNECT where none has
     * answered yet.
     */
    public FallbackLadder.AutomaticTier tier() {
        FallbackLadder.AutomaticTier worst = null;
        for (Ladder ladder : ladders.values()) {
            FallbackLadder.AutomaticTier each = ladder.tier;
            if (worst == null || each.ordinal() > worst.ordinal()) {
                worst = each;
            }
        }
        return worst != null ? worst
                : FallbackLadder.tierFor(FallbackLadder.Health.CONNECTION_LOST);
    }

    /**
     * How many times any subscription on this transport has ENTERED {@code tier}.
     *
     * <p>⚠️ **ENTRIES, NOT POLLS**: a stream down for a minute is one
     * RECONNECT, whatever its backoff retried, so the count says how often a
     * consumer fell down the ladder rather than how long it stayed.
     */
    public long tierEntries(FallbackLadder.AutomaticTier tier) {
        return metrics.count(tierCounter(tier));
    }

    private void enter(Ladder ladder, FallbackLadder.Health health) {
        FallbackLadder.AutomaticTier next = FallbackLadder.tierFor(health);
        if (next != ladder.tier) {
            ladder.tier = next;
            metrics.increment(tierCounter(next));
        }
    }


    /**
     * Why a poll did not deliver, one class per outcome (M8.37).
     *
     * <p>⚠️ **CLASSES, NEVER A STATUS OR AN INDEX**: observability.md rule 1
     * keeps labels to a closed set, and a consumer wedged against a full or a
     * misconfigured ingester only has to be told apart from an idle one.
     */
    public enum PollFailure {
        /** 503: the ingester is draining or at its session cap. */
        UNAVAILABLE,
        /** Any 4xx, a missing route included: this consumer asked for something wrong. */
        REFUSED,
        /** Any other status that is not 200. */
        SERVER_ERROR,
        /** No answer at all: refused, reset or timed out. */
        UNREACHABLE,
        /** A 200 whose body could not be read or decoded. */
        MALFORMED,
        /** A local callback threw while handling an answered poll. */
        CALLBACK
    }

    private static SubscriptionMetrics.Counter failureCounter(PollFailure kind) {
        return switch (kind) {
            case UNAVAILABLE -> SubscriptionMetrics.Counter.POLL_FAILURE_UNAVAILABLE;
            case REFUSED -> SubscriptionMetrics.Counter.POLL_FAILURE_REFUSED;
            case SERVER_ERROR -> SubscriptionMetrics.Counter.POLL_FAILURE_SERVER_ERROR;
            case UNREACHABLE -> SubscriptionMetrics.Counter.POLL_FAILURE_UNREACHABLE;
            case MALFORMED -> SubscriptionMetrics.Counter.POLL_FAILURE_MALFORMED;
            case CALLBACK -> SubscriptionMetrics.Counter.POLL_FAILURE_CALLBACK;
        };
    }

    /** The in-memory counter bank exposed to the node-level metrics adapter. */
    public SubscriptionMetrics metrics() {
        return metrics;
    }

    /** An answer that was not 200, already counted by its class. */
    private static final class NotOk extends IOException {
        NotOk(String message) {
            super(message);
        }
    }

    @Override
    public boolean ingesterAnswers() {
        return tier() == FallbackLadder.AutomaticTier.PUSH;
    }

    /** Keeps a consumer callback failure distinct from malformed wire data. */
    private static final class CallbackFailure extends RuntimeException {
        CallbackFailure(RuntimeException cause) {
            super("a subscription callback failed", cause);
        }
    }
    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * @param endpoint the ingester this node subscribes to
     * @param onReconnect run after every re-establishment, including the first.
     *     ⚠️ **INCLUDING THE FIRST**, because the ingester may have been
     *     restarted before this node ever connected — there is no first
     *     connection that can be assumed to find a populated catalog.
     */
    public HttpSubscriptionTransport(String endpoint, Runnable onReconnect,
            Duration retryFloor, Duration retryCeiling, Duration timeout) {
        this(endpoint, onReconnect, retryFloor, retryCeiling, timeout, DEFAULT_POLL_WAIT);
    }

    /**
     * The same, with the poll wait given.
     *
     * <p>⚠️ **INJECTABLE SO IT CAN BE ASSERTED**: at the 25 s default a test
     * sees one poll, so "this fires once per CONNECTION and not once per POLL"
     * is unobservable — and review measured exactly that mutation surviving.
     */
    public HttpSubscriptionTransport(String endpoint, Runnable onReconnect,
            Duration retryFloor, Duration retryCeiling, Duration timeout, Duration pollWait) {
        this(endpoint, onReconnect, retryFloor, retryCeiling, timeout, pollWait, MAX_ANSWER_BYTES);
    }

    /**
     * The same, with the answer cap given.
     *
     * <p>⚠️ **INJECTABLE FOR THE REFUSAL CASE.** With the cap fixed at 32 MiB a
     * case can only assert that a 32 MiB answer is refused — which it would be
     * by a check made AFTER buffering, the defect this parameter exists to let a
     * test tell apart. Lowering it makes "refused before it was buffered"
     * observable in a body a test can produce.
     */
    public HttpSubscriptionTransport(String endpoint, Runnable onReconnect,
            Duration retryFloor, Duration retryCeiling, Duration timeout, Duration pollWait,
            int maxAnswerBytes) {
        this(endpoint, onReconnect, retryFloor, retryCeiling, timeout, pollWait, maxAnswerBytes,
                "");
    }

    /**
     * The same, declaring the zone this consumer runs in (M9.2, NFR-5).
     *
     * <p>⚠️ **EVERY OTHER CONSTRUCTOR DECLARES NONE**, and an undeclared zone
     * is counted by the ingester as cross-AZ rather than dropped -- see
     * {@link #AZ_PARAM}. A blank is sent as nothing at all, so a consumer
     * cannot accidentally claim the zone named by an empty variable.
     */
    public HttpSubscriptionTransport(String endpoint, Runnable onReconnect,
            Duration retryFloor, Duration retryCeiling, Duration timeout, Duration pollWait,
            int maxAnswerBytes, String az) {
        this.az = Objects.requireNonNull(az, "az").trim();
        Objects.requireNonNull(endpoint, "endpoint");
        this.onReconnect = Objects.requireNonNull(onReconnect, "onReconnect");
        this.retryFloor = Objects.requireNonNull(retryFloor, "retryFloor");
        this.retryCeiling = Objects.requireNonNull(retryCeiling, "retryCeiling");
        this.pollWait = Objects.requireNonNull(pollWait, "pollWait");
        if (maxAnswerBytes <= 0) {
            throw new IllegalArgumentException("the answer cap must be positive: " + maxAnswerBytes);
        }
        this.maxAnswerBytes = maxAnswerBytes;
        Objects.requireNonNull(timeout, "timeout");
        if (retryFloor.isNegative() || retryFloor.isZero() || retryCeiling.compareTo(retryFloor) < 0) {
            throw new IllegalArgumentException("retry floor must be positive and at or below the "
                    + "ceiling: " + retryFloor + " / " + retryCeiling);
        }
        this.catchUpExchange = new HttpCatchUpExchange(endpoint, timeout, pollWait);
        this.client = WebClient.builder()
                .baseUri(endpoint)
                .connectTimeout(timeout)
                // ⚠️ THE READ TIMEOUT MUST EXCEED THE POLL WAIT, or every idle
                // subscription tears itself down on a schedule and every
                // consumer on the node reconnects in a herd. The ingester
                // answers within its own 30 s ceiling; this leaves headroom
                // over that rather than over the 25 s asked for.
                .readTimeout(pollWait.plusSeconds(20))
                .build();
    }

    /**
     * The values {@link #AZ_PARAM} is sent with: none at all when this
     * consumer was told no zone.
     *
     * <p>⚠️ **EXTRACTED SO THE CHOICE IS ASSERTABLE WITHOUT A SOCKET**, the
     * same reason {@code SubscriptionService.waitFor(String)} is. Sending
     * {@code az=} instead of nothing tells the ingester a zone whose NAME is
     * the empty string; it compares that against its own and counts a
     * mismatch, which reads in a cost report as a consumer in another zone
     * rather than as one that never said.
     */
    static String[] azParam(String az) {
        return az == null || az.isBlank() ? new String[0] : new String[] {az.trim()};
    }

    @Override
    public AutoCloseable subscribe(RunKey key, Listener listener) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(listener, "listener");
        AtomicBoolean stopped = new AtomicBoolean();
        // ⚠️ ONE ID PER SUBSCRIPTION, STABLE ACROSS POLLS AND ACROSS
        // RECONNECTS. It is what lets the ingester keep this consumer's queue
        // between polls; without it a push published in the gap between two
        // polls reaches nobody, with no gap and no error -- measured.
        String id = java.util.UUID.randomUUID().toString();
        Thread reader = Thread.ofVirtual()
                .name("subscription-" + key.indexId() + "-" + key.partitionId())
                .start(() -> readForever(key, id, listener, stopped));
        return () -> {
            stopped.set(true);
            reader.interrupt();
        };
    }

    @Override
    public CatchUpResult requestCatchUp(CatchUpRequestFrame request,
            Consumer<SubscriptionEvent> lane) throws IOException {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(lane, "lane");
        if (closed.get()) {
            throw new IllegalStateException("transport is closed");
        }
        return catchUpExchange.request(request, lane);
    }

    private void readForever(RunKey key, String id, Listener listener, AtomicBoolean stopped) {
        Ladder ladder = new Ladder();
        ladders.put(id, ladder);
        try {
            readForever(key, id, listener, stopped, ladder);
        } finally {
            ladders.remove(id);
        }
    }

    private void readForever(RunKey key, String id, Listener listener, AtomicBoolean stopped,
            Ladder ladder) {

        Duration backoff = retryFloor;
        boolean connected = false;
        while (!stopped.get() && !closed.get()) {
            boolean answered = false;
            // ⚠️ THE FIRST POLL AFTER A FAILURE ASKS FOR ALMOST NO WAIT, and
            // that is a handshake rather than an optimisation: with the long
            // wait, a consumer cannot tell "the ingester is there and quiet"
            // from "the ingester is gone" for thirty seconds -- and neither can
            // `onReconnect`, which must fire before the node's registrations
            // are missed for that long.
            Duration wait = connected ? pollWait : HANDSHAKE_WAIT;
            try (HttpClientResponse response = client.get(path(key))
                    .queryParam("wait", String.valueOf(wait.toMillis()))
                    .queryParam("sub", id)
                    // ⚠️ ASKED PER POLL OF THE LISTENER, which asks only while
                    // a resume waits on a fresh floor (ADR-0056).
                    .queryParam(FLOOR_PARAM, wantsFloor(listener, key) ? "1" : "0")
                    // ⚠️ SENT ONLY WHEN KNOWN. An empty `az=` would tell the
                    // ingester a zone whose name is the empty string, which it
                    // would then compare against its own and count as a
                    // MISMATCH -- the same answer as absent, arrived at by a
                    // claim rather than by its absence.
                    .queryParam(AZ_PARAM, azParam(az))
                    .request()) {
                int code = response.status().code();
                if (code != Status.OK_200.code()) {
                    metrics.increment(failureCounter(code == Status.SERVICE_UNAVAILABLE_503.code()
                            ? PollFailure.UNAVAILABLE
                            : code >= 400 && code < 500 ? PollFailure.REFUSED
                            : PollFailure.SERVER_ERROR));
                    throw new NotOk("subscribe answered " + response.status());
                }
                answered = true;
                if (!connected) {
                    connected = true;
                    metrics.increment(SubscriptionMetrics.Counter.SUBSCRIPTION_RECONNECTS);
                    enter(ladder, FallbackLadder.Health.PUSHING);
                    // ⚠️ AFTER THE FIRST ANSWER, NOT BEFORE IT: a registration
                    // pushed at a node that has not answered yet is racing the
                    // same window the registrar's own retry covers. ⚠️ AND ONCE
                    // PER CONNECTION, NOT PER POLL -- a poll answers every 30 s
                    // on an idle stream, and re-registering every index on the
                    // node that often is a message storm for no new fact.
                    try {
                        onReconnect.run();
                    } catch (RuntimeException callback) {
                        throw new CallbackFailure(callback);
                    }
                }
                backoff = retryFloor;
                deliver(response, listener, stopped);
            } catch (IOException | RuntimeException dropped) {
                if (!(dropped instanceof NotOk) && !stopped.get() && !closed.get()) {
                    metrics.increment(failureCounter(dropped instanceof CallbackFailure
                            ? PollFailure.CALLBACK
                            : answered ? PollFailure.MALFORMED : PollFailure.UNREACHABLE));
                }

                // ⚠️ ORDINARY. A rolling deploy drops every subscription on the
                // node; the answer is to come back, counted above, not logged. ⚠️ AND THE
                // NEXT SUCCESS COUNTS AS A RECONNECT, because the ingester that
                // answers it may be a different process with an empty catalog
                // (M6.15) -- which is the whole reason `onReconnect` exists.
                connected = false;
                if (stopped.get() || closed.get()) {
                    return;
                }
                enter(ladder, FallbackLadder.Health.CONNECTION_LOST);
                backoff = sleepAndGrow(backoff);
            }
        }
    }

    private void deliver(HttpClientResponse response, Listener listener, AtomicBoolean stopped)
            throws IOException {
        // ⚠️ THROUGH THE STREAM, NOT `entity().as(byte[])`: an EMPTY 200 --
        // which is every quiet poll, the common case -- makes `entity()` throw
        // `IllegalStateException: No entity`, and the reader loop would treat
        // an idle stream as a failure and reconnect for ever. MEASURED.
        // ⚠️ BOUNDED WHILE READING, NOT AFTER. An earlier draft called
        // `readAllBytes()` and compared the length afterwards, which is a cap
        // that never runs: the allocation is what fails first, with an
        // `OutOfMemoryError` that is an `Error` and so escapes this loop's
        // `catch` -- the subscription's thread would die silently, in the
        // OpenSearch node process, with no delivery and no reconnect.
        byte[] body;
        try (var in = response.inputStream()) {
            body = StreamFraming.readBounded(in, maxAnswerBytes);
        }
        var in = new java.io.ByteArrayInputStream(body);
        byte[] frame;
        while ((frame = StreamFraming.readFrame(in)) != null) {
            if (stopped.get() || closed.get()) {
                return;
            }
            // ⚠️ DISPATCHED BY MAGIC, NOT BY TRYING ONE DECODER AND CATCHING
            // THE OTHER's REFUSAL: the answer is mixed since ADR-0056, and a
            // torn EVENT read as "not a floor, move on" would drop records.
            if (io.github.huyz0.os.biningester.format.RetainedFloor.isRetainedFloor(frame)) {
                io.github.huyz0.os.biningester.format.RetainedFloor floor = io.github.huyz0.os.biningester.format.RetainedFloor.decode(frame);
                try {
                    listener.onRetainedFloor(floor.key(), floor.oldestRetainedOffset());
                } catch (RuntimeException callback) {
                    throw new CallbackFailure(callback);
                }
                continue;
            }
            try {
                listener.onDelivery(deliveryFor(SubscriptionEvent.decode(frame)));
            } catch (RuntimeException callback) {
                throw new CallbackFailure(callback);
            }
        }
    }

    private static boolean wantsFloor(Listener listener, RunKey key) {
        try {
            return listener.wantsRetainedFloor(key);
        } catch (RuntimeException callback) {
            throw new CallbackFailure(callback);
        }
    }

    /** What one event looks like to a consumer. */
    static Delivery deliveryFor(SubscriptionEvent event) {
        return new Delivery(event.key(), event.segmentKey(), event.recordCount(),
                event.firstOffset(), event.via(), event.inline(), event.grant(),
                event.sequencerEpoch(), event.chainSequence());
    }

    private Duration sleepAndGrow(Duration backoff) {
        try {
            Thread.sleep(jitteredMillis(backoff));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return backoff;
        }
        return grow(backoff, retryCeiling);
    }

    /**
     * How long one retry waits.
     *
     * <p>⚠️ **JITTERED, AND THE JITTER IS THE POINT RATHER THAN THE DELAY**:
     * every consumer on every node reconnects at the same instant after a
     * deployment, and an unjittered retry turns that into a synchronised storm
     * against a node that has just started (research 08 §7 step 2's thundering
     * herd, arriving from the other direction). ⚠️ **PURE AND VISIBLE SO IT CAN
     * BE ASSERTED**: with the sleep inline, review measured that removing every
     * one of jitter, growth and ceiling left all twenty socket cases green.
     *
     * <p>⚠️ The spread is HALF the backoff either side of it, and the RESULT is
     * floored at 1 ms — a retry that rounds to zero is a hot loop, and a
     * sub-millisecond backoff rounds to zero for every draw.
     */
    static long jitteredMillis(Duration backoff) {
        long millis = backoff.toMillis();
        return Math.max(1, millis / 2 + (long) (Math.random() * millis));
    }

    /**
     * The next backoff: doubled, and never past the ceiling.
     *
     * <p>⚠️ **BOUNDED.** Unbounded doubling reaches hours, and a consumer whose
     * ingester came back an hour ago is a delivery gap nobody can see.
     */
    static Duration grow(Duration backoff, Duration ceiling) {
        Duration grown = backoff.multipliedBy(2);
        return grown.compareTo(ceiling) > 0 ? ceiling : grown;
    }

    private static String path(RunKey key) {
        return SUBSCRIBE_PREFIX + key.indexId() + "/" + key.partitionId();
    }

    @Override
    public void register(IndexRegistration registration) {
        post(REGISTER_PATH, registration.encode(),
                "registration for " + registration.indexName());
    }

    @Override
    public void report(ConsumerProgress progress) {
        post(PROGRESS_PATH, progress.encode(),
                "progress for " + progress.entries().size() + " copies");
    }

    /**
     * ⚠️ **THROWS RATHER THAN SWALLOWING, BECAUSE BOTH CALLERS RETRY.**
     * {@code IndexRegistrar} attempts each push three times and carries a
     * failure to the next cluster-state change; {@code ProgressReporter} counts
     * a failure and lets the next interval carry more. A transport that
     * accepted silently would leave the ingester never learning any index's
     * shape while the deployment looked healthy.
     */
    private void post(String path, byte[] body, String what) {
        if (closed.get()) {
            throw new IllegalStateException("transport is closed, so " + what + " went nowhere");
        }
        try (HttpClientResponse response = client.post(path).submit(body)) {
            if (response.status().code() != Status.NO_CONTENT_204.code()) {
                throw new IllegalStateException(what + " was refused: " + response.status());
            }
        }
    }

    /**
     * How many times a stream has been established, first connection included.
     *
     * <p>⚠️ **PUBLIC BECAUSE THE CASE THAT NEEDS IT IS IN ANOTHER MODULE** —
     * the channel's own tests live beside the ingester's half in `http`, since a
     * subscription needs both ends. It is the only handle on "the stream is
     * open" that does not require guessing with a sleep.
     */
    public long reconnects() {
        return metrics.count(SubscriptionMetrics.Counter.SUBSCRIPTION_RECONNECTS);
    }

    /** How many polls failed with {@code kind}, across every subscription (M8.37). */
    public long pollFailures(PollFailure kind) {
        return metrics.count(failureCounter(kind));
    }

    /** The configured first reconnect delay, exposed for production wiring checks. */
    public Duration retryFloor() {
        return retryFloor;
    }

    /**
     * Stops this transport; every subscription's reader loop exits at its next
     * turn.
     *
     * <p>⚠️ **NOT an override**: {@code SubscriptionTransport} has no
     * {@code close}, because a consumer's subscription handle is what it closes
     * (that interface's {@code subscribe} returns one). This is the process's
     * handle on the transport itself, for the root that built it.
     */
    public void close() {
        if (closed.compareAndSet(false, true)) {
            catchUpExchange.close();
            client.closeResource();
        }
    }
}
