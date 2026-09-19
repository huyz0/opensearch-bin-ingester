// SPDX-License-Identifier: Apache-2.0
package binjava.http;

import binjava.client.HttpSubscriptionTransport;
import binjava.format.ConsumerProgress;
import binjava.format.FetchMode;
import binjava.format.IndexRegistration;
import binjava.format.RunKey;
import binjava.format.SubscriptionEvent;
import binjava.ingest.IndexCatalog;
import binjava.ingest.SubscriptionHub;
import binjava.ingest.WatermarkTable;
import io.helidon.http.Status;
import io.helidon.webserver.http.HttpRules;
import io.helidon.webserver.http.HttpService;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.time.Clock;
import java.time.Duration;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * The ingester's half of the subscription channel (M8.21, M5.6e, FR-16, FR-9).
 *
 * <p>⚠️ **ONE CHANNEL, THREE MESSAGES, AND THAT IS THE DESIGN RATHER THAN A
 * SHORTCUT.** A push goes out on the stream a consumer holds open; a
 * registration (ADR-0047) and a progress frame (ADR-0049) come back on the same
 * connection's origin. A second endpoint would be a second credential and a
 * second unauthenticated surface, for one message per index per cluster-state
 * change and one per node per interval.
 *
 * <p>⚠️ **A LONG POLL, NOT AN OPEN STREAM, AND THAT WAS MEASURED RATHER THAN
 * PREFERRED.** The first implementation held one chunked response open per
 * subscription and wrote length-prefixed frames into it. Helidon's client-side
 * stream does not block across a chunk boundary: every heartbeat arrived and
 * the first real frame killed the connection with
 * {@code InsufficientDataAvailableException}, byte-at-a-time reads included. A
 * poll answers with a COMPLETE body, which needs none of that.
 *
 * <p>⚠️ **THE BODY IS LENGTH-PREFIXED ANYWAY**, because one answer carries
 * every push already queued: four big-endian bytes then that many bytes of
 * {@link SubscriptionEvent}, repeated. Draining is what keeps a busy stream at
 * one request per ROUND TRIP rather than one per segment.
 *
 * <p>⚠️ **THE COST IS AN HTTP REQUEST PER SUBSCRIPTION PER 30 s WHEN IDLE**,
 * and it is worth naming because this project counts requests: it is a
 * SAME-AZ request to a peer, never an object-store one, so NFR-2 (zero idle
 * store requests) is untouched. What it does cost is ingester CPU and a
 * connection per subscription, which is why the wait is 30 s and the
 * server bounds it.
 *
 * <p>⚠️ **A SLOW CONSUMER LOSES ITS QUEUED PUSHES, NEVER THE INGESTER'S
 * MEMORY.** The queue between the hub's publishing thread and this poll is
 * fixed and the hub offers rather than blocks; a consumer that stopped polling
 * simply finds nothing waiting and resumes from its own committed position
 * (ADR-0005). Buffering instead would make one wedged node's backlog the
 * ingester's memory problem, which is the shape NFR-6 forbids on the write
 * path.
 */
public final class SubscriptionService implements HttpService {

    /**
     * {@code GET} here, with an index uuid and a partition, to subscribe.
     *
     * <p>⚠️ **THE PATHS ARE THE CLIENT'S CONSTANTS**, so that one change moves
     * both ends. `http` depends on `client`, never the reverse.
     */
    public static final String SUBSCRIBE_PATH =
            HttpSubscriptionTransport.SUBSCRIBE_PREFIX + "{indexUuid}/{partition}";

    /** {@code POST} here to tell the ingester an index's shape (ADR-0047). */
    public static final String REGISTER_PATH = HttpSubscriptionTransport.REGISTER_PATH;

    /** {@code POST} here to tell the ingester where a node's copies are (ADR-0049). */
    public static final String PROGRESS_PATH = HttpSubscriptionTransport.PROGRESS_PATH;

    /**
     * ⚠️ 64 pushes. Deep enough that a consumer doing ordinary work never sees
     * it, shallow enough that a wedged one is disconnected in seconds rather
     * than holding megabytes of another node's problem.
     */
    static final int QUEUE_DEPTH = 64;

    /** ⚠️ The same 1 MiB cap the commit route uses, for the same reason. */
    static final long MAX_FRAME_BYTES = 1L << 20;

    private final SubscriptionHub hub;
    private final IndexCatalog catalog;
    private final WatermarkTable watermarks;
    private final Clock clock;

    public SubscriptionService(SubscriptionHub hub, IndexCatalog catalog,
            WatermarkTable watermarks, Clock clock) {
        this(hub, catalog, watermarks, IDLE_EXPIRY, clock);
    }

    /**
     * The production shape: default expiry and session cap, and the floors
     * this node serves (M8.6).
     */
    public SubscriptionService(SubscriptionHub hub, IndexCatalog catalog,
            WatermarkTable watermarks, Clock clock, binjava.ingest.RetainedFloors floors) {
        this(hub, catalog, watermarks, IDLE_EXPIRY, clock, MAX_SESSIONS, floors);
    }

    /**
     * The same, with the idle expiry given.
     *
     * <p>⚠️ **INJECTABLE SO THE SWEEP CAN BE ASSERTED**: at ninety seconds a
     * test can only watch a session NOT be swept, which is what review measured
     * — the case passed with the sweep deleted.
     */
    public SubscriptionService(SubscriptionHub hub, IndexCatalog catalog,
            WatermarkTable watermarks, Duration idleExpiry, Clock clock) {
        this(hub, catalog, watermarks, idleExpiry, clock, MAX_SESSIONS);
    }

    /**
     * The same, with the session cap given.
     *
     * <p>⚠️ **INJECTABLE SO THE REFUSAL CAN BE ASSERTED**: at 4,096 a case
     * would have to open four thousand sessions over a real socket to see the
     * bound, which is a slow test of a cheap predicate.
     */
    public SubscriptionService(SubscriptionHub hub, IndexCatalog catalog,
            WatermarkTable watermarks, Duration idleExpiry, Clock clock, int maxSessions) {
        this(hub, catalog, watermarks, idleExpiry, clock, maxSessions,
                binjava.ingest.RetainedFloors.unknown());
    }

    /**
     * The same, with the retained floors this node serves (M8.6, ADR-0056).
     *
     * <p>⚠️ **EVERY OTHER CONSTRUCTOR SERVES FLOORS THAT ARE NEVER KNOWN**,
     * which is exactly the behaviour before ADR-0056: a floor frame is then
     * never written, and a consumer's floor stays unknown and refuses nothing.
     * Only the composition root passes a real one.
     */
    public SubscriptionService(SubscriptionHub hub, IndexCatalog catalog,
            WatermarkTable watermarks, Duration idleExpiry, Clock clock, int maxSessions,
            binjava.ingest.RetainedFloors floors) {
        this(hub, catalog, watermarks, idleExpiry, clock, maxSessions, floors, new DrainGate());
    }

    /**
     * The production shape, with the gate the node drains on shutdown (M8.7).
     */
    public SubscriptionService(SubscriptionHub hub, IndexCatalog catalog,
            WatermarkTable watermarks, Clock clock, binjava.ingest.RetainedFloors floors,
            DrainGate gate) {
        this(hub, catalog, watermarks, IDLE_EXPIRY, clock, MAX_SESSIONS, floors, gate);
    }

    /**
     * Everything given.
     *
     * <p>⚠️ **EVERY OTHER CONSTRUCTOR GETS A GATE NOBODY DRAINS**, which is
     * the behaviour before M8.7.
     */
    public SubscriptionService(SubscriptionHub hub, IndexCatalog catalog,
            WatermarkTable watermarks, Duration idleExpiry, Clock clock, int maxSessions,
            binjava.ingest.RetainedFloors floors, DrainGate gate) {
        this.gate = Objects.requireNonNull(gate, "gate");
        this.floors = Objects.requireNonNull(floors, "floors");
        this.hub = Objects.requireNonNull(hub, "hub");
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.watermarks = Objects.requireNonNull(watermarks, "watermarks");
        this.idleExpiryMillis = Objects.requireNonNull(idleExpiry, "idleExpiry").toMillis();
        // ⚠️ THE INJECTED CLOCK, not `System.nanoTime` -- non-negotiable 7 and
        // `check-io-seam`, which went red on the first draft. The sweep's
        // deadline is the one thing in this class a test has to be able to
        // move, and reaching for the real clock is what made it unmovable.
        this.clock = Objects.requireNonNull(clock, "clock");
        if (maxSessions <= 0) {
            throw new IllegalArgumentException("the session cap must be positive: " + maxSessions);
        }
        this.maxSessions = maxSessions;
    }

    private final DrainGate gate;
    private final long idleExpiryMillis;
    private final int maxSessions;
    private final binjava.ingest.RetainedFloors floors;

    /**
     * The query parameter a consumer sets to be sent its stream's retained
     * floor (ADR-0056).
     *
     * <p>⚠️ **ASKED FOR, NEVER VOLUNTEERED.** Every decoder in {@code format}
     * refuses a magic it does not know, so an OLD consumer handed a floor
     * frame would refuse the whole answer it arrived in -- a rolling upgrade of
     * the ingester would stop every plugin that had not been upgraded first.
     */
    public static final String FLOOR_PARAM = HttpSubscriptionTransport.FLOOR_PARAM;

    /** How many polls this service has answered, for a request-count assertion. */
    private final java.util.concurrent.atomic.AtomicLong polls =
            new java.util.concurrent.atomic.AtomicLong();

    long pollCount() {
        return polls.get();
    }

    @Override
    public void routing(HttpRules rules) {
        rules.get(SUBSCRIBE_PATH, this::subscribe)
                .post(REGISTER_PATH, this::register)
                .post(PROGRESS_PATH, this::progress);
    }

    /**
     * One consumer's standing interest in one stream, across polls.
     *
     * <p>⚠️ **THE SUBSCRIPTION MUST OUTLIVE THE POLL, AND MEASURING THAT IS
     * WHAT PRODUCED THIS CLASS.** The first implementation subscribed to the
     * hub for the duration of one poll: a push published in the gap between two
     * polls reached nobody, and the consumer never learned it existed — no gap,
     * no error, just records it would never be delivered. A consumer's position
     * is its own (ADR-0005), so nothing downstream would have noticed either.
     */
    private static final class Session {
        private final BlockingQueue<SubscriptionHub.Push> queue =
                new ArrayBlockingQueue<>(QUEUE_DEPTH);
        private final AutoCloseable subscription;
        private volatile long lastPolledMillis;

        Session(SubscriptionHub hub, RunKey key, long nowMillis) {
            // ⚠️ `offer`, NOT `put`: `put` BLOCKS THE HUB'S PUBLISHING THREAD,
            // which is shared by every subscriber on this node -- so one
            // consumer that stopped polling would stop deliveries to all of
            // them. A full queue drops, and the consumer resumes from its own
            // committed position.
            this.subscription = hub.subscribe(key, SubscriptionHub.assembling(queue::offer));
            this.lastPolledMillis = nowMillis;
        }

        /**
         * ⚠️ **NOT `AutoCloseable`**: its `close` would declare
         * `InterruptedException`, and `-Werror` refuses a resource whose close
         * can throw one. Nothing here is used in a try-with-resources anyway —
         * a session outlives every poll, which is its whole point.
         */
        void release() throws Exception {
            subscription.close();
        }
    }

    private final java.util.Map<String, Session> sessions = new java.util.concurrent.ConcurrentHashMap<>();

    private void subscribe(ServerRequest request, ServerResponse response) {
        RunKey key;
        try {
            key = new RunKey(UUID.fromString(request.path().pathParameters().get("indexUuid")),
                    Integer.parseInt(request.path().pathParameters().get("partition")));
        } catch (IllegalArgumentException notAStream) {
            // ⚠️ 400: a path this node cannot read names no stream, so there is
            // nothing to subscribe to and nothing to retry.
            response.status(Status.BAD_REQUEST_400).send("not a stream: " + notAStream.getMessage());
            return;
        }
        String id = request.query().first("sub").orElse(null);
        if (id == null || id.isBlank()) {
            // ⚠️ REFUSED RATHER THAN SYNTHESISED. An id the SERVER invents is a
            // new session per poll, which is the defect this class exists to
            // fix, arriving through the front door.
            response.status(Status.BAD_REQUEST_400).send("a poll carries a subscriber id");
            return;
        }

        if (!gate.enterPoll()) {
            response.status(Status.SERVICE_UNAVAILABLE_503).send(DRAINING);
            return;
        }
        try {
            admitted(request, response, key, id);
        } finally {
            gate.exitPoll();
        }
    }

    /**
     * ⚠️ **WHAT A DRAINING NODE SAYS TO A POLL (research 08 §7 step 2).** The
     * consumer's transport reads a 503 as a failed poll and reconnects with
     * jittered backoff, through the cluster address, to a pod that is still
     * ready.
     */
    static final String DRAINING = "this ingester is draining; reconnect";

    private void admitted(ServerRequest request, ServerResponse response, RunKey key,
            String id) {
        polls.incrementAndGet();
        sweepIdle();
        String sessionKey = id + "@" + key;
        if (!sessions.containsKey(sessionKey) && sessions.size() >= maxSessions) {
            // ⚠️ REFUSED RATHER THAN ADMITTED. The id is the CALLER'S, on a
            // route this deployment does not authenticate, and a session is a
            // hub subscription plus a 64-push queue held until the idle expiry
            // -- so a caller sending a fresh id per poll allocates both, faster
            // than the sweep can reclaim them, until the node dies of it. The
            // cap is the whole defence: the sweep is not one, because every
            // such session is freshly polled.
            response.status(Status.SERVICE_UNAVAILABLE_503)
                    .send("this ingester holds " + maxSessions + " subscriptions already");
            return;
        }
        Session session = sessions.computeIfAbsent(sessionKey,
                unused -> new Session(hub, key, clock.millis()));
        session.lastPolledMillis = clock.millis();
        try {
            java.io.ByteArrayOutputStream body = new java.io.ByteArrayOutputStream();
            // ⚠️ THE FLOOR GOES FIRST, TO ANY POLL THAT ASKS (ADR-0056). An
            // earlier version sent it only on the answer that CREATED the
            // session, and review found the hole: that answer can be lost on
            // the network, the retry carries the same `sub` id, and the
            // session never hears its floor. The consumer asks until one
            // arrives and then stops, so the cost is a cache lookup per poll
            // only while the floor is unknown to it -- and the cache, not the
            // poll, is what reads the store.
            if (request.query().first(FLOOR_PARAM).map("1"::equals).orElse(false)) {
                java.util.OptionalLong floor = floors.floorOf(key);
                if (floor.isPresent()) {
                    writeFrame(body, new binjava.format.RetainedFloor(key, floor.getAsLong())
                            .encode());
                }
            }
            SubscriptionHub.Push first = session.queue.poll(waitFor(request).toMillis(),
                    TimeUnit.MILLISECONDS);
            if (first != null) {
                writeFrame(body, eventFor(first, id).encode());
                // ⚠️ EVERYTHING ELSE ALREADY QUEUED GOES IN THE SAME ANSWER.
                // One poll per push would make a busy stream cost a request per
                // segment; draining makes it cost a request per ROUND TRIP,
                // which is what long-polling is for.
                for (SubscriptionHub.Push more = session.queue.poll(); more != null;
                        more = session.queue.poll()) {
                    writeFrame(body, eventFor(more, id).encode());
                }
            }
            session.lastPolledMillis = clock.millis();
            // ⚠️ AN EMPTY 200 IS THE QUIET-STREAM ANSWER, not a 204: the
            // consumer re-polls either way, and one status for "nothing yet"
            // and another for "here is something" is one more thing for a proxy
            // to treat differently.
            response.status(Status.OK_200).send(body.toByteArray());
        } catch (InterruptedException interrupted) {
            if (gate.pollsRefused()) {
                // ⚠️ WOKEN BY THE DRAIN, SO TOLD TO GO rather than handed an
                // empty 200, which the consumer would answer by polling this
                // pod again. The interrupt was the gate's, and the gate clears
                // it when the poll exits.
                response.status(Status.SERVICE_UNAVAILABLE_503).send(DRAINING);
                return;
            }
            Thread.currentThread().interrupt();
            response.status(Status.OK_200).send(new byte[0]);
        } catch (IOException impossible) {
            // ⚠️ THE BODY IS A `ByteArrayOutputStream`, which cannot fail --
            // said here rather than swallowed, because an empty catch on an
            // IOException is where a real one hides later.
            throw new IllegalStateException("building a poll answer in memory failed",
                    impossible);
        }
    }

    /**
     * Drops sessions nobody has polled.
     *
     * <p>⚠️ **WITHOUT THIS, AN ABANDONED CONSUMER LEAKS A HUB SUBSCRIPTION AND
     * A QUEUE FOR EVER** — and a node that relocates a shard away abandons one
     * every time. Swept opportunistically on each poll rather than on a timer,
     * because a timer here is a thread and a clock seam for a map walk.
     */
    private void sweepIdle() {
        long deadline = clock.millis() - idleExpiryMillis;
        sessions.entrySet().removeIf(entry -> {
            if (entry.getValue().lastPolledMillis > deadline) {
                return false;
            }
            try {
                entry.getValue().release();
            } catch (Exception ignored) {
                // ⚠️ The hub is dropping it either way; what matters is that
                // this map does.
            }
            return true;
        });
    }

    /**
     * ⚠️ Three times the longest poll: a consumer answering promptly renews its
     * session on every poll, and one that has genuinely gone is reclaimed
     * within a couple of minutes rather than at the next restart.
     */
    static final Duration IDLE_EXPIRY = Duration.ofSeconds(90);

    /** How many standing subscriptions this service holds. */
    int sessionCount() {
        return sessions.size();
    }

    /**
     * How long this poll waits for a push before answering empty.
     *
     * <p>⚠️ **BOUNDED BY THE SERVER, NOT BY THE CALLER.** A client asking for an
     * hour would hold a thread and a connection for an hour; a client asking
     * for zero would turn long-polling into a busy loop against the ingester.
     */
    static Duration waitFor(ServerRequest request) {
        // ⚠️ AN UNREADABLE `wait` IS THE CALLER'S MISTAKE AND IS CLAMPED, NOT
        // THROWN: every other unreadable input on this route answers 400, and
        // `Long::parseLong` on `?wait=abc` would answer 500 instead -- an
        // ingester fault for a consumer's typo. There is a right answer here
        // and it is the default wait.
        return waitFor(request.query().first("wait").orElse(null));
    }

    /**
     * The same over the raw parameter, which is what makes the clamp assertable
     * without a socket — review measured every bound removable with all twenty
     * socket cases still green.
     */
    static Duration waitFor(String asked) {
        long millis = asked == null ? MAX_WAIT_MILLIS : millisOrDefault(asked);
        return Duration.ofMillis(Math.max(MIN_WAIT_MILLIS, Math.min(MAX_WAIT_MILLIS, millis)));
    }

    private static long millisOrDefault(String asked) {
        try {
            return Long.parseLong(asked);
        } catch (NumberFormatException notANumber) {
            return MAX_WAIT_MILLIS;
        }
    }

    /**
     * ⚠️ 32,768 subscriptions per ingester node, and the arithmetic matters
     * because an earlier draft picked 4,096 off criterion 3's "1,600 idle
     * shards" and was wrong by more than 2×. The cap is per INGESTER, not per
     * consumer node: cost.md R11 puts on the order of 100 data nodes behind
     * one, a session is one (index, partition) per consumer node —
     * {@code NodeSubscriptions} takes the non-merging default, so one per
     * RunKey — and 1,600 shards with one replica each is already 3,200.
     *
     * <p>⚠️ **IT IS A BOUND, NOT A BUDGET.** What it exists to stop is a caller
     * inventing a fresh id per poll on a route this deployment does not
     * authenticate; a deployment that legitimately reaches it has outgrown one
     * ingester, and the 503 says so. Past the cap shards go unsubscribed, which
     * is why it sits an order of magnitude above the largest shape the
     * requirements name rather than just above it.
     */
    static final int MAX_SESSIONS = 32_768;

    /** ⚠️ 30 s: long enough that an idle stream costs two requests a minute. */
    static final long MAX_WAIT_MILLIS = 30_000;

    /** ⚠️ 50 ms, so a caller cannot ask for a busy loop. */
    static final long MIN_WAIT_MILLIS = 50;

    /** What one push looks like on the wire (ADR-0043, ADR-0044). */
    static SubscriptionEvent eventFor(SubscriptionHub.Push push, String session) {
        byte[] inline = push.via() == FetchMode.INLINE && push.segment() != null
                ? push.segment()
                : new byte[0];
        // ⚠️ `EPOCH_UNKNOWN` IS -1 AND THE EVENT REFUSES A NEGATIVE EPOCH, so a
        // publisher that models no chain becomes 0 here -- which M4.4b reserves
        // for "no lease", and is the honest reading of "this push carries no
        // term" on a wire that cannot express absence.
        long epoch = Math.max(0, push.sequencerEpoch());
        return new SubscriptionEvent(session, epoch, 1L, push.key(),
                push.segmentKey(), push.firstOffset(), push.recordCount(), push.via(), inline);
    }

    private static void writeFrame(OutputStream out, byte[] frame) throws IOException {
        out.write(ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(frame.length).array());
        out.write(frame);
    }

    private void register(ServerRequest request, ServerResponse response) {
        try {
            IndexRegistration registration =
                    IndexRegistration.decode(bounded(request));
            catalog.register(registration);
            response.status(Status.NO_CONTENT_204).send();
        } catch (BodyTooLargeException tooLarge) {
            response.status(Status.REQUEST_ENTITY_TOO_LARGE_413).send(tooLarge.getMessage());
        } catch (IOException | IllegalArgumentException malformed) {
            // ⚠️ 400 AND THE REGISTRAR RETRIES. A registration lost to one
            // dropped message turns into a sustained stream of refused writes
            // for every index on that node (M6.7), so the caller must be able
            // to tell "I sent something wrong" from "you are not listening".
            response.status(Status.BAD_REQUEST_400).send(String.valueOf(malformed.getMessage()));
        }
    }

    private void progress(ServerRequest request, ServerResponse response) {
        try {
            watermarks.observe(ConsumerProgress.decode(bounded(request)));
            response.status(Status.NO_CONTENT_204).send();
        } catch (BodyTooLargeException tooLarge) {
            response.status(Status.REQUEST_ENTITY_TOO_LARGE_413).send(tooLarge.getMessage());
        } catch (IOException | IllegalArgumentException malformed) {
            // ⚠️ A REFUSED FRAME LEAVES THE WATERMARK WHERE IT WAS, which is
            // the safe direction: an unmoved watermark KEEPS data (research 09
            // §6.3), where a frame applied from bytes nobody sent could advance
            // it past what a shard has indexed.
            response.status(Status.BAD_REQUEST_400).send(String.valueOf(malformed.getMessage()));
        }
    }

    private static byte[] bounded(ServerRequest request) throws IOException {
        try (var in = new BoundedStream(request.content().inputStream(), MAX_FRAME_BYTES)) {
            return in.readAllBytes();
        }
    }

    /** ⚠️ Unused today; kept so the routing table reads as one list. */
    static List<String> paths() {
        return List.of(SUBSCRIBE_PATH, REGISTER_PATH, PROGRESS_PATH);
    }
}
