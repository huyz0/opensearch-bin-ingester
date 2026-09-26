// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.DeltaHintFrame;
import io.github.huyz0.os.biningester.format.DeltaPushFrame;
import io.helidon.webclient.api.WebClient;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ObjLongConsumer;
import java.util.function.Supplier;

/**
 * The leaseholder's side of ADR-0075: every delta it makes durable is
 * published here, pushed whole to every other ready pod of its own AZ, and
 * hinted -- 24 bytes -- to one relay in each remote AZ (M10.19).
 *
 * <p>⚠️ **ONE ORDERED LANE PER PEER.** A lane sends one frame at a time and
 * retries it, backing off up to {@link #MAX_BACKOFF}, until the peer accepts
 * it or leaves the ready set (ADR-0075 decision 2), so a peer receives deltas
 * in the order they were committed; its chain publisher drops anything that
 * arrives out of order anyway. A lane re-reads the membership before each
 * retry, so a departed peer -- or a relay no longer its AZ's lowest ready
 * pod -- is noticed without waiting for the next commit, and its whole queue
 * is dropped at once. Lanes are pruned by membership, never by which peers
 * one delta happened to target.
 * What a departed peer's lane still held, what a full lane refuses (by
 * {@link #LANE_DEPTH} frames or {@link #LANE_BYTES}), a delta committed after
 * {@link #close}, and what close cannot deliver in time are dropped and
 * counted: a gap on that peer that only catch-up off the
 * leaseholder repairs until M10.22. Nothing here blocks the commit path.
 *
 * <p>⚠️ **NOT WIRED INTO A NODE HERE.** M10.20 adds the relay and switches
 * nodes to publish through the chain together; switching first would leave a
 * remote-AZ pod receiving nothing, not even its own writes.
 *
 * <p>⚠️ **THE RELAY IS THE LOWEST READY POD ID IN ITS AZ**, by
 * {@link #relayOf}, and the hint route accepts a hint only on the pod that
 * rule names (M10.20); both read the same membership, so a change in it moves
 * both together.
 *
 * <p>⚠️ **BYTES ARE COUNTED PER ATTEMPT**, against the peer's AZ: a retry
 * crosses the wire again, so 24 bytes per (delta, remote AZ) holds exactly
 * only when no hint is retried. A push is same-AZ by construction, so its
 * cross-AZ count staying at zero is the NFR-5 evidence M10.21 reads.
 */
public final class DeltaFanOut implements AutoCloseable {

    /** The route a whole delta is pushed to, on a same-AZ pod. */
    public static final String PUSH_PATH = "/ctl/push";

    /** The route a hint is sent to, on a remote AZ's relay. */
    public static final String HINT_PATH = "/ctl/hint";

    /** Frames that may wait for one peer. */
    public static final int LANE_DEPTH = 1024;

    /**
     * Bytes that may wait for one peer. ⚠️ A push is a whole delta (~150 KiB at
     * scale, up to 8 MiB), so a count alone would let one ready-but-stuck peer
     * pin hundreds of MiB of the leaseholder's heap -- the reason
     * {@code ChainPublisher} bounds its queue in bytes too.
     */
    public static final long LANE_BYTES = 32L << 20;

    /** The longest wait between two attempts at one frame. */
    public static final Duration MAX_BACKOFF = Duration.ofSeconds(2);

    /** How long {@link #close} lets the lanes finish what they hold. */
    static final Duration CLOSE_WAIT = Duration.ofSeconds(2);

    private static final System.Logger LOG = System.getLogger(DeltaFanOut.class.getName());

    /** Sends one frame to one peer; a non-2xx answer is an {@link IOException}. */
    @FunctionalInterface
    public interface PeerPost {
        void post(String endpoint, String path, byte[] body) throws IOException;
    }

    private record Frame(String path, byte[] body, CrossAzBytes.Transport transport, String az) {
    }

    private final String selfPodId;
    private final String selfAz;
    private final int port;
    private final CrossAzBytes crossAz;
    private final Supplier<List<EndpointSliceView.Endpoint>> members;
    private final ObjLongConsumer<CommitDelta> local;
    private final PeerPost post;
    private final Duration backoff;
    private final Pause pause;
    private final Map<String, Lane> lanes = new HashMap<>();
    private final AtomicLong delivered = new AtomicLong();
    private final AtomicLong dropped = new AtomicLong();
    private final AtomicLong overflowed = new AtomicLong();
    private final AtomicLong unpushable = new AtomicLong();
    private boolean closed;

    /** Waits between two attempts; {@code Thread::sleep} in production. */
    @FunctionalInterface
    interface Pause {
        void pause(long millis) throws InterruptedException;
    }

    /**
     * @param local this pod's own chain publisher
     * @param backoff the first retry's wait, doubled for each attempt after it
     */
    public DeltaFanOut(String selfPodId, String selfAz, int port, CrossAzBytes crossAz,
            Supplier<List<EndpointSliceView.Endpoint>> members, ObjLongConsumer<CommitDelta> local,
            PeerPost post, Duration backoff) {
        this(selfPodId, selfAz, port, crossAz, members, local, post, backoff, Thread::sleep);
    }

    DeltaFanOut(String selfPodId, String selfAz, int port, CrossAzBytes crossAz,
            Supplier<List<EndpointSliceView.Endpoint>> members, ObjLongConsumer<CommitDelta> local,
            PeerPost post, Duration backoff, Pause pause) {
        this.pause = Objects.requireNonNull(pause, "pause");
        this.selfPodId = Objects.requireNonNull(selfPodId, "selfPodId");
        this.selfAz = Objects.requireNonNull(selfAz, "selfAz");
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("port is not valid: " + port);
        }
        this.port = port;
        this.crossAz = Objects.requireNonNull(crossAz, "crossAz");
        this.members = Objects.requireNonNull(members, "members");
        this.local = Objects.requireNonNull(local, "local");
        this.post = Objects.requireNonNull(post, "post");
        this.backoff = Objects.requireNonNull(backoff, "backoff");
    }

    /** Pooled Helidon clients and a 100 ms first backoff. */
    public DeltaFanOut(String selfPodId, String selfAz, int port, CrossAzBytes crossAz,
            Supplier<List<EndpointSliceView.Endpoint>> members, ObjLongConsumer<CommitDelta> local) {
        this(selfPodId, selfAz, port, crossAz, members, local, new HttpPost(),
                Duration.ofMillis(100));
    }

    /** The relay of {@code az}: its lowest ready pod id, if it has any ready pod. */
    public static Optional<EndpointSliceView.Endpoint> relayOf(
            List<EndpointSliceView.Endpoint> ready, String az) {
        return ready.stream()
                .filter(endpoint -> endpoint != null && az.equals(endpoint.az()))
                .min(java.util.Comparator.comparing(EndpointSliceView.Endpoint::podId)
                        .thenComparing(EndpointSliceView.Endpoint::address));
    }

    /** The committed-delta hook: never throws into the commit that called it. */
    public void committed(CommitDelta delta, long epoch) {
        Objects.requireNonNull(delta, "delta");
        local.accept(delta, epoch);
        byte[] push;
        try {
            push = new DeltaPushFrame(epoch, delta).encode();
        } catch (IllegalStateException tooLarge) {
            LOG.log(System.Logger.Level.WARNING, "delta at epoch " + epoch + " sequence "
                    + delta.sequence() + " is too large to push; only catch-up reaches it");
            push = null;
            unpushable.incrementAndGet();
        }
        byte[] hint = new DeltaHintFrame(epoch, delta.sequence()).encode();

        Map<String, String> wanted = wanted(members.get());
        Map<String, Frame> targets = new HashMap<>();
        for (Map.Entry<String, String> peer : wanted.entrySet()) {
            String az = peer.getValue();
            if (!selfAz.equals(az)) {
                targets.put(peer.getKey(),
                        new Frame(HINT_PATH, hint, CrossAzBytes.Transport.DELTA_HINT, az));
            } else if (push != null) {
                targets.put(peer.getKey(),
                        new Frame(PUSH_PATH, push, CrossAzBytes.Transport.DELTA_PUSH, az));
            }
        }
        synchronized (lanes) {
            if (closed) {
                dropped.addAndGet(targets.size());
                return;
            }
            // ⚠️ PRUNED BY MEMBERSHIP, NOT BY THIS DELTA's TARGETS: an unpushable
            // delta has no same-AZ target, and must not stop a still-ready
            // peer's lane and drop the backlog it holds.
            lanes.entrySet().removeIf(lane -> {
                if (wanted.containsKey(lane.getKey())) {
                    return false;
                }
                lane.getValue().stop();
                return true;
            });
            targets.forEach((uri, frame) -> {
                if (!lanes.computeIfAbsent(uri, Lane::new).offer(frame)) {
                    overflowed.incrementAndGet();
                }
            });
        }
    }

    /**
     * Every peer this pod sends to, by URI, with its AZ: each other ready pod
     * of this AZ, and the relay of each remote AZ.
     */
    private Map<String, String> wanted(List<EndpointSliceView.Endpoint> ready) {
        Map<String, String> wanted = new HashMap<>();
        Set<String> remoteAzs = new TreeSet<>();
        for (EndpointSliceView.Endpoint endpoint : ready) {
            if (endpoint == null || endpoint.az() == null || endpoint.az().isBlank()
                    || endpoint.address() == null || selfPodId.equals(endpoint.podId())) {
                continue;
            }
            if (selfAz.equals(endpoint.az())) {
                wanted.put(uriOf(endpoint.address(), port), endpoint.az());
            } else {
                remoteAzs.add(endpoint.az());
            }
        }
        for (String az : remoteAzs) {
            relayOf(ready, az).ifPresent(relay -> wanted.put(uriOf(relay.address(), port), az));
        }
        return wanted;
    }

    /** Frames a peer acknowledged. */
    public long delivered() {
        return delivered.get();
    }

    /** Frames discarded unsent because their peer left the ready set or this closed. */
    public long dropped() {
        return dropped.get();
    }

    /** Frames dropped because a peer's lane was full. */
    public long overflowed() {
        return overflowed.get();
    }

    /** Deltas too large to push: hinted across AZs, but no same-AZ pod was pushed them. */
    public long unpushable() {
        return unpushable.get();
    }

    /** The wait after {@code wait}: doubled, never above {@link #MAX_BACKOFF}. */
    static long nextBackoff(long wait) {
        return Math.min(wait * 2, MAX_BACKOFF.toMillis());
    }

    /** Peers that currently have a lane; test-visible. */
    int lanes() {
        synchronized (lanes) {
            return lanes.size();
        }
    }

    /**
     * Lets every lane deliver what it holds for {@link #CLOSE_WAIT} -- the
     * final flush's delta among it -- then stops the rest and counts it.
     */
    @Override
    public void close() {
        List<Lane> closing;
        synchronized (lanes) {
            closed = true;
            closing = List.copyOf(lanes.values());
            lanes.clear();
        }
        java.util.concurrent.CountDownLatch ended =
                new java.util.concurrent.CountDownLatch(closing.size());
        closing.forEach(lane -> lane.finish(ended));
        try {
            if (!ended.await(CLOSE_WAIT.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS)) {
                closing.forEach(Lane::stop);
                ended.await(CLOSE_WAIT.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            closing.forEach(Lane::stop);
        }
    }

    /**
     * One peer's ordered sender. ⚠️ A FRAME IS RETRIED UNTIL IT IS ACCEPTED OR
     * THE PEER LEAVES THE READY SET (ADR-0075 decision 2): a ready peer that
     * stalls for a GC pause or a restart must not lose a delta nobody else can
     * give it. What a stopped lane still holds is counted as dropped.
     */
    private final class Lane {
        private final String endpoint;
        private final BlockingQueue<Frame> queue = new ArrayBlockingQueue<>(LANE_DEPTH);
        private final java.util.concurrent.atomic.AtomicLong queuedBytes =
                new java.util.concurrent.atomic.AtomicLong();
        private final Thread worker;
        private volatile boolean finishing;
        private volatile java.util.concurrent.CountDownLatch ended;
        private final java.util.concurrent.atomic.AtomicBoolean counted =
                new java.util.concurrent.atomic.AtomicBoolean();

        Lane(String endpoint) {
            this.endpoint = endpoint;
            this.worker = Thread.ofVirtual().name("delta-lane").start(this::run);
        }

        boolean offer(Frame frame) {
            long bytes = frame.body().length;
            if (queuedBytes.addAndGet(bytes) > LANE_BYTES) {
                queuedBytes.addAndGet(-bytes);
                return false;
            }
            if (!queue.offer(frame)) {
                queuedBytes.addAndGet(-bytes);
                return false;
            }
            return true;
        }

        /**
         * Whether this lane's peer is still one this pod sends to -- ready,
         * and for a hint lane still its AZ's relay -- read from memory.
         */
        private boolean stillWanted() {
            return wanted(members.get()).containsKey(endpoint);
        }

        /** The peer left the ready set: stop now. */
        void stop() {
            worker.interrupt();
        }

        /** Deliver what is queued, then end and count down {@code whenEnded}. */
        void finish(java.util.concurrent.CountDownLatch whenEnded) {
            ended = whenEnded;
            finishing = true;
            if (!worker.isAlive()) {
                countEnded();
            }
        }

        private void countEnded() {
            java.util.concurrent.CountDownLatch latch = ended;
            if (latch != null && counted.compareAndSet(false, true)) {
                latch.countDown();
            }
        }

        private void run() {
            try {
                drain();
            } finally {
                countEnded();
            }
        }

        private void drain() {
            Frame current = null;
            try {
                while (true) {
                    current = queue.poll(50, java.util.concurrent.TimeUnit.MILLISECONDS);
                    if (current == null) {
                        if (finishing) {
                            return;
                        }
                        continue;
                    }
                    queuedBytes.addAndGet(-current.body().length);
                    boolean accepted;
                    try {
                        accepted = send(current);
                    } catch (RuntimeException broken) {
                        // ⚠️ A lane never dies silently: a peer still wanted
                        // would keep a queue nobody drains. This frame is lost.
                        LOG.log(System.Logger.Level.WARNING, "delta lane failed a frame", broken);
                        dropped.incrementAndGet();
                        current = null;
                        continue;
                    }
                    if (!accepted) {
                        // ⚠️ THE WHOLE LANE, not one frame: every frame behind
                        // it was for the same departed peer.
                        dropped.addAndGet(1 + queue.size());
                        queue.clear();
                        queuedBytes.set(0);
                    }
                    current = null;
                }
            } catch (InterruptedException stopped) {
                Thread.currentThread().interrupt();
                dropped.addAndGet(queue.size() + (current == null ? 0 : 1));
                queue.clear();
            }
        }

        /** @return false when the peer left before accepting it */
        private boolean send(Frame frame) throws InterruptedException {
            long wait = backoff.toMillis();
            while (true) {
                crossAz.sent(frame.transport(), frame.az(), frame.body().length);
                try {
                    post.post(endpoint, frame.path(), frame.body());
                    delivered.incrementAndGet();
                    return true;
                } catch (IOException | RuntimeException failed) {
                    pause.pause(wait);
                    if (!stillWanted()) {
                        return false; // it left: noticed without a commit
                    }
                    wait = nextBackoff(wait);
                }
            }
        }
    }

    private static final class HttpPost implements PeerPost {
        private static final Duration CONNECT = Duration.ofMillis(200);
        private static final Duration READ = Duration.ofSeconds(2);
        private static final int MAX_POOLED_CLIENTS = 64;
        private final Map<String, WebClient> clients = new ConcurrentHashMap<>();

        @Override
        public void post(String endpoint, String path, byte[] body) throws IOException {
            int status;
            try (var response = clientFor(endpoint).post(path).submit(body)) {
                status = response.status().code();
            } catch (RuntimeException unreachable) {
                throw new IOException("peer unreachable", unreachable);
            }
            if (status / 100 != 2) {
                throw new IOException("peer answered " + status);
            }
        }

        private WebClient clientFor(String endpoint) {
            if (clients.size() >= MAX_POOLED_CLIENTS && !clients.containsKey(endpoint)) {
                clients.clear();
            }
            return clients.computeIfAbsent(endpoint, uri -> WebClient.builder()
                    .baseUri(URI.create(uri)).connectTimeout(CONNECT).readTimeout(READ).build());
        }
    }

    /** Placed last: the wired gate's string scan mis-reads "//" in a literal (M10.13). */
    private static String uriOf(String address, int port) {
        String host = address.indexOf(':') >= 0 && !address.startsWith("[")
                ? "[" + address + "]" : address;
        return "http://" + host + ":" + port;
    }
}
