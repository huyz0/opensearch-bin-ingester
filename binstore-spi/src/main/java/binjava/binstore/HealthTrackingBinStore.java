// SPDX-License-Identifier: Apache-2.0
package binjava.binstore;

import java.io.IOException;
import java.io.InputStream;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Whether the store is answering, judged from the calls a node already makes
 * (M8.15, NFR-8).
 *
 * <p>⚠️ **IT ISSUES NO REQUEST OF ITS OWN.** A health check that probed the
 * store would put a request on a timer, and an idle pod must issue none
 * (NFR-2). So this only watches the calls that happen anyway, and an idle pod
 * that makes none is reported healthy: it has no evidence otherwise, and it
 * has nothing buffered that an outage could strand.
 *
 * <p>⚠️ **TWO SIGNALS, BECAUSE A PARTITION HAS TWO SHAPES.**
 * <ul>
 * <li>A black-holed network answers nothing, so calls hang until their own
 *     timeout, 30 s for the S3 client. That is seen as a call outstanding past
 *     {@code stall} with NO call having succeeded within the last
 *     {@code stall}. One hung connection on a node whose other calls succeed is
 *     not an outage, and review measured it would otherwise keep a busy node
 *     unready for 20 s.
 * <li>A store that refuses answers at once with an error. That is seen as
 *     {@code failures} consecutive failed WRITES, with no success between.
 *     Reads do not count: a GET of a key that is gone fails legitimately, for
 *     example a lagging reader fetching a collected segment, and a write never
 *     fails for "not found".
 * </ul>
 *
 * <p>⚠️ **WHAT IS NOT WATCHED**: only the call itself is tracked, so a
 * {@code get} whose body read hangs after the call returned, and the parts of
 * a multipart upload after it opened, are invisible here.
 *
 * <p>⚠️ **AN UNREADY LEADER CAN LOSE ITS TERM, AND THAT IS INTENDED.** With the
 * {@code EndpointSlice} watch on (M8.13), an endpoint that is neither ready nor
 * terminating counts as gone, so followers take the term early. A leader that
 * cannot reach the store cannot commit, and a follower that can should take
 * over, sooner than the TTL. The cost is a failover for a blip that outlasts
 * the stall or three failed writes; fencing keeps it safe.
 */
public final class HealthTrackingBinStore implements BinStore {

    /** How long one call may be outstanding before the node stops being ready. */
    public static final Duration DEFAULT_STALL = Duration.ofSeconds(10);

    /** How many consecutive failed WRITES make the node unready. Reads never count. */
    public static final int DEFAULT_FAILURES = 3;

    private final BinStore delegate;
    private final Clock clock;
    private final Duration stall;
    private final int failures;
    private final Map<Long, Long> inFlight = new ConcurrentHashMap<>();
    private final AtomicLong ids = new AtomicLong();
    private final AtomicInteger consecutiveFailures = new AtomicInteger();
    /** When a call last succeeded, or {@code -1} if none has. */
    private final AtomicLong lastSuccessMillis = new AtomicLong(-1);

    public HealthTrackingBinStore(BinStore delegate, Clock clock, Duration stall, int failures) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.stall = Objects.requireNonNull(stall, "stall");
        if (stall.isZero() || stall.isNegative() || failures < 1) {
            throw new IllegalArgumentException("stall must be positive and failures at least 1");
        }
        this.failures = failures;
    }

    /** Whether the store is answering: see the class javadoc for the two signals. */
    public boolean healthy() {
        if (consecutiveFailures.get() >= failures) {
            return false;
        }
        long now = clock.millis();
        long lastSuccess = lastSuccessMillis.get();
        if (lastSuccess >= 0 && now - lastSuccess <= stall.toMillis()) {
            return true;
        }
        for (long started : inFlight.values()) {
            if (now - started > stall.toMillis()) {
                return false;
            }
        }
        return true;
    }

    @FunctionalInterface
    private interface Call<T> {
        T run() throws IOException;
    }

    private <T> T read(Call<T> call) throws IOException {
        return track(call, false);
    }

    private <T> T write(Call<T> call) throws IOException {
        return track(call, true);
    }

    private <T> T track(Call<T> call, boolean write) throws IOException {
        long id = ids.incrementAndGet();
        inFlight.put(id, clock.millis());
        try {
            T result = call.run();
            consecutiveFailures.set(0);
            lastSuccessMillis.set(clock.millis());
            return result;
        } catch (IOException failed) {
            if (write) {
                consecutiveFailures.incrementAndGet();
            }
            throw failed;
        } finally {
            inFlight.remove(id);
        }
    }

    @Override
    public InputStream get(String key) throws IOException {
        return read(() -> delegate.get(key));
    }

    @Override
    public InputStream getRange(String key, long start, long endIncl) throws IOException {
        return read(() -> delegate.getRange(key, start, endIncl));
    }

    @Override
    public Optional<ObjectStat> stat(String key) throws IOException {
        return read(() -> delegate.stat(key));
    }

    @Override
    public Version put(String key, Body body) throws IOException {
        return write(() -> delegate.put(key, body));
    }

    @Override
    public Optional<Version> putIfAbsent(String key, Body body) throws IOException {
        return write(() -> delegate.putIfAbsent(key, body));
    }

    @Override
    public Optional<Version> putIfMatch(String key, Body body, Version expected)
            throws IOException {
        return write(() -> delegate.putIfMatch(key, body, expected));
    }

    @Override
    public MultipartWriter multipart(String key) throws IOException {
        return write(() -> delegate.multipart(key));
    }

    @Override
    public ListPage list(String prefix, String startAfter, int maxKeys) throws IOException {
        return read(() -> delegate.list(prefix, startAfter, maxKeys));
    }

    @Override
    public void delete(List<String> keys) throws IOException {
        write(() -> {
            delegate.delete(keys);
            return null;
        });
    }

    @Override
    public Capabilities capabilities() {
        return delegate.capabilities();
    }

    @Override
    public SignedUrl presign(String key, Duration ttl) throws IOException {
        return delegate.presign(key, ttl);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
