// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.Capabilities;
import io.github.huyz0.os.biningester.binstore.ListPage;
import io.github.huyz0.os.biningester.binstore.MultipartWriter;
import io.github.huyz0.os.biningester.binstore.ObjectStat;
import io.github.huyz0.os.biningester.binstore.Version;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * The holder's half of ADR-0081 §3's lease-time fence (M13.26): a leader
 * exposes only while BOTH its wall clock is before the renewal's stamped
 * expiry less the margin AND its monotonic clock is before the renewal's SEND
 * instant plus the TTL less the margin.
 *
 * <p>⚠️ EACH CLOCK IS THE OTHER'S GUARD: a stopped monotonic clock is caught by
 * the wall check, a stopped wall clock by the monotonic one, and a renewal
 * whose response arrives late buys no validity, since it runs from the send.
 */
class FastLeaseFenceHolderTest {

    private static final Duration TTL = Duration.ofSeconds(10);
    private static final Duration RENEW = Duration.ofSeconds(3);

    /** A monotonic clock advanced by hand, never read from the system. */
    static final class Mono implements MonotonicClock {
        long nanos = 5_000_000_000L;

        @Override
        public long nanos() {
            return nanos;
        }

        void advance(Duration d) {
            nanos += d.toNanos();
        }
    }

    record Pod(LeaseManager leases, FastLeaseFence fence, SimulatedClock wall, Mono mono) {
        void advanceBoth(Duration d) {
            wall.advance(d);
            mono.advance(d);
        }
    }

    static Pod pod(BinStore store, String podId) {
        SimulatedClock wall = new SimulatedClock(1_000_000L);
        Mono mono = new Mono();
        FastLeaseFence fence = new FastLeaseFence(TTL, wall, mono);
        LeaseManager leases = new LeaseManager(store,
                new LeaseConfig("p", podId, "", "uid-" + podId, TTL, RENEW), wall,
                LeaseChallenge.NEVER, fence);
        return new Pod(leases, fence, wall, mono);
    }

    @Test
    void aHOLDERExposesUntilTheWallExpiryLessTheMargin() throws Exception {
        Pod a = pod(new MemoryBinStore(), "a");
        assertThat(a.fence().mayExpose()).as("nothing held yet").isFalse();
        a.leases().tryAcquire();

        a.advanceBoth(Duration.ofMillis(8_999));
        assertThat(a.fence().mayExpose()).isTrue();
        a.advanceBoth(Duration.ofMillis(1));
        assertThat(a.fence().mayExpose()).as("9 s into a 10 s lease is the 1 s margin").isFalse();
    }

    @Test
    void aSTOPPEDWallClockIsCaughtByTheMonotonicBound() throws Exception {
        Pod a = pod(new MemoryBinStore(), "a");
        a.leases().tryAcquire();

        a.mono().advance(Duration.ofMillis(8_999));
        assertThat(a.fence().mayExpose()).isTrue();
        a.mono().advance(Duration.ofMillis(1));

        assertThat(a.fence().mayExpose()).as("the wall clock never moved; the monotonic one did")
                .isFalse();
    }

    @Test
    void aSTOPPEDMonotonicClockIsCaughtByTheWallBound() throws Exception {
        Pod a = pod(new MemoryBinStore(), "a");
        a.leases().tryAcquire();

        a.wall().advance(Duration.ofSeconds(9));

        assertThat(a.fence().mayExpose()).isFalse();
    }

    /** Delays every conditional write's answer by advancing the holder's monotonic clock. */
    static final class SlowAnswerStore implements BinStore {
        private final BinStore delegate;
        private final Mono mono;
        private final Duration delay;

        SlowAnswerStore(BinStore delegate, Mono mono, Duration delay) {
            this.delegate = delegate;
            this.mono = mono;
            this.delay = delay;
        }

        @Override
        public Optional<Version> putIfMatch(String key, Body body, Version expected)
                throws IOException {
            Optional<Version> won = delegate.putIfMatch(key, body, expected);
            mono.advance(delay);
            return won;
        }

        @Override
        public Optional<Version> putIfAbsent(String key, Body body) throws IOException {
            return delegate.putIfAbsent(key, body);
        }

        @Override
        public InputStream get(String key) throws IOException {
            return delegate.get(key);
        }

        @Override
        public InputStream getRange(String key, long start, long endIncl) throws IOException {
            return delegate.getRange(key, start, endIncl);
        }

        @Override
        public Optional<ObjectStat> stat(String key) throws IOException {
            return delegate.stat(key);
        }

        @Override
        public Version put(String key, Body body) throws IOException {
            return delegate.put(key, body);
        }

        @Override
        public MultipartWriter multipart(String key) throws IOException {
            return delegate.multipart(key);
        }

        @Override
        public ListPage list(String prefix, String startAfter, int maxKeys) throws IOException {
            return delegate.list(prefix, startAfter, maxKeys);
        }

        @Override
        public void delete(List<String> keys) throws IOException {
            delegate.delete(keys);
        }

        @Override
        public Capabilities capabilities() {
            return delegate.capabilities();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    @Test
    void aRENEWALWhoseAnswerArrivesLateCountsFromItsSend() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        SimulatedClock wall = new SimulatedClock(1_000_000L);
        Mono mono = new Mono();
        FastLeaseFence fence = new FastLeaseFence(TTL, wall, mono);
        LeaseManager leases = new LeaseManager(new SlowAnswerStore(backing, mono,
                Duration.ofSeconds(5)), new LeaseConfig("p", "a", "", "uid-a", TTL, RENEW), wall,
                LeaseChallenge.NEVER, fence);
        leases.tryAcquire();
        wall.advance(Duration.ofSeconds(2));
        mono.advance(Duration.ofSeconds(2));

        assertThat(leases.renew()).isPresent();
        mono.advance(Duration.ofMillis(3_999));
        assertThat(fence.mayExpose()).as("8.999 s after the renewal's send").isTrue();
        mono.advance(Duration.ofMillis(1));

        assertThat(fence.mayExpose())
                .as("9 s after the SEND, though only 4 s after the answer arrived").isFalse();
    }

    @Test
    void aFENCEDHolderStopsExposing() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        Pod a = pod(store, "a");
        a.leases().tryAcquire();
        Pod b = pod(store, "b");
        b.wall().advance(Duration.ofSeconds(11));
        assertThat(b.leases().tryAcquire()).isPresent();

        assertThat(a.leases().renew()).isEmpty();

        assertThat(a.fence().mayExpose()).isFalse();
    }

    @Test
    void aRELEASEDOrStoppingHolderStopsExposing() throws Exception {
        Pod a = pod(new MemoryBinStore(), "a");
        a.leases().tryAcquire();
        a.fence().stopExposing();
        assertThat(a.fence().mayExpose()).as("a leader shutting down stops first").isFalse();

        Pod b = pod(new MemoryBinStore(), "b");
        b.leases().tryAcquire();
        b.leases().release();
        assertThat(b.fence().mayExpose()).isFalse();
    }

    @Test
    void anACQUISITIONSettledLaterCountsFromItsOwnSend() throws Exception {
        AmbiguousPutStore store = new AmbiguousPutStore(new MemoryBinStore(),
                AmbiguousPutStore.Mode.LANDED, AmbiguousPutStore.Target.PUT_IF_ABSENT);
        Pod a = pod(store, "a");
        try {
            a.leases().tryAcquire();
        } catch (IOException expected) {
            // the write landed and its answer was lost
        }
        a.mono().advance(Duration.ofSeconds(5));
        assertThat(a.leases().tryAcquire()).as("the re-read finds its own lease").isPresent();
        a.mono().advance(Duration.ofMillis(3_999));
        assertThat(a.fence().mayExpose()).isTrue();
        a.mono().advance(Duration.ofMillis(1));

        assertThat(a.fence().mayExpose())
                .as("9 s after the write that landed, not after the read that found it")
                .isFalse();
    }
}
