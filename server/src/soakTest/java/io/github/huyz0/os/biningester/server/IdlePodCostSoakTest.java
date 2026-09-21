// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.client.HttpSubscriptionTransport;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.SequencerTransport;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * M8's criterion 3: an idle assembled pod owes its lease renewals and its
 * flushes, and nothing else, at 1 subscriber and at 1,000, over five minutes
 * of wall clock (M8.40, NFR-2, R3).
 *
 * <p>⚠️ **THE TWO PODS RUN SIDE BY SIDE, OVER ONE WINDOW**, so what differs
 * between them is the subscriber count and nothing else. A heartbeat, a
 * metrics push or a per-subscriber poll of the store shows up as a request
 * that is not the lease's, and as a difference between the two.
 *
 * <p>⚠️ **EXACT WHERE IT CAN BE**: every request that is not a lease renewal
 * must be ZERO in both, which is equality. The renewals are the lease's own
 * timer, and two pods started a moment apart can each land one renewal
 * either side of the window's edge, so they are held to one renewal of each
 * other rather than to identity. With nothing appended there is nothing to
 * flush, so the flushes this criterion allows are zero here.
 *
 * <p>⚠️ **WALL CLOCK, ON PURPOSE**: the timers under test are the real ones, so
 * this sleeps through the window rather than moving a fake clock, and lives in
 * the soak suite (build.md L2S) rather than in {@code ./gradlew test}.
 */
@Timeout(value = 12, unit = TimeUnit.MINUTES, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class IdlePodCostSoakTest {

    private static final Duration WINDOW = Duration.ofMinutes(5);
    private static final int MANY = 1_000;

    /** Store requests, split into the lease's and everything else's. */
    private static final class Meter {
        final AtomicLong lease = new AtomicLong();
        final AtomicLong other = new AtomicLong();
        final Map<String, AtomicLong> otherByVerb = new java.util.concurrent.ConcurrentHashMap<>();

        BinStore around(BinStore real) {
            return (BinStore) Proxy.newProxyInstance(BinStore.class.getClassLoader(),
                    new Class<?>[] {BinStore.class}, (proxy, method, args) -> {
                        String verb = method.getName();
                        if (!verb.equals("capabilities") && !verb.equals("close")) {
                            String key = args != null && args.length > 0
                                    && args[0] instanceof String k ? k : "";
                            if (key.contains("/ctl/lease/")) {
                                lease.incrementAndGet();
                            } else {
                                other.incrementAndGet();
                                otherByVerb.computeIfAbsent(verb + " " + key,
                                        unused -> new AtomicLong()).incrementAndGet();
                            }
                        }
                        try {
                            return method.invoke(real, args);
                        } catch (InvocationTargetException thrown) {
                            throw thrown.getCause();
                        }
                    });
        }

        long[] read() {
            return new long[] {lease.get(), other.get()};
        }
    }

    /** One pod, its meter, and its subscribers. */
    private static final class Pod implements AutoCloseable {
        final Meter meter = new Meter();
        final MemoryBinStore backing = new MemoryBinStore();
        final Assembly assembly;
        final FrontDoor door;
        final HttpSubscriptionTransport transport;
        final List<AutoCloseable> subscriptions = new ArrayList<>();

        Pod(String podId, int subscribers) throws Exception {
            Map<String, String> settings = new HashMap<>();
            settings.put(ServerProperties.POD_ID, podId);
            settings.put(ServerProperties.POD_AZ, "az-a");
            settings.put(ServerProperties.TRUST_DOMAIN, "cluster-a");
            settings.put(ServerProperties.PREFIX, "bins/" + podId);
            settings.put(ServerProperties.STORE_KIND, "memory");
            settings.put(ServerProperties.HTTP_PORT, "0");
            settings.put(ServerProperties.ENDPOINT, "http://localhost:0");
            settings.put(ServerProperties.PRODUCER_SUBJECT, "producer-1");
            settings.put(ServerProperties.PRODUCER_ALLOWED_INDICES, "logs");
            ServerConfig config = ServerProperties.parse(settings);
            assembly = Assembly.open(config, meter.around(backing), noPeers(),
                    Clock.systemUTC());
            door = FrontDoor.start(assembly, Clock.systemUTC());
            transport = new HttpSubscriptionTransport("http://localhost:" + door.port(),
                    () -> { }, Duration.ofSeconds(1), Duration.ofSeconds(30),
                    Duration.ofSeconds(40));
            for (int s = 0; s < subscribers; s++) {
                subscriptions.add(transport.subscribe(new RunKey(UUID.randomUUID(), s % 8),
                        delivery -> { }));
            }
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (transport.reconnects() < subscribers) {
                assertThat(System.nanoTime()).as("%s's %d subscriptions open", podId,
                        subscribers).isLessThan(deadline);
                Thread.sleep(50);
            }
        }

        @Override
        public void close() throws Exception {
            for (AutoCloseable subscription : subscriptions) {
                subscription.close();
            }
            transport.close();
            door.close();
            assembly.close();
            backing.close();
        }
    }

    private static SequencerTransport noPeers() {
        return new SequencerTransport() {
            @Override
            public CommitDelta send(String endpoint, CommitRequest request) {
                throw new UnsupportedOperationException("one pod: no peer: " + endpoint);
            }

            @Override
            public void close() {
            }
        };
    }

    @Test
    void anIDLEPodOwesOnlyITSLeaseAtOneSubscriberAndAtATHOUSAND() throws Exception {
        try (Pod one = new Pod("podone", 1); Pod many = new Pod("podmany", MANY)) {
            long[] oneBefore = one.meter.read();
            long[] manyBefore = many.meter.read();
            // ⚠️ THE WINDOW IS SLEPT THROUGH: see the class javadoc.
            Thread.sleep(WINDOW.toMillis());
            long[] oneAfter = one.meter.read();
            long[] manyAfter = many.meter.read();

            long oneLease = oneAfter[0] - oneBefore[0];
            long manyLease = manyAfter[0] - manyBefore[0];
            long oneOther = oneAfter[1] - oneBefore[1];
            long manyOther = manyAfter[1] - manyBefore[1];
            System.out.println("M8.40 over " + WINDOW.toMinutes() + " min idle: 1 subscriber "
                    + oneLease + " lease + " + oneOther + " other; " + MANY + " subscribers "
                    + manyLease + " lease + " + manyOther + " other " + many.meter.otherByVerb);

            assertThat(manyOther)
                    .as("⚠️ NOTHING BUT THE LEASE AT %d SUBSCRIBERS: %s", MANY,
                            many.meter.otherByVerb)
                    .isZero();
            assertThat(oneOther)
                    .as("⚠️ AND NOTHING BUT THE LEASE AT ONE: %s", one.meter.otherByVerb)
                    .isZero();
            assertThat(oneLease).as("the premise: the lease was renewed").isPositive();
            long perRenewal = Math.max(1, Math.max(oneLease, manyLease)
                    / Math.max(1, WINDOW.dividedBy(one.assembly.config().leaseRenewInterval())));
            assertThat(Math.abs(oneLease - manyLease))
                    .as("⚠️ THE SAME RENEWALS AT 1 AND AT %d, to within one renewal's "
                            + "requests (%d): a subscriber-driven lease cost would not be",
                            MANY, perRenewal)
                    .isLessThanOrEqualTo(perRenewal);
        }
    }
}
