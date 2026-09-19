// SPDX-License-Identifier: Apache-2.0
package binjava.binstore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Whether the store is answering, from the calls a node makes anyway (M8.15).
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class HealthTrackingBinStoreTest {

    /** A clock the test moves. */
    private static final class MovableClock extends Clock {
        volatile long millis = 1_000_000;

        @Override
        public long millis() {
            return millis;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }
    }

    /** What every call on the fake store does. */
    private interface Behaviour {
        Object on(String method) throws IOException;
    }

    private static BinStore store(Behaviour behaviour) {
        return (BinStore) Proxy.newProxyInstance(BinStore.class.getClassLoader(),
                new Class<?>[] {BinStore.class}, (proxy, method, args) -> {
                    if (method.getName().equals("close")) {
                        return null;
                    }
                    return behaviour.on(method.getName());
                });
    }

    /** Every tracked call, invoked with harmless arguments. */
    private static final List<String> TRACKED = List.of("get", "getRange", "stat", "list",
            "put", "putIfAbsent", "putIfMatch", "multipart", "delete");
    private static final List<String> WRITES = List.of("put", "putIfAbsent", "putIfMatch",
            "multipart", "delete");

    private static void call(BinStore store, String name) throws IOException {
        try {
            switch (name) {
                case "get" -> store.get("k");
                case "getRange" -> store.getRange("k", 0, 1);
                case "stat" -> store.stat("k");
                case "list" -> store.list("p", null, 10);
                case "put" -> store.put("k", Body.ofBytes(new byte[1]));
                case "putIfAbsent" -> store.putIfAbsent("k", Body.ofBytes(new byte[1]));
                case "putIfMatch" -> store.putIfMatch("k", Body.ofBytes(new byte[1]), null);
                case "multipart" -> store.multipart("k");
                case "delete" -> store.delete(List.of("k"));
                default -> throw new IllegalArgumentException(name);
            }
        } catch (IOException | RuntimeException passedThrough) {
            throw passedThrough;
        }
    }

    @Test
    void anIDLEStoreIsHEALTHY() {
        // ⚠️ No call, no evidence: and an idle pod must not probe (NFR-2).
        HealthTrackingBinStore store = new HealthTrackingBinStore(store(m -> null),
                new MovableClock(), Duration.ofSeconds(10), 3);
        assertThat(store.healthy()).isTrue();
    }

    @Test
    void EVERYCallThatHANGSPastTheStallMakesItUNHEALTHY() throws Exception {
        for (String name : TRACKED) {
            MovableClock clock = new MovableClock();
            CountDownLatch inside = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            HealthTrackingBinStore store = new HealthTrackingBinStore(store(m -> {
                inside.countDown();
                try {
                    release.await();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
                return null;
            }), clock, Duration.ofSeconds(10), 3);
            Thread hung = Thread.ofVirtual().start(() -> {
                try {
                    call(store, name);
                } catch (IOException | RuntimeException ignored) {
                    // the fake returns null where a type is expected; irrelevant
                }
            });
            inside.await();
            clock.millis += 10_000;
            assertThat(store.healthy()).as("%s at the bound", name).isTrue();
            clock.millis += 1;
            assertThat(store.healthy()).as("⚠️ %s STALLED PAST THE BOUND", name).isFalse();
            release.countDown();
            hung.join();
            assertThat(store.healthy()).as("%s returned: healthy again", name).isTrue();
        }
    }

    @Test
    void oneHUNGCallWhileOTHERSSucceedIsNOTAnOutage() throws Exception {
        // ⚠️ REVIEW MEASURED THE FIRST DRAFT keeping a busy node unready for
        // 20 s over one connection hung to its timeout while hundreds of other
        // calls succeeded.
        MovableClock clock = new MovableClock();
        CountDownLatch inside = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        boolean[] hangNext = {true};
        HealthTrackingBinStore store = new HealthTrackingBinStore(store(m -> {
            if (hangNext[0]) {
                hangNext[0] = false;
                inside.countDown();
                try {
                    release.await();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
            return Optional.empty();
        }), clock, Duration.ofSeconds(10), 3);
        Thread hung = Thread.ofVirtual().start(() -> {
            try {
                store.stat("hung");
            } catch (IOException unexpected) {
                throw new AssertionError(unexpected);
            }
        });
        inside.await();

        clock.millis += 11_000;
        store.stat("answered");

        assertThat(store.healthy()).as("⚠️ THE STORE IS ANSWERING").isTrue();
        clock.millis += 11_000;
        assertThat(store.healthy())
                .as("but with no success for a whole stall, the hung call counts again")
                .isFalse();
        release.countDown();
        hung.join();
    }

    @Test
    void CONSECUTIVEFailedWRITESAreUNHEALTHYAndOneSUCCESSHeals() throws Exception {
        for (String name : WRITES) {
            boolean[] failing = {true};
            HealthTrackingBinStore store = new HealthTrackingBinStore(store(m -> {
                if (failing[0]) {
                    throw new IOException("connection refused");
                }
                return null;
            }), new MovableClock(), Duration.ofSeconds(10), 3);

            for (int i = 0; i < 2; i++) {
                assertThatThrownBy(() -> call(store, name)).isInstanceOf(IOException.class);
            }
            assertThat(store.healthy()).as("%s: two failures, under the threshold", name)
                    .isTrue();
            assertThatThrownBy(() -> call(store, name)).isInstanceOf(IOException.class);
            assertThat(store.healthy()).as("⚠️ %s: A REFUSING STORE", name).isFalse();

            failing[0] = false;
            try {
                call(store, name);
            } catch (RuntimeException nullFromTheFake) {
                // the success is what matters
            }
            assertThat(store.healthy()).as("⚠️ %s: ONE SUCCESS CLEARS IT", name).isTrue();
        }
    }

    @Test
    void failedREADSAreNOTAnOutage() throws Exception {
        // ⚠️ A GET of a key that is gone fails legitimately -- a lagging reader
        // fetching a collected segment -- and three in a row would otherwise
        // make a working node unready.
        HealthTrackingBinStore store = new HealthTrackingBinStore(store(m -> {
            throw new IOException("no such key");
        }), new MovableClock(), Duration.ofSeconds(10), 3);
        for (String read : List.of("get", "getRange", "stat", "list", "get", "get")) {
            assertThatThrownBy(() -> call(store, read)).isInstanceOf(IOException.class);
        }
        assertThat(store.healthy()).isTrue();
    }

    @Test
    void aSUCCESSBetweenFailedWritesRESETSTheCount() throws Exception {
        int[] calls = {0};
        HealthTrackingBinStore store = new HealthTrackingBinStore(store(m -> {
            calls[0]++;
            if (calls[0] % 3 == 0) {
                return null;
            }
            throw new IOException("refused");
        }), new MovableClock(), Duration.ofSeconds(10), 3);

        for (int i = 0; i < 9; i++) {
            try {
                store.put("k", Body.ofBytes(new byte[1]));
            } catch (IOException expected) {
                // two failures, then a success, three times over
            }
        }

        assertThat(store.healthy()).isTrue();
    }

}
