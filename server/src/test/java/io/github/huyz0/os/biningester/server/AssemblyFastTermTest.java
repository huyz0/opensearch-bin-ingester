// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.SequencerTransport;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Every elected term of the assembled server starts its fast term before it
 * serves, fast index or not, and a term whose start fails is given back
 * (ADR-0081 §5 steps 2-3; M13.27d).
 */
class AssemblyFastTermTest {

    private static final String PREFIX = "bins/cluster-a";

    private static ServerConfig config(String podId) {
        return new ServerConfig(podId, "az-a", "cluster-a", PREFIX,
                new StoreConfig("memory", Optional.empty()),
                Duration.ofSeconds(10), Duration.ofSeconds(3), "http://" + podId + ":8080",
                IngestConfig.defaults("cluster-a"), 0, "producer-1", java.util.Set.of("logs"),
                RetentionConfig.defaults(), Optional.empty(), "uid-" + podId,
                io.github.huyz0.os.biningester.ingest.CostTopKReporter.DEFAULT_INTERVAL,
                io.github.huyz0.os.biningester.ingest.IndexQuotas.Config.none(), false, java.util.Optional.empty());
    }

    private static SequencerTransport noPeers() {
        return new SequencerTransport() {
            @Override
            public io.github.huyz0.os.biningester.format.CommitDelta send(String endpoint,
                    CommitRequest request) {
                throw new UnsupportedOperationException("no peer expected: " + endpoint);
            }

            @Override
            public void close() {
            }
        };
    }

    /** {@code backing}, refusing every roster creation as a failed store would. */
    private static BinStore refusingRosters(MemoryBinStore backing) {
        return (BinStore) Proxy.newProxyInstance(BinStore.class.getClassLoader(),
                new Class<?>[] {BinStore.class}, (proxy, method, args) -> {
                    if (method.getName().equals("putIfAbsent")
                            && ((String) args[0]).endsWith(".roster")) {
                        throw new IOException("store down");
                    }
                    try {
                        return method.invoke(backing, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    private static Optional<Long> latest(BinStore store) throws IOException {
        if (store.stat(Roster.latestKey(PREFIX)).isEmpty()) {
            return Optional.empty();
        }
        try (InputStream in = store.get(Roster.latestKey(PREFIX))) {
            return Optional.of(Roster.decodeLatest(in.readAllBytes()));
        }
    }

    private static Roster roster(BinStore store, long epoch) throws IOException {
        try (InputStream in = store.get(Roster.key(PREFIX, epoch))) {
            return Roster.decode(in.readAllBytes());
        }
    }

    @Test
    void theASSEMBLEDLeaderStartsItsFastTermNamedByItsUid() throws Exception {
        MemoryBinStore store = new MemoryBinStore();

        try (Assembly ignored = Assembly.open(config("pod1"), store, noPeers(),
                Clock.systemUTC())) {
            assertThat(latest(store)).contains(1L);
            assertThat(roster(store, 1).leader().podUid()).isEqualTo("uid-pod1");
            assertThat(roster(store, 1).leader().az()).isEqualTo("az-a");
        }
    }

    @Test
    void theNEXTLeaderClosesTheEmptyTermBeforeIt() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        try (Assembly first = Assembly.open(config("pod1"), store, noPeers(),
                Clock.systemUTC())) {
            assertThat(latest(store)).contains(1L);
        }

        try (Assembly second = Assembly.open(config("pod2"), store, noPeers(),
                Clock.systemUTC())) {
            assertThat(latest(store)).contains(2L);
            assertThat(roster(store, 1).fencedBy()).isEqualTo(2);
            assertThat(roster(store, 1).closed()).as("no fast index: closed at once").isTrue();
        }
    }

    @Test
    void aTERMWhoseStartFailsIsGivenBackAtOnce() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        try (Assembly failed = Assembly.open(config("pod1"), refusingRosters(backing), noPeers(),
                Clock.systemUTC())) {
            assertThat(latest(backing)).as("no term started").isEmpty();

            try (Assembly next = Assembly.open(config("pod2"), backing, noPeers(),
                    Clock.systemUTC())) {
                assertThat(latest(backing)).as("pod2 took the lease without waiting a TTL")
                        .contains(2L);
                assertThat(roster(backing, 2).leader().podUid()).isEqualTo("uid-pod2");
            }
        }
    }
}
