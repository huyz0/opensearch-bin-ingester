// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.Lease;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.SequencerTransport;
import java.io.InputStream;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

/**
 * The assembled sequencer lease reports to the fence, so a takeover's roster
 * carries the lease's wait, and a deposed start serves no term (M13.27d review
 * round 1, T1, T2).
 */
class AssemblyFastTermWaitTest {

    private static final String PREFIX = "bins/cluster-a";
    private static final Duration TTL = Duration.ofSeconds(10);

    private static ServerConfig config(String podId) {
        return new ServerConfig(podId, "az-a", "cluster-a", PREFIX,
                new StoreConfig("memory", Optional.empty()), TTL, Duration.ofSeconds(3),
                "http://" + podId + ":8080", IngestConfig.defaults("cluster-a"), 0, "producer-1",
                java.util.Set.of("logs"), RetentionConfig.defaults(), Optional.empty(),
                "uid-" + podId,
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

    private static Roster roster(MemoryBinStore store, long epoch) throws Exception {
        try (InputStream in = store.get(Roster.key(PREFIX, epoch))) {
            return Roster.decode(in.readAllBytes());
        }
    }

    @Test
    void aTAKEOVERsRosterCarriesTheReplacedLeasesWait() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        try (Assembly first = Assembly.open(config("pod1"), store, noPeers(),
                Clock.systemUTC())) {
            assertThat(first.heldTerm()).isNotNull();
        }
        Lease replaced;
        try (InputStream in = store.get(PREFIX + "/ctl/lease/0.json")) {
            replaced = Lease.decode(in.readAllBytes());
        }

        try (Assembly second = Assembly.open(config("pod2"), store, noPeers(),
                Clock.systemUTC())) {
            assertThat(roster(store, 2).notBefore())
                    .as("the replaced lease's expiry plus TTL / 10 (ADR-0081 §3)")
                    .isEqualTo(replaced.expiresAtMillis() + TTL.toMillis() / 10);
        }
    }

    @Test
    void aDEPOSEDStartServesNoTerm() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        Roster.Incarnation newer = new Roster.Incarnation("pod9", "uid-pod9", "az-b", "");
        store.put(Roster.key(PREFIX, 5), Body.ofBytes(new Roster(5, -1, newer,
                List.of(new Roster.Member(newer, Roster.State.ROSTERED)),
                List.of(new Roster.TermRecord(0, new TreeMap<>())), List.of(), 0, 0, false)
                .encode()));
        store.put(Roster.latestKey(PREFIX), Body.ofBytes(Roster.encodeLatest(5)));

        try (Assembly deposed = Assembly.open(config("pod1"), store, noPeers(),
                Clock.systemUTC())) {
            assertThat(deposed.heldTerm()).as("a newer term exists: nothing served").isNull();
            assertThat(store.stat(Roster.key(PREFIX, 1))).isEmpty();
        }
    }
}
