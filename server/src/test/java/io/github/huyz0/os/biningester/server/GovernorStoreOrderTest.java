// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.CostGovernor;
import io.github.huyz0.os.biningester.binstore.GovernorRefusedException;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
import io.github.huyz0.os.biningester.sequencer.ChainBackfill;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.Inbox;
import io.github.huyz0.os.biningester.sequencer.SequencerTransport;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * The assembled store stack puts the governor ABOVE the request counter
 * (M10.11, ADR-0075).
 *
 * <p>⚠️ **A REFUSED LIST NEVER REACHED THE STORE**, so it is not a request and
 * must not be metered as one: a counter above the governor would bill every
 * refusal as the LIST it prevented, and a runaway the governor is stopping
 * would still read, on the meter, as a runaway.
 */
class GovernorStoreOrderTest {

    private static SequencerTransport noPeers() {
        return new SequencerTransport() {
            @Override
            public CommitDelta send(String endpoint, CommitRequest request) {
                throw new UnsupportedOperationException("no peer expected: " + endpoint);
            }

            @Override
            public void close() {
            }
        };
    }

    @Test
    void aLISTTheASSEMBLEDStoreRefusesIsNEVERCountedAsAStoreRequest() throws Exception {
        ServerConfig config = new ServerConfig("pod1", "az-a", "cluster-a", "bins/cluster-a",
                new StoreConfig("memory", Optional.empty()),
                Duration.ofSeconds(10), Duration.ofSeconds(3), "http://pod1:8080",
                IngestConfig.defaults("cluster-a"), 0, "producer-1", Set.of("logs"));
        try (ObservedStore shared = new ObservedStore(Inbox.prefixFor("bins/cluster-a"));
                Assembly assembly = Assembly.openForTest(config, shared, noPeers(),
                        Clock.systemUTC(), ChainBackfill::inBackground,
                        (c, clock, spacing) -> {
                            // ⚠️ ONE TOKEN, SPENT, AND A FROZEN CLOCK: every undeclared
                            // LIST is refused for the life of the case.
                            CostGovernor governor = new CostGovernor(
                                    new CostGovernor.Settings(1.0, 1, Duration.ofMinutes(1),
                                            IngestConfig.DEFAULT_MAX_SEGMENT_BYTES),
                                    Clock.fixed(Instant.parse("2026-09-27T00:00:00Z"),
                                            ZoneOffset.UTC), spacing);
                            governor.admitList();
                            return governor;
                        })) {
            // ⚠️ THE STARTUP DRAIN FIRST. Its declared LIST runs on its own thread
            // and is counted like any other; taken inside this case's window it
            // read as the refused LIST having been counted (2 in ~22 runs). A
            // declared LIST is counted before it reaches the store, so once it
            // has RETURNED it is in the baseline.
            shared.awaitInboxListed();
            long listsBefore = assembly.storeCounts().lists();
            long refusalsBefore = assembly.governor().counts().listRefusals();

            assertThatThrownBy(() -> assembly.nodeStore().list("bins/cluster-a/data/", null, 10))
                    .as("the node's own store is governed")
                    .isInstanceOf(GovernorRefusedException.class);

            assertThat(assembly.governor().counts().listRefusals() - refusalsBefore)
                    .as("the refusal is counted by the governor").isEqualTo(1);
            assertThat(assembly.storeCounts().lists() - listsBefore)
                    .as("⚠️ AND NOT BY THE REQUEST COUNTER: it never reached the store")
                    .isZero();
        }
    }
}
