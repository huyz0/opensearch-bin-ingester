// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.format.DurableSegmentSignalFrame;
import io.github.huyz0.os.biningester.format.SegmentKey;
import io.github.huyz0.os.biningester.http.DurableSegmentSignalSender;
import io.github.huyz0.os.biningester.http.EndpointSliceView;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * A durable-segment hint is sent to its owner's PEER port (ADR-0084 decision
 * 2; M13.52c review round 1, P1): the route is served on the peer listener
 * only, and a hint to the producer port is answered 404 -- and, its failures
 * swallowed, lost without a word.
 */
class DurableSignalPeerPortTest {

    @Test
    void aHINTGoesToTheOwnersPeerPortNotItsProducerPort() {
        ServerConfig config = new ServerConfig("writera", "az-a", "cluster-a", "bins/cluster-a",
                new StoreConfig("memory", Optional.empty(), Optional.empty(), Optional.empty(),
                        Optional.empty(), false),
                Duration.ofSeconds(10), Duration.ofSeconds(3), "http://localhost:7001",
                IngestConfig.defaults("cluster-a"), 7000, "producer", Set.of("logs"),
                RetentionConfig.defaults(), Optional.empty(), "uid-writera",
                io.github.huyz0.os.biningester.ingest.CostTopKReporter.DEFAULT_INTERVAL,
                io.github.huyz0.os.biningester.ingest.IndexQuotas.Config.none(), false,
                Optional.empty(), PeerConfig.off(7001));
        List<String> posted = new ArrayList<>();
        DurableSegmentSignalSender sender = EndpointMembership.signalSender(config,
                new EndpointSliceView(), CrossAzBytes.untracked(),
                (endpoint, body) -> posted.add(endpoint));

        sender.send(new DurableSegmentSignalFrame("writera", "az-a",
                        new SegmentKey("bins/cluster-a", 1, "writera", 1, 48).key()),
                List.of(new EndpointSliceView.Endpoint("ownerb", "10.0.0.2", "az-b")));

        assertThat(posted).containsExactly("http://10.0.0.2:7001");
    }
}
