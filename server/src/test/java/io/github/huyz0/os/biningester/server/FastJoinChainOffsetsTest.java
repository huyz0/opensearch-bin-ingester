// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.SequencerTransport;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The JOINED status is judged over the term's own chain: a group below a
 * stream's committed next offset is COMMITTED and released below it (ADR-0081
 * §5 step 7; M13.27j review round 3, T2; M13.27o).
 */
class FastJoinChainOffsetsTest {

    private static final String INDEX = "logs";
    private static final UUID INDEX_ID = new UUID(7, 7);
    private static final Roster.Incarnation POD =
            new Roster.Incarnation("p", "uid-p", "az-b", "http://p:1");

    private static String base64(UUID uuid) {
        ByteBuffer b = ByteBuffer.allocate(16);
        b.putLong(uuid.getMostSignificantBits());
        b.putLong(uuid.getLeastSignificantBits());
        return Base64.getUrlEncoder().withoutPadding().encodeToString(b.array());
    }

    private static ServerConfig config() {
        return new ServerConfig("pod1", "az-a", "cluster-a", "bins/cluster-a",
                new StoreConfig("memory", Optional.empty()),
                Duration.ofSeconds(10), Duration.ofSeconds(3), "http://pod1:8080",
                IngestConfig.defaults("cluster-a"), 0, "producer-1", java.util.Set.of(INDEX),
                RetentionConfig.defaults(), Optional.empty(), "uid-pod1",
                io.github.huyz0.os.biningester.ingest.CostTopKReporter.DEFAULT_INTERVAL,
                io.github.huyz0.os.biningester.ingest.IndexQuotas.Config.none(), false,
                Optional.empty());
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

    @Test
    void aGROUPBelowTheChainsCommittedOffsetIsCommittedAndReleasedBelowIt() throws Exception {
        ServerConfig config = config();
        try (Assembly assembly = Assembly.open(config, new MemoryBinStore(), noPeers(),
                Clock.systemUTC())) {
            String uuid = base64(INDEX_ID);
            assembly.catalog().register(new IndexRegistration(uuid, INDEX, List.of(), 4, 4, 1, 1));
            for (String id : List.of("a", "b", "c")) {
                assembly.ingest().append(config.principal(), INDEX, 0, sink -> sink.accept(
                        new SegmentRecord(id, OpType.INDEX, OptionalLong.of(1),
                                id.getBytes(StandardCharsets.UTF_8))));
            }
            List<FastFrame.HeldStream> streams = new ArrayList<>();
            for (int partition = 0; partition < 4; partition++) {
                streams.add(new FastFrame.HeldStream(RunKey.ofIndexUuid(uuid, partition),
                        List.of(new FastFrame.HeldGroup(1, 0, 0, 0))));
            }

            FastFrame.Joined joined = (FastFrame.Joined) FastJoins.answer(assembly.heldTerm(),
                    new FastFrame.Header(FastFrame.KIND_JOIN, 1, POD.podUid(), "uid-pod1"),
                    new FastFrame.Join(POD, new FastFrame.Held(streams)));

            assertThat(joined.status().streams()).extracting(s -> s.releaseBelow())
                    .as("the three records' stream is committed through 3, the rest at 0")
                    .containsExactlyInAnyOrder(3L, 0L, 0L, 0L);
            assertThat(joined.status().streams()).filteredOn(s -> s.releaseBelow() == 3)
                    .flatExtracting(s -> s.groups()).extracting(g -> g.status())
                    .containsExactly(FastFrame.Status.COMMITTED);
        }
    }
}
