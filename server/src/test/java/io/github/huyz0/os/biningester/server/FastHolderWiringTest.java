// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.FastWriteFrame;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.http.HttpFastTransport;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The holder's pieces are the pod's own (M13.28a review round 1, T1-T3): the
 * watch's joins land in the fast disk's terms, a departure closes the fast
 * disk's gate -- the one a REPLICA appends under -- and a node routes a
 * REPLICA to its holder rather than answering 501.
 */
class FastHolderWiringTest {

    @TempDir
    Path dir;

    private IngesterNode node;

    @AfterEach
    void stop() throws Exception {
        if (node != null) {
            node.close();
        }
    }

    @Test
    void theWATCHsJoinsLandInTheDisksTerms() throws Exception {
        MemoryBinStore store = FastDepartureTest.leaseHeldBy("uid-l");
        long epoch = FastDepartureTest.read(store).epoch();
        ServerConfig config = FastDepartureTest.config();
        try (FastDisk disk = FastDepartureTest.journaledDisk(store)) {
            FastPeer.joiner(config, disk, (endpoint, frame) -> FastFrame.encode(epoch, "uid-l",
                    config.podUid(), new FastFrame.Joined(0, FastFrame.HeldStatus.NONE)))
                    .join(epoch, "uid-l", "http://l:1");

            assertThat(disk.joined().leaderOf(epoch)).contains("uid-l");
        }
    }

    @Test
    void aDEPARTUREClosesTheDisksGate() throws Exception {
        MemoryBinStore store = FastDepartureTest.leaseHeldBy("uid-l");
        try (FastDisk disk = FastDepartureTest.journaledDisk(store)) {
            FastDepartureTest.Time time = new FastDepartureTest.Time();
            FastDeparture.depart(FastDepartureTest.config(), () -> FastDepartureTest.read(store),
                    disk, new FastDepartureTest.Leader(), false, Duration.ofSeconds(10),
                    time.nanos::get, interval -> time.nanos.addAndGet(interval.toNanos()));

            assertThat(disk.gate().accepting()).as("no REPLICA lands after it").isFalse();
        }
    }

    @Test
    void aNODERoutesAReplicaToItsHolder() throws Exception {
        Map<String, String> settings = new HashMap<>();
        settings.put(ServerProperties.POD_ID, "pod1");
        settings.put(ServerProperties.POD_UID, "uid-pod1");
        settings.put(ServerProperties.PEER_TLS, "off");
        settings.put(ServerProperties.PEER_PORT, "0");
        settings.put(ServerProperties.POD_AZ, "az-a");
        settings.put(ServerProperties.TRUST_DOMAIN, "cluster-a");
        settings.put(ServerProperties.PREFIX, "bins/cluster-a");
        settings.put(ServerProperties.STORE_KIND, "memory");
        settings.put(ServerProperties.ENDPOINT, "http://localhost:0");
        settings.put(ServerProperties.HTTP_PORT, "0");
        settings.put(ServerProperties.PRODUCER_SUBJECT, "producer-1");
        settings.put(ServerProperties.PRODUCER_ALLOWED_INDICES, "logs");
        settings.put(ServerProperties.FAST_JOURNAL_DIR, dir.resolve("journal").toString());
        node = IngesterNode.start(ServerProperties.parse(settings), Clock.systemUTC());
        Roster.Incarnation leader = new Roster.Incarnation("l", "uid-l", "az-b", "");
        FastWriteFrame.Replica replica = new FastWriteFrame.Replica(List.of(
                new FastJournalRecord.Entry(1, new io.github.huyz0.os.biningester.format.RunKey(
                        new UUID(1, 1), 0), 0, 2, 0,
                        new FastJournalRecord.IdempotencyKey("w", new UUID(9, 9), 1),
                        List.of(new SegmentRecord("d", OpType.INDEX, OptionalLong.empty(),
                                new byte[] {1})))));

        FastFrame.Body answer = FastFrame.decode(new HttpFastTransport(Duration.ofSeconds(5),
                CrossAzBytes.untracked()).exchange("http://localhost:" + node.peerPort(),
                FastFrame.encode(1, leader.podUid(), "uid-pod1", replica))).body();

        // ⚠️ REACHED THE HOLDER: an unrouted kind gets a 501 and no answer.
        assertThat(((FastFrame.Refused) answer).reason())
                .as("this pod joined no term of that leader")
                .isEqualTo(FastFrame.Reason.NOT_ROSTERED);
    }
}
