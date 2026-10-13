// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.security.Principal;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A fleet with no {@code wal=true} index (SPEC test-plan row 19; M13.86;
 * ADR-0081 §1, §9): a crash takeover, a graceful takeover and a rolling
 * restart each make exactly the store writes those sections list -- the
 * roster, {@code LATEST}, a fence and a close where a term was left open, a
 * {@code DEPARTED} per departure, a join write per term a pod joins -- and
 * none beyond: no fast frame carries a record, and an idle fleet asks the
 * store nothing of fast mode.
 */
class FastWalFalseFleetTest {

    private static final String PREFIX = "bins/cluster-a";
    private static final Principal PRODUCER =
            new Principal("cluster-a", "producer-1", Set.of("logs"));
    private static final IndexRegistration LOGS =
            IndexRegistration.unsplit("Qk1rUnfdRyCrk1L0DLaXEg", "logs", 1);

    @TempDir
    Path dir;

    private final FleetStoreTrace trace = new FleetStoreTrace();
    private final Map<String, IngesterNode> pods = new LinkedHashMap<>();
    private final List<String> uids = new java.util.ArrayList<>();
    /** A crashed pod's close, which fails on its own thread. */
    private Thread crashing;

    @AfterEach
    void stop() throws Exception {
        for (IngesterNode node : pods.values()) {
            node.close();
        }
        if (crashing != null) {
            crashing.join(Duration.ofSeconds(60));
        }
    }

    private IngesterNode start(String pod, String uid, String az) throws Exception {
        RuntimeException last = null;
        for (int attempt = 0; attempt < 5; attempt++) {
            int port;
            int peerPort;
            try (ServerSocket probe = new ServerSocket(0);
                    ServerSocket peerProbe = new ServerSocket(0)) {
                port = probe.getLocalPort();
                peerPort = peerProbe.getLocalPort();
            }
            Map<String, String> settings = new HashMap<>();
            settings.put(ServerProperties.POD_ID, pod);
            settings.put(ServerProperties.POD_UID, uid);
            settings.put(ServerProperties.PEER_TLS, "off");
            settings.put(ServerProperties.PEER_PORT, Integer.toString(peerPort));
            settings.put(ServerProperties.POD_AZ, az);
            settings.put(ServerProperties.TRUST_DOMAIN, "cluster-a");
            settings.put(ServerProperties.PREFIX, PREFIX);
            settings.put(ServerProperties.STORE_KIND, "memory");
            settings.put(ServerProperties.ENDPOINT, "http://localhost:" + peerPort);
            settings.put(ServerProperties.HTTP_PORT, Integer.toString(port));
            settings.put(ServerProperties.PRODUCER_SUBJECT, "producer-1");
            settings.put(ServerProperties.PRODUCER_ALLOWED_INDICES, "logs");
            settings.put(ServerProperties.LEASE_TTL, "PT3S");
            settings.put(ServerProperties.LEASE_RENEW, "PT1S");
            // ⚠️ JOURNALED PODS (review round 1, T1): a fleet with no fast
            // index that journals an entry is what this must catch, and a
            // diskless pod could not. Its epoch file is kept beside the journal
            // (ADR-0081 section 3); that such a fleet needs no disk at all is
            // FastDiskTest's diskless case.
            settings.put(ServerProperties.FAST_JOURNAL_DIR, journal(uid).toString());
            try {
                IngesterNode node = IngesterNode.start(ServerProperties.parse(settings),
                        Clock.systemUTC(), SequencerAssembly.following(Clock.systemUTC()),
                        Optional::empty, List.of(), Optional.empty(), trace.forPod(pod));
                node.assembly().catalog().register(LOGS);
                pods.put(pod, node);
                uids.add(uid);
                return node;
            } catch (RuntimeException taken) {
                last = taken;
            }
        }
        throw last;
    }

    private Roster latest() throws Exception {
        var store = trace.store();
        long epoch;
        try (var in = store.get(Roster.latestKey(PREFIX))) {
            epoch = Roster.decodeLatest(in.readAllBytes());
        }
        try (var in = store.get(Roster.key(PREFIX, epoch))) {
            return Roster.decode(in.readAllBytes());
        }
    }

    /**
     * Default-path writes through {@code pod} until a term above {@code above}
     * is in {@code LATEST}: a commit is what elects a leader into a vacancy,
     * and one made before the dead term's lease ran out is deferred instead.
     */
    private void writeUntilATermAbove(String pod, long above) {
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500))
                .ignoreExceptions().until(() -> {
                    pods.get(pod).assembly().ingest().append(PRODUCER, "logs", 0,
                            sink -> sink.accept(new SegmentRecord("doc", OpType.INDEX,
                                    OptionalLong.empty(), new byte[] {1})));
                    return latest().epoch() > above;
                });
    }

    private void fleetOfThree() throws Exception {
        start("pod1", "uid-pod1", "az-a");
        start("pod2", "uid-pod2", "az-b");
        start("pod3", "uid-pod3", "az-c");
        await().atMost(Duration.ofSeconds(20)).until(() -> {
            Roster r = latest();
            return r.member("uid-pod2").isPresent() && r.member("uid-pod3").isPresent();
        });
    }

    private Path journal(String uid) {
        return dir.resolve("journal-" + uid);
    }

    /** No pod, of every incarnation started, journaled an entry. */
    private void noEntryJournaled() throws Exception {
        for (String uid : uids) {
            assertThat(Files.size(journal(uid).resolve(FastDisk.JOURNAL)))
                    .as("%s journaled nothing", uid).isZero();
        }
    }

    private void noFastDataSent() {
        for (IngesterNode node : pods.values()) {
            CrossAzBytes bytes = node.crossAzBytes();
            assertThat(bytes.crossAzBytes(CrossAzBytes.Transport.FAST_DATA)
                    + bytes.sameAzBytes(CrossAzBytes.Transport.FAST_DATA))
                    .as("no fast frame carries a record").isZero();
        }
    }

    /** Nothing of fast mode is asked of the store while nothing happens. */
    private void idleFleetAsksNothing() {
        trace.clear();
        await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(5))
                .until(() -> trace.requests().isEmpty());
    }

    @Test
    void aCRASHTakeoverFencesAndClosesTheDeadTermAndAJoinIsOneWrite() throws Exception {
        fleetOfThree();
        long dead = latest().epoch();
        trace.clear();

        trace.kill("pod1");
        IngesterNode crashed = pods.remove("pod1");
        crashing = Thread.ofVirtual().start(() -> {
            try {
                crashed.close();
            } catch (Exception ignored) {
                // a dead pod's close fails: nothing it writes arrives
            }
        });
        writeUntilATermAbove("pod2", dead);
        await().atMost(Duration.ofSeconds(20)).until(() -> latest().member("uid-pod3")
                .isPresent());

        assertThat(trace.writes()).as("ADR-0081 section 1: the dead term fenced and closed, "
                + "the new roster and LATEST, one join").containsExactly(
                "pod2 FENCE 1", "pod2 CREATE 2", "pod2 LATEST 2", "pod2 CLOSE 1",
                "pod2 JOIN pod3 2");
        assertThat(trace.requests()).as("and nothing beyond: the walk's reads, each "
                + "write's read, the joiner's one").containsExactlyInAnyOrder(
                "pod2 stat LATEST", "pod2 get LATEST",
                "pod2 stat 0000000000000001.roster", "pod2 get 0000000000000001.roster",
                "pod2 putIfMatch 0000000000000001.roster",
                "pod2 putIfAbsent 0000000000000002.roster", "pod2 putIfMatch LATEST",
                "pod2 stat 0000000000000001.roster", "pod2 get 0000000000000001.roster",
                "pod2 putIfMatch 0000000000000001.roster",
                "pod3 get 0000000000000002.roster",
                "pod2 stat 0000000000000002.roster", "pod2 get 0000000000000002.roster",
                "pod2 putIfMatch 0000000000000002.roster");
        noFastDataSent();
        noEntryJournaled();
        idleFleetAsksNothing();
    }

    @Test
    void aGRACEFULTakeoverWritesNoFence() throws Exception {
        fleetOfThree();
        long left = latest().epoch();
        trace.clear();

        pods.remove("pod1").close();
        writeUntilATermAbove("pod2", left);
        await().atMost(Duration.ofSeconds(20)).until(() -> latest().member("uid-pod3")
                .isPresent());

        assertThat(trace.writes()).as("ADR-0081 section 9: the leader departed and closed "
                + "its roster in one write; its successor fences nothing").containsExactly(
                "pod1 DEPARTED pod1+CLOSE 1", "pod2 CREATE 2", "pod2 LATEST 2",
                "pod2 JOIN pod3 2");
        assertThat(trace.requests()).as("and nothing beyond").containsExactlyInAnyOrder(
                "pod1 stat 0000000000000001.roster", "pod1 get 0000000000000001.roster",
                "pod1 putIfMatch 0000000000000001.roster", "pod1 get 0000000000000001.roster",
                "pod2 stat LATEST", "pod2 get LATEST",
                "pod2 stat 0000000000000001.roster", "pod2 get 0000000000000001.roster",
                "pod2 putIfAbsent 0000000000000002.roster", "pod2 putIfMatch LATEST",
                "pod3 get 0000000000000002.roster",
                "pod2 stat 0000000000000002.roster", "pod2 get 0000000000000002.roster",
                "pod2 putIfMatch 0000000000000002.roster");
        noFastDataSent();
        noEntryJournaled();
        idleFleetAsksNothing();
    }

    @Test
    void aROLLINGRestartDepartsAndJoinsEachPodOncePerTerm() throws Exception {
        fleetOfThree();
        long first = latest().epoch();
        trace.clear();

        for (String pod : List.of("pod3", "pod2")) {
            String az = pod.equals("pod3") ? "az-c" : "az-b";
            pods.remove(pod).close();
            start(pod, "uid-" + pod + "-b", az);
            await().atMost(Duration.ofSeconds(20)).until(() -> latest().member("uid-" + pod
                    + "-b").isPresent());
        }
        pods.remove("pod1").close();
        writeUntilATermAbove("pod2", first);
        start("pod1", "uid-pod1-b", "az-a");
        await().atMost(Duration.ofSeconds(20)).until(() -> {
            Roster r = latest();
            return r.member("uid-pod1-b").isPresent() && r.member("uid-pod3-b").isPresent();
        });

        // ⚠️ BY WHAT EACH WRITE CHANGED, joins counted however batched: each
        // pod departs once and joins once per term, never once per JOIN frame.
        assertThat(trace.writes().stream()
                .flatMap(w -> {
                    String[] parts = w.split(" ", 2);
                    String epoch = parts[1].substring(parts[1].lastIndexOf(' '));
                    return java.util.Arrays.stream(parts[1].substring(0,
                            parts[1].lastIndexOf(' ')).split("[+]"))
                            .map(change -> parts[0] + " " + change + epoch);
                }).toList()).containsExactlyInAnyOrder(
                "pod1 DEPARTED pod3 1", "pod1 JOIN pod3 1",
                "pod1 DEPARTED pod2 1", "pod1 JOIN pod2 1",
                "pod1 DEPARTED pod1 1", "pod1 CLOSE 1",
                "pod2 CREATE 2", "pod2 LATEST 2", "pod2 JOIN pod3 2", "pod2 JOIN pod1 2");
        noFastDataSent();
        noEntryJournaled();
        idleFleetAsksNothing();
    }
}
