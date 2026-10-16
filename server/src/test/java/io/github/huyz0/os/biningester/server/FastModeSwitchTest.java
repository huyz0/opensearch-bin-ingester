// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import io.github.huyz0.os.biningester.format.SegmentReader;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.ingest.AppendResult;
import io.github.huyz0.os.biningester.security.Principal;
import io.github.huyz0.os.biningester.sequencer.LocalSequencer;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Switching {@code wal} on a live index under skew (SPEC criterion 15,
 * test-plan row 15; ADR-0081 §8; M13.34y): two pods hold the index in
 * different modes for a while, in each direction, while a writer on each
 * never stops -- and every offset of the stream is committed once and
 * contiguously, every acked append's offset holds its record, and every
 * record committed is one a producer sent. The leader is seen in each mode,
 * the one switched from and the one switched to, with acks in both.
 *
 * <p>⚠️ A RECORD MAY BE COMMITTED TWICE, AND THAT IS THE CONTRACT (ADR-0081
 * §2): a fast write whose EXPOSED did not arrive in time is re-sent and
 * assigned anew while its first batch may still be exposed, and the duplicate
 * is removed by {@code _id}, as the default path's retries are. Exactly once
 * by record identity is the split segment's (M13.34z).
 */
@Timeout(value = 180, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class FastModeSwitchTest {

    private static final String UUID_B64 = "nVzgup36TLqWp7VBBREj1w";
    private static final RunKey S = RunKey.ofIndexUuid(UUID_B64, 0);
    private static final Principal PRODUCER =
            new Principal("cluster-a", "producer-1", Set.of("orders"));
    private static final IndexRegistration DEFAULT = new IndexRegistration(UUID_B64, "orders",
            List.of(), 1, 1, 1, 1);
    private static final IndexRegistration FAST = DEFAULT.withFastSettings(250, true, 1);

    @TempDir
    Path dir;

    private final List<IngesterNode> nodes = new ArrayList<>();
    private final List<Writer> writers = new ArrayList<>();

    @AfterEach
    void stop() throws Exception {
        // ⚠️ its review round 1, T3: halted on a failed run too, before the nodes close.
        for (Writer writer : writers) {
            writer.halt();
        }
        for (IngesterNode node : nodes.reversed()) {
            node.close();
        }
    }

    private IngesterNode start(String pod, String az) throws Exception {
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
            settings.put(ServerProperties.POD_UID, "uid-" + pod);
            settings.put(ServerProperties.PEER_TLS, "off");
            settings.put(ServerProperties.PEER_PORT, Integer.toString(peerPort));
            settings.put(ServerProperties.POD_AZ, az);
            settings.put(ServerProperties.TRUST_DOMAIN, "cluster-a");
            settings.put(ServerProperties.PREFIX, "bins/cluster-a");
            settings.put(ServerProperties.STORE_KIND, "local-fs");
            settings.put(ServerProperties.STORE_ROOT, dir.resolve("store").toString());
            settings.put(ServerProperties.ENDPOINT, "http://localhost:" + peerPort);
            settings.put(ServerProperties.HTTP_PORT, Integer.toString(port));
            settings.put(ServerProperties.LEASE_RENEW, "PT1S");
            settings.put(ServerProperties.PRODUCER_SUBJECT, "producer-1");
            settings.put(ServerProperties.PRODUCER_ALLOWED_INDICES, "orders");
            settings.put(ServerProperties.FAST_JOURNAL_DIR,
                    dir.resolve("journal-" + pod).toString());
            try {
                IngesterNode node = IngesterNode.start(ServerProperties.parse(settings),
                        Clock.systemUTC());
                nodes.add(node);
                return node;
            } catch (RuntimeException taken) {
                last = taken;
            }
        }
        throw last;
    }

    /** A producer appending one record at a time through {@code node} until stopped. */
    private static final class Writer implements Runnable {
        final IngesterNode node;
        final String prefix;
        final AtomicBoolean running = new AtomicBoolean(true);
        final Map<String, Long> acked = new ConcurrentHashMap<>();
        final Set<String> failed = ConcurrentHashMap.newKeySet();
        final AtomicInteger next = new AtomicInteger();
        Thread thread;

        Writer(IngesterNode node, String prefix) {
            this.node = node;
            this.prefix = prefix;
        }

        Writer begin() {
            thread = Thread.ofVirtual().start(this);
            return this;
        }

        @Override
        public void run() {
            while (running.get()) {
                String id = prefix + next.getAndIncrement();
                try {
                    AppendResult got = node.assembly().ingest().append(PRODUCER, "orders", 0,
                            sink -> sink.accept(new SegmentRecord(id, OpType.INDEX,
                                    OptionalLong.empty(), new byte[] {1})));
                    acked.put(id, got.firstOffset());
                } catch (Exception refused) {
                    failed.add(id);
                }
            }
        }

        void halt() throws InterruptedException {
            running.set(false);
            thread.join(Duration.ofSeconds(60));
        }
    }

    private LocalSequencer chainOf(IngesterNode leader) {
        return LocalSequencer.underneath(leader.assembly().heldTerm()).orElseThrow();
    }

    /** Every committed record of {@code S}, by offset: read back from its segment. */
    private Map<Long, String> committed(IngesterNode leader) throws Exception {
        Map<Long, String> byOffset = new TreeMap<>();
        for (CommitDelta delta : chainOf(leader).chain().snapshot().deltas()) {
            for (SegmentCommit segment : delta.segments()) {
                for (RunCommit run : segment.runs()) {
                    if (!run.key().equals(S)) {
                        continue;
                    }
                    byte[] bytes;
                    try (var in = leader.assembly().store().get(segment.segmentKey())) {
                        bytes = in.readAllBytes();
                    }
                    SegmentReader reader = SegmentReader.open(bytes);
                    List<SegmentRecord> records = reader.read(reader.find(S).orElseThrow());
                    for (int i = 0; i < run.recordCount(); i++) {
                        String prior = byOffset.put(run.firstOffset() + i, records.get(i).id());
                        assertThat(prior).as("offset %d committed twice",
                                run.firstOffset() + i).isNull();
                    }
                }
            }
        }
        return byOffset;
    }

    private void switchesUnderSkew(IndexRegistration from, IndexRegistration to,
            boolean leaderFirst) throws Exception {
        IngesterNode leader = start("pod1", "az-a");
        IngesterNode follower = start("pod2", "az-b");
        leader.assembly().catalog().register(from);
        follower.assembly().catalog().register(from);
        Writer onLeader = new Writer(leader, "l").begin();
        Writer onFollower = new Writer(follower, "f").begin();
        writers.add(onLeader);
        writers.add(onFollower);
        await().atMost(Duration.ofSeconds(60)).until(() ->
                onLeader.acked.size() >= 20 && onFollower.acked.size() >= 20);
        assertThat(leader.assembly().heldTerm()).as("the premise: pod1 leads").isNotNull();
        assertThat(chainOf(leader).fastTerm().orElseThrow().fastHere(S))
                .as("the premise: the mode switched from").isEqualTo(from == FAST);

        (leaderFirst ? leader : follower).assembly().catalog().register(to);
        int leaderAt = onLeader.acked.size();
        int followerAt = onFollower.acked.size();
        await().atMost(Duration.ofSeconds(60)).until(() ->
                onLeader.acked.size() >= leaderAt + 20
                        && onFollower.acked.size() >= followerAt + 20);
        (leaderFirst ? follower : leader).assembly().catalog().register(to);
        // ⚠️ THE SWITCH HAPPENED (its review round 1, T1): the leader's term
        // reaches the mode switched to, and both producers are acked in it.
        await().atMost(Duration.ofSeconds(60)).until(() ->
                chainOf(leader).fastTerm().orElseThrow().fastHere(S) == (to == FAST));
        int leaderThen = onLeader.acked.size();
        int followerThen = onFollower.acked.size();
        await().atMost(Duration.ofSeconds(60)).until(() ->
                onLeader.acked.size() >= leaderThen + 20
                        && onFollower.acked.size() >= followerThen + 20);
        onLeader.halt();
        onFollower.halt();

        Map<String, Long> acked = new HashMap<>(onLeader.acked);
        acked.putAll(onFollower.acked);
        // ⚠️ A DEFERRED APPEND (a 202, no offset) lands once its inbox drains.
        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(500))
                .until(() -> committed(leader).values().containsAll(acked.keySet()));
        Map<Long, String> committed = committed(leader);

        assertThat(committed.keySet()).as("every offset once, contiguous from 0")
                .containsExactlyElementsOf(java.util.stream.LongStream
                        .range(0, committed.size()).boxed().toList());
        acked.forEach((id, offset) -> {
            if (offset >= 0) {
                assertThat(committed.get(offset)).as("acked offset %d holds its record %s",
                        offset, id).isEqualTo(id);
            }
        });
        Set<String> sent = new java.util.HashSet<>(acked.keySet());
        sent.addAll(onLeader.failed);
        sent.addAll(onFollower.failed);
        assertThat(sent).as("every committed record was sent by a producer")
                .containsAll(committed.values());
    }

    @Test
    void aSWITCHToTheDefaultPathLeaderFirstKeepsEveryOffsetOnceAndEveryAckTrue() throws Exception {
        switchesUnderSkew(FAST, DEFAULT, true);
    }

    @Test
    void aSWITCHToTheDefaultPathFollowerFirstKeepsEveryOffsetOnceAndEveryAckTrue() throws Exception {
        switchesUnderSkew(FAST, DEFAULT, false);
    }

    @Test
    void aSWITCHToFastModeLeaderFirstKeepsEveryOffsetOnceAndEveryAckTrue() throws Exception {
        switchesUnderSkew(DEFAULT, FAST, true);
    }

    @Test
    void aSWITCHToFastModeFollowerFirstKeepsEveryOffsetOnceAndEveryAckTrue() throws Exception {
        switchesUnderSkew(DEFAULT, FAST, false);
    }
}
