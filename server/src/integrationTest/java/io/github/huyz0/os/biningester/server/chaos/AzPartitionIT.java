// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server.chaos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.github.huyz0.os.biningester.binstore.backend.S3Fixture;
import io.github.huyz0.os.biningester.format.CommitRequestFrame;
import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.format.RunKey;
import io.helidon.webclient.api.WebClient;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * An AZ partition: a pod cut off from its sequencer but not from the store
 * engages the inbox, and the heal applies it with no offset assigned twice
 * (M8.14, M8's criteria 11 and 17 as amended by ADR-0058, FR-11, NFR-3).
 *
 * <p>⚠️ **THE CUT HOLDS BYTES, IT DOES NOT DROP THEM** ({@link ChaosProxy}):
 * the forward the follower sent into the cut waits out the peer-commit timeout
 * and defers -- and at the heal those held bytes still reach the leader. So
 * the same flush can land TWICE, once as that late forward and once from the
 * drain, and the only thing between that and a double assignment is the
 * dedupe window. That is the case this row exists for.
 *
 * <p>⚠️ **THREE INDICES IN ONE FLUSH**, because the one-index fixture every
 * other test uses cannot see an intent written per index (criterion 17): an
 * intent here must name streams of more than one index.
 */
@Timeout(value = 600, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class AzPartitionIT {

    private static final List<String> INDICES = List.of("logs", "metrics", "audit");

    @TempDir
    Path dir;

    @BeforeAll
    static void container() {
        assumeTrue(S3Fixture.dockerAvailable(), "no Docker daemon: this is a chaos suite");
    }

    private static String uuid64(UUID uuid) {
        var buffer = java.nio.ByteBuffer.allocate(16);
        buffer.putLong(uuid.getMostSignificantBits());
        buffer.putLong(uuid.getLeastSignificantBits());
        return java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(buffer.array());
    }

    private static WebClient patient(NodeProcess node) {
        // ⚠️ LONGER THAN THE TTL: an append in the cut waits out the forward.
        return WebClient.builder().baseUri("http://localhost:" + node.port())
                .connectTimeout(Duration.ofSeconds(2)).readTimeout(Duration.ofSeconds(60)).build();
    }

    private static String body(List<String> ids) {
        StringBuilder body = new StringBuilder();
        for (String id : ids) {
            body.append("{\"index\":{\"_id\":\"").append(id)
                    .append("\",\"_version\":1}}\n{\"n\":1}\n");
        }
        return body.toString();
    }

    private static List<String> ids(String tag, int n) {
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            ids.add(tag + "-" + i);
        }
        return ids;
    }

    /** Writes one batch to each index at once, and returns what was acked. */
    private static Set<String> writeAll(NodeProcess node, String tag) throws Exception {
        Set<String> acked = ConcurrentHashMap.newKeySet();
        List<Thread> writers = new ArrayList<>();
        for (String index : INDICES) {
            List<String> batch = ids(tag + "-" + index, 20);
            writers.add(Thread.ofVirtual().start(() -> {
                int status = patient(node).post("/" + index + "/_bulk")
                        .queryParam("partition", "0").submit(body(batch)).status().code();
                if (status == 202) {
                    acked.addAll(batch);
                }
            }));
        }
        for (Thread writer : writers) {
            writer.join(TimeUnit.SECONDS.toMillis(90));
        }
        return acked;
    }

    @Test
    void aPARTITIONEDPodDEFERSToTheInboxAndTheHEALAppliesEveryIntentONCE() throws Exception {
        List<NodeProcess> nodes = new ArrayList<>();
        try (ChaosBucket bucket = ChaosBucket.create()) {
            Map<String, String> settings = new HashMap<>(bucket.nodeSettings());
            settings.put("ingest.interval-floor", "PT0.2S");
            settings.put("producer.allowed-indices", String.join(",", INDICES));
            NodeProcess leader = NodeProcess.start(Files.createDirectories(dir.resolve("l")),
                    "pod0", settings, new NodeProcess.Options(Duration.ZERO, true));
            nodes.add(leader);
            NodeProcess follower = NodeProcess.start(Files.createDirectories(dir.resolve("f")),
                    "pod1", settings);
            nodes.add(follower);
            Map<String, UUID> uuids = new HashMap<>();
            for (String index : INDICES) {
                UUID uuid = UUID.randomUUID();
                uuids.put(index, uuid);
                assertThat(patient(follower).post(io.github.huyz0.os.biningester.http.SubscriptionService.REGISTER_PATH)
                        .submit(new IndexRegistration(uuid64(uuid), index, List.of(), 1, 1, 1, 1)
                                .encode()).status().code()).isEqualTo(204);
            }
            assertThat(bucket.lease().orElseThrow().holderPodId())
                    .as("the premise: pod0 leads").isEqualTo("pod0");

            Set<String> acked = new HashSet<>(writeAll(follower, "before"));
            leader.peers().cut();
            Set<String> duringCut = writeAll(follower, "cut");
            acked.addAll(duringCut);

            List<String> intents = new ArrayList<>();
            for (String key : bucket.keys(ChaosBucket.PREFIX + "/ctl/inbox/0/")) {
                if (key.endsWith(".intent")) {
                    intents.add(key);
                }
            }
            assertThat(intents)
                    .as("the three cut bulk flushes persist at most one intent object each")
                    .hasSizeLessThanOrEqualTo(3);
            assertThat(duringCut)
                    .as("⚠️ THE WRITES IN THE CUT WERE ACKED -- on a durable intent (ADR-0058)")
                    .hasSize(60);
            assertThat(intents).as("the inbox engaged").isNotEmpty();
            boolean spansIndices = false;
            for (String key : intents) {
                Set<UUID> indices = new HashSet<>();
                for (RunKey stream : CommitRequestFrame.decode(bucket.get(key))
                        .recordCounts().keySet()) {
                    indices.add(stream.indexId());
                }
                spansIndices |= indices.size() > 1;
            }
            assertThat(spansIndices)
                    .as("⚠️ AN INTENT NAMES STREAMS OF MORE THAN ONE INDEX: one per pod per "
                            + "flush, never one per index (criterion 17, cost.md rule 6)")
                    .isTrue();

            leader.peers().heal();
            Set<String> after = writeAll(follower, "after");
            assertThat(after).as("the first writes after the heal are acked").hasSize(60);
            acked.addAll(after);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (bucket.keys(ChaosBucket.PREFIX + "/ctl/inbox/0/").stream()
                    .anyMatch(k -> k.endsWith(".intent"))) {
                assertThat(System.nanoTime()).as("the inbox never drained")
                        .isLessThan(deadline);
                Thread.sleep(100);
            }
            // ⚠️ AND IT RETURNS TO NORMAL (criterion 17): a pod still deferring
            // after the heal would keep paying an intent PUT per flush for ever,
            // and every assertion below would stay green.
            Set<String> healed = writeAll(follower, "healed");
            assertThat(healed)
                    .as("⚠️ ACKED, not refused: a pod answering 5xx after the heal writes no "
                            + "intent either")
                    .hasSize(60);
            acked.addAll(healed);
            assertThat(bucket.keys(ChaosBucket.PREFIX + "/ctl/inbox/0/"))
                    .as("⚠️ A FLUSH AFTER THE HEAL WRITES NO INTENT: it forwards again")
                    .noneMatch(k -> k.endsWith(".intent"));
            for (NodeProcess node : nodes) {
                node.terminate();
            }

            ChainAudit audit = ChainAudit.of(bucket);
            assertThat(audit.violations())
                    .as("⚠️ NO DIVERGENCE: no offset assigned twice, the late forward and the "
                            + "drain of the same flush answered once")
                    .isEmpty();
            Map<String, Integer> committed = KillSequencerMidCommitIT.committedIds(bucket,
                    audit.committedSegments());
            Set<String> lost = new HashSet<>(acked);
            lost.removeAll(committed.keySet());
            assertThat(lost).as("⚠️ EVERY ACK -- the deferred ones included -- is committed")
                    .isEmpty();
            assertThat(committed.values())
                    .as("⚠️ AND ONCE: a flush committed both late and drained is a record twice")
                    .allMatch(n -> n == 1);
        } finally {
            nodes.forEach(NodeProcess::close);
        }
    }
}
