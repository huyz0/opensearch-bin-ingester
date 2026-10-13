// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.ingest.AppendResult;
import io.github.huyz0.os.biningester.security.Principal;
import io.github.huyz0.os.biningester.sequencer.LocalSequencer;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * A stream switched to the default path while it holds a fast tail (M13.34r;
 * ADR-0081 §8): the default-path run pauses it, the tail is decided and
 * uploaded, and the run is committed AFTER it -- never at offsets the fast
 * cursor had already assigned, which before this the default path took.
 */
@Timeout(value = 120, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class FastSwitchToDefaultTest {

    private static final String UUID_B64 = "nVzgup36TLqWp7VBBREj1w";
    private static final Principal PRODUCER =
            new Principal("cluster-a", "producer-1", Set.of("orders"));
    private static final RunKey S = RunKey.ofIndexUuid(UUID_B64, 0);

    @TempDir
    Path dir;

    private IngesterNode node;

    @AfterEach
    void stop() throws Exception {
        if (node != null) {
            node.close();
        }
    }

    private IngesterNode startOnAFreePort() throws Exception {
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
            settings.put(ServerProperties.POD_ID, "pod1");
            settings.put(ServerProperties.POD_UID, "uid-pod1");
            settings.put(ServerProperties.PEER_TLS, "off");
            settings.put(ServerProperties.PEER_PORT, Integer.toString(peerPort));
            settings.put(ServerProperties.POD_AZ, "az-a");
            settings.put(ServerProperties.TRUST_DOMAIN, "cluster-a");
            settings.put(ServerProperties.PREFIX, "bins/cluster-a");
            settings.put(ServerProperties.STORE_KIND, "local-fs");
            settings.put(ServerProperties.STORE_ROOT, dir.resolve("store").toString());
            settings.put(ServerProperties.ENDPOINT, "http://localhost:" + peerPort);
            settings.put(ServerProperties.HTTP_PORT, Integer.toString(port));
            settings.put(ServerProperties.PRODUCER_SUBJECT, "producer-1");
            settings.put(ServerProperties.PRODUCER_ALLOWED_INDICES, "orders");
            settings.put(ServerProperties.FAST_JOURNAL_DIR, dir.resolve("journal").toString());
            try {
                return IngesterNode.start(ServerProperties.parse(settings), Clock.systemUTC());
            } catch (RuntimeException taken) {
                last = taken;
            }
        }
        throw last;
    }

    private LocalSequencer chain() {
        return LocalSequencer.underneath(node.assembly().heldTerm()).orElseThrow();
    }

    private AppendResult append(String id) throws Exception {
        return append(0, id);
    }

    private AppendResult append(int partition, String id) throws Exception {
        return node.assembly().ingest().append(PRODUCER, "orders", partition,
                List.of(new SegmentRecord(id, OpType.INDEX, OptionalLong.empty(),
                        new byte[] {1}))::forEach);
    }

    /** The delta that committed {@code stream}'s run at {@code first}, and its segment. */
    private io.github.huyz0.os.biningester.format.CommitDelta deltaOf(RunKey stream, long first) {
        return chain().chain().snapshot().deltas().stream()
                .filter(d -> d.segments().stream().anyMatch(sg -> sg.runs().stream()
                        .anyMatch(run -> run.key().equals(stream) && run.firstOffset() == first)))
                .findFirst().orElseThrow();
    }

    @Test
    void aDEFAULTRunAfterTheSwitchIsCommittedAfterTheFastTailNeverOverIt() throws Exception {
        node = startOnAFreePort();
        // a minute's flush_timer: once the term's first upload is past, a fast
        // entry stays exposed and uncommitted -- the tail the switch must handle
        node.assembly().catalog().register(new IndexRegistration(UUID_B64, "orders",
                List.of(), 1, 1, 1, 1).withFastSettings(60_000, true, 1));
        await().atMost(Duration.ofSeconds(30)).until(() ->
                chain().fastTerm().map(t -> t.fastHere(S)).orElse(false));
        AppendResult first = append("f1");
        await().atMost(Duration.ofSeconds(30)).until(() ->
                chain().committedNext(S) > first.lastOffset());
        AppendResult tail = append("f2");
        assertThat(chain().committedNext(S)).as("the premise: an uncommitted fast tail")
                .isLessThanOrEqualTo(tail.firstOffset());

        node.assembly().catalog().register(new IndexRegistration(UUID_B64, "orders",
                List.of(), 1, 1, 1, 1));
        await().atMost(Duration.ofSeconds(30)).until(() ->
                !chain().fastTerm().orElseThrow().fastHere(S));
        AppendResult defaulted = append("d1");

        assertThat(defaulted.firstOffset()).as("after the fast tail, never over it")
                .isEqualTo(tail.lastOffset() + 1);
        assertThat(chain().committedNext(S)).isEqualTo(defaulted.lastOffset() + 1);
    }

    @Test
    void aRUNTheSwitchHoldsWaitsAloneTheRequestsOtherStreamCommittedFirst()
            throws Exception {
        // ⚠️ M13.34s (ADR-0081 §8: "No other stream's acks wait"): partition 0
        // holds a fast tail, partition 1 none; written in one flush, 1 is
        // committed in an earlier delta than 0's, of the same segment.
        node = startOnAFreePort();
        RunKey other = RunKey.ofIndexUuid(UUID_B64, 1);
        node.assembly().catalog().register(new IndexRegistration(UUID_B64, "orders",
                List.of(), 2, 2, 1, 1).withFastSettings(60_000, true, 1));
        await().atMost(Duration.ofSeconds(30)).until(() ->
                chain().fastTerm().map(t -> t.fastHere(S)).orElse(false));
        AppendResult first = append("f1");
        await().atMost(Duration.ofSeconds(30)).until(() ->
                chain().committedNext(S) > first.lastOffset());
        AppendResult tail = append("f2");
        node.assembly().catalog().register(new IndexRegistration(UUID_B64, "orders",
                List.of(), 2, 2, 1, 1));
        await().atMost(Duration.ofSeconds(30)).until(() ->
                !chain().fastTerm().orElseThrow().fastHere(S));

        var held = java.util.concurrent.CompletableFuture.supplyAsync(() -> {
            try {
                return append(0, "d0");
            } catch (Exception failed) {
                throw new java.util.concurrent.CompletionException(failed);
            }
        });
        var free = java.util.concurrent.CompletableFuture.supplyAsync(() -> {
            try {
                return append(1, "d1");
            } catch (Exception failed) {
                throw new java.util.concurrent.CompletionException(failed);
            }
        });
        AppendResult zero = held.get(30, java.util.concurrent.TimeUnit.SECONDS);
        AppendResult one = free.get(30, java.util.concurrent.TimeUnit.SECONDS);

        assertThat(zero.firstOffset()).isEqualTo(tail.lastOffset() + 1);
        var heldDelta = deltaOf(S, zero.firstOffset());
        var freeDelta = deltaOf(other, one.firstOffset());
        assertThat(freeDelta.segments().get(0).segmentKey()).as("the premise: one flush")
                .isEqualTo(heldDelta.segments().stream().filter(sg -> sg.runs().stream()
                        .anyMatch(run -> run.key().equals(S))).findFirst().orElseThrow()
                        .segmentKey());
        assertThat(freeDelta.sequence()).as("the free stream committed first")
                .isLessThan(heldDelta.sequence());
    }
}
