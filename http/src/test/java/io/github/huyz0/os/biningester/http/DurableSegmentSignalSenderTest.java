// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.format.DurableSegmentSignalFrame;
import io.github.huyz0.os.biningester.format.SegmentKey;
import io.github.huyz0.os.biningester.ingest.AzPeers;
import io.github.huyz0.os.biningester.ingest.Peer;
import io.github.huyz0.os.biningester.ingest.PeerRing;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class DurableSegmentSignalSenderTest {
    @Test
    void sendsOneBoundedFrameToTheOwnerInEachRemoteAzAndCountsTheBytes() {
        CrossAzBytes bytes = new CrossAzBytes("az-a");
        List<String> posted = new ArrayList<>();
        DurableSegmentSignalSender sender = new DurableSegmentSignalSender(bytes, 8080,
                (endpoint, body) -> posted.add(endpoint + "#" + body.length));
        var endpoints = List.of(
                new EndpointSliceView.Endpoint("writera", "10.0.0.1", "az-a"),
                new EndpointSliceView.Endpoint("nodeb1", "10.0.1.1", "az-b"),
                new EndpointSliceView.Endpoint("nodeb2", "10.0.1.2", "az-b"),
                new EndpointSliceView.Endpoint("nodeb3", "10.0.1.3", "az-b"),
                new EndpointSliceView.Endpoint("nodec1", "10.0.2.1", "az-c"),
                new EndpointSliceView.Endpoint("nodec2", "10.0.2.2", "az-c"));
        var azB = new AzPeers("az-b", List.of(
                new Peer("nodeb1", "http://10.0.1.1:8080", "az-b"),
                new Peer("nodeb2", "http://10.0.1.2:8080", "az-b"),
                new Peer("nodeb3", "http://10.0.1.3:8080", "az-b")));
        var azC = new AzPeers("az-c", List.of(
                new Peer("nodec1", "http://10.0.2.1:8080", "az-c"),
                new Peer("nodec2", "http://10.0.2.2:8080", "az-c")));
        String firstB = azB.peers().get(0).endpoint();
        String firstC = azC.peers().get(0).endpoint();
        String key = null;
        List<String> expectedOwners = List.of();
        for (long sequence = 1; sequence <= 64; sequence++) {
            String candidate = new SegmentKey("bins/c", 1_700_000_000_000L, "writera",
                    sequence, 48).key();
            List<String> owners = List.of(
                    PeerRing.ownerOf(candidate, azB).orElseThrow().endpoint(),
                    PeerRing.ownerOf(candidate, azC).orElseThrow().endpoint());
            if (!owners.contains(firstB) && !owners.contains(firstC)) {
                key = candidate;
                expectedOwners = owners;
                break;
            }
        }
        assertThat(key).as("fixture must distinguish ring ownership from first-peer selection")
                .isNotNull();
        var frame = new DurableSegmentSignalFrame("writera", "az-a", key);

        List<String> targets = sender.send(frame, endpoints);

        assertThat(targets).containsExactlyElementsOf(expectedOwners);
        assertThat(posted).hasSize(2).allSatisfy(value ->
                assertThat(value).endsWith("#" + frame.encode().length));
        assertThat(targets).noneMatch(target -> target.contains("10.0.0.1"));
        long sent = bytes.crossAzBytes(CrossAzBytes.Transport.DURABLE_SEGMENT_SIGNAL);
        assertThat(sent).isEqualTo((long) frame.encode().length * 2);
        assertThat((double) (1550L * 2) / (8L * 1024 * 1024) * 100)
                .isLessThanOrEqualTo(0.037);
    }

    @Test
    void aFailedHintDoesNotStopAttemptingOtherAzOwners() {
        List<String> attempted = new ArrayList<>();
        DurableSegmentSignalSender sender = new DurableSegmentSignalSender(
                new CrossAzBytes("az-a"), 8080, (endpoint, body) -> {
                    attempted.add(endpoint);
                    if (endpoint.contains("10.0.1.")) {
                        throw new java.io.IOException("unreachable");
                    }
                });
        var endpoints = List.of(
                new EndpointSliceView.Endpoint("writera", "10.0.0.1", "az-a"),
                new EndpointSliceView.Endpoint("nodeb", "10.0.1.1", "az-b"),
                new EndpointSliceView.Endpoint("nodec", "10.0.2.1", "az-c"));
        String key = new SegmentKey("bins/c", 1_700_000_000_000L, "writera", 1, 48).key();

        assertThat(sender.send(new DurableSegmentSignalFrame("writera", "az-a", key), endpoints))
                .hasSize(2);
        assertThat(attempted).hasSize(2);
    }
}
