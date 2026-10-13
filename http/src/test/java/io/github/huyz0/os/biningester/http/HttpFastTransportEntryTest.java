// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A leader's exposed entry fetched by fast key (M13.30p): its bytes and the
 * epoch its {@code Binstore-Fast-Epoch} header names, nothing for a
 * {@code 404}, and a failure for an answer naming no epoch or any other status.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class HttpFastTransportEntryTest {

    @Test
    void anENTRYIsFetchedWithItsEpochAndA404IsNone() throws Exception {
        Map<String, FastEntryService.Served> held = Map.of("k",
                new FastEntryService.Served(9, new byte[] {1, 2}));
        WebServer server = WebServer.builder().port(0).routing(HttpRouting.builder().register(
                new FastEntryService(key -> Optional.ofNullable(held.get(key)),
                        PeerBinding.off()))).build().start();
        try {
            HttpFastTransport transport = new HttpFastTransport(Duration.ofSeconds(5),
                    CrossAzBytes.untracked());
            String endpoint = "http://localhost:" + server.port();

            Optional<FastEntryService.Served> served = transport.fetchEntry(endpoint, "k", "uid-b");
            Optional<FastEntryService.Served> absent = transport.fetchEntry(endpoint, "x", "uid-b");

            assertThat(served).hasValueSatisfying(s -> {
                assertThat(s.epoch()).isEqualTo(9);
                assertThat(s.bytes()).containsExactly(1, 2);
            });
            assertThat(absent).isEmpty();
            assertThatThrownBy(() -> transport.fetchEntry(endpoint, "k", ""))
                    .as("a 400 is a failure, not an absent entry")
                    .isInstanceOf(IOException.class);
        } finally {
            server.stop();
        }
    }

    @Test
    void anANSWERNamingNoEpochIsAFailure() throws Exception {
        WebServer server = WebServer.builder().port(0).routing(HttpRouting.builder()
                .get(FastEntryService.PATH, (req, res) -> res.send(new byte[] {1})))
                .build().start();
        try {
            assertThatThrownBy(() -> new HttpFastTransport(Duration.ofSeconds(5),
                    CrossAzBytes.untracked()).fetchEntry("http://localhost:" + server.port(),
                            "k", "uid-b"))
                    .isInstanceOf(IOException.class);
        } finally {
            server.stop();
        }
    }

    @Test
    void aFORWARDEDEntryIsCountedAsFastDataAgainstTheAskingPodsZone() throws Exception {
        // ⚠️ M13.30p review round 1, P1: counted where it is sent, as /seg's are.
        CrossAzBytes leaderZone = new CrossAzBytes("az-a");
        WebServer server = WebServer.builder().port(0).routing(HttpRouting.builder().register(
                new FastEntryService(key -> Optional.of(new FastEntryService.Served(9,
                        new byte[] {1, 2, 3})), PeerBinding.off(), leaderZone)))
                .build().start();
        try {
            new HttpFastTransport(Duration.ofSeconds(5), new CrossAzBytes("az-b"))
                    .fetchEntry("http://localhost:" + server.port(), "k", "uid-b");

            assertThat(leaderZone.crossAzBytes(CrossAzBytes.Transport.FAST_DATA)).isEqualTo(3);
            assertThat(leaderZone.unknownPeerBytes()).as("the asking zone named").isZero();
        } finally {
            server.stop();
        }
    }

    @Test
    void aFORWARDEDEntryWithinTheZoneIsCountedSameZone() throws Exception {
        // ⚠️ M13.30p review round 2, T5: an unnamed zone counts cross-AZ too.
        CrossAzBytes leaderZone = new CrossAzBytes("az-a");
        WebServer server = WebServer.builder().port(0).routing(HttpRouting.builder().register(
                new FastEntryService(key -> Optional.of(new FastEntryService.Served(9,
                        new byte[] {1, 2, 3})), PeerBinding.off(), leaderZone)))
                .build().start();
        try {
            new HttpFastTransport(Duration.ofSeconds(5), new CrossAzBytes("az-a"))
                    .fetchEntry("http://localhost:" + server.port(), "k", "uid-b");

            assertThat(leaderZone.sameAzBytes(CrossAzBytes.Transport.FAST_DATA)).isEqualTo(3);
            assertThat(leaderZone.crossAzBytes()).isZero();
        } finally {
            server.stop();
        }
    }

    @Test
    void aBLANKKeyIsUnnamed() throws Exception {
        WebServer server = WebServer.builder().port(0).routing(HttpRouting.builder().register(
                new FastEntryService(key -> Optional.of(new FastEntryService.Served(9,
                        new byte[] {1})), PeerBinding.off()))).build().start();
        try {
            assertThatThrownBy(() -> new HttpFastTransport(Duration.ofSeconds(5),
                    CrossAzBytes.untracked()).fetchEntry("http://localhost:" + server.port(),
                            " ", "uid-b"))
                    .as("a 400, not the entry a lookup of a blank key would find")
                    .isInstanceOf(IOException.class);
        } finally {
            server.stop();
        }
    }
}
