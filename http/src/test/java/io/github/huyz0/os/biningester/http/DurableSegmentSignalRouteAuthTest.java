// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.DurableSegmentSignalFrame;
import io.github.huyz0.os.biningester.format.SegmentKey;
import io.helidon.webclient.api.WebClient;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class DurableSegmentSignalRouteAuthTest {
    @Test
    void httpRouteRefusesAValidClaimFromAnAddressOutsideReadyMembership() throws Exception {
        EndpointSliceView emptyMembership = new EndpointSliceView();
        var calls = new AtomicInteger();
        var service = new DurableSegmentSignalService(emptyMembership,
                (key, az) -> calls.incrementAndGet());
        var server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().register(service)).build().start();
        String key = new SegmentKey("bins/c", 1_700_000_000_000L, "writera", 1, 48).key();
        byte[] body = new DurableSegmentSignalFrame("writera", "az-a", key).encode();
        try {
            try (var response = WebClient.builder()
                    .baseUri("http://127.0.0.1:" + server.port()).build()
                    .post(DurableSegmentSignalService.PATH).submit(body)) {
                assertThat(response.status().code()).isEqualTo(403);
            }
        } finally {
            server.stop();
        }
        assertThat(calls).hasValue(0);
    }
}
