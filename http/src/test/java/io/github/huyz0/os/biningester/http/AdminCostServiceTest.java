// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;

import io.helidon.webclient.api.HttpClientResponse;
import io.helidon.webclient.api.WebClient;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * {@code GET /admin/cost} checks its two parameters and hands the source the
 * bound, refusing anything else before a report is built (M11.4, M11
 * criterion 7).
 */
class AdminCostServiceTest {

    private WebServer server;
    private final List<Integer> asked = new CopyOnWriteArrayList<>();

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop();
        }
    }

    private WebClient start() {
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().register(new AdminCostService(top -> {
                    asked.add(top);
                    return "{\"top\":" + top + "}";
                })))
                .build().start();
        return WebClient.builder().baseUri("http://localhost:" + server.port()).build();
    }

    /** {@code query} as {@code name=value} pairs joined by {@code &}, without the {@code ?}. */
    private static int status(WebClient http, String query) {
        var get = http.get(AdminCostService.PATH);
        for (String pair : query.substring(1).split("&")) {
            String[] nameValue = pair.split("=", 2);
            get = get.queryParam(nameValue[0], nameValue[1]);
        }
        try (HttpClientResponse response = get.request()) {
            return response.status().code();
        }
    }

    @Test
    void theDefaultIsTheTopFiftyByIndexAsJson() {
        WebClient http = start();

        try (HttpClientResponse response = http.get(AdminCostService.PATH).request()) {
            assertThat(response.status().code()).isEqualTo(200);
            assertThat(response.headers().contentType().orElseThrow().text())
                    .startsWith("application/json");
            assertThat(response.entity().as(String.class)).isEqualTo("{\"top\":50}");
        }
        assertThat(asked).containsExactly(50);
    }

    @Test
    void aTopInsideTheBoundIsPassedThroughExactly() {
        WebClient http = start();

        assertThat(status(http, "?by=index&top=1")).isEqualTo(200);
        assertThat(status(http, "?top=1000")).isEqualTo(200);

        assertThat(asked).containsExactly(1, 1000);
    }

    @Test
    void aTopOutsideTheBoundOrNotANumberAndAnyOtherByAreRefusedBeforeAReport() {
        WebClient http = start();

        assertThat(status(http, "?top=0")).isEqualTo(400);
        assertThat(status(http, "?top=1001")).isEqualTo(400);
        assertThat(status(http, "?top=ten")).isEqualTo(400);
        assertThat(status(http, "?by=partition")).isEqualTo(400);

        assertThat(asked).as("⚠️ NO REPORT BUILT for a refused request").isEmpty();
    }
}
