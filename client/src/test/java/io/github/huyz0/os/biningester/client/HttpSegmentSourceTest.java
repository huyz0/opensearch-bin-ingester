// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.format.Grant;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The production {@link SegmentSource}: one HTTP GET of a grant's URL (M8.31,
 * FR-10).
 *
 * <p>⚠️ **BEFORE THIS, NO {@code SegmentSource} IN {@code src/main} FETCHED.**
 * {@code NodeSegmentSource} is a cache whose delegate only tests supplied, so
 * a {@code direct} delivery reaching a real node had nothing to read it with.
 */
@Timeout(30)
class HttpSegmentSourceTest {

    private static final String SIGNATURE = "X-Amz-Signature=deadbeefcafe";

    private HttpServer server;
    private final AtomicInteger gets = new AtomicInteger();
    private volatile int status = 200;
    private volatile String rawQuery;

    @BeforeEach
    void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            gets.incrementAndGet();
            rawQuery = exchange.getRequestURI().getRawQuery();
            byte[] body = ("bytes of " + exchange.getRequestURI().getPath())
                    .getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status, status == 200 ? body.length : -1);
            if (status == 200) {
                exchange.getResponseBody().write(body);
            }
            exchange.close();
        });
        server.start();
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    private Grant grant(String key) {
        return new Grant("http://127.0.0.1:" + server.getAddress().getPort() + "/bucket/" + key
                + "?" + SIGNATURE, Instant.EPOCH.plusSeconds(60));
    }

    @Test
    void aFETCHIsONEGetOfTheGRANTsURLAndReturnsItsBODY() throws Exception {
        HttpSegmentSource source = new HttpSegmentSource(Duration.ofSeconds(5));
        assertThat(new String(source.fetch(grant("seg-1")), StandardCharsets.UTF_8))
                .isEqualTo("bytes of /bucket/seg-1");
        assertThat(gets.get()).as("one GET, no probe and no retry").isEqualTo(1);
    }

    @Test
    void theSIGNEDQueryArrivesBYTEForBYTENeverReEncoded() throws Exception {
        // ⚠️ A PRESIGNED URL's CREDENTIAL CARRIES `%2F`, and the signature is
        // over the exact bytes: a client that decodes or re-encodes the query
        // turns every GET into a 403 from a store that is fine.
        String signed = "X-Amz-Credential=AKIA%2F20260919%2Fus-east-1%2Fs3&" + SIGNATURE;
        Grant grant = new Grant("http://127.0.0.1:" + server.getAddress().getPort()
                + "/bucket/seg-1?" + signed, Instant.EPOCH.plusSeconds(60));

        new HttpSegmentSource(Duration.ofSeconds(5)).fetch(grant);

        assertThat(rawQuery).isEqualTo(signed);
    }

    @Test
    void aNON200IsANIOExceptionNAMINGTheStatusAndNEVERTheSignedURL() throws Exception {
        status = 403;
        HttpSegmentSource source = new HttpSegmentSource(Duration.ofSeconds(5));
        assertThatThrownBy(() -> source.fetch(grant("seg-1")))
                .as("⚠️ AN EMPTY ARRAY WOULD ADVANCE THE OFFSETS PAST RECORDS NOBODY READ; "
                        + "and a signed URL in a message is a credential in a log")
                .isInstanceOf(IOException.class)
                .hasMessageContaining("403")
                .hasMessageNotContaining(SIGNATURE);
    }
}
