// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.client.HttpSubscriptionTransport;
import io.github.huyz0.os.biningester.format.CatchUpEndFrame;
import io.github.huyz0.os.biningester.format.CatchUpRequestFrame;
import io.github.huyz0.os.biningester.format.RunKey;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A catch-up's answer states the fast term its pod leads (M13.32h; ADR-0082
 * §2): in its {@code Binstore-Fast-Epoch} header, set before the body streams,
 * for a pod forwarding it to check against its fence -- and none when the pod
 * leads none.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class CatchUpServiceEpochTest {

    private static final UUID REQUEST = new UUID(3, 3);
    private static final RunKey KEY = new RunKey(new UUID(1, 1), 0);

    private static HttpResponse<byte[]> catchUp(long epoch) throws Exception {
        WebServer server = WebServer.builder().port(0).routing(HttpRouting.builder().register(
                new CatchUpService((CatchUpService.StreamingResponder) (request, sink) ->
                        sink.write(new CatchUpEndFrame(REQUEST).encode()), () -> epoch)))
                .build().start();
        try {
            return HttpClient.newHttpClient().send(HttpRequest.newBuilder(URI.create(
                    "http://localhost:" + server.port() + HttpSubscriptionTransport.SUBSCRIBE_PREFIX
                            + KEY.indexId() + "/" + KEY.partitionId()))
                    .POST(HttpRequest.BodyPublishers.ofByteArray(new CatchUpRequestFrame(REQUEST,
                            List.of(new CatchUpRequestFrame.Stream(KEY, 0))).encode())).build(),
                    HttpResponse.BodyHandlers.ofByteArray());
        } finally {
            server.stop();
        }
    }

    @Test
    void anANSWERCarriesTheFastTermItsPodLeadsAndNoneWhenItLeadsNone() throws Exception {
        HttpResponse<byte[]> leading = catchUp(7);
        HttpResponse<byte[]> following = catchUp(0);

        assertThat(leading.statusCode()).isEqualTo(200);
        assertThat(leading.headers().firstValue(FastEntryService.EPOCH_HEADER)).hasValue("7");
        assertThat(following.headers().firstValue(FastEntryService.EPOCH_HEADER)).isEmpty();
    }
}
