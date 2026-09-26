// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import io.helidon.webclient.api.HttpClientRequest;
import io.helidon.webclient.api.HttpClientResponse;
import io.helidon.webclient.api.WebClient;
import java.io.IOException;
import java.time.Duration;
import java.util.Objects;

/**
 * The production {@link ProxySource}: one GET of the ingester node's segment
 * route (M10.2, ADR-0073).
 *
 * <p>⚠️ **SAME ENDPOINT AS THE SUBSCRIPTION**, the plugin's one configured
 * ingester endpoint, which a deployment places in the plugin's own AZ. If that
 * endpoint is a single pod, the node that pushed the {@code proxy} event is
 * the node asked for the bytes and usually holds them. Behind a load-balanced
 * Kubernetes {@code Service} the fetch may land on another pod of the same
 * AZ, which then pays at most one cold store GET for that segment -- bounded
 * by nodes, not by consumers. There is no in-AZ failover here (M10 spec, Not in scope): a failed fetch
 * fails the delivery and the existing reconnect path takes over.
 *
 * <p>⚠️ **THIS FETCHES EVERY TIME IT IS ASKED** and must be wrapped in the
 * node's {@code NodeSegmentSource}, as {@link HttpSegmentSource} is.
 */
public final class HttpProxySource implements ProxySource {

    /** The ingester's segment route. */
    public static final String PATH = "/seg";

    /** The query parameter naming the segment's object key. */
    public static final String KEY_PARAM = "key";

    private final WebClient client;
    private final String consumerAz;

    /**
     * @param consumerAz this consumer's zone, sent for the ingester's byte
     *     accounting only; null or blank sends none
     */
    public HttpProxySource(String endpoint, Duration timeout, String consumerAz) {
        Objects.requireNonNull(endpoint, "endpoint");
        Objects.requireNonNull(timeout, "timeout");
        this.client = WebClient.builder().baseUri(endpoint)
                .connectTimeout(timeout).readTimeout(timeout).build();
        this.consumerAz = consumerAz == null || consumerAz.isBlank() ? null : consumerAz;
    }

    @Override
    public byte[] fetch(String segmentKey) throws IOException {
        Objects.requireNonNull(segmentKey, "segmentKey");
        HttpClientRequest request = client.get(PATH).queryParam(KEY_PARAM, segmentKey);
        if (consumerAz != null) {
            request = request.queryParam(HttpSubscriptionTransport.AZ_PARAM, consumerAz);
        }
        try (HttpClientResponse response = request.request()) {
            int status = response.status().code();
            if (status != 200) {
                throw new IOException("the ingester's segment route answered " + status
                        + " for " + segmentKey);
            }
            return response.entity().as(byte[].class);
        } catch (RuntimeException failed) {
            throw new IOException("the ingester's segment route failed for " + segmentKey
                    + ": " + failed.getClass().getSimpleName(), failed);
        }
    }
}
