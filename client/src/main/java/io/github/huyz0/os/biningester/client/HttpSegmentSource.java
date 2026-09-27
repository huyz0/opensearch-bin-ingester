// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import io.github.huyz0.os.biningester.format.Grant;
import io.helidon.webclient.api.HttpClientResponse;
import io.helidon.webclient.api.WebClient;
import java.io.IOException;
import java.time.Duration;
import java.util.Objects;

/**
 * The production {@link SegmentSource}: one HTTP GET of a grant's URL (M8.31,
 * FR-10, ADR-0041), or of the ingester's proxy segment route (M10.2,
 * ADR-0073).
 *
 * <p>⚠️ **BEFORE THIS, NO {@code SegmentSource} IN {@code src/main} FETCHED**
 * (M5.91b). A node wraps this in its {@code NodeSegmentSource}, which is what
 * makes the GET once per (node, segment) rather than once per run; this class
 * fetches every time it is asked and must never be handed to a client alone.
 *
 * <p>⚠️ **THE URL IS SENT AS SIGNED.** A presigned credential carries
 * {@code %2F}, and the signature is over those exact bytes, so the query is
 * never decoded or re-encoded on the way out.
 *
 * <p>⚠️ **AND IT NEVER APPEARS IN A MESSAGE.** A signed URL is a bearer
 * credential until it expires; {@link Grant#toString} redacts it for the same
 * reason (security.md).
 */
public final class HttpSegmentSource implements SegmentSource {

    private final WebClient client;
    private final String ingesterEndpoint;
    private final String consumerAz;

    public HttpSegmentSource(Duration timeout) {
        this(timeout, null, null);
    }

    /**
     * A source that also fetches {@code proxy} segments by key from the
     * ingester's segment route (M10.2, ADR-0073).
     *
     * @param ingesterEndpoint the consumer's OWN ingester -- the same-AZ one in
     *     production -- or {@code null} for a grants-only source
     * @param consumerAz this consumer's zone, sent for NFR-5 accounting; may
     *     be {@code null}, which the ingester counts as cross-AZ
     */
    public HttpSegmentSource(Duration timeout, String ingesterEndpoint, String consumerAz) {
        Objects.requireNonNull(timeout, "timeout");
        this.client = WebClient.builder().connectTimeout(timeout).readTimeout(timeout).build();
        // ⚠️ A TRAILING SLASH IS DROPPED: the subscription transport accepts
        // one through `baseUri`, and appending the route to it would ask for
        // `//seg`, a 404 while subscriptions kept working.
        this.ingesterEndpoint = ingesterEndpoint == null ? null
                : ingesterEndpoint.replaceAll("/+$", "");
        this.consumerAz = consumerAz;
    }

    /**
     * The object's bytes.
     *
     * @throws IOException on any answer but 200, or any failure to get one --
     *     ⚠️ NEVER an empty array, which would advance every run's offsets past
     *     records nobody read
     */
    @Override
    public byte[] fetch(Grant grant) throws IOException {
        Objects.requireNonNull(grant, "grant");
        try (HttpClientResponse response = client.get().skipUriEncoding(true)
                .uri(grant.url()).request()) {
            int status = response.status().code();
            if (status != 200) {
                throw new IOException("a segment GET under a grant answered " + status
                        + " (the url is withheld: it is signed)");
            }
            return response.entity().as(byte[].class);
        } catch (RuntimeException failed) {
            // ⚠️ THE CAUSE's MESSAGE IS NOT COPIED, because a client failure
            // names the URI it was fetching.
            throw new IOException("a segment GET under a grant failed: "
                    + failed.getClass().getSimpleName() + " (the url is withheld: it is signed)");
        }
    }

    /**
     * The segment's bytes from the ingester's proxy route.
     *
     * <p>⚠️ THE KEY IS NOT A SECRET, unlike a grant's URL: it is announced to
     * every subscriber in the event, so a failure may name it.
     *
     * @throws IOException on any answer but 200, any failure to get one, or
     *     when this source was built without an ingester -- ⚠️ never an empty
     *     array
     */
    @Override
    public byte[] fetchSegment(String segmentKey) throws IOException {
        Objects.requireNonNull(segmentKey, "segmentKey");
        if (ingesterEndpoint == null) {
            return SegmentSource.super.fetchSegment(segmentKey);
        }
        var request = client.get(ingesterEndpoint + SegmentFetchRoute.PATH)
                .queryParam(SegmentFetchRoute.KEY_PARAM, segmentKey);
        // ⚠️ THE SUBSCRIPTION'S OWN RULE (azParam): a blank zone is not sent,
        // and a named one is trimmed -- `az=` would be counted as a zone
        // named "" rather than as unattributed.
        String[] az = HttpSubscriptionTransport.azParam(consumerAz);
        if (az.length > 0) {
            request.queryParam(SegmentFetchRoute.AZ_PARAM, az);
        }
        try (HttpClientResponse response = request.request()) {
            int status = response.status().code();
            if (status != 200) {
                throw new IOException("proxied segment " + segmentKey + " answered " + status);
            }
            return response.entity().as(byte[].class);
        } catch (RuntimeException failed) {
            throw new IOException("proxied segment " + segmentKey + " could not be fetched: "
                    + failed.getMessage(), failed);
        }
    }
}
