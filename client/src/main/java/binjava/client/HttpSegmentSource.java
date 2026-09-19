// SPDX-License-Identifier: Apache-2.0
package binjava.client;

import binjava.format.Grant;
import io.helidon.webclient.api.HttpClientResponse;
import io.helidon.webclient.api.WebClient;
import java.io.IOException;
import java.time.Duration;
import java.util.Objects;

/**
 * The production {@link SegmentSource}: one HTTP GET of a grant's URL (M8.31,
 * FR-10, ADR-0041).
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

    public HttpSegmentSource(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        this.client = WebClient.builder().connectTimeout(timeout).readTimeout(timeout).build();
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
}
