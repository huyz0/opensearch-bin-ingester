// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.format.SegmentKey;
import io.github.huyz0.os.biningester.ingest.SegmentSink;
import io.helidon.http.Status;
import io.helidon.webserver.http.HttpRules;
import io.helidon.webserver.http.HttpService;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;

/**
 * The proxy route: one segment, streamed to the consumer that was pushed a
 * {@code proxy} event for it (M10.1, FR-6, ADR-0073).
 *
 * <p>⚠️ **BEFORE THIS ROUTE A {@code proxy} DELIVERY COULD NOT BE DECODED.** The
 * event names a segment and carries no bytes, and nothing served them, so a
 * consumer handed one decoded an empty array and threw -- with the default
 * 256 KiB inline cap, any run larger than that.
 *
 * <p>⚠️ **ONLY THIS DEPLOYMENT's SEGMENT KEYS ARE ADDRESSABLE.** The key must
 * parse as a canonical {@link SegmentKey} AND carry this deployment's prefix:
 * {@code SegmentKey.parse} accepts any prefix, so without the second check a
 * bucket shared by two deployments would serve one's segments to the other's
 * consumers (security.md rule 1). Leases, the commit log, checkpoints and
 * {@code ctl/} have no segment-key shape and are refused before the store is
 * asked. The refusal never echoes the key.
 *
 * <p>⚠️ **THE STATUS IS DECIDED BEFORE THE FIRST BYTE.** A store failure then
 * answers 503. After it, the status is committed; a failure truncates the
 * chunked body, which the consumer's client reports as an error and
 * {@code SegmentReader.open}'s footer check refuses regardless -- never a
 * short segment under 200. A missing object is 503 too: the store SPI's
 * {@code get} does not distinguish "absent" from "failing", and telling them
 * apart would cost a HEAD per failure.
 *
 * <p>⚠️ **EVERY BODY BYTE IS COUNTED ONCE AS {@code PROXY_READ}** against the
 * consumer's {@code az} parameter -- the same self-reported, accounting-only
 * value the poll uses ({@link SubscriptionService#AZ_PARAM}). It never routes.
 * A consumer naming no zone is counted as unknown, which the counter treats as
 * cross-AZ: the safe side for NFR-5.
 */
public final class SegmentService implements HttpService {

    /** The route. */
    public static final String PATH = "/seg";

    /** The query parameter naming the segment's object key. */
    public static final String KEY_PARAM = "key";

    /** The consumer's zone, for accounting only. */
    public static final String AZ_PARAM = SubscriptionService.AZ_PARAM;

    /** Streams one segment to one sink; {@code SegmentReads#serve} in production. */
    @FunctionalInterface
    public interface Reader {
        /** @return whether the sink took the whole segment */
        boolean serve(String segmentKey, SegmentSink sink) throws IOException;
    }

    private final Reader reader;
    private final String segmentPrefix;
    private final CrossAzBytes crossAz;

    public SegmentService(Reader reader, String segmentPrefix, CrossAzBytes crossAz) {
        this.reader = Objects.requireNonNull(reader, "reader");
        this.segmentPrefix = Objects.requireNonNull(segmentPrefix, "segmentPrefix");
        this.crossAz = Objects.requireNonNull(crossAz, "crossAz");
    }

    @Override
    public void routing(HttpRules rules) {
        rules.get(PATH, this::serve);
    }

    void serve(ServerRequest request, ServerResponse response) {
        Optional<String> key = request.query().first(KEY_PARAM).asOptional()
                .filter(this::isThisDeploymentsSegment);
        if (key.isEmpty()) {
            response.status(Status.BAD_REQUEST_400)
                    .send("not a segment key of this deployment".getBytes(StandardCharsets.UTF_8));
            return;
        }
        String consumerAz = request.query().first(AZ_PARAM).asOptional().orElse(null);

        OutputStream[] body = new OutputStream[1];
        try {
            reader.serve(key.get(), (buffer, offset, length) -> {
                if (body[0] == null) {
                    response.status(Status.OK_200);
                    body[0] = response.outputStream();
                }
                body[0].write(buffer, offset, length);
                crossAz.sent(CrossAzBytes.Transport.PROXY_READ, consumerAz, length);
            });
            if (body[0] == null) {
                response.status(Status.OK_200).send(new byte[0]);
            }
        } catch (IOException storeFailed) {
            if (body[0] == null) {
                response.status(Status.SERVICE_UNAVAILABLE_503)
                        .send("segment temporarily unavailable".getBytes(StandardCharsets.UTF_8));
            } else {
                // Streaming began: the only honest signal left is a truncated
                // body, which the chunked framing makes visible to the client.
                throw new IllegalStateException("segment stream failed after it began",
                        storeFailed);
            }
        }
        // No close here: Helidon completes the response when this handler
        // returns, and ends a chunked body cleanly only if no exception
        // escaped -- which is the truncation signal above.
    }

    private boolean isThisDeploymentsSegment(String key) {
        // SegmentKey.parse refuses a key over MAX_KEY_BYTES before parsing it.
        try {
            return SegmentKey.parse(key).prefix().equals(segmentPrefix);
        } catch (IllegalArgumentException | IllegalStateException notASegmentKey) {
            return false;
        }
    }
}
