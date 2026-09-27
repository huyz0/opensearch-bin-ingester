// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.client.SegmentFetchRoute;
import io.github.huyz0.os.biningester.format.SegmentKey;
import io.github.huyz0.os.biningester.ingest.SegmentProxy;
import io.github.huyz0.os.biningester.ingest.SegmentSink;
import io.helidon.http.HeaderNames;
import io.helidon.http.Status;
import io.helidon.webserver.http.HttpRules;
import io.helidon.webserver.http.HttpService;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Objects;

/**
 * The proxy segment route: {@code GET /seg?key=…&az=…} answers the WHOLE
 * segment, streamed (M10.1, ADR-0073, FR-6).
 *
 * <p>⚠️ **THE KEY IS CHECKED BEFORE ANY STORE REQUEST.** It must be a
 * canonical data {@link SegmentKey} under this deployment's own prefix; a
 * lease, a commit-log key, another trust domain's segment or anything
 * {@code SegmentKey.parse} cannot reproduce exactly is a {@code 400} that costs
 * nothing. This route is unauthenticated like every consumer route, so without
 * the check it would read any object it was named -- control-plane objects and
 * other domains' data included.
 *
 * <p>⚠️ **STREAMED THROUGH {@link SegmentProxy}, NEVER BUFFERED THEN
 * FORWARDED** (research 10 §1: +0.5 ms streamed against +6 ms per 8 MiB
 * buffered). A hit is written from the node-wide cache; a miss is one store GET
 * relayed chunk by chunk while it fills that cache -- the same fill the
 * prefetcher and the subscription path use. A fetch after a prefetch costs no
 * store request ONLY on the pod that prefetched, the AZ's ring owner
 * (ADR-0066); on any other pod of that AZ the first fetch is one cold GET.
 *
 * <p>⚠️ **THE RESPONSE IS COMMITTED BY THE FIRST BYTE, AND FLUSHED THERE TO
 * MAKE THAT TRUE.** Helidon buffers a small response, so without the flush a
 * failure after a few hundred bytes was answered as a complete {@code 500}
 * with a body -- measured in review -- rather than as an aborted stream. Until
 * the first byte the status can still say what went wrong: {@code 404} when
 * the object is absent, {@code 502} when the store failed on an object that
 * exists or could not be asked. A {@code 404} is permanent to a consumer, so
 * an outage must never be reported as one. After the first byte a store
 * failure can only abort the connection; the consumer then holds a truncated
 * segment, which {@code SegmentReader} refuses by its footer.
 *
 * <p>⚠️ **A KEY THAT WAS ABSENT IS REMEMBERED.** The route is unauthenticated,
 * and an absent key costs a GET and a STAT; a consumer re-asking for a segment
 * GC has deleted would otherwise buy both on every retry. A segment is
 * written once and announced only after it is durable, so an absent key never
 * becomes present. What this does NOT bound is a caller inventing a fresh
 * well-formed key per request -- two store requests each; ADR-0073 records
 * that exposure.
 *
 * <p>⚠️ **COUNTED WHERE IT IS SENT**, as {@code PROXY_READ} against the
 * caller's {@code az} -- the poll's rule (M9.2), so NFR-5's largest term is a
 * measured count. Absent is cross-AZ and unattributed, never same-AZ.
 */
public final class SegmentFetchService implements HttpService {

    /** Reads a segment into sinks: the ingester's {@link SegmentProxy}. */
    @FunctionalInterface
    interface Streamer {
        void stream(String key, SegmentSink sink) throws IOException;
    }

    /** Whether an object exists: asked only after a failed read. */
    @FunctionalInterface
    interface Presence {
        boolean exists(String key) throws IOException;
    }

    private final String prefix;
    private final Streamer streamer;
    private final Presence presence;
    private final CrossAzBytes crossAz;

    /** Keys answered {@code 404}, most recent last; bounded. */
    private final java.util.Map<String, Boolean> absent = java.util.Collections.synchronizedMap(
            new java.util.LinkedHashMap<>(64, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(java.util.Map.Entry<String, Boolean> eldest) {
                    return size() > ABSENT_REMEMBERED;
                }
            });

    /** How many absent keys are remembered: a bound on memory, not on correctness. */
    static final int ABSENT_REMEMBERED = 1_024;

    SegmentFetchService(String prefix, Streamer streamer, Presence presence,
            CrossAzBytes crossAz) {
        this.prefix = Objects.requireNonNull(prefix, "prefix");
        this.streamer = Objects.requireNonNull(streamer, "streamer");
        this.presence = Objects.requireNonNull(presence, "presence");
        this.crossAz = Objects.requireNonNull(crossAz, "crossAz");
    }

    /**
     * The route over a pod's proxy and store.
     *
     * @param prefix this deployment's key prefix; only data segments under it
     *     are served
     */
    public static SegmentFetchService over(String prefix, SegmentProxy proxy, BinStore store,
            CrossAzBytes crossAz) {
        Objects.requireNonNull(proxy, "proxy");
        Objects.requireNonNull(store, "store");
        return new SegmentFetchService(prefix,
                (key, sink) -> proxy.streamTo(key, List.of(sink)),
                key -> store.stat(key).isPresent(), crossAz);
    }

    @Override
    public void routing(HttpRules rules) {
        rules.get(SegmentFetchRoute.PATH, this::fetch);
    }

    /** Whether {@code key} is a canonical data segment of this deployment. */
    boolean servable(String key) {
        if (key == null || key.isEmpty()) {
            return false;
        }
        try {
            return prefix.equals(SegmentKey.parse(key).prefix());
        } catch (IllegalArgumentException | IllegalStateException notASegment) {
            return false;
        }
    }

    private void fetch(ServerRequest request, ServerResponse response) {
        String key = request.query().first(SegmentFetchRoute.KEY_PARAM).orElse(null);
        if (!servable(key)) {
            response.status(Status.BAD_REQUEST_400)
                    .send("not a data segment of this deployment");
            return;
        }
        if (absent.containsKey(key)) {
            response.status(Status.NOT_FOUND_404).send("no such segment");
            return;
        }
        String callerAz = request.query().first(SegmentFetchRoute.AZ_PARAM).orElse(null);
        Relay relay = new Relay(response);
        try {
            streamer.stream(key, relay);
        } catch (IOException failed) {
            if (relay.started()) {
                relay.count(callerAz);
                // ⚠️ NOTHING TRUTHFUL CAN BE SAID ON A COMMITTED RESPONSE:
                // aborting the connection is what tells the consumer the
                // segment is incomplete.
                throw new UncheckedIOException("the store failed mid-segment", failed);
            }
            answerFailure(key, response);
            return;
        }
        relay.count(callerAz);
        if (!relay.started()) {
            // ⚠️ A zero-length object is not a segment, but it is what is
            // stored; sent as such rather than invented into an error.
            response.header(HeaderNames.CONTENT_TYPE, "application/octet-stream")
                    .send(new byte[0]);
            return;
        }
        relay.finish();
    }

    private void answerFailure(String key, ServerResponse response) {
        boolean exists;
        try {
            exists = presence.exists(key);
        } catch (IOException unreachable) {
            exists = true;
        }
        if (exists) {
            response.status(Status.BAD_GATEWAY_502).send("the object store did not answer");
        } else {
            absent.put(key, Boolean.TRUE);
            response.status(Status.NOT_FOUND_404).send("no such segment");
        }
    }

    /** The response as a sink, opened by its first byte and counting what it sent. */
    private final class Relay implements SegmentSink {
        private final ServerResponse response;
        private OutputStream out;
        private long sent;

        Relay(ServerResponse response) {
            this.response = response;
        }

        @Override
        public void write(byte[] buffer, int offset, int length) throws IOException {
            boolean first = out == null;
            if (first) {
                response.header(HeaderNames.CONTENT_TYPE, "application/octet-stream");
                out = response.outputStream();
            }
            out.write(buffer, offset, length);
            if (first) {
                // ⚠️ COMMITS THE HEADERS: see the class comment.
                out.flush();
            }
            // ⚠️ WRITTEN, NOT PROVEN DELIVERED: bytes handed to a socket that
            // then drops are counted, the same over-count, in the same
            // direction, as the poll's.
            sent += length;
        }

        boolean started() {
            return out != null;
        }

        void count(String callerAz) {
            crossAz.sent(CrossAzBytes.Transport.PROXY_READ, callerAz, sent);
        }

        void finish() {
            try {
                out.close();
            } catch (IOException closed) {
                throw new UncheckedIOException(closed);
            }
        }
    }
}
