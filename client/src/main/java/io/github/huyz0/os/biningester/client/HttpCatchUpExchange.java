// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import io.github.huyz0.os.biningester.client.SubscriptionTransport.CatchUpResult;
import io.github.huyz0.os.biningester.format.CatchUpEndFrame;
import io.github.huyz0.os.biningester.format.CatchUpEventFrame;
import io.github.huyz0.os.biningester.format.CatchUpRequestFrame;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SubscriptionEvent;
import io.helidon.http.Status;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

/** Streams and validates one node-scoped catch-up exchange over the existing subscription route. */
final class HttpCatchUpExchange implements AutoCloseable {

    private final URI endpoint;
    private final Duration requestTimeout;
    private final HttpClient streamingClient;

    HttpCatchUpExchange(String endpoint, Duration connectTimeout, Duration pollWait) {
        this.endpoint = URI.create(Objects.requireNonNull(endpoint, "endpoint"));
        Objects.requireNonNull(connectTimeout, "connectTimeout");
        this.requestTimeout = Objects.requireNonNull(pollWait, "pollWait").plusSeconds(20);
        this.streamingClient = HttpClient.newBuilder()
                .connectTimeout(connectTimeout)
                .build();
    }

    CatchUpResult request(CatchUpRequestFrame request, Consumer<SubscriptionEvent> lane)
            throws IOException {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(lane, "lane");
        RunKey routeKey = request.streams().getFirst().key();
        HttpRequest httpRequest = HttpRequest.newBuilder(endpoint.resolve(path(routeKey)))
                .timeout(requestTimeout)
                .header("Content-Type", "application/octet-stream")
                .POST(HttpRequest.BodyPublishers.ofByteArray(request.encode()))
                .build();
        HttpResponse<InputStream> response;
        try {
            response = streamingClient.send(httpRequest, HttpResponse.BodyHandlers.ofInputStream());
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("catch-up exchange was interrupted", interrupted);
        }
        try (InputStream input = response.body()) {
            int status = response.statusCode();
            if (status == Status.NOT_FOUND_404.code()
                    || status == Status.METHOD_NOT_ALLOWED_405.code()
                    || status == Status.NOT_IMPLEMENTED_501.code()) {
                return CatchUpResult.UNSUPPORTED;
            }
            if (request.version() == CatchUpRequestFrame.VERSION_2
                    && (status == Status.BAD_REQUEST_400.code()
                            || status == Status.REQUEST_ENTITY_TOO_LARGE_413.code())) {
                // A v1 peer rejects the v2 stream bound as an unknown version
                // (400), or its old body cap rejects the larger request (413).
                return CatchUpResult.UNSUPPORTED;
            }
            if (status != Status.OK_200.code()) {
                throw new IOException("catch-up answered HTTP " + status);
            }
            return dispatch(input, request, lane);
        }
    }

    /** Reads one bounded frame at a time so a bounded consumer lane applies backpressure. */
    private static CatchUpResult dispatch(InputStream input, CatchUpRequestFrame request,
            Consumer<SubscriptionEvent> lane) throws IOException {
        Set<RunKey> requested = new HashSet<>();
        request.streams().forEach(stream -> requested.add(stream.key()));
        byte[] frame;
        while ((frame = StreamFraming.readFrame(input)) != null) {
            if (frame.length < 9) {
                throw new IOException("catch-up response frame is shorter than its header");
            }
            ByteBuffer header = ByteBuffer.wrap(frame).order(ByteOrder.BIG_ENDIAN);
            int magic = header.getInt();
            int version = header.getInt();
            int kind = Byte.toUnsignedInt(frame[8]);
            if (magic != CatchUpRequestFrame.MAGIC) {
                throw new IOException("catch-up response has invalid magic 0x"
                        + Integer.toHexString(magic));
            }
            if (version != CatchUpRequestFrame.VERSION_1) {
                throw new IOException("catch-up response version " + version
                        + " is not supported");
            }
            if (kind == CatchUpEventFrame.FRAME_KIND) {
                CatchUpEventFrame event = CatchUpEventFrame.decode(frame);
                if (event.requestId().equals(request.requestId())
                        && requested.contains(event.event().key())) {
                    lane.accept(event.event());
                }
            } else if (kind == CatchUpEndFrame.FRAME_KIND) {
                CatchUpEndFrame end = CatchUpEndFrame.decode(frame);
                if (end.requestId().equals(request.requestId())) {
                    return CatchUpResult.COMPLETE;
                }
            } else {
                throw new IOException("catch-up response has unexpected frame kind " + kind);
            }
        }
        throw new IOException("catch-up response ended before its matching end frame");
    }

    private static String path(RunKey key) {
        return "/sub/" + key.indexId() + "/" + key.partitionId();
    }

    @Override
    public void close() {
        streamingClient.close();
    }
}
